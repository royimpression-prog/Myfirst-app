import asyncio
import hashlib
import hmac
import logging
import math
import secrets
from contextlib import asynccontextmanager
from datetime import date, datetime, timedelta
from typing import Any, Literal
from urllib.parse import urlencode

from fastapi import Depends, FastAPI, Header, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from pydantic import AliasChoices, BaseModel, ConfigDict, Field, field_validator, model_validator

from .database import Database
from .news import FirebaseSender, configured_feeds, news_worker
from .timeutils import (
    current_session_day_bounds,
    ist_session_day,
    is_market_open,
    session_day_start_utc,
    utc_iso,
    utc_now,
)
from .upstox import UpstoxClient, UpstoxError


logger = logging.getLogger(__name__)


class Settings:
    def __init__(
        self,
        client_id: str = "",
        client_secret: str = "",
        redirect_uri: str = "",
        session_secret: str = "",
        token_encryption_key: str = "",
        database_path: str = "upstox.sqlite3",
        poll_seconds: int = 15,
        request_timeout: float = 10,
        market_alert_change_percent: float = 1.0,
        cors_origins: list[str] | None = None,
    ):
        if not math.isfinite(market_alert_change_percent) or market_alert_change_percent <= 0:
            raise ValueError("MARKET_ALERT_CHANGE_PERCENT must be a finite positive number")
        self.client_id = client_id
        self.client_secret = client_secret
        self.redirect_uri = redirect_uri
        self.session_secret = session_secret
        self.token_encryption_key = token_encryption_key
        self.database_path = database_path
        self.poll_seconds = poll_seconds
        self.request_timeout = request_timeout
        self.market_alert_change_percent = market_alert_change_percent
        self.cors_origins = cors_origins or []

    @classmethod
    def from_env(cls) -> "Settings":
        import os

        return cls(
            client_id=os.getenv("UPSTOX_CLIENT_ID", ""),
            client_secret=os.getenv("UPSTOX_CLIENT_SECRET", ""),
            redirect_uri=os.getenv("UPSTOX_REDIRECT_URI", ""),
            session_secret=os.getenv("SESSION_SECRET", ""),
            token_encryption_key=os.getenv("UPSTOX_TOKEN_ENCRYPTION_KEY", ""),
            database_path=os.getenv("DATABASE_PATH", "upstox.sqlite3"),
            market_alert_change_percent=float(
                os.getenv("MARKET_ALERT_CHANGE_PERCENT", "1.0")
            ),
            cors_origins=[
                origin.strip()
                for origin in os.getenv("CORS_ORIGINS", "").split(",")
                if origin.strip()
            ],
        )


class ExchangeRequest(BaseModel):
    code: str = Field(
        min_length=1,
        max_length=4096,
        validation_alias=AliasChoices("code", "auth_code"),
    )

    @field_validator("code")
    @classmethod
    def non_whitespace_code(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("auth_code must not be blank")
        return value.strip()


class WatchSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")
    kind: Literal["option_chain", "stock_future"]
    instrument_key: str = Field(min_length=1, max_length=256)
    expiry: str | None = None
    symbol: str | None = None

    @field_validator("instrument_key", "symbol")
    @classmethod
    def trim_values(cls, value: str | None) -> str | None:
        return value.strip() if value is not None else None

    @model_validator(mode="after")
    def check_kind_fields(self) -> "WatchSpec":
        if self.kind == "option_chain":
            if not self.expiry:
                raise ValueError("option_chain requires expiry (YYYY-MM-DD)")
            try:
                date.fromisoformat(self.expiry)
            except ValueError as exc:
                raise ValueError("expiry must be YYYY-MM-DD") from exc
            if self.symbol:
                raise ValueError("option_chain does not accept symbol")
        else:
            if not self.symbol:
                raise ValueError("stock_future requires symbol")
            if self.expiry:
                raise ValueError("stock_future does not accept expiry")
        return self


class HeartbeatRequest(BaseModel):
    watches: list[WatchSpec] = Field(max_length=100)
    lease_seconds: int = Field(default=180, ge=15, le=300)


class ApiWatchSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")
    kind: Literal["option_chain", "future"]
    instrument_key: str = Field(min_length=1, max_length=256)
    expiry_date: date | None = None
    trading_symbol: str | None = None

    @field_validator("instrument_key", "trading_symbol")
    @classmethod
    def trim_api_values(cls, value: str | None) -> str | None:
        return value.strip() if value is not None else None

    @model_validator(mode="after")
    def validate_api_watch(self) -> "ApiWatchSpec":
        if self.kind == "option_chain" and (self.expiry_date is None or self.trading_symbol):
            raise ValueError("option_chain requires expiry_date and does not accept trading_symbol")
        if self.kind == "future" and (not self.trading_symbol or self.expiry_date is not None):
            raise ValueError("future requires trading_symbol and does not accept expiry_date")
        return self


class ApiWatchesRequest(BaseModel):
    watches: list[ApiWatchSpec] = Field(max_length=100)
    lease_seconds: int = Field(default=180, ge=15, le=300)


class DeviceTokenRequest(BaseModel):
    token: str = Field(min_length=1, max_length=4096)
    client_id: str = Field(min_length=1, max_length=128)

    @field_validator("token", "client_id")
    @classmethod
    def trim_values(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("field must not be blank")
        return value


def _token_digest(token: str, secret: str) -> str:
    return hmac.new(secret.encode(), token.encode(), hashlib.sha256).hexdigest()


def _oi_change(market_data: dict[str, Any]) -> Any:
    for key in ("oi_change", "change_oi", "change_in_oi"):
        if market_data.get(key) is not None:
            return market_data[key]
    oi = market_data.get("oi", market_data.get("open_interest"))
    previous = market_data.get("prev_oi", market_data.get("previous_oi"))
    if oi is not None and previous is not None:
        try:
            return float(oi) - float(previous)
        except (TypeError, ValueError):
            pass
    return None


def _history_record(tick: dict[str, Any]) -> dict[str, Any]:
    return {
        "contract_key": tick["instrument_key"],
        "timestamp": tick["observed_at"],
        "oi": tick["oi"],
        "oi_change": tick["oi_change"],
        "ltp": tick["ltp"],
        "volume": tick["volume"],
    }


def _data_list(payload: dict[str, Any]) -> list[Any]:
    data = payload.get("data", [])
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        return [data]
    return []


def _quote_map(payload: dict[str, Any], instrument_key: str) -> dict[str, Any]:
    data = payload.get("data", {})
    if isinstance(data, dict):
        quote = data.get(instrument_key)
        if isinstance(quote, dict):
            return data
        if len(data) == 1 and isinstance(next(iter(data.values())), dict):
            return {instrument_key: next(iter(data.values()))}
        return data
    if isinstance(data, list) and data and isinstance(data[0], dict):
        return {instrument_key: data[0]}
    return {}


async def _poll_one(db: Database, provider: UpstoxClient, watch: dict[str, Any]) -> float | None:
    token = watch["access_token"]
    if watch["kind"] == "option_chain":
        payload = await provider.option_chain(token, watch["instrument_key"], watch["expiry"])
        timestamp = utc_iso()
        db.save_market_cache(
            watch["upstox_user_id"],
            "option_chain",
            watch["instrument_key"],
            payload,
            observed_at=timestamp,
            expiry=watch["expiry"],
        )
        chain = _data_list(payload)
        underlying_spot = None
        for row in chain:
            if not isinstance(row, dict):
                continue
            if underlying_spot is None:
                try:
                    candidate = float(row.get("underlying_spot_price"))
                    if candidate > 0:
                        underlying_spot = candidate
                except (TypeError, ValueError):
                    pass
            for leg_name, tick_kind in (("call_options", "option_call"), ("put_options", "option_put")):
                leg = row.get(leg_name)
                if not isinstance(leg, dict):
                    continue
                market = leg.get("market_data") or {}
                if not isinstance(market, dict):
                    market = {}
                contract_key = leg.get("instrument_key")
                if not isinstance(contract_key, str) or not contract_key:
                    continue
                db.insert_tick(
                    watch,
                    observed_at=timestamp,
                    instrument_key=contract_key,
                    tick_kind=tick_kind,
                    symbol=watch.get("symbol", ""),
                    oi=market.get("oi"),
                    oi_change=_oi_change(market),
                    ltp=market.get("ltp"),
                    volume=market.get("volume"),
                    raw=leg,
                )
        return underlying_spot
    else:
        payload = await provider.future_quote(token, watch["instrument_key"])
        timestamp = utc_iso()
        db.save_market_cache(
            watch["upstox_user_id"],
            "stock_future",
            watch["instrument_key"],
            payload,
            observed_at=timestamp,
            symbol=watch["symbol"],
        )
        quotes = _quote_map(payload, watch["instrument_key"])
        quote = quotes.get(watch["instrument_key"])
        if not isinstance(quote, dict):
            return None
        try:
            market_price = float(quote.get("last_price", quote.get("ltp")))
        except (TypeError, ValueError):
            market_price = None
        db.insert_tick(
            watch,
            observed_at=timestamp,
            instrument_key=watch["instrument_key"],
            tick_kind="stock_future",
            symbol=watch["symbol"],
            oi=quote.get("oi", quote.get("open_interest")),
            oi_change=_oi_change(quote),
            ltp=quote.get("last_price", quote.get("ltp")),
            volume=quote.get("volume"),
            raw=quote,
        )
        return market_price


async def _worker(app: FastAPI) -> None:
    db: Database = app.state.db
    provider: UpstoxClient = app.state.provider
    while True:
        cycle_started = asyncio.get_running_loop().time()
        now = utc_now()
        if is_market_open(now):
            day_start = session_day_start_utc(ist_session_day(now))
            db.prune_before(utc_iso(day_start))
            watches = db.active_watches(utc_iso(now))
            prices = await asyncio.gather(
                *(_poll_safely(db, provider, watch) for watch in watches)
            )
            for watch, price in zip(watches, prices):
                if price is None:
                    continue
                change = db.record_market_quote(
                    watch["instrument_key"],
                    price,
                    app.state.settings.market_alert_change_percent,
                    observed_at=utc_iso(),
                )
                if change is not None:
                    await _send_market_alert(app, watch, price, change)
        elapsed = asyncio.get_running_loop().time() - cycle_started
        await asyncio.sleep(max(0, app.state.settings.poll_seconds - elapsed))


async def _supervise_workers(app: FastAPI) -> None:
    workers = [
        asyncio.create_task(_worker(app), name="market-data-worker"),
        asyncio.create_task(news_worker(app), name="news-worker"),
    ]
    try:
        await asyncio.gather(*workers)
    except asyncio.CancelledError:
        raise
    except Exception:
        logger.exception("A background worker stopped unexpectedly")
        raise
    finally:
        for worker in workers:
            if not worker.done():
                worker.cancel()
        await asyncio.gather(*workers, return_exceptions=True)


async def _poll_safely(
    db: Database, provider: UpstoxClient, watch: dict[str, Any]
) -> float | None:
    try:
        return await _poll_one(db, provider, watch)
    except UpstoxError as exc:
        if exc.token_invalid:
            db.invalidate_account_token(watch["upstox_user_id"])
        logger.error(
            "Market-data poll failed for %s (%s): %s",
            watch.get("instrument_key", "unknown instrument"),
            watch.get("kind", "unknown watch"),
            exc.code,
        )
        return None
    except Exception as exc:
        logger.error(
            "Market-data poll failed for %s (%s): %s",
            watch.get("instrument_key", "unknown instrument"),
            watch.get("kind", "unknown watch"),
            type(exc).__name__,
        )
        return None


async def _send_market_alert(
    app: FastAPI, watch: dict[str, Any], price: float, change: float
) -> None:
    instrument_key = watch["instrument_key"]
    symbol = watch.get("symbol") or instrument_key
    direction = "up" if change > 0 else "down"
    alert = {
        "id": f"market:{instrument_key}",
        "title": f"Market alert: {symbol} {change:+.2f}%",
        "body": f"{symbol} is {direction} {abs(change):.2f}% at {price:g}.",
        "data": {
            "type": "market_alert",
            "instrument_key": instrument_key,
            "change_percent": f"{change:.4f}",
        },
    }
    sender = app.state.fcm_sender
    if not sender.enabled:
        logger.warning(
            "Market fluctuation alert skipped because Firebase is not configured"
        )
        return
    for token in app.state.db.device_tokens():
        await sender.send(token, alert)


def create_app(
    settings: Settings | None = None,
    *,
    provider: UpstoxClient | None = None,
    start_worker: bool = True,
) -> FastAPI:
    settings = settings or Settings.from_env()
    db = (
        Database(settings.database_path, settings.token_encryption_key)
        if settings.token_encryption_key
        else None
    )
    provider = provider or UpstoxClient(
        settings.client_id,
        settings.client_secret,
        settings.redirect_uri,
        settings.request_timeout,
    )
    news_feeds = configured_feeds()
    fcm_sender = FirebaseSender()

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        if db is None:
            raise RuntimeError("UPSTOX_TOKEN_ENCRYPTION_KEY is required")
        task = asyncio.create_task(_supervise_workers(app)) if start_worker else None
        try:
            yield
        finally:
            if task:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

    app = FastAPI(title="Upstox Market Data Backend", version="1.0.0", lifespan=lifespan)
    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.cors_origins,
        allow_origin_regex=r"^https?://(localhost|127\.0\.0\.1|\[::1\]|10\.0\.2\.2)(:\d+)?$",
        allow_credentials=False,
        allow_methods=["GET", "POST", "DELETE", "OPTIONS"],
        allow_headers=["Authorization", "Content-Type", "Accept"],
    )
    app.state.db = db
    app.state.provider = provider
    app.state.settings = settings
    app.state.news_feeds = news_feeds
    app.state.fcm_sender = fcm_sender
    app.state.market_cache_locks = {}

    async def current_user(
        request: Request, authorization: str | None = Header(default=None)
    ) -> str:
        if not authorization:
            raise HTTPException(401, detail={"code": "missing_bearer_token"})
        scheme, separator, token = authorization.partition(" ")
        if not separator or scheme.lower() != "bearer" or not token or token.strip() != token:
            raise HTTPException(401, detail={"code": "invalid_authorization_header"})
        if not settings.session_secret:
            raise HTTPException(503, detail={"code": "session_auth_not_configured"})
        session_hash = _token_digest(token, settings.session_secret)
        user_id = db.user_for_session(session_hash)
        if not user_id:
            raise HTTPException(401, detail={"code": "invalid_or_expired_session"})
        request.state.session_hash = session_hash
        return user_id

    def access_token(user_id: str) -> str:
        token = db.get_access_token(user_id)
        if not token:
            raise HTTPException(401, detail={"code": "upstox_account_not_connected"})
        return token

    def map_upstox_error(exc: UpstoxError, user_id: str | None = None) -> HTTPException:
        if exc.token_invalid and user_id:
            db.invalidate_account_token(user_id)
        return HTTPException(exc.status_code, detail={"code": exc.code})

    async def cached_payload(
        user_id: str,
        kind: str,
        instrument_key: str,
        fetch,
        *,
        expiry: str = "",
        symbol: str = "",
        max_age_seconds: int | None = None,
    ) -> dict[str, Any]:
        max_age_seconds = max_age_seconds or max(30, settings.poll_seconds * 2)
        newer_than = utc_iso(utc_now() - timedelta(seconds=max_age_seconds))
        cached = db.get_market_cache(
            user_id,
            kind,
            instrument_key,
            expiry=expiry,
            symbol=symbol,
            newer_than=newer_than,
        )
        if cached:
            return cached["raw"]
        cache_key = (user_id, kind, instrument_key, expiry, symbol)
        lock = app.state.market_cache_locks.setdefault(cache_key, asyncio.Lock())
        async with lock:
            cached = db.get_market_cache(
                user_id,
                kind,
                instrument_key,
                expiry=expiry,
                symbol=symbol,
                newer_than=newer_than,
            )
            if cached:
                return cached["raw"]
            payload = await fetch()
            db.save_market_cache(
                user_id,
                kind,
                instrument_key,
                payload,
                observed_at=utc_iso(),
                expiry=expiry,
                symbol=symbol,
            )
            return payload

    def register_session(raw_session: str, user_id: str) -> str:
        expires_at = utc_iso(utc_now() + timedelta(days=30))
        db.create_session(_token_digest(raw_session, settings.session_secret), user_id, expires_at)
        return expires_at

    async def exchange_auth_code(code: str) -> dict[str, Any]:
        if not settings.session_secret:
            raise HTTPException(503, detail={"code": "session_auth_not_configured"})
        try:
            upstox_token, user_id = await provider.exchange_code(code)
        except UpstoxError as exc:
            raise map_upstox_error(exc) from exc
        db.save_account(user_id, upstox_token)
        raw_session = secrets.token_urlsafe(32)
        expires_at = register_session(raw_session, user_id)
        return {
            "session_token": raw_session,
            "token_type": "Bearer",
            "expires_at": expires_at,
            "upstox_user_id": user_id,
        }

    async def get_option_chain_for(
        user_id: str, instrument_key: str, expiry: date
    ) -> dict[str, Any]:
        if not db.owns_active_watch(user_id, "option_chain", instrument_key, expiry.isoformat()):
            raise HTTPException(404, detail={"code": "active_watch_not_found"})
        try:
            payload = await cached_payload(
                user_id,
                "option_chain",
                instrument_key,
                lambda: provider.option_chain(
                    access_token(user_id), instrument_key, expiry.isoformat()
                ),
                expiry=expiry.isoformat(),
            )
            return {"data": _data_list(payload)}
        except UpstoxError as exc:
            raise map_upstox_error(exc, user_id) from exc

    async def get_future_quote_for(
        user_id: str, instrument_key: str, symbol: str | None = None
    ) -> dict[str, Any]:
        if not db.owns_active_watch(user_id, "stock_future", instrument_key, symbol=symbol):
            raise HTTPException(404, detail={"code": "active_watch_not_found"})
        try:
            watch = next(
                watch
                for watch in db.active_watches()
                if watch["upstox_user_id"] == user_id
                and watch["kind"] == "stock_future"
                and watch["instrument_key"] == instrument_key
                and (symbol is None or watch["symbol"] == symbol)
            )
            payload = await cached_payload(
                user_id,
                "stock_future",
                instrument_key,
                lambda: provider.future_quote(access_token(user_id), instrument_key),
                symbol=watch["symbol"],
            )
            return {"data": _quote_map(payload, instrument_key)}
        except UpstoxError as exc:
            raise map_upstox_error(exc, user_id) from exc

    @app.get("/health")
    async def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.post("/auth/exchange")
    async def exchange(body: ExchangeRequest) -> dict[str, Any]:
        return await exchange_auth_code(body.code)

    @app.get("/api/auth/login-url")
    async def login_url() -> dict[str, str]:
        if not (settings.client_id and settings.redirect_uri):
            raise HTTPException(503, detail={"code": "oauth_not_configured"})
        query = urlencode(
            {
                "client_id": settings.client_id,
                "redirect_uri": settings.redirect_uri,
                "response_type": "code",
            }
        )
        return {"url": f"https://api.upstox.com/v2/login/authorization/dialog?{query}"}

    @app.post("/api/auth/exchange")
    async def api_exchange(body: ExchangeRequest) -> dict[str, Any]:
        return await exchange_auth_code(body.code)

    @app.get("/api/auth/session")
    async def api_session(user_id: str = Depends(current_user)) -> dict[str, Any]:
        return {"authenticated": True, "upstox_user_id": user_id}

    @app.post("/api/auth/logout")
    async def api_logout(
        request: Request, user_id: str = Depends(current_user)
    ) -> dict[str, bool]:
        del user_id
        db.delete_session(request.state.session_hash)
        return {"logged_out": True}

    @app.post("/api/devices/fcm-token")
    async def register_news_device(
        body: DeviceTokenRequest,
        request: Request,
        user_id: str = Depends(current_user),
    ) -> dict[str, bool]:
        del user_id
        db.register_device_token(body.token, request.state.session_hash, body.client_id)
        return {"registered": True}

    @app.delete("/api/devices/fcm-token")
    async def unregister_news_device(
        request: Request,
        client_id: str = Query(min_length=1, max_length=128),
        user_id: str = Depends(current_user),
    ) -> dict[str, bool]:
        del user_id
        client_id = client_id.strip()
        if not client_id:
            raise HTTPException(422, detail={"code": "invalid_client_id"})
        removed = db.unregister_device_token(
            request.state.session_hash, client_id
        )
        return {"unregistered": removed}

    @app.get("/api/news")
    async def api_news(
        limit: int = Query(default=50, ge=1, le=100),
        user_id: str = Depends(current_user),
    ) -> dict[str, Any]:
        del user_id
        rows = db.latest_news(limit)
        return {
            "data": [
                {
                    "id": item["id"],
                    "title": item["title"],
                    "summary": item["description"],
                    "source": item["source"],
                    "url": item["url"],
                    "published_at": item["published_at"],
                    "impact": item["sentiment"].lower(),
                    "reason": item["cause_en"],
                    "reason_bn": item["cause_bn"],
                    "impact_bn": item["impact_bn"],
                    "high_impact": item["high_impact"],
                }
                for item in rows
            ]
        }

    @app.post("/heartbeat")
    async def heartbeat(
        body: HeartbeatRequest,
        request: Request,
        user_id: str = Depends(current_user),
    ) -> dict[str, Any]:
        keys = [
            (item.kind, item.instrument_key, item.expiry or "", item.symbol or "")
            for item in body.watches
        ]
        if len(keys) != len(set(keys)):
            raise HTTPException(422, detail={"code": "duplicate_watch"})
        lease_until = utc_iso(utc_now() + timedelta(seconds=body.lease_seconds))
        rows = db.replace_watches(
            user_id,
            [item.model_dump(exclude_none=True) for item in body.watches],
            lease_until,
            request.state.session_hash,
        )
        return {"lease_until": lease_until, "watches": rows}

    @app.post("/api/watches")
    async def api_watches(
        body: ApiWatchesRequest,
        request: Request,
        user_id: str = Depends(current_user),
    ) -> dict[str, Any]:
        specs = [
            {
                "kind": "option_chain" if watch.kind == "option_chain" else "stock_future",
                "instrument_key": watch.instrument_key,
                **(
                    {"expiry": watch.expiry_date.isoformat()}
                    if watch.expiry_date
                    else {"symbol": watch.trading_symbol}
                ),
            }
            for watch in body.watches
        ]
        keys = [
            (item["kind"], item["instrument_key"], item.get("expiry", ""), item.get("symbol", ""))
            for item in specs
        ]
        if len(keys) != len(set(keys)):
            raise HTTPException(422, detail={"code": "duplicate_watch"})
        lease_until = utc_iso(utc_now() + timedelta(seconds=body.lease_seconds))
        rows = db.replace_watches(
            user_id, specs, lease_until, request.state.session_hash
        )
        response_watches = []
        for row in rows:
            watch: dict[str, Any] = {
                "id": row["id"],
                "kind": "option_chain" if row["kind"] == "option_chain" else "future",
                "instrument_key": row["instrument_key"],
                "lease_until": row["lease_until"],
            }
            if row["kind"] == "option_chain":
                watch["expiry_date"] = row["expiry"]
            else:
                watch["trading_symbol"] = row["symbol"]
            response_watches.append(watch)
        return {"lease_until": lease_until, "watches": response_watches}

    @app.get("/option-chain")
    async def option_chain(
        instrument_key: str = Query(min_length=1),
        expiry: date = Query(),
        user_id: str = Depends(current_user),
    ) -> dict[str, Any]:
        return await get_option_chain_for(user_id, instrument_key, expiry)

    @app.get("/api/option-chain")
    async def api_option_chain(
        instrument_key: str = Query(min_length=1),
        expiry_date: date = Query(),
        user_id: str = Depends(current_user),
    ) -> dict[str, Any]:
        return await get_option_chain_for(user_id, instrument_key, expiry_date)

    @app.get("/futures/quote")
    async def futures_quote(
        instrument_key: str = Query(min_length=1),
        user_id: str = Depends(current_user),
    ) -> dict[str, Any]:
        return await get_future_quote_for(user_id, instrument_key)

    @app.get("/api/futures-quote")
    async def api_futures_quote(
        instrument_key: str = Query(min_length=1),
        trading_symbol: str = Query(min_length=1),
        user_id: str = Depends(current_user),
    ) -> dict[str, Any]:
        if not db.owns_active_watch(
            user_id, "stock_future", instrument_key, symbol=trading_symbol
        ):
            raise HTTPException(404, detail={"code": "active_watch_not_found"})
        return await get_future_quote_for(user_id, instrument_key, trading_symbol)

    @app.get("/api/ltp")
    async def api_ltp(
        instrument_key: str = Query(min_length=1),
        user_id: str = Depends(current_user),
    ) -> dict[str, Any]:
        future_watch = next(
            (
                watch
                for watch in db.active_watches()
                if watch["upstox_user_id"] == user_id
                and watch["kind"] == "stock_future"
                and watch["instrument_key"] == instrument_key
            ),
            None,
        )
        option_watch = next(
            (
                watch
                for watch in db.active_watches()
                if watch["upstox_user_id"] == user_id
                and watch["kind"] == "option_chain"
                and watch["instrument_key"] == instrument_key
            ),
            None,
        )
        if future_watch is not None:
            response = await get_future_quote_for(user_id, instrument_key)
            quote = response["data"].get(instrument_key)
            if not isinstance(quote, dict):
                raise HTTPException(502, detail={"code": "upstox_quote_missing"})
            last_price = quote.get("last_price", quote.get("ltp"))
        elif option_watch is not None:
            try:
                payload = await cached_payload(
                    user_id,
                    "spot_ltp",
                    instrument_key,
                    lambda: provider.fetch(
                        access_token(user_id),
                        "/market-quote/ltp",
                        {"instrument_key": instrument_key},
                    ),
                    max_age_seconds=settings.poll_seconds,
                )
            except UpstoxError as exc:
                raise map_upstox_error(exc, user_id) from exc
            quotes = _quote_map(payload, instrument_key)
            quote = quotes.get(instrument_key)
            if not isinstance(quote, dict):
                raise HTTPException(502, detail={"code": "upstox_quote_missing"})
            last_price = quote.get("last_price", quote.get("ltp"))
        else:
            raise HTTPException(404, detail={"code": "active_watch_not_found"})
        if last_price is None:
            raise HTTPException(502, detail={"code": "upstox_quote_missing_last_price"})
        return {"last_price": last_price}

    @app.get("/option-contracts")
    async def option_contracts(
        instrument_key: str = Query(min_length=1),
        expiry: date | None = Query(default=None),
        user_id: str = Depends(current_user),
    ) -> dict[str, Any]:
        try:
            payload = await cached_payload(
                user_id,
                "option_contracts",
                instrument_key,
                lambda: provider.option_contracts(
                    access_token(user_id),
                    instrument_key,
                    expiry.isoformat() if expiry else None,
                ),
                expiry=expiry.isoformat() if expiry else "",
                max_age_seconds=3600,
            )
            return {"data": _data_list(payload)}
        except UpstoxError as exc:
            raise map_upstox_error(exc, user_id) from exc

    @app.get("/api/option-contracts")
    async def api_option_contracts(
        instrument_key: str = Query(min_length=1),
        expiry_date: date | None = Query(default=None),
        user_id: str = Depends(current_user),
    ) -> dict[str, Any]:
        try:
            payload = await cached_payload(
                user_id,
                "option_contracts",
                instrument_key,
                lambda: provider.option_contracts(
                    access_token(user_id),
                    instrument_key,
                    expiry_date.isoformat() if expiry_date else None,
                ),
                expiry=expiry_date.isoformat() if expiry_date else "",
                max_age_seconds=3600,
            )
            return {"data": _data_list(payload)}
        except UpstoxError as exc:
            raise map_upstox_error(exc, user_id) from exc

    @app.get("/option-expiries")
    async def option_expiries(
        instrument_key: str = Query(min_length=1),
        user_id: str = Depends(current_user),
    ) -> dict[str, Any]:
        try:
            return await provider.option_expiries(access_token(user_id), instrument_key)
        except UpstoxError as exc:
            raise map_upstox_error(exc, user_id) from exc

    @app.get("/history")
    async def history(
        watch_id: int | None = Query(default=None, ge=1),
        instrument_key: str | None = Query(default=None, min_length=1),
        expiry_date: date | None = Query(default=None),
        contract_key: str | None = Query(default=None, min_length=1),
        limit: int = Query(default=1000, ge=1, le=10000),
        user_id: str = Depends(current_user),
    ) -> dict[str, Any]:
        if watch_id is None and instrument_key is None and contract_key is None:
            raise HTTPException(422, detail={"code": "watch_id_or_instrument_key_required"})
        start, end = current_session_day_bounds()
        queried_key = contract_key or instrument_key
        ticks = db.history(
            user_id,
            watch_id=watch_id,
            instrument_key=queried_key,
            expiry=expiry_date.isoformat() if expiry_date else None,
            start=start,
            end=end,
            limit=limit,
        )
        records = [_history_record(tick) for tick in ticks]
        return {
            "session_day": ist_session_day().isoformat(),
            "data": records,
        }

    @app.get("/api/history")
    async def api_history(
        instrument_key: str = Query(min_length=1),
        expiry_date: date | None = Query(default=None),
        contract_key: str | None = Query(default=None, min_length=1),
        limit: int = Query(default=1000, ge=1, le=10000),
        user_id: str = Depends(current_user),
    ) -> dict[str, Any]:
        start, end = current_session_day_bounds()
        ticks = db.history(
            user_id,
            watch_id=None,
            instrument_key=contract_key or instrument_key,
            expiry=expiry_date.isoformat() if expiry_date else None,
            start=start,
            end=end,
            limit=limit,
        )
        records = [_history_record(tick) for tick in ticks]
        return {"data": records}

    return app


app = create_app()
