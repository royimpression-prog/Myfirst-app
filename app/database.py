import json
import sqlite3
from pathlib import Path
from typing import Any

from cryptography.fernet import Fernet, InvalidToken

from .timeutils import utc_iso, utc_now


SCHEMA = """
PRAGMA foreign_keys = ON;
CREATE TABLE IF NOT EXISTS accounts (
    upstox_user_id TEXT PRIMARY KEY,
    access_token TEXT NOT NULL,
    token_valid INTEGER NOT NULL DEFAULT 1,
    updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS sessions (
    token_hash TEXT PRIMARY KEY,
    upstox_user_id TEXT NOT NULL REFERENCES accounts(upstox_user_id) ON DELETE CASCADE,
    expires_at TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS sessions_expiry ON sessions(expires_at);
CREATE TABLE IF NOT EXISTS watches (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    upstox_user_id TEXT NOT NULL REFERENCES accounts(upstox_user_id) ON DELETE CASCADE,
    kind TEXT NOT NULL CHECK (kind IN ('option_chain', 'stock_future')),
    instrument_key TEXT NOT NULL,
    expiry TEXT NOT NULL DEFAULT '',
    symbol TEXT NOT NULL DEFAULT '',
    session_hash TEXT NOT NULL DEFAULT '',
    active INTEGER NOT NULL DEFAULT 1,
    lease_until TEXT NOT NULL,
    UNIQUE(session_hash, kind, instrument_key, expiry, symbol)
);
CREATE INDEX IF NOT EXISTS watches_lease ON watches(lease_until);
CREATE TABLE IF NOT EXISTS ticks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    watch_id INTEGER NOT NULL REFERENCES watches(id) ON DELETE CASCADE,
    upstox_user_id TEXT NOT NULL,
    observed_at TEXT NOT NULL,
    instrument_key TEXT NOT NULL,
    tick_kind TEXT NOT NULL,
    symbol TEXT NOT NULL DEFAULT '',
    oi REAL,
    oi_change REAL,
    ltp REAL,
    volume REAL,
    raw_json TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS ticks_watch_time ON ticks(watch_id, observed_at);
CREATE INDEX IF NOT EXISTS ticks_user_instrument_time
    ON ticks(upstox_user_id, instrument_key, observed_at);
CREATE TABLE IF NOT EXISTS market_cache (
    upstox_user_id TEXT NOT NULL,
    kind TEXT NOT NULL,
    instrument_key TEXT NOT NULL,
    expiry TEXT NOT NULL DEFAULT '',
    symbol TEXT NOT NULL DEFAULT '',
    observed_at TEXT NOT NULL,
    raw_json TEXT NOT NULL,
    PRIMARY KEY(upstox_user_id, kind, instrument_key, expiry, symbol)
);
CREATE TABLE IF NOT EXISTS news_items (
    id TEXT PRIMARY KEY,
    source TEXT NOT NULL,
    title TEXT NOT NULL,
    description TEXT NOT NULL DEFAULT '',
    url TEXT NOT NULL DEFAULT '',
    published_at TEXT NOT NULL,
    sentiment TEXT NOT NULL CHECK (sentiment IN ('Bullish', 'Bearish', 'Neutral')),
    cause_en TEXT NOT NULL,
    cause_bn TEXT NOT NULL,
    impact_en TEXT NOT NULL,
    impact_bn TEXT NOT NULL,
    high_impact INTEGER NOT NULL DEFAULT 0 CHECK (high_impact IN (0, 1)),
    fetched_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS news_published ON news_items(published_at DESC);
CREATE TABLE IF NOT EXISTS device_tokens (
    token TEXT PRIMARY KEY,
    session_hash TEXT NOT NULL REFERENCES sessions(token_hash) ON DELETE CASCADE,
    client_id TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS device_tokens_session ON device_tokens(session_hash);
CREATE TABLE IF NOT EXISTS market_quote_state (
    instrument_key TEXT PRIMARY KEY,
    price REAL NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS market_alert_state (
    instrument_key TEXT PRIMARY KEY,
    alerted_at TEXT NOT NULL
);
"""


class Database:
    def __init__(self, path: str, token_encryption_key: str):
        if not token_encryption_key:
            raise ValueError("UPSTOX_TOKEN_ENCRYPTION_KEY is required")
        try:
            self._cipher = Fernet(token_encryption_key.encode("ascii"))
        except (ValueError, UnicodeEncodeError) as exc:
            raise ValueError(
                "UPSTOX_TOKEN_ENCRYPTION_KEY must be a valid Fernet key"
            ) from exc
        self.path = path
        if path != ":memory:":
            Path(path).expanduser().resolve().parent.mkdir(parents=True, exist_ok=True)
        self.initialize()

    def connect(self) -> sqlite3.Connection:
        db = sqlite3.connect(self.path, timeout=10)
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA foreign_keys = ON")
        return db

    def initialize(self) -> None:
        with self.connect() as db:
            db.executescript(SCHEMA)
            columns = {row["name"] for row in db.execute("PRAGMA table_info(ticks)")}
            if "oi_change" not in columns:
                db.execute("ALTER TABLE ticks ADD COLUMN oi_change REAL")
            account_columns = {
                row["name"] for row in db.execute("PRAGMA table_info(accounts)")
            }
            if "token_valid" not in account_columns:
                db.execute(
                    "ALTER TABLE accounts ADD COLUMN token_valid INTEGER NOT NULL DEFAULT 1"
                )
            watch_columns = {
                row["name"] for row in db.execute("PRAGMA table_info(watches)")
            }
            if "active" not in watch_columns:
                db.execute(
                    "ALTER TABLE watches ADD COLUMN active INTEGER NOT NULL DEFAULT 1"
                )
            device_token_columns = {
                row["name"] for row in db.execute("PRAGMA table_info(device_tokens)")
            }
            if "client_id" not in device_token_columns:
                db.execute(
                    "ALTER TABLE device_tokens ADD COLUMN client_id TEXT NOT NULL DEFAULT ''"
                )
            db.execute(
                "UPDATE device_tokens SET client_id=token WHERE client_id=''"
            )
            db.execute(
                """CREATE UNIQUE INDEX IF NOT EXISTS device_tokens_session_client
                   ON device_tokens(session_hash, client_id)"""
            )
            # Upgrade existing databases in place; legacy access tokens were plaintext.
            rows = db.execute(
                "SELECT upstox_user_id, access_token FROM accounts"
            ).fetchall()
            migrated = False
            for row in rows:
                stored = row["access_token"]
                if not stored.startswith("enc:v1:"):
                    encrypted = self._encrypt_token(stored)
                    db.execute(
                        "UPDATE accounts SET access_token=? WHERE upstox_user_id=?",
                        (encrypted, row["upstox_user_id"]),
                    )
                    migrated = True
                else:
                    self._decrypt_token(stored)
            if migrated and self.path != ":memory:":
                db.commit()
                db.execute("VACUUM")

    def save_account(self, user_id: str, access_token: str) -> None:
        encrypted_token = self._encrypt_token(access_token)
        with self.connect() as db:
            db.execute(
                """INSERT INTO accounts(upstox_user_id, access_token, updated_at)
                   VALUES (?, ?, ?)
                   ON CONFLICT(upstox_user_id) DO UPDATE SET
                     access_token=excluded.access_token, token_valid=1,
                     updated_at=excluded.updated_at""",
                (user_id, encrypted_token, utc_iso()),
            )

    def create_session(self, token_hash: str, user_id: str, expires_at: str) -> None:
        with self.connect() as db:
            db.execute(
                "INSERT INTO sessions(token_hash, upstox_user_id, expires_at, created_at) VALUES (?, ?, ?, ?)",
                (token_hash, user_id, expires_at, utc_iso()),
            )

    def delete_session(self, token_hash: str) -> None:
        with self.connect() as db:
            db.execute(
                "UPDATE watches SET active=0, lease_until=? WHERE session_hash=?",
                (utc_iso(), token_hash),
            )
            db.execute("DELETE FROM sessions WHERE token_hash=?", (token_hash,))

    def register_device_token(self, token: str, session_hash: str, client_id: str) -> None:
        with self.connect() as db:
            db.execute(
                "DELETE FROM device_tokens WHERE session_hash=? AND client_id=?",
                (session_hash, client_id),
            )
            db.execute(
                """INSERT INTO device_tokens(token, session_hash, client_id, created_at)
                   VALUES (?, ?, ?, ?)
                   ON CONFLICT(token) DO UPDATE SET
                     session_hash=excluded.session_hash, client_id=excluded.client_id,
                     created_at=excluded.created_at""",
                (token, session_hash, client_id, utc_iso()),
            )

    def unregister_device_token(self, session_hash: str, client_id: str) -> bool:
        with self.connect() as db:
            cursor = db.execute(
                "DELETE FROM device_tokens WHERE session_hash=? AND client_id=?",
                (session_hash, client_id),
            )
            return cursor.rowcount > 0

    def device_tokens(self) -> list[str]:
        now = utc_iso()
        with self.connect() as db:
            rows = db.execute(
                """SELECT d.token FROM device_tokens d
                   JOIN sessions s ON s.token_hash=d.session_hash
                   WHERE s.expires_at>?""",
                (now,),
            ).fetchall()
        return [str(row["token"]) for row in rows]

    def save_news(self, item: dict[str, Any]) -> bool:
        with self.connect() as db:
            cursor = db.execute(
                """INSERT OR IGNORE INTO news_items(
                       id, source, title, description, url, published_at, sentiment,
                       cause_en, cause_bn, impact_en, impact_bn, high_impact, fetched_at
                   ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    item["id"], item["source"], item["title"], item.get("description", ""),
                    item.get("url", ""), item["published_at"], item["sentiment"],
                    item["cause_en"], item["cause_bn"], item["impact_en"], item["impact_bn"],
                    int(bool(item["high_impact"])), utc_iso(),
                ),
            )
            return cursor.rowcount == 1

    def latest_news(self, limit: int = 50) -> list[dict[str, Any]]:
        with self.connect() as db:
            rows = db.execute(
                """SELECT id, source, title, description, url, published_at, sentiment,
                          cause_en, cause_bn, impact_en, impact_bn, high_impact
                   FROM news_items ORDER BY published_at DESC, fetched_at DESC LIMIT ?""",
                (limit,),
            ).fetchall()
        result = [dict(row) for row in rows]
        for item in result:
            item["high_impact"] = bool(item["high_impact"])
        return result

    def record_market_quote(
        self,
        instrument_key: str,
        price: float,
        threshold_percent: float,
        *,
        observed_at: str,
        cooldown_seconds: int = 300,
    ) -> float | None:
        if price <= 0:
            return None
        from datetime import datetime, timedelta

        cutoff = utc_iso(
            datetime.fromisoformat(observed_at) - timedelta(seconds=cooldown_seconds)
        )
        with self.connect() as db:
            previous = db.execute(
                "SELECT price FROM market_quote_state WHERE instrument_key=?",
                (instrument_key,),
            ).fetchone()
            db.execute(
                """INSERT INTO market_quote_state(instrument_key, price, updated_at)
                   VALUES (?, ?, ?)
                   ON CONFLICT(instrument_key) DO UPDATE SET
                     price=excluded.price, updated_at=excluded.updated_at""",
                (instrument_key, price, observed_at),
            )
            if previous is None or previous["price"] <= 0:
                return None
            percent_change = (price - float(previous["price"])) / float(previous["price"]) * 100
            if abs(percent_change) < threshold_percent:
                return None
            prior_alert = db.execute(
                "SELECT alerted_at FROM market_alert_state WHERE instrument_key=?",
                (instrument_key,),
            ).fetchone()
            if prior_alert is not None and prior_alert["alerted_at"] > cutoff:
                return None
            db.execute(
                """INSERT INTO market_alert_state(instrument_key, alerted_at)
                   VALUES (?, ?)
                   ON CONFLICT(instrument_key) DO UPDATE SET alerted_at=excluded.alerted_at""",
                (instrument_key, observed_at),
            )
            return percent_change

    def invalidate_account_token(self, user_id: str) -> None:
        with self.connect() as db:
            db.execute(
                "UPDATE accounts SET token_valid=0, updated_at=? WHERE upstox_user_id=?",
                (utc_iso(), user_id),
            )

    def user_for_session(self, token_hash: str) -> str | None:
        now = utc_iso()
        with self.connect() as db:
            row = db.execute(
                "SELECT upstox_user_id FROM sessions WHERE token_hash=? AND expires_at>?",
                (token_hash, now),
            ).fetchone()
        return str(row["upstox_user_id"]) if row else None

    def get_access_token(self, user_id: str) -> str | None:
        with self.connect() as db:
            row = db.execute(
                "SELECT access_token FROM accounts WHERE upstox_user_id=?", (user_id,)
            ).fetchone()
        return self._decrypt_token(str(row["access_token"])) if row else None

    def save_market_cache(
        self,
        user_id: str,
        kind: str,
        instrument_key: str,
        raw: dict[str, Any],
        *,
        observed_at: str,
        expiry: str = "",
        symbol: str = "",
    ) -> None:
        with self.connect() as db:
            db.execute(
                """INSERT INTO market_cache(
                       upstox_user_id, kind, instrument_key, expiry, symbol, observed_at, raw_json
                   ) VALUES (?, ?, ?, ?, ?, ?, ?)
                   ON CONFLICT(upstox_user_id, kind, instrument_key, expiry, symbol)
                   DO UPDATE SET observed_at=excluded.observed_at, raw_json=excluded.raw_json""",
                (
                    user_id,
                    kind,
                    instrument_key,
                    expiry,
                    symbol,
                    observed_at,
                    json.dumps(raw, separators=(",", ":"), ensure_ascii=False),
                ),
            )

    def get_market_cache(
        self,
        user_id: str,
        kind: str,
        instrument_key: str,
        *,
        expiry: str = "",
        symbol: str = "",
        newer_than: str | None = None,
    ) -> dict[str, Any] | None:
        sql = """SELECT observed_at, raw_json FROM market_cache
                 WHERE upstox_user_id=? AND kind=? AND instrument_key=?
                   AND expiry=? AND symbol=?"""
        args: list[Any] = [user_id, kind, instrument_key, expiry, symbol]
        if newer_than is not None:
            sql += " AND observed_at>=?"
            args.append(newer_than)
        with self.connect() as db:
            row = db.execute(sql, args).fetchone()
        if row is None:
            return None
        return {"observed_at": row["observed_at"], "raw": json.loads(row["raw_json"])}

    def replace_watches(
        self,
        user_id: str,
        watches: list[dict[str, str]],
        lease_until: str,
        session_hash: str = "",
    ) -> list[dict[str, Any]]:
        with self.connect() as db:
            db.execute(
                """UPDATE watches SET active=0, lease_until=?
                   WHERE upstox_user_id=? AND session_hash=?""",
                (utc_iso(), user_id, session_hash),
            )
            for watch in watches:
                db.execute(
                    """INSERT INTO watches(
                           upstox_user_id, kind, instrument_key, expiry, symbol, session_hash, active, lease_until
                       ) VALUES (?, ?, ?, ?, ?, ?, 1, ?)
                       ON CONFLICT(session_hash, kind, instrument_key, expiry, symbol)
                       DO UPDATE SET active=1, lease_until=excluded.lease_until""",
                    (
                        user_id,
                        watch["kind"],
                        watch["instrument_key"],
                        watch.get("expiry", ""),
                        watch.get("symbol", ""),
                        session_hash,
                        lease_until,
                    ),
                )
            rows = db.execute(
                """SELECT * FROM watches
                   WHERE upstox_user_id=? AND session_hash=? AND active=1 AND lease_until=?
                   ORDER BY id""",
                (user_id, session_hash, lease_until),
            ).fetchall()
        result = [dict(row) for row in rows]
        for row in result:
            row.pop("session_hash", None)
        return result

    def active_watches(self, now: str | None = None) -> list[dict[str, Any]]:
        with self.connect() as db:
            rows = db.execute(
                """SELECT w.*, a.access_token FROM watches w
                   JOIN accounts a USING(upstox_user_id)
                   JOIN sessions s ON s.token_hash=w.session_hash
                   WHERE a.token_valid=1 AND s.expires_at>? AND w.active=1 AND w.id=(
                     SELECT MIN(w2.id) FROM watches w2
                     WHERE w2.upstox_user_id=w.upstox_user_id AND w2.kind=w.kind
                       AND w2.instrument_key=w.instrument_key AND w2.expiry=w.expiry
                       AND w2.symbol=w.symbol AND w2.active=1 AND EXISTS(
                         SELECT 1 FROM sessions s2
                         WHERE s2.token_hash=w2.session_hash AND s2.expires_at>?
                       )
                   ) ORDER BY w.id""",
                (now or utc_iso(), now or utc_iso()),
            ).fetchall()
        result = [dict(row) for row in rows]
        for row in result:
            row["access_token"] = self._decrypt_token(row["access_token"])
        return result

    def _encrypt_token(self, token: str) -> str:
        return "enc:v1:" + self._cipher.encrypt(token.encode("utf-8")).decode("ascii")

    def _decrypt_token(self, stored: str) -> str:
        if not stored.startswith("enc:v1:"):
            raise RuntimeError("Unencrypted Upstox token found after database migration")
        try:
            return self._cipher.decrypt(stored[len("enc:v1:") :].encode("ascii")).decode(
                "utf-8"
            )
        except (InvalidToken, UnicodeEncodeError, UnicodeDecodeError) as exc:
            raise RuntimeError(
                "Unable to decrypt stored Upstox token; check the encryption key"
            ) from exc

    def owns_active_watch(
        self,
        user_id: str,
        kind: str,
        instrument_key: str,
        expiry: str = "",
        symbol: str | None = None,
    ) -> bool:
        where = """w.upstox_user_id=? AND w.kind=? AND w.instrument_key=?
                   AND w.expiry=? AND w.active=1 AND s.expires_at>? AND a.token_valid=1"""
        args: list[Any] = [user_id, kind, instrument_key, expiry, utc_iso()]
        if symbol is not None:
            where += " AND symbol=?"
            args.append(symbol)
        with self.connect() as db:
            row = db.execute(
                f"""SELECT 1 FROM watches w
                    JOIN sessions s ON s.token_hash=w.session_hash
                    JOIN accounts a ON a.upstox_user_id=w.upstox_user_id
                    WHERE {where} LIMIT 1""",
                args,
            ).fetchone()
        return row is not None

    def insert_tick(
        self,
        watch: dict[str, Any],
        *,
        observed_at: str,
        instrument_key: str,
        tick_kind: str,
        symbol: str,
        oi: Any,
        oi_change: Any = None,
        ltp: Any,
        volume: Any,
        raw: dict[str, Any],
    ) -> int:
        with self.connect() as db:
            cursor = db.execute(
                """INSERT INTO ticks(
                   watch_id, upstox_user_id, observed_at, instrument_key, tick_kind,
                   symbol, oi, oi_change, ltp, volume, raw_json
                   ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    watch["id"],
                    watch["upstox_user_id"],
                    observed_at,
                    instrument_key,
                    tick_kind,
                    symbol,
                    _number(oi),
                    _number(oi_change),
                    _number(ltp),
                    _number(volume),
                    json.dumps(raw, separators=(",", ":"), ensure_ascii=False),
                ),
            )
            return int(cursor.lastrowid)

    def history(
        self,
        user_id: str,
        *,
        watch_id: int | None,
        instrument_key: str | None,
        expiry: str | None = None,
        start: str,
        end: str,
        limit: int,
    ) -> list[dict[str, Any]]:
        where = ["upstox_user_id=?", "observed_at>=?", "observed_at<?"]
        args: list[Any] = [user_id, start, end]
        if watch_id is not None:
            where.append("watch_id=?")
            args.append(watch_id)
        if instrument_key is not None:
            where.append("instrument_key=?")
            args.append(instrument_key)
        if expiry is not None:
            where.append(
                "watch_id IN (SELECT id FROM watches WHERE upstox_user_id=? AND expiry=?)"
            )
            args.extend((user_id, expiry))
        args.append(limit)
        with self.connect() as db:
            rows = db.execute(
                f"""SELECT id, watch_id, observed_at, instrument_key, tick_kind,
                           symbol, oi, oi_change, ltp, volume, raw_json
                    FROM ticks WHERE {' AND '.join(where)}
                    ORDER BY observed_at DESC, id DESC LIMIT ?""",
                args,
            ).fetchall()
        result = []
        for row in rows:
            item = dict(row)
            item["raw"] = json.loads(item.pop("raw_json"))
            result.append(item)
        return result

    def prune_before(self, timestamp: str) -> int:
        with self.connect() as db:
            cursor = db.execute("DELETE FROM ticks WHERE observed_at<?", (timestamp,))
            db.execute("DELETE FROM sessions WHERE expires_at<?", (utc_iso(),))
            return cursor.rowcount


def _number(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    try:
        return float(value) if value is not None else None
    except (TypeError, ValueError):
        return None
