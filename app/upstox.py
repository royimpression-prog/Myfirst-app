from typing import Any

import httpx


API_BASE = "https://api.upstox.com/v2"


class UpstoxError(Exception):
    def __init__(self, code: str, status_code: int = 502, *, token_invalid: bool = False):
        self.code = code
        self.status_code = status_code
        self.token_invalid = token_invalid
        super().__init__(code)


class UpstoxClient:
    def __init__(
        self,
        client_id: str,
        client_secret: str,
        redirect_uri: str,
        timeout: float = 10.0,
    ):
        self.client_id = client_id
        self.client_secret = client_secret
        self.redirect_uri = redirect_uri
        self.timeout = timeout

    async def exchange_code(self, code: str) -> tuple[str, str]:
        if not (self.client_id and self.client_secret and self.redirect_uri):
            raise UpstoxError("oauth_not_configured", 503)
        try:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                response = await client.post(
                    f"{API_BASE}/login/authorization/token",
                    data={
                        "code": code,
                        "client_id": self.client_id,
                        "client_secret": self.client_secret,
                        "redirect_uri": self.redirect_uri,
                        "grant_type": "authorization_code",
                    },
                    headers={"Accept": "application/json"},
                )
                response.raise_for_status()
                token_data = response.json()
                access_token = token_data.get("access_token")
                if not isinstance(access_token, str) or not access_token:
                    raise UpstoxError("oauth_invalid_response")
                profile_response = await client.get(
                    f"{API_BASE}/user/profile",
                    headers={"Authorization": f"Bearer {access_token}", "Accept": "application/json"},
                )
                profile_response.raise_for_status()
                profile = profile_response.json().get("data", {})
                user_id = profile.get("user_id")
                if not isinstance(user_id, str) or not user_id.strip():
                    raise UpstoxError("profile_missing_user_id")
                return access_token, user_id.strip()
        except UpstoxError:
            raise
        except httpx.TimeoutException as exc:
            raise UpstoxError("upstox_timeout", 504) from exc
        except (httpx.HTTPError, ValueError, TypeError) as exc:
            raise UpstoxError("upstox_oauth_failed") from exc

    async def fetch(self, access_token: str, path: str, params: dict[str, str]) -> dict[str, Any]:
        try:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                response = await client.get(
                    f"{API_BASE}{path}",
                    params=params,
                    headers={"Authorization": f"Bearer {access_token}", "Accept": "application/json"},
                )
                response.raise_for_status()
                data = response.json()
                if not isinstance(data, dict):
                    raise UpstoxError("upstox_invalid_response")
                return data
        except UpstoxError:
            raise
        except httpx.TimeoutException as exc:
            raise UpstoxError("upstox_timeout", 504) from exc
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code in (401, 403):
                raise UpstoxError("upstox_token_invalid", 502, token_invalid=True) from exc
            raise UpstoxError("upstox_request_failed") from exc
        except (httpx.HTTPError, ValueError, TypeError) as exc:
            raise UpstoxError("upstox_request_failed") from exc

    async def option_chain(self, token: str, instrument_key: str, expiry: str) -> dict[str, Any]:
        return await self.fetch(
            token,
            "/option/chain",
            {"instrument_key": instrument_key, "expiry_date": expiry},
        )

    async def future_quote(self, token: str, instrument_key: str) -> dict[str, Any]:
        return await self.fetch(
            token, "/market-quote/quotes", {"instrument_key": instrument_key}
        )

    async def option_contracts(
        self, token: str, instrument_key: str, expiry: str | None = None
    ) -> dict[str, Any]:
        params = {"instrument_key": instrument_key}
        if expiry:
            params["expiry_date"] = expiry
        return await self.fetch(token, "/option/contract", params)

    async def option_expiries(self, token: str, instrument_key: str) -> dict[str, Any]:
        return await self.fetch(
            token, "/option/expiry", {"instrument_key": instrument_key}
        )
