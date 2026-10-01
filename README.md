# Upstox market-data backend

Small FastAPI service for multi-device Upstox access. It stores Upstox access
tokens server-side, hands clients an opaque 30-day session token, and polls
session subscriptions every 15 seconds during the weekday NSE session
(09:15–15:30 Asia/Kolkata). SQLite stores observations without tick/minute deduplication;
history is retained for the current IST session day. Keep the database on a
persistent volume when deploying.

## Run

Python 3.11+ is required. Set the variables in `.env.example` in the process
environment, then run:

```sh
pip install -r requirements.txt
uvicorn app.main:app --host 0.0.0.0 --port 8000
```

Or build from this folder with `docker build -t upstox-market-data .`; pass the
environment variables and mount a persistent directory for `DATABASE_PATH`.
The service never logs authorization data. Use HTTPS at the deployment edge.
Run a single backend replica when using SQLite; multiple replicas would each
start a poll worker and the SQLite database is not intended as shared network
storage.
`UPSTOX_TOKEN_ENCRYPTION_KEY` is required at startup and must be a Fernet key
generated specifically for this deployment. Generate one with:

```sh
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

## API contract

All authenticated endpoints require `Authorization: Bearer <session_token>`.
Flutter's stable routes are under `/api`. The legacy unprefixed routes remain
available for compatibility.

* `GET /api/auth/login-url` returns `{"url":"https://..."}` for the configured
  Upstox OAuth redirect.
* `POST /api/auth/exchange` accepts `{"code":"..."}` and returns
  `{"session_token":"...","token_type":"Bearer","expires_at":"...","upstox_user_id":"..."}`.
  The legacy `/auth/exchange` route also accepts `auth_code`.
* `GET /api/auth/session` validates the bearer and returns
  `{"authenticated":true,"upstox_user_id":"..."}`.
* `POST /api/auth/logout` revokes that session and returns
  `{"logged_out":true}`.
* `GET /api/option-contracts?instrument_key=...`
* `GET /api/option-chain?instrument_key=...&expiry_date=YYYY-MM-DD`
* `GET /api/futures-quote?instrument_key=...&trading_symbol=...`
* `POST /api/watches` accepts `{ "watches": [...] }`, and an optional
  `lease_seconds` (15–300; default 180):

```json
{
  "watches": [
    {"kind":"option_chain","instrument_key":"NSE_INDEX|Nifty 50","expiry_date":"2026-10-29"},
    {"kind":"future","instrument_key":"NSE_FO|12345","trading_symbol":"RELIANCE"}
  ]
}
```

The response contains `lease_until` and `watches` in the same kind/field
conventions (`option_chain`/`expiry_date` or `future`/`trading_symbol`).
`lease_seconds` and `lease_until` report recent heartbeat activity; they do
not expire the watch subscription or control polling. Posting the watches
creates/updates a persistent subscription for that authenticated bearer
session/device. Flutter may heartbeat every 15 seconds while foregrounded, but
the app does not need to heartbeat while suspended: the service continues
polling the subscription during market hours as long as its session remains
valid. Posting an empty list explicitly removes that session's subscriptions.
Each device session has an independent subscription set; changing or removing
one device's watches does not affect another device. Session tokens expire
after 30 days unless revoked sooner with `POST /api/auth/logout`. Logout
revokes that device session and stops its polling. When all sessions are
revoked/expired, polling stops. An Upstox 401/403 marks the stored account
token invalid and stops polling its subscriptions; a fresh OAuth exchange
restores polling eligibility.
Identical account/instrument subscriptions are coalesced so the worker polls
Upstox once per 15-second cycle, regardless of the number of devices.
* `GET /api/history?instrument_key=...&expiry_date=...[&contract_key=...]`
  returns `{"data":[...]}`; `expiry_date` is optional and `contract_key`
  filters to an individual option contract. Each record is
  `{"contract_key":"...","timestamp":"...","oi":0,"oi_change":0,"ltp":0,"volume":0}`.
  Timestamps are UTC ISO-8601 strings. Every polling observation is persisted
  without minute deduplication.
* `GET /api/ltp?instrument_key=...` returns `{"last_price":0}`.

Option-chain and option-contract responses use `{"data":[...]}`; futures quote
responses use `{"data":{"<instrument_key>":{...}}}`. The worker polls active
session subscriptions every 15 seconds and saves both ticks and the full latest
upstream snapshot. Render endpoints serve that cached snapshot instead of
making per-device Upstox calls. If a cache is missing or has not refreshed for
two poll intervals (at least 30 seconds), one request refreshes it; concurrent
requests in the same service process share that refresh. Option-contract
metadata is cached for up to one hour. A live market-data request requires an
active subscription for the corresponding instrument.

The OAuth exchange is server-side: the server reads `/user/profile` for the
account's `user_id` and stores its Upstox access token encrypted with
`UPSTOX_TOKEN_ENCRYPTION_KEY` before writing it to SQLite. App session tokens
remain opaque and hash-only in the database; they are never stored in
recoverable form.

Back up the encryption key separately from SQLite in a secrets manager or
other protected key store. A database backup without its matching key cannot
be used to decrypt Upstox tokens. Keep a recoverable copy of the old key for
all backups encrypted with it. Replacing the key without decrypting and
re-encrypting existing rows makes those tokens unreadable; planned rotation
requires an offline migration that decrypts with the old key and writes with
the new key, followed by secure retirement of the old key. Existing databases
with plaintext Upstox token rows are encrypted in place during initialization.
Keep legacy database backups protected as plaintext until they have been
replaced with encrypted backups; migration only updates the active database.

For previous unprefixed app endpoint details, `POST /auth/exchange` accepts:

```json
{"auth_code":"<authorization code returned by Upstox>"}
```

Legacy routes include:

* `POST /heartbeat` – accepts legacy `option_chain`/`stock_future` watch specs.
* `GET /option-chain?instrument_key=...&expiry=YYYY-MM-DD` – live chain for an
  active option watch.
* `GET /futures/quote?instrument_key=...` – live quote for an active future.
* `GET /option-contracts?instrument_key=...[&expiry=YYYY-MM-DD]`
* `GET /option-expiries?instrument_key=...`
* `GET /history?watch_id=...` or `?instrument_key=...[&limit=1000]` – current
  IST session-day observations, newest first. Option-chain observations are
  stored separately for call and put contracts.
* `GET /health`

Successful upstream proxy routes return Upstox's JSON response unchanged.
Errors are JSON `{"detail":{"code":"..."}}`; missing/invalid sessions return
401, invalid input 422, and Upstox/network failures return 502/504.

For production, use a high-entropy `SESSION_SECRET`, restrict network access,
and configure a persistent writable database volume. API tokens and auth
codes are accepted only over HTTPS and are never included in API error detail.
The API allows browser origins on localhost/127.0.0.1 (any port) and the
Android-emulator host address for local Flutter Web development. For a deployed
Flutter Web origin, add its exact origin (scheme and host, without a path) to
the comma-separated `CORS_ORIGINS` environment setting. Other origins are not
allowed by default.

## RSS news

The backend refreshes configured Moneycontrol and Investing.com RSS/Atom feeds
every 60 seconds independently of Flutter clients, stores/deduplicates items in
SQLite, and exposes the latest authenticated feed at `GET /api/news?limit=50`
(limit 1â€“100; response is `{"data":[...]}`). Each item includes source, title,
link, publication time, a lightweight Bullish/Bearish/Neutral label, brief
English and Bengali cause/impact descriptions, and a `high_impact` flag.
`NEWS_FEEDS` accepts a JSON array of `{ "name": "...", "url": "..." }` entries
or a comma-separated `name|url` list. Requests use bounded network timeouts;
feed parsing rejects oversized documents and DTD/entity declarations.

To enable high-impact push notifications, configure
`GOOGLE_APPLICATION_CREDENTIALS` with the path to a Firebase service-account
file. Without it (or when Firebase Admin cannot initialize), aggregation and
the news API remain available; the backend explicitly logs that push delivery
is disabled and does not report successful delivery. `GET /api/news` returns
`{"data":[{"id":"...","title":"...","summary":"...","source":"...","url":"...","published_at":"...","impact":"bullish|bearish|neutral","reason":"...","reason_bn":"...","impact_bn":"...","high_impact":false}]}`.
Authenticated devices register at `POST /api/devices/fcm-token` with
`{"token":"...","client_id":"..."}` and unregister at
`DELETE /api/devices/fcm-token?client_id=...`. A stable client ID scopes the
token to its bearer session; logout revokes the session's registrations.
High-impact push notifications include a visible title/body, Android high
priority, the `market_news_alerts` Android notification channel ID, and default
system notification sound. The Flutter app should create that channel with
sound enabled for background FCM notifications; foreground messages can be
presented through `flutter_local_notifications`.

Market fluctuation alerts are enabled with the optional
`MARKET_ALERT_CHANGE_PERCENT` setting (default `1.0`). The market worker compares
each persisted futures quote and index spot quote with its preceding persisted
quote; when the absolute change reaches the threshold, it sends a visible,
high-priority notification. Alerts are deduplicated per instrument for five
minutes, including across backend restarts.
