from datetime import date, datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo

IST = ZoneInfo("Asia/Kolkata")
UTC = timezone.utc


def utc_now() -> datetime:
    return datetime.now(UTC)


def utc_iso(value: datetime | None = None) -> str:
    value = value or utc_now()
    return value.astimezone(UTC).isoformat(timespec="seconds")


def ist_session_day(now: datetime | None = None) -> date:
    return (now or utc_now()).astimezone(IST).date()


def is_market_open(now: datetime | None = None) -> bool:
    local = (now or utc_now()).astimezone(IST)
    return (
        local.weekday() < 5
        and time(9, 15) <= local.time().replace(tzinfo=None) < time(15, 30)
    )


def session_day_start_utc(day: date) -> datetime:
    return datetime.combine(day, time.min, tzinfo=IST).astimezone(UTC)


def current_session_day_bounds(now: datetime | None = None) -> tuple[str, str]:
    day = ist_session_day(now)
    start = session_day_start_utc(day)
    end = session_day_start_utc(day + timedelta(days=1))
    return utc_iso(start), utc_iso(end)
