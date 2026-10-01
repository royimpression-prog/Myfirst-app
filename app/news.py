import asyncio
import hashlib
import html
import json
import logging
import os
import re
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Any
from urllib.parse import urlparse

import httpx

logger = logging.getLogger(__name__)

DEFAULT_FEEDS = [
    {"name": "Moneycontrol", "url": "https://www.moneycontrol.com/rss/marketreports.xml"},
    {"name": "Investing.com", "url": "https://www.investing.com/rss/news_25.rss"},
]
MAX_FEEDS = 20
MAX_FEED_BYTES = 2_000_000
MAX_ITEMS_PER_FEED = 500

_BULLISH = {
    "gain", "gains", "rise", "rises", "rally", "surge", "surges", "soar", "high",
    "record", "bull", "bullish", "positive", "profit", "profits", "growth", "beat",
    "upgrade", "buy", "inflow", "strong", "উত্থান", "বৃদ্ধি", "লাভ", "ঊর্ধ্বগতি",
    "বুলিশ", "ইতিবাচক", "রেকর্ড", "শক্তিশালী",
}
_BEARISH = {
    "fall", "falls", "drop", "drops", "decline", "declines", "crash", "plunge",
    "low", "bear", "bearish", "negative", "loss", "losses", "cut", "downgrade",
    "sell", "outflow", "weak", "slump", "সংশোধন", "পতন", "ক্ষতি", "হ্রাস",
    "বিয়ারিশ", "নেতিবাচক", "দুর্বল", "ধস",
}
_IMPACT = {
    "crash", "plunge", "surge", "record", "emergency", "crisis", "shock", "ban",
    "default", "war", "merger", "acquisition", "rate decision", "rate hike",
    "rate cut", "fraud", "bankruptcy", "ধস", "রেকর্ড", "জরুরি", "সংকট", "যুদ্ধ",
    "একীভূত", "সুদহার", "দেউলিয়া",
}
_CAUSES = (
    (("earnings", "profit", "results", "revenue", "লাভ", "মুনাফা"), "earnings", "আয় ও মুনাফার খবর"),
    (("rate", "reserve bank", "fed", "interest", "সুদহার", "রিজার্ভ ব্যাংক"), "interest rates", "সুদের হারের পরিবর্তন"),
    (("inflation", "price rise", "মুদ্রাস্ফীতি"), "inflation", "মুদ্রাস্ফীতির খবর"),
    (("oil", "crude", "energy", "তেল"), "energy prices", "জ্বালানির দামের পরিবর্তন"),
    (("war", "geopolitical", "conflict", "যুদ্ধ"), "geopolitical developments", "ভূরাজনৈতিক পরিস্থিতি"),
    (("budget", "policy", "tax", "নীতি", "বাজেট"), "policy or budget news", "নীতি বা বাজেটের খবর"),
)


def configured_feeds(raw: str | None = None) -> list[dict[str, str]]:
    value = os.getenv("NEWS_FEEDS") if raw is None else raw
    if not value:
        feeds = DEFAULT_FEEDS
    else:
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            parsed = [
                dict(zip(("name", "url"), part.split("|", 1)))
                for part in value.split(",") if "|" in part
            ]
        if not isinstance(parsed, list):
            raise ValueError("NEWS_FEEDS must be a JSON array or name|url list")
        feeds = parsed
    if len(feeds) > MAX_FEEDS:
        raise ValueError(f"NEWS_FEEDS may contain at most {MAX_FEEDS} feeds")
    result = []
    for item in feeds:
        if not isinstance(item, dict) or not isinstance(item.get("name"), str) or not isinstance(item.get("url"), str):
            raise ValueError("each NEWS_FEEDS item requires a name and url")
        name, url = item["name"].strip(), item["url"].strip()
        parsed_url = urlparse(url)
        if not name or len(name) > 80 or len(url) > 2048 or parsed_url.scheme not in ("http", "https") or not parsed_url.netloc:
            raise ValueError("invalid news feed name or URL")
        result.append({"name": name, "url": url})
    return result


def _local_name(tag: str) -> str:
    return tag.rsplit("}", 1)[-1].lower()


def _clean_text(value: str) -> str:
    value = html.unescape(value)
    value = re.sub(r"<[^>]*>", " ", value)
    return re.sub(r"\s+", " ", html.unescape(value)).strip()


def _child_text(element: ET.Element, *names: str) -> str:
    names = tuple(name.lower() for name in names)
    for child in element.iter():
        if child is element:
            continue
        if _local_name(child.tag) in names:
            if child.text:
                return _clean_text(child.text)
            # Atom content can contain nested XHTML.
            return _clean_text(
                " ".join(part.strip() for part in child.itertext() if part.strip())
            )
    return ""


def _entry_link(element: ET.Element) -> str:
    for child in element:
        if _local_name(child.tag) == "link":
            href = child.attrib.get("href")
            if href:
                return href.strip()
            if child.text:
                return child.text.strip()
    return ""


def _published(value: str) -> str:
    if value:
        try:
            parsed = parsedate_to_datetime(value)
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            return parsed.astimezone(timezone.utc).isoformat()
        except (TypeError, ValueError, OverflowError):
            try:
                parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
                if parsed.tzinfo is None:
                    parsed = parsed.replace(tzinfo=timezone.utc)
                return parsed.astimezone(timezone.utc).isoformat()
            except ValueError:
                pass
    return datetime.now(timezone.utc).isoformat()


def parse_feed(xml_data: bytes | str, source: str) -> list[dict[str, Any]]:
    raw = xml_data.encode("utf-8") if isinstance(xml_data, str) else xml_data
    if (
        len(raw) > MAX_FEED_BYTES
        or b"\x00" in raw
        or re.search(br"<!\s*(DOCTYPE|ENTITY)", raw, re.I)
    ):
        raise ValueError("unsafe or oversized feed document")
    try:
        root = ET.fromstring(raw)
    except ET.ParseError as exc:
        raise ValueError("invalid feed XML") from exc
    records = []
    for element in root.iter():
        if _local_name(element.tag) not in ("item", "entry"):
            continue
        title = _child_text(element, "title")
        if not title:
            continue
        description = _child_text(element, "description", "summary", "content", "encoded")
        link = _entry_link(element) or _child_text(element, "link")
        guid = _child_text(element, "guid", "id")
        published_raw = _child_text(element, "pubdate", "published", "updated", "date")
        published = _published(published_raw)
        identity = link or guid or f"{source}\x1f{title}\x1f{published_raw.strip()}"
        item_id = hashlib.sha256(identity.encode("utf-8")).hexdigest()
        sentiment = classify(title, description)
        records.append({
            "id": item_id,
            "source": source,
            "title": title[:500],
            "description": description[:4000],
            "url": link[:2048],
            "published_at": published,
            **sentiment,
        })
        if len(records) >= MAX_ITEMS_PER_FEED:
            break
    return records


def classify(title: str, description: str = "") -> dict[str, Any]:
    text = f"{title} {description}".lower()
    words = set(re.findall(r"[a-z]+|[\u0980-\u09ff]+", text))
    bullish = sum(1 for word in _BULLISH if word in words or word in text)
    bearish = sum(1 for word in _BEARISH if word in words or word in text)
    sentiment = "Bullish" if bullish > bearish else "Bearish" if bearish > bullish else "Neutral"
    cause_en, cause_bn = "market or company news", "বাজার বা প্রতিষ্ঠানের খবর"
    for terms, english, bengali in _CAUSES:
        if any(term in text for term in terms):
            cause_en, cause_bn = english, bengali
            break
    direction_en = {
        "Bullish": "May support prices and improve investor sentiment.",
        "Bearish": "May pressure prices and weaken investor sentiment.",
        "Neutral": "Likely to have a limited or mixed market effect.",
    }[sentiment]
    direction_bn = {
        "Bullish": "দাম ও বিনিয়োগকারীদের মনোভাবকে সহায়তা করতে পারে।",
        "Bearish": "দামের ওপর চাপ ও বিনিয়োগকারীদের মনোভাবে দুর্বলতা আনতে পারে।",
        "Neutral": "বাজারে প্রভাব সীমিত বা মিশ্র হতে পারে।",
    }[sentiment]
    return {
        "sentiment": sentiment,
        "cause_en": f"Cause: {cause_en}.",
        "cause_bn": f"কারণ: {cause_bn}।",
        "impact_en": direction_en,
        "impact_bn": direction_bn,
        "high_impact": any(term in text for term in _IMPACT),
    }


async def fetch_feed(client: httpx.AsyncClient, feed: dict[str, str]) -> list[dict[str, Any]]:
    chunks = bytearray()
    async with client.stream(
        "GET", feed["url"], headers={"User-Agent": "MarketNewsAggregator/1.0"}
    ) as response:
        response.raise_for_status()
        async for chunk in response.aiter_bytes():
            if len(chunks) + len(chunk) > MAX_FEED_BYTES:
                raise ValueError("feed response exceeds size limit")
            chunks.extend(chunk)
    return parse_feed(bytes(chunks), feed["name"])


class FirebaseSender:
    def __init__(self, credentials_path: str | None = None):
        self._messaging = None
        path = credentials_path or os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "")
        if not path:
            logger.warning("FCM is disabled: GOOGLE_APPLICATION_CREDENTIALS is not configured")
            return
        try:
            import firebase_admin
            from firebase_admin import credentials, messaging

            try:
                firebase_admin.get_app()
            except ValueError:
                firebase_admin.initialize_app(credentials.Certificate(path))
            self._messaging = messaging
        except Exception as exc:
            logger.warning("FCM is disabled: Firebase Admin initialization failed (%s)", type(exc).__name__)

    @property
    def enabled(self) -> bool:
        return self._messaging is not None

    async def send(self, token: str, item: dict[str, Any]) -> bool:
        if self._messaging is None:
            return False

        def _send() -> None:
            data = item.get("data") or {
                "type": "news",
                "news_id": item["id"],
                "sentiment": item.get("sentiment", ""),
            }
            self._messaging.send(self._messaging.Message(
                notification=self._messaging.Notification(
                    title=item["title"][:200],
                    body=item.get("body", item.get("impact_en", ""))[:500],
                ),
                data={key: str(value) for key, value in data.items()},
                android=self._messaging.AndroidConfig(
                    priority="high",
                    notification=self._messaging.AndroidNotification(
                        channel_id="market_news_alerts",
                        sound="default",
                    ),
                ),
                apns=self._messaging.APNSConfig(
                    headers={"apns-priority": "10"},
                    payload=self._messaging.APNSPayload(
                        aps=self._messaging.Aps(sound="default")
                    ),
                ),
                token=token,
            ))

        try:
            await asyncio.to_thread(_send)
            return True
        except Exception as exc:
            logger.warning("FCM delivery failed (%s)", type(exc).__name__)
            return False


async def news_worker(app: Any) -> None:
    feeds: list[dict[str, str]] = app.state.news_feeds
    timeout = httpx.Timeout(app.state.settings.request_timeout)
    async with httpx.AsyncClient(timeout=timeout, follow_redirects=True) as client:
        while True:
            cycle_started = time.monotonic()
            for feed in feeds:
                try:
                    items = await fetch_feed(client, feed)
                    for item in items:
                        if app.state.db.save_news(item):
                            if item["high_impact"]:
                                await notify_news(app, item)
                except Exception as exc:
                    logger.warning("News feed refresh failed for %s (%s)", feed["name"], type(exc).__name__)
            await asyncio.sleep(max(0, 60 - (time.monotonic() - cycle_started)))


async def notify_news(app: Any, item: dict[str, Any]) -> None:
    sender: FirebaseSender = app.state.fcm_sender
    if not sender.enabled:
        # Keep this explicit even though initialization also warns: a high-impact item
        # arrived, but no push delivery was attempted.
        logger.warning("High-impact news saved; FCM notification skipped because Firebase is not configured")
        return
    for token in app.state.db.device_tokens():
        await sender.send(token, item)
