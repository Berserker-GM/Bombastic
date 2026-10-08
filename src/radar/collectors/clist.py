"""CLIST contest collector. Live HTTP runs only from ``__main__``."""

import asyncio
import json
import logging
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from typing import Any
from urllib.parse import quote, quote_plus

import httpx
from pydantic import ValidationError

from radar.collectors.base import Collector, CollectResult, CollectStatus, RawEvent
from radar.collectors.platforms import platform_slug
from radar.config import get_settings
from radar.models.enums import EventCategory, Lifecycle

CONTEST_URL = "https://clist.by/api/v4/contest/"
_SOURCE_NAME = "clist"
_TIMEOUT_SECONDS = 30.0
_PAGE_LIMIT = 100
_MAX_PAGES = 20
_MAX_RETRIES = 3
_MIN_PAGE_INTERVAL_SECONDS = 6.5
_HOSTS = frozenset(
    {
        "codechef.com",
        "atcoder.jp",
        "leetcode.com",
        "hackerrank.com",
        "hackerearth.com",
    }
)
SleepFn = Callable[[float], Awaitable[None]]


class ClistAuthError(Exception):
    """CLIST rejected the API credentials."""


class ClistRateLimitError(Exception):
    """CLIST kept returning HTTP 429 after the retries."""


class _Redactor:
    def __init__(self) -> None:
        self.username = ""
        self.api_key = ""

    def apply(self, text: str) -> str:
        return redact_secrets(text, username=self.username, api_key=self.api_key)


_redactor = _Redactor()


class _RedactingFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        if isinstance(record.msg, str):
            record.msg = _redactor.apply(record.msg)
        record.args = _redact_log_args(record.args)
        if record.exc_text:
            record.exc_text = _redactor.apply(record.exc_text)
        return True


def redact_secrets(text: str, *, username: str, api_key: str) -> str:
    secrets = [secret for secret in (api_key, username) if secret]
    secrets.sort(key=len, reverse=True)
    redacted = text
    for secret in secrets:
        variants = sorted(_secret_variants(secret), key=len, reverse=True)
        for variant in variants:
            redacted = redacted.replace(variant, "REDACTED")
    return redacted


def _secret_variants(secret: str) -> set[str]:
    encoded = {secret, quote(secret, safe=""), quote_plus(secret)}
    return {variant for variant in encoded if variant} | {
        variant.lower() for variant in encoded if variant
    }


def _redact_log_args(args: Any) -> Any:
    if isinstance(args, tuple):
        return tuple(_redact_log_value(arg) for arg in args)
    if isinstance(args, dict):
        return {key: _redact_log_value(value) for key, value in args.items()}
    return args


def _redact_log_value(value: Any) -> Any:
    if isinstance(value, str):
        return _redactor.apply(value)
    rendered = str(value)
    redacted = _redactor.apply(rendered)
    if redacted == rendered:
        return value
    return redacted


def _configure_http_logging() -> None:
    for name in ("httpx", "httpcore"):
        logger = logging.getLogger(name)
        logger.setLevel(logging.WARNING)
        if not any(isinstance(item, _RedactingFilter) for item in logger.filters):
            logger.addFilter(_RedactingFilter())


_configure_http_logging()


def _arm_redaction() -> tuple[str, str]:
    _configure_http_logging()
    settings = get_settings()
    username = settings.clist_username or ""
    api_key = settings.clist_api_key or ""
    _redactor.username = username
    _redactor.api_key = api_key
    return username, api_key


def _redact_exception(exc: Exception) -> str:
    message = _redactor.apply(str(exc))
    exc.args = (message,)
    request = getattr(exc, "request", None)
    url = getattr(request, "url", None)
    if request is not None and url is not None:
        request.url = httpx.URL(_redactor.apply(str(url)))
    return message


def _failure(status: CollectStatus, exc: Exception) -> CollectResult:
    return CollectResult(status=status, events=[], error=_redact_exception(exc))


class ClistCollector(Collector):
    min_expected = 1

    def __init__(
        self,
        *,
        sleep: SleepFn | None = None,
        page_limit: int = _PAGE_LIMIT,
    ) -> None:
        self._sleep = asyncio.sleep if sleep is None else sleep
        self._page_limit = page_limit

    async def collect(self) -> CollectResult:
        _arm_redaction()
        try:
            events = await self.fetch()
        except ClistAuthError:
            return _failure("unreachable", ClistAuthError("CLIST auth rejected"))
        except ClistRateLimitError:
            return _failure("unreachable", ClistRateLimitError("CLIST rate limit"))
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code in (401, 403):
                return _failure("unreachable", ClistAuthError("CLIST auth rejected"))
            if exc.response.status_code == 429:
                return _failure("unreachable", ClistRateLimitError("CLIST rate limit"))
            status: CollectStatus = (
                "unreachable" if exc.response.status_code >= 500 else "malformed"
            )
            return _failure(status, exc)
        except httpx.TransportError as exc:
            return _failure("unreachable", exc)
        except (
            ValidationError,
            KeyError,
            TypeError,
            ValueError,
            json.JSONDecodeError,
        ) as exc:
            return _failure("malformed", exc)
        return self._from_events(events)

    async def fetch(self) -> list[RawEvent]:
        username, api_key = _arm_redaction()
        if not username or not api_key:
            raise TypeError("CLIST_API_KEY or CLIST_USERNAME is not set")
        now = datetime.now(UTC)
        contests = await self._fetch_pages(
            username=username,
            api_key=api_key,
            now=now,
        )
        return events_from_contests(contests, now=now)

    async def _fetch_pages(
        self,
        *,
        username: str,
        api_key: str,
        now: datetime,
    ) -> list[Any]:
        # meta.next embeds the API key. Page with our own offset instead.
        contests: list[Any] = []
        offset = 0
        async with httpx.AsyncClient(timeout=_TIMEOUT_SECONDS) as client:
            for page in range(_MAX_PAGES):
                if page:
                    await self._sleep(_MIN_PAGE_INTERVAL_SECONDS)
                payload = await self._get_page(
                    client,
                    username=username,
                    api_key=api_key,
                    now=now,
                    offset=offset,
                )
                page_objects = _objects(payload)
                contests.extend(page_objects)
                if len(page_objects) < self._page_limit:
                    break
                offset += self._page_limit
        return contests

    async def _get_page(
        self,
        client: httpx.AsyncClient,
        *,
        username: str,
        api_key: str,
        now: datetime,
        offset: int,
    ) -> Any:
        params = {
            "format": "json",
            "username": username,
            "api_key": api_key,
            "upcoming": "true",
            "start__gt": now.strftime("%Y-%m-%dT%H:%M:%S"),
            "limit": str(self._page_limit),
            "offset": str(offset),
            "order_by": "start",
        }
        attempt = 0
        while True:
            response = await client.get(CONTEST_URL, params=params)
            if response.status_code in (401, 403):
                raise ClistAuthError("CLIST auth rejected")
            if response.status_code == 429:
                if attempt >= _MAX_RETRIES:
                    raise ClistRateLimitError("CLIST rate limit")
                attempt += 1
                await self._sleep(_MIN_PAGE_INTERVAL_SECONDS)
                continue
            response.raise_for_status()
            return response.json()


def events_from_contests(contests: list[Any], *, now: datetime) -> list[RawEvent]:
    events: list[RawEvent] = []
    for contest in contests:
        event = _upcoming_event(contest, now=now)
        if event is not None:
            events.append(event)
    return events


def _objects(payload: Any) -> list[Any]:
    if not isinstance(payload, dict):
        raise TypeError("contest page is not an object")
    contests = payload["objects"]
    if not isinstance(contests, list):
        raise TypeError("objects is not a list")
    return contests


def _upcoming_event(contest: Any, *, now: datetime) -> RawEvent | None:
    if not isinstance(contest, dict):
        raise TypeError("contest is not an object")
    host = contest["host"]
    if not isinstance(host, str) or not host.strip():
        raise TypeError("host is not a string")
    if host.casefold() not in _HOSTS:
        return None
    start_at = _parse_utc(contest["start"])
    if start_at <= now:
        return None
    return RawEvent(
        source_name=_SOURCE_NAME,
        title=contest["event"],
        category=EventCategory.CONTEST,
        external_id=str(contest["id"]),
        url=contest["href"],
        start_at=start_at,
        end_at=_parse_utc(contest["end"]),
        lifecycle=Lifecycle.UPCOMING,
        tags=[platform_slug(host)],
    )


def _parse_utc(value: Any) -> datetime:
    if not isinstance(value, str):
        raise TypeError("contest time is not a string")
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def main() -> None:
    result = asyncio.run(ClistCollector().collect())
    print(result.model_dump_json(indent=2))


if __name__ == "__main__":
    main()
