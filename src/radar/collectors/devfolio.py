"""Devfolio hackathon listing collector. Live HTTP runs only from ``__main__``."""

import asyncio
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from typing import Any

import httpx
from pydantic import ValidationError

from radar.collectors.base import Collector, CollectResult, RawEvent
from radar.models.enums import EventCategory, EventMode, Lifecycle

HACKATHONS_URL = "https://api.devfolio.co/api/hackathons"
_SOURCE_NAME = "devfolio"
_TIMEOUT_SECONDS = 30.0
_MAX_PAGES = 10
_PAGE_INTERVAL_SECONDS = 1.0
_PAGE_CAP_WARNING = "stopped after 10 pages"
_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
)
SleepFn = Callable[[float], Awaitable[None]]


class DevfolioCollector(Collector):
    min_expected = 1

    def __init__(
        self,
        *,
        now: datetime | None = None,
        sleep: SleepFn | None = None,
    ) -> None:
        self._now = now
        self._sleep = asyncio.sleep if sleep is None else sleep
        self._page_cap_hit = False
        self._warnings: list[str] = []
        self._failed_items = 0

    async def fetch(self) -> list[RawEvent]:
        self._page_cap_hit = False
        self._warnings = []
        self._failed_items = 0
        now = self._now if self._now is not None else datetime.now(UTC)
        events: list[RawEvent] = []
        async with httpx.AsyncClient(
            timeout=_TIMEOUT_SECONDS,
            follow_redirects=True,
            headers={"Accept": "application/json", "User-Agent": _USER_AGENT},
        ) as client:
            for page in range(1, _MAX_PAGES + 1):
                if page > 1:
                    await self._sleep(_PAGE_INTERVAL_SECONDS)
                response = await client.get(HACKATHONS_URL, params=_page_params(page))
                response.raise_for_status()
                batch, warnings, count = parse_page(response.json(), now=now)
                self._warnings.extend(warnings)
                self._failed_items += count - len(batch)
                if count == 0:
                    break
                events.extend(batch)
            else:
                self._page_cap_hit = True
        return events

    def _from_events(self, events: list[RawEvent]) -> CollectResult:
        warnings = list(self._warnings)
        if not events and self._failed_items:
            message = "every item failed: " + "; ".join(warnings)
            if self._page_cap_hit:
                message = f"{_PAGE_CAP_WARNING}; {message}"
                warnings = [_PAGE_CAP_WARNING, *warnings]
            return CollectResult(
                status="malformed",
                events=[],
                error=message,
                warnings=warnings,
            )
        result = super()._from_events(events)
        parts: list[str] = []
        if self._page_cap_hit and result.status == "ok":
            parts.append(_PAGE_CAP_WARNING)
            warnings = [_PAGE_CAP_WARNING, *warnings]
        if self._warnings and result.status == "ok":
            parts.append("; ".join(self._warnings))
        if parts or warnings:
            return result.model_copy(
                update={"warning": "; ".join(parts) or None, "warnings": warnings}
            )
        return result


def parse_page(
    payload: dict[str, Any], *, now: datetime
) -> tuple[list[RawEvent], list[str], int]:
    items = _items(payload)
    events: list[RawEvent] = []
    warnings: list[str] = []
    for index, item in enumerate(items):
        try:
            events.append(event_from_item(item, now=now))
        except (KeyError, TypeError, ValidationError) as exc:
            warnings.append(f"{_label(item, index)}: {_problem(exc)}")
    return events, warnings, len(items)


def event_from_item(item: Any, *, now: datetime) -> RawEvent:
    if not isinstance(item, dict):
        raise TypeError("item is not an object")
    start_at = _timestamp(item.get("starts_at"))
    end_at = _timestamp(item.get("ends_at"))
    deadline = _timestamp(_setting(item).get("reg_ends_at"))
    return RawEvent(
        source_name=_SOURCE_NAME,
        title=_required_str(item, "name"),
        category=EventCategory.HACKATHON,
        external_id=_external_id(item),
        url=_url(item),
        start_at=start_at,
        end_at=end_at,
        registration_deadline=deadline,
        location=_location(item),
        mode=_mode(item),
        prize_text=None,
        prize_amount_inr=None,
        eligibility=None,
        tags=_themes(item),
        lifecycle=_lifecycle(deadline, now),
        extra=_extra(item, start_at=start_at, end_at=end_at, deadline=deadline),
    )


def _page_params(page: int) -> dict[str, str]:
    return {"filter": "application_open", "page": str(page), "limit": "20"}


def _items(payload: dict[str, Any]) -> list[Any]:
    if not isinstance(payload, dict):
        raise TypeError("listing is not an object")
    items = payload["result"]
    if not isinstance(items, list):
        raise TypeError("result is not a list")
    return items


def _setting(item: dict[str, Any]) -> dict[str, Any]:
    if "hackathon_setting" not in item or item["hackathon_setting"] is None:
        return {}
    setting = item["hackathon_setting"]
    if not isinstance(setting, dict):
        raise TypeError("hackathon_setting is not an object")
    return setting


def _url(item: dict[str, Any]) -> str:
    slug = _required_str(item, "slug")
    if "/" in slug or any(char.isspace() for char in slug):
        raise TypeError("slug is not a subdomain")
    return f"https://{slug}.devfolio.co/"


def _location(item: dict[str, Any]) -> str | None:
    parts = [
        part
        for part in (_optional_str(item, "city"), _optional_str(item, "country"))
        if part
    ]
    return ", ".join(parts) or None


def _mode(item: dict[str, Any]) -> EventMode:
    if "is_online" not in item or item["is_online"] is None:
        return EventMode.UNKNOWN
    online = item["is_online"]
    if not isinstance(online, bool):
        raise TypeError("is_online is not a boolean")
    if online:
        return EventMode.ONLINE
    return EventMode.OFFLINE


def _themes(item: dict[str, Any]) -> list[str] | None:
    if "themes" not in item or item["themes"] is None:
        return None
    themes = item["themes"]
    if not isinstance(themes, list):
        raise TypeError("themes is not a list")
    names: list[str] = []
    for theme in themes:
        if not isinstance(theme, dict):
            raise TypeError("theme is not an object")
        name = theme.get("name")
        if isinstance(name, str) and name.strip():
            names.append(name.strip())
    return names or None


def _lifecycle(deadline: datetime | None, now: datetime) -> Lifecycle:
    if deadline is None:
        return Lifecycle.UNKNOWN
    if _as_utc(now) <= deadline:
        return Lifecycle.REGISTRATION_OPEN
    return Lifecycle.REGISTRATION_CLOSED


def _extra(
    item: dict[str, Any],
    *,
    start_at: datetime | None,
    end_at: datetime | None,
    deadline: datetime | None,
) -> dict[str, Any] | None:
    extra: dict[str, Any] = {}
    if start_at is not None or end_at is not None or deadline is not None:
        extra["date_precision"] = "datetime"
    for key in ("team_min", "team_size"):
        value = _whole_number(item.get(key))
        if value is not None:
            extra[key] = value
    return extra or None


def _external_id(item: dict[str, Any]) -> str:
    if item.get("uuid") is not None:
        return _id_text(item["uuid"])
    if item.get("id") is not None:
        return _id_text(item["id"])
    raise KeyError("uuid")


def _id_text(value: Any) -> str:
    if isinstance(value, bool) or not isinstance(value, int | str):
        raise TypeError("id is not a string or int")
    text = str(value).strip()
    if not text:
        raise TypeError("id is empty")
    return text


def _timestamp(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value.strip():
        return None
    text = value.strip().replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return None
    return parsed.astimezone(UTC)


def _whole_number(value: Any) -> int | None:
    if isinstance(value, bool) or not isinstance(value, int):
        return None
    return value


def _required_str(payload: dict[str, Any], key: str) -> str:
    value = payload[key]
    if not isinstance(value, str) or not value.strip():
        raise TypeError(f"{key} is not a string")
    return value.strip()


def _optional_str(payload: dict[str, Any], key: str) -> str | None:
    if key not in payload or payload[key] is None:
        return None
    value = payload[key]
    if not isinstance(value, str):
        raise TypeError(f"{key} is not a string")
    stripped = value.strip()
    return stripped or None


def _label(item: Any, index: int) -> str:
    if isinstance(item, dict):
        for key in ("uuid", "id", "slug"):
            value = item.get(key)
            if isinstance(value, int | str) and not isinstance(value, bool):
                text = str(value).strip()
                if text:
                    return f"item {text}"
    return f"item #{index + 1}"


def _problem(exc: Exception) -> str:
    if isinstance(exc, KeyError) and exc.args:
        return f"missing {exc.args[0]}"
    return str(exc)


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def main() -> None:
    result = asyncio.run(DevfolioCollector().collect())
    print(result.model_dump_json(indent=2))


if __name__ == "__main__":
    main()
