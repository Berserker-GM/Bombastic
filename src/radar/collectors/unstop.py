"""Unstop hackathon listing collector. Live HTTP runs only from ``__main__``."""

import asyncio
import re
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from typing import Any

import httpx
from pydantic import ValidationError

from radar.collectors.base import Collector, CollectResult, RawEvent
from radar.models.enums import EventCategory, EventMode, Lifecycle

SEARCH_URL = "https://unstop.com/api/public/opportunity/search-result"
_SOURCE_NAME = "unstop"
_TIMEOUT_SECONDS = 30.0
_MAX_PAGES = 10
_PAGE_INTERVAL_SECONDS = 1.0
_PAGE_CAP_WARNING = "stopped after 10 pages"
_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
)
SleepFn = Callable[[float], Awaitable[None]]
_TAGS = re.compile(r"<[^>]*>")
_PRIZE_WORD = re.compile(r"\b(?:prizes?|worth|pools?)\b", re.IGNORECASE)
_CURRENCY = re.compile(
    r"₹|(?<![A-Za-z])Rs\.?(?![A-Za-z])|(?<![A-Za-z])INR(?![A-Za-z])"
    r"|\$|(?<![A-Za-z])USD(?![A-Za-z])|(?<![A-Za-z])EUR(?![A-Za-z])"
    r"|€|£|(?<![A-Za-z])GBP(?![A-Za-z])",
    re.IGNORECASE,
)
_AMOUNT = re.compile(
    r"(?P<amount>\d{1,3}(?:,\d{2,3})+|\d+)"
    r"(?:\.(?P<fraction>\d+))?"
    r"(?![A-Za-z+])"
    r"(?:\s*(?P<unit>lakhs?|lacs?|crores?))?",
    re.IGNORECASE,
)
_QUALIFIER = re.compile(
    r"\b(?:up to|upto|at least|more than|over|around|approx(?:imately)?)\s*$",
    re.IGNORECASE,
)
_INR = frozenset({"₹", "rs", "rs.", "inr"})
_UNIT = {
    "lakh": 100_000,
    "lakhs": 100_000,
    "lac": 100_000,
    "lacs": 100_000,
    "crore": 10_000_000,
    "crores": 10_000_000,
}


class UnstopCollector(Collector):
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
                response = await client.get(SEARCH_URL, params=_page_params(page))
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
    deadline = _timestamp(_regn(item).get("end_regn_dt"))
    end_at = _timestamp(item.get("end_date"))
    prize_text, prize_amount_inr = prize_from_details(_details(item))
    return RawEvent(
        source_name=_SOURCE_NAME,
        title=_required_str(item, "title"),
        category=EventCategory.HACKATHON,
        external_id=_external_id(item["id"]),
        url=_url(item),
        start_at=None,
        end_at=end_at,
        registration_deadline=deadline,
        mode=_mode(item),
        prize_text=prize_text,
        prize_amount_inr=prize_amount_inr,
        organizer=_organizer(item),
        eligibility=_eligibility(item),
        tags=_tags(item),
        lifecycle=_lifecycle(deadline, now),
        extra=_extra(item, deadline=deadline, end_at=end_at),
    )


def prize_from_details(html: str | None) -> tuple[str | None, int | None]:
    if not html:
        return (None, None)
    for sentence in _sentences(_plain(html)):
        if _PRIZE_WORD.search(sentence) is None:
            continue
        amount = _first_inr_amount(sentence)
        if amount is _NO_MONEY:
            continue
        return (sentence, amount if isinstance(amount, int) else None)
    return (None, None)


_NO_MONEY = object()


def _page_params(page: int) -> dict[str, str]:
    return {"opportunity": "hackathons", "per_page": "18", "page": str(page)}


def _items(payload: dict[str, Any]) -> list[Any]:
    if not isinstance(payload, dict):
        raise TypeError("listing is not an object")
    data = payload["data"]
    if not isinstance(data, dict):
        raise TypeError("data is not an object")
    items = data["data"]
    if not isinstance(items, list):
        raise TypeError("data.data is not a list")
    return items


def _regn(item: dict[str, Any]) -> dict[str, Any]:
    if "regnRequirements" not in item or item["regnRequirements"] is None:
        return {}
    regn = item["regnRequirements"]
    if not isinstance(regn, dict):
        raise TypeError("regnRequirements is not an object")
    return regn


def _details(item: dict[str, Any]) -> str | None:
    if "details" not in item or item["details"] is None:
        return None
    details = item["details"]
    if not isinstance(details, str):
        raise TypeError("details is not a string")
    return details


def _url(item: dict[str, Any]) -> str:
    return "https://unstop.com/" + _required_str(item, "public_url").lstrip("/")


def _organizer(item: dict[str, Any]) -> str | None:
    if "organisation" not in item or item["organisation"] is None:
        return None
    organisation = item["organisation"]
    if not isinstance(organisation, dict):
        raise TypeError("organisation is not an object")
    return _optional_str(organisation, "name")


def _eligibility(item: dict[str, Any]) -> str | None:
    filters = _object_list(item, "filters")
    if filters is None:
        return None
    names: list[str] = []
    for entry in filters:
        if entry.get("type") != "eligible":
            continue
        name = entry.get("name")
        if isinstance(name, str) and name.strip():
            names.append(name.strip())
    return ", ".join(names) or None


def _tags(item: dict[str, Any]) -> list[str] | None:
    names: list[str] = []
    skills = _object_list(item, "required_skills")
    if skills is not None:
        for skill in skills:
            name = skill.get("skill")
            if isinstance(name, str) and name.strip():
                names.append(name.strip())
    if "tags" in item and item["tags"] is not None:
        tags = item["tags"]
        if not isinstance(tags, list):
            raise TypeError("tags is not a list")
        for tag in tags:
            if tag is None:
                continue
            if not isinstance(tag, str):
                raise TypeError("tag is not a string")
            if tag.strip():
                names.append(tag.strip())
    return names or None


def _mode(item: dict[str, Any]) -> EventMode:
    region = _optional_str(item, "region")
    if region is None:
        return EventMode.UNKNOWN
    folded = region.casefold()
    if folded == "online":
        return EventMode.ONLINE
    if folded == "offline":
        return EventMode.OFFLINE
    return EventMode.UNKNOWN


def _lifecycle(deadline: datetime | None, now: datetime) -> Lifecycle:
    if deadline is None:
        return Lifecycle.UNKNOWN
    if _as_utc(now) <= deadline:
        return Lifecycle.REGISTRATION_OPEN
    return Lifecycle.REGISTRATION_CLOSED


def _extra(
    item: dict[str, Any],
    *,
    deadline: datetime | None,
    end_at: datetime | None,
) -> dict[str, Any] | None:
    extra: dict[str, Any] = {}
    if deadline is not None or end_at is not None:
        extra["date_precision"] = "datetime"
    regn = _regn(item)
    for key in ("min_team_size", "max_team_size"):
        value = _whole_number(regn.get(key))
        if value is not None:
            extra[key] = value
    count = _whole_number(item.get("registerCount"))
    if count is not None:
        extra["registerCount"] = count
    return extra or None


def _first_inr_amount(sentence: str) -> int | None | object:
    for match in _CURRENCY.finditer(sentence):
        number = _amount_after(sentence, match.end())
        if number is None:
            number = _amount_before(sentence, match.start())
        if number is None:
            continue
        window = sentence[max(0, match.start() - 16) : match.start()]
        qualified = _QUALIFIER.search(window) is not None
        currency = match.group(0).casefold()
        if qualified or currency not in _INR or isinstance(number, float):
            return None
        return number
    return _NO_MONEY


def _amount_after(sentence: str, end: int) -> int | float | None:
    match = _AMOUNT.match(sentence[end : end + 32].lstrip())
    if match is None:
        return None
    return _number(match)


def _amount_before(sentence: str, start: int) -> int | float | None:
    head = sentence[max(0, start - 32) : start].rstrip()
    match = None
    for candidate in _AMOUNT.finditer(head):
        match = candidate
    if match is None or match.end() != len(head):
        return None
    return _number(match)


def _number(match: re.Match[str]) -> int | float:
    whole = int(match.group("amount").replace(",", ""))
    unit = match.group("unit")
    if unit is not None:
        whole *= _UNIT[unit.casefold()]
    fraction = match.group("fraction")
    if fraction is not None and set(fraction) - {"0"}:
        return float(f"{whole}.{fraction}")
    return whole


def _plain(html: str) -> str:
    text = _TAGS.sub("", html)
    text = text.replace("&nbsp;", " ").replace("&#160;", " ").replace("\xa0", " ")
    return " ".join(text.split())


def _sentences(text: str) -> list[str]:
    return [part.strip() for part in re.split(r"(?<=[.!?])\s+", text) if part.strip()]


def _timestamp(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        parsed = datetime.fromisoformat(value.strip())
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return None
    return parsed.astimezone(UTC)


def _whole_number(value: Any) -> int | None:
    if isinstance(value, bool) or not isinstance(value, int):
        return None
    return value


def _object_list(item: dict[str, Any], key: str) -> list[dict[str, Any]] | None:
    if key not in item or item[key] is None:
        return None
    values = item[key]
    if not isinstance(values, list):
        raise TypeError(f"{key} is not a list")
    objects: list[dict[str, Any]] = []
    for value in values:
        if not isinstance(value, dict):
            raise TypeError(f"{key} entry is not an object")
        objects.append(value)
    return objects


def _external_id(value: Any) -> str:
    if isinstance(value, bool) or not isinstance(value, int | str):
        raise TypeError("id is not a string or int")
    external_id = str(value).strip()
    if not external_id:
        raise TypeError("id is empty")
    return external_id


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
        value = item.get("id")
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
    result = asyncio.run(UnstopCollector().collect())
    print(result.model_dump_json(indent=2))


if __name__ == "__main__":
    main()
