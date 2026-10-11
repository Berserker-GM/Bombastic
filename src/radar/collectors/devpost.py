"""Devpost hackathon listing collector. Live HTTP runs only from ``__main__``."""

import asyncio
import re
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from typing import Any

import httpx
from pydantic import ValidationError

from radar.collectors.base import Collector, CollectResult, RawEvent
from radar.models.enums import EventCategory, EventMode, Lifecycle

HACKATHONS_URL = "https://devpost.com/api/hackathons"
_SOURCE_NAME = "devpost"
_TIMEOUT_SECONDS = 30.0
_MAX_PAGES = 30
_PAGE_INTERVAL_SECONDS = 1.0
_PAGE_CAP_WARNING = "stopped after 30 pages"
SleepFn = Callable[[float], Awaitable[None]]
_TAGS = re.compile(r"<[^>]*>")
_MONEY = re.compile(
    r"(?P<currency>₹|(?<![A-Za-z])Rs\.?(?![A-Za-z])|(?<![A-Za-z])INR(?![A-Za-z])"
    r"|\$|(?<![A-Za-z])USD(?![A-Za-z])|(?<![A-Za-z])EUR(?![A-Za-z])|€|£"
    r"|(?<![A-Za-z])GBP(?![A-Za-z]))"
    r"\s*(?P<amount>\d{1,3}(?:,\d{3})+|\d+)(?:\.(?P<fraction>\d+))?",
    re.IGNORECASE,
)
_PERIOD = re.compile(
    r"^(?P<start_month>[A-Za-z]{3})\s+(?P<start_day>\d{1,2})"
    r"(?:,\s*(?P<start_year>\d{4}))?\s+-\s+"
    r"(?:(?P<end_month>[A-Za-z]{3})\s+)?"
    r"(?P<end_day>\d{1,2}),\s*(?P<end_year>\d{4})$"
)
_SINGLE = re.compile(r"^(?P<month>[A-Za-z]{3})\s+(?P<day>\d{1,2}),\s*(?P<year>\d{4})$")
_MONTHS = {
    "jan": 1,
    "feb": 2,
    "mar": 3,
    "apr": 4,
    "may": 5,
    "jun": 6,
    "jul": 7,
    "aug": 8,
    "sep": 9,
    "oct": 10,
    "nov": 11,
    "dec": 12,
}
_CURRENCY = {
    "₹": "INR",
    "rs": "INR",
    "rs.": "INR",
    "inr": "INR",
    "$": "USD",
    "usd": "USD",
}
_OPEN_STATE = {
    "open": Lifecycle.REGISTRATION_OPEN,
    "upcoming": Lifecycle.UPCOMING,
    "ended": Lifecycle.REGISTRATION_CLOSED,
}
_DATE_CONFIDENCE = {
    "registration_deadline": 0.6,
    "start_at": 0.6,
    "end_at": 0.6,
}


class DevpostCollector(Collector):
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
            headers={"Accept": "application/json", "User-Agent": "radar/0.1"},
        ) as client:
            for page in range(1, _MAX_PAGES + 1):
                if page > 1:
                    await self._sleep(_PAGE_INTERVAL_SECONDS)
                response = await client.get(HACKATHONS_URL, params=_page_params(page))
                response.raise_for_status()
                batch, warnings, count = parse_listing(response.json(), now=now)
                self._warnings.extend(warnings)
                self._failed_items += count - len(batch)
                if count == 0:
                    break
                events.extend(batch)
            else:
                self._page_cap_hit = True
        return events

    def _from_events(self, events: list[RawEvent]) -> CollectResult:
        if not events and self._failed_items:
            message = "every item failed: " + "; ".join(self._warnings)
            return CollectResult(
                status="malformed",
                events=[],
                error=message,
                warnings=list(self._warnings),
            )
        result = super()._from_events(events)
        warnings = list(self._warnings)
        warning = result.warning
        if self._page_cap_hit and result.status == "ok":
            warnings = [_PAGE_CAP_WARNING, *warnings]
            warning = _PAGE_CAP_WARNING
            if self._warnings:
                warning = f"{_PAGE_CAP_WARNING}; " + "; ".join(self._warnings)
        elif self._warnings and result.status == "ok":
            warning = "; ".join(self._warnings)
        if warnings or warning != result.warning:
            return result.model_copy(update={"warning": warning, "warnings": warnings})
        return result


def events_from_listing(payload: dict[str, Any], *, now: datetime) -> list[RawEvent]:
    events, _warnings, _failed = parse_listing(payload, now=now)
    return events


def parse_listing(
    payload: dict[str, Any], *, now: datetime
) -> tuple[list[RawEvent], list[str], int]:
    if not isinstance(payload, dict):
        raise TypeError("listing is not an object")
    hackathons = payload["hackathons"]
    if not isinstance(hackathons, list):
        raise TypeError("hackathons is not a list")
    events: list[RawEvent] = []
    warnings: list[str] = []
    for index, hackathon in enumerate(hackathons):
        try:
            events.append(_event(hackathon, now=now))
        except (KeyError, TypeError, ValueError, ValidationError) as exc:
            warnings.append(f"{_hackathon_label(hackathon, index)}: {exc}")
    return events, warnings, len(hackathons)


def _hackathon_label(hackathon: Any, index: int) -> str:
    if isinstance(hackathon, dict) and hackathon.get("id") is not None:
        return f"item {hackathon['id']}"
    return f"item {index}"


def parse_prize_amount_inr(text: str | None) -> int | None:
    parsed = parse_prize(text)
    if parsed is None:
        return None
    amount, currency = parsed
    if currency != "INR" or isinstance(amount, float):
        return None
    return amount


def parse_prize(text: str | None) -> tuple[int | float, str] | None:
    plain = plain_prize_text(text)
    if plain is None:
        return None
    match = _MONEY.search(plain)
    if match is None:
        return None
    currency = _CURRENCY.get(match.group("currency").casefold())
    if currency is None:
        return None
    fraction = match.group("fraction")
    whole = match.group("amount").replace(",", "")
    if fraction is None or set(fraction) <= {"0"}:
        return (int(whole), currency)
    return (float(f"{whole}.{fraction}"), currency)


def plain_prize_text(text: str | None) -> str | None:
    if text is None:
        return None
    cleaned = _TAGS.sub("", text).replace("\xa0", " ")
    collapsed = " ".join(cleaned.split())
    return collapsed or None


def parse_submission_period(
    text: str,
) -> tuple[datetime | None, datetime | None]:
    stripped = text.strip()
    single = _SINGLE.fullmatch(stripped)
    if single is not None:
        day = _calendar_date(
            int(single.group("year")),
            single.group("month"),
            single.group("day"),
        )
        if day is None:
            return (None, None)
        return (day, day)
    match = _PERIOD.fullmatch(stripped)
    if match is None:
        return (None, None)
    end_year = int(match.group("end_year"))
    end_month_name = match.group("end_month") or match.group("start_month")
    end = _calendar_date(end_year, end_month_name, match.group("end_day"))
    start_year_text = match.group("start_year")
    start_year = end_year if start_year_text is None else int(start_year_text)
    start = _calendar_date(
        start_year, match.group("start_month"), match.group("start_day")
    )
    if (
        start is not None
        and end is not None
        and start > end
        and start_year_text is None
    ):
        start = _calendar_date(
            end_year - 1, match.group("start_month"), match.group("start_day")
        )
    if start is None or end is None or start > end:
        return (None, None)
    return (start, end)


def _event(hackathon: Any, *, now: datetime) -> RawEvent:
    if not isinstance(hackathon, dict):
        raise TypeError("hackathon is not an object")
    start_at, end_at = _period(hackathon)
    location = _location(hackathon)
    prize_text = plain_prize_text(_optional_str(hackathon, "prize_amount"))
    return RawEvent(
        source_name=_SOURCE_NAME,
        title=_required_str(hackathon, "title"),
        category=EventCategory.HACKATHON,
        external_id=_external_id(hackathon["id"]),
        url=_required_str(hackathon, "url"),
        start_at=start_at,
        end_at=end_at,
        registration_deadline=end_at,
        location=location,
        mode=_mode(location),
        prize_text=prize_text,
        prize_amount_inr=parse_prize_amount_inr(prize_text),
        organizer=_optional_str(hackathon, "organization_name"),
        eligibility=_optional_str(
            hackathon, "eligibility_requirement_invite_only_description"
        ),
        tags=_themes(hackathon),
        lifecycle=_lifecycle(hackathon, start_at, end_at, now),
        confidence=dict(_DATE_CONFIDENCE),
        extra=_extra(prize_text),
    )


def _page_params(page: int) -> list[tuple[str, str]]:
    return [("status[]", "open"), ("status[]", "upcoming"), ("page", str(page))]


def _period(hackathon: dict[str, Any]) -> tuple[datetime | None, datetime | None]:
    value = hackathon.get("submission_period_dates")
    if not isinstance(value, str):
        return (None, None)
    return parse_submission_period(value)


def _extra(prize_text: str | None) -> dict[str, Any]:
    extra: dict[str, Any] = {
        "date_precision": "date",
        "deadline_kind": "submission_end",
    }
    parsed = parse_prize(prize_text)
    if parsed is None:
        return extra
    amount, currency = parsed
    extra["prize_amount"] = amount
    extra["prize_currency"] = currency
    return extra


def _location(hackathon: dict[str, Any]) -> str | None:
    if "displayed_location" not in hackathon:
        return None
    displayed = hackathon["displayed_location"]
    if displayed is None:
        return None
    if not isinstance(displayed, dict):
        raise TypeError("displayed_location is not an object")
    return _optional_str(displayed, "location")


def _mode(location: str | None) -> EventMode:
    if location is None:
        return EventMode.UNKNOWN
    if location.casefold() == "online":
        return EventMode.ONLINE
    return EventMode.OFFLINE


def _themes(hackathon: dict[str, Any]) -> list[str] | None:
    if "themes" not in hackathon:
        return None
    themes = hackathon["themes"]
    if themes is None:
        return None
    if not isinstance(themes, list):
        raise TypeError("themes is not a list")
    names: list[str] = []
    for theme in themes:
        if not isinstance(theme, dict):
            raise TypeError("theme is not an object")
        name = theme["name"]
        if not isinstance(name, str) or not name.strip():
            raise TypeError("theme name is not a string")
        names.append(name.strip())
    return names


def _lifecycle(
    hackathon: dict[str, Any],
    start_at: datetime | None,
    end_at: datetime | None,
    now: datetime,
) -> Lifecycle:
    if "open_state" in hackathon and hackathon["open_state"] is not None:
        state = hackathon["open_state"]
        if isinstance(state, str) and state.strip():
            return _OPEN_STATE.get(state.strip().casefold(), Lifecycle.UNKNOWN)
    return _lifecycle_from_dates(start_at, end_at, now)


def _lifecycle_from_dates(
    start_at: datetime | None,
    end_at: datetime | None,
    now: datetime,
) -> Lifecycle:
    if start_at is None or end_at is None:
        return Lifecycle.UNKNOWN
    today = _as_utc(now).date()
    if today < start_at.date():
        return Lifecycle.UPCOMING
    if today > end_at.date():
        return Lifecycle.REGISTRATION_CLOSED
    return Lifecycle.REGISTRATION_OPEN


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
    if key not in payload:
        return None
    value = payload[key]
    if value is None:
        return None
    if not isinstance(value, str):
        raise TypeError(f"{key} is not a string")
    stripped = value.strip()
    return stripped or None


def _calendar_date(year: int, month_name: str, day_text: str) -> datetime | None:
    month = _MONTHS.get(month_name.casefold())
    if month is None:
        return None
    try:
        return datetime(year, month, int(day_text), tzinfo=UTC)
    except ValueError:
        return None


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def main() -> None:
    result = asyncio.run(DevpostCollector().collect())
    print(result.model_dump_json(indent=2))


if __name__ == "__main__":
    main()
