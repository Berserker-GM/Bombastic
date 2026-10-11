"""Devpost hackathon listing collector. Live HTTP runs only from ``__main__``."""

import asyncio
import re
from datetime import UTC, datetime
from typing import Any

import httpx

from radar.collectors.base import Collector, RawEvent
from radar.models.enums import EventCategory, EventMode, Lifecycle

HACKATHONS_URL = "https://devpost.com/api/hackathons"
_SOURCE_NAME = "devpost"
_TIMEOUT_SECONDS = 30.0
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
_INR = frozenset({"₹", "rs", "rs.", "inr"})


class DevpostCollector(Collector):
    min_expected = 1

    def __init__(self, *, now: datetime | None = None) -> None:
        self._now = now

    async def fetch(self) -> list[RawEvent]:
        async with httpx.AsyncClient(
            timeout=_TIMEOUT_SECONDS,
            follow_redirects=True,
            headers={"Accept": "application/json", "User-Agent": "radar/0.1"},
        ) as client:
            response = await client.get(HACKATHONS_URL)
            response.raise_for_status()
            payload = response.json()
        now = self._now if self._now is not None else datetime.now(UTC)
        return events_from_listing(payload, now=now)


def events_from_listing(payload: dict[str, Any], *, now: datetime) -> list[RawEvent]:
    if not isinstance(payload, dict):
        raise TypeError("listing is not an object")
    hackathons = payload["hackathons"]
    if not isinstance(hackathons, list):
        raise TypeError("hackathons is not a list")
    return [_event(hackathon, now=now) for hackathon in hackathons]


def parse_prize_amount_inr(text: str | None) -> int | None:
    if text is None:
        return None
    cleaned = _TAGS.sub("", text).replace("\xa0", " ")
    match = _MONEY.search(cleaned)
    if match is None:
        return None
    if match.group("currency").casefold() not in _INR:
        return None
    fraction = match.group("fraction")
    if fraction is not None and set(fraction) != {"0"}:
        return None
    return int(match.group("amount").replace(",", ""))


def parse_submission_period(
    text: str,
) -> tuple[datetime | None, datetime | None]:
    match = _PERIOD.fullmatch(text.strip())
    if match is None:
        return (None, None)
    end_year = int(match.group("end_year"))
    start_year = match.group("start_year")
    year = end_year if start_year is None else int(start_year)
    end_month_name = match.group("end_month") or match.group("start_month")
    start = _calendar_date(year, match.group("start_month"), match.group("start_day"))
    end = _calendar_date(end_year, end_month_name, match.group("end_day"))
    if start is None or end is None or start > end:
        return (None, None)
    return (start, end)


def _event(hackathon: Any, *, now: datetime) -> RawEvent:
    if not isinstance(hackathon, dict):
        raise TypeError("hackathon is not an object")
    start_at, end_at = _period(hackathon)
    location = _location(hackathon)
    prize_text = _optional_str(hackathon, "prize_amount")
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
        lifecycle=_lifecycle(start_at, end_at, now),
    )


def _period(hackathon: dict[str, Any]) -> tuple[datetime | None, datetime | None]:
    if "submission_period_dates" not in hackathon:
        return (None, None)
    value = hackathon["submission_period_dates"]
    if value is None:
        return (None, None)
    if not isinstance(value, str):
        raise TypeError("submission_period_dates is not a string")
    return parse_submission_period(value)


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
