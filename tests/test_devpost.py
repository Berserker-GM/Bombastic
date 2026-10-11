import asyncio
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import httpx
import pytest
import respx

from radar.collectors.base import CollectResult
from radar.collectors.devpost import (
    HACKATHONS_URL,
    DevpostCollector,
    events_from_listing,
    parse_prize_amount_inr,
    parse_submission_period,
)
from radar.models.enums import EventCategory, EventMode, Lifecycle

FIXTURE = Path(__file__).parent / "fixtures" / "devpost_hackathons.json"
NOW = datetime(2026, 10, 11, tzinfo=UTC)


def _payload() -> dict[str, Any]:
    return json.loads(FIXTURE.read_text(encoding="utf-8"))


async def _noop_sleep(_: float) -> None:
    return None


def _collect(response: httpx.Response, *, now: datetime = NOW) -> CollectResult:
    def respond(request: httpx.Request) -> httpx.Response:
        if request.url.params.get("page") not in (None, "1"):
            return httpx.Response(200, json={"hackathons": []})
        return response

    async def run() -> CollectResult:
        async with respx.mock:
            respx.get(url__startswith=HACKATHONS_URL).mock(side_effect=respond)
            return await DevpostCollector(now=now, sleep=_noop_sleep).collect()

    return asyncio.run(run())


def test_listing_maps_dates_prizes_and_themes() -> None:
    result = _collect(httpx.Response(200, json=_payload()))

    assert result.status == "ok"
    assert result.error is None
    assert [event.external_id for event in result.events] == [
        "30992",
        "31481",
        "31503",
    ]

    amazon = result.events[0]
    assert amazon.source_name == "devpost"
    assert amazon.title == "Build, Ship, Shape: Amazon Developer Hackathon"
    assert amazon.category is EventCategory.HACKATHON
    assert amazon.url == "https://amazonappdev2026.devpost.com/"
    assert amazon.start_at == datetime(2026, 8, 31, tzinfo=UTC)
    assert amazon.end_at == datetime(2026, 10, 23, tzinfo=UTC)
    assert amazon.registration_deadline == datetime(2026, 10, 23, tzinfo=UTC)
    assert amazon.prize_text == "$138,000"
    assert amazon.prize_amount_inr is None
    assert amazon.confidence == {
        "registration_deadline": 0.6,
        "start_at": 0.6,
        "end_at": 0.6,
    }
    assert amazon.extra == {
        "date_precision": "date",
        "deadline_kind": "submission_end",
        "prize_amount": 138_000,
        "prize_currency": "USD",
    }
    assert amazon.location == "Online"
    assert amazon.mode is EventMode.ONLINE
    assert amazon.lifecycle is Lifecycle.REGISTRATION_OPEN
    assert amazon.organizer == "Amazon"
    assert amazon.eligibility is None
    assert amazon.tags == ["Machine Learning/AI", "Open Ended", "Productivity"]
    assert amazon.evidence == []

    rupee = result.events[1]
    assert rupee.title == "Multimodal AI Hackathon 2026"
    assert rupee.prize_text == "₹ 100,000"
    assert rupee.prize_amount_inr == 100_000
    assert rupee.extra is not None
    assert rupee.extra["prize_amount"] == 100_000
    assert rupee.extra["prize_currency"] == "INR"
    assert rupee.start_at == datetime(2026, 9, 29, tzinfo=UTC)
    assert rupee.registration_deadline == datetime(2026, 10, 14, tzinfo=UTC)
    assert rupee.organizer == "Kamand Prompt, IIT Mandi"
    assert rupee.tags == ["Machine Learning/AI"]

    gitlab = result.events[2]
    assert gitlab.title == "Life After Code"
    assert gitlab.start_at == datetime(2026, 10, 5, tzinfo=UTC)
    assert gitlab.end_at == datetime(2026, 10, 27, tzinfo=UTC)
    assert gitlab.prize_text == "$45,000"
    assert gitlab.extra is not None
    assert gitlab.extra["prize_amount"] == 45_000
    assert gitlab.extra["prize_currency"] == "USD"
    assert gitlab.prize_amount_inr is None
    assert gitlab.tags == ["DevOps", "Machine Learning/AI", "Productivity"]
    assert gitlab.organizer == "GitLab"


def test_lifecycle_follows_the_submission_period() -> None:
    payload = _payload()
    hackathon = payload["hackathons"][0]
    del hackathon["open_state"]
    amazon = {"hackathons": [hackathon]}

    before = events_from_listing(amazon, now=datetime(2026, 8, 1, tzinfo=UTC))
    during = events_from_listing(amazon, now=datetime(2026, 10, 23, 18, tzinfo=UTC))
    after = events_from_listing(amazon, now=datetime(2026, 10, 24, tzinfo=UTC))

    assert before[0].lifecycle is Lifecycle.UPCOMING
    assert during[0].lifecycle is Lifecycle.REGISTRATION_OPEN
    assert after[0].lifecycle is Lifecycle.REGISTRATION_CLOSED


def test_unclear_period_leaves_dates_unknown() -> None:
    hackathon = _payload()["hackathons"][0]
    del hackathon["open_state"]
    hackathon["submission_period_dates"] = "soon"

    event = events_from_listing({"hackathons": [hackathon]}, now=NOW)[0]

    assert event.start_at is None
    assert event.end_at is None
    assert event.registration_deadline is None
    assert event.lifecycle is Lifecycle.UNKNOWN


def test_in_person_location_and_eligibility_text() -> None:
    hackathon = _payload()["hackathons"][0]
    hackathon["displayed_location"]["location"] = "San Francisco, CA"
    hackathon["eligibility_requirement_invite_only_description"] = "Students only"

    event = events_from_listing({"hackathons": [hackathon]}, now=NOW)[0]

    assert event.location == "San Francisco, CA"
    assert event.mode is EventMode.OFFLINE
    assert event.eligibility == "Students only"


def test_missing_location_is_unknown_mode() -> None:
    hackathon = _payload()["hackathons"][0]
    del hackathon["displayed_location"]

    event = events_from_listing({"hackathons": [hackathon]}, now=NOW)[0]

    assert event.location is None
    assert event.mode is EventMode.UNKNOWN


@pytest.mark.parametrize(
    ("text", "amount"),
    [
        ("$10,000 in prizes", None),
        ("$<span data-currency-value>138,000</span>", None),
        ("₹ <span data-currency-value>100,000</span>", 100_000),
        ("₹ 1,500.00", 1_500),
        ("₹ 1,500.50", None),
        ("free swag", None),
        (None, None),
    ],
)
def test_prize_amounts_parse_defensively(text: str | None, amount: int | None) -> None:
    assert parse_prize_amount_inr(text) == amount


def test_dollar_prize_text_stays_raw() -> None:
    hackathon = _payload()["hackathons"][0]
    hackathon["prize_amount"] = "$10,000 in prizes"

    event = events_from_listing({"hackathons": [hackathon]}, now=NOW)[0]

    assert event.prize_text == "$10,000 in prizes"
    assert event.prize_amount_inr is None
    assert event.extra is not None
    assert event.extra["prize_amount"] == 10_000
    assert event.extra["prize_currency"] == "USD"


def test_http_500_is_unreachable() -> None:
    result = _collect(httpx.Response(500, text="unavailable"))

    assert result.status == "unreachable"
    assert result.events == []
    assert result.error is not None


def test_missing_hackathons_is_malformed() -> None:
    result = _collect(httpx.Response(200, json={"meta": {}}))

    assert result.status == "malformed"
    assert result.events == []
    assert result.error is not None


@pytest.mark.parametrize(
    ("text", "start", "end"),
    [
        (
            "Dec 20 - Jan 5, 2027",
            datetime(2026, 12, 20, tzinfo=UTC),
            datetime(2027, 1, 5, tzinfo=UTC),
        ),
        (
            "Oct 5, 2026",
            datetime(2026, 10, 5, tzinfo=UTC),
            datetime(2026, 10, 5, tzinfo=UTC),
        ),
        (
            "Oct 05 - 27, 2026",
            datetime(2026, 10, 5, tzinfo=UTC),
            datetime(2026, 10, 27, tzinfo=UTC),
        ),
    ],
)
def test_submission_period_parses_ranges_and_single_days(
    text: str, start: datetime, end: datetime
) -> None:
    assert parse_submission_period(text) == (start, end)


def test_unparsable_period_uses_open_state_and_does_not_raise() -> None:
    hackathon = _payload()["hackathons"][0]
    hackathon["submission_period_dates"] = "soon"
    hackathon["open_state"] = "ended"

    event = events_from_listing({"hackathons": [hackathon]}, now=NOW)[0]

    assert event.start_at is None
    assert event.end_at is None
    assert event.registration_deadline is None
    assert event.lifecycle is Lifecycle.REGISTRATION_CLOSED


def test_open_state_overrides_dates() -> None:
    hackathon = _payload()["hackathons"][0]

    hackathon["open_state"] = "upcoming"
    upcoming = events_from_listing({"hackathons": [hackathon]}, now=NOW)[0]
    hackathon["open_state"] = "ended"
    ended = events_from_listing({"hackathons": [hackathon]}, now=NOW)[0]
    hackathon["open_state"] = "paused"
    unknown = events_from_listing({"hackathons": [hackathon]}, now=NOW)[0]

    assert upcoming.lifecycle is Lifecycle.UPCOMING
    assert ended.lifecycle is Lifecycle.REGISTRATION_CLOSED
    assert unknown.lifecycle is Lifecycle.UNKNOWN


def test_pages_open_and_upcoming_until_an_empty_page() -> None:
    body = _payload()
    delays: list[float] = []
    seen: list[tuple[str, list[str]]] = []

    async def sleep(seconds: float) -> None:
        delays.append(seconds)

    def respond(request: httpx.Request) -> httpx.Response:
        params = request.url.params
        seen.append((params.get("page", ""), params.get_list("status[]")))
        if params.get("page") in (None, "1"):
            return httpx.Response(200, json=body)
        return httpx.Response(200, json={"hackathons": []})

    async def run() -> CollectResult:
        async with respx.mock:
            respx.get(url__startswith=HACKATHONS_URL).mock(side_effect=respond)
            return await DevpostCollector(now=NOW, sleep=sleep).collect()

    result = asyncio.run(run())

    assert seen == [("1", ["open", "upcoming"]), ("2", ["open", "upcoming"])]
    assert delays == [1.0]
    assert result.status == "ok"
    assert result.warning is None
    assert [event.external_id for event in result.events] == [
        "30992",
        "31481",
        "31503",
    ]


def test_page_cap_returns_ok_with_a_warning() -> None:
    delays: list[float] = []
    pages: list[str] = []

    async def sleep(seconds: float) -> None:
        delays.append(seconds)

    def respond(request: httpx.Request) -> httpx.Response:
        page = request.url.params.get("page", "")
        pages.append(page)
        hackathon = _payload()["hackathons"][0]
        hackathon["id"] = int(page)
        hackathon["title"] = f"Hack {page}"
        return httpx.Response(200, json={"hackathons": [hackathon]})

    async def run() -> CollectResult:
        async with respx.mock:
            respx.get(url__startswith=HACKATHONS_URL).mock(side_effect=respond)
            return await DevpostCollector(now=NOW, sleep=sleep).collect()

    result = asyncio.run(run())

    assert pages == [str(page) for page in range(1, 31)]
    assert delays == [1.0] * 29
    assert result.status == "ok"
    assert result.warning == "stopped after 30 pages"
    assert [event.external_id for event in result.events] == [
        str(page) for page in range(1, 31)
    ]


def test_empty_listing_is_suspiciously_empty() -> None:
    result = _collect(httpx.Response(200, json={"hackathons": []}))

    assert result.status == "malformed"
    assert result.error == "suspiciously empty"
    assert result.events == []
