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
)
from radar.models.enums import EventCategory, EventMode, Lifecycle

FIXTURE = Path(__file__).parent / "fixtures" / "devpost_hackathons.json"
NOW = datetime(2026, 10, 11, tzinfo=UTC)
_USD_PRIZE = "$<span data-currency-value>138,000</span>"
_INR_PRIZE = "₹ <span data-currency-value>100,000</span>"


def _payload() -> dict[str, Any]:
    return json.loads(FIXTURE.read_text(encoding="utf-8"))


def _collect(response: httpx.Response, *, now: datetime = NOW) -> CollectResult:
    async def run() -> CollectResult:
        async with respx.mock:
            respx.get(HACKATHONS_URL).mock(return_value=response)
            return await DevpostCollector(now=now).collect()

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
    assert amazon.prize_text == _USD_PRIZE
    assert amazon.prize_amount_inr is None
    assert amazon.location == "Online"
    assert amazon.mode is EventMode.ONLINE
    assert amazon.lifecycle is Lifecycle.REGISTRATION_OPEN
    assert amazon.organizer == "Amazon"
    assert amazon.eligibility is None
    assert amazon.tags == ["Machine Learning/AI", "Open Ended", "Productivity"]
    assert amazon.confidence == {}
    assert amazon.evidence == []

    rupee = result.events[1]
    assert rupee.title == "Multimodal AI Hackathon 2026"
    assert rupee.prize_text == _INR_PRIZE
    assert rupee.prize_amount_inr == 100_000
    assert rupee.start_at == datetime(2026, 9, 29, tzinfo=UTC)
    assert rupee.registration_deadline == datetime(2026, 10, 14, tzinfo=UTC)
    assert rupee.organizer == "Kamand Prompt, IIT Mandi"
    assert rupee.tags == ["Machine Learning/AI"]

    gitlab = result.events[2]
    assert gitlab.title == "Life After Code"
    assert gitlab.start_at == datetime(2026, 10, 5, tzinfo=UTC)
    assert gitlab.end_at == datetime(2026, 10, 27, tzinfo=UTC)
    assert gitlab.prize_text == "$<span data-currency-value>45,000</span>"
    assert gitlab.prize_amount_inr is None
    assert gitlab.tags == ["DevOps", "Machine Learning/AI", "Productivity"]
    assert gitlab.organizer == "GitLab"


def test_lifecycle_follows_the_submission_period() -> None:
    payload = _payload()
    amazon = {"hackathons": [payload["hackathons"][0]]}

    before = events_from_listing(amazon, now=datetime(2026, 8, 1, tzinfo=UTC))
    during = events_from_listing(amazon, now=datetime(2026, 10, 23, 18, tzinfo=UTC))
    after = events_from_listing(amazon, now=datetime(2026, 10, 24, tzinfo=UTC))

    assert before[0].lifecycle is Lifecycle.UPCOMING
    assert during[0].lifecycle is Lifecycle.REGISTRATION_OPEN
    assert after[0].lifecycle is Lifecycle.REGISTRATION_CLOSED


def test_unclear_period_leaves_dates_unknown() -> None:
    hackathon = _payload()["hackathons"][0]
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


def test_empty_listing_is_suspiciously_empty() -> None:
    result = _collect(httpx.Response(200, json={"hackathons": []}))

    assert result.status == "malformed"
    assert result.error == "suspiciously empty"
    assert result.events == []
