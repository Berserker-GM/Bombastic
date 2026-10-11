import asyncio
import json
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import httpx
import respx

from radar.collectors.base import CollectResult
from radar.collectors.unstop import SEARCH_URL, UnstopCollector, event_from_item
from radar.models.enums import EventMode, Lifecycle

FIXTURE = Path(__file__).parent / "fixtures" / "unstop_hackathons.json"
NOW = datetime(2026, 10, 11, tzinfo=UTC)
_PRIZE = (
    "Join our upcoming CTF (Capture the Flag) event and compete for cash prizes "
    "worth INR 6000 and an exclusive internship opportunity!"
)


def _payload() -> dict[str, Any]:
    loaded = json.loads(FIXTURE.read_text(encoding="utf-8"))
    if not isinstance(loaded, dict):
        raise TypeError("fixture is not an object")
    return loaded


def test_listing_maps_the_real_fixture() -> None:
    result = _collect(httpx.Response(200, json=_payload()))

    assert result.status == "ok"
    assert result.warning is None
    assert result.error is None
    events = {event.external_id: event for event in result.events}
    iot = events["1369976"]
    assert iot.source_name == "unstop"
    assert iot.title == "IoT Hackathon"
    assert iot.url == (
        "https://unstop.com/hackathons/"
        "iot-hackathon-v-prayukti25-bannari-amman-institute-of-technology-1369976"
    )
    assert iot.organizer == "Bannari Amman Institute of Technology"
    assert iot.registration_deadline == datetime(2025, 2, 4, 18, 29, 51, tzinfo=UTC)
    assert iot.start_at is None
    assert iot.end_at == datetime(2025, 2, 8, 3, 30, tzinfo=UTC)
    assert iot.mode is EventMode.OFFLINE
    assert iot.lifecycle is Lifecycle.REGISTRATION_CLOSED
    assert iot.eligibility == "Engineering Students, Postgraduate, Undergraduate"
    assert iot.tags == [
        "Embedded Systems Design",
        "Project Management",
        "Collaboration Skills",
        "Internet of Things (IoT)",
        "Problem Solving",
    ]
    assert iot.prize_text is None
    assert iot.prize_amount_inr is None
    assert iot.extra == {
        "date_precision": "datetime",
        "min_team_size": 2,
        "max_team_size": 4,
        "registerCount": 30,
    }

    prize = events["593666"]
    assert prize.prize_text == _PRIZE
    assert prize.prize_amount_inr == 6000
    assert prize.organizer == "S.I.W.S College"

    points = events["1437224"]
    assert points.prize_text is None
    assert points.prize_amount_inr is None
    assert points.tags == [
        "C++ Programming Language",
        "Java",
        "Problem Solving",
        "Hackathon On D2C",
    ]
    assert points.extra is not None
    assert points.extra["registerCount"] == 52
    assert points.extra["min_team_size"] == 1
    assert points.extra["max_team_size"] == 1


def test_region_and_deadline_rules() -> None:
    item = _payload()["data"]["data"][0]
    assert isinstance(item, dict)
    item["region"] = "online"
    online = event_from_item(item, now=NOW)
    item["region"] = "india"
    unknown = event_from_item(item, now=NOW)
    item["region"] = "offline"
    open_until = event_from_item(item, now=datetime(2025, 2, 4, 18, 29, 51, tzinfo=UTC))
    closed_after = event_from_item(
        item, now=datetime(2025, 2, 4, 18, 29, 52, tzinfo=UTC)
    )
    item["regnRequirements"]["end_regn_dt"] = "soon"
    unparsed = event_from_item(item, now=NOW)
    del item["regnRequirements"]["end_regn_dt"]
    missing = event_from_item(item, now=NOW)

    assert online.mode is EventMode.ONLINE
    assert unknown.mode is EventMode.UNKNOWN
    assert open_until.lifecycle is Lifecycle.REGISTRATION_OPEN
    assert closed_after.lifecycle is Lifecycle.REGISTRATION_CLOSED
    assert unparsed.start_at is None
    assert unparsed.registration_deadline is None
    assert unparsed.lifecycle is Lifecycle.UNKNOWN
    assert missing.lifecycle is Lifecycle.UNKNOWN


def test_qualified_or_foreign_prize_text_does_not_invent_inr() -> None:
    item = _payload()["data"]["data"][0]
    assert isinstance(item, dict)
    item["details"] = (
        "<p>Made for the strong-willed, with prizes of up to INR 1 lakh, "
        "this is not an opportunity you want to miss.</p>"
    )
    capped = event_from_item(item, now=NOW)
    item["details"] = (
        "<p>The top 25 contestants will compete for the championship title "
        "and a cash prize of $15,000 USD.</p>"
    )
    dollars = event_from_item(item, now=NOW)

    assert capped.prize_text == (
        "Made for the strong-willed, with prizes of up to INR 1 lakh, "
        "this is not an opportunity you want to miss."
    )
    assert capped.prize_amount_inr is None
    assert dollars.prize_text == (
        "The top 25 contestants will compete for the championship title "
        "and a cash prize of $15,000 USD."
    )
    assert dollars.prize_amount_inr is None


def test_empty_page_stops_paging() -> None:
    body = _payload()
    delays: list[float] = []
    seen: list[tuple[str, str, str, str]] = []

    async def sleep(seconds: float) -> None:
        delays.append(seconds)

    def respond(request: httpx.Request) -> httpx.Response:
        params = request.url.params
        seen.append(
            (
                params.get("page", ""),
                params.get("opportunity", ""),
                params.get("per_page", ""),
                request.headers["user-agent"],
            )
        )
        if params.get("page") in (None, "1"):
            return httpx.Response(200, json=body)
        return httpx.Response(200, json={"data": {"data": []}})

    result = asyncio.run(_run(respond, sleep))

    assert seen == [
        ("1", "hackathons", "18", seen[0][3]),
        ("2", "hackathons", "18", seen[0][3]),
    ]
    assert seen[0][3].startswith("Mozilla/5.0")
    assert delays == [1.0]
    assert result.status == "ok"
    assert result.warning is None
    assert [event.external_id for event in result.events] == [
        "1369976",
        "593666",
        "1437224",
    ]


def test_one_bad_item_warns_and_keeps_the_rest() -> None:
    body = _payload()
    items = body["data"]["data"]
    assert isinstance(items, list)
    items[1]["title"] = None

    result = _collect(httpx.Response(200, json=body))

    assert result.status == "ok"
    assert [event.external_id for event in result.events] == ["1369976", "1437224"]
    assert result.warning == "item 593666: title is not a string"


def test_every_failed_item_is_malformed() -> None:
    body = _payload()
    items = body["data"]["data"]
    assert isinstance(items, list)
    for item in items:
        item["title"] = None

    result = _collect(httpx.Response(200, json=body))

    assert result.status == "malformed"
    assert result.events == []
    assert result.error == (
        "every item failed: "
        "item 1369976: title is not a string; "
        "item 593666: title is not a string; "
        "item 1437224: title is not a string"
    )


def test_page_cap_returns_ok_with_a_warning() -> None:
    delays: list[float] = []
    pages: list[str] = []

    async def sleep(seconds: float) -> None:
        delays.append(seconds)

    def respond(request: httpx.Request) -> httpx.Response:
        page = request.url.params.get("page", "")
        pages.append(page)
        item = _payload()["data"]["data"][0]
        assert isinstance(item, dict)
        item["id"] = int(page)
        item["title"] = f"Hack {page}"
        return httpx.Response(200, json={"data": {"data": [item]}})

    result = asyncio.run(_run(respond, sleep))

    assert pages == [str(page) for page in range(1, 11)]
    assert delays == [1.0] * 9
    assert result.status == "ok"
    assert result.warning == "stopped after 10 pages"
    assert [event.external_id for event in result.events] == [
        str(page) for page in range(1, 11)
    ]


async def _noop_sleep(_: float) -> None:
    return None


def _collect(response: httpx.Response) -> CollectResult:
    def respond(request: httpx.Request) -> httpx.Response:
        if request.url.params.get("page") not in (None, "1"):
            return httpx.Response(200, json={"data": {"data": []}})
        return response

    return asyncio.run(_run(respond, _noop_sleep))


def _run(
    respond: Callable[[httpx.Request], httpx.Response],
    sleep: Callable[[float], Awaitable[None]],
) -> Awaitable[CollectResult]:
    async def run() -> CollectResult:
        async with respx.mock:
            respx.get(url__startswith=SEARCH_URL).mock(side_effect=respond)
            return await UnstopCollector(now=NOW, sleep=sleep).collect()

    return run()
