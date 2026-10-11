import asyncio
import json
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import httpx
import respx

from radar.collectors.base import CollectResult
from radar.collectors.devfolio import HACKATHONS_URL, DevfolioCollector, event_from_item
from radar.models.enums import EventMode, Lifecycle

FIXTURE = Path(__file__).parent / "fixtures" / "devfolio_hackathons.json"
NOW = datetime(2026, 10, 11, tzinfo=UTC)


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
    winter = events["d959fbb33372440883f65f9ff9054982"]
    assert winter.source_name == "devfolio"
    assert winter.title == "Winter of Code 2026"
    assert winter.url == "https://winter-of-code-4.devfolio.co/"
    assert winter.start_at == datetime(2026, 10, 14, 18, 30, tzinfo=UTC)
    assert winter.end_at == datetime(2026, 11, 30, 18, 30, tzinfo=UTC)
    assert winter.registration_deadline == datetime(2026, 10, 13, 18, 30, tzinfo=UTC)
    assert winter.mode is EventMode.ONLINE
    assert winter.location is None
    assert winter.lifecycle is Lifecycle.REGISTRATION_OPEN
    assert winter.tags == ["No Restrictions"]
    assert winter.prize_text is None
    assert winter.prize_amount_inr is None
    assert winter.eligibility is None
    assert winter.extra == {
        "date_precision": "datetime",
        "team_min": 1,
        "team_size": 1,
    }

    storm = events["69bc320cd0ff44f4bf84e9facd534485"]
    assert storm.tags == ["Design", "AI", "Future Mobility"]
    assert storm.extra is not None
    assert storm.extra["team_size"] == 6

    bios = events["9355584ad9e543b5a90f602a27a596b4"]
    assert bios.url == "https://biosv2.devfolio.co/"
    assert bios.mode is EventMode.OFFLINE
    assert bios.location == "Patiala, India"
    assert bios.extra is not None
    assert bios.extra["team_min"] == 4
    assert bios.extra["team_size"] == 4


def test_deadline_location_and_id_fallbacks() -> None:
    item = _payload()["result"][0]
    assert isinstance(item, dict)
    open_until = event_from_item(item, now=datetime(2026, 10, 13, 18, 30, tzinfo=UTC))
    closed_after = event_from_item(
        item, now=datetime(2026, 10, 13, 18, 30, 1, tzinfo=UTC)
    )
    item["hackathon_setting"]["reg_ends_at"] = "soon"
    unparsed = event_from_item(item, now=NOW)
    del item["hackathon_setting"]
    missing = event_from_item(item, now=NOW)
    item["city"] = None
    item["country"] = "India"
    country_only = event_from_item(item, now=NOW)
    item["is_online"] = None
    unknown_mode = event_from_item(item, now=NOW)
    del item["uuid"]
    item["id"] = "legacy-id"
    fallback = event_from_item(item, now=NOW)

    assert open_until.lifecycle is Lifecycle.REGISTRATION_OPEN
    assert closed_after.lifecycle is Lifecycle.REGISTRATION_CLOSED
    assert unparsed.registration_deadline is None
    assert unparsed.lifecycle is Lifecycle.UNKNOWN
    assert unparsed.start_at == datetime(2026, 10, 14, 18, 30, tzinfo=UTC)
    assert missing.lifecycle is Lifecycle.UNKNOWN
    assert country_only.location == "India"
    assert unknown_mode.mode is EventMode.UNKNOWN
    assert fallback.external_id == "legacy-id"


def test_empty_page_stops_paging() -> None:
    body = _payload()
    delays: list[float] = []
    seen: list[tuple[str, str, str]] = []

    async def sleep(seconds: float) -> None:
        delays.append(seconds)

    def respond(request: httpx.Request) -> httpx.Response:
        params = request.url.params
        seen.append(
            (
                params.get("page", ""),
                params.get("limit", ""),
                params.get("filter", ""),
            )
        )
        if params.get("page") in (None, "1"):
            return httpx.Response(200, json=body)
        return httpx.Response(200, json={"result": []})

    result = asyncio.run(_run(respond, sleep))

    assert seen == [("1", "20", "application_open"), ("2", "20", "application_open")]
    assert delays == [1.0]
    assert result.status == "ok"
    assert result.warning is None
    assert len(result.events) == 3


def test_one_bad_item_warns_and_keeps_the_rest() -> None:
    body = _payload()
    items = body["result"]
    assert isinstance(items, list)
    items[1]["name"] = None

    result = _collect(httpx.Response(200, json=body))

    assert result.status == "ok"
    assert [event.external_id for event in result.events] == [
        "d959fbb33372440883f65f9ff9054982",
        "9355584ad9e543b5a90f602a27a596b4",
    ]
    assert result.warning == (
        "item 69bc320cd0ff44f4bf84e9facd534485: name is not a string"
    )


def test_every_failed_item_is_malformed() -> None:
    body = _payload()
    items = body["result"]
    assert isinstance(items, list)
    for item in items:
        item["name"] = None

    result = _collect(httpx.Response(200, json=body))

    assert result.status == "malformed"
    assert result.events == []
    assert result.error == (
        "every item failed: "
        "item d959fbb33372440883f65f9ff9054982: name is not a string; "
        "item 69bc320cd0ff44f4bf84e9facd534485: name is not a string; "
        "item 9355584ad9e543b5a90f602a27a596b4: name is not a string"
    )


def test_page_cap_returns_ok_with_a_warning() -> None:
    delays: list[float] = []
    pages: list[str] = []

    async def sleep(seconds: float) -> None:
        delays.append(seconds)

    def respond(request: httpx.Request) -> httpx.Response:
        page = request.url.params.get("page", "")
        pages.append(page)
        item = _payload()["result"][0]
        assert isinstance(item, dict)
        item["uuid"] = page
        item["name"] = f"Hack {page}"
        return httpx.Response(200, json={"result": [item]})

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
            return httpx.Response(200, json={"result": []})
        return response

    return asyncio.run(_run(respond, _noop_sleep))


def _run(
    respond: Callable[[httpx.Request], httpx.Response],
    sleep: Callable[[float], Awaitable[None]],
) -> Awaitable[CollectResult]:
    async def run() -> CollectResult:
        async with respx.mock:
            respx.get(url__startswith=HACKATHONS_URL).mock(side_effect=respond)
            return await DevfolioCollector(now=NOW, sleep=sleep).collect()

    return run()
