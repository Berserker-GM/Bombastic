import asyncio
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import httpx
import respx

from radar.collectors.base import CollectResult
from radar.collectors.codeforces import CONTEST_LIST_URL, CodeforcesCollector
from radar.collectors.platforms import platform_slug
from radar.models.enums import EventCategory, Lifecycle

FIXTURE = Path(__file__).parent / "fixtures" / "codeforces_contest_list.json"


def _collect(response: httpx.Response) -> CollectResult:
    async def run() -> CollectResult:
        async with respx.mock:
            respx.get(CONTEST_LIST_URL).mock(return_value=response)
            return await CodeforcesCollector().collect()

    return asyncio.run(run())


def test_valid_response_is_ok() -> None:
    payload = json.loads(FIXTURE.read_text(encoding="utf-8"))
    result = _collect(httpx.Response(200, json=payload))

    assert result.status == "ok"
    assert result.error is None
    assert [event.external_id for event in result.events] == ["2148", "2149"]

    first = result.events[0]
    start_at = datetime.fromtimestamp(1792000000, UTC)
    assert first.source_name == "codeforces"
    assert first.title == "Codeforces Round 1048 (Div. 2)"
    assert first.category is EventCategory.CONTEST
    assert first.lifecycle is Lifecycle.UPCOMING
    assert first.start_at == start_at
    assert first.end_at == start_at + timedelta(seconds=7200)
    assert first.url == "https://codeforces.com/contest/2148"
    assert first.tags == [platform_slug("codeforces.com")]
    assert first.tags == ["codeforces"]
    assert first.confidence == {}
    assert first.evidence == []
    assert first.mode is None
    assert first.registration_deadline is None

    second = result.events[1]
    second_start = datetime.fromtimestamp(1792500000, UTC)
    assert second.end_at == second_start + timedelta(seconds=9000)


def test_http_500_is_unreachable() -> None:
    result = _collect(httpx.Response(500, text="unavailable"))

    assert result.status == "unreachable"
    assert result.events == []
    assert result.error is not None


def test_missing_result_is_malformed() -> None:
    result = _collect(httpx.Response(200, json={"status": "OK"}))

    assert result.status == "malformed"
    assert result.events == []
    assert result.error is not None
