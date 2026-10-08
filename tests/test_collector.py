import asyncio

import httpx
import pytest

from radar.collectors.base import Collector, RawEvent
from radar.models.enums import EventCategory


class _Empty(Collector):
    min_expected = 1

    async def fetch(self) -> list[RawEvent]:
        return []


class _Quiet(Collector):
    async def fetch(self) -> list[RawEvent]:
        return []


class _Timeout(Collector):
    async def fetch(self) -> list[RawEvent]:
        raise httpx.ReadTimeout("timed out")


def test_min_expected_marks_suspiciously_empty() -> None:
    result = asyncio.run(_Empty().collect())

    assert result.status == "malformed"
    assert result.error == "suspiciously empty"


def test_empty_without_min_expected_is_ok() -> None:
    result = asyncio.run(_Quiet().collect())

    assert result.status == "ok"
    assert result.events == []
    assert result.error is None


@pytest.mark.parametrize("status_code", [403, 429])
def test_forbidden_and_rate_limit_are_unreachable(status_code: int) -> None:
    class _Denied(Collector):
        async def fetch(self) -> list[RawEvent]:
            request = httpx.Request("GET", "https://example.com/contests")
            response = httpx.Response(status_code, request=request)
            raise httpx.HTTPStatusError("nope", request=request, response=response)

    result = asyncio.run(_Denied().collect())

    assert result.status == "unreachable"
    assert result.events == []


def test_timeout_is_unreachable() -> None:
    result = asyncio.run(_Timeout().collect())

    assert result.status == "unreachable"
    assert result.events == []
    assert result.error is not None


def test_raw_event_requires_source_name_title_and_category() -> None:
    event = RawEvent(
        source_name="codeforces",
        title="Round",
        category=EventCategory.CONTEST,
    )

    assert event.confidence == {}
    assert event.evidence == []
    assert event.external_id is None
