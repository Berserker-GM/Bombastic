import asyncio
import json
import logging
from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import httpx
import pytest
import respx
from sqlalchemy import create_engine, func, select
from sqlalchemy import event as sa_event
from sqlalchemy.orm import Session
from sqlalchemy.pool import StaticPool

from radar.cli.__main__ import scan_clist
from radar.collectors.base import CollectResult
from radar.collectors.clist import CONTEST_URL, ClistCollector, events_from_contests
from radar.collectors.platforms import platform_slug
from radar.models import Base, Event, SourceResult
from radar.models.enums import EventCategory, Lifecycle

FIXTURE = Path(__file__).parent / "fixtures" / "clist_contest_page.json"
_ORIGINAL_IDS = {63529573, 66453358}
_NOW = datetime(2026, 10, 8, tzinfo=UTC)


@pytest.fixture
def clist_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("CLIST_API_KEY", "test-key")
    monkeypatch.setenv("CLIST_USERNAME", "test-user")


def _fixture() -> dict[str, Any]:
    payload = json.loads(FIXTURE.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise TypeError("fixture is not an object")
    return payload


def _contest(contest_id: int, host: str, *, title: str = "Contest") -> dict[str, Any]:
    return {
        "id": contest_id,
        "event": title,
        "host": host,
        "resource": host,
        "resource_id": contest_id,
        "href": f"https://{host}/contest/{contest_id}",
        "start": "2099-06-01T00:00:00",
        "end": "2099-06-01T02:00:00",
        "duration": 7200,
    }


def _run(
    handler: Any,
    *,
    sleep: Any = None,
    page_limit: int = 100,
) -> tuple[CollectResult, list[httpx.Request]]:
    async def run() -> tuple[CollectResult, list[httpx.Request]]:
        async with respx.mock:
            route = respx.get(CONTEST_URL).mock(side_effect=handler)
            result = await ClistCollector(sleep=sleep, page_limit=page_limit).collect()
            return result, [call.request for call in route.calls]

    return asyncio.run(run())


@pytest.mark.parametrize(
    ("host", "slug"),
    [
        ("codechef.com", "codechef"),
        ("atcoder.jp", "atcoder"),
        ("leetcode.com", "leetcode"),
        ("hackerrank.com", "hackerrank"),
        ("hackerearth.com", "hackerearth"),
        ("codeforces.com", "codeforces"),
        ("WWW.CodeChef.com", "codechef"),
    ],
)
def test_platform_slug(host: str, slug: str) -> None:
    assert platform_slug(host) == slug


def test_fixture_keeps_listed_upcoming_hosts(clist_env: None) -> None:
    payload = _fixture()
    hosts = [contest["host"] for contest in payload["objects"]]
    assert hosts[:2] == ["datsteam.dev", "kaggle.com"]

    result, requests = _run(lambda _request: httpx.Response(200, json=payload))

    assert result.status == "ok"
    assert result.error is None
    assert [event.external_id for event in result.events] == ["9001", "9002", "9003"]
    assert len(requests) == 1
    requested = requests[0]
    assert requested.url.params["offset"] == "0"
    assert requested.url.params["username"] == "test-user"
    assert requested.url.params["api_key"] == "test-key"
    assert "api_key=REDACTED" not in str(requested.url)
    assert "username=REDACTED" not in str(requested.url)

    codechef = result.events[0]
    assert codechef.source_name == "clist"
    assert codechef.title == "CodeChef Starters 200"
    assert codechef.category is EventCategory.CONTEST
    assert codechef.lifecycle is Lifecycle.UPCOMING
    assert codechef.tags == [platform_slug("codechef.com")]
    assert codechef.tags == ["codechef"]
    assert codechef.url == "https://www.codechef.com/START200"
    assert codechef.start_at == datetime(2099, 6, 1, 14, 30, tzinfo=UTC)
    assert codechef.end_at == datetime(2099, 6, 1, 17, 0, tzinfo=UTC)
    assert codechef.confidence == {}
    assert codechef.evidence == []
    assert result.events[1].tags == [platform_slug("atcoder.jp")]
    assert result.events[1].tags == ["atcoder"]
    assert result.events[2].tags == [platform_slug("leetcode.com")]
    assert result.events[2].tags == ["leetcode"]


def test_unlisted_hosts_are_malformed(clist_env: None) -> None:
    payload = _fixture()
    payload["objects"] = [
        contest for contest in payload["objects"] if contest["id"] in _ORIGINAL_IDS
    ]
    assert len(payload["objects"]) == 2

    result, _requests = _run(lambda _request: httpx.Response(200, json=payload))

    assert result.status == "malformed"
    assert result.error == "suspiciously empty"
    assert result.events == []


def test_other_allowed_hosts_use_platform_slug() -> None:
    contests = [
        _contest(1, "hackerrank.com", title="HR Cup"),
        _contest(2, "hackerearth.com", title="HE Cup"),
        _contest(3, "kaggle.com", title="Other"),
    ]

    events = events_from_contests(contests, now=_NOW)

    assert [event.tags for event in events] == [["hackerrank"], ["hackerearth"]]
    assert [event.external_id for event in events] == ["1", "2"]


def test_started_contest_is_skipped() -> None:
    contest = _contest(1, "codechef.com")
    contest["start"] = "2020-01-01T00:00:00"

    assert events_from_contests([contest], now=_NOW) == []


@pytest.mark.parametrize("status_code", [401, 403])
def test_auth_rejected_is_unreachable(clist_env: None, status_code: int) -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(status_code, text="rejected test-key test-user")

    result, requests = _run(handler)

    assert result.status == "unreachable"
    assert result.error == "CLIST auth rejected"
    assert result.events == []
    assert len(requests) == 1


def test_rate_limit_is_unreachable(clist_env: None) -> None:
    delays: list[float] = []

    async def record(seconds: float) -> None:
        delays.append(seconds)

    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(429, text="slow down test-key test-user")

    result, requests = _run(handler, sleep=record)

    assert result.status == "unreachable"
    assert result.error == "CLIST rate limit"
    assert result.events == []
    assert len(requests) == 4
    assert len(delays) == 3
    assert all(delay >= 6.5 for delay in delays)


def test_retries_429_then_returns_events(clist_env: None) -> None:
    calls = 0

    async def noop(_seconds: float) -> None:
        return None

    def handler(_request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        if calls <= 3:
            return httpx.Response(429, text="slow down")
        return httpx.Response(200, json=_fixture())

    result, requests = _run(handler, sleep=noop)

    assert calls == 4
    assert len(requests) == 4
    assert result.status == "ok"
    assert [event.external_id for event in result.events] == ["9001", "9002", "9003"]


def test_paginates_by_offset_not_meta_next(clist_env: None) -> None:
    delays: list[float] = []

    async def record(seconds: float) -> None:
        delays.append(seconds)

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.params["username"] == "test-user"
        assert request.url.params["api_key"] == "test-key"
        offset = int(request.url.params["offset"])
        limit = int(request.url.params["limit"])
        trap = "/api/v4/contest/?username=leak-user&api_key=leak-key&offset=999"
        if offset == 0:
            objects = [_contest(index, "codechef.com") for index in range(limit)]
            return httpx.Response(
                200,
                json={
                    "meta": {
                        "next": trap,
                        "total_count": None,
                        "estimated_count": 1,
                    },
                    "objects": objects,
                },
            )
        if offset == limit:
            return httpx.Response(
                200,
                json={
                    "meta": {"next": trap, "total_count": None},
                    "objects": [_contest(10_000, "atcoder.jp")],
                },
            )
        raise AssertionError(offset)

    result, requests = _run(handler, sleep=record, page_limit=2)

    assert [int(request.url.params["offset"]) for request in requests] == [0, 2]
    assert all("leak-key" not in str(request.url) for request in requests)
    assert all("leak-user" not in str(request.url) for request in requests)
    assert result.status == "ok"
    assert result.error is None
    assert len(result.events) == 3
    assert len(delays) == 1
    assert delays[0] >= 6.5


def test_stops_after_20_pages(clist_env: None) -> None:
    delays: list[float] = []

    async def record(seconds: float) -> None:
        delays.append(seconds)

    def handler(request: httpx.Request) -> httpx.Response:
        offset = int(request.url.params["offset"])
        return httpx.Response(
            200,
            json={
                "meta": {
                    "next": "/api/v4/contest/?username=leak-user&api_key=leak-key",
                    "total_count": None,
                    "estimated_count": 1000,
                },
                "objects": [_contest(offset + 1, "leetcode.com")],
            },
        )

    result, requests = _run(handler, sleep=record, page_limit=1)

    assert len(requests) == 20
    assert [int(request.url.params["offset"]) for request in requests] == list(
        range(20)
    )
    assert len(delays) == 19
    assert all(delay >= 6.5 for delay in delays)
    assert result.status == "ok"
    assert len(result.events) == 20
    assert all("leak-key" not in str(request.url) for request in requests)


@pytest.mark.parametrize(
    "payload",
    [
        {"meta": {"next": None, "total_count": None}},
        {"objects": "nope"},
        {
            "objects": [
                {
                    "id": 1,
                    "event": "Broken",
                    "host": {"name": "codechef.com"},
                    "resource": "codechef.com",
                    "resource_id": 1,
                    "href": "https://www.codechef.com/x",
                    "start": "2099-01-01T00:00:00",
                    "end": "2099-01-01T01:00:00",
                }
            ]
        },
    ],
)
def test_unexpected_shape_is_malformed(
    clist_env: None, payload: dict[str, Any]
) -> None:
    result, _requests = _run(lambda _request: httpx.Response(200, json=payload))

    assert result.status == "malformed"
    assert result.events == []
    assert result.error is not None


def test_errors_and_logs_redact_credentials(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    monkeypatch.setenv("CLIST_API_KEY", "key/secret")
    monkeypatch.setenv("CLIST_USERNAME", "user name")
    caplog.set_level(logging.DEBUG)

    result, requests = _run(lambda _request: httpx.Response(500, text="unavailable"))
    logging.getLogger("httpx").warning("url %s", requests[0].url)
    logging.getLogger("httpcore").warning("url %s", requests[0].url)

    assert result.status == "unreachable"
    assert result.error is not None
    assert requests[0].url.params["api_key"] == "key/secret"
    assert requests[0].url.params["username"] == "user name"
    blob = result.error + "\n" + caplog.text
    for secret in (
        "key/secret",
        "key%2Fsecret",
        "key%2fsecret",
        "user name",
        "user+name",
        "user%20name",
    ):
        assert secret not in blob
    assert "api_key=REDACTED" in result.error
    assert "username=REDACTED" in result.error
    assert "REDACTED" in caplog.text
    assert logging.getLogger("httpx").level == logging.WARNING
    assert logging.getLogger("httpcore").level == logging.WARNING


def test_fixtures_only_contain_redacted_api_keys() -> None:
    needle = "api_key="
    fixtures = Path(__file__).parent / "fixtures"
    for path in fixtures.iterdir():
        if not path.is_file():
            continue
        text = path.read_text(encoding="utf-8")
        start = 0
        while True:
            index = text.find(needle, start)
            if index < 0:
                break
            assert text.startswith("REDACTED", index + len(needle)), path.name
            start = index + len(needle)


def test_missing_api_key_is_malformed(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "radar.collectors.clist.get_settings",
        lambda: type(
            "Settings",
            (),
            {"clist_api_key": None, "clist_username": None},
        )(),
    )

    async def run() -> CollectResult:
        async with respx.mock:
            route = respx.get(CONTEST_URL).mock(return_value=httpx.Response(500))
            result = await ClistCollector().collect()
            assert not route.called
            return result

    result = asyncio.run(run())

    assert result.status == "malformed"
    assert result.events == []
    assert result.error is not None


@pytest.fixture
def session() -> Iterator[Session]:
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )

    @sa_event.listens_for(engine, "connect")
    def _enable_foreign_keys(dbapi_connection: Any, _: Any) -> None:
        cursor = dbapi_connection.cursor()
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.close()

    Base.metadata.create_all(engine)
    with Session(engine) as db_session:
        yield db_session
        db_session.rollback()


def test_scan_clist_ingests_fixture(session: Session, clist_env: None) -> None:
    async def run() -> SourceResult:
        async with respx.mock:
            respx.get(CONTEST_URL).mock(
                return_value=httpx.Response(200, json=_fixture())
            )
            outcome = await scan_clist(session)
        return outcome.result

    assert asyncio.run(run()) is SourceResult.OK_NEW
    counted = session.scalar(select(func.count()).select_from(Event))
    assert counted == 3
