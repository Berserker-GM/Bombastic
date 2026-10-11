import asyncio
import json
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import httpx
import pytest
import respx
from sqlalchemy import create_engine, event, func, select
from sqlalchemy.orm import Session
from sqlalchemy.pool import StaticPool

from radar.cli.__main__ import scan_codeforces
from radar.collectors.codeforces import CONTEST_LIST_URL
from radar.models import Base, Event, SourceResult

FIXTURE = Path(__file__).parent / "fixtures" / "codeforces_contest_list.json"


@pytest.fixture
def session() -> Iterator[Session]:
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )

    @event.listens_for(engine, "connect")
    def _enable_foreign_keys(dbapi_connection: Any, _: Any) -> None:
        cursor = dbapi_connection.cursor()
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.close()

    Base.metadata.create_all(engine)
    with Session(engine) as db_session:
        yield db_session
        db_session.rollback()


def test_devpost_scan_is_registered() -> None:
    from radar.cli.__main__ import _SCANNERS

    assert "devpost" in _SCANNERS


def test_scan_devpost_ingests_fixture(session: Session) -> None:
    from radar.cli.__main__ import _SCANNERS
    from radar.collectors.devpost import HACKATHONS_URL

    payload = json.loads(
        (Path(__file__).parent / "fixtures" / "devpost_hackathons.json").read_text(
            encoding="utf-8"
        )
    )

    def respond(request: httpx.Request) -> httpx.Response:
        if request.url.params.get("page") in (None, "1"):
            return httpx.Response(200, json=payload)
        return httpx.Response(200, json={"hackathons": []})

    async def run() -> SourceResult:
        async with respx.mock:
            respx.get(url__startswith=HACKATHONS_URL).mock(side_effect=respond)
            outcome = await _SCANNERS["devpost"](session)
        return outcome.result

    assert asyncio.run(run()) is SourceResult.OK_NEW
    counted = session.scalar(select(func.count()).select_from(Event))
    assert counted == 3


def test_unstop_scan_is_registered() -> None:
    from radar.cli.__main__ import _SCANNERS

    assert "unstop" in _SCANNERS


def test_scan_unstop_ingests_fixture(session: Session) -> None:
    from radar.cli.__main__ import _SCANNERS
    from radar.collectors.unstop import SEARCH_URL

    payload = json.loads(
        (Path(__file__).parent / "fixtures" / "unstop_hackathons.json").read_text(
            encoding="utf-8"
        )
    )

    def respond(request: httpx.Request) -> httpx.Response:
        if request.url.params.get("page") in (None, "1"):
            return httpx.Response(200, json=payload)
        return httpx.Response(200, json={"data": {"data": []}})

    async def run() -> SourceResult:
        async with respx.mock:
            respx.get(url__startswith=SEARCH_URL).mock(side_effect=respond)
            outcome = await _SCANNERS["unstop"](session)
        return outcome.result

    assert asyncio.run(run()) is SourceResult.OK_NEW
    counted = session.scalar(select(func.count()).select_from(Event))
    assert counted == 3


def test_devfolio_scan_is_registered() -> None:
    from radar.cli.__main__ import _SCANNERS

    assert "devfolio" in _SCANNERS


def test_scan_devfolio_ingests_fixture(session: Session) -> None:
    from radar.cli.__main__ import _SCANNERS
    from radar.collectors.devfolio import HACKATHONS_URL

    payload = json.loads(
        (Path(__file__).parent / "fixtures" / "devfolio_hackathons.json").read_text(
            encoding="utf-8"
        )
    )

    def respond(request: httpx.Request) -> httpx.Response:
        if request.url.params.get("page") in (None, "1"):
            return httpx.Response(200, json=payload)
        return httpx.Response(200, json={"result": []})

    async def run() -> SourceResult:
        async with respx.mock:
            respx.get(url__startswith=HACKATHONS_URL).mock(side_effect=respond)
            outcome = await _SCANNERS["devfolio"](session)
        return outcome.result

    assert asyncio.run(run()) is SourceResult.OK_NEW
    counted = session.scalar(select(func.count()).select_from(Event))
    assert counted == 3


def test_scan_codeforces_ingests_fixture(session: Session) -> None:
    payload = json.loads(FIXTURE.read_text(encoding="utf-8"))

    async def run() -> SourceResult:
        async with respx.mock:
            respx.get(CONTEST_LIST_URL).mock(
                return_value=httpx.Response(200, json=payload)
            )
            outcome = await scan_codeforces(session)
        return outcome.result

    assert asyncio.run(run()) is SourceResult.OK_NEW
    counted = session.scalar(select(func.count()).select_from(Event))
    assert counted == 2
