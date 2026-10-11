from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from sqlalchemy import create_engine, func, select
from sqlalchemy import event as sa_event
from sqlalchemy.orm import Session
from sqlalchemy.pool import StaticPool

from radar.collectors.base import RawEvent
from radar.models import (
    Base,
    Event,
    EventCategory,
    HealthStatus,
    Lifecycle,
    Source,
    SourceKind,
)
from radar.pipeline.ingest import ingest

START = datetime(2026, 10, 1, tzinfo=UTC)


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


def _source(db: Session, name: str) -> Source:
    source = Source(
        kind=SourceKind.API,
        name=name,
        config={},
        enabled=True,
        health_status=HealthStatus.HEALTHY,
        consecutive_failures=0,
    )
    db.add(source)
    db.flush()
    return source


def _hackathon(source_name: str, **overrides: Any) -> RawEvent:
    values: dict[str, Any] = {
        "source_name": source_name,
        "title": "Winter of Code",
        "category": EventCategory.HACKATHON,
        "external_id": f"{source_name}-1",
        "url": f"https://{source_name}.example/winter",
        "start_at": START,
        "lifecycle": Lifecycle.REGISTRATION_OPEN,
    }
    values.update(overrides)
    return RawEvent(**values)


def _events(db: Session) -> list[Event]:
    return list(db.scalars(select(Event).order_by(Event.id)))


def test_matching_hackathons_from_two_sources_become_one_event(
    session: Session,
) -> None:
    devpost = _source(session, "devpost")
    unstop = _source(session, "unstop")
    ingest(
        session,
        devpost,
        [
            _hackathon(
                "devpost",
                title="Winter of Code Hackathon",
                url="https://winter.devpost.com/",
                location="Bengaluru, India",
                external_id="dp-1",
            )
        ],
    )
    ingest(
        session,
        unstop,
        [
            _hackathon(
                "unstop",
                title="Winter of Code 2026",
                url="https://unstop.com/hackathons/winter-of-code",
                start_at=START + timedelta(days=1),
                end_at=START + timedelta(days=20),
                organizer="IEEE",
                prize_text="INR 6000",
                tags=["AI"],
                external_id="un-9",
            )
        ],
    )

    events = _events(session)
    assert len(events) == 1
    event = events[0]
    assert event.source_id == devpost.id
    assert event.external_id == "dp-1"
    assert event.url == "https://unstop.com/hackathons/winter-of-code"
    assert event.location == "Bengaluru, India"
    assert event.organizer == "IEEE"
    assert event.prize_text == "INR 6000"
    assert event.start_at == START + timedelta(days=1)
    assert event.extra is not None
    assert event.extra["alt_sources"] == [
        {"source": "devpost", "url": "https://winter.devpost.com/"},
        {"source": "unstop", "url": "https://unstop.com/hackathons/winter-of-code"},
    ]

    ingest(
        session,
        devpost,
        [
            _hackathon(
                "devpost",
                title="Winter of Code Hackathon",
                url="https://winter.devpost.com/",
                location=None,
                external_id="dp-1",
            )
        ],
    )
    again = _events(session)
    assert len(again) == 1
    assert again[0].location == "Bengaluru, India"
    assert again[0].organizer == "IEEE"
    assert again[0].extra is not None
    assert again[0].extra["alt_sources"] == [
        {"source": "devpost", "url": "https://winter.devpost.com/"},
        {"source": "unstop", "url": "https://unstop.com/hackathons/winter-of-code"},
    ]


def test_same_name_in_a_different_year_stays_separate(session: Session) -> None:
    devpost = _source(session, "devpost")
    unstop = _source(session, "unstop")
    ingest(session, devpost, [_hackathon("devpost", title="Winter of Code 2026")])
    ingest(
        session,
        unstop,
        [
            _hackathon(
                "unstop",
                title="Winter of Code 2027",
                start_at=datetime(2027, 10, 1, tzinfo=UTC),
            )
        ],
    )

    assert len(_events(session)) == 2


def test_hack_the_north_and_hack_the_planet_stay_separate(session: Session) -> None:
    devpost = _source(session, "devpost")
    unstop = _source(session, "unstop")
    ingest(session, devpost, [_hackathon("devpost", title="Hack the North")])
    ingest(session, unstop, [_hackathon("unstop", title="Hack the Planet")])

    assert len(_events(session)) == 2


def test_contests_are_not_matched_across_sources(session: Session) -> None:
    codeforces = _source(session, "codeforces")
    clist = _source(session, "clist")
    ingest(
        session,
        codeforces,
        [_hackathon("codeforces", category=EventCategory.CONTEST, title="Round 100")],
    )
    ingest(
        session,
        clist,
        [_hackathon("clist", category=EventCategory.CONTEST, title="Round 100")],
    )

    assert len(_events(session)) == 2


def test_missing_start_or_two_candidates_stay_separate(session: Session) -> None:
    devpost = _source(session, "devpost")
    unstop = _source(session, "unstop")
    devfolio = _source(session, "devfolio")
    ingest(
        session, devpost, [_hackathon("devpost", title="Undated Cup", start_at=None)]
    )
    ingest(
        session,
        unstop,
        [_hackathon("unstop", title="Undated Cup Hackathon", start_at=START)],
    )
    assert len(_events(session)) == 2

    ingest(
        session,
        devpost,
        [
            _hackathon(
                "devpost",
                title="Split Meet",
                external_id="early",
                start_at=START,
                url="https://devpost.example/early",
            )
        ],
    )
    ingest(
        session,
        unstop,
        [
            _hackathon(
                "unstop",
                title="Split Meet",
                external_id="late",
                start_at=START + timedelta(days=4),
                url="https://unstop.example/late",
            )
        ],
    )
    before = len(_events(session))
    ingest(
        session,
        devfolio,
        [
            _hackathon(
                "devfolio",
                title="Split Meet Hackathon",
                start_at=START + timedelta(days=2),
                url="https://split.devfolio.co/",
            )
        ],
    )

    assert len(_events(session)) == before + 1
    counted = session.scalar(select(func.count()).select_from(Event))
    assert counted == before + 1
