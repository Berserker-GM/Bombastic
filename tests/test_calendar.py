from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from sqlalchemy import create_engine, func, select
from sqlalchemy import event as sa_event
from sqlalchemy.orm import Session
from sqlalchemy.pool import StaticPool

from radar.calendar.google import REMINDERS, GoogleCalendarService, google_event_body
from radar.models import (
    Base,
    CalendarLink,
    Event,
    EventCategory,
    HealthStatus,
    Lifecycle,
    Source,
    SourceKind,
    UserPrefs,
)

START = datetime(2026, 11, 1, 12, tzinfo=UTC)
OCT_15 = datetime(2026, 10, 15, tzinfo=UTC)
OCT_20 = datetime(2026, 10, 20, tzinfo=UTC)
USER = "user-1"


class FakeGoogleEvents:
    def __init__(self) -> None:
        self.inserted: list[dict[str, Any]] = []
        self.updated: list[tuple[str, dict[str, Any]]] = []
        self.deleted: list[str] = []
        self._next = 0

    def insert(self, body: dict[str, Any]) -> str:
        self._next += 1
        self.inserted.append(body)
        return f"gcal-{self._next}"

    def update(self, google_event_id: str, body: dict[str, Any]) -> None:
        self.updated.append((google_event_id, body))

    def delete(self, google_event_id: str) -> None:
        self.deleted.append(google_event_id)


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


def _source(session: Session) -> Source:
    source = Source(
        kind=SourceKind.API,
        name="college",
        config={},
        enabled=True,
        health_status=HealthStatus.HEALTHY,
        consecutive_failures=0,
    )
    session.add(source)
    session.flush()
    return source


def _event(session: Session, source: Source, **overrides: Any) -> Event:
    values: dict[str, Any] = {
        "source_id": source.id,
        "fingerprint": "notice",
        "title": "Registration",
        "category": EventCategory.COLLEGE,
        "lifecycle": Lifecycle.UPCOMING,
        "url": "https://college.example/notice",
        "registration_deadline": OCT_15,
        "first_seen_at": OCT_15,
        "last_seen_at": OCT_15,
        "content_hash": "hash-1",
    }
    values.update(overrides)
    event = Event(**values)
    session.add(event)
    session.flush()
    return event


def _prefs(session: Session, mode: str) -> None:
    session.add(UserPrefs(user_id=USER, prefs={"calendar_mode": mode}))
    session.flush()


def _link_count(session: Session) -> int:
    counted = session.scalar(select(func.count()).select_from(CalendarLink))
    assert counted is not None
    return counted


def _minutes(body: dict[str, Any]) -> list[int]:
    overrides = body["reminders"]["overrides"]
    return [item["minutes"] for item in overrides]


def test_create_skips_unchanged_then_updates_once(session: Session) -> None:
    source = _source(session)
    event = _event(session, source)
    _prefs(session, "auto")
    fake = FakeGoogleEvents()
    service = GoogleCalendarService(fake)

    created = service.upsert_event(session, USER, event)

    assert created is not None
    assert created.google_event_id == "gcal-1"
    assert created.last_synced_hash == "hash-1"
    assert len(fake.inserted) == 1
    assert fake.updated == []
    assert _minutes(fake.inserted[0]) == [3 * 24 * 60, 3 * 60]
    assert fake.inserted[0]["start"]["dateTime"] == OCT_15.isoformat()

    again = service.upsert_event(session, USER, event)

    assert again is created
    assert len(fake.inserted) == 1
    assert fake.updated == []
    assert _link_count(session) == 1

    event.registration_deadline = OCT_20
    event.content_hash = "hash-2"
    service.upsert_event(session, USER, event)

    assert len(fake.inserted) == 1
    assert len(fake.updated) == 1
    assert fake.updated[0][0] == "gcal-1"
    assert fake.updated[0][1]["start"]["dateTime"] == OCT_20.isoformat()
    assert created.last_synced_hash == "hash-2"
    assert _link_count(session) == 1


def test_confirm_mode_syncs_only_when_added(session: Session) -> None:
    source = _source(session)
    event = _event(session, source)
    _prefs(session, "confirm")
    fake = FakeGoogleEvents()
    service = GoogleCalendarService(fake)

    assert service.upsert_event(session, USER, event) is None
    assert fake.inserted == []

    added = service.add_to_calendar(session, USER, event.id)

    assert added is not None
    assert len(fake.inserted) == 1
    event.content_hash = "hash-2"
    assert service.upsert_event(session, USER, event) is added
    assert fake.updated == []

    service.add_to_calendar(session, USER, event.id)

    assert len(fake.inserted) == 1
    assert len(fake.updated) == 1


def test_delete_removes_the_google_event_once(session: Session) -> None:
    source = _source(session)
    event = _event(session, source)
    _prefs(session, "auto")
    fake = FakeGoogleEvents()
    service = GoogleCalendarService(fake)
    service.upsert_event(session, USER, event)

    service.delete(session, USER, event)
    service.delete(session, USER, event)

    assert fake.deleted == ["gcal-1"]
    assert _link_count(session) == 0


@pytest.mark.parametrize(
    ("category", "start_at", "end_at", "deadline", "minutes", "end"),
    [
        (
            EventCategory.CONTEST,
            START,
            START + timedelta(hours=2),
            None,
            (30,),
            START + timedelta(hours=2),
        ),
        (
            EventCategory.CONTEST,
            START,
            None,
            None,
            (30,),
            START + timedelta(minutes=REMINDERS["duration_minutes"]),
        ),
        (
            EventCategory.HACKATHON,
            START,
            START + timedelta(days=2),
            OCT_15,
            (24 * 60,),
            OCT_15 + timedelta(minutes=REMINDERS["duration_minutes"]),
        ),
    ],
)
def test_reminder_rules(
    category: EventCategory,
    start_at: datetime | None,
    end_at: datetime | None,
    deadline: datetime | None,
    minutes: tuple[int, ...],
    end: datetime,
) -> None:
    event = Event(
        source_id=1,
        fingerprint="fp",
        title="Item",
        category=category,
        lifecycle=Lifecycle.UPCOMING,
        start_at=start_at,
        end_at=end_at,
        registration_deadline=deadline,
        first_seen_at=START,
        last_seen_at=START,
        content_hash="hash",
    )

    body = google_event_body(event)

    assert _minutes(body) == list(minutes)
    assert body["end"]["dateTime"] == end.isoformat()
