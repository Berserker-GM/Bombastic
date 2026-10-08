from collections.abc import Iterator
from datetime import UTC, datetime
from typing import Any

import pytest
from sqlalchemy import create_engine, event
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session
from sqlalchemy.pool import StaticPool

from radar.models import (
    Base,
    Event,
    EventCategory,
    EventEvidence,
    EventVersion,
    HealthStatus,
    Importance,
    Lifecycle,
    Source,
    SourceKind,
    Urgency,
    UserEventState,
    UserState,
)


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


def _source() -> Source:
    return Source(
        kind=SourceKind.API,
        name="Codeforces",
        config={},
        enabled=True,
        health_status=HealthStatus.HEALTHY,
        consecutive_failures=0,
    )


def _event(source: Source, external_id: str, content_hash: str) -> Event:
    now = datetime.now(UTC)
    return Event(
        source=source,
        external_id=external_id,
        fingerprint=content_hash,
        category=EventCategory.CONTEST,
        title="Round",
        lifecycle=Lifecycle.REGISTRATION_OPEN,
        confidence={"start_at": 1.0},
        first_seen_at=now,
        last_seen_at=now,
        content_hash=content_hash,
    )


def test_insert_event_version_and_evidence(session: Session) -> None:
    now = datetime.now(UTC)
    source = _source()
    event = _event(source, "cf-1900", "hash-1")
    version = EventVersion(
        event=event,
        seen_at=now,
        changed_fields={"title": [None, "Round"]},
        importance=Importance.IMPORTANT,
        snapshot={"title": "Round"},
    )
    evidence = EventEvidence(
        event=event,
        field="start_at",
        source_url="https://codeforces.com/contest/1900",
        source_text="Contest starts at 2026-10-08 14:35 UTC",
        confidence=0.95,
        captured_at=now,
    )
    session.add_all([source, event, version, evidence])
    session.commit()

    assert event.id is not None
    assert version.event_id == event.id
    assert version.importance is Importance.IMPORTANT
    assert evidence.event_id == event.id
    assert event.lifecycle is Lifecycle.REGISTRATION_OPEN
    assert event.confidence == {"start_at": 1.0}


def test_unique_source_external_id_rejects_duplicate(session: Session) -> None:
    source = _source()
    session.add(source)
    session.add(_event(source, "cf-1900", "hash-1"))
    session.add(_event(source, "cf-1900", "hash-2"))

    with pytest.raises(IntegrityError):
        session.commit()
    session.rollback()


def test_saved_state_is_independent_of_lifecycle(session: Session) -> None:
    source = _source()
    event = _event(source, "cf-1900", "hash-1")
    state = UserEventState(
        user_id="user-1",
        event=event,
        state=UserState.SAVED,
        relevance=80,
        urgency=Urgency.SOON,
        reasons=["topic match"],
    )
    session.add_all([source, event, state])
    session.commit()

    assert event.lifecycle is Lifecycle.REGISTRATION_OPEN
    assert state.state is UserState.SAVED
