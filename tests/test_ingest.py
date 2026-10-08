from collections.abc import Iterator
from datetime import UTC, datetime
from typing import Any

import pytest
from sqlalchemy import create_engine, func, select
from sqlalchemy import event as sa_event
from sqlalchemy.orm import Session
from sqlalchemy.pool import StaticPool

from radar.collectors.base import CollectResult, RawEvent
from radar.models import (
    Base,
    Event,
    EventCategory,
    EventVersion,
    HealthStatus,
    Importance,
    Lifecycle,
    Source,
    SourceKind,
    SourceResult,
)
from radar.pipeline.ingest import ingest, ingest_run

OCT_15 = datetime(2026, 10, 15, tzinfo=UTC)
OCT_20 = datetime(2026, 10, 20, tzinfo=UTC)
START = datetime(2026, 11, 1, 12, tzinfo=UTC)


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
        name="codeforces",
        config={},
        enabled=True,
        health_status=HealthStatus.HEALTHY,
        consecutive_failures=0,
    )
    session.add(source)
    session.flush()
    return source


def _raw(**overrides: Any) -> RawEvent:
    values: dict[str, Any] = {
        "source_name": "codeforces",
        "title": "Round",
        "category": EventCategory.CONTEST,
        "external_id": "100",
        "lifecycle": Lifecycle.UPCOMING,
        "start_at": START,
        "url": "https://codeforces.com/contest/100",
        "tags": ["codeforces"],
    }
    values.update(overrides)
    return RawEvent(**values)


def _count(session: Session, model: type[Event] | type[EventVersion]) -> int:
    counted = session.scalar(select(func.count()).select_from(model))
    assert counted is not None
    return counted


def test_first_ingest_creates_events(session: Session) -> None:
    source = _source(session)
    result = ingest(
        session,
        source,
        [_raw(external_id="1", title="One"), _raw(external_id="2", title="Two")],
    )

    assert len(result.new) == 2
    assert result.changed == []
    assert result.unchanged == []
    assert _count(session, Event) == 2
    assert _count(session, EventVersion) == 2


def test_identical_reingest_is_unchanged(session: Session) -> None:
    source = _source(session)
    raw_events = [
        _raw(external_id="1", title="One"),
        _raw(external_id="2", title="Two"),
    ]
    first = ingest(session, source, raw_events)
    for row in first.new:
        row.last_seen_at = datetime(2020, 1, 1, tzinfo=UTC)
    first_seen = {event.id: event.first_seen_at for event in first.new}
    session.flush()

    second = ingest(session, source, raw_events)

    assert second.new == []
    assert second.changed == []
    assert len(second.unchanged) == 2
    assert _count(session, Event) == 2
    assert _count(session, EventVersion) == 2
    for event in second.unchanged:
        assert event.last_seen_at > datetime(2020, 1, 1, tzinfo=UTC)
        assert event.first_seen_at == first_seen[event.id]


def test_deadline_change_is_one_important_version(session: Session) -> None:
    source = _source(session)
    ingest(session, source, [_raw(registration_deadline=OCT_15)])
    ingest(session, source, [_raw(registration_deadline=OCT_20)])

    versions = session.scalars(select(EventVersion)).all()
    deadline_versions = [
        version
        for version in versions
        if set(version.changed_fields) == {"registration_deadline"}
    ]
    assert len(deadline_versions) == 1
    assert deadline_versions[0].changed_fields == {
        "registration_deadline": [OCT_15.isoformat(), OCT_20.isoformat()]
    }
    assert deadline_versions[0].importance is Importance.IMPORTANT
    assert _count(session, Event) == 1


def test_tag_change_is_minor(session: Session) -> None:
    source = _source(session)
    ingest(session, source, [_raw(tags=["codeforces"])])
    ingest(session, source, [_raw(tags=["codeforces", "algo"])])

    versions = session.scalars(select(EventVersion)).all()
    tag_versions = [
        version for version in versions if set(version.changed_fields) == {"tags"}
    ]
    assert len(tag_versions) == 1
    assert tag_versions[0].changed_fields == {
        "tags": [["codeforces"], ["codeforces", "algo"]]
    }
    assert tag_versions[0].importance is Importance.MINOR
    assert _count(session, Event) == 1


def test_hand_edited_deadline_is_important_on_reingest(session: Session) -> None:
    source = _source(session)
    raw = _raw(registration_deadline=OCT_15)
    created = ingest(session, source, [raw])
    event = created.new[0]
    event.registration_deadline = OCT_20
    session.flush()

    second = ingest(session, source, [raw])

    versions = session.scalars(select(EventVersion)).all()
    deadline_versions = [
        version
        for version in versions
        if version.changed_fields.get("registration_deadline")
        == [OCT_20.isoformat(), OCT_15.isoformat()]
    ]
    assert second.new == []
    assert [item.id for item in second.changed] == [event.id]
    assert len(deadline_versions) == 1
    assert deadline_versions[0].importance is Importance.IMPORTANT
    assert event.registration_deadline == OCT_15
    assert _count(session, Event) == 1


def test_title_case_and_whitespace_are_not_a_change(session: Session) -> None:
    source = _source(session)
    first = ingest(session, source, [_raw(title="  Round   One ")])
    versions_before = _count(session, EventVersion)

    second = ingest(session, source, [_raw(title="round one")])

    assert first.new[0].title == "Round One"
    assert second.new == []
    assert second.changed == []
    assert len(second.unchanged) == 1
    assert second.unchanged[0].title == "Round One"
    assert _count(session, EventVersion) == versions_before
    assert _count(session, Event) == 1


def test_fingerprint_matches_title_punctuation(session: Session) -> None:
    source = _source(session)
    first = ingest(
        session,
        source,
        [_raw(title="City Hackathon", external_id=None, start_at=START)],
    )
    second = ingest(
        session,
        source,
        [_raw(title="City Hackathon!", external_id=None, start_at=START)],
    )

    assert second.new == []
    assert [event.id for event in second.changed] == [first.new[0].id]
    assert _count(session, Event) == 1
    assert first.new[0].fingerprint == second.changed[0].fingerprint


def test_unreachable_does_not_touch_events(session: Session) -> None:
    source = _source(session)
    created = ingest(session, source, [_raw()])
    event = created.new[0]
    seen = event.last_seen_at
    title = event.title
    event_count = _count(session, Event)
    version_count = _count(session, EventVersion)

    result = ingest_run(
        session,
        source,
        CollectResult(
            status="unreachable",
            events=[_raw(title="Brand new", external_id="999")],
            error="timeout",
        ),
    )

    assert result.result is SourceResult.UNREACHABLE
    assert result.error == "timeout"
    assert result.new == []
    assert result.changed == []
    assert result.unchanged == []
    assert _count(session, Event) == event_count
    assert _count(session, EventVersion) == version_count
    assert event.last_seen_at == seen
    assert event.title == title
    assert not session.new
    assert not session.dirty
