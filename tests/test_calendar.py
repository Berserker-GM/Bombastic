import json
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
from google.auth.exceptions import RefreshError
from google.oauth2.credentials import Credentials
from googleapiclient.errors import HttpError
from sqlalchemy import create_engine, func, select
from sqlalchemy import event as sa_event
from sqlalchemy.orm import Session
from sqlalchemy.pool import StaticPool

from radar.calendar.google import (
    REMINDERS,
    CalendarAuthError,
    GoogleCalendarService,
    build_google_events_client,
    google_event_body,
    load_credentials,
)
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
NOW = datetime(2026, 10, 1, tzinfo=UTC)
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


def _service(client: FakeGoogleEvents) -> GoogleCalendarService:
    return GoogleCalendarService(client, now=lambda: NOW)


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
    service = _service(fake)

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
    service = _service(fake)

    assert service.upsert_event(session, USER, event) is None
    assert fake.inserted == []

    added = service.add_to_calendar(session, USER, event.id)

    assert added is not None
    assert len(fake.inserted) == 1
    event.registration_deadline = OCT_20
    event.content_hash = "hash-2"
    updated = service.upsert_event(session, USER, event)

    assert updated is added
    assert len(fake.inserted) == 1
    assert len(fake.updated) == 1
    assert fake.updated[0][0] == "gcal-1"
    assert fake.updated[0][1]["start"]["dateTime"] == OCT_20.isoformat()
    assert added.last_synced_hash == "hash-2"


def test_delete_removes_the_google_event_once(session: Session) -> None:
    source = _source(session)
    event = _event(session, source)
    _prefs(session, "auto")
    fake = FakeGoogleEvents()
    service = _service(fake)
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


PAST = datetime(2020, 1, 1, tzinfo=UTC)
_TOKEN = {
    "token": "access-token",
    "refresh_token": "refresh-token",
    "client_id": "client-id",
    "client_secret": "client-secret",
}


def _http_error(status: int) -> HttpError:
    class _Response:
        def __init__(self) -> None:
            self.status = status
            self.reason = "missing"

    return HttpError(_Response(), b"")


def test_skips_events_with_no_or_past_key_time(session: Session) -> None:
    source = _source(session)
    _prefs(session, "auto")
    fake = FakeGoogleEvents()
    service = _service(fake)
    undated = _event(
        session,
        source,
        fingerprint="undated",
        registration_deadline=None,
        content_hash="undated",
    )
    past = _event(
        session,
        source,
        fingerprint="past",
        registration_deadline=PAST,
        content_hash="past",
    )

    assert service.upsert_event(session, USER, undated) is None
    assert service.upsert_event(session, USER, past) is None
    assert service.add_to_calendar(session, USER, past.id) is None
    assert fake.inserted == []
    assert fake.updated == []

    future = _event(session, source, fingerprint="future", content_hash="future")
    service.upsert_event(session, USER, future)
    future.registration_deadline = PAST
    future.content_hash = "now-past"

    assert service.upsert_event(session, USER, future) is None
    assert fake.updated == []
    assert _link_count(session) == 1


@pytest.mark.parametrize("status", [404, 410])
def test_deleted_google_event_is_recreated(session: Session, status: int) -> None:
    source = _source(session)
    event = _event(session, source)
    _prefs(session, "auto")

    class Gone(FakeGoogleEvents):
        def update(self, google_event_id: str, body: dict[str, Any]) -> None:
            raise _http_error(status)

    fake = Gone()
    service = _service(fake)
    created = service.upsert_event(session, USER, event)
    assert created is not None
    event.registration_deadline = OCT_20
    event.content_hash = "hash-2"

    updated = service.upsert_event(session, USER, event)

    assert updated is created
    assert len(fake.inserted) == 2
    assert updated.google_event_id == "gcal-2"
    assert updated.last_synced_hash == "hash-2"
    assert _link_count(session) == 1


def test_update_http_500_is_not_recreated(session: Session) -> None:
    source = _source(session)
    event = _event(session, source)
    _prefs(session, "auto")

    class Broken(FakeGoogleEvents):
        def update(self, google_event_id: str, body: dict[str, Any]) -> None:
            raise _http_error(500)

    fake = Broken()
    service = _service(fake)
    service.upsert_event(session, USER, event)
    event.content_hash = "hash-2"

    with pytest.raises(HttpError) as raised:
        service.upsert_event(session, USER, event)

    assert raised.value.status_code == 500
    assert len(fake.inserted) == 1


@pytest.mark.parametrize("status", [404, 410])
def test_delete_of_missing_google_event_succeeds(session: Session, status: int) -> None:
    source = _source(session)
    event = _event(session, source)
    _prefs(session, "auto")

    class Gone(FakeGoogleEvents):
        def delete(self, google_event_id: str) -> None:
            raise _http_error(status)

    fake = Gone()
    service = _service(fake)
    service.upsert_event(session, USER, event)

    service.delete(session, USER, event)

    assert _link_count(session) == 0


def test_build_client_uses_configured_calendar_id(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("GOOGLE_CALENDAR_ID", "team-cal")
    future = {**_TOKEN, "expiry": "2027-01-01T00:00:00Z"}
    monkeypatch.setenv("GOOGLE_TOKEN_JSON", json.dumps(future))
    seen: dict[str, str] = {}

    class _Call:
        def execute(self) -> dict[str, str]:
            return {"id": "evt"}

    class _Resource:
        def events(self) -> "_Resource":
            return self

        def insert(self, *, calendarId: str, body: dict[str, Any]) -> _Call:
            seen["calendarId"] = calendarId
            return _Call()

    monkeypatch.setattr(
        "radar.calendar.google.build", lambda *args, **kwargs: _Resource()
    )
    client = build_google_events_client()
    client.insert({"summary": "x"})

    assert seen["calendarId"] == "team-cal"


def test_token_json_does_not_touch_the_filesystem(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def fail(*args: Any, **kwargs: Any) -> Credentials:
        raise AssertionError("token file was opened")

    monkeypatch.setattr(Credentials, "from_authorized_user_file", fail)
    path = tmp_path / "token.json"
    future = {**_TOKEN, "expiry": "2027-01-01T00:00:00Z"}

    creds = load_credentials(token_json=json.dumps(future), token_path=path)

    assert creds.token == "access-token"
    assert not path.exists()


def test_token_path_is_used_when_json_is_absent(tmp_path: Path) -> None:
    path = tmp_path / "token.json"
    future = {**_TOKEN, "expiry": "2027-01-01T00:00:00Z"}
    path.write_text(json.dumps(future), encoding="utf-8")

    creds = load_credentials(token_json=None, token_path=path)

    assert creds.token == "access-token"


def test_failed_refresh_raises_calendar_auth_error(tmp_path: Path) -> None:
    expired = {**_TOKEN, "expiry": "2020-01-01T00:00:00Z"}

    def boom(creds: Credentials) -> None:
        raise RefreshError("invalid_grant")

    with pytest.raises(
        CalendarAuthError,
        match="Google token expired or revoked: re-run the auth script",
    ):
        load_credentials(
            token_json=json.dumps(expired),
            token_path=tmp_path / "token.json",
            refresh=boom,
        )
