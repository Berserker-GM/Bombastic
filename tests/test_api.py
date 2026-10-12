from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, select
from sqlalchemy import event as sa_event
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from radar.api.main import USER_ID, create_app, get_calendar_service, get_session
from radar.calendar.google import CalendarAuthError
from radar.collectors.base import CollectResult
from radar.models import (
    Base,
    Event,
    EventCategory,
    EventEvidence,
    EventVersion,
    HealthStatus,
    Importance,
    Lifecycle,
    ScanRun,
    ScanStatus,
    Source,
    SourceKind,
    UserEventState,
)
from radar.pipeline.scan import run_scan

TOKEN = "test-token"
DEADLINE = datetime(2027, 6, 1, tzinfo=UTC)
NOW = datetime(2026, 10, 11, tzinfo=UTC)


@pytest.fixture
def session_factory() -> sessionmaker[Session]:
    engine = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )

    @sa_event.listens_for(engine, "connect")
    def _enable_foreign_keys(dbapi_connection: Any, _: Any) -> None:
        cursor = dbapi_connection.cursor()
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.close()

    Base.metadata.create_all(engine)
    return sessionmaker(bind=engine)


@pytest.fixture
def client(
    session_factory: sessionmaker[Session], monkeypatch: pytest.MonkeyPatch
) -> Iterator[TestClient]:
    monkeypatch.setenv("API_TOKEN", TOKEN)
    app = create_app(session_factory)
    app.dependency_overrides[get_session] = _session_override(session_factory)
    with TestClient(app) as test_client:
        yield test_client


def _session_override(factory: sessionmaker[Session]):
    def override() -> Iterator[Session]:
        session = factory()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    return override


def _auth() -> dict[str, str]:
    return {"Authorization": f"Bearer {TOKEN}"}


def _event(factory: sessionmaker[Session], **overrides: Any) -> int:
    with factory() as session:
        source = Source(
            kind=SourceKind.API,
            name="devpost",
            config={},
            enabled=True,
            health_status=HealthStatus.HEALTHY,
            consecutive_failures=0,
        )
        session.add(source)
        session.flush()
        values: dict[str, Any] = {
            "source_id": source.id,
            "fingerprint": "winter",
            "title": "Winter of Code",
            "category": EventCategory.HACKATHON,
            "lifecycle": Lifecycle.REGISTRATION_OPEN,
            "registration_deadline": DEADLINE,
            "first_seen_at": NOW,
            "last_seen_at": NOW,
            "content_hash": "hash",
            "confidence": {"registration_deadline": 0.6},
            "url": "https://winter.devpost.com/",
        }
        values.update(overrides)
        event = Event(**values)
        session.add(event)
        session.commit()
        return event.id


def _section_ids(body: dict[str, Any], name: str) -> list[int]:
    return [item["event_id"] for item in body[name]]


def _all_ids(body: dict[str, Any]) -> set[int]:
    names = (
        "important_updates",
        "deadlines_soon",
        "new_opportunities",
        "todays_contests",
        "saved",
        "college",
        "hackathons",
    )
    found: set[int] = set()
    for name in names:
        found.update(_section_ids(body, name))
    return found


def test_feed_requires_a_bearer_token(client: TestClient) -> None:
    assert client.get("/feed").status_code == 401
    assert (
        client.get("/feed", headers={"Authorization": "Bearer wrong"}).status_code
        == 401
    )


def test_health_requires_no_auth(client: TestClient) -> None:
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"ok": True}
    denied = client.get("/health", headers={"Authorization": "Bearer wrong"})
    assert denied.status_code == 200
    assert denied.json() == {"ok": True}


@pytest.mark.parametrize(
    "origin",
    ["http://tauri.localhost", "tauri://localhost", "http://localhost:1420"],
)
def test_preflight_from_an_allowed_origin_returns_cors_headers(
    client: TestClient, origin: str
) -> None:
    response = client.options(
        "/health",
        headers={
            "Origin": origin,
            "Access-Control-Request-Method": "GET",
            "Access-Control-Request-Headers": "authorization,content-type",
        },
    )
    assert response.status_code == 200
    assert response.headers["access-control-allow-origin"] == origin
    assert response.headers["access-control-allow-origin"] != "*"
    methods = response.headers["access-control-allow-methods"]
    assert "GET" in methods
    assert "POST" in methods
    assert "PUT" in methods
    allowed = response.headers["access-control-allow-headers"].lower()
    assert "authorization" in allowed
    assert "content-type" in allowed


def test_preflight_from_a_random_origin_does_not_return_cors_headers(
    client: TestClient,
) -> None:
    response = client.options(
        "/feed",
        headers={
            "Origin": "https://evil.example",
            "Access-Control-Request-Method": "GET",
            "Access-Control-Request-Headers": "authorization,content-type",
        },
    )
    assert "access-control-allow-origin" not in response.headers


def test_cors_origins_env_extends_the_allowlist(
    session_factory: sessionmaker[Session], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv(
        "CORS_ORIGINS",
        " https://desk.example , *, https://other.example ",
    )
    app = create_app(session_factory)
    with TestClient(app) as test_client:
        allowed = test_client.options(
            "/health",
            headers={
                "Origin": "https://desk.example",
                "Access-Control-Request-Method": "POST",
            },
        )
        starred = test_client.options(
            "/health",
            headers={
                "Origin": "https://evil.example",
                "Access-Control-Request-Method": "GET",
            },
        )
    assert allowed.status_code == 200
    assert allowed.headers["access-control-allow-origin"] == "https://desk.example"
    assert "access-control-allow-origin" not in starred.headers


def test_ignore_removes_the_event_and_save_puts_it_in_saved(
    client: TestClient, session_factory: sessionmaker[Session]
) -> None:
    event_id = _event(session_factory)
    listed = client.get("/feed", headers=_auth())
    assert listed.status_code == 200
    assert event_id in _all_ids(listed.json())

    ignored = client.post(
        f"/events/{event_id}/action",
        headers=_auth(),
        json={"action": "ignore"},
    )
    assert ignored.status_code == 200
    hidden = client.get("/feed", headers=_auth()).json()
    assert event_id not in _all_ids(hidden)

    saved = client.post(
        f"/events/{event_id}/action",
        headers=_auth(),
        json={"action": "save"},
    )
    assert saved.status_code == 200
    feed = client.get("/feed", headers=_auth()).json()
    assert _section_ids(feed, "saved") == [event_id]


def test_add_to_calendar_calls_the_service_once(
    client: TestClient, session_factory: sessionmaker[Session]
) -> None:
    event_id = _event(session_factory)
    calls: list[int] = []

    class FakeCalendar:
        def add_to_calendar(
            self, session: Session, user_id: str, event_id: int
        ) -> None:
            calls.append(event_id)
            assert user_id == USER_ID

        def upsert_event(self, session: Session, user_id: str, event: Event) -> None:
            raise AssertionError("upsert_event")

        def delete(self, session: Session, user_id: str, event: Event) -> None:
            raise AssertionError("delete")

    client.app.dependency_overrides[get_calendar_service] = FakeCalendar
    response = client.post(
        f"/events/{event_id}/action",
        headers=_auth(),
        json={"action": "add_to_calendar"},
    )

    assert response.status_code == 200
    assert calls == [event_id]


def test_unhealthy_source_is_listed_in_source_health(
    client: TestClient, session_factory: sessionmaker[Session]
) -> None:
    with session_factory() as session:
        session.add_all(
            [
                Source(
                    kind=SourceKind.API,
                    name="devpost",
                    config={},
                    enabled=True,
                    health_status=HealthStatus.HEALTHY,
                    consecutive_failures=0,
                ),
                Source(
                    kind=SourceKind.API,
                    name="unstop",
                    config={},
                    enabled=True,
                    health_status=HealthStatus.FAILED,
                    consecutive_failures=3,
                    last_error="timeout",
                    last_success_at=NOW,
                ),
            ]
        )
        session.commit()

    body = client.get("/feed", headers=_auth()).json()

    assert [row["name"] for row in body["source_health"]] == ["unstop"]
    unhealthy = body["source_health"][0]
    assert unhealthy["health_status"] == "failed"
    assert unhealthy["last_error"] == "timeout"
    seen = datetime.fromisoformat(unhealthy["last_success_at"].replace("Z", "+00:00"))
    if seen.tzinfo is None:
        seen = seen.replace(tzinfo=UTC)
    assert seen == NOW
    scanned = datetime.fromisoformat(body["scan"]["next_scan_at"])
    assert scanned.astimezone(UTC).hour == 7
    assert scanned.astimezone(UTC).minute == 30


def test_event_detail_includes_evidence_and_versions(
    client: TestClient, session_factory: sessionmaker[Session]
) -> None:
    event_id = _event(session_factory)
    with session_factory() as session:
        session.add(
            EventEvidence(
                event_id=event_id,
                field="registration_deadline",
                source_url="https://college.example/notice",
                source_text="Deadline: 1 June 2027",
                confidence=0.8,
                captured_at=NOW,
            )
        )
        session.add(
            EventVersion(
                event_id=event_id,
                seen_at=NOW,
                changed_fields={"location": [None, "Pune"]},
                importance=Importance.IMPORTANT,
                snapshot={"title": "winter of code"},
            )
        )
        session.commit()

    body = client.get(f"/events/{event_id}", headers=_auth()).json()

    assert body["confidence"] == {"registration_deadline": 0.6}
    assert body["evidence"][0]["source_text"] == "Deadline: 1 June 2027"
    assert body["evidence"][0]["source_url"] == "https://college.example/notice"
    assert body["versions"][0]["changed_fields"] == {"location": [None, "Pune"]}
    assert body["versions"][0]["importance"] == "important"


def test_scan_now_conflicts_while_a_scan_is_running(
    client: TestClient, session_factory: sessionmaker[Session]
) -> None:
    with session_factory() as session:
        session.add(
            ScanRun(
                started_at=datetime.now(UTC),
                finished_at=None,
                status=ScanStatus.RUNNING,
                per_source={},
            )
        )
        session.commit()

    response = client.post("/scan/now", headers=_auth())

    assert response.status_code == 409


def test_missing_google_token_is_503(
    client: TestClient,
    session_factory: sessionmaker[Session],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("GOOGLE_TOKEN_JSON", "")
    monkeypatch.setenv("GOOGLE_TOKEN_PATH", "missing-google-token.json")
    event_id = _event(session_factory)
    response = client.post(
        f"/events/{event_id}/action",
        headers=_auth(),
        json={"action": "add_to_calendar"},
    )

    assert response.status_code == 503
    assert response.json() == {"detail": "calendar not configured"}


def test_calendar_auth_error_is_502(
    client: TestClient, session_factory: sessionmaker[Session]
) -> None:
    event_id = _event(session_factory)

    class FakeCalendar:
        def add_to_calendar(
            self, session: Session, user_id: str, event_id: int
        ) -> None:
            raise CalendarAuthError("Google token expired or revoked")

        def upsert_event(self, session: Session, user_id: str, event: Event) -> None:
            raise AssertionError("upsert_event")

        def delete(self, session: Session, user_id: str, event: Event) -> None:
            raise AssertionError("delete")

    client.app.dependency_overrides[get_calendar_service] = FakeCalendar
    response = client.post(
        f"/events/{event_id}/action",
        headers=_auth(),
        json={"action": "add_to_calendar"},
    )

    assert response.status_code == 502
    assert response.json()["detail"] == "Google token expired or revoked"


def test_run_scan_records_each_source_and_finishes(
    session_factory: sessionmaker[Session],
) -> None:
    def down() -> CollectResult:
        raise RuntimeError("timeout")

    with session_factory() as session:
        session.add(
            Source(
                kind=SourceKind.API,
                name="devpost",
                config={},
                enabled=True,
                health_status=HealthStatus.DEGRADED,
                consecutive_failures=1,
                last_success_at=datetime.now(UTC),
            )
        )
        session.add(
            Source(
                kind=SourceKind.API,
                name="unstop",
                config={},
                enabled=True,
                health_status=HealthStatus.HEALTHY,
                consecutive_failures=0,
            )
        )
        run = ScanRun(
            started_at=datetime.now(UTC),
            finished_at=None,
            status=ScanStatus.RUNNING,
            per_source={},
        )
        session.add(run)
        session.commit()
        code = run_scan(
            session,
            ["devpost", "unstop"],
            scan_id=run.id,
            collectors={
                "devpost": lambda: CollectResult(status="ok", events=[]),
                "unstop": down,
            },
            calendar=None,
            alerter=lambda _message: None,
        )
        stored = session.get(ScanRun, run.id)
        assert stored is not None
        status = stored.status
        per_source = stored.per_source

    assert code == 0
    assert status == ScanStatus.DONE
    assert per_source["devpost"]["result"] == "ok_nothing_new"
    assert per_source["unstop"]["result"] == "malformed"
    assert per_source["unstop"]["error"] == "timeout"
    with session_factory() as session:
        health = {
            source.name: source.health_status
            for source in session.scalars(select(Source))
        }
        assert health["devpost"] == HealthStatus.HEALTHY
        assert health["unstop"] == HealthStatus.STALE
        states = session.scalars(select(UserEventState)).all()
        assert states == []


def test_scan_now_ignores_a_running_row_older_than_30_minutes(
    client: TestClient,
    session_factory: sessionmaker[Session],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("radar.api.main.run_scan", lambda *_args, **_kwargs: 0)
    with session_factory() as session:
        session.add(
            ScanRun(
                started_at=datetime.now(UTC) - timedelta(minutes=31),
                finished_at=None,
                status=ScanStatus.RUNNING,
                per_source={},
            )
        )
        session.commit()

    response = client.post("/scan/now", headers=_auth())

    assert response.status_code == 202
