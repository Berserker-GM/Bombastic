import inspect
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy import event as sa_event
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from radar.calendar.google import CalendarAuthError
from radar.cli.__main__ import format_health_table, main
from radar.collectors.base import CollectResult, RawEvent
from radar.db import normalize_database_url
from radar.models import (
    Base,
    Event,
    EventCategory,
    HealthStatus,
    ScanRun,
    ScanStatus,
    Source,
    SourceKind,
    SourceResult,
)
from radar.pipeline.scan import (
    ALL_SOURCES_FAILED_ALERT,
    CALENDAR_AUTH_ALERT,
    ScanAlreadyRunning,
    failed_alert,
    run_scan,
    stale_alert,
    webhook_body,
)

NOW = datetime(2026, 10, 11, 12, tzinfo=UTC)


@pytest.fixture
def session() -> Iterator[Session]:
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
    with Session(engine) as db_session:
        yield db_session


def test_health_goes_degraded_then_failed_then_healthy_and_alerts_once(
    session: Session,
) -> None:
    session.add(_source("alpha", last_success=NOW))
    session.commit()
    alerts: list[str] = []
    failed = failed_alert("alpha", "down")

    def run(collector: Any) -> int:
        return run_scan(
            session,
            collectors={"alpha": collector},
            calendar=None,
            alerter=alerts.append,
            now=NOW,
        )

    assert run(_down) == 1
    alpha = _named(session, "alpha")
    assert alpha.health_status == HealthStatus.DEGRADED
    assert alpha.consecutive_failures == 1
    assert alpha.last_error == "down"
    assert _instant(alpha.last_failure_at) == NOW
    assert _instant(alpha.last_success_at) == NOW
    assert alerts == [ALL_SOURCES_FAILED_ALERT]

    assert run(_down) == 1
    alpha = _named(session, "alpha")
    assert alpha.health_status == HealthStatus.DEGRADED
    assert alpha.consecutive_failures == 2
    assert alerts == [ALL_SOURCES_FAILED_ALERT]

    assert run(_down) == 1
    alpha = _named(session, "alpha")
    assert alpha.health_status == HealthStatus.FAILED
    assert alpha.consecutive_failures == 3
    assert alerts == [ALL_SOURCES_FAILED_ALERT, failed]

    assert run(_down) == 1
    alpha = _named(session, "alpha")
    assert alpha.health_status == HealthStatus.FAILED
    assert alpha.consecutive_failures == 4
    assert alerts.count(failed) == 1
    assert alerts.count(ALL_SOURCES_FAILED_ALERT) == 1

    assert run(_ok) == 0
    alpha = _named(session, "alpha")
    assert alpha.health_status == HealthStatus.HEALTHY
    assert alpha.consecutive_failures == 0
    assert alpha.last_error is None
    assert _instant(alpha.last_success_at) == NOW
    assert alerts.count(failed) == 1


def test_stale_when_no_success_in_48_hours_alerts_once(session: Session) -> None:
    session.add(_source("old", last_success=NOW - timedelta(hours=49)))
    session.add(_source("recent", last_success=NOW - timedelta(hours=47)))
    session.add(_source("fresh", last_success=NOW))
    session.add(
        _source(
            "off",
            enabled=False,
            last_success=NOW - timedelta(hours=49),
        )
    )
    session.commit()
    alerts: list[str] = []

    def run() -> int:
        return run_scan(
            session,
            collectors={"recent": _down, "fresh": _ok},
            calendar=None,
            alerter=alerts.append,
            now=NOW,
        )

    assert run() == 0
    assert _named(session, "old").health_status == HealthStatus.STALE
    assert _named(session, "old").consecutive_failures == 0
    assert _named(session, "recent").health_status == HealthStatus.DEGRADED
    assert _named(session, "fresh").health_status == HealthStatus.HEALTHY
    assert _named(session, "off").health_status == HealthStatus.HEALTHY
    assert alerts == [stale_alert("old")]

    assert run() == 0
    assert _named(session, "recent").consecutive_failures == 2
    assert _named(session, "recent").health_status == HealthStatus.DEGRADED
    assert _named(session, "old").health_status == HealthStatus.STALE
    assert alerts == [stale_alert("old")]


def test_running_scan_blocks_until_it_is_older_than_30_minutes(
    session: Session,
) -> None:
    session.add(
        ScanRun(
            started_at=NOW - timedelta(minutes=5),
            finished_at=None,
            status=ScanStatus.RUNNING,
            per_source={},
        )
    )
    session.commit()

    with pytest.raises(ScanAlreadyRunning):
        run_scan(session, collectors={}, calendar=None, alerter=_ignore, now=NOW)
    assert len(session.scalars(select(ScanRun)).all()) == 1

    blocking = session.scalars(select(ScanRun)).one()
    blocking.started_at = NOW - timedelta(minutes=31)
    session.commit()
    code = run_scan(session, collectors={}, calendar=None, alerter=_ignore, now=NOW)

    rows = session.scalars(select(ScanRun).order_by(ScanRun.id)).all()
    assert code == 0
    assert [row.status for row in rows] == [ScanStatus.RUNNING, ScanStatus.DONE]


def test_unexpected_exception_is_malformed_and_other_sources_continue(
    session: Session,
) -> None:
    session.add(_source("boom", last_success=NOW))
    session.add(_source("ok", last_success=NOW))
    session.commit()

    code = run_scan(
        session,
        collectors={"boom": _boom, "ok": _ok},
        calendar=None,
        alerter=_ignore,
        now=NOW,
    )

    stored = _latest(session)
    assert code == 0
    assert stored.per_source["boom"]["result"] == SourceResult.MALFORMED.value
    assert stored.per_source["boom"]["error"] == "parser blew up"
    assert stored.per_source["ok"]["result"] == SourceResult.OK_NOTHING_NEW.value
    assert _named(session, "boom").health_status == HealthStatus.DEGRADED


def test_calendar_upsert_is_recorded_and_errors_do_not_fail_the_scan(
    session: Session,
) -> None:
    session.add(_source("alpha", last_success=NOW))
    session.commit()
    calendar = _RecordingCalendar()
    code = run_scan(
        session,
        collectors={"alpha": _one_event},
        calendar=calendar,
        alerter=_ignore,
        now=NOW,
    )

    stored = _latest(session)
    assert code == 0
    assert stored.per_source["alpha"]["result"] == SourceResult.OK_NEW.value
    assert stored.per_source["alpha"]["counts"] == {
        "new": 1,
        "changed": 0,
        "unchanged": 0,
    }
    assert len(calendar.calls) == 1
    assert calendar.calls[0][0] == "local"
    assert stored.per_source["calendar"]["upserted"] == 1
    assert session.scalars(select(Event)).all()

    broken = _RecordingCalendar(error=RuntimeError("google down"))
    again = run_scan(
        session,
        collectors={"alpha": _changed_event},
        calendar=broken,
        alerter=_ignore,
        now=NOW,
    )
    assert again == 0
    report = _latest(session).per_source["calendar"]
    assert report["failed"] == 1
    assert report["auth_error"] is False
    assert len(session.scalars(select(Event)).all()) == 1


def test_calendar_auth_error_alerts_every_run_until_fixed(
    session: Session, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("GOOGLE_TOKEN_JSON", "{}")

    def expired(*_args: object, **_kwargs: object) -> None:
        raise CalendarAuthError("expired")

    monkeypatch.setattr("radar.pipeline.scan.build_google_events_client", expired)
    session.add(_source("alpha", last_success=NOW))
    session.commit()
    alerts: list[str] = []

    for _ in range(2):
        code = run_scan(
            session,
            collectors={"alpha": _ok},
            alerter=alerts.append,
            now=NOW,
        )
        assert code == 0

    assert alerts == [CALENDAR_AUTH_ALERT, CALENDAR_AUTH_ALERT]
    assert _latest(session).per_source["calendar"]["auth_error"] is True
    assert _named(session, "alpha").health_status == HealthStatus.HEALTHY


def test_webhook_body_matches_discord_or_telegram() -> None:
    assert webhook_body("https://discord.com/api/webhooks/1/token", "hi") == {
        "content": "hi"
    }
    assert webhook_body("https://api.telegram.org/bot123/sendMessage", "hi") == {
        "text": "hi"
    }


def test_normalize_database_url_uses_psycopg() -> None:
    neon = "postgresql://user:pw@ep.neon.tech/radar?sslmode=require"
    supabase = "postgres://user:pw@db.supabase.co:5432/postgres"
    assert normalize_database_url(neon) == (
        "postgresql+psycopg://user:pw@ep.neon.tech/radar?sslmode=require"
    )
    assert normalize_database_url(supabase) == (
        "postgresql+psycopg://user:pw@db.supabase.co:5432/postgres"
    )
    assert normalize_database_url("sqlite:///./radar.db") == "sqlite:///./radar.db"


def test_health_table_lists_status() -> None:
    text = format_health_table(
        [
            _source(
                "alpha",
                health=HealthStatus.DEGRADED,
                failures=2,
                last_success=NOW,
                last_error="timeout",
            )
        ]
    )
    assert "alpha" in text
    assert "degraded" in text
    assert "timeout" in text


def test_sources_command_prints_the_health_table(
    session: Session,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    session.add(
        _source(
            "alpha",
            health=HealthStatus.FAILED,
            failures=3,
            last_error="down",
        )
    )
    session.commit()
    factory = sessionmaker(bind=session.get_bind())
    monkeypatch.setattr("radar.cli.__main__.SessionLocal", factory)

    main(["sources"])

    output = capsys.readouterr().out
    assert "alpha" in output
    assert "failed" in output
    assert "down" in output


def test_scan_all_exit_code_is_nonzero_only_when_every_source_failed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("radar.cli.__main__.SessionLocal", lambda: _QuietSession())
    monkeypatch.setattr(
        "radar.cli.__main__.run_scan", lambda session, only_sources=None: 0
    )
    with pytest.raises(SystemExit) as ok:
        main(["scan-all"])
    assert ok.value.code == 0

    monkeypatch.setattr(
        "radar.cli.__main__.run_scan", lambda session, only_sources=None: 1
    )
    with pytest.raises(SystemExit) as failed:
        main(["scan-all"])
    assert failed.value.code == 1

    def blocked(session: Session, only_sources: None = None) -> int:
        raise ScanAlreadyRunning

    monkeypatch.setattr("radar.cli.__main__.run_scan", blocked)
    with pytest.raises(SystemExit) as locked:
        main(["scan-all"])
    assert locked.value.code == 1


def test_unexpected_exception_marks_the_scan_failed(
    session: Session, monkeypatch: pytest.MonkeyPatch
) -> None:
    session.add(_source("alpha", last_success=NOW))
    session.commit()

    def explode(*_args: object, **_kwargs: object) -> None:
        raise RuntimeError("staleness blew up")

    monkeypatch.setattr("radar.pipeline.scan._apply_staleness", explode)
    with pytest.raises(RuntimeError, match="staleness blew up"):
        run_scan(
            session,
            collectors={"alpha": _ok},
            calendar=None,
            alerter=_ignore,
            now=NOW,
        )

    stored = _latest(session)
    assert stored.status == ScanStatus.FAILED
    assert stored.finished_at is not None


def test_disabled_source_is_skipped(session: Session) -> None:
    session.add(_source("alpha", enabled=False, last_success=NOW))
    session.add(_source("beta", last_success=NOW))
    session.commit()

    def unused() -> CollectResult:
        raise AssertionError("disabled source was collected")

    code = run_scan(
        session,
        collectors={"alpha": unused, "beta": _ok},
        calendar=None,
        alerter=_ignore,
        now=NOW,
    )

    stored = _latest(session)
    assert code == 0
    assert stored.status == ScanStatus.DONE
    assert stored.per_source["alpha"]["result"] == "skipped"
    assert stored.per_source["beta"]["result"] == "ok_nothing_new"
    alpha = _named(session, "alpha")
    assert alpha.health_status == HealthStatus.DISABLED
    assert alpha.consecutive_failures == 0


def test_every_failed_source_sets_failed_status(session: Session) -> None:
    session.add(_source("alpha", last_success=NOW))
    session.add(_source("off", enabled=False, last_success=NOW))
    session.commit()

    code = run_scan(
        session,
        collectors={"alpha": _down, "off": _ok},
        calendar=None,
        alerter=_ignore,
        now=NOW,
    )

    stored = _latest(session)
    assert code == 1
    assert stored.status == ScanStatus.FAILED
    assert stored.per_source["alpha"]["result"] == "unreachable"
    assert stored.per_source["off"]["result"] == "skipped"


def test_one_success_keeps_the_run_done(session: Session) -> None:
    session.add(_source("alpha", last_success=NOW))
    session.add(_source("beta", last_success=NOW))
    session.commit()

    code = run_scan(
        session,
        collectors={"alpha": _down, "beta": _ok},
        calendar=None,
        alerter=_ignore,
        now=NOW,
    )

    stored = _latest(session)
    assert code == 0
    assert stored.status == ScanStatus.DONE
    assert stored.per_source["alpha"]["result"] == "unreachable"
    assert stored.per_source["beta"]["result"] == "ok_nothing_new"


def test_per_source_keeps_the_warning_count_and_first_five(session: Session) -> None:
    warnings = [f"item {index}" for index in range(6)]
    session.add(_source("alpha", last_success=NOW))
    session.commit()

    code = run_scan(
        session,
        collectors={
            "alpha": lambda: CollectResult(status="ok", events=[], warnings=warnings)
        },
        calendar=None,
        alerter=_ignore,
        now=NOW,
    )

    stored = _latest(session)
    assert code == 0
    assert stored.per_source["alpha"]["warning_count"] == 6
    assert stored.per_source["alpha"]["warnings"] == warnings[:5]


def test_pipeline_does_not_import_the_cli() -> None:
    import radar.pipeline.scan as scan
    import radar.pipeline.sources as sources

    assert "radar.cli" not in inspect.getsource(scan)
    assert "radar.cli" not in inspect.getsource(sources)


def test_gitignore_names_client_secrets() -> None:
    root = Path(__file__).resolve().parents[1]
    text = (root / ".gitignore").read_text(encoding="utf-8")
    assert "client_secret*.json" in text
    assert "cilent_secret" not in text
    assert not (root / "notices.html").exists()


class _QuietSession:
    def __enter__(self) -> "_QuietSession":
        return self

    def __exit__(self, *_args: object) -> None:
        return None


class _RecordingCalendar:
    def __init__(self, error: Exception | None = None) -> None:
        self.calls: list[tuple[str, int]] = []
        self.error = error

    def upsert_event(
        self, session: Session, user_id: str, event: Event
    ) -> object | None:
        if self.error is not None:
            raise self.error
        self.calls.append((user_id, event.id))
        return object()


def _source(
    name: str,
    *,
    health: HealthStatus = HealthStatus.HEALTHY,
    failures: int = 0,
    last_success: datetime | None = None,
    last_error: str | None = None,
    enabled: bool = True,
) -> Source:
    return Source(
        kind=SourceKind.API,
        name=name,
        config={},
        enabled=enabled,
        health_status=health,
        consecutive_failures=failures,
        last_success_at=last_success,
        last_error=last_error,
    )


def _down() -> CollectResult:
    return CollectResult(status="unreachable", error="down")


def _ok() -> CollectResult:
    return CollectResult(status="ok", events=[])


def _boom() -> CollectResult:
    raise RuntimeError("parser blew up")


def _one_event() -> CollectResult:
    return CollectResult(status="ok", events=[_raw("Campus Cup", "cup-1")])


def _changed_event() -> CollectResult:
    return CollectResult(
        status="ok",
        events=[_raw("Campus Cup", "cup-1", url="https://example.com/cup-2")],
    )


def _raw(
    title: str, external_id: str, url: str = "https://example.com/cup"
) -> RawEvent:
    return RawEvent(
        source_name="alpha",
        title=title,
        category=EventCategory.CONTEST,
        external_id=external_id,
        url=url,
        start_at=datetime(2027, 1, 1, tzinfo=UTC),
    )


def _named(session: Session, name: str) -> Source:
    source = session.scalar(select(Source).where(Source.name == name))
    assert source is not None
    return source


def _latest(session: Session) -> ScanRun:
    run = session.scalar(select(ScanRun).order_by(ScanRun.id.desc()))
    assert run is not None
    return run


def _instant(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _ignore(_message: str) -> None:
    return None
