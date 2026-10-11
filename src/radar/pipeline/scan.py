"""Run every enabled source once. The CLI and POST /scan/now both call this."""

import asyncio
import inspect
from collections.abc import Awaitable, Callable, Mapping, Sequence
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import httpx
from sqlalchemy import select
from sqlalchemy.orm import Session

from radar.calendar.google import (
    CalendarAuthError,
    CalendarService,
    GoogleCalendarService,
    build_google_events_client,
)
from radar.collectors.base import CollectResult
from radar.config import Settings, get_settings
from radar.models import (
    HealthStatus,
    ScanRun,
    ScanStatus,
    Source,
    SourceKind,
    SourceResult,
)
from radar.pipeline.ingest import SourceRunResult, ingest_run
from radar.pipeline.sources import SCANNERS

LOCAL_USER = "local"
CALENDAR_AUTH_ALERT = "Google token expired, re-run the auth script"
ALL_SOURCES_FAILED_ALERT = "every source failed"
BUILTIN_SOURCE_NAMES: tuple[str, ...] = tuple(SCANNERS)

_RUNNING_LOCK = timedelta(minutes=30)
_STALE_AFTER = timedelta(hours=48)
_ERROR_LIMIT = 200
_OK = frozenset({SourceResult.OK_NEW, SourceResult.OK_NOTHING_NEW})
_FAILED_RESULTS = frozenset(
    {SourceResult.UNREACHABLE.value, SourceResult.MALFORMED.value}
)
_WARNING_LIMIT = 5
_EMPTY_COUNTS = {"new": 0, "changed": 0, "unchanged": 0}
_DEFAULT_CALENDAR = object()

CollectorFn = Callable[[], CollectResult | Awaitable[CollectResult]]
Alerter = Callable[[str], None]


class ScanAlreadyRunning(Exception):
    """A scan started within the last 30 minutes is still marked running."""

    def __init__(self) -> None:
        super().__init__("scan already running")


def failed_alert(name: str, error: str | None) -> str:
    detail = error if error else "no error"
    return f"{name} is failed: {detail}"


def stale_alert(name: str) -> str:
    return f"{name} is stale"


def webhook_body(url: str, message: str) -> dict[str, str]:
    """Discord reads ``content``. A Telegram sendMessage URL reads ``text``."""

    if "api.telegram.org" in url:
        return {"text": message}
    return {"content": message}


def post_alert(message: str, url: str | None = None) -> None:
    target = url if url is not None else get_settings().alert_webhook_url
    if not target:
        return
    try:
        httpx.post(target, json=webhook_body(target, message), timeout=10.0)
    except httpx.HTTPError:
        return


def scan_blocks(session: Session, now: datetime | None = None) -> bool:
    moment = now if now is not None else datetime.now(UTC)
    return _live_running(session, moment, ignore_id=None) is not None


def run_scan(
    session: Session,
    only_sources: Sequence[str] | None = None,
    *,
    collectors: Mapping[str, CollectorFn] | None = None,
    calendar: CalendarService | None | object = _DEFAULT_CALENDAR,
    alerter: Alerter | None = None,
    now: datetime | None = None,
    scan_id: int | None = None,
) -> int:
    """Scan sources. Return 1 when every non-skipped source failed.

    A running ScanRun newer than 30 minutes raises ``ScanAlreadyRunning``.
    An older running row is treated as dead and does not block.
    An unexpected exception marks the ScanRun failed before it propagates.
    """

    moment = now if now is not None else datetime.now(UTC)
    run_id = _acquire(session, moment, scan_id)
    completed = False
    try:
        code = _perform(
            session,
            run_id,
            only_sources,
            collectors,
            calendar,
            alerter if alerter is not None else post_alert,
            moment,
        )
        completed = True
        return code
    finally:
        if not completed:
            _mark_failed(session, run_id, moment)


def _acquire(session: Session, now: datetime, scan_id: int | None) -> int:
    if scan_id is None:
        if _live_running(session, now, ignore_id=None) is not None:
            raise ScanAlreadyRunning
        run = ScanRun(
            started_at=now,
            finished_at=None,
            status=ScanStatus.RUNNING,
            per_source={},
        )
        session.add(run)
        session.flush()
        run_id = run.id
        session.commit()
        return run_id
    if session.get(ScanRun, scan_id) is None:
        raise ValueError("scan run not found")
    if _live_running(session, now, ignore_id=scan_id) is not None:
        raise ScanAlreadyRunning
    return scan_id


def _perform(
    session: Session,
    run_id: int,
    only_sources: Sequence[str] | None,
    collectors: Mapping[str, CollectorFn] | None,
    calendar: CalendarService | None | object,
    alerter: Alerter,
    now: datetime,
) -> int:
    registry = _known_names(collectors)
    if collectors is None and only_sources is None:
        _ensure_builtins(session)
    sources, missing = _select_sources(session, registry, only_sources)
    before = {
        source.id: source.health_status
        for source in session.scalars(
            select(Source).where(Source.enabled.is_(True))
        ).all()
    }
    service, open_error, auth_error = _resolve_calendar(calendar)
    per_source: dict[str, Any] = {}
    calendar_report = _empty_calendar()
    if open_error is not None:
        calendar_report["errors"].append(open_error)
        calendar_report["auth_error"] = auth_error
    for name in missing:
        per_source[name] = _entry(
            SourceResult.MALFORMED, dict(_EMPTY_COUNTS), "unknown source"
        )
    for source in sources:
        if not source.enabled:
            source.health_status = HealthStatus.DISABLED
            per_source[source.name] = _entry(
                SourceResult.SKIPPED, dict(_EMPTY_COUNTS), None
            )
            continue
        outcome = _run_source(session, source, collectors)
        _apply_health(source, outcome, now)
        if service is not None and not auth_error:
            auth_error = _sync_calendar(
                session, service, source.name, outcome, calendar_report
            )
        warnings = list(outcome.warnings)
        if outcome.warning and not warnings:
            warnings = [outcome.warning]
        per_source[source.name] = _entry(
            outcome.result,
            {
                "new": len(outcome.new),
                "changed": len(outcome.changed),
                "unchanged": len(outcome.unchanged),
            },
            outcome.error,
            warnings,
        )
    calendar_report["auth_error"] = auth_error or bool(calendar_report["auth_error"])
    _apply_staleness(session, now)
    per_source["calendar"] = calendar_report
    messages = _transition_alerts(session, before)
    if _every_source_failed(per_source) and not _previous_all_failed(session, run_id):
        messages.append(ALL_SOURCES_FAILED_ALERT)
    if calendar_report["auth_error"]:
        messages.append(CALENDAR_AUTH_ALERT)
    run = session.get(ScanRun, run_id)
    if run is None:
        raise RuntimeError("scan run disappeared")
    run.per_source = per_source
    run.status = (
        ScanStatus.FAILED if _every_source_failed(per_source) else ScanStatus.DONE
    )
    run.finished_at = now
    session.commit()
    for message in messages:
        alerter(message)
    if run.status == ScanStatus.FAILED:
        return 1
    return 0


def _known_names(
    collectors: Mapping[str, CollectorFn] | None,
) -> Mapping[str, Any]:
    if collectors is not None:
        return collectors
    return SCANNERS


def _ensure_builtins(session: Session) -> None:
    existing = set(session.scalars(select(Source.name)).all())
    for name in BUILTIN_SOURCE_NAMES:
        if name in existing:
            continue
        session.add(
            Source(
                kind=SourceKind.API,
                name=name,
                config={},
                enabled=True,
                health_status=HealthStatus.HEALTHY,
                consecutive_failures=0,
            )
        )
    session.flush()


def _select_sources(
    session: Session,
    registry: Mapping[str, Any],
    only_sources: Sequence[str] | None,
) -> tuple[list[Source], list[str]]:
    rows = {
        source.name: source
        for source in session.scalars(select(Source).order_by(Source.name)).all()
    }
    if only_sources is None:
        return [rows[name] for name in sorted(rows) if name in registry], []
    selected: list[Source] = []
    missing: list[str] = []
    for name in only_sources:
        source = rows.get(name)
        if source is None:
            missing.append(name)
            continue
        selected.append(source)
    return selected, missing


async def _invoke(fn: CollectorFn) -> CollectResult:
    value = fn()
    if inspect.isawaitable(value):
        value = await value
    if not isinstance(value, CollectResult):
        raise TypeError("collector did not return CollectResult")
    return value


def _run_source(
    session: Session,
    source: Source,
    collectors: Mapping[str, CollectorFn] | None,
) -> SourceRunResult:
    try:
        if collectors is None:
            scanner = SCANNERS.get(source.name)
            if scanner is None:
                return SourceRunResult(
                    result=SourceResult.MALFORMED, error="no collector"
                )
            return asyncio.run(scanner(session))
        fn = collectors.get(source.name)
        if fn is None:
            return SourceRunResult(result=SourceResult.MALFORMED, error="no collector")
        return ingest_run(session, source, _collect(fn))
    except Exception as exc:
        return SourceRunResult(
            result=SourceResult.MALFORMED, error=_exception_error(exc)
        )


def _collect(fn: CollectorFn) -> CollectResult:
    return asyncio.run(_invoke(fn))


def _apply_health(source: Source, outcome: SourceRunResult, now: datetime) -> None:
    if outcome.result == SourceResult.SKIPPED or not source.enabled:
        source.health_status = HealthStatus.DISABLED
        return
    if outcome.result in _OK:
        source.health_status = HealthStatus.HEALTHY
        source.consecutive_failures = 0
        source.last_success_at = now
        source.last_error = None
        return
    source.consecutive_failures += 1
    source.last_failure_at = now
    source.last_error = _short_error(outcome.error)
    if source.consecutive_failures >= 3:
        source.health_status = HealthStatus.FAILED
    else:
        source.health_status = HealthStatus.DEGRADED


def _apply_staleness(session: Session, now: datetime) -> None:
    sources = session.scalars(select(Source).where(Source.enabled.is_(True))).all()
    for source in sources:
        last_success_at = source.last_success_at
        if last_success_at is not None:
            last_success = _as_utc(last_success_at)
            if now - last_success < _STALE_AFTER:
                continue
        source.health_status = HealthStatus.STALE


def _sync_calendar(
    session: Session,
    service: CalendarService,
    source_name: str,
    outcome: SourceRunResult,
    report: dict[str, Any],
) -> bool:
    auth_error = False
    for event in (*outcome.new, *outcome.changed):
        try:
            link = service.upsert_event(session, LOCAL_USER, event)
        except CalendarAuthError:
            auth_error = True
            report["errors"].append(CALENDAR_AUTH_ALERT)
            break
        except Exception as exc:
            report["failed"] += 1
            report["errors"].append(f"{source_name}: {_exception_error(exc)}")
            continue
        if link is None:
            report["skipped"] += 1
        else:
            report["upserted"] += 1
    return auth_error


def _resolve_calendar(
    calendar: CalendarService | None | object,
) -> tuple[Any, str | None, bool]:
    if calendar is not _DEFAULT_CALENDAR:
        return calendar, None, False
    settings = get_settings()
    if not _calendar_configured(settings):
        return None, None, False
    try:
        return GoogleCalendarService(build_google_events_client()), None, False
    except CalendarAuthError:
        return None, CALENDAR_AUTH_ALERT, True
    except Exception as exc:
        return None, _exception_error(exc), False


def _calendar_configured(settings: Settings) -> bool:
    token_json = settings.google_token_json
    if isinstance(token_json, str) and token_json.strip():
        return True
    return Path(settings.google_token_path).is_file()


def _transition_alerts(
    session: Session, before: Mapping[int, HealthStatus]
) -> list[str]:
    messages: list[str] = []
    sources = session.scalars(select(Source).order_by(Source.name)).all()
    for source in sources:
        if not source.enabled:
            continue
        previous = before.get(source.id)
        if source.health_status == previous:
            continue
        if source.health_status == HealthStatus.FAILED:
            messages.append(failed_alert(source.name, source.last_error))
        elif source.health_status == HealthStatus.STALE:
            messages.append(stale_alert(source.name))
    return messages


def _previous_all_failed(session: Session, run_id: int) -> bool:
    previous = session.scalar(
        select(ScanRun)
        .where(
            ScanRun.status.in_((ScanStatus.DONE, ScanStatus.FAILED)),
            ScanRun.id != run_id,
        )
        .order_by(ScanRun.finished_at.desc(), ScanRun.id.desc())
    )
    if previous is None or not isinstance(previous.per_source, dict):
        return False
    return _every_source_failed(previous.per_source)


def _every_source_failed(per_source: Mapping[str, Any]) -> bool:
    entries = _scanned_entries(per_source)
    return bool(entries) and all(
        entry.get("result") in _FAILED_RESULTS for entry in entries
    )


def _scanned_entries(per_source: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    entries: list[Mapping[str, Any]] = []
    for name, payload in per_source.items():
        if name == "calendar" or not isinstance(payload, dict):
            continue
        if "result" not in payload:
            continue
        if payload.get("result") == SourceResult.SKIPPED.value:
            continue
        entries.append(payload)
    return entries


def _entry(
    result: SourceResult,
    counts: dict[str, int],
    error: str | None,
    warnings: Sequence[str] | None = None,
) -> dict[str, Any]:
    listed = [warning for warning in (warnings or []) if warning]
    return {
        "result": result.value,
        "counts": counts,
        "error": _short_error(error),
        "warning_count": len(listed),
        "warnings": listed[:_WARNING_LIMIT],
    }


def _empty_calendar() -> dict[str, Any]:
    return {
        "upserted": 0,
        "skipped": 0,
        "failed": 0,
        "auth_error": False,
        "errors": [],
    }


def _mark_failed(session: Session, run_id: int, now: datetime) -> None:
    session.rollback()
    run = session.get(ScanRun, run_id)
    if run is None:
        return
    run.status = ScanStatus.FAILED
    if run.finished_at is None:
        run.finished_at = now
    session.commit()


def _live_running(
    session: Session, now: datetime, ignore_id: int | None
) -> ScanRun | None:
    rows = session.scalars(
        select(ScanRun).where(ScanRun.status == ScanStatus.RUNNING)
    ).all()
    for row in rows:
        if ignore_id is not None and row.id == ignore_id:
            continue
        if now - _as_utc(row.started_at) <= _RUNNING_LOCK:
            return row
    return None


def _exception_error(exc: BaseException) -> str:
    text = _short_error(str(exc))
    if text:
        return text
    return exc.__class__.__name__


def _short_error(error: str | None) -> str | None:
    if error is None:
        return None
    compact = " ".join(error.split())
    if not compact:
        return None
    if len(compact) > _ERROR_LIMIT:
        return compact[: _ERROR_LIMIT - 3] + "..."
    return compact


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)
