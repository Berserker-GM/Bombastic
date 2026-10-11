"""Run a scan across sources. The API and the daily job both call this."""

import asyncio
from collections.abc import Awaitable, Callable, Mapping
from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session, sessionmaker

from radar.cli.__main__ import (
    scan_clist,
    scan_codeforces,
    scan_devfolio,
    scan_devpost,
    scan_unstop,
)
from radar.db import SessionLocal
from radar.models import HealthStatus, ScanRun, ScanStatus, Source, SourceResult
from radar.pipeline.ingest import SourceRunResult

Scanner = Callable[[Session], Awaitable[SourceRunResult]]
SCANNERS: dict[str, Scanner] = {
    "codeforces": scan_codeforces,
    "clist": scan_clist,
    "devpost": scan_devpost,
    "unstop": scan_unstop,
    "devfolio": scan_devfolio,
}
_STALE_AFTER = timedelta(hours=48)
_EMPTY_COUNTS = {"new": 0, "changed": 0, "unchanged": 0}


async def run_scan(
    sources: list[str] | None = None,
    *,
    scan_id: int | None = None,
    session_factory: sessionmaker[Session] | None = None,
    scanners: Mapping[str, Scanner] | None = None,
) -> int:
    """Execute a scan and return its ScanRun id.

    Pass ``scan_id`` when the caller already inserted a running row.
    """

    factory = session_factory or SessionLocal
    selected = scanners if scanners is not None else SCANNERS
    names = list(selected) if sources is None else list(sources)
    with factory() as session:
        run = _running_row(session, scan_id)
        run_id = run.id
        session.commit()
    per_source: dict[str, Any] = {}
    now = datetime.now(UTC)
    with factory() as session:
        for name in names:
            per_source[name] = await _scan_one(session, selected, name, now)
        run = session.get(ScanRun, run_id)
        if run is None:
            raise RuntimeError("scan run disappeared")
        run.per_source = per_source
        run.status = ScanStatus.DONE
        run.finished_at = datetime.now(UTC)
        session.commit()
    return run_id


def _running_row(session: Session, scan_id: int | None) -> ScanRun:
    if scan_id is not None:
        run = session.get(ScanRun, scan_id)
        if run is None:
            raise ValueError("scan run not found")
        return run
    run = ScanRun(
        started_at=datetime.now(UTC),
        finished_at=None,
        status=ScanStatus.RUNNING,
        per_source={},
    )
    session.add(run)
    session.flush()
    return run


async def _scan_one(
    session: Session,
    scanners: Mapping[str, Scanner],
    name: str,
    now: datetime,
) -> dict[str, Any]:
    source = session.scalar(select(Source).where(Source.name == name))
    if source is not None and not source.enabled:
        source.health_status = HealthStatus.DISABLED
        return {
            "result": SourceResult.MALFORMED.value,
            "counts": dict(_EMPTY_COUNTS),
            "error": "disabled",
        }
    scanner = scanners.get(name)
    if scanner is None:
        return {
            "result": SourceResult.MALFORMED.value,
            "counts": dict(_EMPTY_COUNTS),
            "error": "unknown source",
        }
    try:
        outcome = await scanner(session)
    except Exception as exc:
        _mark_failure(session, name, str(exc), now)
        return {
            "result": SourceResult.UNREACHABLE.value,
            "counts": dict(_EMPTY_COUNTS),
            "error": str(exc),
        }
    _mark_outcome(session, name, outcome, now)
    return {
        "result": outcome.result.value,
        "counts": {
            "new": len(outcome.new),
            "changed": len(outcome.changed),
            "unchanged": len(outcome.unchanged),
        },
        "error": outcome.error,
    }


def _mark_outcome(
    session: Session, name: str, outcome: SourceRunResult, now: datetime
) -> None:
    source = session.scalar(select(Source).where(Source.name == name))
    if source is None or not source.enabled:
        return
    ok = outcome.result in (SourceResult.OK_NEW, SourceResult.OK_NOTHING_NEW)
    if ok:
        source.health_status = HealthStatus.HEALTHY
        source.last_success_at = now
        source.consecutive_failures = 0
        source.last_error = None
        return
    _mark_failure(session, name, outcome.error, now)


def _mark_failure(
    session: Session, name: str, error: str | None, now: datetime
) -> None:
    source = session.scalar(select(Source).where(Source.name == name))
    if source is None or not source.enabled:
        return
    source.consecutive_failures += 1
    source.last_failure_at = now
    source.last_error = error
    last_success = _as_utc(source.last_success_at)
    stale = last_success is None or now - last_success >= _STALE_AFTER
    if source.consecutive_failures >= 3:
        source.health_status = HealthStatus.FAILED
    elif stale:
        source.health_status = HealthStatus.STALE
    else:
        source.health_status = HealthStatus.DEGRADED


def _as_utc(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def main() -> None:
    asyncio.run(run_scan())


if __name__ == "__main__":
    main()
