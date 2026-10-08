"""Scan a source and ingest the result."""

import argparse
import asyncio
import json

from sqlalchemy import select
from sqlalchemy.orm import Session

from radar.collectors.clist import ClistCollector
from radar.collectors.codeforces import CodeforcesCollector
from radar.db import SessionLocal
from radar.models import EventVersion, HealthStatus, Source, SourceKind
from radar.pipeline.ingest import SourceRunResult, ingest_run

_CODEFORCES = "codeforces"
_CLIST = "clist"


async def scan_codeforces(session: Session) -> SourceRunResult:
    collected = await CodeforcesCollector().collect()
    return ingest_run(session, _get_source(session, _CODEFORCES), collected)


async def scan_clist(session: Session) -> SourceRunResult:
    collected = await ClistCollector().collect()
    return ingest_run(session, _get_source(session, _CLIST), collected)


_SCANNERS = {
    _CODEFORCES: scan_codeforces,
    _CLIST: scan_clist,
}


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="radar.cli")
    sub = parser.add_subparsers(dest="command", required=True)
    scan = sub.add_parser("scan")
    scan.add_argument("source")
    args = parser.parse_args(argv)
    if args.command != "scan" or args.source not in _SCANNERS:
        parser.error(f"unknown source {args.source}")
    with SessionLocal() as session:
        outcome = asyncio.run(_SCANNERS[args.source](session))
        changes = _change_details(outcome)
        session.commit()
    _print_outcome(outcome, changes)


def _get_source(session: Session, name: str) -> Source:
    source = session.scalar(select(Source).where(Source.name == name))
    if source is not None:
        return source
    source = Source(
        kind=SourceKind.API,
        name=name,
        config={},
        enabled=True,
        health_status=HealthStatus.HEALTHY,
        consecutive_failures=0,
    )
    session.add(source)
    session.flush()
    return source


def _change_details(outcome: SourceRunResult) -> list[dict[str, object]]:
    details: list[dict[str, object]] = []
    for event in outcome.changed:
        version = _latest_version(event.versions)
        details.append(
            {
                "event_id": event.id,
                "external_id": event.external_id,
                "title": event.title,
                "importance": version.importance.value,
                "changed_fields": version.changed_fields,
            }
        )
    return details


def _latest_version(versions: list[EventVersion]) -> EventVersion:
    return max(versions, key=lambda version: (version.seen_at, version.id or 0))


def _print_outcome(outcome: SourceRunResult, changes: list[dict[str, object]]) -> None:
    print(
        json.dumps(
            {
                "result": outcome.result.value,
                "error": outcome.error,
                "new": len(outcome.new),
                "changed": len(outcome.changed),
                "unchanged": len(outcome.unchanged),
                "changes": changes,
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
