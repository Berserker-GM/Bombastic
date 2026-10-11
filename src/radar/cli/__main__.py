"""Scan a source and ingest the result."""

import argparse
import asyncio
import json
import sys
from collections.abc import Sequence
from datetime import UTC, datetime

from sqlalchemy import select

from radar.db import SessionLocal
from radar.models import EventVersion, Source
from radar.pipeline.ingest import SourceRunResult
from radar.pipeline.scan import ScanAlreadyRunning, run_scan
from radar.pipeline.sources import (
    SCANNERS as _SCANNERS,
)
from radar.pipeline.sources import (
    scan_clist,
    scan_codeforces,
    scan_devfolio,
    scan_devpost,
    scan_unstop,
)

__all__ = [
    "main",
    "scan_clist",
    "scan_codeforces",
    "scan_devfolio",
    "scan_devpost",
    "scan_unstop",
]


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="radar.cli")
    sub = parser.add_subparsers(dest="command", required=True)
    scan = sub.add_parser("scan")
    scan.add_argument("source")
    sub.add_parser("scan-all")
    sub.add_parser("sources")
    args = parser.parse_args(argv)
    if args.command == "scan-all":
        _scan_all()
        return
    if args.command == "sources":
        _print_sources()
        return
    if args.command != "scan" or args.source not in _SCANNERS:
        parser.error(f"unknown source {args.source}")
    with SessionLocal() as session:
        outcome = asyncio.run(_SCANNERS[args.source](session))
        changes = _change_details(outcome)
        session.commit()
    _print_outcome(outcome, changes)


def format_health_table(sources: Sequence[Source]) -> str:
    header = ("name", "health", "failures", "last_success", "last_error")
    body = [
        (
            source.name,
            source.health_status.value,
            str(source.consecutive_failures),
            _format_dt(source.last_success_at),
            source.last_error or "-",
        )
        for source in sources
    ]
    widths = [len(cell) for cell in header]
    for row in body:
        for index, cell in enumerate(row):
            widths[index] = max(widths[index], len(cell))

    def render(row: tuple[str, ...]) -> str:
        return "  ".join(cell.ljust(widths[index]) for index, cell in enumerate(row))

    lines = [render(header)]
    lines.extend(render(row) for row in body)
    return "\n".join(lines)


def _scan_all() -> None:
    with SessionLocal() as session:
        try:
            code = run_scan(session)
        except ScanAlreadyRunning as exc:
            print(str(exc), file=sys.stderr)
            raise SystemExit(1) from exc
    raise SystemExit(code)


def _print_sources() -> None:
    with SessionLocal() as session:
        sources = list(session.scalars(select(Source).order_by(Source.name)).all())
    print(format_health_table(sources))


def _format_dt(value: datetime | None) -> str:
    if value is None:
        return "-"
    if value.tzinfo is None:
        utc = value.replace(tzinfo=UTC)
    else:
        utc = value.astimezone(UTC)
    return utc.strftime("%Y-%m-%dT%H:%M:%SZ")


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
    payload: dict[str, object] = {
        "result": outcome.result.value,
        "error": outcome.error,
        "new": len(outcome.new),
        "changed": len(outcome.changed),
        "unchanged": len(outcome.unchanged),
        "changes": changes,
    }
    if outcome.warning:
        payload["warning"] = outcome.warning
    print(json.dumps(payload, indent=2))


if __name__ == "__main__":
    main()
