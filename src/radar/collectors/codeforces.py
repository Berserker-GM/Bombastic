"""Codeforces contest.list collector. Live HTTP runs only from ``__main__``."""

import asyncio
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx
from pydantic import ValidationError

from radar.collectors.base import Collector, CollectResult, RawEvent
from radar.collectors.platforms import platform_slug
from radar.models.enums import EventCategory, Lifecycle

CONTEST_LIST_URL = "https://codeforces.com/api/contest.list"
_SOURCE_NAME = "codeforces"
_TIMEOUT_SECONDS = 30.0


class CodeforcesCollector(Collector):
    def __init__(self) -> None:
        self._warnings: list[str] = []
        self._failed_items = 0

    async def fetch(self) -> list[RawEvent]:
        self._warnings = []
        self._failed_items = 0
        async with httpx.AsyncClient(timeout=_TIMEOUT_SECONDS) as client:
            response = await client.get(CONTEST_LIST_URL)
            response.raise_for_status()
            payload = response.json()
        events, warnings, failed = parse_contest_list(payload)
        self._warnings = warnings
        self._failed_items = failed
        return events

    def _from_events(self, events: list[RawEvent]) -> CollectResult:
        if not events and self._failed_items:
            message = "every item failed: " + "; ".join(self._warnings)
            return CollectResult(
                status="malformed",
                events=[],
                error=message,
                warnings=list(self._warnings),
            )
        result = super()._from_events(events)
        if not self._warnings:
            return result
        return result.model_copy(
            update={
                "warnings": list(self._warnings),
                "warning": "; ".join(self._warnings),
            }
        )


def events_from_contest_list(payload: dict[str, Any]) -> list[RawEvent]:
    events, _warnings, _failed = parse_contest_list(payload)
    return events


def parse_contest_list(
    payload: dict[str, Any],
) -> tuple[list[RawEvent], list[str], int]:
    contests = payload["result"]
    if not isinstance(contests, list):
        raise TypeError("result is not a list")
    events: list[RawEvent] = []
    warnings: list[str] = []
    failed = 0
    for index, contest in enumerate(contests):
        try:
            event = _upcoming_event(contest)
        except (KeyError, TypeError, ValueError, ValidationError, OverflowError) as exc:
            failed += 1
            warnings.append(f"{_contest_label(contest, index)}: {exc}")
            continue
        if event is not None:
            events.append(event)
    return events, warnings, failed


def _contest_label(contest: Any, index: int) -> str:
    if isinstance(contest, dict) and contest.get("id") is not None:
        return f"contest {contest['id']}"
    return f"item {index}"


def _upcoming_event(contest: Any) -> RawEvent | None:
    if not isinstance(contest, dict):
        raise TypeError("contest is not an object")
    if contest["phase"] != "BEFORE":
        return None
    start_at = datetime.fromtimestamp(contest["startTimeSeconds"], UTC)
    end_at = start_at + timedelta(seconds=contest["durationSeconds"])
    contest_id = contest["id"]
    return RawEvent(
        source_name=_SOURCE_NAME,
        title=contest["name"],
        category=EventCategory.CONTEST,
        external_id=str(contest_id),
        url=f"https://codeforces.com/contest/{contest_id}",
        start_at=start_at,
        end_at=end_at,
        lifecycle=Lifecycle.UPCOMING,
        tags=[platform_slug("codeforces.com")],
    )


def main() -> None:
    result = asyncio.run(CodeforcesCollector().collect())
    print(result.model_dump_json(indent=2))


if __name__ == "__main__":
    main()
