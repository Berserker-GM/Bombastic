"""Codeforces contest.list collector. Live HTTP runs only from ``__main__``."""

import asyncio
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx

from radar.collectors.base import Collector, RawEvent
from radar.collectors.platforms import platform_slug
from radar.models.enums import EventCategory, Lifecycle

CONTEST_LIST_URL = "https://codeforces.com/api/contest.list"
_SOURCE_NAME = "codeforces"
_TIMEOUT_SECONDS = 30.0


class CodeforcesCollector(Collector):
    async def fetch(self) -> list[RawEvent]:
        async with httpx.AsyncClient(timeout=_TIMEOUT_SECONDS) as client:
            response = await client.get(CONTEST_LIST_URL)
            response.raise_for_status()
            payload = response.json()
        return events_from_contest_list(payload)


def events_from_contest_list(payload: dict[str, Any]) -> list[RawEvent]:
    contests = payload["result"]
    if not isinstance(contests, list):
        raise TypeError("result is not a list")
    events: list[RawEvent] = []
    for contest in contests:
        event = _upcoming_event(contest)
        if event is not None:
            events.append(event)
    return events


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
