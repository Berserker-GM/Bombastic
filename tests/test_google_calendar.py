"""Live Google Calendar round trip. Skipped unless RADAR_LIVE_TESTS=1."""

import os
from datetime import UTC, datetime, timedelta

import pytest

from radar.calendar.google import build_google_events_client
from radar.config import get_settings

pytestmark = pytest.mark.live

_SKIP = pytest.mark.skipif(
    os.environ.get("RADAR_LIVE_TESTS") != "1",
    reason="set RADAR_LIVE_TESTS=1 to call Google Calendar",
)


@_SKIP
def test_live_event_is_created_in_configured_calendar_and_deleted() -> None:
    settings = get_settings()
    client = build_google_events_client()
    start = datetime.now(UTC) + timedelta(days=30)
    end = start + timedelta(minutes=30)
    created: list[str] = []
    try:
        google_event_id = client.insert(
            {
                "summary": "radar live test",
                "start": {"dateTime": start.isoformat(), "timeZone": "UTC"},
                "end": {"dateTime": end.isoformat(), "timeZone": "UTC"},
            }
        )
        created.append(google_event_id)
        assert client.calendar_id == settings.google_calendar_id
    finally:
        for google_event_id in created:
            client.delete(google_event_id)
