"""Live demo: create, update, then delete one fake event in your Google Calendar.

Run from the repo root:  uv run python scripts/demo_calendar_event.py

It goes through the real GoogleCalendarService (CalendarLink bookkeeping included)
using a throwaway in-memory database, so nothing is written to radar.db.
Uses GOOGLE_CALENDAR_ID (default "primary") and token.json / GOOGLE_TOKEN_JSON.
"""

from datetime import UTC, datetime, timedelta

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from radar.calendar.google import GoogleCalendarService, build_google_events_client
from radar.models import Event, Source
from radar.models.enums import EventCategory, HealthStatus, Lifecycle, SourceKind
from radar.models.schema import Base

USER = "demo"
TITLE = "Radar demo event"


def run(service: GoogleCalendarService, *, pause=input) -> None:
    engine = create_engine("sqlite://")
    Base.metadata.create_all(engine)
    now = datetime.now(UTC)
    start = now + timedelta(minutes=5)
    with Session(engine) as session:
        source = Source(
            kind=SourceKind.API,
            name="demo",
            config={},
            enabled=True,
            health_status=HealthStatus.HEALTHY,
            consecutive_failures=0,
        )
        session.add(source)
        session.flush()
        event = Event(
            source_id=source.id,
            fingerprint="radar demo event",
            category=EventCategory.CONTEST,
            title=TITLE,
            url="https://example.com/radar-demo",
            start_at=start,
            end_at=start + timedelta(minutes=30),
            lifecycle=Lifecycle.UPCOMING,
            first_seen_at=now,
            last_seen_at=now,
            content_hash="v1",
        )
        session.add(event)
        session.flush()

        link = service.add_to_calendar(session, USER, event.id)
        if link is None:
            raise SystemExit("Nothing was created (event had no usable time).")
        local = start.astimezone()
        print(f"1) CREATED '{TITLE}' at {local:%H:%M} local. id={link.google_event_id}")
        pause("   Look at your calendar, then press Enter to simulate a change... ")

        event.title = f"{TITLE} (UPDATED)"
        event.start_at = start + timedelta(minutes=30)
        event.end_at = event.start_at + timedelta(minutes=30)
        event.content_hash = "v2"
        session.flush()
        updated = service.add_to_calendar(session, USER, event.id)
        assert updated is not None
        same = updated.google_event_id == link.google_event_id
        print(f"2) UPDATED. Same Google event id: {same} (it moved, no duplicate)")
        pause("   Check it moved 30 minutes later, then press Enter to delete it... ")

        service.delete(session, USER, event)
        print("3) DELETED. The event should be gone from your calendar.")


def main() -> None:
    run(GoogleCalendarService(build_google_events_client()))


if __name__ == "__main__":
    main()