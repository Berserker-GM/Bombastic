"""Google Calendar sync. One Google event per user and Event."""

from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Protocol

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from sqlalchemy import select
from sqlalchemy.orm import Session

from radar.config import get_settings
from radar.models import CalendarLink, Event, UserPrefs
from radar.models.enums import EventCategory
from radar.pipeline.scoring import key_datetime

# Popup minutes before the calendar item. duration_minutes is the length used
# when the source has a single instant (a deadline) rather than an end time.
REMINDERS: dict[str, Any] = {
    "contest_popup_minutes": (30,),
    "college_popup_minutes": (3 * 24 * 60, 3 * 60),
    "hackathon_popup_minutes": (24 * 60,),
    "duration_minutes": 30,
}

_SCOPES = ["https://www.googleapis.com/auth/calendar.events"]
_CALENDAR_ID = "primary"
_POPUP_KEYS = {
    EventCategory.CONTEST: "contest_popup_minutes",
    EventCategory.COLLEGE: "college_popup_minutes",
    EventCategory.HACKATHON: "hackathon_popup_minutes",
}


class CalendarService(Protocol):
    """Upsert and delete. A hosted OAuth implementation can replace Google later."""

    def upsert_event(
        self, session: Session, user_id: str, event: Event
    ) -> CalendarLink | None: ...

    def delete(self, session: Session, user_id: str, event: Event) -> None: ...


class GoogleCalendarClient(Protocol):
    def insert(self, body: dict[str, Any]) -> str: ...

    def update(self, google_event_id: str, body: dict[str, Any]) -> None: ...

    def delete(self, google_event_id: str) -> None: ...


class GoogleApiEvents:
    """Adapter over the discovery client's events resource."""

    def __init__(self, resource: Any, calendar_id: str = _CALENDAR_ID) -> None:
        self._resource = resource
        self._calendar_id = calendar_id

    def insert(self, body: dict[str, Any]) -> str:
        created = (
            self._resource.events()
            .insert(calendarId=self._calendar_id, body=body)
            .execute()
        )
        event_id = created.get("id")
        if not isinstance(event_id, str) or not event_id:
            raise RuntimeError("Google event id missing")
        return event_id

    def update(self, google_event_id: str, body: dict[str, Any]) -> None:
        (
            self._resource.events()
            .update(
                calendarId=self._calendar_id,
                eventId=google_event_id,
                body=body,
            )
            .execute()
        )

    def delete(self, google_event_id: str) -> None:
        (
            self._resource.events()
            .delete(calendarId=self._calendar_id, eventId=google_event_id)
            .execute()
        )


class GoogleCalendarService:
    def __init__(self, client: GoogleCalendarClient) -> None:
        self._client = client

    def upsert_event(
        self, session: Session, user_id: str, event: Event
    ) -> CalendarLink | None:
        if _calendar_mode(session, user_id) != "auto":
            return _existing_link(session, user_id, event.id)
        return self._sync(session, user_id, event)

    def add_to_calendar(
        self, session: Session, user_id: str, event_id: int
    ) -> CalendarLink | None:
        event = session.get(Event, event_id)
        if event is None:
            raise ValueError("event not found")
        return self._sync(session, user_id, event)

    def delete(self, session: Session, user_id: str, event: Event) -> None:
        link = _existing_link(session, user_id, event.id)
        if link is None:
            return
        self._client.delete(link.google_event_id)
        session.delete(link)
        session.flush()

    def _sync(
        self, session: Session, user_id: str, event: Event
    ) -> CalendarLink | None:
        link = _existing_link(session, user_id, event.id)
        if link is not None and link.last_synced_hash == event.content_hash:
            return link
        body = google_event_body(event)
        if link is None:
            google_event_id = self._client.insert(body)
            link = CalendarLink(
                user_id=user_id,
                event_id=event.id,
                google_event_id=google_event_id,
                last_synced_hash=event.content_hash,
            )
            session.add(link)
        else:
            self._client.update(link.google_event_id, body)
            link.last_synced_hash = event.content_hash
        session.flush()
        return link


def google_event_body(event: Event) -> dict[str, Any]:
    start, end = _interval(event)
    popup = REMINDERS.get(_POPUP_KEYS.get(event.category, ""), ())
    minutes = tuple(popup) if isinstance(popup, tuple) else ()
    body: dict[str, Any] = {
        "summary": event.title,
        "start": {"dateTime": start.isoformat(), "timeZone": "UTC"},
        "end": {"dateTime": end.isoformat(), "timeZone": "UTC"},
        "reminders": {
            "useDefault": False,
            "overrides": [{"method": "popup", "minutes": minute} for minute in minutes],
        },
    }
    if event.url:
        body["description"] = event.url
    if event.location:
        body["location"] = event.location
    return body


def build_google_events_client(token_path: Path | None = None) -> GoogleApiEvents:
    path = token_path or Path(get_settings().google_token_path)
    creds = _load_credentials(path)
    service = build("calendar", "v3", credentials=creds, cache_discovery=False)
    return GoogleApiEvents(service)


def write_local_token(client_secrets_path: Path, token_path: Path) -> None:
    """Run the local browser OAuth flow and write token.json."""
    flow = InstalledAppFlow.from_client_secrets_file(str(client_secrets_path), _SCOPES)
    creds = flow.run_local_server(port=0)
    token_path.write_text(creds.to_json(), encoding="utf-8")


def _load_credentials(path: Path) -> Credentials:
    # token.json on disk is a development stand-in.
    # Production needs encrypted storage for the refresh token.
    creds = Credentials.from_authorized_user_file(str(path), _SCOPES)
    if creds.expired and creds.refresh_token:
        creds.refresh(Request())
        path.write_text(creds.to_json(), encoding="utf-8")
    return creds


def _interval(event: Event) -> tuple[datetime, datetime]:
    start = key_datetime(event)
    if start is None:
        raise ValueError("event has no calendar time")
    if event.category == EventCategory.CONTEST:
        end = _as_utc(event.end_at)
        if end is not None and end > start:
            return start, end
    duration = REMINDERS["duration_minutes"]
    if isinstance(duration, bool) or not isinstance(duration, int):
        raise ValueError("duration_minutes must be an int")
    return start, start + timedelta(minutes=duration)


def _calendar_mode(session: Session, user_id: str) -> str | None:
    row = session.get(UserPrefs, user_id)
    if row is None or not isinstance(row.prefs, dict):
        return None
    mode = row.prefs.get("calendar_mode")
    if mode in ("auto", "confirm"):
        return mode
    return None


def _existing_link(
    session: Session, user_id: str, event_id: int
) -> CalendarLink | None:
    return session.scalar(
        select(CalendarLink).where(
            CalendarLink.user_id == user_id,
            CalendarLink.event_id == event_id,
        )
    )


def _as_utc(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)
