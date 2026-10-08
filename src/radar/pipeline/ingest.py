"""Match raw collector rows to Events and record what changed."""

import hashlib
import json
import re
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from radar.collectors.base import CollectResult, RawEvent
from radar.models import (
    Event,
    EventVersion,
    Importance,
    Lifecycle,
    Source,
    SourceResult,
)
from radar.models.enums import EventCategory, EventMode

IMPORTANT_FIELDS: tuple[str, ...] = (
    "registration_deadline",
    "start_at",
    "end_at",
    "location",
    "mode",
    "eligibility",
    "lifecycle",
    "prize_amount_inr",
)

TRACKED_FIELDS: tuple[str, ...] = (
    "title",
    "category",
    "url",
    "start_at",
    "end_at",
    "registration_deadline",
    "location",
    "mode",
    "prize_text",
    "prize_amount_inr",
    "organizer",
    "sponsors",
    "eligibility",
    "team_min",
    "team_max",
    "tags",
    "lifecycle",
    "confidence",
    "extra",
)

_WHITESPACE = re.compile(r"\s+")
_PUNCTUATION = re.compile(r"[^\w\s]", re.UNICODE)
_DATE_WINDOW = timedelta(days=3)


@dataclass
class IngestResult:
    new: list[Event]
    changed: list[Event]
    unchanged: list[Event]


@dataclass
class SourceRunResult:
    result: SourceResult
    error: str | None = None
    new: list[Event] = field(default_factory=list)
    changed: list[Event] = field(default_factory=list)
    unchanged: list[Event] = field(default_factory=list)


def normalize_title(title: str) -> str:
    return _WHITESPACE.sub(" ", title.strip())


def fingerprint_title(title: str) -> str:
    folded = normalize_title(title).casefold()
    stripped = _PUNCTUATION.sub("", folded)
    return _WHITESPACE.sub(" ", stripped).strip()


def ingest(
    session: Session, source: Source, raw_events: list[RawEvent]
) -> IngestResult:
    if source.id is None:
        session.flush()
    now = datetime.now(UTC)
    result = IngestResult(new=[], changed=[], unchanged=[])
    for raw in raw_events:
        _ingest_one(session, source, raw, now, result)
    return result


def ingest_run(
    session: Session, source: Source, collect_result: CollectResult
) -> SourceRunResult:
    if collect_result.status != "ok":
        status = (
            SourceResult.UNREACHABLE
            if collect_result.status == "unreachable"
            else SourceResult.MALFORMED
        )
        return SourceRunResult(result=status, error=collect_result.error)
    ingested = ingest(session, source, collect_result.events)
    status = (
        SourceResult.OK_NEW
        if ingested.new or ingested.changed
        else SourceResult.OK_NOTHING_NEW
    )
    return SourceRunResult(
        result=status,
        error=None,
        new=ingested.new,
        changed=ingested.changed,
        unchanged=ingested.unchanged,
    )


def _ingest_one(
    session: Session,
    source: Source,
    raw: RawEvent,
    now: datetime,
    result: IngestResult,
) -> None:
    snapshot = _snapshot_from_raw(raw)
    digest = _content_hash(snapshot)
    fingerprint = fingerprint_title(raw.title)
    existing = _match(session, source.id, raw, fingerprint)
    if existing is None:
        _insert(session, source, raw, snapshot, digest, fingerprint, now, result)
        return
    previous = _snapshot_from_event(existing)
    changed_fields = _diff(previous, snapshot)
    if existing.content_hash == digest and not changed_fields:
        existing.last_seen_at = now
        result.unchanged.append(existing)
        return
    if not changed_fields:
        existing.last_seen_at = now
        existing.content_hash = digest
        result.unchanged.append(existing)
        return
    _apply(existing, raw, digest, fingerprint, now)
    _add_version(session, existing, now, changed_fields, snapshot)
    result.changed.append(existing)


def _insert(
    session: Session,
    source: Source,
    raw: RawEvent,
    snapshot: dict[str, Any],
    digest: str,
    fingerprint: str,
    now: datetime,
    result: IngestResult,
) -> None:
    event = Event(
        source_id=source.id,
        external_id=raw.external_id,
        fingerprint=fingerprint,
        title=normalize_title(raw.title),
        category=raw.category,
        url=raw.url,
        start_at=_as_utc(raw.start_at),
        end_at=_as_utc(raw.end_at),
        registration_deadline=_as_utc(raw.registration_deadline),
        location=raw.location,
        mode=raw.mode,
        prize_text=raw.prize_text,
        prize_amount_inr=raw.prize_amount_inr,
        organizer=raw.organizer,
        sponsors=raw.sponsors,
        eligibility=raw.eligibility,
        team_min=raw.team_min,
        team_max=raw.team_max,
        tags=list(raw.tags) if raw.tags is not None else None,
        lifecycle=raw.lifecycle or Lifecycle.UNKNOWN,
        confidence=dict(raw.confidence),
        extra=raw.extra,
        first_seen_at=now,
        last_seen_at=now,
        content_hash=digest,
    )
    blank = dict.fromkeys(TRACKED_FIELDS)
    changed_fields = _diff(blank, snapshot)
    session.add(event)
    _add_version(
        session,
        event,
        now,
        changed_fields,
        snapshot,
        importance=Importance.MINOR,
    )
    session.flush()
    result.new.append(event)


def _apply(
    event: Event,
    raw: RawEvent,
    digest: str,
    fingerprint: str,
    now: datetime,
) -> None:
    event.title = normalize_title(raw.title)
    event.fingerprint = fingerprint
    event.category = raw.category
    event.url = raw.url
    event.start_at = _as_utc(raw.start_at)
    event.end_at = _as_utc(raw.end_at)
    event.registration_deadline = _as_utc(raw.registration_deadline)
    event.location = raw.location
    event.mode = raw.mode
    event.prize_text = raw.prize_text
    event.prize_amount_inr = raw.prize_amount_inr
    event.organizer = raw.organizer
    event.sponsors = raw.sponsors
    event.eligibility = raw.eligibility
    event.team_min = raw.team_min
    event.team_max = raw.team_max
    event.tags = list(raw.tags) if raw.tags is not None else None
    event.lifecycle = raw.lifecycle or Lifecycle.UNKNOWN
    event.confidence = dict(raw.confidence)
    event.extra = raw.extra
    event.content_hash = digest
    event.last_seen_at = now
    if raw.external_id is not None and event.external_id is None:
        event.external_id = raw.external_id


def _add_version(
    session: Session,
    event: Event,
    now: datetime,
    changed_fields: dict[str, list[Any]],
    snapshot: dict[str, Any],
    *,
    importance: Importance | None = None,
) -> None:
    session.add(
        EventVersion(
            event=event,
            seen_at=now,
            changed_fields=changed_fields,
            importance=(
                _importance(changed_fields) if importance is None else importance
            ),
            snapshot=snapshot,
        )
    )


def _match(
    session: Session, source_id: int, raw: RawEvent, fingerprint: str
) -> Event | None:
    if raw.external_id is not None:
        found = session.scalar(
            select(Event).where(
                Event.source_id == source_id,
                Event.external_id == raw.external_id,
            )
        )
        if found is not None:
            return found
    candidates = session.scalars(
        select(Event).where(
            Event.source_id == source_id,
            Event.fingerprint == fingerprint,
        )
    ).all()
    if raw.external_id is not None:
        candidates = [
            event
            for event in candidates
            if event.external_id in (None, raw.external_id)
        ]
    return _closest(candidates, raw)


def _closest(candidates: list[Event], raw: RawEvent) -> Event | None:
    ranked: list[tuple[float, int, Event]] = []
    for event in candidates:
        if not _dates_within_window(event, raw):
            continue
        ranked.append((_closeness(event, raw), event.id, event))
    if not ranked:
        return None
    ranked.sort()
    return ranked[0][2]


def _dates_within_window(event: Event, raw: RawEvent) -> bool:
    event_dated = _has_match_date(event.start_at, event.registration_deadline)
    raw_dated = _has_match_date(raw.start_at, raw.registration_deadline)
    if not event_dated and not raw_dated:
        return True
    checks = [
        _within_window(left, right)
        for left, right in (
            (event.start_at, raw.start_at),
            (event.registration_deadline, raw.registration_deadline),
        )
        if left is not None and right is not None
    ]
    return bool(checks) and all(checks)


def _has_match_date(start_at: datetime | None, deadline: datetime | None) -> bool:
    return start_at is not None or deadline is not None


def _within_window(left: datetime, right: datetime) -> bool:
    return abs(_as_utc(left) - _as_utc(right)) <= _DATE_WINDOW


def _closeness(event: Event, raw: RawEvent) -> float:
    deltas = [
        abs(_as_utc(left) - _as_utc(right)).total_seconds()
        for left, right in (
            (event.start_at, raw.start_at),
            (event.registration_deadline, raw.registration_deadline),
        )
        if left is not None and right is not None
    ]
    if not deltas:
        return 0.0
    return min(deltas)


def _snapshot_from_raw(raw: RawEvent) -> dict[str, Any]:
    return _snapshot(
        title=raw.title,
        category=raw.category,
        url=raw.url,
        start_at=raw.start_at,
        end_at=raw.end_at,
        registration_deadline=raw.registration_deadline,
        location=raw.location,
        mode=raw.mode,
        prize_text=raw.prize_text,
        prize_amount_inr=raw.prize_amount_inr,
        organizer=raw.organizer,
        sponsors=raw.sponsors,
        eligibility=raw.eligibility,
        team_min=raw.team_min,
        team_max=raw.team_max,
        tags=raw.tags,
        lifecycle=raw.lifecycle or Lifecycle.UNKNOWN,
        confidence=raw.confidence,
        extra=raw.extra,
    )


def _snapshot_from_event(event: Event) -> dict[str, Any]:
    return _snapshot(
        title=event.title,
        category=event.category,
        url=event.url,
        start_at=event.start_at,
        end_at=event.end_at,
        registration_deadline=event.registration_deadline,
        location=event.location,
        mode=event.mode,
        prize_text=event.prize_text,
        prize_amount_inr=event.prize_amount_inr,
        organizer=event.organizer,
        sponsors=event.sponsors,
        eligibility=event.eligibility,
        team_min=event.team_min,
        team_max=event.team_max,
        tags=event.tags,
        lifecycle=event.lifecycle,
        confidence=event.confidence,
        extra=event.extra,
    )


def _snapshot(
    *,
    title: str,
    category: EventCategory,
    url: str | None,
    start_at: datetime | None,
    end_at: datetime | None,
    registration_deadline: datetime | None,
    location: str | None,
    mode: EventMode | None,
    prize_text: str | None,
    prize_amount_inr: int | None,
    organizer: str | None,
    sponsors: Any | None,
    eligibility: str | None,
    team_min: int | None,
    team_max: int | None,
    tags: list[str] | None,
    lifecycle: Lifecycle,
    confidence: dict[str, Any] | None,
    extra: dict[str, Any] | None,
) -> dict[str, Any]:
    return {
        "title": normalize_title(title).casefold(),
        "category": category.value,
        "url": url,
        "start_at": _json_datetime(start_at),
        "end_at": _json_datetime(end_at),
        "registration_deadline": _json_datetime(registration_deadline),
        "location": location,
        "mode": None if mode is None else mode.value,
        "prize_text": prize_text,
        "prize_amount_inr": prize_amount_inr,
        "organizer": organizer,
        "sponsors": _json_ready(sponsors),
        "eligibility": eligibility,
        "team_min": team_min,
        "team_max": team_max,
        "tags": None if tags is None else [_json_ready(tag) for tag in tags],
        "lifecycle": lifecycle.value,
        "confidence": {} if confidence is None else _json_ready(confidence),
        "extra": _json_ready(extra),
    }


def _diff(old: dict[str, Any], new: dict[str, Any]) -> dict[str, list[Any]]:
    changed: dict[str, list[Any]] = {}
    for name in TRACKED_FIELDS:
        if old.get(name) != new.get(name):
            changed[name] = [old.get(name), new.get(name)]
    return changed


def _importance(changed_fields: dict[str, list[Any]]) -> Importance:
    if any(name in IMPORTANT_FIELDS for name in changed_fields):
        return Importance.IMPORTANT
    return Importance.MINOR


def _content_hash(snapshot: dict[str, Any]) -> str:
    payload = json.dumps(
        snapshot, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _as_utc(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _json_datetime(value: datetime | None) -> str | None:
    utc = _as_utc(value)
    if utc is None:
        return None
    return utc.isoformat()


def _json_ready(value: Any) -> Any:
    if isinstance(value, datetime):
        return _json_datetime(value)
    if isinstance(value, EventCategory | EventMode | Lifecycle):
        return value.value
    if isinstance(value, dict):
        return {str(key): _json_ready(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_ready(item) for item in value]
    return value
