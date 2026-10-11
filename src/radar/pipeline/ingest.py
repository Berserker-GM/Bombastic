"""Match raw collector rows to Events and record what changed."""

import hashlib
import json
import re
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from difflib import SequenceMatcher
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from radar.collectors.base import CollectResult, RawEvent
from radar.models import (
    Event,
    EventEvidence,
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
_HACKATHON_START_WINDOW = timedelta(days=2)
_HACKATHON_SIMILARITY = 0.9
_HACKATHON_NOISE = frozenset({"hackathon", "hackathons", "season", "seasons"})
_YEAR_WORD = re.compile(r"^(?:19|20)\d{2}$")
_COMPLETENESS_FIELDS = (
    "title",
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
)


@dataclass
class IngestResult:
    new: list[Event]
    changed: list[Event]
    unchanged: list[Event]


@dataclass
class SourceRunResult:
    result: SourceResult
    error: str | None = None
    warning: str | None = None
    warnings: list[str] = field(default_factory=list)
    new: list[Event] = field(default_factory=list)
    changed: list[Event] = field(default_factory=list)
    unchanged: list[Event] = field(default_factory=list)


def normalize_title(title: str) -> str:
    return _WHITESPACE.sub(" ", title.strip())


def fingerprint_title(title: str) -> str:
    folded = normalize_title(title).casefold()
    stripped = _PUNCTUATION.sub("", folded)
    return _WHITESPACE.sub(" ", stripped).strip()


def hackathon_match_key(title: str) -> str:
    words = [
        word
        for word in fingerprint_title(title).split()
        if word not in _HACKATHON_NOISE and _YEAR_WORD.fullmatch(word) is None
    ]
    return " ".join(words)


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
        return SourceRunResult(
            result=status,
            error=collect_result.error,
            warning=collect_result.warning,
            warnings=list(collect_result.warnings),
        )
    ingested = ingest(session, source, collect_result.events)
    status = (
        SourceResult.OK_NEW
        if ingested.new or ingested.changed
        else SourceResult.OK_NOTHING_NEW
    )
    return SourceRunResult(
        result=status,
        error=None,
        warning=collect_result.warning,
        warnings=list(collect_result.warnings),
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
    if existing is None and raw.category == EventCategory.HACKATHON:
        existing = _match_hackathon(session, source, raw)
    if existing is None:
        _insert(session, source, raw, snapshot, digest, fingerprint, now, result)
        return
    if raw.category == EventCategory.HACKATHON and (
        existing.source_id != source.id or _has_alt_sources(existing)
    ):
        _merge_hackathon(session, source, existing, raw, now, result)
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
    _persist_evidence(session, existing, raw, now)
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
    _persist_evidence(session, event, raw, now)
    session.flush()
    result.new.append(event)


def _persist_evidence(
    session: Session,
    event: Event,
    raw: RawEvent,
    now: datetime,
) -> None:
    for item in raw.evidence:
        session.add(
            EventEvidence(
                event=event,
                field=item.field,
                source_url=item.source_url,
                source_text=item.source_text,
                confidence=item.confidence,
                captured_at=_as_utc(item.captured_at) or now,
            )
        )


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


def _match_hackathon(session: Session, source: Source, raw: RawEvent) -> Event | None:
    key = hackathon_match_key(raw.title)
    if not key or source.id is None:
        return None
    rows = session.scalars(
        select(Event).where(
            Event.category == EventCategory.HACKATHON,
            Event.source_id != source.id,
        )
    ).all()
    found = [event for event in rows if _same_hackathon(event, raw, key)]
    if len(found) != 1:
        return None
    return found[0]


def _same_hackathon(event: Event, raw: RawEvent, raw_key: str) -> bool:
    event_key = hackathon_match_key(event.title)
    if not event_key:
        return False
    event_start = _as_utc(event.start_at)
    raw_start = _as_utc(raw.start_at)
    if event_start is None and raw_start is None:
        return event_key == raw_key
    if event_start is None or raw_start is None:
        return False
    if abs(event_start - raw_start) > _HACKATHON_START_WINDOW:
        return False
    if event_key == raw_key:
        return True
    return SequenceMatcher(None, event_key, raw_key).ratio() >= _HACKATHON_SIMILARITY


def _merge_hackathon(
    session: Session,
    source: Source,
    event: Event,
    raw: RawEvent,
    now: datetime,
    result: IngestResult,
) -> None:
    previous = _snapshot_from_event(event)
    chosen = _chosen_hackathon_fields(event, raw)
    owner = session.get(Source, event.source_id)
    owner_name = owner.name if owner is not None else str(event.source_id)
    chosen["extra"] = _with_alt_sources(
        chosen["extra"],
        _source_urls(event, owner_name, source.name, raw.url),
    )
    merged = _snapshot_from_event_values(event, chosen)
    changed_fields = _diff(previous, merged)
    if not changed_fields:
        event.last_seen_at = now
        result.unchanged.append(event)
        return
    _write_hackathon(event, chosen, _content_hash(merged), now)
    _add_version(session, event, now, changed_fields, merged)
    _persist_evidence(session, event, raw, now)
    result.changed.append(event)


def _chosen_hackathon_fields(event: Event, raw: RawEvent) -> dict[str, Any]:
    current = _event_values(event)
    incoming = _raw_values(raw)
    prefer_incoming = _completeness(incoming) > _completeness(current)
    chosen: dict[str, Any] = {}
    for name in _COMPLETENESS_FIELDS:
        chosen[name] = _prefer_field(
            name,
            current.get(name),
            incoming.get(name),
            prefer_incoming=prefer_incoming,
        )
    chosen["title"] = normalize_title(str(chosen["title"]))
    chosen["confidence"] = _prefer_field(
        "confidence",
        current.get("confidence"),
        incoming.get("confidence"),
        prefer_incoming=prefer_incoming,
    )
    chosen["extra"] = _merge_extra(
        current.get("extra"),
        incoming.get("extra"),
        prefer_incoming=prefer_incoming,
    )
    if chosen["lifecycle"] is None:
        chosen["lifecycle"] = event.lifecycle or Lifecycle.UNKNOWN
    if chosen["tags"] is not None:
        chosen["tags"] = list(chosen["tags"])
    return chosen


def _event_values(event: Event) -> dict[str, Any]:
    return {
        "title": event.title,
        "url": event.url,
        "start_at": event.start_at,
        "end_at": event.end_at,
        "registration_deadline": event.registration_deadline,
        "location": event.location,
        "mode": event.mode,
        "prize_text": event.prize_text,
        "prize_amount_inr": event.prize_amount_inr,
        "organizer": event.organizer,
        "sponsors": event.sponsors,
        "eligibility": event.eligibility,
        "team_min": event.team_min,
        "team_max": event.team_max,
        "tags": event.tags,
        "lifecycle": event.lifecycle,
        "confidence": event.confidence,
        "extra": event.extra,
    }


def _raw_values(raw: RawEvent) -> dict[str, Any]:
    return {
        "title": raw.title,
        "url": raw.url,
        "start_at": raw.start_at,
        "end_at": raw.end_at,
        "registration_deadline": raw.registration_deadline,
        "location": raw.location,
        "mode": raw.mode,
        "prize_text": raw.prize_text,
        "prize_amount_inr": raw.prize_amount_inr,
        "organizer": raw.organizer,
        "sponsors": raw.sponsors,
        "eligibility": raw.eligibility,
        "team_min": raw.team_min,
        "team_max": raw.team_max,
        "tags": raw.tags,
        "lifecycle": raw.lifecycle,
        "confidence": raw.confidence,
        "extra": raw.extra,
    }


def _completeness(values: dict[str, Any]) -> int:
    score = sum(1 for name in _COMPLETENESS_FIELDS if _filled(name, values.get(name)))
    extra = values.get("extra")
    if isinstance(extra, dict) and any(
        key != "alt_sources" and _filled(key, value) for key, value in extra.items()
    ):
        score += 1
    return score


def _prefer_field(
    name: str,
    current: Any,
    incoming: Any,
    *,
    prefer_incoming: bool,
) -> Any:
    current_ok = _filled(name, current)
    incoming_ok = _filled(name, incoming)
    if not incoming_ok:
        return current
    if not current_ok:
        return incoming
    if prefer_incoming:
        return incoming
    return current


def _merge_extra(
    current: Any,
    incoming: Any,
    *,
    prefer_incoming: bool,
) -> dict[str, Any] | None:
    current_extra = dict(current) if isinstance(current, dict) else {}
    incoming_extra = dict(incoming) if isinstance(incoming, dict) else {}
    current_extra.pop("alt_sources", None)
    incoming_extra.pop("alt_sources", None)
    merged: dict[str, Any] = {}
    for key in set(current_extra) | set(incoming_extra):
        value = _prefer_field(
            key,
            current_extra.get(key),
            incoming_extra.get(key),
            prefer_incoming=prefer_incoming,
        )
        if _filled(key, value):
            merged[key] = value
    return merged or None


def _source_urls(
    event: Event,
    owner_name: str,
    incoming_name: str,
    incoming_url: str | None,
) -> list[tuple[str, str]]:
    pairs: list[tuple[str, str]] = []

    def add(name: Any, url: Any) -> None:
        if not isinstance(name, str) or not name.strip():
            return
        if not isinstance(url, str) or not url.strip():
            return
        key = name.strip()
        pairs[:] = [pair for pair in pairs if pair[0] != key]
        pairs.append((key, url.strip()))

    extra = event.extra if isinstance(event.extra, dict) else {}
    listed = extra.get("alt_sources")
    if isinstance(listed, list):
        for entry in listed:
            if isinstance(entry, dict):
                add(entry.get("source"), entry.get("url"))
    if not listed:
        add(owner_name, event.url)
    add(incoming_name, incoming_url)
    return pairs


def _with_alt_sources(
    extra: dict[str, Any] | None,
    pairs: list[tuple[str, str]],
) -> dict[str, Any] | None:
    merged = dict(extra or {})
    alts = [{"source": name, "url": url} for name, url in pairs]
    alts.sort(key=lambda item: (item["source"], item["url"]))
    if alts:
        merged["alt_sources"] = alts
    else:
        merged.pop("alt_sources", None)
    return merged or None


def _snapshot_from_event_values(event: Event, chosen: dict[str, Any]) -> dict[str, Any]:
    return _snapshot(
        title=chosen["title"],
        category=event.category,
        url=chosen["url"],
        start_at=chosen["start_at"],
        end_at=chosen["end_at"],
        registration_deadline=chosen["registration_deadline"],
        location=chosen["location"],
        mode=chosen["mode"],
        prize_text=chosen["prize_text"],
        prize_amount_inr=chosen["prize_amount_inr"],
        organizer=chosen["organizer"],
        sponsors=chosen["sponsors"],
        eligibility=chosen["eligibility"],
        team_min=chosen["team_min"],
        team_max=chosen["team_max"],
        tags=chosen["tags"],
        lifecycle=chosen["lifecycle"],
        confidence=chosen["confidence"],
        extra=chosen["extra"],
    )


def _write_hackathon(
    event: Event, chosen: dict[str, Any], digest: str, now: datetime
) -> None:
    event.title = chosen["title"]
    event.fingerprint = fingerprint_title(chosen["title"])
    event.url = chosen["url"]
    event.start_at = _as_utc(chosen["start_at"])
    event.end_at = _as_utc(chosen["end_at"])
    event.registration_deadline = _as_utc(chosen["registration_deadline"])
    event.location = chosen["location"]
    event.mode = chosen["mode"]
    event.prize_text = chosen["prize_text"]
    event.prize_amount_inr = chosen["prize_amount_inr"]
    event.organizer = chosen["organizer"]
    event.sponsors = chosen["sponsors"]
    event.eligibility = chosen["eligibility"]
    event.team_min = chosen["team_min"]
    event.team_max = chosen["team_max"]
    event.tags = chosen["tags"]
    event.lifecycle = chosen["lifecycle"] or Lifecycle.UNKNOWN
    event.confidence = chosen["confidence"]
    event.extra = chosen["extra"]
    event.content_hash = digest
    event.last_seen_at = now


def _filled(name: str, value: Any) -> bool:
    if value is None:
        return False
    if name == "mode" and value == EventMode.UNKNOWN:
        return False
    if name == "lifecycle" and value == Lifecycle.UNKNOWN:
        return False
    if isinstance(value, str) and not value.strip():
        return False
    if isinstance(value, list | dict) and len(value) == 0:
        return False
    return True


def _has_alt_sources(event: Event) -> bool:
    extra = event.extra
    if not isinstance(extra, dict):
        return False
    listed = extra.get("alt_sources")
    return isinstance(listed, list) and len(listed) > 0


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
