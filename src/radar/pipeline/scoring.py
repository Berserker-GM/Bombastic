"""Relevance, urgency, and confidence stay separate. Nothing here sums them."""

import re
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta, timezone
from typing import Any, Literal

from sqlalchemy import select
from sqlalchemy.orm import Session

from radar.models import Event, EventVersion, Importance, UserEventState, UserPrefs
from radar.models.enums import EventCategory, EventMode, Urgency, UserState
from radar.pipeline.ingest import IMPORTANT_FIELDS

# Tune these. Relevance never folds in urgency or confidence.
WEIGHTS: dict[str, int] = {
    "college": 100,
    "contest_platform": 40,
    "prize": 25,
    "topic": 20,
    "company": 20,
}

# Lower tier sorts first. high_relevance_min is the cutoff inside tier 3.
FEED_TIERS: dict[str, int] = {
    "important_unseen": 1,
    "critical_deadline": 2,
    "new_high_relevance": 3,
    "soon_deadline": 4,
    "other": 5,
    "high_relevance_min": 80,
}

URGENCY_WITHIN: dict[str, timedelta] = {
    "critical": timedelta(hours=24),
    "soon": timedelta(days=3),
    "upcoming": timedelta(days=7),
}

CONFIDENCE_AT_LEAST: dict[str, float] = {
    "high": 0.9,
    "medium": 0.6,
}

_DATE_FIELDS: tuple[str, ...] = ("registration_deadline", "start_at", "end_at")
_DEADLINE_CATEGORIES = frozenset({EventCategory.COLLEGE, EventCategory.HACKATHON})
# Asia/Kolkata has no daylight-saving shift.
_DISPLAY_ZONE = timezone(timedelta(hours=5, minutes=30), name="Asia/Kolkata")
_MONTHS = (
    "Jan",
    "Feb",
    "Mar",
    "Apr",
    "May",
    "Jun",
    "Jul",
    "Aug",
    "Sep",
    "Oct",
    "Nov",
    "Dec",
)
_STUDENT_EXCLUSIONS: tuple[str, ...] = (
    "not for students",
    "not open to students",
    "no students",
    "non-student",
    "excludes students",
    "students not eligible",
    "students are not eligible",
    "professionals only",
    "professional only",
    "working professionals only",
)
_FAR_FUTURE = datetime.max.replace(tzinfo=UTC)

ConfidenceLabel = Literal["high", "medium", "low"]


@dataclass(frozen=True)
class Prefs:
    modes: tuple[str, ...] = ()
    locations: tuple[str, ...] = ()
    min_prize: int | None = None
    student_only: bool = False
    companies: tuple[str, ...] = ()
    topics: tuple[str, ...] = ()
    contest_platforms: tuple[str, ...] = ()


@dataclass(frozen=True)
class Relevance:
    score: int
    reasons: list[str]
    hidden: bool


@dataclass
class FeedItem:
    event_id: int
    title: str
    url: str | None
    relevance: int
    urgency: Urgency
    confidence_label: ConfidenceLabel
    reasons: list[str]
    diff: str | None
    key_at: datetime | None
    important_unseen: bool
    is_new: bool


@dataclass
class Feed:
    important_updates: list[FeedItem]
    deadlines_soon: list[FeedItem]
    new_opportunities: list[FeedItem]
    todays_contests: list[FeedItem]
    saved: list[FeedItem]
    college: list[FeedItem]
    hackathons: list[FeedItem]
    counts: dict[str, int]


def relevance(event: Event, prefs: Prefs | dict[str, Any]) -> Relevance:
    parsed = _coerce_prefs(prefs)
    if event.category == EventCategory.COLLEGE:
        return Relevance(_clamp(WEIGHTS["college"]), ["College"], False)
    if event.category == EventCategory.CONTEST:
        return _contest_relevance(event, parsed)
    if event.category == EventCategory.HACKATHON:
        return _hackathon_relevance(event, parsed)
    return Relevance(0, [], False)


def key_datetime(event: Event) -> datetime | None:
    if event.category in _DEADLINE_CATEGORIES:
        return _as_utc(event.registration_deadline)
    if event.category == EventCategory.CONTEST:
        return _as_utc(event.start_at)
    return None


def urgency(event: Event, now: datetime) -> Urgency:
    key = key_datetime(event)
    moment = _as_utc(now)
    if key is None or moment is None or key < moment:
        return Urgency.NONE
    remaining = key - moment
    if remaining < URGENCY_WITHIN["critical"]:
        return Urgency.CRITICAL
    if remaining < URGENCY_WITHIN["soon"]:
        return Urgency.SOON
    if remaining < URGENCY_WITHIN["upcoming"]:
        return Urgency.UPCOMING
    return Urgency.LATER


def confidence_label(event: Event) -> ConfidenceLabel:
    raw = event.confidence if isinstance(event.confidence, dict) else {}
    lowest = min(_field_confidence(raw, field) for field in _DATE_FIELDS)
    if lowest >= CONFIDENCE_AT_LEAST["high"]:
        return "high"
    if lowest >= CONFIDENCE_AT_LEAST["medium"]:
        return "medium"
    return "low"


def feed_tier(item: FeedItem) -> int:
    if item.important_unseen:
        return FEED_TIERS["important_unseen"]
    if item.urgency == Urgency.CRITICAL:
        return FEED_TIERS["critical_deadline"]
    if item.is_new and item.relevance >= FEED_TIERS["high_relevance_min"]:
        return FEED_TIERS["new_high_relevance"]
    if item.urgency == Urgency.SOON:
        return FEED_TIERS["soon_deadline"]
    return FEED_TIERS["other"]


def feed_sort_key(item: FeedItem) -> tuple[int, int, datetime]:
    when = item.key_at if item.key_at is not None else _FAR_FUTURE
    return (feed_tier(item), -item.relevance, when)


def build_feed(session: Session, user_id: str, now: datetime) -> Feed:
    moment = _as_utc(now)
    if moment is None:
        raise ValueError("now is required")
    parsed = _load_prefs(session, user_id)
    states = {
        row.event_id: row
        for row in session.scalars(
            select(UserEventState).where(UserEventState.user_id == user_id)
        )
    }
    versions: dict[int, list[EventVersion]] = {}
    for version in session.scalars(select(EventVersion)):
        versions.setdefault(version.event_id, []).append(version)

    important_updates: list[FeedItem] = []
    deadlines_soon: list[FeedItem] = []
    new_opportunities: list[FeedItem] = []
    todays_contests: list[FeedItem] = []
    saved: list[FeedItem] = []
    college: list[FeedItem] = []
    hackathons: list[FeedItem] = []

    for event in session.scalars(select(Event)):
        state = states.get(event.id)
        if state is not None and state.state == UserState.IGNORED:
            continue
        scored = relevance(event, parsed)
        is_saved = state is not None and state.state == UserState.SAVED
        if scored.hidden and not is_saved:
            continue
        item = _feed_item(event, scored, state, versions.get(event.id, []), moment)
        if is_saved:
            saved.append(item)
        if scored.hidden:
            continue
        if item.important_unseen:
            important_updates.append(item)
        if item.urgency in (Urgency.CRITICAL, Urgency.SOON):
            deadlines_soon.append(item)
        if item.is_new:
            new_opportunities.append(item)
        if event.category == EventCategory.CONTEST and _is_today(
            event.start_at, moment
        ):
            todays_contests.append(item)
        if event.category == EventCategory.COLLEGE:
            college.append(item)
        if event.category == EventCategory.HACKATHON:
            hackathons.append(item)

    sections = (
        important_updates,
        deadlines_soon,
        new_opportunities,
        todays_contests,
        saved,
        college,
        hackathons,
    )
    for section in sections:
        section.sort(key=feed_sort_key)
    return Feed(
        important_updates=important_updates,
        deadlines_soon=deadlines_soon,
        new_opportunities=new_opportunities,
        todays_contests=todays_contests,
        saved=saved,
        college=college,
        hackathons=hackathons,
        counts={
            "important_updates": len(important_updates),
            "deadlines_soon": len(deadlines_soon),
            "new": len(new_opportunities),
            "saved": len(saved),
        },
    )


def prefs_from_mapping(raw: dict[str, Any]) -> Prefs:
    return Prefs(
        modes=_strings(raw.get("modes")),
        locations=_strings(raw.get("locations")),
        min_prize=_prize(raw.get("min_prize")),
        student_only=raw.get("student_only") is True,
        companies=_strings(raw.get("companies")),
        topics=_strings(raw.get("topics")),
        contest_platforms=_strings(raw.get("contest_platforms")),
    )


def _coerce_prefs(prefs: Prefs | dict[str, Any]) -> Prefs:
    if isinstance(prefs, Prefs):
        return prefs
    return prefs_from_mapping(prefs)


def _contest_relevance(event: Event, prefs: Prefs) -> Relevance:
    matched = _matching_names(prefs.contest_platforms, _tags(event))
    if not matched:
        return Relevance(0, [], True)
    return Relevance(
        _clamp(WEIGHTS["contest_platform"]),
        [f"Platform: {name}" for name in matched],
        False,
    )


def _hackathon_relevance(event: Event, prefs: Prefs) -> Relevance:
    hidden = _excludes_students(event.eligibility) if prefs.student_only else False
    if _mode_hides(event.mode, prefs.modes):
        hidden = True
    location_name = _matched_location(event, prefs.locations)
    if not _location_passes(event, prefs.locations):
        hidden = True

    reasons: list[str] = []
    score = 0
    if event.mode in (EventMode.ONLINE, EventMode.OFFLINE, EventMode.HYBRID):
        reasons.append(event.mode.value.capitalize())
    if location_name is not None:
        reasons.append(f"Location: {location_name}")
    if _prize_matches(event, prefs):
        score += WEIGHTS["prize"]
        reasons.append(f"Prize {_format_inr(prefs.min_prize or 0)}+")
    for topic in _matching_names(prefs.topics, _tags(event)):
        score += WEIGHTS["topic"]
        reasons.append(f"Topic: {topic}")
    for company in _matching_companies(event, prefs.companies):
        score += WEIGHTS["company"]
        reasons.append(f"Company: {company}")
    return Relevance(_clamp(score), reasons, hidden)


def _feed_item(
    event: Event,
    scored: Relevance,
    state: UserEventState | None,
    versions: list[EventVersion],
    now: datetime,
) -> FeedItem:
    important = _latest_important(versions)
    unseen = important is not None and (state is None or state.state != UserState.SEEN)
    return FeedItem(
        event_id=event.id,
        title=event.title,
        url=event.url,
        relevance=scored.score,
        urgency=urgency(event, now),
        confidence_label=confidence_label(event),
        reasons=list(scored.reasons),
        diff=_format_diff(important.changed_fields) if unseen and important else None,
        key_at=key_datetime(event),
        important_unseen=unseen,
        is_new=state is None or state.state == UserState.NEW,
    )


def _latest_important(versions: list[EventVersion]) -> EventVersion | None:
    important = [
        version for version in versions if version.importance == Importance.IMPORTANT
    ]
    if not important:
        return None
    return max(important, key=lambda version: (version.seen_at, version.id or 0))


def _format_diff(changed_fields: dict[str, Any]) -> str:
    ordered = [name for name in IMPORTANT_FIELDS if name in changed_fields]
    ordered.extend(sorted(name for name in changed_fields if name not in ordered))
    parts: list[str] = []
    for name in ordered:
        pair = changed_fields[name]
        if not isinstance(pair, (list, tuple)) or len(pair) != 2:
            continue
        old = _format_value(pair[0])
        new = _format_value(pair[1])
        parts.append(f"{name}: {old} -> {new}")
    return "; ".join(parts)


def _format_value(value: Any) -> str:
    parsed = _coerce_datetime(value)
    if parsed is not None:
        local = parsed.astimezone(_DISPLAY_ZONE)
        return f"{_MONTHS[local.month - 1]} {local.day}"
    if value is None:
        return "none"
    return str(value)


def _load_prefs(session: Session, user_id: str) -> Prefs:
    row = session.get(UserPrefs, user_id)
    if row is None or not isinstance(row.prefs, dict):
        return Prefs()
    return prefs_from_mapping(row.prefs)


def _mode_hides(mode: EventMode | None, modes: tuple[str, ...]) -> bool:
    if not modes or mode is None or mode == EventMode.UNKNOWN:
        return False
    return mode.value.casefold() not in {item.casefold() for item in modes}


def _location_passes(event: Event, locations: tuple[str, ...]) -> bool:
    if not locations or event.mode == EventMode.ONLINE:
        return True
    if event.mode not in (EventMode.OFFLINE, EventMode.HYBRID):
        return True
    return _matched_location(event, locations) is not None


def _matched_location(event: Event, locations: tuple[str, ...]) -> str | None:
    if event.mode == EventMode.ONLINE or not event.location:
        return None
    for name in locations:
        if _named_in(event.location, name):
            return name
    return None


def _prize_matches(event: Event, prefs: Prefs) -> bool:
    if prefs.min_prize is None or event.prize_amount_inr is None:
        return False
    return event.prize_amount_inr >= prefs.min_prize


def _excludes_students(eligibility: str | None) -> bool:
    if eligibility is None or not eligibility.strip():
        return False
    text = eligibility.casefold()
    return any(phrase in text for phrase in _STUDENT_EXCLUSIONS)


def _matching_companies(event: Event, companies: tuple[str, ...]) -> list[str]:
    haystacks = _sponsor_names(event.sponsors)
    if isinstance(event.organizer, str) and event.organizer.strip():
        haystacks.append(event.organizer)
    return [
        name for name in companies if any(_named_in(text, name) for text in haystacks)
    ]


def _matching_names(wanted: tuple[str, ...], tags: list[str]) -> list[str]:
    folded = {tag.casefold() for tag in tags}
    return [name for name in wanted if name.casefold() in folded]


def _sponsor_names(sponsors: Any) -> list[str]:
    if isinstance(sponsors, str):
        return [sponsors]
    if isinstance(sponsors, dict):
        return _sponsor_names([sponsors])
    if not isinstance(sponsors, list):
        return []
    names: list[str] = []
    for item in sponsors:
        if isinstance(item, str):
            names.append(item)
        elif isinstance(item, dict) and isinstance(item.get("name"), str):
            names.append(item["name"])
    return names


def _tags(event: Event) -> list[str]:
    if not isinstance(event.tags, list):
        return []
    return [tag for tag in event.tags if isinstance(tag, str)]


def _named_in(text: str, name: str) -> bool:
    if not name.strip():
        return False
    return re.search(rf"\b{re.escape(name)}\b", text, re.IGNORECASE) is not None


def _field_confidence(raw: dict[str, Any], field: str) -> float:
    if field not in raw:
        return 1.0
    value = raw[field]
    if isinstance(value, bool) or not isinstance(value, int | float):
        return 0.0
    return float(value)


def _is_today(moment: datetime | None, now: datetime) -> bool:
    parsed = _as_utc(moment)
    if parsed is None:
        return False
    return (
        parsed.astimezone(_DISPLAY_ZONE).date() == now.astimezone(_DISPLAY_ZONE).date()
    )


def _format_inr(amount: int) -> str:
    sign = "-" if amount < 0 else ""
    digits = str(abs(amount))
    if len(digits) <= 3:
        return sign + digits
    head, tail = digits[:-3], digits[-3:]
    parts: list[str] = []
    while head:
        parts.append(head[-2:])
        head = head[:-2]
    return sign + ",".join(reversed(parts)) + "," + tail


def _strings(value: Any) -> tuple[str, ...]:
    if not isinstance(value, list):
        return ()
    return tuple(item for item in value if isinstance(item, str))


def _prize(value: Any) -> int | None:
    if isinstance(value, bool) or not isinstance(value, int):
        return None
    return value


def _clamp(score: int) -> int:
    return max(0, min(100, score))


def _as_utc(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _coerce_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return _as_utc(value)
    if isinstance(value, str):
        try:
            return _as_utc(datetime.fromisoformat(value))
        except ValueError:
            return None
    return None
