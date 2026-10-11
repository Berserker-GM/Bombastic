"""Extract a college notice into a RawEvent. Unquoted fields are dropped."""

import json
import logging
import re
from datetime import date, datetime, timedelta, timezone
from enum import StrEnum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, ValidationError

from radar.collectors.base import EvidenceItem, RawEvent
from radar.llm.client import LLM, InvalidJSONError
from radar.models.enums import EventCategory, Lifecycle

logger = logging.getLogger(__name__)

# Zoneless notice times are local to the default display zone, then stored as UTC.
_KOLKATA = timezone(timedelta(hours=5, minutes=30))
_WHITESPACE = re.compile(r"\s+")
_REQUIRED_FIELDS: tuple[str, ...] = (
    "event_date",
    "registration_deadline",
    "submission_deadline",
    "location",
    "eligibility",
)
_OPTIONAL_QUOTE_FIELDS: tuple[str, ...] = (
    "title",
    "summary",
    "deadline_changed",
    "mentions_extension",
)
_EVENT_FIELD: dict[str, str] = {
    "event_date": "start_at",
    "registration_deadline": "registration_deadline",
    "submission_deadline": "submission_deadline",
    "location": "location",
    "eligibility": "eligibility",
}


class ExtractionError(Exception):
    """The model returned invalid JSON on both attempts."""


class NoticeCategory(StrEnum):
    COLLEGE = "college"
    SCHOLARSHIP = "scholarship"
    CLUB = "club"
    PLACEMENT = "placement"
    EXAM = "exam"
    OTHER = "other"


class Evidence(BaseModel):
    model_config = ConfigDict(extra="ignore")

    quote: str
    confidence: float = Field(ge=0, le=1)


class OtherDate(BaseModel):
    model_config = ConfigDict(extra="ignore")

    type: str
    label: str
    at: datetime | None = None


class ExtractedNotice(BaseModel):
    model_config = ConfigDict(extra="ignore")

    is_actionable: bool
    category: NoticeCategory
    title: str | None = None
    summary: str | None = None
    event_date: datetime | None = None
    registration_deadline: datetime | None = None
    submission_deadline: datetime | None = None
    other_dates: list[OtherDate] = Field(default_factory=list)
    deadline_changed: bool | None = None
    mentions_extension: bool | None = None
    location: str | None = None
    eligibility: str | None = None
    confidence: float = Field(ge=0, le=1)
    evidence: dict[str, Evidence] = Field(default_factory=dict)


def extraction_prompt(text: str, today: date) -> str:
    rules = (
        f"Today's date is {today.isoformat()}.\n"
        "\n"
        "Extract the notice into the schema.\n"
        "Return null for anything not explicitly stated. NEVER guess.\n"
        "A date that is merely mentioned is not a deadline.\n"
        "Distinguish event date, registration deadline, submission deadline, "
        "and a date that is only mentioned.\n"
        "Back every non-null field with a verbatim quote from the notice.\n"
        "Every non-null date, location, and eligibility needs an evidence "
        "entry. The quote must be copied verbatim from the notice.\n"
        "Mentioned dates belong in other_dates. Evidence for other_dates "
        "item i uses the key other_dates.i.\n"
        "If the notice gives no time, return the date with no time of day.\n"
        "If the notice gives no year, use the year from today's date.\n"
        "deadline_changed is true only when the notice states that a deadline "
        "changed or was extended; otherwise null.\n"
        "mentions_extension is true only when the notice says a deadline was "
        "extended; otherwise null.\n"
    )
    return f"{rules}\nNotice:\n{text}"


def extract_notice(
    text: str,
    llm: LLM,
    *,
    today: date | None = None,
) -> ExtractedNotice:
    current_day = today if today is not None else date.today()
    prompt = extraction_prompt(text, current_day)
    schema = ExtractedNotice.model_json_schema()
    last_error: Exception | None = None
    for _attempt in range(2):
        try:
            payload = llm.complete_json(prompt, schema)
        except (InvalidJSONError, json.JSONDecodeError) as exc:
            last_error = exc
            continue
        if not isinstance(payload, dict):
            last_error = InvalidJSONError("LLM JSON was not an object")
            continue
        try:
            notice = ExtractedNotice.model_validate(payload)
        except ValidationError as exc:
            last_error = exc
            continue
        verified, warnings = verify_evidence(text, notice)
        for warning in warnings:
            logger.warning("%s", warning)
        return verified
    detail = "" if last_error is None else f": {last_error}"
    raise ExtractionError(f"invalid LLM JSON after retry{detail}") from last_error


def verify_evidence(
    text: str, notice: ExtractedNotice
) -> tuple[ExtractedNotice, list[str]]:
    warnings: list[str] = []
    evidence = dict(notice.evidence)
    updates: dict[str, Any] = {}

    for field in _REQUIRED_FIELDS:
        if getattr(notice, field) is None:
            evidence.pop(field, None)
            continue
        problem = _quote_problem(text, evidence.get(field))
        if problem is None:
            continue
        updates[field] = None
        evidence.pop(field, None)
        warnings.append(f"{field}: {problem}")

    for field in _OPTIONAL_QUOTE_FIELDS:
        if field not in evidence:
            continue
        problem = _quote_problem(text, evidence.get(field))
        if problem is None:
            continue
        updates[field] = None
        evidence.pop(field, None)
        warnings.append(f"{field}: {problem}")

    kept_dates: list[OtherDate] = []
    rewritten: dict[str, Evidence] = {
        key: value
        for key, value in evidence.items()
        if not key.startswith("other_dates.")
    }
    for index, item in enumerate(notice.other_dates):
        key = f"other_dates.{index}"
        found = evidence.get(key)
        if item.at is None:
            warnings.append(f"{key}: evidence quote is missing")
            continue
        problem = _quote_problem(text, found)
        if problem is not None:
            warnings.append(f"{key}: {problem}")
            continue
        assert found is not None
        rewritten[f"other_dates.{len(kept_dates)}"] = found
        kept_dates.append(item)

    updates["other_dates"] = kept_dates
    updates["evidence"] = rewritten
    return notice.model_copy(update=updates), warnings


def quote_in_source(quote: str, source: str) -> bool:
    normalized = normalize_text(quote)
    if not normalized:
        return False
    return normalized in normalize_text(source)


def normalize_text(text: str) -> str:
    return _WHITESPACE.sub(" ", text).casefold().strip()


def to_raw_event(
    notice: ExtractedNotice,
    *,
    source_name: str,
    url: str,
    notice_hash: str | None = None,
    list_title: str | None = None,
) -> RawEvent | None:
    if not notice.is_actionable:
        return None
    title = (notice.title or list_title or "").strip()
    if not title:
        return None

    confidence: dict[str, float] = {}
    evidence: list[EvidenceItem] = []
    for extracted_name, event_field in _EVENT_FIELD.items():
        if getattr(notice, extracted_name) is None:
            continue
        item = notice.evidence.get(extracted_name)
        if item is None:
            continue
        confidence[event_field] = item.confidence
        evidence.append(
            EvidenceItem(
                field=event_field,
                source_url=url,
                source_text=item.quote,
                confidence=item.confidence,
            )
        )

    other_dates: list[dict[str, Any]] = []
    for index, item in enumerate(notice.other_dates):
        if item.at is None:
            continue
        found = notice.evidence.get(f"other_dates.{index}")
        if found is None:
            continue
        other_dates.append(
            {
                "type": item.type,
                "label": item.label,
                "at": _localize(item.at).isoformat(),
                "confidence": found.confidence,
            }
        )
        evidence.append(
            EvidenceItem(
                field=f"other_dates.{index}",
                source_url=url,
                source_text=found.quote,
                confidence=found.confidence,
            )
        )

    consumed = set(_EVENT_FIELD) | {
        f"other_dates.{index}" for index in range(len(other_dates))
    }
    for key, item in notice.evidence.items():
        if key in consumed:
            continue
        evidence.append(
            EvidenceItem(
                field=key,
                source_url=url,
                source_text=item.quote,
                confidence=item.confidence,
            )
        )

    extra: dict[str, Any] = {}
    if other_dates:
        extra["other_dates"] = other_dates
    if notice.submission_deadline is not None:
        submitted = _localize(notice.submission_deadline)
        extra["submission_deadline"] = submitted.isoformat()
    if notice.deadline_changed is not None:
        extra["deadline_changed"] = notice.deadline_changed
    if notice.mentions_extension is not None:
        extra["mentions_extension"] = notice.mentions_extension
    if notice.summary:
        extra["summary"] = notice.summary
    if notice_hash is not None:
        extra["notice_hash"] = notice_hash
    if notice.category is NoticeCategory.EXAM:
        extra["notice_category"] = NoticeCategory.EXAM.value

    return RawEvent(
        source_name=source_name,
        title=title,
        category=_event_category(notice.category),
        external_id=url,
        url=url,
        start_at=_localize_optional(notice.event_date),
        registration_deadline=_localize_optional(notice.registration_deadline),
        location=notice.location,
        eligibility=notice.eligibility,
        lifecycle=Lifecycle.UNKNOWN,
        confidence=confidence,
        evidence=evidence,
        extra=extra or None,
    )


def _event_category(category: NoticeCategory) -> EventCategory:
    if category is NoticeCategory.EXAM:
        return EventCategory.OTHER
    return EventCategory(category.value)


def _quote_problem(text: str, evidence: Evidence | None) -> str | None:
    if evidence is None or not evidence.quote.strip():
        return "evidence quote is missing"
    if not quote_in_source(evidence.quote, text):
        return "evidence quote was not found in the notice"
    return None


def _localize_optional(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    return _localize(value)


def _localize(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=_KOLKATA)
    return value.astimezone(_KOLKATA)
