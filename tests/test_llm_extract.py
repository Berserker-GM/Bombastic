import json
import logging
import os
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pytest

from radar.llm.client import InvalidJSONError, llm_from_env
from radar.llm.extract import (
    Evidence,
    ExtractedNotice,
    ExtractionError,
    NoticeCategory,
    extract_notice,
    to_raw_event,
    verify_evidence,
)
from radar.models.enums import EventCategory, Lifecycle

TODAY = date(2026, 10, 11)
URL = "https://college.example/notices/workshop"
_KOLKATA = timezone(timedelta(hours=5, minutes=30))
_FIXTURES = Path(__file__).parent / "fixtures" / "college"
_LLM = _FIXTURES / "llm"


class ScriptedLLM:
    def __init__(self, steps: list[Any]) -> None:
        self._steps = list(steps)
        self.calls = 0
        self.prompts: list[str] = []
        self.schemas: list[dict[str, Any]] = []

    def complete_json(self, prompt: str, schema: dict[str, Any]) -> dict[str, Any]:
        self.calls += 1
        self.prompts.append(prompt)
        self.schemas.append(schema)
        step = self._steps.pop(0)
        if isinstance(step, Exception):
            raise step
        if not isinstance(step, dict):
            raise TypeError("scripted step must be a dict or an exception")
        return step


def _payload(name: str) -> dict[str, Any]:
    return json.loads((_LLM / name).read_text(encoding="utf-8"))


def _text(name: str) -> str:
    return (_LLM / name).read_text(encoding="utf-8")


def test_workshop_date_is_not_a_registration_deadline() -> None:
    llm = ScriptedLLM([_payload("workshop.json")])
    notice = extract_notice(_text("workshop.txt"), llm, today=TODAY)
    raw = to_raw_event(notice, source_name="college", url=URL, notice_hash="abc")

    assert notice.event_date is not None
    assert notice.event_date.date() == date(2026, 10, 15)
    assert notice.registration_deadline is None
    assert llm.calls == 1
    assert "2026-10-11" in llm.prompts[0]
    assert "NEVER guess" in llm.prompts[0]
    assert "verbatim quote" in llm.prompts[0]
    assert "registration deadline" in llm.prompts[0]
    assert "submission deadline" in llm.prompts[0]
    assert "is_actionable" in llm.schemas[0]["properties"]

    assert raw is not None
    assert raw.category is EventCategory.COLLEGE
    assert raw.external_id == URL
    assert raw.url == URL
    assert raw.start_at == datetime(2026, 10, 15, tzinfo=_KOLKATA)
    assert raw.registration_deadline is None
    assert raw.lifecycle is Lifecycle.UNKNOWN
    assert raw.confidence == {"start_at": 0.95, "location": 0.8}
    assert raw.extra is not None
    assert raw.extra["notice_hash"] == "abc"
    by_field = {item.field: item for item in raw.evidence}
    assert by_field["start_at"].source_text == "workshop on October 15"
    assert by_field["start_at"].source_url == URL
    assert by_field["start_at"].confidence == 0.95
    assert by_field["location"].source_text == "main auditorium"
    assert by_field["location"].source_url == URL


def test_quote_not_in_the_notice_drops_that_field(
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.WARNING)
    llm = ScriptedLLM([_payload("bad_quote.json")])
    notice = extract_notice(_text("workshop.txt"), llm, today=TODAY)
    raw = to_raw_event(notice, source_name="college", url=URL)

    assert notice.event_date is not None
    assert notice.registration_deadline is None
    assert "registration_deadline" not in notice.evidence
    assert "registration_deadline" in caplog.text
    assert raw is not None
    assert raw.registration_deadline is None
    assert raw.start_at == datetime(2026, 10, 15, tzinfo=_KOLKATA)
    assert "registration_deadline" not in {item.field for item in raw.evidence}


def test_extension_notice_marks_deadline_changed() -> None:
    text = (_FIXTURES / "notice05.txt").read_text(encoding="utf-8")
    notice = extract_notice(
        text, ScriptedLLM([_payload("extension.json")]), today=TODAY
    )
    raw = to_raw_event(
        notice,
        source_name="college",
        url="https://college.example/notices/extension",
    )

    assert notice.deadline_changed is True
    assert notice.mentions_extension is True
    assert notice.registration_deadline is not None
    assert raw is not None
    assert raw.extra is not None
    assert raw.extra["deadline_changed"] is True
    assert raw.extra["mentions_extension"] is True
    assert raw.lifecycle is Lifecycle.UNKNOWN
    assert raw.registration_deadline == datetime(2026, 10, 10, 23, 59, tzinfo=_KOLKATA)
    assert raw.evidence[0].source_text == "extended to 10th October, 11:59 PM"
    assert raw.evidence[0].source_url == "https://college.example/notices/extension"


def test_mentioned_date_stays_out_of_the_deadline() -> None:
    notice = extract_notice(
        _text("mentioned.txt"),
        ScriptedLLM([_payload("mentioned.json")]),
        today=TODAY,
    )
    raw = to_raw_event(notice, source_name="college", url=URL)

    assert notice.registration_deadline is None
    assert raw is not None
    assert raw.registration_deadline is None
    assert raw.start_at == datetime(2026, 10, 21, tzinfo=_KOLKATA)
    assert raw.extra is not None
    other_dates = raw.extra["other_dates"]
    assert other_dates == [
        {
            "type": "registration_opens",
            "label": "Registration opens",
            "at": datetime(2026, 10, 14, tzinfo=_KOLKATA).isoformat(),
            "confidence": 0.7,
        }
    ]
    by_field = {item.field: item for item in raw.evidence}
    assert by_field["other_dates.0"].source_url == URL
    assert by_field["other_dates.0"].source_text == "Registration opens on 14 October"


def test_quote_matches_when_case_and_whitespace_differ() -> None:
    notice = ExtractedNotice(
        is_actionable=True,
        category=NoticeCategory.COLLEGE,
        title="Workshop",
        event_date=datetime(2026, 10, 15),
        confidence=1,
        evidence={
            "event_date": Evidence(quote="  WORKSHOP   on   october 15  ", confidence=1)
        },
    )

    verified, warnings = verify_evidence("Room 1\nWorkshop   on October 15\n", notice)

    assert verified.event_date is not None
    assert warnings == []


def test_missing_evidence_drops_the_field() -> None:
    notice = ExtractedNotice(
        is_actionable=True,
        category=NoticeCategory.COLLEGE,
        title="Workshop",
        registration_deadline=datetime(2026, 10, 1),
        confidence=1,
    )

    verified, warnings = verify_evidence("Workshop next week.", notice)

    assert verified.registration_deadline is None
    assert any(warning.startswith("registration_deadline:") for warning in warnings)


def test_invalid_json_is_retried_once_then_raised() -> None:
    llm = ScriptedLLM(
        [InvalidJSONError("not json"), InvalidJSONError("still not json")]
    )

    with pytest.raises(ExtractionError):
        extract_notice(_text("workshop.txt"), llm, today=TODAY)

    assert llm.calls == 2


def test_invalid_json_succeeds_on_the_retry() -> None:
    llm = ScriptedLLM([InvalidJSONError("not json"), _payload("workshop.json")])

    notice = extract_notice(_text("workshop.txt"), llm, today=TODAY)

    assert llm.calls == 2
    assert notice.event_date is not None
    assert notice.registration_deadline is None


def test_schema_failure_is_retried_once() -> None:
    llm = ScriptedLLM([{"is_actionable": "nope"}, _payload("workshop.json")])

    notice = extract_notice(_text("workshop.txt"), llm, today=TODAY)

    assert llm.calls == 2
    assert notice.event_date is not None


def test_exam_category_maps_to_other() -> None:
    notice = ExtractedNotice(
        is_actionable=True,
        category=NoticeCategory.EXAM,
        title="Midterm",
        confidence=1,
    )

    raw = to_raw_event(notice, source_name="college", url=URL)

    assert raw is not None
    assert raw.category is EventCategory.OTHER
    assert raw.lifecycle is Lifecycle.UNKNOWN
    assert raw.extra is not None
    assert raw.extra["notice_category"] == "exam"


def test_non_actionable_notice_is_not_an_event() -> None:
    notice = ExtractedNotice(
        is_actionable=False,
        category=NoticeCategory.OTHER,
        title="Holiday Circular",
        confidence=0.8,
    )

    assert to_raw_event(notice, source_name="college", url=URL) is None


@pytest.mark.live
@pytest.mark.skipif(
    os.environ.get("RADAR_LIVE_TESTS") != "1",
    reason="set RADAR_LIVE_TESTS=1 to call the configured LLM",
)
def test_live_llm_keeps_the_workshop_date() -> None:
    text = (
        "Department workshop\n\n"
        "The department will hold a workshop on October 15 "
        "in the main auditorium.\n"
        "This notice does not give a registration deadline.\n"
    )

    notice = extract_notice(text, llm_from_env(), today=TODAY)

    assert notice.event_date is not None
    assert notice.event_date.year == 2026
    assert notice.event_date.month == 10
    assert notice.event_date.day == 15
    assert notice.registration_deadline is None
