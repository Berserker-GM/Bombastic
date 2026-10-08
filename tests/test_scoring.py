from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from sqlalchemy import create_engine
from sqlalchemy import event as sa_event
from sqlalchemy.orm import Session
from sqlalchemy.pool import StaticPool

from radar.models import (
    Base,
    Event,
    EventCategory,
    EventMode,
    EventVersion,
    HealthStatus,
    Importance,
    Lifecycle,
    Source,
    SourceKind,
    Urgency,
    UserEventState,
    UserPrefs,
    UserState,
)
from radar.pipeline.scoring import (
    WEIGHTS,
    Feed,
    FeedItem,
    Prefs,
    build_feed,
    confidence_label,
    feed_sort_key,
    feed_tier,
    key_datetime,
    relevance,
    urgency,
)

NOW = datetime(2026, 10, 8, 6, tzinfo=UTC)
USER = "user-1"


def _bare(**overrides: Any) -> Event:
    values: dict[str, Any] = {
        "source_id": 1,
        "fingerprint": "fp",
        "title": "Event",
        "category": EventCategory.HACKATHON,
        "lifecycle": Lifecycle.UNKNOWN,
        "first_seen_at": NOW,
        "last_seen_at": NOW,
        "content_hash": "hash",
        "confidence": {},
        "tags": [],
    }
    values.update(overrides)
    return Event(**values)


def _prefs(**overrides: Any) -> Prefs:
    values: dict[str, Any] = {
        "modes": ("online",),
        "locations": ("Pune",),
        "min_prize": 100_000,
        "student_only": True,
        "companies": ("Google",),
        "topics": ("AI",),
        "contest_platforms": ("codeforces",),
    }
    values.update(overrides)
    return Prefs(**values)


@pytest.mark.parametrize(
    ("category", "start_at", "deadline", "expected"),
    [
        (
            EventCategory.CONTEST,
            NOW + timedelta(days=2),
            NOW + timedelta(hours=1),
            NOW + timedelta(days=2),
        ),
        (
            EventCategory.HACKATHON,
            NOW + timedelta(hours=1),
            NOW + timedelta(days=4),
            NOW + timedelta(days=4),
        ),
        (
            EventCategory.COLLEGE,
            NOW + timedelta(hours=1),
            NOW + timedelta(days=4),
            NOW + timedelta(days=4),
        ),
        (EventCategory.SCHOLARSHIP, NOW, NOW, None),
    ],
)
def test_key_datetime_follows_category(
    category: EventCategory,
    start_at: datetime,
    deadline: datetime,
    expected: datetime | None,
) -> None:
    event = _bare(
        category=category,
        start_at=start_at,
        registration_deadline=deadline,
    )

    assert key_datetime(event) == expected


@pytest.mark.parametrize(
    ("delta", "expected"),
    [
        (timedelta(seconds=-1), Urgency.NONE),
        (timedelta(0), Urgency.CRITICAL),
        (timedelta(hours=24) - timedelta(seconds=1), Urgency.CRITICAL),
        (timedelta(hours=24), Urgency.SOON),
        (timedelta(days=3) - timedelta(seconds=1), Urgency.SOON),
        (timedelta(days=3), Urgency.UPCOMING),
        (timedelta(days=7) - timedelta(seconds=1), Urgency.UPCOMING),
        (timedelta(days=7), Urgency.LATER),
        (None, Urgency.NONE),
    ],
)
def test_urgency_buckets(delta: timedelta | None, expected: Urgency) -> None:
    deadline = None if delta is None else NOW + delta
    event = _bare(category=EventCategory.COLLEGE, registration_deadline=deadline)

    assert urgency(event, NOW) is expected


@pytest.mark.parametrize(
    ("confidence", "expected"),
    [
        ({}, "high"),
        ({"start_at": 0.9}, "high"),
        ({"start_at": 0.89}, "medium"),
        ({"end_at": 0.6}, "medium"),
        ({"registration_deadline": 0.59}, "low"),
        ({"start_at": 0.95, "end_at": 1, "registration_deadline": 0.2}, "low"),
    ],
)
def test_confidence_label_uses_the_weakest_date(
    confidence: dict[str, float],
    expected: str,
) -> None:
    assert confidence_label(_bare(confidence=confidence)) == expected


@pytest.mark.parametrize(
    ("event_kwargs", "pref_kwargs", "score", "hidden", "reasons"),
    [
        (
            {"category": EventCategory.COLLEGE},
            {"student_only": True},
            100,
            False,
            ["College"],
        ),
        (
            {
                "category": EventCategory.CONTEST,
                "tags": ["codechef"],
            },
            {},
            0,
            True,
            [],
        ),
        (
            {
                "category": EventCategory.CONTEST,
                "tags": ["Codeforces"],
            },
            {"contest_platforms": ("codeforces",)},
            WEIGHTS["contest_platform"],
            False,
            ["Platform: codeforces"],
        ),
        (
            {
                "category": EventCategory.HACKATHON,
                "mode": EventMode.ONLINE,
                "location": "Delhi",
                "prize_amount_inr": 150_000,
                "tags": ["ai"],
                "organizer": "Hosted by Google",
                "eligibility": "Open to students",
            },
            {},
            WEIGHTS["prize"] + WEIGHTS["topic"] + WEIGHTS["company"],
            False,
            ["Online", "Prize 1,00,000+", "Topic: AI", "Company: Google"],
        ),
        (
            {
                "category": EventCategory.HACKATHON,
                "mode": EventMode.OFFLINE,
                "location": "Delhi",
                "eligibility": "Open to students",
            },
            {"modes": ("offline",)},
            0,
            True,
            ["Offline"],
        ),
        (
            {
                "category": EventCategory.HACKATHON,
                "mode": EventMode.OFFLINE,
                "location": "Pune, India",
                "eligibility": "Open to students",
            },
            {"modes": ("offline",)},
            0,
            False,
            ["Offline", "Location: Pune"],
        ),
        (
            {
                "category": EventCategory.HACKATHON,
                "mode": EventMode.ONLINE,
                "eligibility": "Working professionals only",
            },
            {},
            0,
            True,
            ["Online"],
        ),
        (
            {
                "category": EventCategory.HACKATHON,
                "mode": EventMode.OFFLINE,
                "eligibility": "Working professionals only",
            },
            {"student_only": False, "modes": (), "locations": ()},
            0,
            False,
            ["Offline"],
        ),
        (
            {
                "category": EventCategory.SCHOLARSHIP,
                "mode": EventMode.OFFLINE,
                "location": "Delhi",
            },
            {},
            0,
            False,
            [],
        ),
    ],
)
def test_relevance_table(
    event_kwargs: dict[str, Any],
    pref_kwargs: dict[str, Any],
    score: int,
    hidden: bool,
    reasons: list[str],
) -> None:
    result = relevance(_bare(**event_kwargs), _prefs(**pref_kwargs))

    assert result.score == score
    assert result.hidden is hidden
    assert result.reasons == reasons


def test_relevance_caps_at_100() -> None:
    event = _bare(
        category=EventCategory.HACKATHON,
        mode=EventMode.ONLINE,
        tags=["ai", "ml", "cv", "nlp", "systems"],
        eligibility="Open to students",
        prize_amount_inr=500_000,
        sponsors=["Google", "Microsoft", "Amazon"],
    )
    prefs = _prefs(
        topics=("AI", "ML", "CV", "NLP", "Systems"),
        companies=("Google", "Microsoft", "Amazon"),
        student_only=False,
        modes=(),
        locations=(),
    )

    assert relevance(event, prefs).score == 100


def test_low_confidence_does_not_hide() -> None:
    event = _bare(
        category=EventCategory.HACKATHON,
        mode=EventMode.ONLINE,
        eligibility="Open to students",
        confidence={"registration_deadline": 0.1, "start_at": 0.1, "end_at": 0.1},
    )

    result = relevance(event, _prefs(student_only=False, modes=(), locations=()))

    assert result.hidden is False
    assert confidence_label(event) == "low"


def _item(**overrides: Any) -> FeedItem:
    values: dict[str, Any] = {
        "event_id": 1,
        "title": "Item",
        "url": None,
        "relevance": 40,
        "urgency": Urgency.LATER,
        "confidence_label": "high",
        "reasons": [],
        "diff": None,
        "key_at": NOW + timedelta(days=10),
        "important_unseen": False,
        "is_new": False,
    }
    values.update(overrides)
    return FeedItem(**values)


@pytest.mark.parametrize(
    ("kwargs", "tier"),
    [
        ({"important_unseen": True, "urgency": Urgency.CRITICAL}, 1),
        ({"urgency": Urgency.CRITICAL, "is_new": True, "relevance": 100}, 2),
        ({"is_new": True, "relevance": 80, "urgency": Urgency.LATER}, 3),
        ({"is_new": True, "relevance": 79, "urgency": Urgency.SOON}, 4),
        ({"urgency": Urgency.LATER, "relevance": 100}, 5),
    ],
)
def test_feed_tier_priority(kwargs: dict[str, Any], tier: int) -> None:
    assert feed_tier(_item(**kwargs)) == tier


def test_same_relevance_orders_by_urgency() -> None:
    soon = _item(
        title="Soon",
        relevance=40,
        urgency=Urgency.SOON,
        key_at=NOW + timedelta(days=2),
    )
    critical = _item(
        title="Critical",
        relevance=40,
        urgency=Urgency.CRITICAL,
        key_at=NOW + timedelta(hours=2),
    )
    earlier_soon = _item(
        title="Earlier soon",
        relevance=40,
        urgency=Urgency.SOON,
        key_at=NOW + timedelta(days=1),
    )

    ordered = sorted([soon, critical, earlier_soon], key=feed_sort_key)

    assert [item.title for item in ordered] == ["Critical", "Earlier soon", "Soon"]


def test_important_update_outranks_a_higher_relevance_critical_deadline() -> None:
    update = _item(
        title="Updated",
        relevance=10,
        urgency=Urgency.LATER,
        important_unseen=True,
        key_at=NOW + timedelta(days=10),
    )
    critical = _item(
        title="Due",
        relevance=100,
        urgency=Urgency.CRITICAL,
        key_at=NOW + timedelta(hours=1),
    )

    ordered = sorted([critical, update], key=feed_sort_key)

    assert feed_tier(update) < feed_tier(critical)
    assert [item.title for item in ordered] == ["Updated", "Due"]


@pytest.fixture
def session() -> Iterator[Session]:
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )

    @sa_event.listens_for(engine, "connect")
    def _enable_foreign_keys(dbapi_connection: Any, _: Any) -> None:
        cursor = dbapi_connection.cursor()
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.close()

    Base.metadata.create_all(engine)
    with Session(engine) as db_session:
        yield db_session
        db_session.rollback()


def _source(session: Session) -> Source:
    source = Source(
        kind=SourceKind.API,
        name="sample",
        config={},
        enabled=True,
        health_status=HealthStatus.HEALTHY,
        consecutive_failures=0,
    )
    session.add(source)
    session.flush()
    return source


def _persist(session: Session, source: Source, **overrides: Any) -> Event:
    event = _bare(source_id=source.id, **overrides)
    session.add(event)
    session.flush()
    return event


def _state(session: Session, event: Event, state: UserState) -> None:
    session.add(
        UserEventState(
            user_id=USER,
            event_id=event.id,
            state=state,
            relevance=0,
            urgency=Urgency.NONE,
            reasons=[],
        )
    )


def _version(
    session: Session,
    event: Event,
    importance: Importance,
    changed_fields: dict[str, Any],
) -> None:
    session.add(
        EventVersion(
            event_id=event.id,
            seen_at=NOW,
            changed_fields=changed_fields,
            importance=importance,
            snapshot={},
        )
    )


def _save_prefs(session: Session, **overrides: Any) -> None:
    prefs = _prefs(**overrides)
    session.add(
        UserPrefs(
            user_id=USER,
            prefs={
                "modes": list(prefs.modes),
                "locations": list(prefs.locations),
                "min_prize": prefs.min_prize,
                "student_only": prefs.student_only,
                "companies": list(prefs.companies),
                "topics": list(prefs.topics),
                "contest_platforms": list(prefs.contest_platforms),
            },
        )
    )


def _titles(feed: Feed) -> set[str]:
    found: set[str] = set()
    for section in (
        feed.important_updates,
        feed.deadlines_soon,
        feed.new_opportunities,
        feed.todays_contests,
        feed.saved,
        feed.college,
        feed.hackathons,
    ):
        found.update(item.title for item in section)
    return found


def test_feed_orders_equal_relevance_by_urgency(session: Session) -> None:
    source = _source(session)
    _save_prefs(session, contest_platforms=("codeforces",), modes=(), locations=())
    critical = _persist(
        session,
        source,
        title="Critical Cup",
        category=EventCategory.CONTEST,
        tags=["codeforces"],
        start_at=NOW + timedelta(hours=2),
        url="https://codeforces.com/contest/1",
    )
    soon = _persist(
        session,
        source,
        title="Soon Cup",
        category=EventCategory.CONTEST,
        tags=["codeforces"],
        start_at=NOW + timedelta(days=2),
        url="https://codeforces.com/contest/2",
        fingerprint="soon",
    )
    _state(session, critical, UserState.SEEN)
    _state(session, soon, UserState.SEEN)
    session.flush()

    feed = build_feed(session, USER, NOW)

    assert [item.title for item in feed.deadlines_soon] == ["Critical Cup", "Soon Cup"]
    assert feed.deadlines_soon[0].relevance == feed.deadlines_soon[1].relevance
    assert feed.deadlines_soon[0].urgency is Urgency.CRITICAL
    assert feed.deadlines_soon[1].urgency is Urgency.SOON


def test_feed_important_update_outranks_critical_deadline(session: Session) -> None:
    source = _source(session)
    _save_prefs(
        session,
        student_only=False,
        modes=(),
        locations=(),
        topics=("AI",),
        min_prize=100_000,
    )
    updated = _persist(
        session,
        source,
        title="Updated",
        registration_deadline=NOW + timedelta(days=10),
        url="https://example.com/updated",
    )
    due = _persist(
        session,
        source,
        title="Due",
        registration_deadline=NOW + timedelta(hours=1),
        prize_amount_inr=200_000,
        tags=["ai"],
        url="https://example.com/due",
        fingerprint="due",
    )
    _version(
        session,
        updated,
        Importance.IMPORTANT,
        {
            "registration_deadline": [
                "2026-10-15T00:00:00+00:00",
                "2026-10-12T00:00:00+00:00",
            ]
        },
    )
    _state(session, due, UserState.SEEN)
    session.flush()

    feed = build_feed(session, USER, NOW)
    update = feed.important_updates[0]

    assert [item.title for item in feed.hackathons] == ["Updated", "Due"]
    assert update.relevance < feed.hackathons[1].relevance
    assert update.diff == "registration_deadline: Oct 15 -> Oct 12"
    assert update.url == "https://example.com/updated"
    assert feed.counts["important_updates"] == 1
    assert feed.counts["deadlines_soon"] == 1


def test_saved_event_survives_a_hiding_filter(session: Session) -> None:
    source = _source(session)
    _save_prefs(session, contest_platforms=("codeforces",))
    hidden = _persist(
        session,
        source,
        title="Hidden Cup",
        category=EventCategory.CONTEST,
        tags=["codechef"],
        start_at=NOW,
        url="https://www.codechef.com/START",
    )
    _persist(
        session,
        source,
        title="Shown Cup",
        category=EventCategory.CONTEST,
        tags=["codeforces"],
        start_at=NOW,
        url="https://codeforces.com/contest/9",
        fingerprint="shown",
    )
    _state(session, hidden, UserState.SAVED)
    session.flush()

    feed = build_feed(session, USER, NOW)

    assert [item.title for item in feed.saved] == ["Hidden Cup"]
    assert [item.title for item in feed.todays_contests] == ["Shown Cup"]
    assert "Hidden Cup" not in {item.title for item in feed.new_opportunities}
    assert feed.counts["saved"] == 1


def test_ignored_event_is_excluded(session: Session) -> None:
    source = _source(session)
    _save_prefs(session)
    ignored = _persist(
        session,
        source,
        title="Ignored Notice",
        category=EventCategory.COLLEGE,
        url="https://college.example/ignored",
    )
    _persist(
        session,
        source,
        title="Visible Notice",
        category=EventCategory.COLLEGE,
        url="https://college.example/visible",
        fingerprint="visible",
    )
    _state(session, ignored, UserState.IGNORED)
    session.flush()

    feed = build_feed(session, USER, NOW)

    assert "Ignored Notice" not in _titles(feed)
    assert [item.title for item in feed.college] == ["Visible Notice"]


def test_low_confidence_event_is_shown_with_low_label(session: Session) -> None:
    source = _source(session)
    _save_prefs(session)
    _persist(
        session,
        source,
        title="Uncertain Notice",
        category=EventCategory.COLLEGE,
        url="https://college.example/uncertain",
        confidence={
            "registration_deadline": 0.1,
            "start_at": 0.4,
            "end_at": 0.9,
        },
    )
    session.flush()

    feed = build_feed(session, USER, NOW)
    item = feed.college[0]

    assert item.title == "Uncertain Notice"
    assert item.confidence_label == "low"
    assert item.relevance == 100
    assert feed.counts["new"] == 1


def test_minor_only_change_is_not_an_important_update(session: Session) -> None:
    source = _source(session)
    _save_prefs(session, student_only=False, modes=(), locations=())
    event = _persist(
        session,
        source,
        title="Renamed",
        registration_deadline=NOW + timedelta(days=10),
        url="https://example.com/renamed",
    )
    _version(
        session,
        event,
        Importance.MINOR,
        {"title": ["old name", "Renamed"]},
    )
    session.flush()

    feed = build_feed(session, USER, NOW)

    assert feed.important_updates == []
    assert feed.counts["important_updates"] == 0
    assert feed.new_opportunities[0].diff is None
    assert feed.new_opportunities[0].title == "Renamed"
