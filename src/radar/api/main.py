"""Single-user HTTP API. The desktop client sends ``Authorization: Bearer``."""

import secrets
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Literal

from fastapi import APIRouter, BackgroundTasks, Depends, FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pydantic import BaseModel, Field
from sqlalchemy import select
from sqlalchemy.orm import Session, sessionmaker

from radar.calendar.google import (
    CalendarAuthError,
    CalendarService,
    GoogleCalendarService,
    build_google_events_client,
)
from radar.config import Settings, get_settings
from radar.db import SessionLocal
from radar.models import (
    Event,
    EventEvidence,
    EventVersion,
    HealthStatus,
    ScanRun,
    ScanStatus,
    Source,
    UserEventState,
    UserPrefs,
    UserState,
)
from radar.pipeline.scan import BUILTIN_SOURCE_NAMES, run_scan, scan_blocks
from radar.pipeline.scoring import FeedItem, build_feed, relevance, urgency

USER_ID = "local"
_IST = timezone(timedelta(hours=5, minutes=30), name="Asia/Kolkata")
_SCAN_COOLDOWN = timedelta(minutes=10)
_bearer = HTTPBearer(auto_error=False)

ActionName = Literal["add_to_calendar", "ignore", "save", "unsave", "seen"]


class FeedItemOut(BaseModel):
    event_id: int
    title: str
    url: str | None
    relevance: int
    urgency: str
    confidence_label: str
    reasons: list[str]
    diff: str | None
    key_at: datetime | None
    important_unseen: bool
    is_new: bool


class ScanWindow(BaseModel):
    last_scan_at: datetime | None
    next_scan_at: datetime


class SourceHealthOut(BaseModel):
    name: str
    health_status: str
    last_success_at: datetime | None
    last_error: str | None


class FeedOut(BaseModel):
    important_updates: list[FeedItemOut]
    deadlines_soon: list[FeedItemOut]
    new_opportunities: list[FeedItemOut]
    todays_contests: list[FeedItemOut]
    saved: list[FeedItemOut]
    college: list[FeedItemOut]
    hackathons: list[FeedItemOut]
    counts: dict[str, int]
    scan: ScanWindow
    source_health: list[SourceHealthOut]


class EvidenceOut(BaseModel):
    id: int
    field: str
    source_url: str | None
    source_text: str
    confidence: float
    captured_at: datetime | None


class VersionOut(BaseModel):
    id: int
    seen_at: datetime
    changed_fields: dict[str, Any]
    importance: str
    snapshot: dict[str, Any]


class EventDetail(BaseModel):
    id: int
    source_id: int
    external_id: str | None
    fingerprint: str
    category: str
    title: str
    url: str | None
    start_at: datetime | None
    end_at: datetime | None
    registration_deadline: datetime | None
    location: str | None
    mode: str | None
    prize_text: str | None
    prize_amount_inr: int | None
    organizer: str | None
    sponsors: Any | None
    eligibility: str | None
    team_min: int | None
    team_max: int | None
    tags: list[Any] | None
    lifecycle: str
    confidence: dict[str, Any]
    extra: dict[str, Any] | None
    first_seen_at: datetime
    last_seen_at: datetime
    evidence: list[EvidenceOut]
    versions: list[VersionOut]


class ActionBody(BaseModel):
    action: ActionName


class ActionResult(BaseModel):
    action: ActionName
    event_id: int


class PrefsBody(BaseModel):
    modes: list[str] = Field(default_factory=list)
    locations: list[str] = Field(default_factory=list)
    min_prize: int | None = None
    student_only: bool = False
    companies: list[str] = Field(default_factory=list)
    topics: list[str] = Field(default_factory=list)
    contest_platforms: list[str] = Field(default_factory=list)
    calendar_mode: Literal["auto", "confirm"] | None = None


class SourceScanOut(BaseModel):
    result: str
    counts: dict[str, int] = Field(default_factory=dict)
    error: str | None = None


class ScanStatusOut(BaseModel):
    id: int
    started_at: datetime
    finished_at: datetime | None
    status: str
    per_source: dict[str, SourceScanOut]


class ScanAccepted(BaseModel):
    id: int
    status: Literal["running"]


class ScanNowBody(BaseModel):
    sources: list[str] | None = None


class HealthOut(BaseModel):
    ok: Literal[True]


def _auth(
    credentials: HTTPAuthorizationCredentials | None = Depends(_bearer),
) -> None:
    expected = get_settings().api_token
    supplied = None if credentials is None else credentials.credentials
    if (
        not expected
        or credentials is None
        or credentials.scheme.lower() != "bearer"
        or supplied is None
        or not secrets.compare_digest(supplied, expected)
    ):
        raise HTTPException(
            status_code=401,
            detail="unauthorized",
            headers={"WWW-Authenticate": "Bearer"},
        )


def get_session(request: Request) -> Iterator[Session]:
    session = request.app.state.session_factory()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def get_calendar_service() -> CalendarService:
    settings = get_settings()
    if not calendar_configured(settings):
        raise HTTPException(status_code=503, detail="calendar not configured")
    try:
        client = build_google_events_client()
    except CalendarAuthError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc
    return GoogleCalendarService(client)


def calendar_configured(settings: Settings) -> bool:
    token_json = settings.google_token_json
    if isinstance(token_json, str) and token_json.strip():
        return True
    return Path(settings.google_token_path).is_file()


def next_scan_at(now: datetime) -> datetime:
    local = _as_utc(now).astimezone(_IST)
    candidate = local.replace(hour=13, minute=0, second=0, microsecond=0)
    if local > candidate:
        candidate += timedelta(days=1)
    return candidate.astimezone(UTC)


router = APIRouter(dependencies=[Depends(_auth)])


@router.get("/feed", response_model=FeedOut)
def get_feed(session: Session = Depends(get_session)) -> FeedOut:
    now = datetime.now(UTC)
    feed = build_feed(session, USER_ID, now)
    return FeedOut(
        important_updates=_items(feed.important_updates),
        deadlines_soon=_items(feed.deadlines_soon),
        new_opportunities=_items(feed.new_opportunities),
        todays_contests=_items(feed.todays_contests),
        saved=_items(feed.saved),
        college=_items(feed.college),
        hackathons=_items(feed.hackathons),
        counts=feed.counts,
        scan=ScanWindow(
            last_scan_at=_last_scan_at(session),
            next_scan_at=next_scan_at(now),
        ),
        source_health=_source_health(session),
    )


@router.get("/events/{event_id}", response_model=EventDetail)
def get_event(event_id: int, session: Session = Depends(get_session)) -> EventDetail:
    event = session.get(Event, event_id)
    if event is None:
        raise HTTPException(status_code=404, detail="event not found")
    evidence = list(
        session.scalars(
            select(EventEvidence)
            .where(EventEvidence.event_id == event.id)
            .order_by(EventEvidence.id)
        )
    )
    versions = list(
        session.scalars(
            select(EventVersion)
            .where(EventVersion.event_id == event.id)
            .order_by(EventVersion.seen_at, EventVersion.id)
        )
    )
    return _event_detail(event, evidence, versions)


@router.post("/events/{event_id}/action", response_model=ActionResult)
def post_action(
    event_id: int,
    body: ActionBody,
    request: Request,
    session: Session = Depends(get_session),
) -> ActionResult:
    event = session.get(Event, event_id)
    if event is None:
        raise HTTPException(status_code=404, detail="event not found")
    if body.action == "add_to_calendar":
        _add_to_calendar(_resolve_calendar(request), session, event_id)
    else:
        _set_state(session, event, body.action)
    return ActionResult(action=body.action, event_id=event_id)


@router.get("/prefs", response_model=PrefsBody)
def get_prefs(session: Session = Depends(get_session)) -> PrefsBody:
    row = session.get(UserPrefs, USER_ID)
    if row is None or not isinstance(row.prefs, dict):
        return PrefsBody()
    return PrefsBody.model_validate(row.prefs)


@router.put("/prefs", response_model=PrefsBody)
def put_prefs(body: PrefsBody, session: Session = Depends(get_session)) -> PrefsBody:
    stored = body.model_dump()
    row = session.get(UserPrefs, USER_ID)
    if row is None:
        session.add(UserPrefs(user_id=USER_ID, prefs=stored))
    else:
        row.prefs = stored
    return body


@router.get("/scan/status", response_model=ScanStatusOut)
def get_scan_status(session: Session = Depends(get_session)) -> ScanStatusOut:
    run = _latest_scan(session)
    if run is None:
        raise HTTPException(status_code=404, detail="no scan")
    return _scan_status(run)


@router.post("/scan/now", status_code=202, response_model=ScanAccepted)
def post_scan_now(
    request: Request,
    background: BackgroundTasks,
    body: ScanNowBody | None = None,
    session: Session = Depends(get_session),
) -> ScanAccepted:
    names = None if body is None else body.sources
    if names is not None:
        unknown = [name for name in names if name not in BUILTIN_SOURCE_NAMES]
        if unknown:
            raise HTTPException(status_code=422, detail=f"unknown source {unknown[0]}")
    now = datetime.now(UTC)
    if _scan_running(session):
        raise HTTPException(status_code=409, detail="scan already running")
    if _scan_started_since(session, now - _SCAN_COOLDOWN):
        raise HTTPException(status_code=429, detail="scan started recently")
    run = ScanRun(
        started_at=now,
        finished_at=None,
        status=ScanStatus.RUNNING,
        per_source={},
    )
    session.add(run)
    session.flush()
    scan_id = run.id
    session.commit()
    background.add_task(
        _execute_scan,
        request.app.state.session_factory,
        names,
        scan_id,
    )
    return ScanAccepted(id=scan_id, status="running")


def _execute_scan(
    session_factory: sessionmaker[Session],
    sources: list[str] | None,
    scan_id: int,
) -> None:
    with session_factory() as session:
        run_scan(session, sources, scan_id=scan_id)


_TAURI_ORIGINS = (
    "http://tauri.localhost",
    "tauri://localhost",
    "http://localhost:1420",
)


def cors_allow_origins() -> list[str]:
    origins = list(_TAURI_ORIGINS)
    for part in get_settings().cors_origins.split(","):
        origin = part.strip()
        if origin and origin != "*":
            origins.append(origin)
    return origins


public = APIRouter()


@public.get("/health", response_model=HealthOut)
def get_health() -> HealthOut:
    return HealthOut(ok=True)


def create_app(session_factory: sessionmaker[Session] | None = None) -> FastAPI:
    app = FastAPI(title="radar")
    app.state.session_factory = session_factory or SessionLocal
    app.add_middleware(
        CORSMiddleware,
        allow_origins=cors_allow_origins(),
        allow_methods=["GET", "POST", "PUT"],
        allow_headers=["Authorization", "Content-Type"],
    )
    app.include_router(public)
    app.include_router(router)
    return app


app = create_app()


def _resolve_calendar(request: Request) -> CalendarService:
    override = request.app.dependency_overrides.get(get_calendar_service)
    if override is not None:
        return override()
    return get_calendar_service()


def _add_to_calendar(
    calendar: CalendarService, session: Session, event_id: int
) -> None:
    try:
        calendar.add_to_calendar(session, USER_ID, event_id)
    except CalendarAuthError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc


def _set_state(session: Session, event: Event, action: ActionName) -> None:
    target = {
        "ignore": UserState.IGNORED,
        "save": UserState.SAVED,
        "unsave": UserState.SEEN,
        "seen": UserState.SEEN,
    }[action]
    now = datetime.now(UTC)
    scored = relevance(event, _prefs_mapping(session))
    row = session.get(UserEventState, (USER_ID, event.id))
    if row is None:
        session.add(
            UserEventState(
                user_id=USER_ID,
                event_id=event.id,
                state=target,
                relevance=scored.score,
                urgency=urgency(event, now),
                reasons=list(scored.reasons),
            )
        )
        return
    row.state = target
    row.relevance = scored.score
    row.urgency = urgency(event, now)
    row.reasons = list(scored.reasons)


def _prefs_mapping(session: Session) -> dict[str, Any]:
    row = session.get(UserPrefs, USER_ID)
    if row is None or not isinstance(row.prefs, dict):
        return {}
    return row.prefs


def _items(items: list[FeedItem]) -> list[FeedItemOut]:
    return [
        FeedItemOut(
            event_id=item.event_id,
            title=item.title,
            url=item.url,
            relevance=item.relevance,
            urgency=item.urgency.value,
            confidence_label=item.confidence_label,
            reasons=list(item.reasons),
            diff=item.diff,
            key_at=item.key_at,
            important_unseen=item.important_unseen,
            is_new=item.is_new,
        )
        for item in items
    ]


def _event_detail(
    event: Event,
    evidence: list[EventEvidence],
    versions: list[EventVersion],
) -> EventDetail:
    return EventDetail(
        id=event.id,
        source_id=event.source_id,
        external_id=event.external_id,
        fingerprint=event.fingerprint,
        category=event.category.value,
        title=event.title,
        url=event.url,
        start_at=event.start_at,
        end_at=event.end_at,
        registration_deadline=event.registration_deadline,
        location=event.location,
        mode=None if event.mode is None else event.mode.value,
        prize_text=event.prize_text,
        prize_amount_inr=event.prize_amount_inr,
        organizer=event.organizer,
        sponsors=event.sponsors,
        eligibility=event.eligibility,
        team_min=event.team_min,
        team_max=event.team_max,
        tags=None if event.tags is None else list(event.tags),
        lifecycle=event.lifecycle.value,
        confidence=dict(event.confidence or {}),
        extra=event.extra,
        first_seen_at=event.first_seen_at,
        last_seen_at=event.last_seen_at,
        evidence=[
            EvidenceOut(
                id=item.id,
                field=item.field,
                source_url=item.source_url,
                source_text=item.source_text,
                confidence=item.confidence,
                captured_at=item.captured_at,
            )
            for item in evidence
        ],
        versions=[
            VersionOut(
                id=item.id,
                seen_at=item.seen_at,
                changed_fields=item.changed_fields,
                importance=item.importance.value,
                snapshot=item.snapshot,
            )
            for item in versions
        ],
    )


def _source_health(session: Session) -> list[SourceHealthOut]:
    rows = session.scalars(select(Source).order_by(Source.name)).all()
    return [
        SourceHealthOut(
            name=source.name,
            health_status=source.health_status.value,
            last_success_at=source.last_success_at,
            last_error=source.last_error,
        )
        for source in rows
        if source.health_status != HealthStatus.HEALTHY
    ]


def _last_scan_at(session: Session) -> datetime | None:
    run = session.scalar(
        select(ScanRun)
        .where(ScanRun.status == ScanStatus.DONE)
        .order_by(ScanRun.finished_at.desc(), ScanRun.id.desc())
    )
    if run is None:
        return None
    return run.finished_at


def _latest_scan(session: Session) -> ScanRun | None:
    return session.scalar(
        select(ScanRun).order_by(ScanRun.started_at.desc(), ScanRun.id.desc())
    )


def _scan_running(session: Session) -> bool:
    return scan_blocks(session)


def _scan_started_since(session: Session, cutoff: datetime) -> bool:
    for started in session.scalars(select(ScanRun.started_at)):
        if _as_utc(started) >= cutoff:
            return True
    return False


def _scan_status(run: ScanRun) -> ScanStatusOut:
    per_source: dict[str, SourceScanOut] = {}
    raw = run.per_source if isinstance(run.per_source, dict) else {}
    for name, payload in raw.items():
        if name == "calendar" or not isinstance(payload, dict):
            continue
        counts = payload.get("counts")
        error = payload.get("error")
        per_source[str(name)] = SourceScanOut(
            result=str(payload.get("result", "")),
            counts=counts if isinstance(counts, dict) else {},
            error=error if isinstance(error, str) else None,
        )
    return ScanStatusOut(
        id=run.id,
        started_at=run.started_at,
        finished_at=run.finished_at,
        status=run.status.value,
        per_source=per_source,
    )


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)
