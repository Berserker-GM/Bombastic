"""SQLAlchemy 2.0 tables for the radar schema."""

from datetime import datetime
from enum import StrEnum
from typing import Any

from sqlalchemy import (
    JSON,
    Boolean,
    CheckConstraint,
    DateTime,
    Enum,
    ForeignKey,
    Integer,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship

from radar.models.enums import (
    EventCategory,
    EventMode,
    HealthStatus,
    Importance,
    Lifecycle,
    ScanStatus,
    SourceKind,
    Urgency,
    UserState,
)


def _enum(enum_cls: type[StrEnum]) -> Enum:
    return Enum(
        enum_cls,
        values_callable=lambda members: [member.value for member in members],
        native_enum=False,
        length=32,
    )


class Base(DeclarativeBase):
    pass


class Source(Base):
    __tablename__ = "sources"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    kind: Mapped[SourceKind] = mapped_column(_enum(SourceKind), nullable=False)
    name: Mapped[str] = mapped_column(Text, nullable=False)
    config: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    enabled: Mapped[bool] = mapped_column(Boolean, nullable=False)
    health_status: Mapped[HealthStatus] = mapped_column(
        _enum(HealthStatus), nullable=False
    )
    last_success_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    last_failure_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    consecutive_failures: Mapped[int] = mapped_column(Integer, nullable=False)
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)

    events: Mapped[list["Event"]] = relationship(back_populates="source")


class Event(Base):
    __tablename__ = "events"
    __table_args__ = (
        UniqueConstraint(
            "source_id",
            "external_id",
            name="uq_events_source_id_external_id",
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    source_id: Mapped[int] = mapped_column(ForeignKey("sources.id"), nullable=False)
    external_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    fingerprint: Mapped[str] = mapped_column(Text, nullable=False)
    category: Mapped[EventCategory] = mapped_column(
        _enum(EventCategory), nullable=False
    )
    title: Mapped[str] = mapped_column(Text, nullable=False)
    url: Mapped[str | None] = mapped_column(Text, nullable=True)
    start_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    end_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    registration_deadline: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    location: Mapped[str | None] = mapped_column(Text, nullable=True)
    mode: Mapped[EventMode | None] = mapped_column(_enum(EventMode), nullable=True)
    prize_text: Mapped[str | None] = mapped_column(Text, nullable=True)
    prize_amount_inr: Mapped[int | None] = mapped_column(Integer, nullable=True)
    organizer: Mapped[str | None] = mapped_column(Text, nullable=True)
    sponsors: Mapped[Any | None] = mapped_column(JSON, nullable=True)
    eligibility: Mapped[str | None] = mapped_column(Text, nullable=True)
    team_min: Mapped[int | None] = mapped_column(Integer, nullable=True)
    team_max: Mapped[int | None] = mapped_column(Integer, nullable=True)
    tags: Mapped[Any | None] = mapped_column(JSON, nullable=True)
    lifecycle: Mapped[Lifecycle] = mapped_column(_enum(Lifecycle), nullable=False)
    confidence: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    extra: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    first_seen_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )
    last_seen_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )
    content_hash: Mapped[str] = mapped_column(Text, nullable=False)

    source: Mapped[Source] = relationship(back_populates="events")
    evidence: Mapped[list["EventEvidence"]] = relationship(back_populates="event")
    versions: Mapped[list["EventVersion"]] = relationship(back_populates="event")
    user_states: Mapped[list["UserEventState"]] = relationship(back_populates="event")
    calendar_links: Mapped[list["CalendarLink"]] = relationship(back_populates="event")


class EventEvidence(Base):
    __tablename__ = "event_evidence"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    event_id: Mapped[int] = mapped_column(ForeignKey("events.id"), nullable=False)
    field: Mapped[str] = mapped_column(Text, nullable=False)
    source_url: Mapped[str | None] = mapped_column(Text, nullable=True)
    source_text: Mapped[str] = mapped_column(Text, nullable=False)
    confidence: Mapped[float] = mapped_column(nullable=False)
    captured_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )

    event: Mapped[Event] = relationship(back_populates="evidence")


class EventVersion(Base):
    __tablename__ = "event_versions"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    event_id: Mapped[int] = mapped_column(ForeignKey("events.id"), nullable=False)
    seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    changed_fields: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    importance: Mapped[Importance] = mapped_column(_enum(Importance), nullable=False)
    snapshot: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)

    event: Mapped[Event] = relationship(back_populates="versions")


class UserPrefs(Base):
    __tablename__ = "user_prefs"

    user_id: Mapped[str] = mapped_column(Text, primary_key=True)
    prefs: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)


class UserEventState(Base):
    __tablename__ = "user_event_states"
    __table_args__ = (
        CheckConstraint(
            "relevance >= 0 AND relevance <= 100",
            name="ck_user_event_states_relevance",
        ),
    )

    user_id: Mapped[str] = mapped_column(Text, primary_key=True)
    event_id: Mapped[int] = mapped_column(ForeignKey("events.id"), primary_key=True)
    state: Mapped[UserState] = mapped_column(_enum(UserState), nullable=False)
    relevance: Mapped[int] = mapped_column(Integer, nullable=False)
    urgency: Mapped[Urgency] = mapped_column(_enum(Urgency), nullable=False)
    reasons: Mapped[Any | None] = mapped_column(JSON, nullable=True)
    notified_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    event: Mapped[Event] = relationship(back_populates="user_states")


class CalendarLink(Base):
    __tablename__ = "calendar_links"

    user_id: Mapped[str] = mapped_column(Text, primary_key=True)
    event_id: Mapped[int] = mapped_column(ForeignKey("events.id"), primary_key=True)
    google_event_id: Mapped[str] = mapped_column(Text, nullable=False)
    last_synced_hash: Mapped[str | None] = mapped_column(Text, nullable=True)

    event: Mapped[Event] = relationship(back_populates="calendar_links")


class ScanRun(Base):
    __tablename__ = "scan_runs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    started_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )
    finished_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    status: Mapped[ScanStatus] = mapped_column(_enum(ScanStatus), nullable=False)
    per_source: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
