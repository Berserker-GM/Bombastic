"""initial schema

Revision ID: f1fae342a821
Revises:
Create Date: 2026-10-07 19:58:25.266870

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "f1fae342a821"
down_revision: str | Sequence[str] | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Upgrade schema."""
    op.create_table(
        "scan_runs",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "status",
            sa.Enum(
                "running",
                "done",
                name="scanstatus",
                native_enum=False,
                length=32,
            ),
            nullable=False,
        ),
        sa.Column("per_source", sa.JSON(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_table(
        "sources",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column(
            "kind",
            sa.Enum(
                "api",
                "scrape",
                "college",
                name="sourcekind",
                native_enum=False,
                length=32,
            ),
            nullable=False,
        ),
        sa.Column("name", sa.Text(), nullable=False),
        sa.Column("config", sa.JSON(), nullable=False),
        sa.Column("enabled", sa.Boolean(), nullable=False),
        sa.Column(
            "health_status",
            sa.Enum(
                "healthy",
                "degraded",
                "failed",
                "stale",
                "disabled",
                name="healthstatus",
                native_enum=False,
                length=32,
            ),
            nullable=False,
        ),
        sa.Column("last_success_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_failure_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("consecutive_failures", sa.Integer(), nullable=False),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_table(
        "user_prefs",
        sa.Column("user_id", sa.Text(), nullable=False),
        sa.Column("prefs", sa.JSON(), nullable=False),
        sa.PrimaryKeyConstraint("user_id"),
    )
    op.create_table(
        "events",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("source_id", sa.Integer(), nullable=False),
        sa.Column("external_id", sa.Text(), nullable=True),
        sa.Column("fingerprint", sa.Text(), nullable=False),
        sa.Column(
            "category",
            sa.Enum(
                "contest",
                "hackathon",
                "college",
                "scholarship",
                "club",
                "placement",
                "other",
                name="eventcategory",
                native_enum=False,
                length=32,
            ),
            nullable=False,
        ),
        sa.Column("title", sa.Text(), nullable=False),
        sa.Column("url", sa.Text(), nullable=True),
        sa.Column("start_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("end_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("registration_deadline", sa.DateTime(timezone=True), nullable=True),
        sa.Column("location", sa.Text(), nullable=True),
        sa.Column(
            "mode",
            sa.Enum(
                "online",
                "offline",
                "hybrid",
                "unknown",
                name="eventmode",
                native_enum=False,
                length=32,
            ),
            nullable=True,
        ),
        sa.Column("prize_text", sa.Text(), nullable=True),
        sa.Column("prize_amount_inr", sa.Integer(), nullable=True),
        sa.Column("organizer", sa.Text(), nullable=True),
        sa.Column("sponsors", sa.JSON(), nullable=True),
        sa.Column("eligibility", sa.Text(), nullable=True),
        sa.Column("team_min", sa.Integer(), nullable=True),
        sa.Column("team_max", sa.Integer(), nullable=True),
        sa.Column("tags", sa.JSON(), nullable=True),
        sa.Column(
            "lifecycle",
            sa.Enum(
                "upcoming",
                "registration_open",
                "registration_closed",
                "ongoing",
                "completed",
                "postponed",
                "cancelled",
                "unknown",
                name="lifecycle",
                native_enum=False,
                length=32,
            ),
            nullable=False,
        ),
        sa.Column("confidence", sa.JSON(), nullable=True),
        sa.Column("extra", sa.JSON(), nullable=True),
        sa.Column("first_seen_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_seen_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("content_hash", sa.Text(), nullable=False),
        sa.ForeignKeyConstraint(["source_id"], ["sources.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "source_id",
            "external_id",
            name="uq_events_source_id_external_id",
        ),
    )
    op.create_table(
        "calendar_links",
        sa.Column("user_id", sa.Text(), nullable=False),
        sa.Column("event_id", sa.Integer(), nullable=False),
        sa.Column("google_event_id", sa.Text(), nullable=False),
        sa.Column("last_synced_hash", sa.Text(), nullable=True),
        sa.ForeignKeyConstraint(["event_id"], ["events.id"]),
        sa.PrimaryKeyConstraint("user_id", "event_id"),
    )
    op.create_table(
        "event_evidence",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("event_id", sa.Integer(), nullable=False),
        sa.Column("field", sa.Text(), nullable=False),
        sa.Column("source_url", sa.Text(), nullable=True),
        sa.Column("source_text", sa.Text(), nullable=False),
        sa.Column("confidence", sa.Double(), nullable=False),
        sa.Column("captured_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["event_id"], ["events.id"]),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_table(
        "event_versions",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("event_id", sa.Integer(), nullable=False),
        sa.Column("seen_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("changed_fields", sa.JSON(), nullable=False),
        sa.Column(
            "importance",
            sa.Enum(
                "important",
                "minor",
                name="importance",
                native_enum=False,
                length=32,
            ),
            nullable=False,
        ),
        sa.Column("snapshot", sa.JSON(), nullable=False),
        sa.ForeignKeyConstraint(["event_id"], ["events.id"]),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_table(
        "user_event_states",
        sa.Column("user_id", sa.Text(), nullable=False),
        sa.Column("event_id", sa.Integer(), nullable=False),
        sa.Column(
            "state",
            sa.Enum(
                "new",
                "seen",
                "ignored",
                "saved",
                name="userstate",
                native_enum=False,
                length=32,
            ),
            nullable=False,
        ),
        sa.Column("relevance", sa.Integer(), nullable=False),
        sa.Column(
            "urgency",
            sa.Enum(
                "critical",
                "soon",
                "upcoming",
                "later",
                "none",
                name="urgency",
                native_enum=False,
                length=32,
            ),
            nullable=False,
        ),
        sa.Column("reasons", sa.JSON(), nullable=True),
        sa.Column("notified_at", sa.DateTime(timezone=True), nullable=True),
        sa.CheckConstraint(
            "relevance >= 0 AND relevance <= 100",
            name="ck_user_event_states_relevance",
        ),
        sa.ForeignKeyConstraint(["event_id"], ["events.id"]),
        sa.PrimaryKeyConstraint("user_id", "event_id"),
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_table("user_event_states")
    op.drop_table("event_versions")
    op.drop_table("event_evidence")
    op.drop_table("calendar_links")
    op.drop_table("events")
    op.drop_table("user_prefs")
    op.drop_table("sources")
    op.drop_table("scan_runs")
