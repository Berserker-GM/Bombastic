"""Closed vocabularies for the radar schema. Import these; do not redefine them."""

from enum import StrEnum


class SourceKind(StrEnum):
    API = "api"
    SCRAPE = "scrape"
    COLLEGE = "college"


class HealthStatus(StrEnum):
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    FAILED = "failed"
    STALE = "stale"
    DISABLED = "disabled"


class EventCategory(StrEnum):
    CONTEST = "contest"
    HACKATHON = "hackathon"
    COLLEGE = "college"
    SCHOLARSHIP = "scholarship"
    CLUB = "club"
    PLACEMENT = "placement"
    OTHER = "other"


class EventMode(StrEnum):
    ONLINE = "online"
    OFFLINE = "offline"
    HYBRID = "hybrid"
    UNKNOWN = "unknown"


class Lifecycle(StrEnum):
    UPCOMING = "upcoming"
    REGISTRATION_OPEN = "registration_open"
    REGISTRATION_CLOSED = "registration_closed"
    ONGOING = "ongoing"
    COMPLETED = "completed"
    POSTPONED = "postponed"
    CANCELLED = "cancelled"
    UNKNOWN = "unknown"


class Importance(StrEnum):
    IMPORTANT = "important"
    MINOR = "minor"


class UserState(StrEnum):
    NEW = "new"
    SEEN = "seen"
    IGNORED = "ignored"
    SAVED = "saved"


class Urgency(StrEnum):
    CRITICAL = "critical"
    SOON = "soon"
    UPCOMING = "upcoming"
    LATER = "later"
    NONE = "none"


class SourceResult(StrEnum):
    OK_NEW = "ok_new"
    OK_NOTHING_NEW = "ok_nothing_new"
    UNREACHABLE = "unreachable"
    MALFORMED = "malformed"


class ScanStatus(StrEnum):
    RUNNING = "running"
    DONE = "done"
