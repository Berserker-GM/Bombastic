"""Collector contract. A failure is a status, never an empty success."""

import json
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, ClassVar, Literal

import httpx
from pydantic import BaseModel, Field, ValidationError

from radar.models.enums import EventCategory, EventMode, Lifecycle

CollectStatus = Literal["ok", "unreachable", "malformed"]


class EvidenceItem(BaseModel):
    field: str
    source_url: str | None = None
    source_text: str
    confidence: float
    captured_at: datetime | None = None


class RawEvent(BaseModel):
    source_name: str
    title: str
    category: EventCategory
    external_id: str | None = None
    fingerprint: str | None = None
    url: str | None = None
    start_at: datetime | None = None
    end_at: datetime | None = None
    registration_deadline: datetime | None = None
    location: str | None = None
    mode: EventMode | None = None
    prize_text: str | None = None
    prize_amount_inr: int | None = None
    organizer: str | None = None
    sponsors: Any | None = None
    eligibility: str | None = None
    team_min: int | None = None
    team_max: int | None = None
    tags: list[str] | None = None
    lifecycle: Lifecycle | None = None
    extra: dict[str, Any] | None = None
    first_seen_at: datetime | None = None
    last_seen_at: datetime | None = None
    content_hash: str | None = None
    confidence: dict[str, float] = Field(default_factory=dict)
    evidence: list[EvidenceItem] = Field(default_factory=list)


class CollectResult(BaseModel):
    status: CollectStatus
    events: list[RawEvent] = Field(default_factory=list)
    error: str | None = None


class Collector(ABC):
    min_expected: ClassVar[int | None] = None

    async def collect(self) -> CollectResult:
        try:
            events = await self.fetch()
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code >= 500:
                return _failure("unreachable", exc)
            return _failure("malformed", exc)
        except httpx.TransportError as exc:
            return _failure("unreachable", exc)
        except (ValidationError, KeyError, TypeError, json.JSONDecodeError) as exc:
            return _failure("malformed", exc)
        return self._from_events(events)

    @abstractmethod
    async def fetch(self) -> list[RawEvent]:
        """Return parsed events. Transport and shape errors propagate."""

    def _from_events(self, events: list[RawEvent]) -> CollectResult:
        if self.min_expected is not None and len(events) < self.min_expected:
            return CollectResult(
                status="malformed",
                events=events,
                error="suspiciously empty",
            )
        return CollectResult(status="ok", events=events, error=None)


def _failure(status: CollectStatus, exc: Exception) -> CollectResult:
    return CollectResult(status=status, events=[], error=str(exc))
