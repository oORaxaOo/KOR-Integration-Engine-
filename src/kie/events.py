"""Versioned, immutable event envelope shared by independent KOR engines.

Producer identity in this envelope is provenance metadata, not authentication.
The host adapter must authenticate the submitting process separately.
"""
from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
import json
import re
from typing import Any, Mapping
from uuid import UUID

ENGINE_IDS = frozenset({"critical_thinking", "reliability", "evidence", "emotion"})
_EVENT_TYPE = re.compile(r"^[a-z][a-z0-9_.-]{1,95}$")
_MAX_PAYLOAD_BYTES = 64 * 1024

class EpistemicStatus(str, Enum):
    OBSERVED = "observed"
    HYPOTHESIS = "hypothesis"
    UNDER_REVIEW = "under_review"
    VERIFIED = "verified"
    REFUTED = "refuted"
    UNRESOLVED = "unresolved"

class Sensitivity(str, Enum):
    PUBLIC = "public"
    INTERNAL = "internal"
    RESTRICTED = "restricted"

def _aware_datetime(value: datetime, field: str) -> None:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field} must be a timezone-aware datetime")

def _json_value(value: Any) -> Any:
    """Return a detached JSON-compatible value or raise ValueError."""
    try:
        encoded = json.dumps(value, ensure_ascii=False, allow_nan=False, separators=(",", ":"))
        if len(encoded.encode("utf-8")) > _MAX_PAYLOAD_BYTES:
            raise ValueError("payload exceeds 65536 UTF-8 bytes")
        return json.loads(encoded)
    except (TypeError, ValueError, RecursionError) as exc:
        if isinstance(exc, ValueError) and str(exc).startswith("payload exceeds"):
            raise
        raise ValueError("payload must be finite, JSON-compatible data") from exc

@dataclass(frozen=True, slots=True)
class Verification:
    """Explicit, scoped verification metadata; never inferred from repetition."""
    verifier: str
    criteria: str
    scope: str
    verified_at: datetime
    evidence_refs: tuple[str, ...]

    def __post_init__(self) -> None:
        for name in ("verifier", "criteria", "scope"):
            value = getattr(self, name)
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"{name} must be a non-empty string")
        _aware_datetime(self.verified_at, "verified_at")
        if not self.evidence_refs or any(not isinstance(x, str) or not x.strip() for x in self.evidence_refs):
            raise ValueError("verification requires non-empty evidence references")

@dataclass(frozen=True, slots=True)
class IntegrationEvent:
    event_id: UUID
    schema_version: int
    event_type: str
    producer_engine: str
    producer_version: str
    occurred_at: datetime
    recorded_at: datetime
    epistemic_status: EpistemicStatus
    sensitivity: Sensitivity
    allowed_consumers: tuple[str, ...]
    payload: Mapping[str, Any]
    evidence_refs: tuple[str, ...] = ()
    parent_event_ids: tuple[UUID, ...] = ()
    session_id: str | None = None
    task_id: str | None = None
    correlation_id: str | None = None
    expires_at: datetime | None = None
    hop_count: int = 0
    idempotency_key: str | None = None
    verification: Verification | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.event_id, UUID):
            raise ValueError("event_id must be a UUID")
        if isinstance(self.schema_version, bool) or self.schema_version != 1:
            raise ValueError("unsupported schema_version")
        if not isinstance(self.event_type, str) or not _EVENT_TYPE.fullmatch(self.event_type):
            raise ValueError("event_type must be a lowercase identifier (2-96 characters)")
        if self.producer_engine not in ENGINE_IDS:
            raise ValueError("producer_engine is not a registered engine")
        if not isinstance(self.producer_version, str) or not self.producer_version.strip():
            raise ValueError("producer_version must be non-empty")
        _aware_datetime(self.occurred_at, "occurred_at")
        _aware_datetime(self.recorded_at, "recorded_at")
        if self.expires_at is not None:
            _aware_datetime(self.expires_at, "expires_at")
            if self.expires_at <= self.recorded_at:
                raise ValueError("expires_at must be later than recorded_at")
        if not isinstance(self.epistemic_status, EpistemicStatus):
            raise ValueError("epistemic_status must be an EpistemicStatus")
        if not isinstance(self.sensitivity, Sensitivity):
            raise ValueError("sensitivity must be a Sensitivity")
        if not self.allowed_consumers or any(x not in ENGINE_IDS for x in self.allowed_consumers):
            raise ValueError("allowed_consumers must contain registered engine IDs")
        if len(set(self.allowed_consumers)) != len(self.allowed_consumers):
            raise ValueError("allowed_consumers must not contain duplicates")
        if self.producer_engine in self.allowed_consumers:
            raise ValueError("an event cannot be routed back to its producer")
        if isinstance(self.hop_count, bool) or not isinstance(self.hop_count, int) or self.hop_count < 0 or self.hop_count > 4:
            raise ValueError("hop_count must be an integer from 0 to 4")
        if self.epistemic_status in {EpistemicStatus.VERIFIED, EpistemicStatus.REFUTED}:
            if self.verification is None:
                raise ValueError("verified/refuted events require explicit verification metadata")
        elif self.verification is not None:
            raise ValueError("verification metadata is only valid for verified/refuted events")
        for name, value in (("session_id", self.session_id), ("task_id", self.task_id), ("correlation_id", self.correlation_id), ("idempotency_key", self.idempotency_key)):
            if value is not None and (not isinstance(value, str) or not value.strip()):
                raise ValueError(f"{name} must be None or a non-empty string")
        if any(not isinstance(ref, str) or not ref.strip() for ref in self.evidence_refs):
            raise ValueError("evidence_refs must contain non-empty strings")
        if any(not isinstance(parent, UUID) for parent in self.parent_event_ids):
            raise ValueError("parent_event_ids must contain UUID values")
        object.__setattr__(self, "payload", _json_value(dict(self.payload)))

    def is_expired(self, now: datetime) -> bool:
        _aware_datetime(now, "now")
        return self.expires_at is not None and self.expires_at <= now
