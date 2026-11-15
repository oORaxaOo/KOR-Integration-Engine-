"""Versioned, deeply immutable event envelope shared by independent KOR engines.

Producer identity in this envelope is provenance metadata, not authentication.
The host adapter must authenticate the submitting process separately.
"""
from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
import json
import math
import re
from types import MappingProxyType
from typing import Any
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


def _normalize_json(value: Any, *, depth: int = 0) -> Any:
    """Copy and validate JSON values while rejecting coercive key conversion."""
    if depth > 64:
        raise ValueError("payload nesting exceeds 64 levels")
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("payload numbers must be finite")
        return value
    if isinstance(value, Mapping):
        normalized: dict[str, Any] = {}
        for key, child in value.items():
            if not isinstance(key, str):
                raise ValueError("payload object keys must be strings")
            normalized[key] = _normalize_json(child, depth=depth + 1)
        return normalized
    if isinstance(value, (list, tuple)):
        return [_normalize_json(child, depth=depth + 1) for child in value]
    raise ValueError("payload must contain only finite JSON-compatible data")


def _freeze(value: Any) -> Any:
    if isinstance(value, dict):
        return MappingProxyType({key: _freeze(child) for key, child in value.items()})
    if isinstance(value, list):
        return tuple(_freeze(child) for child in value)
    return value


def _thaw(value: Any) -> Any:
    """Return ordinary mutable JSON containers for serialization/export."""
    if isinstance(value, Mapping):
        return {key: _thaw(child) for key, child in value.items()}
    if isinstance(value, tuple):
        return [_thaw(child) for child in value]
    return value


def _string_tuple(value: Any, field: str, *, non_empty: bool = False) -> tuple[str, ...]:
    if not isinstance(value, (tuple, list)):
        raise ValueError(f"{field} must be a list or tuple")
    result = tuple(value)
    if non_empty and not result:
        raise ValueError(f"{field} must not be empty")
    if any(not isinstance(item, str) or not item.strip() for item in result):
        raise ValueError(f"{field} must contain non-empty strings")
    return result


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
        refs = _string_tuple(self.evidence_refs, "verification evidence_refs", non_empty=True)
        object.__setattr__(self, "evidence_refs", refs)


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

        consumers = _string_tuple(self.allowed_consumers, "allowed_consumers", non_empty=True)
        if any(item not in ENGINE_IDS for item in consumers):
            raise ValueError("allowed_consumers must contain registered engine IDs")
        if len(set(consumers)) != len(consumers):
            raise ValueError("allowed_consumers must not contain duplicates")
        if self.producer_engine in consumers:
            raise ValueError("an event cannot be routed back to its producer")
        object.__setattr__(self, "allowed_consumers", consumers)

        if isinstance(self.hop_count, bool) or not isinstance(self.hop_count, int) or not 0 <= self.hop_count <= 4:
            raise ValueError("hop_count must be an integer from 0 to 4")
        if self.epistemic_status in {EpistemicStatus.VERIFIED, EpistemicStatus.REFUTED}:
            if not isinstance(self.verification, Verification):
                raise ValueError("verified/refuted events require explicit verification metadata")
        elif self.verification is not None:
            raise ValueError("verification metadata is only valid for verified/refuted events")

        for name, value in (("session_id", self.session_id), ("task_id", self.task_id), ("correlation_id", self.correlation_id), ("idempotency_key", self.idempotency_key)):
            if value is not None and (not isinstance(value, str) or not value.strip()):
                raise ValueError(f"{name} must be None or a non-empty string")

        refs = _string_tuple(self.evidence_refs, "evidence_refs")
        if not isinstance(self.parent_event_ids, (tuple, list)) or any(not isinstance(parent, UUID) for parent in self.parent_event_ids):
            raise ValueError("parent_event_ids must contain UUID values")
        object.__setattr__(self, "evidence_refs", refs)
        object.__setattr__(self, "parent_event_ids", tuple(self.parent_event_ids))

        if not isinstance(self.payload, Mapping):
            raise ValueError("payload must be a JSON object")
        normalized = _normalize_json(self.payload)
        encoded = json.dumps(normalized, ensure_ascii=False, allow_nan=False, separators=(",", ":"))
        if len(encoded.encode("utf-8")) > _MAX_PAYLOAD_BYTES:
            raise ValueError("payload exceeds 65536 UTF-8 bytes")
        object.__setattr__(self, "payload", _freeze(normalized))

    def is_expired(self, now: datetime) -> bool:
        _aware_datetime(now, "now")
        return self.expires_at is not None and self.expires_at <= now


def event_to_dict(event: IntegrationEvent) -> dict[str, Any]:
    """Serialize an event to the public JSON protocol envelope."""
    if not isinstance(event, IntegrationEvent):
        raise TypeError("event must be an IntegrationEvent")
    verification = None
    if event.verification is not None:
        verification = {
            "verifier": event.verification.verifier,
            "criteria": event.verification.criteria,
            "scope": event.verification.scope,
            "verified_at": event.verification.verified_at.isoformat(),
            "evidence_refs": list(event.verification.evidence_refs),
        }
    return {
        "event_id": str(event.event_id),
        "schema_version": event.schema_version,
        "event_type": event.event_type,
        "producer_engine": event.producer_engine,
        "producer_version": event.producer_version,
        "occurred_at": event.occurred_at.isoformat(),
        "recorded_at": event.recorded_at.isoformat(),
        "epistemic_status": event.epistemic_status.value,
        "sensitivity": event.sensitivity.value,
        "allowed_consumers": list(event.allowed_consumers),
        "payload": _thaw(event.payload),
        "evidence_refs": list(event.evidence_refs),
        "parent_event_ids": [str(item) for item in event.parent_event_ids],
        "session_id": event.session_id,
        "task_id": event.task_id,
        "correlation_id": event.correlation_id,
        "expires_at": event.expires_at.isoformat() if event.expires_at else None,
        "hop_count": event.hop_count,
        "idempotency_key": event.idempotency_key,
        "verification": verification,
    }
