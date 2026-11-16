"""Bounded, privacy-minimized event publication from the four independent engines.

The publisher is an optional integration surface for engine plugins. Each
public helper accepts only an engine-specific summary contract, never a raw
conversation, prompt, tool result, or reflection narrative.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import math
from pathlib import Path
from typing import Any, Mapping
from uuid import UUID, uuid4

from .events import EpistemicStatus, IntegrationEvent, Sensitivity
from .event_store import SQLiteEventStore


@dataclass(frozen=True, slots=True)
class PublishReceipt:
    event_id: UUID
    sequence: int
    inserted: bool


_CTE_REASON_CODES = {
    "The user explicitly requested a critical review.": "explicit_request",
    "High-impact or sensitive decision signal detected.": "high_impact_decision",
    "Technical factual claim detected; evidence and uncertainty need checking.": "technical_factual_claim",
    "Technical proposal or decision detected.": "technical_proposal",
    "Personal experiences are not automatically challenged.": "personal_experience",
    "Opinions are not automatically challenged.": "opinion",
    "No sufficient signal for an unsolicited review.": "no_review_signal",
}
_CTE_INTENTS = {
    "personal_experience", "opinion", "factual_claim", "project_proposal",
    "decision", "question", "unknown",
}
_CTE_LEVELS = {"normal", "review", "critical"}
_RELIABILITY_PRESSURES = {"disabled", "waiting", "normal", "warning", "critical"}
_RELIABILITY_SIGNALS = {
    "provider_error", "invalid_tool_call", "response_not_text",
    "empty_response", "response_too_large", "repeated_lines",
}
_EMOTION_STATUSES = {"awaiting_evidence", "completed", "budget_exhausted"}
_EMOTION_ASSESSMENTS = {
    "supported", "unsupported", "contradicted", "mixed", "undetermined",
}
_CONSUMERS = {
    "critical_thinking": ("reliability", "emotion"),
    "reliability": ("critical_thinking", "emotion"),
    "emotion": ("critical_thinking", "reliability"),
}


def _bounded_count(value: Any, field: str, *, maximum: int = 1_000_000_000) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not 0 <= value <= maximum:
        raise ValueError(f"{field} must be an integer between 0 and {maximum}")
    return value


def _bounded_number(value: Any, field: str, *, maximum: float = 10_000_000.0) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{field} must be a finite non-negative number or None")
    number = float(value)
    if not math.isfinite(number) or not 0 <= number <= maximum:
        raise ValueError(f"{field} must be a finite non-negative number or None")
    return round(number, 4)


def _publish(
    database: str | Path,
    *,
    producer: str,
    event_type: str,
    payload: Mapping[str, Any],
    epistemic_status: EpistemicStatus,
    idempotency_key: str | None = None,
) -> PublishReceipt:
    if not isinstance(database, (str, Path)) or not str(database).strip():
        raise ValueError("database must be a non-empty path")
    now = datetime.now(timezone.utc)
    event_id = uuid4()
    key = idempotency_key or f"{producer}:{event_id}"
    if not isinstance(key, str) or not key.strip() or len(key) > 200:
        raise ValueError("idempotency_key must be a non-empty string of at most 200 characters")
    event = IntegrationEvent(
        event_id=event_id,
        schema_version=1,
        event_type=event_type,
        producer_engine=producer,
        producer_version="0.1.0",
        occurred_at=now,
        recorded_at=now,
        epistemic_status=epistemic_status,
        sensitivity=Sensitivity.INTERNAL,
        allowed_consumers=_CONSUMERS[producer],
        payload=dict(payload),
        idempotency_key=key,
    )
    with SQLiteEventStore(database) as store:
        result = store.append(event)
    return PublishReceipt(result.event_id, result.sequence, result.inserted)


def publish_critical_thinking_review(
    database: str | Path,
    *,
    intent: str,
    review_level: str,
    reason: str,
    signals: tuple[str, ...] | list[str],
    user_requested: bool,
    context_injected: bool,
    idempotency_key: str | None = None,
) -> PublishReceipt:
    """Publish CTE routing metadata without retaining the original message."""
    if intent not in _CTE_INTENTS:
        raise ValueError("intent is not a recognized CTE intent")
    if review_level not in _CTE_LEVELS:
        raise ValueError("review_level is not a recognized CTE review level")
    if not isinstance(reason, str):
        raise ValueError("reason must be a string")
    reason_code = _CTE_REASON_CODES.get(reason, "unknown_reason")
    if not isinstance(signals, (tuple, list)) or len(signals) > 16:
        raise ValueError("signals must contain at most 16 entries")
    safe_signals: list[str] = []
    for signal in signals:
        if not isinstance(signal, str) or not signal.strip() or len(signal) > 64:
            raise ValueError("each signal must be a non-empty string of at most 64 characters")
        if signal not in safe_signals:
            safe_signals.append(signal)
    if not isinstance(user_requested, bool) or not isinstance(context_injected, bool):
        raise ValueError("user_requested and context_injected must be booleans")
    return _publish(
        database,
        producer="critical_thinking",
        event_type="critical_thinking.review_result",
        epistemic_status=EpistemicStatus.OBSERVED,
        idempotency_key=idempotency_key,
        payload={
            "intent": intent,
            "review_level": review_level,
            "reason_code": reason_code,
            "signals": safe_signals,
            "user_requested": user_requested,
            "context_injected": context_injected,
        },
    )


def publish_reliability_snapshot(
    database: str | Path,
    monitor_snapshot: Mapping[str, Any],
    profiler_snapshot: Mapping[str, Any],
    *,
    idempotency_key: str | None = None,
) -> PublishReceipt:
    """Publish only aggregate Reliability counters; omit model/tool labels and error text."""
    if not isinstance(monitor_snapshot, Mapping) or not isinstance(profiler_snapshot, Mapping):
        raise ValueError("monitor_snapshot and profiler_snapshot must be mappings")
    pressure = monitor_snapshot.get("context_pressure")
    if pressure not in _RELIABILITY_PRESSURES:
        raise ValueError("context_pressure is not recognized")

    anomalies = monitor_snapshot.get("response_anomalies", {})
    if not isinstance(anomalies, Mapping) or len(anomalies) > len(_RELIABILITY_SIGNALS):
        raise ValueError("response_anomalies must be a bounded mapping")
    safe_anomalies: dict[str, int] = {}
    for name, count in anomalies.items():
        if name not in _RELIABILITY_SIGNALS:
            raise ValueError("response_anomalies contains an unknown signal")
        safe_anomalies[name] = _bounded_count(count, f"response_anomalies.{name}")

    model_turns = profiler_snapshot.get("model_turns", {})
    if not isinstance(model_turns, Mapping) or len(model_turns) > 32:
        raise ValueError("model_turns must be a bounded mapping")
    total_model_turns = 0
    for count in model_turns.values():
        total_model_turns += _bounded_count(count, "model_turns count")
        if total_model_turns > 10_000_000_000:
            raise ValueError("total model_turns exceeds the supported range")

    payload = {
        "api_requests": _bounded_count(monitor_snapshot.get("api_requests"), "api_requests"),
        "api_successes": _bounded_count(monitor_snapshot.get("api_successes"), "api_successes"),
        "api_errors": _bounded_count(monitor_snapshot.get("api_errors"), "api_errors"),
        "tool_calls": _bounded_count(monitor_snapshot.get("tool_calls"), "tool_calls"),
        "tool_blocks": _bounded_count(monitor_snapshot.get("tool_blocks"), "tool_blocks"),
        "model_turns": total_model_turns,
        "explicit_task_labels": _bounded_count(
            profiler_snapshot.get("user_task_labels"), "user_task_labels"
        ),
        "response_anomalies": safe_anomalies,
        "context_pressure": pressure,
        "max_context_utilization": _bounded_number(
            monitor_snapshot.get("max_context_utilization"), "max_context_utilization", maximum=10.0
        ),
        "average_api_latency_ms": _bounded_number(
            monitor_snapshot.get("average_api_latency_ms"), "average_api_latency_ms"
        ),
        "average_tool_latency_ms": _bounded_number(
            monitor_snapshot.get("average_tool_latency_ms"), "average_tool_latency_ms"
        ),
        "max_input_tokens": _bounded_count(monitor_snapshot.get("max_input_tokens"), "max_input_tokens"),
        "context_warning_events": _bounded_count(
            monitor_snapshot.get("context_warning_events"), "context_warning_events"
        ),
        "context_critical_events": _bounded_count(
            monitor_snapshot.get("context_critical_events"), "context_critical_events"
        ),
        "content_capture": False,
        "retention": "aggregate counters only; snapshot received from Reliability Engine",
    }
    return _publish(
        database,
        producer="reliability",
        event_type="reliability.runtime_snapshot",
        epistemic_status=EpistemicStatus.OBSERVED,
        idempotency_key=idempotency_key,
        payload=payload,
    )


def publish_emotion_reflection(
    database: str | Path,
    *,
    status: str,
    step_count: int,
    steps_remaining: int,
    assessments: tuple[str, ...] | list[str],
    hypothesis_count: int,
    evidence_ref_count: int,
    next_question_available: bool,
    invocation_count: int,
    idempotency_key: str | None = None,
) -> PublishReceipt:
    """Publish reflection metadata only; narratives, hypotheses, and evidence text stay local."""
    if status not in _EMOTION_STATUSES:
        raise ValueError("status is not a recognized Emotion reflection status")
    if not isinstance(assessments, (tuple, list)) or len(assessments) > 4:
        raise ValueError("assessments must contain at most four entries")
    safe_assessments: list[str] = []
    for assessment in assessments:
        if assessment not in _EMOTION_ASSESSMENTS:
            raise ValueError("assessments contains an unknown assessment")
        safe_assessments.append(assessment)
    if not isinstance(next_question_available, bool):
        raise ValueError("next_question_available must be a boolean")
    if isinstance(step_count, bool) or not isinstance(step_count, int) or not 0 <= step_count <= 4:
        raise ValueError("step_count must be an integer from 0 to 4")
    if isinstance(steps_remaining, bool) or not isinstance(steps_remaining, int) or not 0 <= steps_remaining <= 4:
        raise ValueError("steps_remaining must be an integer from 0 to 4")
    if steps_remaining > 4 - step_count:
        raise ValueError("steps_remaining is inconsistent with step_count")
    if len(safe_assessments) != step_count:
        raise ValueError("assessments count must match step_count")
    payload = {
        "status": status,
        "step_count": step_count,
        "steps_remaining": steps_remaining,
        "assessments": safe_assessments,
        "hypothesis_count": _bounded_count(hypothesis_count, "hypothesis_count", maximum=32),
        "evidence_ref_count": _bounded_count(evidence_ref_count, "evidence_ref_count", maximum=96),
        "next_question_available": next_question_available,
        "invocation_count": _bounded_count(invocation_count, "invocation_count", maximum=4),
        "narrative_storage": False,
        "persistent_emotional_memory": False,
    }
    return _publish(
        database,
        producer="emotion",
        event_type="emotion.reflection_summary",
        epistemic_status=EpistemicStatus.HYPOTHESIS,
        idempotency_key=idempotency_key,
        payload=payload,
    )
