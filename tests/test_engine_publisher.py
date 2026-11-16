from __future__ import annotations

import json

import pytest

from kie.events import EpistemicStatus, event_to_dict
from kie.event_store import SQLiteEventStore
from kie.publisher import (
    publish_critical_thinking_review,
    publish_emotion_reflection,
    publish_reliability_snapshot,
)


def test_critical_thinking_publication_records_only_bounded_review_metadata(tmp_path):
    database = tmp_path / "kie.sqlite3"
    receipt = publish_critical_thinking_review(
        database,
        intent="project_proposal",
        review_level="critical",
        reason="The user explicitly requested a critical review.",
        signals=("technical_project_signal", "security"),
        user_requested=True,
        context_injected=True,
    )

    with SQLiteEventStore(database) as store:
        record = store.get_event(receipt.event_id)

    assert record is not None
    event = record.event
    assert event.producer_engine == "critical_thinking"
    assert event.event_type == "critical_thinking.review_result"
    assert event.epistemic_status is EpistemicStatus.OBSERVED
    assert event.payload["review_level"] == "critical"
    assert event.payload["reason_code"] == "explicit_request"
    encoded = json.dumps(event_to_dict(event))
    assert "original user message" not in encoded.lower()


def test_critical_thinking_rejects_unknown_review_levels_before_opening_database(tmp_path):
    database = tmp_path / "must-not-exist.sqlite3"
    with pytest.raises(ValueError):
        publish_critical_thinking_review(
            database,
            intent="project_proposal",
            review_level="certain",
            reason="The user explicitly requested a critical review.",
            signals=(),
            user_requested=False,
            context_injected=True,
        )
    assert not database.exists()


def test_reliability_publication_minimizes_to_aggregate_runtime_counters(tmp_path):
    database = tmp_path / "kie.sqlite3"
    monitor = {
        "api_requests": 12,
        "api_successes": 10,
        "api_errors": 2,
        "tool_calls": 8,
        "tool_blocks": 1,
        "context_pressure": "warning",
        "max_context_utilization": 0.82,
        "average_api_latency_ms": 321.4,
        "average_tool_latency_ms": 54.2,
        "max_input_tokens": 53200,
        "context_warning_events": 2,
        "context_critical_events": 0,
        "response_anomalies": {"empty_response": 1, "repeated_lines": 2},
        "tool_calls_by_name": {"private_tool_name": 8},
        "api_error_types": {"PrivateException": 2},
    }
    profiler = {
        "model_turns": {"private-model-label": 7, "other": 2},
        "user_task_labels": 3,
        "task_outcomes_by_model": {"private-model-label": {"coding": {"success": 3}}},
    }

    receipt = publish_reliability_snapshot(database, monitor, profiler)
    with SQLiteEventStore(database) as store:
        event = store.get_event(receipt.event_id).event

    payload = event_to_dict(event)["payload"]
    assert event.producer_engine == "reliability"
    assert event.event_type == "reliability.runtime_snapshot"
    assert payload["model_turns"] == 9
    assert payload["explicit_task_labels"] == 3
    assert payload["response_anomalies"] == {"empty_response": 1, "repeated_lines": 2}
    encoded = json.dumps(payload)
    assert "private_tool_name" not in encoded
    assert "PrivateException" not in encoded
    assert "private-model-label" not in encoded


def test_reliability_rejects_unknown_anomaly_codes(tmp_path):
    database = tmp_path / "must-not-exist.sqlite3"
    monitor = {
        "api_requests": 0, "api_successes": 0, "api_errors": 0,
        "tool_calls": 0, "tool_blocks": 0, "context_pressure": "waiting",
        "max_context_utilization": 0, "average_api_latency_ms": None,
        "average_tool_latency_ms": None, "max_input_tokens": 0,
        "context_warning_events": 0, "context_critical_events": 0,
        "response_anomalies": {"prompt_leak": 1},
    }
    profiler = {"model_turns": {}, "user_task_labels": 0}
    with pytest.raises(ValueError):
        publish_reliability_snapshot(database, monitor, profiler)
    assert not database.exists()


def test_emotion_publication_contains_reflection_metadata_not_narrative(tmp_path):
    database = tmp_path / "kie.sqlite3"
    receipt = publish_emotion_reflection(
        database,
        status="awaiting_evidence",
        step_count=2,
        steps_remaining=2,
        assessments=("supported", "undetermined"),
        hypothesis_count=1,
        evidence_ref_count=4,
        next_question_available=True,
        invocation_count=2,
    )

    with SQLiteEventStore(database) as store:
        event = store.get_event(receipt.event_id).event

    payload = event_to_dict(event)["payload"]
    assert event.producer_engine == "emotion"
    assert event.event_type == "emotion.reflection_summary"
    assert event.epistemic_status is EpistemicStatus.HYPOTHESIS
    assert payload["assessments"] == ["supported", "undetermined"]
    assert payload["narrative_storage"] is False
    assert payload["persistent_emotional_memory"] is False
    assert set(payload) == {
        "status", "step_count", "steps_remaining", "assessments",
        "hypothesis_count", "evidence_ref_count", "next_question_available",
        "invocation_count", "narrative_storage", "persistent_emotional_memory",
    }


def test_emotion_rejects_invalid_assessment_before_opening_database(tmp_path):
    database = tmp_path / "must-not-exist.sqlite3"
    with pytest.raises(ValueError):
        publish_emotion_reflection(
            database,
            status="completed",
            step_count=1,
            steps_remaining=0,
            assessments=("verified",),
            hypothesis_count=0,
            evidence_ref_count=1,
            next_question_available=False,
            invocation_count=1,
        )
    assert not database.exists()
