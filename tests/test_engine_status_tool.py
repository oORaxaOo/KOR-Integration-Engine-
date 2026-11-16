from __future__ import annotations

import json

from kie.hermes_plugin import register
from kie.publisher import (
    publish_critical_thinking_review,
    publish_emotion_reflection,
    publish_reliability_snapshot,
)


class FakeHermesContext:
    def __init__(self, settings):
        self.settings = settings
        self.tools = {}

    def get_config(self, key, default=None):
        current = self.settings
        for part in key.split("."):
            if not isinstance(current, dict) or part not in current:
                return default
            current = current[part]
        return current

    def register_tool(self, **kwargs):
        self.tools[kwargs["name"]] = kwargs


def _publish_all(database):
    publish_critical_thinking_review(
        database, intent="decision", review_level="review",
        reason="Technical proposal or decision detected.",
        signals=("technical_project_signal",),
        user_requested=False, context_injected=True,
    )
    monitor = {
        "api_requests": 1, "api_successes": 1, "api_errors": 0,
        "tool_calls": 2, "tool_blocks": 0, "context_pressure": "normal",
        "max_context_utilization": 0.3, "average_api_latency_ms": 20.0,
        "average_tool_latency_ms": 4.0, "max_input_tokens": 800,
        "context_warning_events": 0, "context_critical_events": 0,
        "response_anomalies": {},
    }
    profiler = {"model_turns": {"model-a": 1}, "user_task_labels": 0}
    publish_reliability_snapshot(database, monitor, profiler)
    publish_emotion_reflection(
        database, status="completed", step_count=1, steps_remaining=0,
        assessments=("undetermined",), hypothesis_count=1, evidence_ref_count=2,
        next_question_available=False, invocation_count=1,
    )


def test_registers_status_tool_without_changing_evidence_lookup_tool():
    ctx = FakeHermesContext({})
    register(ctx)
    assert set(ctx.tools) == {"kie_evidence_lookup", "kie_engine_status"}
    assert ctx.tools["kie_evidence_lookup"]["is_async"] is True
    assert ctx.tools["kie_engine_status"]["is_async"] if "is_async" in ctx.tools["kie_engine_status"] else True


def test_engine_status_returns_latest_minimized_event_for_each_publisher(tmp_path):
    database = tmp_path / "kie.sqlite3"
    _publish_all(database)
    ctx = FakeHermesContext({"kie_database": str(database)})
    register(ctx)
    handler = ctx.tools["kie_engine_status"]["handler"]

    result = json.loads(handler({}))
    assert result["ok"] is True
    assert {key: value["status"] for key, value in result["engines"].items()} == {
        "critical_thinking": "available",
        "reliability": "available",
        "emotion": "available",
    }
    assert result["engines"]["critical_thinking"]["summary"]["review_level"] == "review"
    assert result["engines"]["reliability"]["summary"]["tool_calls"] == 2
    assert result["engines"]["emotion"]["summary"]["narrative_storage"] is False


def test_engine_status_filters_one_engine_and_does_not_create_missing_database(tmp_path):
    database = tmp_path / "missing.sqlite3"
    ctx = FakeHermesContext({"kie_database": str(database)})
    register(ctx)
    handler = ctx.tools["kie_engine_status"]["handler"]

    result = json.loads(handler({"engine_id": "emotion"}))
    assert result == {"ok": True, "engines": {"emotion": {"status": "no_published_event"}}}
    assert not database.exists()


def test_engine_status_rejects_unknown_engine_ids():
    ctx = FakeHermesContext({"kie_database": "/tmp/kie-unused.sqlite3"})
    register(ctx)
    result = json.loads(ctx.tools["kie_engine_status"]["handler"]({"engine_id": "evidence"}))
    assert result == {"ok": False, "reason": "invalid_engine_id"}
