"""Native Hermes tools for KIE's bounded engine event ledger."""
from __future__ import annotations

import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping
from uuid import UUID, uuid4

from .adapters.evidence import EvidenceReadOnlyAdapter
from .events import HERMES_HOST_ID, EpistemicStatus, IntegrationEvent, Sensitivity, event_to_dict
from .event_store import SQLiteEventStore
from .executor import AdapterRegistry, ExecutionPolicy, WorkflowExecutor
from .transports.evidence_stdio import EvidenceStdioConfig, EvidenceStdioTransport
from .workflow_store import SQLiteWorkflowStore, StepStatus
from .workflows import WorkflowDefinition, WorkflowStep, plan_workflow

_PLUGIN_VERSION = "0.8.0"
_WORKFLOW_ID = "hermes_evidence_lookup"
_TOOL_NAME = "kie_evidence_lookup"
_STATUS_TOOL_NAME = "kie_engine_status"
_ENGINE_EVENT_TYPES = {
    "critical_thinking": "critical_thinking.review_result",
    "reliability": "reliability.runtime_snapshot",
    "emotion": "emotion.reflection_summary",
}


def _json(data: Mapping[str, Any]) -> str:
    return json.dumps(data, ensure_ascii=False, allow_nan=False, separators=(",", ":"))


def _read_kie_database(ctx) -> Path:
    value = ctx.get_config("kie_database", default=None)
    if not isinstance(value, str) or not value.strip():
        raise LookupError("KIE database setting is missing")
    path = Path(value).expanduser()
    if not path.is_absolute():
        raise ValueError("kie_database must be an absolute path")
    return path


def _read_settings(ctx) -> tuple[EvidenceStdioConfig, Path, int]:
    def required(key: str) -> str:
        value = ctx.get_config(key, default=None)
        if not isinstance(value, str) or not value.strip():
            raise LookupError("required integration setting is missing")
        return value

    python_executable = required("evidence_python")
    evidence_source_root = required("evidence_source_root")
    evidence_database = required("evidence_database")
    kie_database = _read_kie_database(ctx)

    timeout_value = ctx.get_config("timeout_seconds", default=10)
    if (
        isinstance(timeout_value, bool)
        or not isinstance(timeout_value, (int, float))
        or not math.isfinite(float(timeout_value))
        or not 1 <= float(timeout_value) <= 60
    ):
        raise ValueError("timeout_seconds must be from 1 to 60 seconds")

    transport_config = EvidenceStdioConfig(
        python_executable=Path(python_executable),
        evidence_source_root=Path(evidence_source_root),
        database_path=Path(evidence_database),
        timeout_seconds=float(timeout_value),
    )
    return transport_config, kie_database, int(math.ceil(float(timeout_value)))


async def _lookup(
    task_id: str,
    transport_config: EvidenceStdioConfig,
    kie_database: Path,
    timeout_seconds: int,
) -> str:
    now = datetime.now(timezone.utc)
    workflow = WorkflowDefinition(
        _WORKFLOW_ID,
        "1.0",
        (WorkflowStep(
            "read_evidence",
            "evidence",
            timeout_seconds=min(300, timeout_seconds + 5),
        ),),
    )
    plan = plan_workflow(workflow, available_engines={"evidence"})
    adapter = EvidenceReadOnlyAdapter(EvidenceStdioTransport(transport_config))

    with SQLiteEventStore(kie_database) as event_store, SQLiteWorkflowStore(kie_database) as workflow_store:
        run = workflow_store.create_run(workflow, plan, now=now)
        request_event = IntegrationEvent(
            event_id=uuid4(),
            schema_version=1,
            event_type="evidence.lookup_request",
            producer_engine=HERMES_HOST_ID,
            producer_version=_PLUGIN_VERSION,
            occurred_at=now,
            recorded_at=now,
            epistemic_status=EpistemicStatus.OBSERVED,
            sensitivity=Sensitivity.INTERNAL,
            allowed_consumers=("evidence",),
            payload={"evidence_task_id": task_id},
            task_id=str(run.run_id),
            correlation_id=str(run.run_id),
            idempotency_key=f"kie:{run.run_id}:evidence-lookup-request",
        )
        event_store.append(request_event)

        executor = WorkflowExecutor(
            workflow_store=workflow_store,
            event_store=event_store,
            adapters=AdapterRegistry({"evidence": adapter}),
            policy=ExecutionPolicy(
                allowed_workflows=frozenset({_WORKFLOW_ID}),
                allowed_engines=frozenset({"evidence"}),
                allowed_input_sensitivities=frozenset({Sensitivity.INTERNAL}),
            ),
        )
        step = await executor.execute_step(
            run.run_id,
            "read_evidence",
            input_event_ids=(request_event.event_id,),
            now=now,
        )
        if step.status is not StepStatus.COMPLETED or step.result_event_id is None:
            return _json({
                "ok": False,
                "workflow_run_id": str(run.run_id),
                "step_status": step.status.value,
                "reason": step.reason or "evidence_lookup_not_completed",
            })

        record = event_store.get_event(step.result_event_id)
        if record is None:
            return _json({
                "ok": False,
                "workflow_run_id": str(run.run_id),
                "step_status": "failed",
                "reason": "evidence_result_event_missing",
            })
        envelope = event_to_dict(record.event)
        return _json({
            "ok": True,
            "workflow_run_id": str(run.run_id),
            "step_status": step.status.value,
            "epistemic_status": record.event.epistemic_status.value,
            "summary": envelope["payload"],
            "evidence_refs": envelope["evidence_refs"],
        })


def _safe_engine_summary(engine_id: str, event) -> dict[str, Any]:
    payload = event.payload
    if engine_id == "critical_thinking":
        return {
            "intent": payload["intent"],
            "review_level": payload["review_level"],
            "reason_code": payload["reason_code"],
            "context_injected": payload["context_injected"],
        }
    if engine_id == "reliability":
        return {
            "api_requests": payload["api_requests"],
            "api_errors": payload["api_errors"],
            "tool_calls": payload["tool_calls"],
            "tool_blocks": payload["tool_blocks"],
            "model_turns": payload["model_turns"],
            "context_pressure": payload["context_pressure"],
            "response_anomalies": dict(payload["response_anomalies"]),
        }
    return {
        "status": payload["status"],
        "step_count": payload["step_count"],
        "steps_remaining": payload["steps_remaining"],
        "assessments": list(payload["assessments"]),
        "hypothesis_count": payload["hypothesis_count"],
        "next_question_available": payload["next_question_available"],
        "narrative_storage": False,
        "persistent_emotional_memory": False,
    }


def _engine_status(database: Path, engine_id: str | None) -> str:
    selected = (engine_id,) if engine_id is not None else tuple(_ENGINE_EVENT_TYPES)
    with SQLiteEventStore(database) as store:
        # Inspect only the most recent global page; never scan an unbounded ledger.
        after_sequence = max(0, store.count() - 1000)
        response: dict[str, Any] = {"ok": True, "engines": {}}
        for current in selected:
            records = store.read_events(
                after_sequence=after_sequence,
                limit=1000,
                producer_engine=current,
                event_type=_ENGINE_EVENT_TYPES[current],
            )
            if not records:
                response["engines"][current] = {"status": "no_published_event"}
                continue
            latest = records[-1]
            try:
                summary = _safe_engine_summary(current, latest.event)
            except (KeyError, TypeError):
                response["engines"][current] = {"status": "invalid_event_contract"}
                continue
            response["engines"][current] = {
                "status": "available",
                "sequence": latest.sequence,
                "recorded_at": latest.event.recorded_at.isoformat(),
                "epistemic_status": latest.event.epistemic_status.value,
                "summary": summary,
            }
    return _json(response)


def register(ctx) -> None:
    """Register opt-in evidence lookup and read-only engine status tools."""
    schema = {
        "name": _TOOL_NAME,
        "description": (
            "Read one existing Evidence Engine task by canonical UUID. "
            "Returns a minimized, KIE-recorded status snapshot. This tool is read-only; "
            "it cannot create tasks, add evidence, run commands, or change verification state."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "evidence_task_id": {
                    "type": "string",
                    "description": "Canonical UUID of an existing Evidence Engine task.",
                }
            },
            "required": ["evidence_task_id"],
            "additionalProperties": False,
        },
    }

    async def handle_lookup(params, **kwargs):
        del kwargs
        if not isinstance(params, Mapping):
            return _json({"ok": False, "reason": "invalid_tool_arguments"})
        raw_task_id = params.get("evidence_task_id")
        if not isinstance(raw_task_id, str):
            return _json({"ok": False, "reason": "invalid_evidence_task_id"})
        try:
            task_id = str(UUID(raw_task_id))
        except ValueError:
            return _json({"ok": False, "reason": "invalid_evidence_task_id"})
        if task_id != raw_task_id:
            return _json({"ok": False, "reason": "invalid_evidence_task_id"})

        try:
            transport_config, kie_database, timeout_seconds = _read_settings(ctx)
        except LookupError:
            return _json({"ok": False, "reason": "integration_not_configured"})
        except (OSError, TypeError, ValueError):
            return _json({"ok": False, "reason": "integration_configuration_invalid"})

        try:
            return await _lookup(task_id, transport_config, kie_database, timeout_seconds)
        except Exception:
            # Keep local paths, SQL details, and subprocess diagnostics out of model-visible output.
            return _json({"ok": False, "reason": "kie_execution_failed"})

    status_schema = {
        "name": _STATUS_TOOL_NAME,
        "description": (
            "Read the latest privacy-minimized event published by the Critical Thinking, "
            "Reliability, and Emotion engines. This does not trigger engine work or expose "
            "conversation text, tool arguments, reflection narratives, or emotional memories."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "engine_id": {
                    "type": "string",
                    "enum": ["critical_thinking", "reliability", "emotion"],
                }
            },
            "required": [],
            "additionalProperties": False,
        },
    }

    def handle_status(params, **kwargs):
        del kwargs
        if not isinstance(params, Mapping):
            return _json({"ok": False, "reason": "invalid_tool_arguments"})
        engine_id = params.get("engine_id")
        if engine_id is not None and engine_id not in _ENGINE_EVENT_TYPES:
            return _json({"ok": False, "reason": "invalid_engine_id"})
        try:
            database = _read_kie_database(ctx)
        except LookupError:
            return _json({"ok": False, "reason": "integration_not_configured"})
        except (OSError, TypeError, ValueError):
            return _json({"ok": False, "reason": "integration_configuration_invalid"})
        if not database.exists():
            return _json({
                "ok": True,
                "engines": {
                    key: {"status": "no_published_event"}
                    for key in ((engine_id,) if engine_id else _ENGINE_EVENT_TYPES)
                },
            })
        try:
            return _engine_status(database, engine_id)
        except Exception:
            return _json({"ok": False, "reason": "kie_status_unavailable"})

    ctx.register_tool(
        name=_TOOL_NAME,
        toolset="kor_kie",
        schema=schema,
        handler=handle_lookup,
        is_async=True,
    )
    ctx.register_tool(
        name=_STATUS_TOOL_NAME,
        toolset="kor_kie",
        schema=status_schema,
        handler=handle_status,
    )
