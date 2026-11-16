"""Native Hermes tool registration for the KIE Evidence snapshot workflow."""
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

_PLUGIN_VERSION = "0.7.0"
_WORKFLOW_ID = "hermes_evidence_lookup"
_TOOL_NAME = "kie_evidence_lookup"


def _json(data: Mapping[str, Any]) -> str:
    return json.dumps(data, ensure_ascii=False, allow_nan=False, separators=(",", ":"))


def _read_settings(ctx) -> tuple[EvidenceStdioConfig, Path, int]:
    def required(key: str) -> str:
        value = ctx.get_config(key, default=None)
        if not isinstance(value, str) or not value.strip():
            raise LookupError("required integration setting is missing")
        return value

    python_executable = required("evidence_python")
    evidence_source_root = required("evidence_source_root")
    evidence_database = required("evidence_database")
    kie_database_value = required("kie_database")
    kie_database = Path(kie_database_value).expanduser()
    if not kie_database.is_absolute():
        raise ValueError("kie_database must be an absolute path")

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


def register(ctx) -> None:
    """Register one async read-only tool with Hermes Agent v0.23.1+."""
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

    ctx.register_tool(
        name=_TOOL_NAME,
        toolset="kor_kie",
        schema=schema,
        handler=handle_lookup,
        is_async=True,
    )
