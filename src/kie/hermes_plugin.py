"""Native Hermes tool registration for the KIE Evidence snapshot workflow."""
from __future__ import annotations

import json
import math
from datetime import datetime, timezone
from pathlib import Path
import logging
import os
import sys
import threading
from typing import Any, Mapping
from uuid import UUID, uuid4

from .adapters.evidence import EvidenceReadOnlyAdapter
from .transports.engine_event_socket import EngineEventSocketServer
from .events import HERMES_HOST_ID, EpistemicStatus, IntegrationEvent, Sensitivity, event_to_dict
from .event_store import SQLiteEventStore
from .executor import AdapterRegistry, ExecutionPolicy, WorkflowExecutor
from .transports.evidence_stdio import EvidenceStdioConfig, EvidenceStdioTransport
from .workflow_store import SQLiteWorkflowStore, StepStatus
from .workflows import WorkflowDefinition, WorkflowStep, plan_workflow
from .health import HealthSupervisor

_PLUGIN_VERSION = "0.8.0"
_WORKFLOW_ID = "hermes_evidence_lookup"
_ENGINE_EVENT_TOOL = "kie_engine_events"
_ENGINE_EVENT_ENGINES = frozenset({"critical_thinking", "reliability", "emotion", "evidence"})
_ENGINE_EVENT_SERVERS: dict[tuple[str, str], EngineEventSocketServer] = {}
_ENGINE_EVENT_SERVERS_LOCK = threading.RLock()
_LOGGER = logging.getLogger("kie.hermes_plugin")


def _start_engine_event_ingress(ctx) -> EngineEventSocketServer | None:
    """Start the optional same-user Unix-socket receiver when configured."""
    socket_value = ctx.get_config("engine_event_socket", default=None)
    database_value = ctx.get_config("kie_database", default=None)
    if not isinstance(socket_value, str) or not socket_value.strip():
        return None
    if not isinstance(database_value, str) or not database_value.strip():
        _LOGGER.warning("KIE engine event ingress is disabled: kie_database is not configured")
        return None
    socket_path = Path(socket_value).expanduser()
    database_path = Path(database_value).expanduser()
    if not socket_path.is_absolute() or not database_path.is_absolute():
        _LOGGER.warning("KIE engine event ingress is disabled: paths must be absolute")
        return None
    key = (str(socket_path), str(database_path))
    with _ENGINE_EVENT_SERVERS_LOCK:
        server = _ENGINE_EVENT_SERVERS.get(key)
        if server is None:
            server = EngineEventSocketServer(socket_path, database_path)
            _ENGINE_EVENT_SERVERS[key] = server
        try:
            server.start()
        except (OSError, TypeError, ValueError):
            _LOGGER.exception("KIE engine event ingress could not start")
            return None
        return server
_TOOL_NAME = "kie_evidence_lookup"


def _json(data: Mapping[str, Any]) -> str:
    return json.dumps(data, ensure_ascii=False, allow_nan=False, separators=(",", ":"))


def _hermes_home() -> Path:
    return Path(os.environ.get("HERMES_HOME", str(Path.home() / ".hermes"))).expanduser()


def _default_evidence_database() -> Path:
    return Path(os.environ.get(
        "EVIDENCE_ENGINE_DB",
        str(Path.home() / ".hermes" / "evidence-engine" / "evidence.sqlite3"),
    )).expanduser()


def _health_supervisor(ctx, event_server) -> HealthSupervisor:
    home = _hermes_home()
    def configured_path(key: str, default: Path) -> Path:
        value = ctx.get_config(key, default=str(default))
        if not isinstance(value, str) or not value.strip():
            return default
        return Path(value).expanduser()

    return HealthSupervisor(
        ctx,
        kie_database=configured_path("kie_database", home / "kie" / "kie.sqlite3"),
        event_server=event_server,
        evidence_python=configured_path("evidence_python", Path(sys.executable)),
        evidence_source_root=configured_path(
            "evidence_source_root", home / "plugins" / "evidence-engine"
        ),
        evidence_database=configured_path("evidence_database", _default_evidence_database()),
    )


def _read_settings(ctx) -> tuple[EvidenceStdioConfig, Path, int]:
    home = _hermes_home()

    def configured(key: str, default: str) -> str:
        value = ctx.get_config(key, default=default)
        if not isinstance(value, str) or not value.strip():
            raise LookupError("required integration setting is missing")
        return value

    python_executable = configured("evidence_python", sys.executable)
    evidence_source_root = configured(
        "evidence_source_root", str(home / "plugins" / "evidence-engine")
    )
    evidence_database = configured(
        "evidence_database", str(_default_evidence_database())
    )
    kie_database_value = configured(
        "kie_database", str(home / "kie" / "kie.sqlite3")
    )
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




def _recent_engine_events(engine_id: str, limit: int, database_path: Path) -> str:
    """Return a bounded read-only view of sanitized engine metadata."""
    with SQLiteEventStore(database_path) as store:
        records = store.read_recent_events(limit=limit, producer_engine=engine_id)
    return _json({
        "ok": True,
        "engine_id": engine_id,
        "event_count": len(records),
        "events": [
            {
                "sequence": record.sequence,
                "event_id": str(record.event.event_id),
                "event_type": record.event.event_type,
                "occurred_at": record.event.occurred_at.isoformat(),
                "sensitivity": record.event.sensitivity.value,
                "epistemic_status": record.event.epistemic_status.value,
                "payload": dict(record.event.payload),
            }
            for record in records
        ],
    })


def _register_engine_events_tool(ctx) -> None:
    """Expose only allowlisted, already-sanitized engine events to Hermes."""
    schema = {
        "name": _ENGINE_EVENT_TOOL,
        "description": (
            "Read a bounded, read-only view of recent metadata events from one KOR engine. "
            "Use only when the user asks for engine status or recent engine activity. "
            "Returns sanitized routing/runtime/reflection metadata, never conversation content."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "engine_id": {
                    "type": "string",
                    "enum": sorted(_ENGINE_EVENT_ENGINES),
                },
                "limit": {
                    "type": "integer",
                    "minimum": 1,
                    "maximum": 20,
                    "default": 5,
                },
            },
            "required": ["engine_id"],
            "additionalProperties": False,
        },
    }

    def handle_events(params, **kwargs):
        del kwargs
        if not isinstance(params, Mapping) or set(params) - {"engine_id", "limit"}:
            return _json({"ok": False, "reason": "invalid_tool_arguments"})
        engine_id = params.get("engine_id")
        limit = params.get("limit", 5)
        if not isinstance(engine_id, str) or engine_id not in _ENGINE_EVENT_ENGINES:
            return _json({"ok": False, "reason": "invalid_engine_id"})
        if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= 20:
            return _json({"ok": False, "reason": "invalid_limit"})
        try:
            database_value = ctx.get_config("kie_database", default=None)
            if not isinstance(database_value, str) or not database_value.strip():
                return _json({"ok": False, "reason": "integration_not_configured"})
            database_path = Path(database_value).expanduser()
            if not database_path.is_absolute():
                return _json({"ok": False, "reason": "integration_configuration_invalid"})
            if not database_path.is_file():
                return _json({"ok": False, "reason": "kie_database_unavailable"})
            return _recent_engine_events(engine_id, limit, database_path)
        except Exception:
            return _json({"ok": False, "reason": "kie_event_read_failed"})

    ctx.register_tool(
        name=_ENGINE_EVENT_TOOL,
        toolset="kor_kie",
        schema=schema,
        handler=handle_events,
    )


def _register_system_health_tool(ctx, supervisor: HealthSupervisor) -> None:
    schema = {
        "name": "kie_system_health",
        "description": (
            "Read a deterministic, content-free health report for KIE and all four "
            "independent KOR engines. Reports plugin load state, the event receiver, "
            "Evidence bridge configuration, and recent allowlisted operational failures."
        ),
        "parameters": {
            "type": "object",
            "properties": {},
            "required": [],
            "additionalProperties": False,
        },
    }

    def handle_health(params, **kwargs):
        del kwargs
        if not isinstance(params, Mapping) or params:
            return _json({"ok": False, "reason": "invalid_tool_arguments"})
        try:
            return _json(supervisor.get_report())
        except Exception:
            return _json({"ok": False, "reason": "health_report_unavailable"})

    ctx.register_tool(
        name="kie_system_health",
        toolset="kor_kie",
        schema=schema,
        handler=handle_health,
    )


def register(ctx) -> None:
    """Register read-only KIE tools, event ingress, and deterministic health supervision."""
    event_server = _start_engine_event_ingress(ctx)
    if event_server is not None and callable(getattr(ctx, "on_unload", None)):
        ctx.on_unload(event_server.shutdown)
    _register_engine_events_tool(ctx)
    supervisor = _health_supervisor(ctx, event_server)
    _register_system_health_tool(ctx, supervisor)
    ctx.register_hook("pre_llm_call", supervisor.on_pre_llm_call)
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
