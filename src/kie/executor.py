"""Controlled asynchronous executor for explicitly registered KOR engine adapters.

The executor never imports arbitrary modules, runs shell commands, or scans
plugins. The host must register each adapter and supply an explicit policy.
"""
from __future__ import annotations

import asyncio
from dataclasses import dataclass
from enum import Enum
from typing import Mapping, Protocol
from uuid import UUID

from .events import ENGINE_IDS, IntegrationEvent, Sensitivity
from .event_store import EventRecord, SQLiteEventStore
from .workflow_store import (
    InvalidWorkflowTransition,
    RunStatus,
    SQLiteWorkflowStore,
    StepStatus,
    WorkflowStepRecord,
)
from .workflows import WorkflowPlan


class ExecutorError(RuntimeError):
    """Base exception for controlled execution failures."""


class AuthorizationDenied(ExecutorError):
    """The configured execution policy denied the requested work or input."""


class AdapterUnavailable(ExecutorError):
    """No adapter is registered for the requested engine."""


class InvalidEngineResult(ExecutorError):
    """An adapter returned an invalid or misattributed result."""


class ExecutionAlreadyRunning(ExecutorError):
    """A step is running and has no safely recoverable recorded result."""


class ExecutionOutcome(str, Enum):
    COMPLETED = "completed"
    NO_DATA = "no_data"
    UNAVAILABLE = "unavailable"
    FAILED = "failed"


@dataclass(frozen=True, slots=True)
class ExecutionRequest:
    run_id: UUID
    workflow_id: str
    workflow_version: str
    step: WorkflowStepRecord
    attempt: int
    idempotency_key: str
    input_events: tuple[EventRecord, ...]


@dataclass(frozen=True, slots=True)
class ExecutionResult:
    outcome: ExecutionOutcome
    event: IntegrationEvent | None = None
    reason: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.outcome, ExecutionOutcome):
            raise ValueError("outcome must be an ExecutionOutcome")
        if self.outcome is ExecutionOutcome.COMPLETED:
            if not isinstance(self.event, IntegrationEvent):
                raise ValueError("a completed result requires an event")
            if self.reason is not None:
                raise ValueError("a completed result must not include a failure reason")
        else:
            if self.event is not None:
                raise ValueError("a non-success result must not include an event")
            if not isinstance(self.reason, str) or not self.reason.strip():
                raise ValueError("a non-success result requires a non-empty reason")


class EngineAdapter(Protocol):
    """An explicitly registered async adapter for one fixed engine identity."""

    engine_id: str

    async def execute(self, request: ExecutionRequest) -> ExecutionResult:
        """Execute one declared step and return a versioned result event."""


class ExecutionPolicyProtocol(Protocol):
    def authorize(self, request: ExecutionRequest) -> bool:
        """Return True only when this exact workflow/engine/input set is allowed."""


@dataclass(frozen=True, slots=True)
class ExecutionPolicy:
    """Small default-deny allowlist policy.

    Workflow and engine IDs are both required. Input event sensitivities must
    also be explicitly allowed. This is an application policy, not host
    authentication; the calling process must protect the policy configuration.
    """

    allowed_workflows: frozenset[str] = frozenset()
    allowed_engines: frozenset[str] = frozenset()
    allowed_input_sensitivities: frozenset[Sensitivity] = frozenset()

    def __post_init__(self) -> None:
        workflows = frozenset(self.allowed_workflows)
        engines = frozenset(self.allowed_engines)
        sensitivities = frozenset(self.allowed_input_sensitivities)
        if any(not isinstance(item, str) or not item.strip() for item in workflows):
            raise ValueError("allowed_workflows must contain non-empty workflow IDs")
        if not engines <= ENGINE_IDS:
            raise ValueError("allowed_engines contains an unknown engine ID")
        if any(not isinstance(item, Sensitivity) for item in sensitivities):
            raise ValueError("allowed_input_sensitivities must contain Sensitivity values")
        object.__setattr__(self, "allowed_workflows", workflows)
        object.__setattr__(self, "allowed_engines", engines)
        object.__setattr__(self, "allowed_input_sensitivities", sensitivities)

    def authorize(self, request: ExecutionRequest) -> bool:
        if request.workflow_id not in self.allowed_workflows:
            return False
        if request.step.engine_id not in self.allowed_engines:
            return False
        return all(
            record.event.sensitivity in self.allowed_input_sensitivities
            for record in request.input_events
        )


class AdapterRegistry:
    """Immutable-by-interface registry; no dynamic imports or discovery."""

    def __init__(self, adapters: Mapping[str, EngineAdapter] | None = None) -> None:
        registered: dict[str, EngineAdapter] = {}
        for engine_id, adapter in (adapters or {}).items():
            if engine_id not in ENGINE_IDS:
                raise ValueError(f"unknown adapter engine ID: {engine_id}")
            if getattr(adapter, "engine_id", None) != engine_id:
                raise ValueError(f"adapter identity does not match registry key {engine_id}")
            execute = getattr(adapter, "execute", None)
            if not callable(execute):
                raise ValueError(f"adapter for {engine_id} must provide execute(request)")
            registered[engine_id] = adapter
        self._adapters = registered

    def get(self, engine_id: str) -> EngineAdapter | None:
        return self._adapters.get(engine_id)

    @property
    def engine_ids(self) -> frozenset[str]:
        return frozenset(self._adapters)


class WorkflowExecutor:
    """Runs one step at a time and persists its explicit result.

    Timeouts cancel the async adapter task cooperatively. They are not an OS
    sandbox and cannot forcibly terminate a blocked native call or subprocess.
    Adapters using external processes or network clients must set hard limits
    in those layers as well.
    """

    def __init__(
        self,
        *,
        workflow_store: SQLiteWorkflowStore,
        event_store: SQLiteEventStore,
        adapters: AdapterRegistry,
        policy: ExecutionPolicyProtocol,
    ) -> None:
        if not isinstance(adapters, AdapterRegistry):
            raise TypeError("adapters must be an AdapterRegistry")
        if not callable(getattr(policy, "authorize", None)):
            raise TypeError("policy must provide authorize(request)")
        self.workflow_store = workflow_store
        self.event_store = event_store
        self.adapters = adapters
        self.policy = policy

    def _collect_inputs(
        self,
        engine_id: str,
        event_ids: tuple[UUID | str, ...],
        *,
        now,
    ) -> tuple[EventRecord, ...]:
        normalized_ids: list[UUID] = []
        for value in event_ids:
            try:
                normalized_ids.append(value if isinstance(value, UUID) else UUID(str(value)))
            except (TypeError, ValueError) as exc:
                raise ValueError("input_event_ids must contain UUID values") from exc
        if len(set(normalized_ids)) != len(normalized_ids):
            raise ValueError("input_event_ids must not contain duplicates")

        records: list[EventRecord] = []
        for event_id in normalized_ids:
            record = self.event_store.get_event(event_id)
            if record is None:
                raise AuthorizationDenied(f"input event {event_id} does not exist")
            if engine_id not in record.event.allowed_consumers:
                raise AuthorizationDenied(
                    f"input event {event_id} is not allowed for engine {engine_id}"
                )
            if record.event.is_expired(now):
                raise AuthorizationDenied(f"input event {event_id} is expired")
            records.append(record)
        return tuple(records)

    @staticmethod
    def _idempotency_key(run_id: UUID, step_id: str, attempt: int) -> str:
        return f"kie:{run_id}:{step_id}:{attempt}"

    def _request(
        self,
        run,
        step: WorkflowStepRecord,
        input_events: tuple[EventRecord, ...],
        attempt: int,
    ) -> ExecutionRequest:
        return ExecutionRequest(
            run_id=run.run_id,
            workflow_id=run.workflow_id,
            workflow_version=run.workflow_version,
            step=step,
            attempt=attempt,
            idempotency_key=self._idempotency_key(run.run_id, step.step_id, attempt),
            input_events=input_events,
        )

    def _find_recorded_result(
        self,
        run_id: UUID,
        step: WorkflowStepRecord,
    ) -> EventRecord | None:
        expected_key = self._idempotency_key(run_id, step.step_id, step.attempt_count)
        cursor = 0
        while True:
            page = self.event_store.read_events(
                after_sequence=cursor,
                limit=1000,
                producer_engine=step.engine_id,
                task_id=str(run_id),
            )
            for record in page:
                if record.event.idempotency_key == expected_key:
                    self._validate_result_event(
                        record.event,
                        run_id=run_id,
                        engine_id=step.engine_id,
                        idempotency_key=expected_key,
                    )
                    return record
            if len(page) < 1000:
                return None
            cursor = page[-1].sequence

    @staticmethod
    def _validate_result_event(
        event: IntegrationEvent,
        *,
        run_id: UUID,
        engine_id: str,
        idempotency_key: str,
    ) -> None:
        if event.producer_engine != engine_id:
            raise InvalidEngineResult(
                f"result event producer {event.producer_engine} does not match step engine {engine_id}"
            )
        if event.task_id != str(run_id):
            raise InvalidEngineResult("result event task_id does not match workflow run")
        if event.idempotency_key != idempotency_key:
            raise InvalidEngineResult("result event idempotency_key does not match this execution attempt")

    async def execute_step(
        self,
        run_id: UUID | str,
        step_id: str,
        *,
        input_event_ids: tuple[UUID | str, ...] = (),
        now=None,
    ) -> WorkflowStepRecord:
        from datetime import datetime, timezone, timedelta

        timestamp = now or datetime.now(timezone.utc)
        if timestamp.tzinfo is None or timestamp.utcoffset() is None:
            raise ValueError("now must be a timezone-aware datetime")
        run = self.workflow_store.get_run(run_id)
        step = next((item for item in run.steps if item.step_id == step_id), None)
        if step is None:
            raise InvalidWorkflowTransition(f"step {step_id} does not exist in workflow run")
        run_uuid = run.run_id

        if step.status is StepStatus.RUNNING:
            existing = self._find_recorded_result(run_uuid, step)
            if existing is not None:
                return self.workflow_store.complete_step(
                    run_uuid, step_id, result_event_id=existing.event.event_id, now=timestamp
                )
            raise ExecutionAlreadyRunning(
                "step is already running; verify the engine state or timeout before retrying"
            )
        if step.status is not StepStatus.PENDING:
            raise InvalidWorkflowTransition(f"step {step_id} is {step.status.value}, not pending")

        inputs = self._collect_inputs(step.engine_id, input_event_ids, now=timestamp)
        request = self._request(run, step, inputs, step.attempt_count + 1)
        if not self.policy.authorize(request):
            raise AuthorizationDenied("execution policy denied this workflow/engine/input combination")

        started = self.workflow_store.start_step(run_uuid, step_id, now=timestamp)
        if started.status is not StepStatus.RUNNING:
            return started

        adapter = self.adapters.get(step.engine_id)
        if adapter is None:
            self.workflow_store.mark_unavailable(
                run_uuid, step_id, reason="adapter_not_registered", now=timestamp
            )
            raise AdapterUnavailable(f"no adapter registered for engine {step.engine_id}")

        request = self._request(run, started, inputs, started.attempt_count)

        try:
            result = await asyncio.wait_for(
                adapter.execute(request),
                timeout=started.timeout_seconds,
            )
        except asyncio.TimeoutError:
            deadline = timestamp + timedelta(seconds=started.timeout_seconds)
            self.workflow_store.expire_timed_out_steps(now=deadline, run_id=run_uuid)
            current = self.workflow_store.get_run(run_uuid)
            return next(item for item in current.steps if item.step_id == step_id)
        except asyncio.CancelledError:
            # A caller cancellation is not proof that external work stopped.
            # Leave RUNNING state for explicit reconciliation.
            raise
        except Exception as exc:
            return self.workflow_store.fail_step(
                run_uuid,
                step_id,
                reason=f"adapter_exception:{type(exc).__name__}",
                now=timestamp,
            )

        if not isinstance(result, ExecutionResult):
            self.workflow_store.fail_step(
                run_uuid, step_id, reason="invalid_adapter_result_type", now=timestamp
            )
            raise InvalidEngineResult("adapter must return an ExecutionResult")

        if result.outcome is ExecutionOutcome.COMPLETED:
            try:
                self._validate_result_event(
                    result.event,
                    run_id=run_uuid,
                    engine_id=step.engine_id,
                    idempotency_key=request.idempotency_key,
                )
            except InvalidEngineResult:
                self.workflow_store.fail_step(
                    run_uuid, step_id, reason="invalid_result_provenance", now=timestamp
                )
                raise
            try:
                self.event_store.append(result.event)
            except Exception as exc:
                self.workflow_store.fail_step(
                    run_uuid,
                    step_id,
                    reason=f"result_persistence_error:{type(exc).__name__}",
                    now=timestamp,
                )
                raise
            return self.workflow_store.complete_step(
                run_uuid, step_id, result_event_id=result.event.event_id, now=timestamp
            )

        if result.outcome is ExecutionOutcome.NO_DATA:
            return self.workflow_store.mark_no_data(
                run_uuid, step_id, reason=result.reason, now=timestamp
            )
        if result.outcome is ExecutionOutcome.UNAVAILABLE:
            return self.workflow_store.mark_unavailable(
                run_uuid, step_id, reason=result.reason, now=timestamp
            )
        return self.workflow_store.fail_step(
            run_uuid, step_id, reason=result.reason, now=timestamp
        )

    async def execute_next(
        self,
        run_id: UUID | str,
        *,
        input_event_ids_by_step: Mapping[str, tuple[UUID | str, ...]] | None = None,
        now=None,
    ) -> WorkflowStepRecord | None:
        """Execute the first pending step whose dependencies are completed."""
        run = self.workflow_store.get_run(run_id)
        if run.status not in {RunStatus.PENDING, RunStatus.RUNNING}:
            return None
        inputs = input_event_ids_by_step or {}
        completed_ids = {
            step.step_id for step in run.steps if step.status is StepStatus.COMPLETED
        }
        terminal_dependency_states = {
            StepStatus.SKIPPED, StepStatus.UNAVAILABLE, StepStatus.NO_DATA,
            StepStatus.FAILED, StepStatus.TIMED_OUT, StepStatus.CANCELLED,
            StepStatus.BLOCKED,
        }
        states_by_id = {step.step_id: step.status for step in run.steps}
        for step in run.steps:
            if step.status is not StepStatus.PENDING:
                continue
            dependencies_completed = all(
                dependency in completed_ids for dependency in step.depends_on
            )
            dependency_terminal = any(
                states_by_id[dependency] in terminal_dependency_states
                for dependency in step.depends_on
            )
            if dependencies_completed or dependency_terminal:
                return await self.execute_step(
                    run.run_id,
                    step.step_id,
                    input_event_ids=inputs.get(step.step_id, ()),
                    now=now,
                )
        return None
