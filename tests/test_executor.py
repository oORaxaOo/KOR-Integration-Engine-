import asyncio
from datetime import datetime, timezone, timedelta
from uuid import uuid4

import pytest

from kie.events import EpistemicStatus, IntegrationEvent, Sensitivity
from kie.event_store import SQLiteEventStore
from kie.workflow_store import (
    InvalidWorkflowTransition,
    RunStatus,
    SQLiteWorkflowStore,
    StepStatus,
)
from kie.workflows import WorkflowDefinition, WorkflowStep, plan_workflow
from kie.executor import (
    AdapterRegistry,
    AdapterUnavailable,
    AuthorizationDenied,
    ExecutionOutcome,
    ExecutionPolicy,
    ExecutionAlreadyRunning,
    ExecutionRequest,
    ExecutionResult,
    InvalidEngineResult,
    WorkflowExecutor,
)

NOW = datetime(2026, 1, 1, tzinfo=timezone.utc)


def make_workflow(timeout=10):
    return WorkflowDefinition("controlled_review", "1.0", (
        WorkflowStep("review", "critical_thinking", timeout_seconds=timeout),
    ))


def make_event(run_id, request, *, producer="critical_thinking", idempotency_key=None, task_id=None, payload=None):
    return IntegrationEvent(
        event_id=uuid4(),
        schema_version=1,
        event_type="engine_result",
        producer_engine=producer,
        producer_version="0.1.0",
        occurred_at=NOW,
        recorded_at=NOW,
        epistemic_status=EpistemicStatus.OBSERVED,
        sensitivity=Sensitivity.INTERNAL,
        allowed_consumers=("critical_thinking", "evidence") if producer == "reliability" else ("reliability", "evidence"),
        payload=payload or {"result": "ok"},
        task_id=str(run_id) if task_id is None else task_id,
        idempotency_key=request.idempotency_key if idempotency_key is None else idempotency_key,
    )


class StaticAdapter:
    engine_id = "critical_thinking"

    def __init__(self, outcome=None, event_factory=None, exception=None):
        self.outcome = outcome
        self.event_factory = event_factory
        self.exception = exception
        self.calls = 0
        self.last_request = None

    async def execute(self, request):
        self.calls += 1
        self.last_request = request
        if self.exception:
            raise self.exception
        event = self.event_factory(request) if self.event_factory else None
        return ExecutionResult(
            outcome=self.outcome or ExecutionOutcome.COMPLETED,
            event=event,
            reason="adapter reported no usable data" if self.outcome is ExecutionOutcome.NO_DATA else None,
        )


class AllowPolicy:
    def authorize(self, request):
        return True


class DenyPolicy:
    def authorize(self, request):
        return False


def setup_executor(tmp_path, adapter=None, policy=None, timeout=10):
    path = tmp_path / "kie.sqlite3"
    event_store = SQLiteEventStore(path)
    workflow_store = SQLiteWorkflowStore(path)
    workflow = make_workflow(timeout=timeout)
    plan = plan_workflow(workflow, available_engines={"critical_thinking"})
    run = workflow_store.create_run(workflow, plan, now=NOW)
    registry = AdapterRegistry(
        {"critical_thinking": adapter} if adapter is not None else {}
    )
    executor = WorkflowExecutor(
        workflow_store=workflow_store,
        event_store=event_store,
        adapters=registry,
        policy=policy or AllowPolicy(),
    )
    return event_store, workflow_store, run, adapter, executor


def test_executor_runs_authorized_adapter_and_persists_result_event(tmp_path):
    adapter = StaticAdapter(event_factory=lambda request: make_event(request.run_id, request))
    event_store, workflow_store, run, _, executor = setup_executor(tmp_path, adapter)

    step = asyncio.run(executor.execute_step(run.run_id, "review", now=NOW))

    assert step.status is StepStatus.COMPLETED
    assert step.result_event_id is not None
    stored = event_store.get_event(step.result_event_id)
    assert stored is not None
    assert stored.event.task_id == str(run.run_id)
    assert stored.event.idempotency_key == adapter.last_request.idempotency_key
    assert workflow_store.get_run(run.run_id).status is RunStatus.COMPLETED
    assert adapter.calls == 1


def test_executor_denies_by_default_without_explicit_policy(tmp_path):
    adapter = StaticAdapter(event_factory=lambda request: make_event(request.run_id, request))
    event_store, workflow_store, run, _, executor = setup_executor(
        tmp_path, adapter, policy=DenyPolicy()
    )

    with pytest.raises(AuthorizationDenied):
        asyncio.run(executor.execute_step(run.run_id, "review", now=NOW))

    current = workflow_store.get_run(run.run_id)
    assert current.steps[0].status is StepStatus.PENDING
    assert adapter.calls == 0
    assert event_store.count() == 0


def test_missing_adapter_marks_step_unavailable_without_dynamic_loading(tmp_path):
    event_store, workflow_store, run, _, executor = setup_executor(tmp_path, adapter=None)

    with pytest.raises(AdapterUnavailable):
        asyncio.run(executor.execute_step(run.run_id, "review", now=NOW))

    assert workflow_store.get_run(run.run_id).steps[0].status is StepStatus.UNAVAILABLE
    assert event_store.count() == 0


def test_output_from_wrong_engine_is_rejected_and_step_fails(tmp_path):
    adapter = StaticAdapter(
        event_factory=lambda request: make_event(request.run_id, request, producer="reliability")
    )
    _, workflow_store, run, _, executor = setup_executor(tmp_path, adapter)

    with pytest.raises(InvalidEngineResult, match="producer"):
        asyncio.run(executor.execute_step(run.run_id, "review", now=NOW))

    assert workflow_store.get_run(run.run_id).steps[0].status is StepStatus.FAILED


def test_output_must_be_linked_to_current_run_and_attempt(tmp_path):
    adapter = StaticAdapter(
        event_factory=lambda request: make_event(request.run_id, request, task_id="unrelated-task")
    )
    _, workflow_store, run, _, executor = setup_executor(tmp_path, adapter)

    with pytest.raises(InvalidEngineResult, match="task_id"):
        asyncio.run(executor.execute_step(run.run_id, "review", now=NOW))

    assert workflow_store.get_run(run.run_id).steps[0].status is StepStatus.FAILED


def test_adapter_exception_is_recorded_as_failed_without_leaking_message(tmp_path):
    adapter = StaticAdapter(exception=RuntimeError("private token=super-secret"))
    _, workflow_store, run, _, executor = setup_executor(tmp_path, adapter)

    step = asyncio.run(executor.execute_step(run.run_id, "review", now=NOW))

    assert step.status is StepStatus.FAILED
    assert "super-secret" not in (step.reason or "")
    assert "RuntimeError" in (step.reason or "")


def test_timeout_is_recorded_in_workflow_state(tmp_path):
    class SlowAdapter:
        engine_id = "critical_thinking"

        async def execute(self, request):
            await asyncio.sleep(2)
            return ExecutionResult(outcome=ExecutionOutcome.FAILED, reason="late")

    _, workflow_store, run, _, executor = setup_executor(tmp_path, SlowAdapter(), timeout=1)

    step = asyncio.run(executor.execute_step(run.run_id, "review", now=NOW))

    assert step.status is StepStatus.TIMED_OUT
    assert workflow_store.get_run(run.run_id).status is RunStatus.FAILED


def test_no_data_result_is_not_marked_completed(tmp_path):
    adapter = StaticAdapter(outcome=ExecutionOutcome.NO_DATA)
    _, workflow_store, run, _, executor = setup_executor(tmp_path, adapter)

    step = asyncio.run(executor.execute_step(run.run_id, "review", now=NOW))

    assert step.status is StepStatus.NO_DATA
    assert workflow_store.get_run(run.run_id).status is RunStatus.FAILED


def test_input_event_must_authorize_target_engine(tmp_path):
    adapter = StaticAdapter(event_factory=lambda request: make_event(request.run_id, request))
    event_store, workflow_store, run, _, executor = setup_executor(tmp_path, adapter)
    input_event = IntegrationEvent(
        event_id=uuid4(), schema_version=1, event_type="input",
        producer_engine="reliability", producer_version="0.1",
        occurred_at=NOW, recorded_at=NOW,
        epistemic_status=EpistemicStatus.OBSERVED,
        sensitivity=Sensitivity.INTERNAL,
        allowed_consumers=("evidence",), payload={"data": "no"},
    )
    event_store.append(input_event)

    with pytest.raises(AuthorizationDenied, match="not allowed"):
        asyncio.run(executor.execute_step(
            run.run_id, "review", input_event_ids=(input_event.event_id,), now=NOW
        ))

    assert adapter.calls == 0
    assert workflow_store.get_run(run.run_id).steps[0].status is StepStatus.PENDING


def test_expired_input_event_is_rejected(tmp_path):
    adapter = StaticAdapter(event_factory=lambda request: make_event(request.run_id, request))
    event_store, workflow_store, run, _, executor = setup_executor(tmp_path, adapter)
    old_event = IntegrationEvent(
        event_id=uuid4(), schema_version=1, event_type="input",
        producer_engine="reliability", producer_version="0.1",
        occurred_at=NOW - timedelta(hours=2), recorded_at=NOW - timedelta(hours=2),
        expires_at=NOW - timedelta(hours=1),
        epistemic_status=EpistemicStatus.OBSERVED,
        sensitivity=Sensitivity.INTERNAL,
        allowed_consumers=("critical_thinking",), payload={"data": "expired"},
    )
    event_store.append(old_event)

    with pytest.raises(AuthorizationDenied, match="expired"):
        asyncio.run(executor.execute_step(
            run.run_id, "review", input_event_ids=(old_event.event_id,), now=NOW
        ))

    assert adapter.calls == 0
    assert workflow_store.get_run(run.run_id).steps[0].status is StepStatus.PENDING


def test_duplicate_input_event_ids_are_rejected(tmp_path):
    adapter = StaticAdapter(event_factory=lambda request: make_event(request.run_id, request))
    event_store, _, run, _, executor = setup_executor(tmp_path, adapter)
    source = IntegrationEvent(
        event_id=uuid4(), schema_version=1, event_type="input",
        producer_engine="reliability", producer_version="0.1",
        occurred_at=NOW, recorded_at=NOW,
        epistemic_status=EpistemicStatus.OBSERVED,
        sensitivity=Sensitivity.INTERNAL,
        allowed_consumers=("critical_thinking",), payload={"data": "input"},
    )
    event_store.append(source)
    with pytest.raises(ValueError, match="duplicate"):
        asyncio.run(executor.execute_step(
            run.run_id, "review", input_event_ids=(source.event_id, source.event_id), now=NOW
        ))


def test_running_step_is_reconciled_from_idempotent_result_without_reexecution(tmp_path):
    adapter = StaticAdapter(event_factory=lambda request: make_event(request.run_id, request))
    event_store, workflow_store, run, _, executor = setup_executor(tmp_path, adapter)

    started = workflow_store.start_step(run.run_id, "review", now=NOW)
    request = ExecutionRequest(
        run_id=run.run_id,
        workflow_id=run.workflow_id,
        workflow_version=run.workflow_version,
        step=started,
        attempt=started.attempt_count,
        idempotency_key=f"kie:{run.run_id}:review:{started.attempt_count}",
        input_events=(),
    )
    event = make_event(run.run_id, request)
    event_store.append(event)

    step = asyncio.run(executor.execute_step(run.run_id, "review", now=NOW))

    assert step.status is StepStatus.COMPLETED
    assert step.result_event_id == event.event_id
    assert adapter.calls == 0


def test_running_step_without_recoverable_result_is_not_started_twice(tmp_path):
    adapter = StaticAdapter(event_factory=lambda request: make_event(request.run_id, request))
    _, workflow_store, run, _, executor = setup_executor(tmp_path, adapter)
    workflow_store.start_step(run.run_id, "review", now=NOW)

    with pytest.raises(ExecutionAlreadyRunning, match="already running"):
        asyncio.run(executor.execute_step(run.run_id, "review", now=NOW))

    assert adapter.calls == 0


def test_executor_rejects_adapter_registry_identity_mismatch():
    class WrongAdapter:
        engine_id = "reliability"
        async def execute(self, request):
            raise AssertionError("must not run")

    with pytest.raises(ValueError, match="does not match"):
        AdapterRegistry({"critical_thinking": WrongAdapter()})


def test_adapter_result_must_be_typed(tmp_path):
    class BadAdapter:
        engine_id = "critical_thinking"
        async def execute(self, request):
            return {"status": "completed"}

    _, workflow_store, run, _, executor = setup_executor(tmp_path, BadAdapter())
    with pytest.raises(InvalidEngineResult, match="ExecutionResult"):
        asyncio.run(executor.execute_step(run.run_id, "review", now=NOW))
    assert workflow_store.get_run(run.run_id).steps[0].status is StepStatus.FAILED


def test_completed_result_must_include_event_and_non_success_must_not_include_one():
    with pytest.raises(ValueError, match="requires an event"):
        ExecutionResult(outcome=ExecutionOutcome.COMPLETED)
    with pytest.raises(ValueError, match="must not include"):
        ExecutionResult(outcome=ExecutionOutcome.FAILED, event=object(), reason="failure")


def test_explicit_policy_allows_only_declared_workflow_and_engine():
    from kie.executor import ExecutionPolicy
    request = ExecutionRequest(
        run_id=uuid4(), workflow_id="controlled_review", workflow_version="1.0",
        step=type("Step", (), {"engine_id": "critical_thinking"})(),
        attempt=1, idempotency_key="test-key", input_events=(),
    )
    policy = ExecutionPolicy(
        allowed_workflows=frozenset({"controlled_review"}),
        allowed_engines=frozenset({"critical_thinking"}),
    )
    assert policy.authorize(request)
    assert not ExecutionPolicy().authorize(request)


def test_explicit_policy_rejects_input_sensitivity_without_grant(tmp_path):
    from kie.executor import ExecutionPolicy
    adapter = StaticAdapter(event_factory=lambda request: make_event(request.run_id, request))
    event_store, _, run, _, executor = setup_executor(tmp_path, adapter)
    source = IntegrationEvent(
        event_id=uuid4(), schema_version=1, event_type="restricted_input",
        producer_engine="reliability", producer_version="0.1",
        occurred_at=NOW, recorded_at=NOW,
        epistemic_status=EpistemicStatus.OBSERVED,
        sensitivity=Sensitivity.RESTRICTED,
        allowed_consumers=("critical_thinking",), payload={"data": "restricted"},
    )
    event_store.append(source)
    policy = ExecutionPolicy(
        allowed_workflows=frozenset({"controlled_review"}),
        allowed_engines=frozenset({"critical_thinking"}),
        allowed_input_sensitivities=frozenset({Sensitivity.INTERNAL}),
    )
    executor.policy = policy

    with pytest.raises(AuthorizationDenied):
        asyncio.run(executor.execute_step(
            run.run_id, "review", input_event_ids=(source.event_id,), now=NOW
        ))
    assert adapter.calls == 0


def test_execute_next_propagates_skipped_optional_dependency(tmp_path):
    workflow = WorkflowDefinition("optional_chain", "1.0", (
        WorkflowStep("primary", "critical_thinking"),
        WorkflowStep("optional_source", "emotion", required=False),
        WorkflowStep("optional_followup", "reliability", depends_on=("optional_source",), required=False),
    ))
    path = tmp_path / "optional-chain.sqlite3"
    event_store = SQLiteEventStore(path)
    workflow_store = SQLiteWorkflowStore(path)
    plan = plan_workflow(
        workflow,
        available_engines={"critical_thinking", "emotion", "reliability"},
    )
    run = workflow_store.create_run(workflow, plan, now=NOW)
    adapter = StaticAdapter(event_factory=lambda request: make_event(request.run_id, request))
    executor = WorkflowExecutor(
        workflow_store=workflow_store,
        event_store=event_store,
        adapters=AdapterRegistry({"critical_thinking": adapter}),
        policy=AllowPolicy(),
    )

    primary = asyncio.run(executor.execute_next(run.run_id, now=NOW))
    assert primary.step_id == "primary"
    assert primary.status is StepStatus.COMPLETED

    workflow_store.skip_step(
        run.run_id, "optional_source", reason="not needed", now=NOW
    )
    followup = asyncio.run(executor.execute_next(run.run_id, now=NOW))

    assert followup.step_id == "optional_followup"
    assert followup.status is StepStatus.SKIPPED
    assert workflow_store.get_run(run.run_id).status is RunStatus.COMPLETED
    assert adapter.calls == 1


def test_executor_timeout_only_expires_the_current_run(tmp_path):
    path = tmp_path / "scope-timeout.sqlite3"
    event_store = SQLiteEventStore(path)
    workflow_store = SQLiteWorkflowStore(path)
    workflow = make_workflow(timeout=1)
    plan = plan_workflow(workflow, available_engines={"critical_thinking"})
    run_a = workflow_store.create_run(workflow, plan, now=NOW)
    run_b = workflow_store.create_run(workflow, plan, now=NOW)
    workflow_store.start_step(run_b.run_id, "review", now=NOW)

    class SlowAdapter:
        engine_id = "critical_thinking"
        async def execute(self, request):
            await asyncio.sleep(2)
            return ExecutionResult(outcome=ExecutionOutcome.FAILED, reason="late")

    executor = WorkflowExecutor(
        workflow_store=workflow_store, event_store=event_store,
        adapters=AdapterRegistry({"critical_thinking": SlowAdapter()}),
        policy=AllowPolicy(),
    )
    # Reconcile the second run as still RUNNING; execute_step on run A times out.
    step = asyncio.run(executor.execute_step(run_a.run_id, "review", now=NOW))
    assert step.status is StepStatus.TIMED_OUT
    assert workflow_store.get_run(run_b.run_id).steps[0].status is StepStatus.RUNNING
