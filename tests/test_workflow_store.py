from datetime import datetime, timezone, timedelta
from uuid import uuid4
import sqlite3

import pytest

from kie.workflows import WorkflowDefinition, WorkflowStep, plan_workflow
from kie.workflow_store import (
    InvalidWorkflowTransition,
    RetryLimitExceeded,
    RunStatus,
    SQLiteWorkflowStore,
    StepStatus,
)

NOW = datetime(2026, 1, 1, tzinfo=timezone.utc)


def make_workflow():
    return WorkflowDefinition("standard_review", "1.0", (
        WorkflowStep("initial_review", "critical_thinking", timeout_seconds=20),
        WorkflowStep("reliability_check", "reliability", depends_on=("initial_review",), timeout_seconds=10),
        WorkflowStep("evidence_check", "evidence", depends_on=("reliability_check",)),
        WorkflowStep("optional_reflection", "emotion", required=False),
    ))


def make_plan(workflow=None, available=None):
    workflow = workflow or make_workflow()
    available = available if available is not None else {
        "critical_thinking", "reliability", "evidence", "emotion"
    }
    return plan_workflow(workflow, available_engines=available)


def test_create_run_persists_plan_and_initial_transitions(tmp_path):
    workflow = make_workflow()
    plan = make_plan(workflow)
    run_id = uuid4()
    with SQLiteWorkflowStore(tmp_path / "workflow.sqlite3") as store:
        record = store.create_run(workflow, plan, run_id=run_id, now=NOW)
        restored = store.get_run(run_id)
        transitions = store.read_transitions(run_id)

        assert record == restored
        assert record.status is RunStatus.PENDING
        assert [step.status for step in record.steps] == [
            StepStatus.PENDING, StepStatus.PENDING, StepStatus.PENDING, StepStatus.PENDING
        ]
        assert len(transitions) == 5  # one run transition, four step transitions


def test_optional_unavailable_step_is_persisted_as_skipped(tmp_path):
    workflow = make_workflow()
    plan = make_plan(workflow, {"critical_thinking", "reliability", "evidence"})
    with SQLiteWorkflowStore(tmp_path / "workflow.sqlite3") as store:
        record = store.create_run(workflow, plan, now=NOW)
        optional = next(step for step in record.steps if step.step_id == "optional_reflection")
        assert optional.status is StepStatus.SKIPPED
        assert record.status is RunStatus.PENDING


def test_required_unavailable_plan_creates_blocked_run(tmp_path):
    workflow = make_workflow()
    plan = make_plan(workflow, {"critical_thinking", "reliability"})
    with SQLiteWorkflowStore(tmp_path / "workflow.sqlite3") as store:
        record = store.create_run(workflow, plan, now=NOW)
        assert record.status is RunStatus.BLOCKED
        assert next(s for s in record.steps if s.step_id == "evidence_check").status is StepStatus.BLOCKED
        with pytest.raises(InvalidWorkflowTransition, match="blocked"):
            store.start_step(record.run_id, "initial_review", now=NOW)


def test_dependencies_must_complete_before_step_can_start(tmp_path):
    workflow = make_workflow()
    plan = make_plan(workflow)
    with SQLiteWorkflowStore(tmp_path / "workflow.sqlite3") as store:
        record = store.create_run(workflow, plan, now=NOW)
        with pytest.raises(InvalidWorkflowTransition, match="dependencies"):
            store.start_step(record.run_id, "reliability_check", now=NOW)

        store.start_step(record.run_id, "initial_review", now=NOW)
        with pytest.raises(InvalidWorkflowTransition, match="dependencies"):
            store.start_step(record.run_id, "reliability_check", now=NOW)


def test_successful_steps_run_in_order_and_complete_workflow(tmp_path):
    workflow = make_workflow()
    with SQLiteWorkflowStore(tmp_path / "workflow.sqlite3") as store:
        run = store.create_run(workflow, make_plan(workflow), now=NOW)
        store.start_step(run.run_id, "initial_review", now=NOW)
        store.complete_step(run.run_id, "initial_review", now=NOW + timedelta(seconds=2))
        store.start_step(run.run_id, "reliability_check", now=NOW + timedelta(seconds=3))
        store.complete_step(run.run_id, "reliability_check", now=NOW + timedelta(seconds=4))
        store.start_step(run.run_id, "evidence_check", now=NOW + timedelta(seconds=5))
        store.complete_step(run.run_id, "evidence_check", now=NOW + timedelta(seconds=6))
        store.skip_step(
            run.run_id, "optional_reflection",
            reason="no additional reflection required", now=NOW + timedelta(seconds=7),
        )

        completed = store.get_run(run.run_id)
        assert completed.status is RunStatus.COMPLETED
        assert all(
            step.status in {StepStatus.COMPLETED, StepStatus.SKIPPED}
            for step in completed.steps
        )


def test_failed_required_step_fails_run(tmp_path):
    workflow = make_workflow()
    with SQLiteWorkflowStore(tmp_path / "workflow.sqlite3") as store:
        run = store.create_run(workflow, make_plan(workflow), now=NOW)
        store.start_step(run.run_id, "initial_review", now=NOW)
        store.fail_step(run.run_id, "initial_review", reason="engine returned error", now=NOW)
        current = store.get_run(run.run_id)
        assert current.status is RunStatus.FAILED


def test_optional_failure_produces_partial_result(tmp_path):
    workflow = make_workflow()
    with SQLiteWorkflowStore(tmp_path / "workflow.sqlite3") as store:
        run = store.create_run(workflow, make_plan(workflow), now=NOW)
        for step_id in ("initial_review", "reliability_check", "evidence_check"):
            store.start_step(run.run_id, step_id, now=NOW)
            store.complete_step(run.run_id, step_id, now=NOW + timedelta(seconds=1))
        store.start_step(run.run_id, "optional_reflection", now=NOW + timedelta(seconds=2))
        store.fail_step(run.run_id, "optional_reflection", reason="optional engine unavailable", now=NOW + timedelta(seconds=3))
        assert store.get_run(run.run_id).status is RunStatus.PARTIAL


def test_no_data_is_not_treated_as_success_for_required_step(tmp_path):
    workflow = make_workflow()
    with SQLiteWorkflowStore(tmp_path / "workflow.sqlite3") as store:
        run = store.create_run(workflow, make_plan(workflow), now=NOW)
        store.start_step(run.run_id, "initial_review", now=NOW)
        store.mark_no_data(run.run_id, "initial_review", reason="no usable observation", now=NOW)
        assert store.get_run(run.run_id).status is RunStatus.FAILED


def test_timeout_sweeper_marks_expired_running_step(tmp_path):
    workflow = make_workflow()
    with SQLiteWorkflowStore(tmp_path / "workflow.sqlite3") as store:
        run = store.create_run(workflow, make_plan(workflow), now=NOW)
        store.start_step(run.run_id, "initial_review", now=NOW)
        changed = store.expire_timed_out_steps(now=NOW + timedelta(seconds=20))
        current = store.get_run(run.run_id)
        step = next(s for s in current.steps if s.step_id == "initial_review")
        assert changed == ((run.run_id, "initial_review"),)
        assert step.status is StepStatus.TIMED_OUT
        assert current.status is RunStatus.FAILED


def test_timeout_is_not_triggered_before_deadline(tmp_path):
    workflow = make_workflow()
    with SQLiteWorkflowStore(tmp_path / "workflow.sqlite3") as store:
        run = store.create_run(workflow, make_plan(workflow), now=NOW)
        store.start_step(run.run_id, "initial_review", now=NOW)
        assert store.expire_timed_out_steps(now=NOW + timedelta(seconds=19)) == ()


def test_retry_limit_is_enforced_and_attempts_are_recorded(tmp_path):
    workflow = make_workflow()
    with SQLiteWorkflowStore(tmp_path / "workflow.sqlite3") as store:
        run = store.create_run(workflow, make_plan(workflow), max_retries=1, now=NOW)
        store.start_step(run.run_id, "initial_review", now=NOW)
        store.fail_step(run.run_id, "initial_review", reason="temporary error", now=NOW)
        store.retry_step(run.run_id, "initial_review", now=NOW + timedelta(seconds=1))
        store.start_step(run.run_id, "initial_review", now=NOW + timedelta(seconds=2))
        store.fail_step(run.run_id, "initial_review", reason="second error", now=NOW + timedelta(seconds=3))
        with pytest.raises(RetryLimitExceeded):
            store.retry_step(run.run_id, "initial_review", now=NOW + timedelta(seconds=4))
        step = next(s for s in store.get_run(run.run_id).steps if s.step_id == "initial_review")
        assert step.attempt_count == 2
        assert step.status is StepStatus.FAILED


def test_cancel_run_cancels_pending_and_running_steps(tmp_path):
    workflow = make_workflow()
    with SQLiteWorkflowStore(tmp_path / "workflow.sqlite3") as store:
        run = store.create_run(workflow, make_plan(workflow), now=NOW)
        store.start_step(run.run_id, "initial_review", now=NOW)
        store.cancel_run(run.run_id, reason="operator requested cancellation", now=NOW)
        current = store.get_run(run.run_id)
        assert current.status is RunStatus.CANCELLED
        assert all(
            step.status in {StepStatus.CANCELLED, StepStatus.SKIPPED}
            for step in current.steps
        )


def test_transition_history_is_append_only(tmp_path):
    path = tmp_path / "workflow.sqlite3"
    with SQLiteWorkflowStore(path) as store:
        run = store.create_run(make_workflow(), make_plan(), now=NOW)
    connection = sqlite3.connect(path)
    try:
        with pytest.raises(sqlite3.IntegrityError, match="append-only"):
            connection.execute("UPDATE kie_workflow_transitions SET reason = 'rewritten'")
        with pytest.raises(sqlite3.IntegrityError, match="append-only"):
            connection.execute("DELETE FROM kie_workflow_transitions")
    finally:
        connection.close()


def test_transition_history_records_attempt_and_status(tmp_path):
    workflow = make_workflow()
    with SQLiteWorkflowStore(tmp_path / "workflow.sqlite3") as store:
        run = store.create_run(workflow, make_plan(workflow), now=NOW)
        store.start_step(run.run_id, "initial_review", now=NOW)
        store.fail_step(run.run_id, "initial_review", reason="test failure", now=NOW)
        transitions = store.read_transitions(run.run_id, step_id="initial_review")
        assert [item.to_status for item in transitions] == [
            StepStatus.PENDING.value, StepStatus.RUNNING.value, StepStatus.FAILED.value
        ]


def test_store_reuses_same_database_as_event_store(tmp_path):
    from kie.event_store import SQLiteEventStore

    path = tmp_path / "shared.sqlite3"
    with SQLiteEventStore(path), SQLiteWorkflowStore(path) as workflow_store:
        run = workflow_store.create_run(make_workflow(), make_plan(), now=NOW)
        assert workflow_store.get_run(run.run_id) is not None


def test_invalid_max_retries_and_timezone_are_rejected(tmp_path):
    with SQLiteWorkflowStore(tmp_path / "workflow.sqlite3") as store:
        with pytest.raises(ValueError, match="max_retries"):
            store.create_run(make_workflow(), make_plan(), max_retries=-1, now=NOW)
        with pytest.raises(ValueError, match="timezone-aware"):
            store.create_run(make_workflow(), make_plan(), now=datetime(2026, 1, 1))


def test_run_and_incomplete_steps_are_recoverable_after_reopen(tmp_path):
    path = tmp_path / "workflow.sqlite3"
    workflow = make_workflow()
    with SQLiteWorkflowStore(path) as first:
        run = first.create_run(workflow, make_plan(workflow), now=NOW)
        first.start_step(run.run_id, "initial_review", now=NOW)
    with SQLiteWorkflowStore(path) as second:
        restored = second.get_run(run.run_id)
        assert restored.status is RunStatus.RUNNING
        step = next(s for s in restored.steps if s.step_id == "initial_review")
        assert step.status is StepStatus.RUNNING
        assert step.attempt_count == 1
        assert step.deadline_at == NOW + timedelta(seconds=20)


def test_second_completion_attempt_is_rejected(tmp_path):
    workflow = make_workflow()
    with SQLiteWorkflowStore(tmp_path / "workflow.sqlite3") as store:
        run = store.create_run(workflow, make_plan(workflow), now=NOW)
        store.start_step(run.run_id, "initial_review", now=NOW)
        store.complete_step(run.run_id, "initial_review", now=NOW + timedelta(seconds=1))
        with pytest.raises(InvalidWorkflowTransition, match="not running"):
            store.complete_step(run.run_id, "initial_review", now=NOW + timedelta(seconds=2))


def test_manual_skip_is_allowed_only_for_optional_pending_step(tmp_path):
    workflow = make_workflow()
    with SQLiteWorkflowStore(tmp_path / "workflow.sqlite3") as store:
        run = store.create_run(workflow, make_plan(workflow), now=NOW)
        with pytest.raises(InvalidWorkflowTransition, match="required"):
            store.skip_step(run.run_id, "initial_review", now=NOW)
        skipped = store.skip_step(run.run_id, "optional_reflection", reason="not relevant", now=NOW)
        assert skipped.status is StepStatus.SKIPPED
        assert skipped.reason == "not relevant"


def test_completed_step_can_link_a_result_event_id(tmp_path):
    workflow = make_workflow()
    result_id = uuid4()
    with SQLiteWorkflowStore(tmp_path / "workflow.sqlite3") as store:
        run = store.create_run(workflow, make_plan(workflow), now=NOW)
        store.start_step(run.run_id, "initial_review", now=NOW)
        completed = store.complete_step(
            run.run_id, "initial_review", result_event_id=result_id,
            now=NOW + timedelta(seconds=1),
        )
        assert completed.result_event_id == result_id


def test_pending_unavailable_step_is_recorded_as_unavailable(tmp_path):
    workflow = make_workflow()
    with SQLiteWorkflowStore(tmp_path / "workflow.sqlite3") as store:
        run = store.create_run(workflow, make_plan(workflow), now=NOW)
        unavailable = store.mark_unavailable(
            run.run_id, "initial_review", reason="engine did not respond", now=NOW,
        )
        assert unavailable.status is StepStatus.UNAVAILABLE
        assert store.get_run(run.run_id).status is RunStatus.FAILED


def test_dependency_failure_blocks_dependent_step(tmp_path):
    workflow = make_workflow()
    with SQLiteWorkflowStore(tmp_path / "workflow.sqlite3") as store:
        run = store.create_run(workflow, make_plan(workflow), now=NOW)
        store.start_step(run.run_id, "initial_review", now=NOW)
        store.fail_step(run.run_id, "initial_review", reason="failed", now=NOW)
        with pytest.raises(InvalidWorkflowTransition, match="not startable"):
            store.start_step(run.run_id, "reliability_check", now=NOW)
        assert store.get_run(run.run_id).status is RunStatus.FAILED


def test_list_runs_can_filter_by_status(tmp_path):
    workflow = make_workflow()
    with SQLiteWorkflowStore(tmp_path / "workflow.sqlite3") as store:
        first = store.create_run(workflow, make_plan(workflow), now=NOW)
        second = store.create_run(workflow, make_plan(workflow), now=NOW + timedelta(seconds=1))
        store.start_step(second.run_id, "initial_review", now=NOW + timedelta(seconds=2))
        pending = store.list_runs(status=RunStatus.PENDING)
        running = store.list_runs(status=RunStatus.RUNNING)
        assert [item.run_id for item in pending] == [first.run_id]
        assert [item.run_id for item in running] == [second.run_id]


def test_duplicate_run_id_is_rejected_without_partial_rows(tmp_path):
    workflow = make_workflow()
    run_id = uuid4()
    with SQLiteWorkflowStore(tmp_path / "workflow.sqlite3") as store:
        store.create_run(workflow, make_plan(workflow), run_id=run_id, now=NOW)
        with pytest.raises(Exception, match="could not create workflow run"):
            store.create_run(workflow, make_plan(workflow), run_id=run_id, now=NOW)
        assert len(store.get_run(run_id).steps) == 4


def test_workflow_does_not_complete_while_available_optional_step_is_pending(tmp_path):
    workflow = make_workflow()
    with SQLiteWorkflowStore(tmp_path / "workflow.sqlite3") as store:
        run = store.create_run(workflow, make_plan(workflow), now=NOW)
        for step_id in ("initial_review", "reliability_check", "evidence_check"):
            store.start_step(run.run_id, step_id, now=NOW)
            store.complete_step(run.run_id, step_id, now=NOW + timedelta(seconds=1))
        current = store.get_run(run.run_id)
        optional = next(s for s in current.steps if s.step_id == "optional_reflection")
        assert optional.status is StepStatus.PENDING
        assert current.status is RunStatus.PENDING


def test_timeout_sweep_can_be_scoped_to_one_run(tmp_path):
    workflow = make_workflow()
    with SQLiteWorkflowStore(tmp_path / "workflow.sqlite3") as store:
        first = store.create_run(workflow, make_plan(workflow), now=NOW)
        second = store.create_run(workflow, make_plan(workflow), now=NOW)
        store.start_step(first.run_id, "initial_review", now=NOW)
        store.start_step(second.run_id, "initial_review", now=NOW)

        changed = store.expire_timed_out_steps(
            now=NOW + timedelta(seconds=20), run_id=first.run_id
        )

        assert changed == ((first.run_id, "initial_review"),)
        assert store.get_run(first.run_id).steps[0].status is StepStatus.TIMED_OUT
        assert store.get_run(second.run_id).steps[0].status is StepStatus.RUNNING
