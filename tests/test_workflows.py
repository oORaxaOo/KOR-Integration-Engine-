import pytest
from kie.workflows import StepPlanStatus, WorkflowDefinition, WorkflowStep, plan_workflow


def test_planner_orders_dependencies_and_preserves_declaration_order_for_ties():
    workflow = WorkflowDefinition("standard_review", "1.0", (
        WorkflowStep("first", "critical_thinking"),
        WorkflowStep("third", "evidence", depends_on=("first",)),
        WorkflowStep("second", "reliability"),
        WorkflowStep("fourth", "emotion", depends_on=("second", "third")),
    ))
    plan = plan_workflow(workflow, available_engines={"critical_thinking", "reliability", "evidence", "emotion"})
    assert [step.step_id for step in plan.steps] == ["first", "third", "second", "fourth"]
    assert all(step.status is StepPlanStatus.READY for step in plan.steps)
    assert plan.can_start


def test_unavailable_optional_engine_is_skipped_without_blocking_workflow():
    workflow = WorkflowDefinition("optional_check", "1.0", (
        WorkflowStep("primary", "critical_thinking"),
        WorkflowStep("optional", "emotion", required=False),
    ))
    plan = plan_workflow(workflow, available_engines={"critical_thinking"})
    assert plan.steps[1].status is StepPlanStatus.SKIPPED
    assert plan.steps[1].reason == "optional_engine_unavailable"
    assert plan.can_start


def test_unavailable_required_engine_blocks_workflow():
    workflow = WorkflowDefinition("required_check", "1.0", (
        WorkflowStep("primary", "critical_thinking"),
        WorkflowStep("proof", "evidence"),
    ))
    plan = plan_workflow(workflow, available_engines={"critical_thinking"})
    assert plan.steps[1].status is StepPlanStatus.BLOCKED
    assert plan.steps[1].reason == "required_engine_unavailable"
    assert not plan.can_start


def test_dependency_on_skipped_optional_step_propagates_block_or_skip():
    workflow = WorkflowDefinition("dependency_check", "1.0", (
        WorkflowStep("optional", "emotion", required=False),
        WorkflowStep("required_followup", "evidence", depends_on=("optional",)),
        WorkflowStep("optional_followup", "reliability", depends_on=("optional",), required=False),
    ))
    plan = plan_workflow(workflow, available_engines={"evidence", "reliability"})
    by_id = {step.step_id: step for step in plan.steps}
    assert by_id["optional"].status is StepPlanStatus.SKIPPED
    assert by_id["required_followup"].status is StepPlanStatus.BLOCKED
    assert by_id["optional_followup"].status is StepPlanStatus.SKIPPED
    assert not plan.can_start


def test_disabled_required_step_is_rejected():
    with pytest.raises(ValueError, match="required workflow step cannot be disabled"):
        WorkflowStep("must_run", "evidence", enabled=False)


def test_unknown_dependency_is_rejected():
    with pytest.raises(ValueError, match="unknown dependencies"):
        WorkflowDefinition("bad_dependency", "1.0", (WorkflowStep("step", "evidence", depends_on=("missing",)),))


def test_dependency_cycle_is_rejected():
    with pytest.raises(ValueError, match="cycle"):
        WorkflowDefinition("cyclic", "1.0", (
            WorkflowStep("alpha", "evidence", depends_on=("beta",)),
            WorkflowStep("beta", "reliability", depends_on=("alpha",)),
        ))


def test_unknown_available_engine_is_rejected():
    workflow = WorkflowDefinition("valid", "1.0", (WorkflowStep("step", "evidence"),))
    with pytest.raises(ValueError, match="unknown available engines"):
        plan_workflow(workflow, available_engines={"unknown"})


def test_timeout_is_bounded():
    with pytest.raises(ValueError, match="1 to 300"):
        WorkflowStep("step", "evidence", timeout_seconds=301)
