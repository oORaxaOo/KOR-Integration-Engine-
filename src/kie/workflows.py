"""Deterministic workflow contracts and planning; deliberately no execution."""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
import re

from .events import ENGINE_IDS

_IDENTIFIER = re.compile(r"^[a-z][a-z0-9_-]{0,63}$")
_MAX_TIMEOUT_SECONDS = 300


class StepPlanStatus(str, Enum):
    READY = "ready"
    SKIPPED = "skipped"
    BLOCKED = "blocked"


@dataclass(frozen=True, slots=True)
class WorkflowStep:
    step_id: str
    engine_id: str
    depends_on: tuple[str, ...] = ()
    required: bool = True
    enabled: bool = True
    timeout_seconds: int = 30

    def __post_init__(self) -> None:
        if not isinstance(self.step_id, str) or not _IDENTIFIER.fullmatch(self.step_id):
            raise ValueError("step_id must be a lowercase identifier (1-64 characters)")
        if self.engine_id not in ENGINE_IDS:
            raise ValueError("engine_id must be a registered engine")
        if not isinstance(self.required, bool) or not isinstance(self.enabled, bool):
            raise ValueError("required and enabled must be booleans")
        if self.required and not self.enabled:
            raise ValueError("a required workflow step cannot be disabled")
        if isinstance(self.timeout_seconds, bool) or not isinstance(self.timeout_seconds, int) or not 1 <= self.timeout_seconds <= _MAX_TIMEOUT_SECONDS:
            raise ValueError("timeout_seconds must be an integer from 1 to 300")
        if not isinstance(self.depends_on, (tuple, list)):
            raise ValueError("depends_on must be a list or tuple")
        dependencies = tuple(self.depends_on)
        if any(not isinstance(item, str) or not _IDENTIFIER.fullmatch(item) for item in dependencies):
            raise ValueError("depends_on must contain valid step IDs")
        if self.step_id in dependencies:
            raise ValueError("a step cannot depend on itself")
        if len(set(dependencies)) != len(dependencies):
            raise ValueError("depends_on must not contain duplicates")
        object.__setattr__(self, "depends_on", dependencies)


@dataclass(frozen=True, slots=True)
class WorkflowDefinition:
    workflow_id: str
    version: str
    steps: tuple[WorkflowStep, ...]

    def __post_init__(self) -> None:
        if not isinstance(self.workflow_id, str) or not _IDENTIFIER.fullmatch(self.workflow_id):
            raise ValueError("workflow_id must be a lowercase identifier (1-64 characters)")
        if not isinstance(self.version, str) or not self.version.strip():
            raise ValueError("version must be a non-empty string")
        if not isinstance(self.steps, (tuple, list)) or not self.steps:
            raise ValueError("workflow must contain at least one step")
        steps = tuple(self.steps)
        if any(not isinstance(step, WorkflowStep) for step in steps):
            raise ValueError("steps must contain WorkflowStep values")
        ids = [step.step_id for step in steps]
        if len(set(ids)) != len(ids):
            raise ValueError("step_id values must be unique")
        known = set(ids)
        for step in steps:
            missing = set(step.depends_on) - known
            if missing:
                raise ValueError(f"unknown dependencies for {step.step_id}: {', '.join(sorted(missing))}")
        object.__setattr__(self, "steps", steps)
        _topological_order(steps)


@dataclass(frozen=True, slots=True)
class PlannedStep:
    step_id: str
    engine_id: str
    status: StepPlanStatus
    reason: str | None
    required: bool
    timeout_seconds: int


@dataclass(frozen=True, slots=True)
class WorkflowPlan:
    workflow_id: str
    version: str
    steps: tuple[PlannedStep, ...]
    can_start: bool

    @property
    def ready_steps(self) -> tuple[PlannedStep, ...]:
        return tuple(step for step in self.steps if step.status is StepPlanStatus.READY)


def _topological_order(steps: tuple[WorkflowStep, ...]) -> tuple[WorkflowStep, ...]:
    """Stable Kahn ordering: declaration order breaks all available-node ties."""
    completed: set[str] = set()
    ordered: list[WorkflowStep] = []
    while len(ordered) < len(steps):
        next_step = next(
            (step for step in steps if step.step_id not in completed and set(step.depends_on) <= completed),
            None,
        )
        if next_step is None:
            raise ValueError("workflow dependencies contain a cycle")
        ordered.append(next_step)
        completed.add(next_step.step_id)
    return tuple(ordered)


def plan_workflow(workflow: WorkflowDefinition, *, available_engines: frozenset[str] | set[str] | tuple[str, ...]) -> WorkflowPlan:
    """Create a stable, side-effect-free plan from a workflow and availability snapshot."""
    if not isinstance(workflow, WorkflowDefinition):
        raise TypeError("workflow must be a WorkflowDefinition")
    available = frozenset(available_engines)
    unknown = available - ENGINE_IDS
    if unknown:
        raise ValueError(f"unknown available engines: {', '.join(sorted(unknown))}")

    planned: list[PlannedStep] = []
    status_by_id: dict[str, StepPlanStatus] = {}
    for step in _topological_order(workflow.steps):
        failed_dependencies = tuple(
            dependency for dependency in step.depends_on
            if status_by_id[dependency] is not StepPlanStatus.READY
        )
        if not step.enabled:
            status, reason = StepPlanStatus.SKIPPED, "disabled_optional_step"
        elif failed_dependencies:
            status = StepPlanStatus.BLOCKED if step.required else StepPlanStatus.SKIPPED
            reason = "dependency_not_ready:" + ",".join(failed_dependencies)
        elif step.engine_id not in available:
            status = StepPlanStatus.BLOCKED if step.required else StepPlanStatus.SKIPPED
            reason = "required_engine_unavailable" if step.required else "optional_engine_unavailable"
        else:
            status, reason = StepPlanStatus.READY, None
        status_by_id[step.step_id] = status
        planned.append(PlannedStep(step.step_id, step.engine_id, status, reason, step.required, step.timeout_seconds))

    can_start = all(not step.required or step.status is StepPlanStatus.READY for step in planned)
    return WorkflowPlan(workflow.workflow_id, workflow.version, tuple(planned), can_start)
