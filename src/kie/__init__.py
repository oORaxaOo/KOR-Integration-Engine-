"""KOR Integration Engine: deterministic coordination primitives."""

from .events import (
    ENGINE_IDS, EpistemicStatus, IntegrationEvent, Sensitivity, Verification,
    event_from_dict, event_to_dict,
)
from .routing import RouteDecision, route_event
from .workflows import (
    PlannedStep, StepPlanStatus, WorkflowDefinition, WorkflowPlan,
    WorkflowStep, plan_workflow,
)
from .event_store import (
    AppendResult, EventConflictError, EventRecord, EventStoreError,
    IdempotencyConflictError, IntegrityReport, SQLiteEventStore,
)
from .workflow_store import (
    InvalidWorkflowTransition, RetryLimitExceeded, RunNotFoundError,
    RunStatus, SQLiteWorkflowStore, StepNotFoundError, StepStatus,
    WorkflowRunRecord, WorkflowStepRecord, WorkflowStoreError,
    WorkflowTransitionRecord,
)

__all__ = [
    "ENGINE_IDS", "EpistemicStatus", "IntegrationEvent", "Sensitivity",
    "Verification", "event_from_dict", "event_to_dict", "RouteDecision",
    "route_event", "PlannedStep", "StepPlanStatus", "WorkflowDefinition",
    "WorkflowPlan", "WorkflowStep", "plan_workflow", "AppendResult",
    "EventConflictError", "EventRecord", "EventStoreError",
    "IdempotencyConflictError", "IntegrityReport", "SQLiteEventStore",
    "InvalidWorkflowTransition", "RetryLimitExceeded", "RunNotFoundError",
    "RunStatus", "SQLiteWorkflowStore", "StepNotFoundError", "StepStatus",
    "WorkflowRunRecord", "WorkflowStepRecord", "WorkflowStoreError",
    "WorkflowTransitionRecord",
]


from .executor import (
    AdapterRegistry, AdapterUnavailable, AuthorizationDenied,
    ExecutionAlreadyRunning, ExecutionOutcome, ExecutionPolicy,
    ExecutionRequest, ExecutionResult, ExecutorError, InvalidEngineResult,
    WorkflowExecutor,
)

__all__ += [
    "AdapterRegistry", "AdapterUnavailable", "AuthorizationDenied",
    "ExecutionAlreadyRunning", "ExecutionOutcome", "ExecutionPolicy",
    "ExecutionRequest", "ExecutionResult", "ExecutorError",
    "InvalidEngineResult", "WorkflowExecutor",
]
