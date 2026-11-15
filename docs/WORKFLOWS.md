# KIE Workflow Contracts — Phase 1

## Purpose

A workflow definition declares which engine steps belong to a task, which steps
are required, and which steps depend on other steps. The planner turns that
definition plus an availability snapshot into a deterministic plan.

## Planner guarantees

- It has no network, database, Hermes, or engine-execution side effects.
- Only declared steps can appear in a plan.
- Dependencies are validated before planning; unknown dependencies and cycles
  are rejected.
- Ordering is topological. Declaration order breaks ties, making plans stable.
- An unavailable optional engine is marked skipped.
- An unavailable required engine is marked blocked.
- A step whose dependency is not ready is itself blocked when required and
  skipped when optional.
- can_start is true only when every required step is ready.
- Timeouts are declarations for a future executor; this phase does not enforce
  wall-clock time limits because it does not execute work.

## Planning statuses

- ready: the step is eligible for a future executor to consider.
- skipped: the step is optional and not scheduled, disabled, or its dependency
  is not ready.
- blocked: a required step cannot currently be scheduled.

These are planning results, not runtime states. They must not be reported as
completed, failed, or verified.

## Deliberate limits

This phase does not launch engines, retry calls, cancel running work, persist
workflow state, or infer success from an engine response. Those are separate
responsibilities for a later executor and durable workflow store. A live
availability check is only a snapshot; the executor must re-check availability
and authorization at execution time.
