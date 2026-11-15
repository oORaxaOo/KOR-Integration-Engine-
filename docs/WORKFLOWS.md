# KIE Workflow Contracts — Planning and State Phase

## Planning
A workflow definition declares which engine steps belong to a task, which
steps are required, and which steps depend on other steps. The planner turns
that definition plus an availability snapshot into a deterministic plan.

- It has no network, database, Hermes, or engine-execution side effects.
- Unknown dependencies and cycles are rejected.
- Ordering is topological; declaration order breaks ties.
- An unavailable optional engine is skipped.
- An unavailable required engine is blocked.
- A step whose dependency is not ready is blocked when required and skipped
  when optional.
- Timeouts are declarations for the future executor.

## Persistent run states
Run states:
- PENDING: created and awaiting execution;
- RUNNING: at least one step has been started;
- COMPLETED: every required step completed and optional steps completed/skipped;
- PARTIAL: required work completed, but optional work did not;
- FAILED: required work failed, timed out, had no data, became unavailable, or was blocked;
- BLOCKED: the initial plan contains a required blocked step;
- CANCELLED: an operator explicitly cancelled the run.

Step states:
- PENDING, RUNNING, COMPLETED, SKIPPED, UNAVAILABLE, NO_DATA,
  FAILED, TIMED_OUT, CANCELLED, BLOCKED.

## Transition rules

- Only PENDING steps can be started or manually skipped.
- Required steps cannot be skipped.
- A step can start only after every dependency is COMPLETED.
- A step's timeout deadline is calculated at start from its declared timeout.
- A RUNNING step can complete, fail, return NO_DATA, or become unavailable.
- A timeout poll marks overdue RUNNING steps as TIMED_OUT.
- Retry is explicit, allowed only for UNAVAILABLE, NO_DATA, FAILED, or TIMED_OUT,
  and bounded by the maximum configured retry count for the run.
- Every start and state change is recorded in the transition history.
- Terminal outcomes are not silently overwritten; retry creates a new
  transition back to PENDING and then a new RUNNING attempt.

The state manager updates the current-state projection and appends its
transition record in the same SQLite transaction. The transition table
rejects normal UPDATE and DELETE operations.

## Restart and recovery

Runs and steps are stored durably. After a host restart, list_runs() and
get_run() expose the last known state. A persisted RUNNING state only proves
that the step was started before shutdown; it does not prove that the engine
process survived. The future executor must verify engine/process state and
reconcile expired deadlines using expire_timed_out_steps() before continuing.

## Deliberate limits

This phase does not execute engines, run background timers, perform automatic
retries, kill processes, or guarantee exactly-once external side effects.
A crash may occur after an engine performs work but before its completion
is recorded. Future adapters/executors must make operations idempotent or
use event-linked result recording to safely handle that ambiguity.

The store can share the same SQLite file as the event ledger, but the two
public APIs use independent connections. Each transition is atomic within the
workflow store; this phase does not claim a distributed transaction across an
engine and the KIE.
