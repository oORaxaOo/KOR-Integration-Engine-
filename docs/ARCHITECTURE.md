# KOR Integration Engine — Architecture

## Mission
The KIE is a deterministic coordination layer. It is not a fifth reasoning
engine, does not own the other engines' internal memories, and does not decide
which engine is inherently more authoritative.

The four engines remain independently deployable and retain ownership of
their internal data:
- Critical Thinking: challenge assumptions and review decisions.
- Reliability: observe technical behavior and operational reliability.
- Evidence: verify explicitly defined criteria against recorded evidence.
- Emotion: bounded appraisal, self-observation, and reflection hypotheses.

## House model
- **Foundation:** the LLM provides language and reasoning capabilities. It is
  not itself the reliability foundation.
- **Walls:** Hermes provides the host runtime, plugin lifecycle, tools, and
  execution environment.
- **Pillars:** the four independent engines provide distinct capabilities.
- **Roof:** KIE coordinates workflows and controlled information exchange.
- Schemas, persistence, tests, permissions, backups, and monitoring are also
  structural parts of system reliability.

## Non-goals
KIE v0.x will not make LLM calls, execute arbitrary tools or commands, read or
write engine databases directly, silently promote hypotheses to facts, rewrite
engine policies or its own permissions, or claim consciousness from emergent
behavior. Emergence observation is not a foundation delivery gate.

## Coordination
A versioned workflow declares which engine steps belong to a task, which are
required, their dependencies, and timeouts. The planner creates a deterministic
availability-aware plan. The Workflow State Manager persists runs and records
step transitions, but does not execute engine code.

Runtime states remain distinct:
- PENDING: eligible but not started;
- RUNNING: an execution has been explicitly started;
- COMPLETED: the caller recorded a successful result;
- SKIPPED: an optional step was not needed;
- UNAVAILABLE: a required or optional engine could not be reached;
- NO_DATA: an engine responded but supplied no usable result;
- FAILED: execution failed;
- TIMED_OUT: the declared deadline passed before completion;
- BLOCKED: a required dependency or engine condition prevents progress;
- CANCELLED: execution was explicitly cancelled.

Run-level outcomes are PENDING, RUNNING, COMPLETED, PARTIAL, FAILED, BLOCKED,
and CANCELLED. Required non-success outcomes fail the run; optional non-success
outcomes can produce PARTIAL. Missing information remains missing.

## Priority
1. System safety, privacy, and authorization.
2. Task requirements and required verification gates.
3. Workflow dependencies and ordering.
4. Resource scheduling and timeouts.

No engine output automatically overrides another. Conflicts are surfaced
with provenance and scope. Verification is authoritative only for its
declared criteria and scope.

## Memory boundaries
Each engine remains the source of truth for its own internal store. KIE stores
event envelopes, provenance, routing history, workflow state, and curated
cross-engine knowledge. Adapters expose only authorized fields. KIE does not
query arbitrary engine databases.

The event ledger is an append-only record of integration events, not a shared
transcript or a replacement for engine memory. The workflow transition table
is an append-only history; run/step tables are the current-state projections
updated transactionally alongside each transition. Corrections are new linked
events; existing event records are not rewritten.

## Event ledger
The SQLite ledger assigns a monotonic sequence, stores a canonical event
envelope, applies event-ID and producer-scoped idempotency checks in a write
transaction, and links each entry to the previous entry with a SHA-256 hash.
Database triggers reject ordinary UPDATE and DELETE operations. Integrity
verification checks SQLite consistency, envelope digests, indexed metadata,
sequence continuity, and the hash chain.

The hash chain is tamper-evident, not tamper-proof. An actor with control of
the database file can remove triggers or rewrite data; truncating the final
records cannot be detected without an independently stored external anchor.
The ledger does not encrypt payloads. File permissions, encryption at rest,
backup handling, and host authentication remain separate requirements.

Historical event replay is read-only: it returns events in ledger order and
never re-invokes engines. Event dispatch and retries belong to a later executor.

## Workflow State Manager
The state manager may share the event store's SQLite file, but maintains its
own schema version. Every run stores a snapshot of the workflow identity and
each step's engine, dependencies, required flag, and timeout. State transitions
are committed in a transaction with the corresponding current-state update.
The transition history rejects ordinary UPDATE and DELETE statements.

Retries are explicit, bounded, and recorded as PENDING -> RUNNING attempt
transitions. A deadline is calculated when a step starts. The current phase
provides a polling method to mark overdue steps TIMED_OUT; it does not start
background threads or kill a running engine. A future executor must enforce
actual process/network timeouts and call the state manager with the result.

A process restart does not erase runs. The next host process can list pending
and running runs, inspect step states and transition history, and reconcile
expired deadlines before it considers resuming. Persisted RUNNING means the
work had been started, not that it is still alive after a crash. The executor
must check the engine's real availability before resuming or retrying.

The transition ledger is append-only but the current-state tables are mutable
projections, not independently tamper-proof. The database remains protected by
host file permissions and backups; SQLite triggers are not authentication.

## Feedback-loop controls
The protocol includes event IDs, parent IDs, correlation IDs, idempotency
keys, expiry, and hop limits. The router enforces consumer allowlists.
Persistence enforces event ID uniqueness and producer-scoped idempotency.
An engine cannot re-ingest its own event as new evidence without independent
information.

## Emergence observation
Emergence observation is deferred. A later observer may compare isolated
baselines with controlled integrated runs. It is not required for foundation
readiness.

## Rollout
1. Freeze and test the shared event protocol.
2. Establish deterministic workflow contracts and planning.
3. Implement and audit the append-only event ledger.
4. Persist workflow execution state and failure semantics.
5. Build a separately permissioned executor.
6. Add one read-only adapter at a time.
7. Test failure, missing-data, permission, and replay behavior.
8. Add curated shared knowledge.
9. Only then consider controlled emergence studies.
