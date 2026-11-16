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
KIE v0.x will not make LLM calls, execute arbitrary tools or shell commands,
read or write engine databases directly, silently promote hypotheses to facts,
rewrite engine policies or its own permissions, or claim consciousness from
emergent behavior. Emergence observation is not a foundation delivery gate.

## Coordination and state
A versioned workflow declares which engine steps belong to a task, which are
required, their dependencies, and timeouts. The planner creates a deterministic
availability-aware plan. The Workflow State Manager persists runs and records
step transitions. Required non-success outcomes fail the run; optional
non-success outcomes can produce PARTIAL.

Run states are PENDING, RUNNING, COMPLETED, PARTIAL, FAILED, BLOCKED, and
CANCELLED. Step states include PENDING, RUNNING, COMPLETED, SKIPPED,
UNAVAILABLE, NO_DATA, FAILED, TIMED_OUT, CANCELLED, and BLOCKED.

## Controlled executor
The executor runs one explicitly selected pending step at a time. It calls
only adapter objects registered by the host; it never imports modules by
name, scans plugin folders, runs shell commands, or reads engine databases.

A default-deny application policy must explicitly permit the workflow ID and
engine ID. Input events must be selected by the caller, must include the
target engine in their consumer allowlist, must not be expired, and must have
a sensitivity permitted by policy. The executor then passes those exact
records to the adapter.

A successful adapter result must contain an IntegrationEvent whose producer
matches the registered engine, whose task ID matches the workflow run, and
whose idempotency key identifies the exact execution attempt. The event is
persisted before the workflow step is marked complete. On recovery, a RUNNING
step is reconciled against that attempt key; if the event is already recorded,
the state is completed without calling the adapter again. If no result exists,
the executor refuses to launch the step again automatically.

The async timeout cancels the adapter coroutine cooperatively. It cannot kill
a blocked native operation or external process. A real adapter must enforce
hard process/network timeouts and make external operations idempotent. The
current executor has no background scheduler or whole-workflow daemon.

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

The event ledger is append-only. Workflow transition history is append-only;
run/step tables are mutable current-state projections updated transactionally.
Corrections are new linked events; existing event records are not rewritten.

## Persistence security
The SQLite ledger uses canonical envelopes, idempotency checks, and a
SHA-256-linked history. The hash chain is tamper-evident, not tamper-proof.
File-level access can bypass triggers and rewrite the database. The database
does not encrypt payloads or authenticate producers. File permissions,
encryption at rest, backup handling, and host authentication remain separate.

## Emergence observation
Emergence observation is deferred. A later observer may compare isolated
baselines with controlled integrated runs. It is not required for foundation
readiness.

## Rollout
1. Freeze and test the shared event protocol.
2. Establish deterministic workflow contracts and planning.
3. Implement and audit the append-only event ledger.
4. Persist workflow execution state and failure semantics.
5. Add the controlled adapter executor.
6. Add one read-only adapter at a time.
7. Test failures, missing data, permission boundaries, timeouts, and recovery.
8. Add curated shared knowledge.
9. Only then consider controlled emergence studies.
