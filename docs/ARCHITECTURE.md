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
required, their dependencies, and timeouts. The current workflow component
validates definitions and creates a side-effect-free deterministic plan; it
does not execute a step or persist workflow state.

Engine runtime states remain distinct:
- PENDING: not yet considered;
- RUNNING: execution requested;
- COMPLETED: valid result returned;
- SKIPPED: workflow says the engine is not needed;
- UNAVAILABLE: required engine could not be reached;
- NO_DATA: engine responded but had no usable data;
- FAILED: execution failed.

A workflow may complete with optional engines skipped. It must not report
complete when a required engine is unavailable, failed, or lacks a required
result. Missing information remains missing.

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
transcript or a replacement for engine memory. It stores only explicitly
submitted event envelopes. Corrections are new linked events; existing
records are not rewritten. The separate curated knowledge store will require
explicit review and retain source references.

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

Historical replay is read-only: it returns events in ledger order and never
re-invokes engines. Event dispatch and retries belong to a later executor.

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
4. Define durable workflow execution state and failure semantics.
5. Add one read-only adapter at a time.
6. Test failure, missing-data, permission, and replay behavior.
7. Add curated shared knowledge.
8. Only then consider controlled emergence studies.
