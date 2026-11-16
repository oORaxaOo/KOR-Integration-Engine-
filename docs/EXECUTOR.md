# KIE Controlled Executor — v0.5

## Scope
The executor coordinates one explicitly selected workflow step. It is not a
background worker, whole-workflow daemon, plugin scanner, process supervisor,
or security sandbox. It does not load engine modules by name or execute shell
commands.

## Adapter registration
The host constructs adapter objects and passes them into AdapterRegistry.
Each adapter declares one fixed engine_id and provides an async execute(request)
method. The registry checks that object identity matches its key. No dynamic
imports, filesystem discovery, or automatic plugin activation occur.

## Authorization
WorkflowExecutor requires an explicit policy object exposing authorize(request).
ExecutionPolicy is a default-deny allowlist requiring both a workflow ID and an
engine ID. When input events are supplied, their sensitivity must also be
explicitly allowed. A host may supply a stricter policy implementation.

This application-level allowlist does not authenticate the host process or
protect policy configuration from local file access. Those controls belong to
the deployment environment. Likewise, producer_engine is provenance metadata:
matching it to the registered adapter is a consistency check, not cryptographic
proof of which process produced the event.

## Input event handling
The caller supplies exact input event IDs. The executor rejects duplicates,
missing records, expired records, and events whose allowed_consumers do not
include the step's target engine. Only those selected records are passed to
the adapter. Sensitivity and workflow/engine grants are then checked by policy.

The executor does not automatically forward all events in a task or expose
the event ledger wholesale.

## Output contract
An adapter returns ExecutionResult:
- COMPLETED: requires an IntegrationEvent.
- NO_DATA: requires a reason and no event.
- UNAVAILABLE: requires a reason and no event.
- FAILED: requires a reason and no event.

A successful result event must:
- name the step's engine as producer;
- set task_id to the workflow run UUID string;
- use an attempt-scoped idempotency key of the form:
  kie:<run-id>:<step-id>:<attempt>.

The result event is appended to SQLiteEventStore before the workflow step is
marked COMPLETED and linked to its event ID. Other outcomes are written to the
workflow state manager and cannot be mistaken for successful evidence.

## Crash recovery
There is an unavoidable boundary between the engine's work and the KIE's
database transaction. The executor therefore does not promise distributed
exactly-once execution.

For every attempt, the adapter receives a stable idempotency key. If the host
crashes after the result event is appended but before the step is marked
complete, the next call finds that attempt's event in the ledger and records
the completion without calling the adapter again.

If the step is still RUNNING and no matching result exists, the executor
raises ExecutionAlreadyRunning. It never assumes the engine stopped after a
host crash. A host must inspect the actual adapter/process state and reconcile
or explicitly time out/retry the step through the workflow state manager.

## Timeouts and cancellation
The executor uses asyncio.wait_for with the step's declared timeout. This
cancels the adapter coroutine cooperatively. It cannot forcibly terminate a
blocked native call or external process, and an adapter that suppresses
cancellation may outlive the nominal timeout. Adapters that cross process or
network boundaries must enforce their own hard limits.

No background timeout sweeper is started. The host explicitly calls the
workflow store's timeout polling method. The method can be scoped to one run;
the executor scopes its timeout handling to the run it is currently executing.
Caller cancellation leaves the step RUNNING for reconciliation because
cancellation is not proof that remote work stopped.

## Deliberate limits
- No live Hermes or WhatsApp registration.
- No direct access to engine-owned databases.
- No LLM calls or model-driven authorization.
- No automatic retries or background polling.
- No distributed transaction across an adapter and SQLite.
- No guarantee of exactly-once external side effects.
