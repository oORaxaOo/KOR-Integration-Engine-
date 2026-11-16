# KOR Integration Engine

The KOR Integration Engine (KIE) is a deterministic coordination layer for
independent KOR engines. It coordinates workflows and controlled information
exchange without replacing engine autonomy.

## Current status

**Foundation v0.5 under development.** The repository contains the shared
event contract, deterministic planning, an append-only SQLite event ledger,
durable workflow run/step state, and a controlled single-step async executor.
Automated tests run in CI on Python 3.11, 3.12, and 3.13. There is no live
Hermes or WhatsApp integration.

## Design principles

- Independent engines retain their own logic and memory.
- KIE routes explicit, authorized event envelopes; it does not access engine
  databases directly.
- Missing, skipped, unavailable, failed, timed out, and blocked are distinct.
- Hypotheses never become facts through repetition.
- Verification is scoped to explicit criteria and evidence.
- No LLM dependency is required for the KIE foundation.
- Planning is deterministic and separate from execution.
- Event and transition histories are append-only; corrections are new records.
- Engine adapters are explicitly registered; no arbitrary dynamic imports.

## Repository map

- docs/ARCHITECTURE.md — system boundaries and rollout.
- docs/PROTOCOL.md — shared event contract.
- docs/WORKFLOWS.md — workflow planning and state contract.
- docs/EVENT_STORE.md — event persistence, idempotency, integrity, and limits.
- docs/EXECUTOR.md — adapter, authorization, dispatch, and recovery contract.
- schemas/integration-event-v1.schema.json — machine-readable event schema.
- src/kie/events.py — immutable event model and envelope serialization.
- src/kie/routing.py — pure routing policy.
- src/kie/workflows.py — workflow definitions and deterministic planner.
- src/kie/event_store.py — SQLite append-only event ledger.
- src/kie/workflow_store.py — durable workflow state manager.
- src/kie/executor.py — controlled async adapter executor.
- tests/ — contract, routing, schema, persistence, and executor tests.

## Development

Python 3.11+; no runtime dependencies. SQLite is included in Python's standard
library.

    python -m pip install -e ".[dev]"
    python -m pytest

The executor defaults to no grants: a host must explicitly register adapters
and provide an allowlist policy for workflow IDs, engine IDs, and input-event
sensitivity. It passes only caller-selected event IDs to an adapter and checks
the event consumer allowlist and expiry before dispatch.

A completed adapter response must include an event produced by the engine
assigned to that step, linked to the workflow run and attempt through its
task ID and idempotency key. A recorded result can be reconciled after a crash
without calling the adapter a second time.

The executor's async timeout uses cooperative cancellation. It does not kill
a blocked native call or external process; adapters must apply hard limits at
their own process/network boundary. A RUNNING step with no recorded result is
never launched a second time automatically.

The event and workflow databases do not encrypt payloads or authenticate
event producers. Protect database files with operating-system permissions
and backups. The KIE is not yet a production security boundary or a live
Hermes/WhatsApp integration.
