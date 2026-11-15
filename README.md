# KOR Integration Engine

The KOR Integration Engine (KIE) is a deterministic coordination layer for
independent KOR engines. It coordinates workflows and controlled information
exchange without replacing engine autonomy.

## Current status

**Foundation v0.4 under development.** The repository contains the shared
event contract, deterministic routing and planning, an append-only SQLite
event ledger, and durable workflow run/step state with an append-only
transition history. Automated tests run in CI on Python 3.11, 3.12, and 3.13.
There is no live Hermes or WhatsApp integration.

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

## Repository map

- docs/ARCHITECTURE.md — system boundaries and rollout.
- docs/PROTOCOL.md — shared event contract.
- docs/WORKFLOWS.md — workflow planning and execution-state contract.
- docs/EVENT_STORE.md — persistence, idempotency, integrity, and security limits.
- schemas/integration-event-v1.schema.json — machine-readable event schema.
- src/kie/events.py — immutable event model and envelope serialization.
- src/kie/routing.py — pure routing policy.
- src/kie/workflows.py — workflow definitions and deterministic planner.
- src/kie/event_store.py — SQLite append-only event ledger.
- src/kie/workflow_store.py — durable workflow state manager.
- tests/ — contract, routing, schema, store, and workflow tests.

## Development

Python 3.11+; no runtime dependencies. SQLite is included in Python's standard
library.

    python -m pip install -e ".[dev]"
    python -m pytest

The event and workflow stores can share one SQLite file. Workflow transitions
are append-only, while current run/step rows are mutable projections maintained
by the store. The database does not encrypt payloads or provide host-level
authentication. Protect it with operating-system permissions and backups.

The state manager does not launch engines or make network calls. A caller or a
future executor must explicitly start steps, submit outcomes, schedule retries,
and poll for expired deadlines. Retrying is bounded and manual in this phase.
There is no background scheduler, automatic retry loop, or live Hermes/WhatsApp
integration.
