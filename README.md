# KOR Integration Engine

The KOR Integration Engine (KIE) is a deterministic coordination layer for
independent KOR engines. It coordinates workflows and controlled information
exchange without replacing engine autonomy.

## Current status

**Foundation v0.3 under development.** The repository contains the shared
event contract, deterministic routing and workflow planning, and an append-only
SQLite event ledger with deduplication and integrity verification. Automated
tests run in CI on Python 3.11, 3.12, and 3.13. There is no live Hermes or
WhatsApp integration.

## Design principles

- Independent engines retain their own logic and memory.
- KIE routes explicit, authorized event envelopes; it does not access engine
  databases directly.
- Missing, skipped, unavailable, and failed are different states.
- Hypotheses never become facts through repetition.
- Verification is scoped to explicit criteria and evidence.
- No LLM dependency is required for the KIE foundation.
- Planning is deterministic and separate from execution.
- The event ledger is append-only; corrections are new linked events.

## Repository map

- docs/ARCHITECTURE.md — system boundaries and rollout.
- docs/PROTOCOL.md — shared event contract.
- docs/WORKFLOWS.md — workflow planning contract.
- docs/EVENT_STORE.md — persistence, idempotency, integrity, and security limits.
- schemas/integration-event-v1.schema.json — machine-readable event schema.
- src/kie/events.py — immutable event model and envelope serialization.
- src/kie/routing.py — pure routing policy.
- src/kie/workflows.py — workflow definitions and deterministic planner.
- src/kie/event_store.py — SQLite append-only event ledger.
- tests/ — contract, routing, schema, store, and workflow tests.

## Development

Python 3.11+; no runtime dependencies. SQLite is included in Python's standard
library.

    python -m pip install -e ".[dev]"
    python -m pytest

The ledger does not encrypt event payloads, replace operating-system file
permissions, or protect against an attacker who can rewrite the database and
its integrity metadata. Do not store raw conversations, credentials, or
unnecessary sensitive data. Backups and host-level access controls remain
separate operational responsibilities.

The workflow planner does not launch engines, perform retries, enforce runtime
cancellation, or persist workflow execution state. Historical replay is
read-only and never dispatches events. KIE is not yet a complete workflow
executor or production security boundary.
