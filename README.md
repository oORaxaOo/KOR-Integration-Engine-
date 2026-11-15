# KOR Integration Engine

The KOR Integration Engine (KIE) is a deterministic coordination layer for
independent KOR engines. It coordinates workflows and controlled information
exchange without replacing engine autonomy.

## Current status
**Foundation v0.2 under development.** The repository now contains the shared
event contract, deterministic routing, a workflow-definition model, and a
side-effect-free workflow planner. Automated tests run in CI on Python 3.11,
3.12, and 3.13. There is no live Hermes or WhatsApp integration.

## Design principles
- Independent engines retain their own logic and memory.
- KIE routes explicit, authorized event envelopes; it does not access engine
  databases directly.
- Missing, skipped, unavailable, and failed are different states.
- Hypotheses never become facts through repetition.
- Verification is scoped to explicit criteria and evidence.
- No LLM dependency is required for the KIE foundation.
- Planning is deterministic and separate from execution.

## Repository map
- docs/ARCHITECTURE.md — system boundaries and rollout.
- docs/PROTOCOL.md — shared event contract.
- docs/WORKFLOWS.md — workflow planning contract.
- schemas/integration-event-v1.schema.json — machine-readable event schema.
- src/kie/events.py — deeply immutable event and verification models.
- src/kie/routing.py — pure routing policy.
- src/kie/workflows.py — workflow definitions and deterministic planner.
- tests/ — contract, routing, schema, and workflow tests.

## Development
Python 3.11+; no runtime dependencies.

    python -m pip install -e ".[dev]"
    python -m pytest

The planner does not launch engines, perform retries, enforce runtime
cancellation, or persist state. The KIE is not yet a complete workflow
executor, persistent event bus, or production security boundary.
