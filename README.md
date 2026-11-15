# KOR Integration Engine

The KOR Integration Engine (KIE) is a deterministic coordination layer for
independent KOR engines. It coordinates workflows and controlled information
exchange without replacing engine autonomy.

## Current status
**v0.1 foundation in development.** Initial scope: shared event contract,
provenance and epistemic status rules, deterministic routing, and tests.
There is no live Hermes or WhatsApp integration.

## Design principles
- Independent engines retain their own logic and memory.
- KIE routes explicit, authorized event envelopes; it does not access engine
  databases directly.
- Missing, skipped, unavailable, and failed are different states.
- Hypotheses never become facts through repetition.
- Verification is scoped to explicit criteria and evidence.
- No LLM dependency is required for the KIE foundation.

## Repository map
- docs/ARCHITECTURE.md — system boundaries and rollout.
- docs/PROTOCOL.md — shared event contract.
- src/kie/events.py — immutable event and verification models.
- src/kie/routing.py — pure routing policy.
- tests/ — contract and routing tests.

## Development
Python 3.11+; no runtime dependencies.

    python -m pip install -e ".[dev]"
    pytest

This is a contract foundation, not yet a complete workflow manager,
persistent event bus, or production security boundary.
