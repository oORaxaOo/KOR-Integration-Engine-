# KOR Integration Engine

A deterministic coordination layer for independent KOR engines. The KIE
provides versioned event contracts, routing, durable event/workflow stores,
and controlled step execution without requiring an LLM.

## Current state

- v0.1–v0.5: event protocol, deterministic routing, SQLite event store,
  workflow state manager, controlled executor.
- v0.6: read-only Evidence Engine snapshot adapter using its fixed
  `get_task_report` JSON-RPC method.
- v0.7: bounded stdio transport and an opt-in Hermes Agent plugin that runs
  the Evidence adapter through a one-step KIE workflow.
- v0.8: optional, bounded engine-event publisher contracts for Critical
  Thinking, Reliability, and Emotion. These let each live engine publish
  minimized result metadata into the KIE event ledger without transferring
  conversation text, tool arguments, reflection narratives, or emotional
  memories. Engine-side publisher wiring is a separate opt-in change.

See [Architecture](docs/ARCHITECTURE.md),
[Executor](docs/EXECUTOR.md),
[Evidence adapter](docs/EVIDENCE_ADAPTER.md),
[Engine event publishing](docs/ENGINE_EVENT_PUBLISHING.md), and
[Hermes integration](docs/HERMES_INTEGRATION.md).

## Safety boundaries

The KIE does not automatically discover plugins, import engines by name, run
shell commands, or open engine-owned databases. Adapters are explicitly
constructed by the host and constrained by a default-deny execution policy.
The Hermes Evidence plugin uses one fixed read-only RPC method over a bounded
stdio subprocess; paths and the interpreter are trusted operator settings,
not model-supplied arguments.

The three new publisher APIs are explicit, allowlisted write surfaces. Each
accepts only a narrow engine-specific summary. They do not accept generic
event envelopes from plugin callers and do not change ownership of any
engine database. KIE remains the owner of its own event ledger.

Hermes remains the runtime host, not a fifth domain engine. Host-originated
events use the reserved `hermes_host` producer identity. The four domain-engine
IDs and consumer allowlist remain unchanged.

## Development

Python 3.11+; no KIE runtime dependencies.

```bash
python -m pip install -e ".[dev]"
python -m pytest
```
