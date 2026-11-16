# KOR Integration Engine

A deterministic coordination layer for independent KOR engines. The KIE
provides versioned event contracts, routing, durable event/workflow stores,
and controlled step execution without requiring an LLM.

## Current state

- v0.1–v0.5: event protocol, deterministic routing, SQLite event store,
  workflow state manager, controlled executor.
- v0.6: first host-registered adapter, a read-only Evidence Engine snapshot
  client using its fixed `get_task_report` JSON-RPC method. It is not a live
  Hermes or WhatsApp integration.

See [Architecture](docs/ARCHITECTURE.md),
[Executor](docs/EXECUTOR.md), and
[Evidence adapter](docs/EVIDENCE_ADAPTER.md).

## Safety boundaries

The KIE does not automatically discover plugins, import engines by name, run
shell commands, or open engine-owned databases. Adapters are explicitly
constructed by the host and constrained by a default-deny execution policy.
The policy is an application-level control, not host authentication.

The Evidence adapter accepts one explicit lookup event and returns a minimized
summary. It does not transfer task titles, criteria, raw evidence payloads,
or command output into the KIE event ledger. Integrity checks are
tamper-evident, not tamper-proof.

## Development

Python 3.11+; no runtime dependencies.

```bash
python -m pip install -e ".[dev]"
python -m pytest
```
