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

See [Architecture](docs/ARCHITECTURE.md),
[Executor](docs/EXECUTOR.md),
[Evidence adapter](docs/EVIDENCE_ADAPTER.md), and
[Hermes integration](docs/HERMES_INTEGRATION.md).

## Safety boundaries

The KIE does not automatically discover plugins, import engines by name, run
shell commands, or open engine-owned databases. Adapters are explicitly
constructed by the host and constrained by a default-deny execution policy.
The Hermes plugin uses one fixed read-only Evidence RPC method over a bounded
stdio subprocess; paths and the interpreter are trusted operator settings, not
model-supplied arguments.

Hermes remains the runtime host, not a fifth domain engine. Host-originated
events use the reserved `hermes_host` producer identity. The four domain-engine
IDs and consumer allowlist remain unchanged.

## Development

Python 3.11+; no KIE runtime dependencies.

```bash
python -m pip install -e ".[dev]"
python -m pytest
```


## Engine event ingress

KIE optionally accepts privacy-bounded runtime events from the Critical Thinking, Reliability, and Emotion engines over a local Unix-domain socket. Each engine remains independent; KIE validates an explicit event allowlist and records the metadata in its own append-only event store. The interface never receives raw prompts, responses, tool arguments/results, or reflection text. Hermes can inspect the latest sanitized metadata through the read-only `kie_engine_events` tool when explicitly asked; no automatic polling or event dispatch occurs. See [Engine Event Ingress](docs/ENGINE_EVENT_INGRESS.md).


## Complete Hermes plugin pack

The pinned `hermes-pack.yaml` installs the complete five-plugin KOR stack in one
Hermes plugin-pack operation: Evidence Engine, Critical Thinking, Reliability,
Emotion, and KIE. The Evidence Engine remains an independent plugin; KIE is the
coordinator. See [Engine Stack Installation](docs/ENGINE_STACK_INSTALL.md).


## KIE health supervision

KIE registers the read-only `kie_system_health` tool and checks the active Hermes
plugin registry, the local event receiver, the Evidence bridge configuration,
and recent allowlisted operational failure events from all four engines. It
injects a deduplicated health alert into the next normal `pre_llm_call` context
when the observed state changes. This does not initiate another LLM call.

The observer does not continuously poll idle engines, automatically restart
plugins, modify engine code, or inspect their private databases. A missing event
alone is not treated as failure. The report distinguishes an Evidence database
that has not yet been initialized from an actual bridge/configuration failure.

See [Engine Stack Installation](docs/ENGINE_STACK_INSTALL.md) for the five-plugin
pack, seeded shared socket settings, Evidence policy setup, and live verification
procedure.
