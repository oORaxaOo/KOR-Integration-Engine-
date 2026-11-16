# Architecture

The KOR Integration Engine (KIE) is a deterministic coordination layer for
independent engines. It does not replace their internal logic or databases.

## Components

- **Event protocol:** immutable, versioned envelopes with explicit producer,
  consumers, sensitivity, epistemic status, provenance, and bounded payloads.
- **Routing:** pure deterministic allowlist/expiry/hop-limit checks.
- **Event store:** append-only SQLite records with idempotency and
  tamper-evident hash-chain validation.
- **Workflow state manager:** durable run/step state and transition history.
- **Controlled executor:** one explicitly authorized step at a time; adapters
  are registered by the host, not discovered dynamically.
- **Engine adapters:** narrow boundaries that translate one engine's output
  into a KIE event. The first adapter is the read-only Evidence snapshot
  adapter.
- **Hermes host plugin:** an opt-in native plugin that exposes one asynchronous
  Evidence lookup tool, runs the one-step KIE workflow, and stores request and
  result events in the KIE database.

## Ownership boundaries

Each domain engine remains the owner of its internal state and persistence.
The KIE stores integration events and workflow state; it does not take
ownership of engine databases. An adapter may use only the public interface it
was written against. The host retains process lifecycle, credentials,
operating-system permissions, and execution-policy configuration.

Hermes is the runtime host, not a fifth domain engine. The event protocol has
one reserved host producer identity (`hermes_host`) for requests created by
trusted host-side plugin code. Event consumers remain restricted to the four
domain engines.

## Current integration status

The Evidence adapter, bounded stdio transport, and native Hermes plugin are
implemented as library/plugin components and covered by tests. They are not
installed or enabled on a user's machine by repository changes alone. The
operator must install the KIE package into Hermes' Python environment, copy
and enable the plugin, configure absolute local paths, and enable its toolset.
Other engines remain unconnected.

## Trust limits

An event's producer identity is provenance metadata, not cryptographic
authentication. Application allowlists do not replace host permissions.
SHA-256 hash chaining detects many accidental or unauthorized changes but is
not tamper-proof if an attacker controls the database and can recompute the
chain. Engine outputs are structured observations, not universal truth claims.
