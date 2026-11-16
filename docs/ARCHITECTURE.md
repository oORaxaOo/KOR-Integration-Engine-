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
  adapter; it calls only `get_task_report` over a host-injected transport.

## Ownership boundaries

Each engine remains the owner of its internal state and persistence. The KIE
stores integration events and workflow state; it does not take ownership of
engine databases. An adapter may use only the public interface it was written
against. The host retains process lifecycle, credentials, operating-system
permissions, and execution-policy configuration.

## Current integration status

The Evidence adapter is implemented as a library component and covered by
tests. It is not yet registered in Hermes, does not launch the Evidence JSON-RPC
bridge, and does not change WhatsApp or gateway configuration. A host must
explicitly supply a bounded transport, register the adapter, and authorize an
appropriate workflow. Other engines remain unconnected.

## Trust limits

An event's producer identity is provenance metadata, not cryptographic
authentication. Application allowlists do not replace host permissions.
SHA-256 hash chaining detects many accidental or unauthorized changes but is
not tamper-proof if an attacker controls the database and can recompute the
chain. Engine outputs are structured observations, not universal truth claims.
