# KIE Event Protocol v1

## Envelope

Every event carries a UUID event ID, schema version, event type, producer
engine and version, timezone-aware occurrence and recording timestamps,
epistemic status, sensitivity, explicit consumer allowlist, bounded JSON
object payload, evidence references, parent event IDs, optional session/task/
correlation IDs, optional expiry, hop count, idempotency key, and optional
scoped verification record.

The machine-readable envelope is schemas/integration-event-v1.schema.json.
The Python event model enforces a 64 KiB UTF-8 payload limit and recursively
freezes payload dictionaries and arrays. event_to_dict exports a detached
JSON-compatible representation; event_from_dict restores and validates it.

The producer field is provenance metadata, not proof of producer identity.
The host adapter must authenticate the submitting process.

## Epistemic rules

- observed: a source reported an observation; interpretation is not proven.
- hypothesis: tentative and remains tentative regardless of repetition.
- under_review: a review is in progress.
- verified/refuted: requires verifier, criteria, evidence references,
  timestamp, and scope.
- unresolved: available information is insufficient.
- Verification applies only to its declared scope and criteria.
- Status changes are new linked events, never silent mutation.

## Routing

Events route only to consumers in their allowlist. The producer cannot be its
own consumer. Expired events and events beyond the configured hop limit are
rejected. The router returns decisions; it does not execute engines or
authenticate callers.

## Persistence and replay

The SQLite event ledger serializes the canonical JSON envelope and appends it
inside a write transaction. Event IDs are globally unique. Idempotency keys
are unique per producer engine; a retry with the same key and matching event
content (excluding generated event ID and recording timestamp) returns the
original record. Reusing a key for different content is rejected.

Historical reads are ordered by the ledger sequence. The replay API is a
read-only history reader; it never dispatches events or re-executes engine
actions. The hash chain and envelope digests support integrity auditing but
are not authentication or tamper-proof storage.

## Privacy

Payloads contain the minimum information needed. Raw transcripts, credentials,
and sensitive personal data are not shared by default. The SQLite ledger does
not encrypt payloads. Database file permissions, encryption at rest, backup
protection, and host authorization must be handled separately.
