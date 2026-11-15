# KIE SQLite Event Store — v0.3

## Responsibilities

The event store provides a local durable history of explicitly submitted KIE
events. It does not own engine-internal memory, collect transcripts, invoke
engines, run workflows, or decide whether an event is true.

## Append behavior

- A new event is written in one SQLite write transaction.
- Each accepted event receives a monotonically increasing sequence number.
- The full versioned event envelope is stored as canonical JSON.
- Event IDs are globally unique.
- An identical event ID and envelope returns the original append result.
- Reusing an event ID with different content raises EventConflictError.
- Idempotency keys are unique per producer engine.
- A retry with the same producer/key and matching content returns the original
  record. The fingerprint ignores event_id and recorded_at, because those may
  be generated again for a retry; all other envelope content must match.
- Reusing a producer/key with different content raises
  IdempotencyConflictError.
- The write transaction serializes competing SQLite writers before checking
  duplicates and assigning the next sequence.

## Append-only and integrity

Ordinary UPDATE and DELETE statements are rejected by SQLite triggers.
Each ledger row stores an envelope SHA-256 digest, previous-row hash, entry
hash, and storage timestamp. verify_integrity() checks SQLite structural
integrity, sequence continuity, envelope digest, parsed event metadata,
previous hash, and entry hash.

This makes accidental or partial modification detectable. It does not make
the database tamper-proof. A person with file-level control can remove
triggers and rewrite the database. An attacker who truncates only the final
records cannot be detected without a trusted external hash anchor or backup.
The ledger does not authenticate the producer; producer authentication must
happen at the adapter boundary.

## Reads and replay

get_event() reads one event by UUID. read_events() returns a bounded page
in sequence order and supports producer, event-type, task, and correlation
filters. replay() is a read-only alias for ordered history reading. It never
re-runs engines or dispatches actions.

A future executor must treat a replayed record as historical input, not as
permission to repeat the action that originally produced it.

## Operational security

Event payloads are stored as plaintext JSON. The event model limits payload
size but does not encrypt it or remove sensitive values. Do not submit raw
WhatsApp conversations, credentials, access tokens, or unnecessary personal
data. Protect the database path with operating-system permissions and backups.
If encryption at rest is required, provide it at the storage/host layer before
using this ledger with restricted data.

For file-backed databases, SQLite WAL mode may create -wal and -shm sidecar
files. Backup procedures must use SQLite's online backup facility or ensure a
consistent checkpoint; do not assume copying only the main database file
while it is live captures every committed transaction.

## Schema and compatibility

The database records schema version 1 using SQLite user_version. A database
created by a newer schema is rejected rather than silently downgraded. Future
schema changes require an explicit migration and backup/rollback plan.
