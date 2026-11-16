# Hermes integration — KIE v0.8

## Evidence

The native Hermes Agent plugin registers the asynchronous read-only tool
`kie_evidence_lookup(evidence_task_id)`. It uses a bounded stdio transport
and a fixed Evidence RPC method. See [Evidence adapter](EVIDENCE_ADAPTER.md)
for the full boundary and installation settings.

## Other three engines

Critical Thinking, Reliability, and Emotion are live Hermes plugins with
different lifecycle roles. v0.8 adds narrow publication APIs in
`kie.publisher` so each engine can report a minimized result into KIE's own
event ledger. These APIs do not call the engines, read their databases, or
change their internal state.

The operator must:

1. Install KIE into the Python environment used by Hermes.
2. Configure the same absolute `kie_database` path in each of the three
   engine plugin settings.
3. Enable the publisher wiring in each engine release that supports it.
4. Keep the engines' own enable/disable settings independent from KIE.

Engine integrations are opt-in and fail-open. If the KIE package is not
installed or the database cannot be written, the engine must continue its
normal operation and log only a bounded technical warning.

## Event types

- `critical_thinking.review_result`
- `reliability.runtime_snapshot`
- `emotion.reflection_summary`

KIE records only the event metadata described in
[Engine event publishing](ENGINE_EVENT_PUBLISHING.md). It never receives raw
conversation content, tool arguments, private exception messages, reflection
narratives, or emotional-memory contents through these contracts.

## Status inspection

The Hermes plugin exposes `kie_engine_status`, a read-only view of the latest
published summary for each of the three engines. It does not execute a new
review, trigger a reflection, or change engine state.
