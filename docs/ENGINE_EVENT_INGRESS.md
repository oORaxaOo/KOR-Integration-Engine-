# KIE Engine Event Ingress

KIE can receive bounded operational events from the Critical Thinking, Reliability,
and Emotion engines over a local Unix-domain socket. This is a push interface:
each engine reports only a small result that it already owns. KIE does not import
the other engines, open their databases, or request their private state.

## Configuration

Add the same absolute socket path to the KIE plugin and to each engine plugin
that should publish events:

\`\`\`yaml
kie_event_socket: /home/USER/.hermes/run/kie-engine-events.sock
\`\`\`

The KIE plugin additionally needs its existing \`kie_database\` setting and this
setting:

\`\`\`yaml
engine_event_socket: /home/USER/.hermes/run/kie-engine-events.sock
\`\`\`

The receiver starts only when both KIE settings are present. Engine publishers
are disabled when \`kie_event_socket\` is absent. The receiver creates the parent
directory and binds a Unix socket with mode \`0600\`.

## Accepted event contracts

| Producer | Event | Accepted data |
|---|---|---|
| Critical Thinking | \`critical_thinking.review_routed\` | Intent class, review level, explicit-request flag, bounded signal identifiers |
| Reliability | \`reliability.runtime_observation\` | Allowlisted mechanical event kind, count, optional allowlisted anomaly codes |
| Reliability | \`reliability.runtime_snapshot\` | Aggregate API/tool counters, context pressure band, model-turn and explicit-label counts |
| Emotion | \`emotion.reflection_completed\` | Reflection status, step count, hypothesis/limitation counts, follow-up-present flag |

Unknown event types, unknown fields, invalid enum values, inconsistent counters,
and messages over 64 KiB are rejected. KIE timestamps accepted events itself and
sets their epistemic status to \`observed\`. No engine can submit an event as
verified through this interface.

## Privacy boundaries

- Critical Thinking sends no original user message and no generated review text.
- Reliability sends no prompts, model responses, tool arguments/results, tool names,
  or raw exception messages.
- Emotion sends no reflection findings, hypotheses, questions, assistant response,
  user-provided evidence, or session identifier. Reflection text remains inside
  the Emotion Engine.
- Emotion metadata is marked \`restricted\`; the other event classes are \`internal\`.
- Events are written only to the KIE event store. The engines retain their own
  databases and state ownership.

## Local security model

The socket is local IPC, not a network listener. Its filesystem permissions limit
connection to the owning operating-system user. This is not cryptographic publisher
authentication: another process running as that same user could submit a message.
The \`producer_engine\` field is provenance metadata, not proof of identity. Do not
place this socket in a shared directory or use it across mutually untrusted users.

The receiver fails closed on contract violations and hides database and process
details from publishers. Engine publishers are designed to fail open: an unavailable
KIE socket must not stop an engine's primary Hermes hook or tool.

This interface records observations; it does not prove correctness, emotion, intent,
or consciousness, and it does not automatically trigger other engines. Hermes can inspect the
stored operational metadata through `kie_engine_events`; this is read-only and
does not cause the producer or another engine to run.
