# Engine event publishing

## Direction

Evidence remains a pull-style, read-only adapter: KIE asks Evidence for a
bounded task snapshot through its fixed JSON-RPC method.

Critical Thinking, Reliability, and Emotion are live Hermes plugins. Their
natural integration direction is push: each plugin publishes a small,
engine-owned result summary to the KIE event ledger at the point where that
result exists. This avoids trying to reconstruct live plugin state in a
separate subprocess.

The engines remain independent. They do not share databases, and the KIE
publisher does not import engine packages or discover plugins. Each engine
uses one explicit KIE publication function only when the operator configures
the shared KIE database path.

## Public publisher contracts

The package exposes three narrow functions from `kie.publisher`:

- `publish_critical_thinking_review(...)`
- `publish_reliability_snapshot(...)`
- `publish_emotion_reflection(...)`

Each function validates input before opening the KIE database and writes one
versioned `IntegrationEvent` with the real engine as producer.

| Producer | Event type | What is transferred |
|---|---|---|
| `critical_thinking` | `critical_thinking.review_result` | Intent, review level, fixed reason code, bounded signal labels, and whether review context was injected |
| `reliability` | `reliability.runtime_snapshot` | Aggregate counters, selected latency/context metrics, and allowlisted anomaly counts |
| `emotion` | `emotion.reflection_summary` | Reflection status, step/assessment counts, evidence-reference count, and whether a next question exists |

## Data excluded by contract

- Critical Thinking: original user message, prompt text, generated review context.
- Reliability: tool names, exception types/messages, prompts, responses, model labels, and tool arguments.
- Emotion: response text, findings, self-challenges, causal-hypothesis text, next-question text, evidence contents, emotional labels, and persistent emotional-memory records.

Emotion events are always marked `hypothesis`, not `verified`. The metadata
does not establish subjective experience or human-like feelings.

## Failure behavior

Publication is synchronous local SQLite I/O. Each engine integration must
wrap the optional publisher call in a fail-open error handler: a missing KIE
configuration, absent KIE package, or write failure must not block the
engine's own operation. The publisher itself raises validation and storage
errors rather than silently reporting success.

Do not call the publisher for every internal sub-step. CTE publishes only
when it actually chooses a review outcome; Reliability should publish a
bounded aggregate snapshot at a meaningful lifecycle boundary (for example,
session end); Emotion publishes only after an explicit reflection action.

## Configuration boundary

The KIE package must be installed in the same Python environment used by
Hermes. Each engine plugin receives only the absolute `kie_database` path
through its own settings. The file remains owned by KIE and protected by
host filesystem permissions. No engine receives a generic SQL writer API.
