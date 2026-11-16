# Evidence Engine read-only adapter (v0.6)

## Purpose

The first concrete KIE engine adapter is a read-only snapshot client for the
Evidence Engine. It uses the existing JSON-RPC 2.0 method
`get_task_report`. The method returns one consistent task/evidence/verification
snapshot, including the effective status, freshness of verification, and
integrity of the retained evidence chain.

The adapter does not decide whether arbitrary prose is true. It only carries
forward the Evidence Engine's own structured result and its explicit limits.

## Request contract

The adapter accepts exactly one caller-selected KIE input event:

- `event_type`: `evidence.lookup_request`
- `payload.evidence_task_id`: canonical UUID of an existing Evidence Engine task
- the event's `allowed_consumers` must include `evidence` (enforced by KIE's
  executor before the adapter is called)

No task ID is discovered from free text. No other payload field can select a
method, database, path, command, or policy.

## RPC boundary

The adapter calls exactly one fixed method: `get_task_report`, with the
fixed parameters `task_id`, `evidence_limit`, and `evidence_offset=0`.
It accepts only a matching JSON-RPC 2.0 response ID.

The host injects an `EvidenceRpcTransport`. The adapter does not start a
subprocess, open a socket, open a database, or import Evidence Engine modules.
A deployment may connect the transport to the existing stdio bridge, but the
host owns process lifecycle, the database path, operating-system permissions,
and response-size limits. The transport must return the complete JSON-RPC
response envelope and enforce a bounded response size.

## Data minimization

Only a bounded status summary crosses into the KIE event ledger:

- task status and effective status;
- ledger-integrity and verification-current/record-valid flags;
- recognized reliability note;
- sequence and evidence-count metadata;
- references (IDs) to a bounded page of evidence and the verification record.

Task titles, success criteria, raw evidence payloads, command output, and
verification check details are deliberately excluded. They remain owned by the
Evidence Engine and should be queried there only by an authorized host-side
reader when needed.

## Epistemic mapping

- `VERIFIED` requires effective/task status `passed`, a `supported`
  verification, a consistent verification record at the latest evidence
  sequence, and a valid ledger hash chain.
- `REFUTED` requires effective/task status `failed`, a current consistent
  verification with status `contradicted` or `invalid`, and a valid ledger
  hash chain.
- Stale, inconsistent, unknown, or integrity-failed states remain
  `UNRESOLVED`; they are never promoted to success.
- Planned/running tasks are `OBSERVED`.

The event's `Verification` object records the verification ID, task scope,
and timestamp. This is provenance from the Evidence Engine; it is not
cryptographic authentication of the host process.

## Failure behavior

- No selected request event: `NO_DATA`.
- Unknown Evidence task: `NO_DATA`.
- Transport unavailable or read-only method unavailable: `UNAVAILABLE`.
- Invalid request, malformed response, or invalid report: `FAILED`.

Error messages are reduced to fixed safe reason codes. Raw transport error text,
database paths, and underlying evidence payloads are not copied into workflow
state or KIE events.

## Activation boundary

This adapter is a library component only. It is not registered in Hermes, does
not launch the Evidence bridge, does not change WhatsApp, and is not active
until a trusted host explicitly constructs the transport, registers the
adapter, and grants the workflow/engine in the execution policy.
