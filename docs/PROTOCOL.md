# KIE Event Protocol v1

## Envelope
Every event carries a UUID event ID, schema version, event type, producer
engine and version, timezone-aware occurrence and recording timestamps,
epistemic status, sensitivity, explicit consumer allowlist, bounded JSON
payload, evidence references, parent event IDs, optional session/task/
correlation IDs, optional expiry, hop count, idempotency key, and optional
scoped verification record.

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

## Privacy
Payloads contain the minimum information needed. Raw transcripts and
sensitive personal data are not shared by default. Restricted events require
an explicit policy and adapter authorization; a field value alone is not
access control.
