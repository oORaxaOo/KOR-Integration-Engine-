"""Read-only bridge from Evidence Engine reports into KIE events.

This adapter never opens an Evidence Engine database and never launches a
process. The host injects a transport connected to the existing read-only
JSON-RPC bridge. Only get_task_report is called.
"""
from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime, timezone
import uuid
from typing import Any, Protocol

from ..events import (
    EpistemicStatus,
    IntegrationEvent,
    Sensitivity,
    Verification,
)
from ..executor import ExecutionOutcome, ExecutionRequest, ExecutionResult


class EvidenceRpcTransport(Protocol):
    """Host-owned JSON-RPC transport; it must enforce bounded responses."""

    async def call(
        self,
        method: str,
        params: Mapping[str, Any],
        *,
        request_id: str,
    ) -> Mapping[str, Any]:
        """Return the complete JSON-RPC response envelope for one request."""


class EvidenceReadOnlyAdapter:
    """Convert one Evidence task report into a small, bounded KIE event."""

    engine_id = "evidence"
    producer_version = "0.1.0"
    request_event_type = "evidence.lookup_request"
    rpc_method = "get_task_report"

    _TASK_STATUSES = frozenset({"planned", "running", "passed", "failed", "unknown"})
    _VERIFICATION_STATUSES = frozenset(
        {"supported", "contradicted", "inconclusive", "invalid"}
    )
    _RELIABILITY_NOTES = frozenset(
        {
            "evidence_hash_chain_invalid",
            "verification_record_inconsistent",
            "task_status_mismatches_verification",
            "verification_is_stale",
        }
    )

    def __init__(
        self,
        transport: EvidenceRpcTransport,
        *,
        evidence_limit: int = 10,
    ) -> None:
        if not callable(getattr(transport, "call", None)):
            raise TypeError("transport must provide async call(method, params, request_id=...)")
        if (
            isinstance(evidence_limit, bool)
            or not isinstance(evidence_limit, int)
            or not 1 <= evidence_limit <= 100
        ):
            raise ValueError("evidence_limit must be an integer from 1 to 100")
        self._transport = transport
        self.evidence_limit = evidence_limit

    @staticmethod
    def _non_negative_int(value: Any, field: str) -> int:
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise ValueError(f"{field} must be a non-negative integer")
        return value

    @staticmethod
    def _aware_timestamp(value: Any, field: str) -> datetime:
        if not isinstance(value, str):
            raise ValueError(f"{field} must be an ISO-8601 timestamp")
        try:
            parsed = datetime.fromisoformat(value)
        except ValueError as exc:
            raise ValueError(f"{field} must be an ISO-8601 timestamp") from exc
        if parsed.tzinfo is None or parsed.utcoffset() is None:
            raise ValueError(f"{field} must include a timezone")
        return parsed

    def _parse_report(self, report: Any, expected_task_id: str) -> dict[str, Any]:
        if not isinstance(report, Mapping):
            raise ValueError("report must be an object")

        task = report.get("task")
        if not isinstance(task, Mapping):
            raise ValueError("task must be an object")
        if task.get("task_id") != expected_task_id:
            raise ValueError("task identity does not match lookup request")

        task_status = task.get("status")
        effective_status = report.get("effective_status")
        if task_status not in self._TASK_STATUSES:
            raise ValueError("task status is invalid")
        if effective_status not in self._TASK_STATUSES:
            raise ValueError("effective status is invalid")

        ledger_integrity = report.get("ledger_integrity")
        verification_current = report.get("verification_current")
        verification_record_valid = report.get("verification_record_valid")
        evidence_truncated = report.get("evidence_truncated")
        for field, value in (
            ("ledger_integrity", ledger_integrity),
            ("verification_current", verification_current),
            ("verification_record_valid", verification_record_valid),
            ("evidence_truncated", evidence_truncated),
        ):
            if not isinstance(value, bool):
                raise ValueError(f"{field} must be a boolean")

        latest_sequence = self._non_negative_int(
            report.get("latest_evidence_sequence"), "latest_evidence_sequence"
        )
        evidence_total = self._non_negative_int(report.get("evidence_total"), "evidence_total")
        evidence_offset = self._non_negative_int(report.get("evidence_offset"), "evidence_offset")
        evidence_page_limit = self._non_negative_int(
            report.get("evidence_limit"), "evidence_limit"
        )
        if evidence_offset != 0 or evidence_page_limit != self.evidence_limit:
            raise ValueError("evidence page does not match the requested snapshot")
        if evidence_total < 0:
            raise ValueError("evidence_total is invalid")

        evidence_rows = report.get("evidence")
        if not isinstance(evidence_rows, list) or len(evidence_rows) > self.evidence_limit:
            raise ValueError("evidence page is invalid")
        evidence_ids: list[str] = []
        for row in evidence_rows:
            if not isinstance(row, Mapping):
                raise ValueError("evidence record must be an object")
            evidence_id = row.get("evidence_id")
            if not isinstance(evidence_id, str):
                raise ValueError("evidence_id must be a UUID")
            try:
                normalized_id = str(uuid.UUID(evidence_id))
            except ValueError as exc:
                raise ValueError("evidence_id must be a UUID") from exc
            if normalized_id != evidence_id:
                raise ValueError("evidence_id must use canonical UUID form")
            self._non_negative_int(row.get("sequence"), "evidence sequence")
            evidence_ids.append(evidence_id)

        verification = report.get("verification")
        parsed_verification: dict[str, Any] | None = None
        if verification is not None:
            if not isinstance(verification, Mapping):
                raise ValueError("verification must be an object or null")
            verification_id = verification.get("verification_id")
            verification_status = verification.get("status")
            record_consistent = verification.get("record_consistent")
            if not isinstance(verification_id, str) or not verification_id.strip():
                raise ValueError("verification_id is invalid")
            if verification_status not in self._VERIFICATION_STATUSES:
                raise ValueError("verification status is invalid")
            if not isinstance(record_consistent, bool):
                raise ValueError("verification record_consistent must be a boolean")
            verification_sequence = self._non_negative_int(
                verification.get("evidence_sequence"), "verification evidence_sequence"
            )
            verified_at = self._aware_timestamp(
                verification.get("verified_at"), "verification verified_at"
            )
            parsed_verification = {
                "verification_id": verification_id,
                "status": verification_status,
                "record_consistent": record_consistent,
                "evidence_sequence": verification_sequence,
                "verified_at": verified_at,
            }

        note = report.get("reliability_note")
        if note is not None and note not in self._RELIABILITY_NOTES:
            raise ValueError("reliability_note is not a recognized status")

        return {
            "task_status": task_status,
            "effective_status": effective_status,
            "reliability_note": note,
            "ledger_integrity": ledger_integrity,
            "verification_current": verification_current,
            "verification_record_valid": verification_record_valid,
            "latest_evidence_sequence": latest_sequence,
            "evidence_total": evidence_total,
            "evidence_truncated": evidence_truncated,
            "evidence_ids": tuple(evidence_ids),
            "verification": parsed_verification,
        }

    @staticmethod
    def _invalid(reason: str) -> ExecutionResult:
        return ExecutionResult(outcome=ExecutionOutcome.FAILED, reason=reason)

    async def execute(self, request: ExecutionRequest) -> ExecutionResult:
        """Read one report and return a minimized, provenance-linked event."""
        if not request.input_events:
            return ExecutionResult(
                outcome=ExecutionOutcome.NO_DATA,
                reason="evidence_lookup_request_missing",
            )
        if len(request.input_events) != 1:
            return self._invalid("evidence_lookup_requires_one_input_event")

        source_event = request.input_events[0].event
        if source_event.event_type != self.request_event_type:
            return self._invalid("evidence_lookup_request_type_invalid")

        raw_task_id = source_event.payload.get("evidence_task_id")
        if not isinstance(raw_task_id, str):
            return self._invalid("evidence_lookup_task_id_invalid")
        try:
            task_id = str(uuid.UUID(raw_task_id))
        except ValueError:
            return self._invalid("evidence_lookup_task_id_invalid")
        if task_id != raw_task_id:
            return self._invalid("evidence_lookup_task_id_invalid")

        try:
            response = await self._transport.call(
                self.rpc_method,
                {
                    "task_id": task_id,
                    "evidence_limit": self.evidence_limit,
                    "evidence_offset": 0,
                },
                request_id=request.idempotency_key,
            )
        except (OSError, ConnectionError, TimeoutError):
            return ExecutionResult(
                outcome=ExecutionOutcome.UNAVAILABLE,
                reason="evidence_transport_unavailable",
            )
        except Exception:
            return ExecutionResult(
                outcome=ExecutionOutcome.FAILED,
                reason="evidence_transport_error",
            )

        if not isinstance(response, Mapping):
            return self._invalid("evidence_rpc_invalid_response")
        if (
            response.get("jsonrpc") != "2.0"
            or response.get("id") != request.idempotency_key
            or ("result" in response) == ("error" in response)
        ):
            return self._invalid("evidence_rpc_invalid_response")

        if "error" in response:
            error = response.get("error")
            if not isinstance(error, Mapping):
                return self._invalid("evidence_rpc_invalid_response")
            code = error.get("code")
            if isinstance(code, bool) or not isinstance(code, int):
                return self._invalid("evidence_rpc_invalid_response")
            if code == -32004:
                return ExecutionResult(
                    outcome=ExecutionOutcome.NO_DATA,
                    reason="evidence_task_not_found",
                )
            if code in {-32601, -32603}:
                return ExecutionResult(
                    outcome=ExecutionOutcome.UNAVAILABLE,
                    reason="evidence_bridge_unavailable",
                )
            return self._invalid("evidence_rpc_rejected_request")

        try:
            summary = self._parse_report(response.get("result"), task_id)
        except (TypeError, ValueError):
            return self._invalid("evidence_rpc_invalid_report")

        verification = summary["verification"]
        verification_is_current = (
            verification is not None
            and verification["record_consistent"]
            and summary["verification_current"]
            and summary["verification_record_valid"]
            and summary["ledger_integrity"]
            and verification["evidence_sequence"] == summary["latest_evidence_sequence"]
        )
        epistemic_status = EpistemicStatus.UNRESOLVED
        event_verification: Verification | None = None
        if (
            verification_is_current
            and summary["effective_status"] == "passed"
            and summary["task_status"] == "passed"
            and verification["status"] == "supported"
        ):
            epistemic_status = EpistemicStatus.VERIFIED
        elif (
            verification_is_current
            and summary["effective_status"] == "failed"
            and summary["task_status"] == "failed"
            and verification["status"] in {"contradicted", "invalid"}
        ):
            epistemic_status = EpistemicStatus.REFUTED
        elif summary["effective_status"] in {"planned", "running"}:
            epistemic_status = EpistemicStatus.OBSERVED

        evidence_refs: list[str] = []
        if verification is not None:
            evidence_refs.append(f"verification:{verification['verification_id']}")
        evidence_refs.extend(f"evidence:{item}" for item in summary["evidence_ids"])

        if epistemic_status in {EpistemicStatus.VERIFIED, EpistemicStatus.REFUTED}:
            # The verification reference guarantees this list is non-empty.
            event_verification = Verification(
                verifier="evidence_engine",
                criteria="predeclared_evidence_engine_criteria",
                scope=f"evidence_task:{task_id}",
                verified_at=verification["verified_at"],
                evidence_refs=tuple(evidence_refs),
            )

        now = datetime.now(timezone.utc)
        occurred_at = verification["verified_at"] if verification is not None else now
        event = IntegrationEvent(
            event_id=uuid.uuid4(),
            schema_version=1,
            event_type="evidence.task_snapshot",
            producer_engine=self.engine_id,
            producer_version=self.producer_version,
            occurred_at=occurred_at,
            recorded_at=now,
            epistemic_status=epistemic_status,
            sensitivity=Sensitivity.INTERNAL,
            allowed_consumers=("critical_thinking", "reliability", "emotion"),
            payload={
                "evidence_task_id": task_id,
                "task_status": summary["task_status"],
                "effective_status": summary["effective_status"],
                "reliability_note": summary["reliability_note"],
                "ledger_integrity": summary["ledger_integrity"],
                "verification_current": summary["verification_current"],
                "verification_record_valid": summary["verification_record_valid"],
                "verification_status": verification["status"] if verification else None,
                "verification_evidence_sequence": (
                    verification["evidence_sequence"] if verification else None
                ),
                "latest_evidence_sequence": summary["latest_evidence_sequence"],
                "evidence_total": summary["evidence_total"],
                "evidence_truncated": summary["evidence_truncated"],
                "evidence_refs": evidence_refs,
            },
            evidence_refs=tuple(evidence_refs),
            parent_event_ids=(source_event.event_id,),
            task_id=str(request.run_id),
            correlation_id=str(request.run_id),
            idempotency_key=request.idempotency_key,
        )
        return ExecutionResult(outcome=ExecutionOutcome.COMPLETED, event=event)
