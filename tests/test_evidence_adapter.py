import asyncio
from datetime import datetime, timezone
from types import SimpleNamespace
from uuid import uuid4

import pytest

from kie.events import EpistemicStatus, IntegrationEvent, Sensitivity
from kie.executor import ExecutionOutcome, ExecutionRequest
from kie.adapters.evidence import EvidenceReadOnlyAdapter


NOW = datetime(2026, 11, 16, 9, 0, tzinfo=timezone.utc)


def make_request(*, payload=None, event_type="evidence.lookup_request"):
    source = IntegrationEvent(
        event_id=uuid4(),
        schema_version=1,
        event_type=event_type,
        producer_engine="reliability",
        producer_version="0.1.0",
        occurred_at=NOW,
        recorded_at=NOW,
        epistemic_status=EpistemicStatus.OBSERVED,
        sensitivity=Sensitivity.INTERNAL,
        allowed_consumers=("evidence",),
        payload=payload if payload is not None else {"evidence_task_id": str(uuid4())},
    )
    return ExecutionRequest(
        run_id=uuid4(),
        workflow_id="evidence_snapshot",
        workflow_version="1.0",
        step=SimpleNamespace(step_id="read_evidence", engine_id="evidence"),
        attempt=1,
        idempotency_key=f"kie:{uuid4()}:read_evidence:1",
        input_events=(SimpleNamespace(event=source),),
    )


def supported_report(task_id):
    return {
        "task": {
            "task_id": task_id,
            "title": "PRIVATE TASK TITLE MUST NOT ENTER THE EVENT",
            "status": "passed",
            "created_at": "2026-11-16T08:00:00+00:00",
            "criteria": [{"kind": "command_result", "field": "return_code", "operator": "eq", "expected": 0, "source": "private-command-policy"}],
        },
        "effective_status": "passed",
        "reliability_note": None,
        "ledger_integrity": True,
        "verification_current": True,
        "verification_record_valid": True,
        "latest_evidence_sequence": 2,
        "evidence_total": 2,
        "evidence_offset": 0,
        "evidence_limit": 10,
        "evidence_truncated": False,
        "evidence": [
            {
                "sequence": 1,
                "evidence_id": str(uuid4()),
                "kind": "command_result",
                "source": "private-command-policy",
                "observed_at": "2026-11-16T08:30:00+00:00",
                "payload": {"stdout": "PRIVATE OUTPUT MUST NOT ENTER THE EVENT"},
            }
        ],
        "verification": {
            "verification_id": str(uuid4()),
            "status": "supported",
            "verified_at": "2026-11-16T08:45:00+00:00",
            "evidence_sequence": 2,
            "record_consistent": True,
            "report": {"checks": [{"detail": "PRIVATE CHECK DETAIL"}]},
        },
    }


class FakeTransport:
    def __init__(self, response=None, exception=None):
        self.response = response
        self.exception = exception
        self.calls = []

    async def call(self, method, params, *, request_id):
        self.calls.append((method, dict(params), request_id))
        if self.exception is not None:
            raise self.exception
        return self.response


def rpc_result(result, request):
    return {"jsonrpc": "2.0", "id": request.idempotency_key, "result": result}


def test_supported_snapshot_becomes_scoped_verified_event_without_raw_evidence():
    request = make_request()
    task_id = request.input_events[0].event.payload["evidence_task_id"]
    report = supported_report(task_id)
    transport = FakeTransport(rpc_result(report, request))
    adapter = EvidenceReadOnlyAdapter(transport)

    result = asyncio.run(adapter.execute(request))

    assert result.outcome is ExecutionOutcome.COMPLETED
    event = result.event
    assert event.producer_engine == "evidence"
    assert event.task_id == str(request.run_id)
    assert event.idempotency_key == request.idempotency_key
    assert event.epistemic_status is EpistemicStatus.VERIFIED
    assert event.verification.verifier == "evidence_engine"
    assert event.verification.scope == f"evidence_task:{task_id}"
    assert event.verification.evidence_refs
    assert event.parent_event_ids == (request.input_events[0].event.event_id,)
    assert event.payload["effective_status"] == "passed"
    assert event.payload["ledger_integrity"] is True
    assert transport.calls == [
        (
            "get_task_report",
            {"task_id": task_id, "evidence_limit": 10, "evidence_offset": 0},
            request.idempotency_key,
        )
    ]


def test_event_contains_only_bounded_summary_not_raw_titles_criteria_or_evidence():
    request = make_request()
    task_id = request.input_events[0].event.payload["evidence_task_id"]
    transport = FakeTransport(rpc_result(supported_report(task_id), request))

    event = asyncio.run(EvidenceReadOnlyAdapter(transport).execute(request)).event
    serialized_payload = str(dict(event.payload))

    assert "PRIVATE TASK TITLE" not in serialized_payload
    assert "private-command-policy" not in serialized_payload
    assert "PRIVATE OUTPUT" not in serialized_payload
    assert "PRIVATE CHECK DETAIL" not in serialized_payload
    assert "criteria" not in event.payload


def test_stale_or_unknown_snapshot_is_not_promoted_to_verified():
    request = make_request()
    task_id = request.input_events[0].event.payload["evidence_task_id"]
    report = supported_report(task_id)
    report["effective_status"] = "unknown"
    report["reliability_note"] = "verification_is_stale"
    report["verification_current"] = False
    transport = FakeTransport(rpc_result(report, request))

    result = asyncio.run(EvidenceReadOnlyAdapter(transport).execute(request))

    assert result.outcome is ExecutionOutcome.COMPLETED
    assert result.event.epistemic_status is EpistemicStatus.UNRESOLVED
    assert result.event.verification is None


def test_missing_lookup_event_returns_no_data_without_rpc_call():
    request = make_request()
    request = ExecutionRequest(
        run_id=request.run_id,
        workflow_id=request.workflow_id,
        workflow_version=request.workflow_version,
        step=request.step,
        attempt=request.attempt,
        idempotency_key=request.idempotency_key,
        input_events=(),
    )
    transport = FakeTransport()
    result = asyncio.run(EvidenceReadOnlyAdapter(transport).execute(request))

    assert result.outcome is ExecutionOutcome.NO_DATA
    assert transport.calls == []


def test_wrong_input_event_type_fails_closed():
    request = make_request(event_type="generic.input")
    transport = FakeTransport()

    result = asyncio.run(EvidenceReadOnlyAdapter(transport).execute(request))

    assert result.outcome is ExecutionOutcome.FAILED
    assert transport.calls == []


def test_invalid_evidence_task_id_is_rejected_before_rpc():
    request = make_request(payload={"evidence_task_id": "../../other-db"})
    transport = FakeTransport()

    result = asyncio.run(EvidenceReadOnlyAdapter(transport).execute(request))

    assert result.outcome is ExecutionOutcome.FAILED
    assert transport.calls == []


def test_unknown_task_maps_to_no_data():
    request = make_request()
    transport = FakeTransport(
        {"jsonrpc": "2.0", "id": request.idempotency_key,
         "error": {"code": -32004, "message": "Task not found"}}
    )

    result = asyncio.run(EvidenceReadOnlyAdapter(transport).execute(request))

    assert result.outcome is ExecutionOutcome.NO_DATA


def test_transport_failure_maps_to_unavailable_without_leaking_error():
    request = make_request()
    transport = FakeTransport(exception=OSError("private socket path"))
    result = asyncio.run(EvidenceReadOnlyAdapter(transport).execute(request))

    assert result.outcome is ExecutionOutcome.UNAVAILABLE
    assert "private socket path" not in result.reason


def test_mismatched_json_rpc_id_is_rejected():
    request = make_request()
    task_id = request.input_events[0].event.payload["evidence_task_id"]
    transport = FakeTransport(
        {"jsonrpc": "2.0", "id": "wrong", "result": supported_report(task_id)}
    )

    result = asyncio.run(EvidenceReadOnlyAdapter(transport).execute(request))

    assert result.outcome is ExecutionOutcome.FAILED
    assert result.reason == "evidence_rpc_invalid_response"



def test_current_contradicted_snapshot_becomes_scoped_refuted_event():
    request = make_request()
    task_id = request.input_events[0].event.payload["evidence_task_id"]
    report = supported_report(task_id)
    report["task"]["status"] = "failed"
    report["effective_status"] = "failed"
    report["verification"]["status"] = "contradicted"
    transport = FakeTransport(rpc_result(report, request))

    result = asyncio.run(EvidenceReadOnlyAdapter(transport).execute(request))

    assert result.outcome is ExecutionOutcome.COMPLETED
    assert result.event.epistemic_status is EpistemicStatus.REFUTED
    assert result.event.verification is not None
    assert result.event.verification.scope == f"evidence_task:{task_id}"


@pytest.mark.parametrize(
    "field,value",
    [
        ("ledger_integrity", False),
        ("verification_current", False),
        ("verification_record_valid", False),
    ],
)
def test_inconsistent_report_never_claims_verified(field, value):
    request = make_request()
    task_id = request.input_events[0].event.payload["evidence_task_id"]
    report = supported_report(task_id)
    report[field] = value
    transport = FakeTransport(rpc_result(report, request))

    result = asyncio.run(EvidenceReadOnlyAdapter(transport).execute(request))

    assert result.outcome is ExecutionOutcome.COMPLETED
    assert result.event.epistemic_status is EpistemicStatus.UNRESOLVED
    assert result.event.verification is None


def test_malformed_report_fails_closed():
    request = make_request()
    transport = FakeTransport(rpc_result({"effective_status": "passed"}, request))

    result = asyncio.run(EvidenceReadOnlyAdapter(transport).execute(request))

    assert result.outcome is ExecutionOutcome.FAILED
    assert result.reason == "evidence_rpc_invalid_report"
