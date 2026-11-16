from datetime import datetime, timezone, timedelta
from uuid import uuid4
import json
import pytest
from kie.events import EpistemicStatus, IntegrationEvent, Sensitivity, Verification, event_to_dict

NOW = datetime(2026, 1, 1, tzinfo=timezone.utc)

def make_event(**overrides):
    values = dict(event_id=uuid4(), schema_version=1, event_type="technical_anomaly",
        producer_engine="reliability", producer_version="0.1.0", occurred_at=NOW,
        recorded_at=NOW, epistemic_status=EpistemicStatus.OBSERVED,
        sensitivity=Sensitivity.INTERNAL, allowed_consumers=("critical_thinking", "evidence"),
        payload={"metric": "tool_error_rate", "value": 0.2})
    values.update(overrides)
    return IntegrationEvent(**values)

def test_payload_is_detached():
    payload = {"nested": {"value": 1}}
    event = make_event(payload=payload)
    payload["nested"]["value"] = 99
    assert event.payload["nested"]["value"] == 1

def test_nested_payload_is_deeply_immutable():
    event = make_event(payload={"nested": {"items": [1, {"value": 2}]}})
    with pytest.raises(TypeError):
        event.payload["nested"]["new"] = "blocked"
    with pytest.raises(TypeError):
        event.payload["nested"]["items"][1]["value"] = 3
    with pytest.raises(AttributeError):
        event.payload["nested"]["items"].append(4)

def test_payload_requires_json_object_root():
    with pytest.raises(ValueError, match="JSON object"):
        make_event(payload=["not", "an", "object"])

def test_payload_rejects_non_string_object_keys():
    with pytest.raises(ValueError, match="keys must be strings"):
        make_event(payload={1: "invalid"})

def test_event_to_dict_returns_mutable_json_data_without_mutating_event():
    event = make_event(payload={"nested": {"items": [1, 2]}})
    exported = event_to_dict(event)
    json.dumps(exported)
    exported["payload"]["nested"]["items"].append(3)
    assert event.payload["nested"]["items"] == (1, 2)

def test_verified_requires_explicit_verification():
    with pytest.raises(ValueError, match="verification metadata"):
        make_event(epistemic_status=EpistemicStatus.VERIFIED)

def test_verified_event_has_scope_and_evidence():
    verification = Verification("evidence", "all_checks_passed", "task:KIE-12", NOW, ("evidence://verification/12",))
    event = make_event(epistemic_status=EpistemicStatus.VERIFIED, verification=verification)
    assert event.verification.scope == "task:KIE-12"

def test_unknown_producer_rejected():
    with pytest.raises(ValueError, match="registered engine"):
        make_event(producer_engine="unknown_engine")

def test_producer_cannot_be_consumer():
    with pytest.raises(ValueError, match="routed back"):
        make_event(allowed_consumers=("reliability",))

def test_naive_timestamp_rejected():
    with pytest.raises(ValueError, match="timezone-aware"):
        make_event(recorded_at=datetime(2026, 1, 1))

def test_non_json_payload_rejected():
    with pytest.raises(ValueError, match="JSON-compatible"):
        make_event(payload={"bad": object()})

def test_oversized_payload_rejected():
    with pytest.raises(ValueError, match="65536"):
        make_event(payload={"large": "x" * 70000})

def test_non_finite_number_rejected():
    with pytest.raises(ValueError, match="finite"):
        make_event(payload={"bad": float("nan")})

def test_expiry():
    event = make_event(expires_at=NOW + timedelta(minutes=1))
    assert not event.is_expired(NOW)
    assert event.is_expired(NOW + timedelta(minutes=1))

def test_hermes_host_can_produce_events_for_engine_consumers():
    from kie.events import HERMES_HOST_ID
    event = make_event(
        producer_engine=HERMES_HOST_ID,
        allowed_consumers=("evidence",),
        event_type="evidence.lookup_request",
    )
    assert event.producer_engine == "hermes_host"
    assert event.allowed_consumers == ("evidence",)
