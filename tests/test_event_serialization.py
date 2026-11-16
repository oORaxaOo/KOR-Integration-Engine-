from datetime import datetime, timezone
from uuid import uuid4
import pytest

from kie.events import (
    EpistemicStatus,
    IntegrationEvent,
    Sensitivity,
    Verification,
    event_from_dict,
    event_to_dict,
)

NOW = datetime(2026, 1, 1, 12, 30, tzinfo=timezone.utc)


def test_event_round_trips_through_public_envelope():
    event = IntegrationEvent(
        event_id=uuid4(),
        schema_version=1,
        event_type="verification_completed",
        producer_engine="evidence",
        producer_version="0.2.0",
        occurred_at=NOW,
        recorded_at=NOW,
        epistemic_status=EpistemicStatus.VERIFIED,
        sensitivity=Sensitivity.INTERNAL,
        allowed_consumers=("critical_thinking", "reliability"),
        payload={"result": {"passed": True, "checks": [1, 2, 3]}},
        evidence_refs=("evidence://run/42",),
        verification=Verification(
            "evidence",
            "all_required_checks_passed",
            "task:42",
            NOW,
            ("evidence://run/42",),
        ),
    )

    restored = event_from_dict(event_to_dict(event))

    assert restored == event
    assert restored.payload["result"]["checks"] == (1, 2, 3)


def test_event_from_dict_rejects_missing_fields():
    with pytest.raises(ValueError, match="missing"):
        event_from_dict({"event_id": str(uuid4())})


def test_event_from_dict_rejects_unknown_fields():
    event = IntegrationEvent(
        event_id=uuid4(), schema_version=1, event_type="simple_event",
        producer_engine="reliability", producer_version="0.1",
        occurred_at=NOW, recorded_at=NOW,
        epistemic_status=EpistemicStatus.OBSERVED,
        sensitivity=Sensitivity.INTERNAL,
        allowed_consumers=("evidence",), payload={},
    )
    envelope = event_to_dict(event)
    envelope["unexpected"] = "not allowed"
    with pytest.raises(ValueError, match="unknown"):
        event_from_dict(envelope)


def test_event_from_dict_rejects_invalid_timestamp():
    event = IntegrationEvent(
        event_id=uuid4(), schema_version=1, event_type="simple_event",
        producer_engine="reliability", producer_version="0.1",
        occurred_at=NOW, recorded_at=NOW,
        epistemic_status=EpistemicStatus.OBSERVED,
        sensitivity=Sensitivity.INTERNAL,
        allowed_consumers=("evidence",), payload={},
    )
    envelope = event_to_dict(event)
    envelope["recorded_at"] = "not-a-timestamp"
    with pytest.raises(ValueError, match="timestamp"):
        event_from_dict(envelope)


def test_event_from_dict_rejects_non_array_collection_fields():
    event = IntegrationEvent(
        event_id=uuid4(), schema_version=1, event_type="simple_event",
        producer_engine="reliability", producer_version="0.1",
        occurred_at=NOW, recorded_at=NOW,
        epistemic_status=EpistemicStatus.OBSERVED,
        sensitivity=Sensitivity.INTERNAL,
        allowed_consumers=("evidence",), payload={},
    )
    envelope = event_to_dict(event)
    envelope["evidence_refs"] = "not-an-array"
    with pytest.raises(ValueError, match="JSON array"):
        event_from_dict(envelope)
