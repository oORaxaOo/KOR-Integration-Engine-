import json
from pathlib import Path
from datetime import datetime, timezone
from uuid import uuid4

from kie.events import EpistemicStatus, IntegrationEvent, Sensitivity, event_to_dict


def test_event_schema_is_valid_json_and_matches_exported_envelope():
    schema_path = Path(__file__).parents[1] / "schemas" / "integration-event-v1.schema.json"
    schema = json.loads(schema_path.read_text(encoding="utf-8"))
    now = datetime(2026, 1, 1, tzinfo=timezone.utc)
    event = IntegrationEvent(
        event_id=uuid4(), schema_version=1, event_type="schema_test",
        producer_engine="reliability", producer_version="0.1.0",
        occurred_at=now, recorded_at=now,
        epistemic_status=EpistemicStatus.OBSERVED, sensitivity=Sensitivity.INTERNAL,
        allowed_consumers=("evidence",), payload={"check": True},
    )
    exported = event_to_dict(event)
    assert set(schema["required"]) == set(exported)
    assert set(schema["properties"]) == set(exported)
    assert schema["properties"]["schema_version"]["const"] == event.schema_version
    assert schema["properties"]["producer_engine"]["enum"]

def test_event_schema_includes_trusted_hermes_host_as_producer():
    from kie.events import HERMES_HOST_ID
    schema_path = Path(__file__).parents[1] / "schemas" / "integration-event-v1.schema.json"
    schema = json.loads(schema_path.read_text(encoding="utf-8"))
    assert HERMES_HOST_ID in schema["properties"]["producer_engine"]["enum"]
