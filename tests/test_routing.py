from datetime import datetime, timezone, timedelta
from uuid import uuid4
from kie.events import EpistemicStatus, IntegrationEvent, Sensitivity
from kie.routing import route_event

NOW = datetime(2026, 1, 1, tzinfo=timezone.utc)

def make_event(**overrides):
    values = dict(event_id=uuid4(), schema_version=1, event_type="review_requested",
        producer_engine="critical_thinking", producer_version="0.1.0",
        occurred_at=NOW, recorded_at=NOW, epistemic_status=EpistemicStatus.HYPOTHESIS,
        sensitivity=Sensitivity.INTERNAL, allowed_consumers=("reliability", "evidence"),
        payload={"summary": "review required"})
    values.update(overrides)
    return IntegrationEvent(**values)

def test_only_allowlisted_consumers_are_routed():
    result = route_event(make_event(), requested_consumers=("reliability", "emotion"), now=NOW)
    assert result.accepted == ("reliability",)
    assert result.rejected == (("emotion", "not_allowlisted_by_event"),)

def test_expired_event_rejected():
    event = make_event(expires_at=NOW + timedelta(seconds=1))
    result = route_event(event, requested_consumers=("evidence",), now=NOW + timedelta(seconds=1))
    assert result.accepted == ()
    assert result.rejected == (("evidence", "event_expired"),)

def test_hop_limit_rejected():
    result = route_event(make_event(hop_count=4), requested_consumers=("evidence",), now=NOW)
    assert result.rejected == (("evidence", "hop_limit_reached"),)

def test_duplicate_consumer_rejected():
    result = route_event(make_event(), requested_consumers=("evidence", "evidence"), now=NOW)
    assert result.accepted == ("evidence",)
    assert result.rejected == (("evidence", "duplicate_consumer"),)
