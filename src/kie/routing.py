"""Pure, deterministic routing decisions. No engine is invoked here."""
from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime
from .events import IntegrationEvent

@dataclass(frozen=True, slots=True)
class RouteDecision:
    accepted: tuple[str, ...]
    rejected: tuple[tuple[str, str], ...]

def route_event(event: IntegrationEvent, *, requested_consumers: tuple[str, ...], now: datetime, max_hops: int = 4) -> RouteDecision:
    """Apply event-level permissions and basic loop/expiry limits.

    Policy evaluation only: this does not authenticate producers, execute
    consumers, or prove that a consumer is trustworthy.
    """
    if isinstance(max_hops, bool) or not isinstance(max_hops, int) or max_hops < 0:
        raise ValueError("max_hops must be a non-negative integer")
    accepted: list[str] = []
    rejected: list[tuple[str, str]] = []
    for consumer in requested_consumers:
        if consumer not in event.allowed_consumers:
            reason = "not_allowlisted_by_event"
        elif consumer == event.producer_engine:
            reason = "producer_loop_blocked"
        elif event.is_expired(now):
            reason = "event_expired"
        elif event.hop_count >= max_hops:
            reason = "hop_limit_reached"
        elif consumer in accepted:
            reason = "duplicate_consumer"
        else:
            accepted.append(consumer)
            continue
        rejected.append((consumer, reason))
    return RouteDecision(tuple(accepted), tuple(rejected))
