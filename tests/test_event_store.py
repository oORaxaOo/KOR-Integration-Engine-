from dataclasses import replace
from datetime import datetime, timezone, timedelta
import sqlite3
from uuid import uuid4

import pytest

from kie.events import EpistemicStatus, IntegrationEvent, Sensitivity
from kie.event_store import (
    EventConflictError,
    IdempotencyConflictError,
    SQLiteEventStore,
)

NOW = datetime(2026, 1, 1, tzinfo=timezone.utc)


def make_event(**overrides):
    values = dict(
        event_id=uuid4(),
        schema_version=1,
        event_type="technical_anomaly",
        producer_engine="reliability",
        producer_version="0.1.0",
        occurred_at=NOW,
        recorded_at=NOW,
        epistemic_status=EpistemicStatus.OBSERVED,
        sensitivity=Sensitivity.INTERNAL,
        allowed_consumers=("critical_thinking", "evidence"),
        payload={"metric": "tool_error_rate", "value": 0.2},
    )
    values.update(overrides)
    return IntegrationEvent(**values)


def test_append_persists_and_returns_event(tmp_path):
    event = make_event()
    with SQLiteEventStore(tmp_path / "events.sqlite3") as store:
        result = store.append(event)
        record = store.get_event(event.event_id)

        assert result.inserted is True
        assert result.sequence == 1
        assert record is not None
        assert record.event == event
        assert len(record.envelope_sha256) == 64
        assert store.count() == 1


def test_duplicate_event_id_with_same_envelope_is_idempotent(tmp_path):
    event = make_event()
    with SQLiteEventStore(tmp_path / "events.sqlite3") as store:
        first = store.append(event)
        second = store.append(event)

        assert first.inserted is True
        assert second.inserted is False
        assert second.sequence == first.sequence
        assert store.count() == 1


def test_duplicate_event_id_with_different_content_is_rejected(tmp_path):
    event = make_event()
    changed = replace(event, payload={"metric": "changed"})
    with SQLiteEventStore(tmp_path / "events.sqlite3") as store:
        store.append(event)
        with pytest.raises(EventConflictError):
            store.append(changed)


def test_idempotency_key_deduplicates_new_event_id_and_recorded_time(tmp_path):
    event = make_event(idempotency_key="reliability:task-17:observation-1")
    retry = replace(
        event,
        event_id=uuid4(),
        recorded_at=NOW + timedelta(seconds=5),
    )
    with SQLiteEventStore(tmp_path / "events.sqlite3") as store:
        first = store.append(event)
        second = store.append(retry)

        assert first.inserted is True
        assert second.inserted is False
        assert second.event_id == event.event_id
        assert store.count() == 1


def test_idempotency_key_reuse_with_changed_payload_is_rejected(tmp_path):
    event = make_event(idempotency_key="reliability:task-17:observation-1")
    changed = replace(event, event_id=uuid4(), payload={"metric": "different"})
    with SQLiteEventStore(tmp_path / "events.sqlite3") as store:
        store.append(event)
        with pytest.raises(IdempotencyConflictError):
            store.append(changed)


def test_same_idempotency_key_from_different_engines_is_independent(tmp_path):
    first = make_event(idempotency_key="same-key")
    second = make_event(
        producer_engine="critical_thinking",
        allowed_consumers=("reliability", "evidence"),
        idempotency_key="same-key",
    )
    with SQLiteEventStore(tmp_path / "events.sqlite3") as store:
        assert store.append(first).inserted
        assert store.append(second).inserted
        assert store.count() == 2


def test_append_only_triggers_reject_updates_and_deletes(tmp_path):
    db_path = tmp_path / "events.sqlite3"
    with SQLiteEventStore(db_path) as store:
        store.append(make_event())

    connection = sqlite3.connect(db_path)
    try:
        with pytest.raises(sqlite3.IntegrityError, match="append-only"):
            connection.execute("UPDATE kie_events SET event_type = 'rewritten' WHERE sequence = 1")
        with pytest.raises(sqlite3.IntegrityError, match="append-only"):
            connection.execute("DELETE FROM kie_events WHERE sequence = 1")
    finally:
        connection.close()


def test_read_events_filters_and_uses_stable_sequence_cursor(tmp_path):
    events = (
        make_event(task_id="task-a"),
        make_event(task_id="task-b"),
        make_event(task_id="task-a", event_type="review_requested"),
    )
    with SQLiteEventStore(tmp_path / "events.sqlite3") as store:
        for event in events:
            store.append(event)

        first_page = store.read_events(limit=2)
        second_page = store.read_events(after_sequence=first_page[-1].sequence, limit=2)
        task_a = store.read_events(task_id="task-a")

        assert [item.sequence for item in first_page] == [1, 2]
        assert [item.sequence for item in second_page] == [3]
        assert [item.event.task_id for item in task_a] == ["task-a", "task-a"]


def test_replay_is_read_only_and_does_not_dispatch_events(tmp_path):
    with SQLiteEventStore(tmp_path / "events.sqlite3") as store:
        store.append(make_event())
        before = store.count()
        replayed = store.replay()

        assert len(replayed) == 1
        assert store.count() == before
        assert replayed[0].sequence == 1


def test_store_survives_close_and_reopen(tmp_path):
    path = tmp_path / "events.sqlite3"
    event = make_event()
    with SQLiteEventStore(path) as first_store:
        first_store.append(event)

    with SQLiteEventStore(path) as second_store:
        assert second_store.get_event(event.event_id).event == event
        assert second_store.verify_integrity().ok


def test_hash_chain_detects_modified_envelope(tmp_path):
    path = tmp_path / "events.sqlite3"
    with SQLiteEventStore(path) as store:
        store.append(make_event())

    connection = sqlite3.connect(path)
    try:
        connection.execute("DROP TRIGGER kie_events_no_update")
        connection.execute(
            "UPDATE kie_events SET envelope_json = ? WHERE sequence = 1",
            ('{"tampered":true}',),
        )
        connection.commit()
    finally:
        connection.close()

    with SQLiteEventStore(path) as store:
        report = store.verify_integrity()
        assert not report.ok
        assert any("envelope hash mismatch" in error for error in report.errors)


def test_hash_chain_detects_modified_index_metadata(tmp_path):
    path = tmp_path / "events.sqlite3"
    with SQLiteEventStore(path) as store:
        store.append(make_event())

    connection = sqlite3.connect(path)
    try:
        connection.execute("DROP TRIGGER kie_events_no_update")
        connection.execute("UPDATE kie_events SET producer_engine = 'emotion' WHERE sequence = 1")
        connection.commit()
    finally:
        connection.close()

    with SQLiteEventStore(path) as store:
        report = store.verify_integrity()
        assert not report.ok
        assert any("metadata mismatch" in error for error in report.errors)


def test_invalid_pagination_is_rejected(tmp_path):
    with SQLiteEventStore(tmp_path / "events.sqlite3") as store:
        with pytest.raises(ValueError, match="limit"):
            store.read_events(limit=0)
        with pytest.raises(ValueError, match="after_sequence"):
            store.read_events(after_sequence=-1)


def test_closed_store_rejects_operations(tmp_path):
    store = SQLiteEventStore(tmp_path / "events.sqlite3")
    store.close()
    with pytest.raises(RuntimeError, match="closed"):
        store.count()


def test_read_recent_events_returns_latest_matching_records_in_chronological_order(tmp_path):
    records = (
        make_event(event_type="ct_review", producer_engine="critical_thinking",
                   allowed_consumers=("reliability",)),
        make_event(event_type="reliability_error", producer_engine="reliability",
                   allowed_consumers=("critical_thinking",)),
        make_event(event_type="ct_review", producer_engine="critical_thinking",
                   allowed_consumers=("reliability",)),
        make_event(event_type="ct_review", producer_engine="critical_thinking",
                   allowed_consumers=("reliability",)),
    )
    with SQLiteEventStore(tmp_path / "events.sqlite3") as store:
        for event in records:
            store.append(event)

        recent = store.read_recent_events(limit=2, producer_engine="critical_thinking")

        assert [item.sequence for item in recent] == [3, 4]
        assert [item.event.event_type for item in recent] == ["ct_review", "ct_review"]


def test_read_recent_events_validates_limit(tmp_path):
    with SQLiteEventStore(tmp_path / "events.sqlite3") as store:
        with pytest.raises(ValueError, match="limit"):
            store.read_recent_events(limit=0)
