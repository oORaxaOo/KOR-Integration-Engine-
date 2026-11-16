import json
import socket
import stat
from pathlib import Path
from uuid import uuid4

import pytest

from kie import SQLiteEventStore
from kie.transports.engine_event_socket import (
    EngineEventRejected,
    EngineEventSocketServer,
    MAX_MESSAGE_BYTES,
    _event_from_message,
    make_engine_event_message,
)


def _cte_message():
    return make_engine_event_message(
        "critical_thinking",
        "critical_thinking.review_routed",
        {
            "intent": "project_proposal",
            "review_level": "critical",
            "user_requested": False,
            "signals": ["datenschutz", "integration"],
        },
    )


def _reliability_message():
    return make_engine_event_message(
        "reliability",
        "reliability.session_snapshot",
        {
            "api_requests": 8,
            "api_successes": 7,
            "api_errors": 1,
            "tool_calls": 5,
            "tool_blocks": 1,
            "anomaly_count": 2,
            "context_pressure": "normal",
            "context_warning_events": 0,
            "context_critical_events": 0,
            "model_turns": 4,
            "user_task_labels": 0,
        },
    )


def _emotion_message():
    return make_engine_event_message(
        "emotion",
        "emotion.reflection_completed",
        {
            "status": "completed",
            "step_count": 2,
            "steps_remaining": 2,
            "hypothesis_count": 1,
            "limitations_count": 2,
            "follow_up_pending": False,
        },
    )


def _send(path: Path, message: dict) -> dict:
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
        client.settimeout(2.0)
        client.connect(str(path))
        client.sendall(json.dumps(message, separators=(",", ":")).encode("utf-8") + b"\n")
        stream = client.makefile("rb")
        return json.loads(stream.readline(1024))


def test_event_contract_accepts_allowlisted_events_from_all_three_engines():
    cte = _event_from_message(_cte_message())
    reliability = _event_from_message(_reliability_message())
    emotion = _event_from_message(_emotion_message())

    assert cte.producer_engine == "critical_thinking"
    assert cte.payload["review_level"] == "critical"
    assert reliability.producer_engine == "reliability"
    assert reliability.payload["api_errors"] == 1
    assert emotion.producer_engine == "emotion"
    assert emotion.sensitivity.value == "restricted"


def test_event_contract_rejects_emotion_reflection_text():
    message = _emotion_message()
    message["payload"]["finding"] = "private reflection content"

    with pytest.raises(EngineEventRejected):
        _event_from_message(message)


def test_event_contract_rejects_unknown_engine_event_pair():
    message = _cte_message()
    message["event_type"] = "critical_thinking.raw_message"

    with pytest.raises(EngineEventRejected):
        _event_from_message(message)


def test_event_contract_rejects_inconsistent_reliability_counters():
    message = _reliability_message()
    message["payload"]["api_successes"] = 9

    with pytest.raises(EngineEventRejected):
        _event_from_message(message)


def test_unix_socket_ingress_persists_sanitized_events(tmp_path):
    database = tmp_path / "kie.sqlite3"
    socket_path = tmp_path / "run" / "kie-engine-events.sock"
    server = EngineEventSocketServer(socket_path, database)
    server.start()
    try:
        assert stat.S_IMODE(socket_path.stat().st_mode) == 0o600
        results = [
            _send(socket_path, _cte_message()),
            _send(socket_path, _reliability_message()),
            _send(socket_path, _emotion_message()),
        ]
        assert all(result["ok"] for result in results)
        with SQLiteEventStore(database) as store:
            records = store.read_events(limit=10)
            assert len(records) == 3
            assert store.verify_integrity().ok
            emotion = next(item.event for item in records if item.event.producer_engine == "emotion")
            assert emotion.sensitivity.value == "restricted"
            assert "finding" not in emotion.payload
    finally:
        server.shutdown()
    assert not socket_path.exists()


def test_server_refuses_to_replace_a_regular_file(tmp_path):
    socket_path = tmp_path / "not-a-socket"
    socket_path.write_text("keep", encoding="utf-8")
    server = EngineEventSocketServer(socket_path, tmp_path / "kie.sqlite3")

    with pytest.raises(ValueError, match="non-socket"):
        server.start()

    assert socket_path.read_text(encoding="utf-8") == "keep"


def test_server_rejects_oversized_message(tmp_path):
    database = tmp_path / "kie.sqlite3"
    socket_path = tmp_path / "kie.sock"
    server = EngineEventSocketServer(socket_path, database)
    server.start()
    try:
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
            client.settimeout(2.0)
            client.connect(str(socket_path))
            client.sendall(b"x" * (MAX_MESSAGE_BYTES + 1) + b"\n")
            result = json.loads(client.makefile("rb").readline(1024))
        assert result == {"ok": False, "reason": "invalid_message_size"}
        assert not database.exists()
    finally:
        server.shutdown()
