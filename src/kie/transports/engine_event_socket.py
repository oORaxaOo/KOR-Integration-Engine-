"""Local, bounded ingress for metadata-only events from independent KOR engines.

The Unix socket is a same-user local IPC boundary, not cryptographic publisher
authentication. The host validates an explicit per-engine event and payload
allowlist before writing anything to KIE's append-only event store.
"""
from __future__ import annotations

import errno
import fcntl
import json
import os
from pathlib import Path
import socket
import socketserver
import stat
import threading
from datetime import datetime, timezone
from typing import Any, Mapping
from uuid import UUID, uuid4

from ..events import EpistemicStatus, IntegrationEvent, Sensitivity
from ..event_store import SQLiteEventStore

MAX_MESSAGE_BYTES = 64 * 1024
_SOCKET_PATH_MAX_BYTES = 100
_HEALTH_CONTROL_MESSAGE = {"control": "health"}
_HEALTH_CONTROL_RESPONSE = {
    "ok": True,
    "service": "kie-engine-event-ingress",
    "protocol_version": 1,
}

_INTENTS = frozenset({
    "personal_experience", "opinion", "factual_claim", "project_proposal",
    "decision", "question", "unknown",
})
_REVIEW_LEVELS = frozenset({"review", "critical"})
_CTE_SIGNALS = frozenset({
    "sicherheit", "security", "datenschutz", "privacy", "zugriff", "berechtigung",
    "irreversibel", "unwiderruflich", "migration", "löschen", "loeschen",
    "produktive umgebung", "systemweit", "architektur ändern",
    "architektur aendern", "komplette architektur",
    "technical_claim_signal", "technical_project_signal",
})
_PRESSURE_LEVELS = frozenset({"disabled", "waiting", "normal", "warning", "critical"})
_RELIABILITY_KINDS = frozenset({"provider_error", "tool_call_blocked", "response_anomaly"})
_ANOMALIES = frozenset({
    "response_not_text", "empty_response", "response_too_large", "repeated_lines",
})
_EMOTION_STATUSES = frozenset({"completed", "budget_exhausted"})
_ENGINE_FAILURE_CODES = {
    "critical_thinking": frozenset({"hook_exception"}),
    "emotion": frozenset({"reflection_handler_error"}),
    "evidence": frozenset({"configuration_missing", "policy_invalid", "verification_exception"}),
}


class EngineEventRejected(ValueError):
    """An engine event did not match the fixed KIE ingress contract."""


def _non_negative_int(payload: Mapping[str, Any], key: str, *, maximum: int = 1_000_000) -> int:
    value = payload.get(key)
    if isinstance(value, bool) or not isinstance(value, int) or not 0 <= value <= maximum:
        raise EngineEventRejected(f"{key} must be an integer from 0 to {maximum}")
    return value


def _exact_keys(payload: Any, required: set[str]) -> Mapping[str, Any]:
    if not isinstance(payload, dict) or set(payload) != required:
        raise EngineEventRejected("payload does not match the event schema")
    return payload


def _validate_payload(engine_id: str, event_type: str, payload: Any) -> tuple[dict[str, Any], Sensitivity, tuple[str, ...]]:
    if engine_id in _ENGINE_FAILURE_CODES and event_type == f"{engine_id}.hook_failure":
        data = _exact_keys(payload, {"failure_code"})
        code = data["failure_code"]
        if not isinstance(code, str) or code not in _ENGINE_FAILURE_CODES[engine_id]:
            raise EngineEventRejected("failure_code is not allowlisted")
        return {"failure_code": code}, Sensitivity.INTERNAL, ("reliability",)

    if engine_id == "emotion" and event_type == "emotion.reflection_failed":
        data = _exact_keys(payload, {"failure_code"})
        code = data["failure_code"]
        if not isinstance(code, str) or code != "reflection_handler_error":
            raise EngineEventRejected("failure_code is not allowlisted")
        return {"failure_code": code}, Sensitivity.INTERNAL, ("reliability",)

    if engine_id == "evidence" and event_type == "evidence.verification_unavailable":
        data = _exact_keys(payload, {"failure_code"})
        code = data["failure_code"]
        if not isinstance(code, str) or code not in _ENGINE_FAILURE_CODES["evidence"]:
            raise EngineEventRejected("failure_code is not allowlisted")
        return {"failure_code": code}, Sensitivity.INTERNAL, ("reliability",)

    if engine_id == "critical_thinking" and event_type == "critical_thinking.review_routed":
        data = _exact_keys(payload, {"intent", "review_level", "user_requested", "signals"})
        if not isinstance(data["intent"], str) or data["intent"] not in _INTENTS:
            raise EngineEventRejected("invalid critical-thinking intent")
        if not isinstance(data["review_level"], str) or data["review_level"] not in _REVIEW_LEVELS:
            raise EngineEventRejected("invalid critical-thinking review level")
        if not isinstance(data["user_requested"], bool):
            raise EngineEventRejected("user_requested must be boolean")
        signals = data["signals"]
        if (
            not isinstance(signals, list)
            or len(signals) > 8
            or any(not isinstance(item, str) or item not in _CTE_SIGNALS for item in signals)
            or len(set(signals)) != len(signals)
        ):
            raise EngineEventRejected("signals must contain at most eight short strings")
        return {
            "intent": data["intent"],
            "review_level": data["review_level"],
            "user_requested": data["user_requested"],
            "signals": list(signals),
        }, Sensitivity.INTERNAL, ("reliability",)

    if engine_id == "reliability" and event_type == "reliability.runtime_observation":
        data = _exact_keys(payload, {"kind", "count", "anomaly_codes"})
        if not isinstance(data["kind"], str) or data["kind"] not in _RELIABILITY_KINDS:
            raise EngineEventRejected("invalid reliability observation kind")
        count = _non_negative_int(data, "count")
        codes = data["anomaly_codes"]
        if (
            not isinstance(codes, list)
            or len(codes) > 4
            or any(code not in _ANOMALIES for code in codes)
        ):
            raise EngineEventRejected("anomaly_codes contains an unsupported value")
        if data["kind"] == "response_anomaly" and not codes:
            raise EngineEventRejected("response_anomaly requires an allowlisted anomaly code")
        if data["kind"] != "response_anomaly" and codes:
            raise EngineEventRejected("anomaly_codes are only valid for response_anomaly")
        return {"kind": data["kind"], "count": count, "anomaly_codes": list(codes)}, Sensitivity.INTERNAL, ("critical_thinking",)

    if engine_id == "reliability" and event_type == "reliability.runtime_snapshot":
        keys = {
            "api_requests", "api_successes", "api_errors", "tool_calls",
            "tool_blocks", "anomaly_count", "context_pressure",
            "context_warning_events", "context_critical_events",
            "model_turns", "user_task_labels",
        }
        data = _exact_keys(payload, keys)
        if not isinstance(data["context_pressure"], str) or data["context_pressure"] not in _PRESSURE_LEVELS:
            raise EngineEventRejected("invalid context_pressure")
        clean = {key: _non_negative_int(data, key) for key in keys if key != "context_pressure"}
        clean["context_pressure"] = data["context_pressure"]
        if clean["api_successes"] + clean["api_errors"] > clean["api_requests"]:
            raise EngineEventRejected("API outcomes exceed API requests")
        return clean, Sensitivity.INTERNAL, ("critical_thinking",)

    if engine_id == "emotion" and event_type == "emotion.reflection_completed":
        data = _exact_keys(payload, {
            "status", "step_count", "steps_remaining", "hypothesis_count",
            "limitations_count", "follow_up_pending",
        })
        if not isinstance(data["status"], str) or data["status"] not in _EMOTION_STATUSES:
            raise EngineEventRejected("invalid emotion reflection status")
        clean = {
            "step_count": _non_negative_int(data, "step_count", maximum=4),
            "steps_remaining": _non_negative_int(data, "steps_remaining", maximum=4),
            "hypothesis_count": _non_negative_int(data, "hypothesis_count", maximum=12),
            "limitations_count": _non_negative_int(data, "limitations_count", maximum=20),
            "follow_up_pending": data["follow_up_pending"],
            "status": data["status"],
        }
        if not isinstance(clean["follow_up_pending"], bool):
            raise EngineEventRejected("follow_up_pending must be boolean")
        if clean["step_count"] + clean["steps_remaining"] > 4:
            raise EngineEventRejected("reflection step budget is invalid")
        return clean, Sensitivity.RESTRICTED, ("reliability",)

    raise EngineEventRejected("engine event type is not allowlisted")


def _event_from_message(message: Any) -> IntegrationEvent:
    if not isinstance(message, dict) or set(message) != {"event_id", "engine_id", "event_type", "payload"}:
        raise EngineEventRejected("message must contain event_id, engine_id, event_type, and payload")
    if not isinstance(message["event_id"], str):
        raise EngineEventRejected("event_id must be a UUID string")
    try:
        event_id = UUID(message["event_id"])
    except (TypeError, ValueError, AttributeError) as exc:
        raise EngineEventRejected("event_id must be a UUID") from exc
    if str(event_id) != message["event_id"]:
        raise EngineEventRejected("event_id must use canonical UUID format")
    engine_id = message["engine_id"]
    event_type = message["event_type"]
    if not isinstance(engine_id, str) or engine_id not in {"critical_thinking", "reliability", "emotion", "evidence"}:
        raise EngineEventRejected("unknown engine identity")
    if not isinstance(event_type, str):
        raise EngineEventRejected("event_type must be a string")
    clean_payload, sensitivity, consumers = _validate_payload(
        engine_id, event_type, message["payload"]
    )
    now = datetime.now(timezone.utc)
    return IntegrationEvent(
        event_id=event_id,
        schema_version=1,
        event_type=event_type,
        producer_engine=engine_id,
        producer_version="0.1.0",
        occurred_at=now,
        recorded_at=now,
        epistemic_status=EpistemicStatus.OBSERVED,
        sensitivity=sensitivity,
        allowed_consumers=consumers,
        payload=clean_payload,
        idempotency_key=f"engine-event:{event_id}",
    )


def _probe_socket(socket_path: Path) -> bool:
    """Check the dedicated control response without creating an engine event."""
    try:
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
            client.settimeout(0.5)
            client.connect(str(socket_path))
            client.sendall(
                json.dumps(_HEALTH_CONTROL_MESSAGE, separators=(",", ":")).encode("utf-8") + b"\\n"
            )
            with client.makefile("rb") as stream:
                response = stream.readline(1024)
        return bool(response) and json.loads(response.decode("utf-8")) == _HEALTH_CONTROL_RESPONSE
    except (OSError, TimeoutError, UnicodeDecodeError, json.JSONDecodeError, ValueError):
        return False


class EngineEventSocketServer:
    """A single-line JSON receiver with cross-process exclusive ownership."""

    def __init__(self, socket_path: str | Path, database_path: str | Path) -> None:
        socket_value = Path(socket_path).expanduser()
        database_value = Path(database_path).expanduser()
        if not socket_value.is_absolute():
            raise ValueError("socket_path must be absolute")
        if not database_value.is_absolute():
            raise ValueError("database_path must be absolute")
        if len(os.fsencode(str(socket_value))) >= _SOCKET_PATH_MAX_BYTES:
            raise ValueError("socket_path is too long for a Unix-domain socket")
        self.socket_path = socket_value
        self.database_path = database_value
        self._server = None
        self._thread: threading.Thread | None = None
        self._lock_fd: int | None = None
        self._observer = False
        self._lock = threading.RLock()

    @property
    def is_running(self) -> bool:
        """Whether this process owns a live local event receiver."""
        with self._lock:
            return self._server is not None and self._thread is not None and self._thread.is_alive()

    @property
    def status(self) -> str:
        """Return owner, connected, or unavailable for the shared ingress."""
        with self._lock:
            if (
                self._server is not None
                and self._thread is not None
                and self._thread.is_alive()
            ):
                return "owner"
            observer = self._observer
        if observer and _probe_socket(self.socket_path):
            return "connected"
        return "unavailable"

    def _release_lock(self) -> None:
        lock_fd = self._lock_fd
        self._lock_fd = None
        if lock_fd is not None:
            try:
                fcntl.flock(lock_fd, fcntl.LOCK_UN)
            finally:
                os.close(lock_fd)

    def start(self) -> None:
        with self._lock:
            if self._server is not None:
                return
            self.socket_path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
            self.database_path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
            if hasattr(os, "getuid"):
                for private_directory in {self.socket_path.parent, self.database_path.parent}:
                    try:
                        if private_directory.stat().st_uid == os.getuid():
                            private_directory.chmod(0o700)
                    except OSError:
                        pass

            # flock is released by the kernel when the owning process exits.
            # Keep the lock file itself: unlinking it could create a second inode
            # and allow two processes to believe they own the same socket.
            lock_path = self.socket_path.with_name(self.socket_path.name + ".lock")
            lock_fd = os.open(lock_path, os.O_CREAT | os.O_RDWR, 0o600)
            try:
                os.fchmod(lock_fd, 0o600)
                fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                os.close(lock_fd)
                self._observer = True
                return
            except Exception:
                os.close(lock_fd)
                raise

            self._lock_fd = lock_fd
            try:
                if os.path.lexists(self.socket_path):
                    info = os.lstat(self.socket_path)
                    if not stat.S_ISSOCK(info.st_mode):
                        raise ValueError("refusing to replace a non-socket path")
                    getuid = getattr(os, "getuid", None)
                    if getuid is not None and info.st_uid != getuid():
                        raise ValueError("refusing to replace a socket owned by another user")
                    probe = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
                    try:
                        probe.settimeout(0.1)
                        probe.connect(str(self.socket_path))
                    except OSError as exc:
                        if exc.errno == errno.ECONNREFUSED:
                            self.socket_path.unlink()
                        elif exc.errno != errno.ENOENT:
                            raise ValueError("could not safely inspect the existing socket") from exc
                    else:
                        raise ValueError("an engine event socket is already active without the KIE lock")
                    finally:
                        probe.close()

                owner = self

                class Handler(socketserver.StreamRequestHandler):
                    def handle(self) -> None:
                        self.request.settimeout(1.0)
                        try:
                            line = self.rfile.readline(MAX_MESSAGE_BYTES + 1)
                            if not line or len(line) > MAX_MESSAGE_BYTES or not line.endswith(b"\\n"):
                                self._reply({"ok": False, "reason": "invalid_message_size"})
                                return
                            message = json.loads(line.decode("utf-8"))
                            if message == _HEALTH_CONTROL_MESSAGE:
                                self._reply(_HEALTH_CONTROL_RESPONSE)
                                return
                            event = _event_from_message(message)
                            with SQLiteEventStore(owner.database_path) as store:
                                result = store.append(event)
                            self._reply({"ok": True, "event_id": str(result.event_id), "inserted": result.inserted})
                        except (UnicodeDecodeError, json.JSONDecodeError, EngineEventRejected, ValueError):
                            self._reply({"ok": False, "reason": "event_rejected"})
                        except (OSError, TimeoutError):
                            self._reply({"ok": False, "reason": "ingress_unavailable"})
                        except Exception:
                            # Do not reveal database paths, SQL, or host diagnostics to publishers.
                            self._reply({"ok": False, "reason": "ingress_failed"})

                    def _reply(self, value: dict[str, Any]) -> None:
                        try:
                            self.wfile.write(json.dumps(value, separators=(",", ":")).encode("utf-8") + b"\\n")
                            self.wfile.flush()
                        except OSError:
                            pass

                class Server(socketserver.ThreadingUnixStreamServer):
                    daemon_threads = True
                    allow_reuse_address = False

                server = Server(str(self.socket_path), Handler)
                os.chmod(self.socket_path, 0o600)
                self._server = server
                self._observer = False
                self._thread = threading.Thread(
                    target=server.serve_forever,
                    name="kie-engine-event-ingress",
                    daemon=True,
                )
                self._thread.start()
            except Exception:
                self._release_lock()
                raise

    def shutdown(self) -> None:
        with self._lock:
            server, thread = self._server, self._thread
            self._server = None
            self._thread = None
            self._observer = False

        try:
            if server is not None:
                server.shutdown()
                server.server_close()
            if thread is not None:
                thread.join(timeout=2.0)
            # Only the process that held the exclusive lock may unlink the socket.
            if server is not None:
                try:
                    if os.path.lexists(self.socket_path) and stat.S_ISSOCK(os.lstat(self.socket_path).st_mode):
                        self.socket_path.unlink()
                except OSError:
                    pass
        finally:
            self._release_lock()


def make_engine_event_message(engine_id: str, event_type: str, payload: Mapping[str, Any]) -> dict[str, Any]:
    """Build the wire envelope used by tests and trusted local engine publishers."""
    return {
        "event_id": str(uuid4()),
        "engine_id": engine_id,
        "event_type": event_type,
        "payload": dict(payload),
    }


__all__ = [
    "EngineEventRejected",
    "EngineEventSocketServer",
    "MAX_MESSAGE_BYTES",
    "make_engine_event_message",
]
