from datetime import datetime, timedelta, timezone
from pathlib import Path
import sys
from uuid import uuid4

from kie.events import EpistemicStatus, IntegrationEvent, Sensitivity
from kie.event_store import SQLiteEventStore
from kie.health import HealthSupervisor


EXPECTED_PLUGINS = {
    "evidence-engine",
    "critical-thinking-engine",
    "reliability-engine",
    "emotion-engine",
}


class FakeContext:
    def __init__(self, loaded=None):
        self.loaded = set(EXPECTED_PLUGINS if loaded is None else loaded)

    def has_plugin(self, plugin_id):
        return plugin_id in self.loaded


class FakeServer:
    def __init__(self, running=True):
        self.is_running = running


def _make_supervisor(tmp_path, *, loaded=None, event_server=None, now=None):
    root = tmp_path / "plugins" / "evidence-engine"
    package = root / "src" / "evidence_engine"
    package.mkdir(parents=True)
    (package / "bridge.py").write_text("# test bridge\n", encoding="utf-8")
    database = tmp_path / "kie.sqlite3"
    return HealthSupervisor(
        FakeContext(loaded),
        kie_database=database,
        event_server=event_server or FakeServer(),
        evidence_python=Path(sys.executable),
        evidence_source_root=root,
        evidence_database=tmp_path / "evidence.sqlite3",
        now=(lambda: now) if now is not None else None,
    ), database


def _append_reliability_event(database, *, kind, recorded_at):
    event = IntegrationEvent(
        event_id=uuid4(),
        schema_version=1,
        event_type="reliability.runtime_observation",
        producer_engine="reliability",
        producer_version="0.1.0",
        occurred_at=recorded_at,
        recorded_at=recorded_at,
        epistemic_status=EpistemicStatus.OBSERVED,
        sensitivity=Sensitivity.INTERNAL,
        allowed_consumers=("critical_thinking",),
        payload={
            "kind": kind,
            "count": 1,
            "anomaly_codes": ["repeated_lines"] if kind == "response_anomaly" else [],
        },
    )
    with SQLiteEventStore(database) as store:
        store.append(event)


def test_reports_all_five_components_healthy_without_creating_evidence_database(tmp_path):
    supervisor, database = _make_supervisor(tmp_path)

    report = supervisor.get_report()

    assert report["status"] == "healthy"
    assert report["plugins"] == {
        "evidence-engine": "loaded",
        "critical-thinking-engine": "loaded",
        "reliability-engine": "loaded",
        "emotion-engine": "loaded",
    }
    assert report["event_ingress"] == "ready"
    assert report["evidence_adapter"]["python"] == "ready"
    assert report["evidence_adapter"]["source"] == "ready"
    assert report["evidence_adapter"]["database"] == "not_initialized"
    assert report["issues"] == []
    assert not database.exists()
    assert not (tmp_path / "evidence.sqlite3").exists()


def test_missing_or_disabled_engine_is_reported_as_degraded(tmp_path):
    supervisor, _ = _make_supervisor(
        tmp_path,
        loaded=EXPECTED_PLUGINS - {"emotion-engine"},
    )

    report = supervisor.get_report()

    assert report["status"] == "degraded"
    assert {
        (issue["component"], issue["code"])
        for issue in report["issues"]
    } == {("emotion-engine", "plugin_missing_or_disabled")}


def test_inactive_event_server_is_a_critical_health_issue(tmp_path):
    supervisor, _ = _make_supervisor(tmp_path, event_server=FakeServer(False))

    report = supervisor.get_report()

    assert report["status"] == "degraded"
    assert any(
        issue["component"] == "kie-event-ingress"
        and issue["code"] == "event_socket_not_running"
        and issue["severity"] == "critical"
        for issue in report["issues"]
    )


def test_recent_reliability_provider_error_is_reported_without_raw_diagnostics(tmp_path):
    now = datetime.now(timezone.utc)
    supervisor, database = _make_supervisor(tmp_path, now=now)
    _append_reliability_event(database, kind="provider_error", recorded_at=now)

    report = supervisor.get_report()

    assert report["status"] == "degraded"
    issue = next(
        issue for issue in report["issues"]
        if issue["code"] == "reliability_provider_error"
    )
    assert issue["severity"] == "warning"
    assert "private" not in str(report).lower()
    assert "exception" not in str(report).lower()


def test_recent_response_anomaly_is_reported_but_old_events_expire(tmp_path):
    now = datetime.now(timezone.utc)
    supervisor, database = _make_supervisor(tmp_path, now=now)
    _append_reliability_event(
        database,
        kind="response_anomaly",
        recorded_at=now - timedelta(minutes=20),
    )

    report = supervisor.get_report()

    assert report["status"] == "healthy"
    assert not any(issue["code"] == "reliability_response_anomaly" for issue in report["issues"])


def test_alert_hook_notifies_only_on_changes_and_announces_recovery(tmp_path):
    supervisor, _ = _make_supervisor(
        tmp_path,
        loaded=EXPECTED_PLUGINS - {"emotion-engine"},
    )

    first = supervisor.on_pre_llm_call()
    repeated = supervisor.on_pre_llm_call()
    supervisor.ctx.loaded.add("emotion-engine")
    recovered = supervisor.on_pre_llm_call()

    assert first and "emotion-engine" in first["context"]
    assert repeated is None
    assert recovered and "recovered" in recovered["context"].lower()


def test_unavailable_kie_database_does_not_get_created_by_health_check(tmp_path):
    supervisor, database = _make_supervisor(
        tmp_path,
        loaded=EXPECTED_PLUGINS,
    )
    database.parent.joinpath("missing").mkdir()
    supervisor.kie_database = database.parent / "missing" / "not-created.sqlite3"

    report = supervisor.get_report()

    assert report["status"] == "healthy"
    assert report["event_store"] == "not_initialized"
    assert not supervisor.kie_database.exists()



def test_critical_reliability_snapshot_creates_critical_health_issue(tmp_path):
    now = datetime.now(timezone.utc)
    supervisor, database = _make_supervisor(tmp_path, now=now)
    event = IntegrationEvent(
        event_id=uuid4(),
        schema_version=1,
        event_type="reliability.runtime_snapshot",
        producer_engine="reliability",
        producer_version="0.1.0",
        occurred_at=now,
        recorded_at=now,
        epistemic_status=EpistemicStatus.OBSERVED,
        sensitivity=Sensitivity.INTERNAL,
        allowed_consumers=("critical_thinking",),
        payload={
            "api_requests": 4,
            "api_successes": 2,
            "api_errors": 2,
            "tool_calls": 3,
            "tool_blocks": 1,
            "anomaly_count": 2,
            "context_pressure": "critical",
            "context_warning_events": 1,
            "context_critical_events": 1,
            "model_turns": 3,
            "user_task_labels": 0,
        },
    )
    with SQLiteEventStore(database) as store:
        store.append(event)

    report = supervisor.get_report()
    codes = {issue["code"]: issue["severity"] for issue in report["issues"]}

    assert report["status"] == "degraded"
    assert codes["reliability_context_pressure_critical"] == "critical"
    assert codes["reliability_api_errors"] == "warning"
    assert codes["reliability_response_anomalies"] == "warning"



def test_recent_evidence_policy_failure_is_reported_without_policy_details(tmp_path):
    now = datetime.now(timezone.utc)
    supervisor, database = _make_supervisor(tmp_path, now=now)
    event = IntegrationEvent(
        event_id=uuid4(),
        schema_version=1,
        event_type="evidence.verification_unavailable",
        producer_engine="evidence",
        producer_version="0.2.0",
        occurred_at=now,
        recorded_at=now,
        epistemic_status=EpistemicStatus.OBSERVED,
        sensitivity=Sensitivity.INTERNAL,
        allowed_consumers=("reliability",),
        payload={"failure_code": "policy_invalid"},
    )
    with SQLiteEventStore(database) as store:
        store.append(event)

    report = supervisor.get_report()
    issue = next(item for item in report["issues"] if item["code"] == "evidence_verification_unavailable")

    assert report["status"] == "degraded"
    assert issue["severity"] == "warning"
    assert "policy_invalid" not in issue["summary"]
    assert "path" not in issue["summary"].lower()



def test_inspection_failure_alert_is_deduplicated(tmp_path):
    supervisor, _ = _make_supervisor(tmp_path)

    def broken_report():
        raise OSError("private database path")

    supervisor.get_report = broken_report

    first = supervisor.on_pre_llm_call()
    repeated = supervisor.on_pre_llm_call()

    assert first and "could not complete its health inspection" in first["context"]
    assert repeated is None
