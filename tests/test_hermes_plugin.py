import asyncio
import json
import sys
from pathlib import Path
from uuid import uuid4

from kie import EpistemicStatus, SQLiteEventStore
from kie.hermes_plugin import register


def make_bridge_source(task_id: str) -> str:
    report = {
        "task": {"task_id": task_id, "title": "Private title", "status": "passed",
                 "created_at": "2026-11-16T09:00:00+00:00",
                 "criteria": [{"kind": "command_result", "field": "return_code"}]},
        "effective_status": "passed", "reliability_note": None,
        "ledger_integrity": True, "verification_current": True,
        "verification_record_valid": True, "latest_evidence_sequence": 1,
        "evidence_total": 1, "evidence_offset": 0, "evidence_limit": 10,
        "evidence_truncated": False,
        "evidence": [{"sequence": 1, "evidence_id": str(uuid4()),
                      "kind": "command_result", "source": "private-command-policy",
                      "observed_at": "2026-11-16T09:01:00+00:00",
                      "payload": {"stdout": "secret output must remain inside Evidence Engine"}}],
        "verification": {"verification_id": "verification-123", "status": "supported",
                         "verified_at": "2026-11-16T09:02:00+00:00",
                         "evidence_sequence": 1, "record_consistent": True,
                         "report": {"checks": [{"detail": "private check"}]}},
    }
    return f"""
import json
import sys
for raw in sys.stdin:
    request = json.loads(raw)
    assert request["method"] == "get_task_report"
    assert request["params"]["task_id"] == {task_id!r}
    print(json.dumps({{"jsonrpc": "2.0", "id": request["id"], "result": {json.dumps(report)}}}), flush=True)
"""


class FakeHermesContext:
    def __init__(self, settings):
        self.settings = settings
        self.tools = {}

    def get_config(self, key, default=None):
        current = self.settings
        for part in key.split("."):
            if not isinstance(current, dict) or part not in current:
                return default
            current = current[part]
        return current

    def register_tool(self, **kwargs):
        self.tools[kwargs["name"]] = kwargs


def test_register_adds_one_async_read_only_evidence_tool():
    ctx = FakeHermesContext({})
    register(ctx)
    assert set(ctx.tools) == {"kie_evidence_lookup"}
    entry = ctx.tools["kie_evidence_lookup"]
    assert entry["is_async"] is True
    assert entry["toolset"] == "kor_kie"
    assert entry["schema"]["parameters"]["required"] == ["evidence_task_id"]


def test_hermes_tool_runs_evidence_adapter_through_kie_and_persists_events(tmp_path):
    task_id = str(uuid4())
    source = tmp_path / "evidence-source"
    package = source / "src" / "evidence_engine"
    package.mkdir(parents=True)
    (package / "__init__.py").write_text("", encoding="utf-8")
    (package / "bridge.py").write_text(make_bridge_source(task_id), encoding="utf-8")
    evidence_db = tmp_path / "evidence.sqlite3"
    evidence_db.write_bytes(b"placeholder")
    kie_db = tmp_path / "kie.sqlite3"
    ctx = FakeHermesContext({
        "evidence_python": sys.executable,
        "evidence_source_root": str(source),
        "evidence_database": str(evidence_db),
        "kie_database": str(kie_db),
        "timeout_seconds": 5,
    })
    register(ctx)
    handler = ctx.tools["kie_evidence_lookup"]["handler"]
    result = json.loads(asyncio.run(handler({"evidence_task_id": task_id}))

    assert result["ok"] is True
    assert result["step_status"] == "completed"
    assert result["epistemic_status"] == "verified"
    assert result["summary"]["effective_status"] == "passed"
    assert "Private title" not in json.dumps(result)
    assert "secret output" not in json.dumps(result)
    assert "private check" not in json.dumps(result)

    with SQLiteEventStore(kie_db) as store:
        records = store.read_events(limit=20)
        assert len(records) == 2
        request_event = next(r.event for r in records if r.event.event_type == "evidence.lookup_request")
        result_event = next(r.event for r in records if r.event.event_type == "evidence.task_snapshot")
        assert request_event.producer_engine == "hermes_host"
        assert request_event.allowed_consumers == ("evidence",)
        assert result_event.producer_engine == "evidence"
        assert result_event.epistemic_status is EpistemicStatus.VERIFIED


def test_hermes_tool_rejects_invalid_task_uuid_before_opening_kie_database(tmp_path):
    ctx = FakeHermesContext({
        "evidence_python": sys.executable,
        "evidence_source_root": str(tmp_path),
        "evidence_database": str(tmp_path / "evidence.sqlite3"),
        "kie_database": str(tmp_path / "kie.sqlite3"),
    })
    register(ctx)
    handler = ctx.tools["kie_evidence_lookup"]["handler"]
    result = json.loads(asyncio.run(handler({"evidence_task_id": "../bad"})))
    assert result["ok"] is False
    assert result["reason"] == "invalid_evidence_task_id"
    assert not (tmp_path / "kie.sqlite3").exists()


def test_hermes_tool_fails_closed_when_settings_are_missing():
    ctx = FakeHermesContext({})
    register(ctx)
    handler = ctx.tools["kie_evidence_lookup"]["handler"]
    result = json.loads(asyncio.run(handler({"evidence_task_id": str(uuid4())})))
    assert result["ok"] is False
    assert result["reason"] == "integration_not_configured"
