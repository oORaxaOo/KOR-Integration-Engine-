import asyncio
import sys
from pathlib import Path
from uuid import uuid4

import pytest

from kie.transports.evidence_stdio import EvidenceStdioConfig, EvidenceStdioTransport, EvidenceTransportError


def make_transport(tmp_path: Path, bridge_source: str, *, timeout=3, max_response_bytes=8192):
    source = tmp_path / "evidence-source"
    package = source / "src" / "evidence_engine"
    package.mkdir(parents=True)
    (package / "__init__.py").write_text("", encoding="utf-8")
    (package / "bridge.py").write_text(bridge_source, encoding="utf-8")
    database = tmp_path / "evidence.sqlite3"
    database.write_bytes(b"placeholder")
    config = EvidenceStdioConfig(
        python_executable=Path(sys.executable),
        evidence_source_root=source,
        database_path=database,
        timeout_seconds=timeout,
        max_response_bytes=max_response_bytes,
    )
    return EvidenceStdioTransport(config)


def test_transport_invokes_only_the_read_only_task_report_method(tmp_path):
    bridge = """
import json
import sys
for raw in sys.stdin:
    request = json.loads(raw)
    print(json.dumps({"jsonrpc": "2.0", "id": request["id"],
        "result": {"method": request["method"], "params": request["params"]}}), flush=True)
"""
    transport = make_transport(tmp_path, bridge)
    task_id = str(uuid4())
    response = asyncio.run(transport.call(
        "get_task_report",
        {"task_id": task_id, "evidence_limit": 10, "evidence_offset": 0},
        request_id="kie:test:1",
    ))
    assert response == {
        "jsonrpc": "2.0", "id": "kie:test:1",
        "result": {"method": "get_task_report",
                   "params": {"task_id": task_id, "evidence_limit": 10, "evidence_offset": 0}},
    }


def test_transport_rejects_any_method_other_than_get_task_report(tmp_path):
    transport = make_transport(tmp_path, "raise SystemExit(0)")
    with pytest.raises(ValueError, match="only get_task_report"):
        asyncio.run(transport.call("record_evidence", {"task_id": str(uuid4())}, request_id="blocked"))


@pytest.mark.parametrize("params", [
    {"task_id": "not-a-uuid", "evidence_limit": 10, "evidence_offset": 0},
    {"task_id": str(uuid4()), "evidence_limit": 0, "evidence_offset": 0},
    {"task_id": str(uuid4()), "evidence_limit": 10, "evidence_offset": -1},
    {"task_id": str(uuid4()), "evidence_limit": 10, "evidence_offset": 0, "sql": "DELETE"},
])
def test_transport_rejects_unexpected_or_invalid_parameters(tmp_path, params):
    transport = make_transport(tmp_path, "raise SystemExit(0)")
    with pytest.raises(ValueError):
        asyncio.run(transport.call("get_task_report", params, request_id="invalid"))


def test_transport_times_out_and_terminates_the_bridge(tmp_path):
    transport = make_transport(tmp_path, "import time\ntime.sleep(5)", timeout=0.1)
    with pytest.raises(EvidenceTransportError, match="timed out"):
        asyncio.run(transport.call(
            "get_task_report", {"task_id": str(uuid4()), "evidence_limit": 10, "evidence_offset": 0},
            request_id="timeout",
        ))


def test_transport_rejects_oversized_bridge_responses(tmp_path):
    bridge = "import sys\nsys.stdin.readline()\nprint('x' * 2048, flush=True)"
    transport = make_transport(tmp_path, bridge, max_response_bytes=256)
    with pytest.raises(EvidenceTransportError, match="response"):
        asyncio.run(transport.call(
            "get_task_report", {"task_id": str(uuid4()), "evidence_limit": 10, "evidence_offset": 0},
            request_id="oversized",
        ))


def test_transport_rejects_non_json_bridge_output(tmp_path):
    bridge = "import sys\nsys.stdin.readline()\nprint('not-json', flush=True)"
    transport = make_transport(tmp_path, bridge)
    with pytest.raises(EvidenceTransportError, match="invalid JSON-RPC"):
        asyncio.run(transport.call(
            "get_task_report", {"task_id": str(uuid4()), "evidence_limit": 10, "evidence_offset": 0},
            request_id="bad-json",
        ))


def test_transport_requires_existing_operator_paths(tmp_path):
    source = tmp_path / "empty-source"
    source.mkdir()
    with pytest.raises(ValueError, match="bridge"):
        EvidenceStdioConfig(
            python_executable=Path(sys.executable),
            evidence_source_root=source,
            database_path=tmp_path / "missing.sqlite3",
        )


def test_transport_forwards_only_required_runtime_loader_environment(tmp_path, monkeypatch):
    monkeypatch.setenv("LD_LIBRARY_PATH", "/test/python/lib")
    monkeypatch.setenv("KOR_TEST_SECRET", "do-not-forward")
    bridge = """
import json
import os
import sys
sys.stdin.readline()
print(json.dumps({
    "jsonrpc": "2.0",
    "id": "env-check",
    "result": {
        "loader": os.environ.get("LD_LIBRARY_PATH"),
        "secret": os.environ.get("KOR_TEST_SECRET"),
        "pythonpath": os.environ.get("PYTHONPATH"),
        "no_user_site": os.environ.get("PYTHONNOUSERSITE"),
    },
}), flush=True)
"""
    transport = make_transport(tmp_path, bridge)
    result = asyncio.run(transport.call(
        "get_task_report",
        {"task_id": str(uuid4()), "evidence_limit": 10, "evidence_offset": 0},
        request_id="env-check",
    ))

    assert result["result"]["loader"] == "/test/python/lib"
    assert result["result"]["secret"] is None
    assert result["result"]["no_user_site"] == "1"
    assert result["result"]["pythonpath"].endswith("/src")
