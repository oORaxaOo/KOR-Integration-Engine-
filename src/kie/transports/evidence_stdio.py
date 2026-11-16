"""Bounded stdio transport for the Evidence Engine's read-only JSON-RPC bridge."""
from __future__ import annotations

import asyncio
from dataclasses import dataclass
import json
import os
from pathlib import Path
from typing import Any, Mapping
import uuid

_MAX_REQUEST_BYTES = 1024 * 1024
_MAX_RESPONSE_BYTES = 1024 * 1024


class EvidenceTransportError(ConnectionError):
    """The configured read-only Evidence bridge could not return one valid response."""


def _absolute_existing_file(value: str | Path, field: str, *, executable: bool = False) -> Path:
    try:
        path = Path(value).expanduser()
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field} must be an absolute path") from exc
    if not path.is_absolute():
        raise ValueError(f"{field} must be an absolute path")
    try:
        resolved = path.resolve(strict=True)
    except OSError as exc:
        raise ValueError(f"{field} does not exist") from exc
    if not resolved.is_file():
        raise ValueError(f"{field} must be a regular file")
    if executable and not os.access(resolved, os.X_OK):
        raise ValueError(f"{field} must be executable")
    return resolved


@dataclass(frozen=True, slots=True)
class EvidenceStdioConfig:
    """Trusted operator configuration for one existing Evidence Engine checkout/database."""

    python_executable: Path
    evidence_source_root: Path
    database_path: Path
    timeout_seconds: float = 10.0
    max_response_bytes: int = _MAX_RESPONSE_BYTES

    def __post_init__(self) -> None:
        python_path = _absolute_existing_file(
            self.python_executable, "python_executable", executable=True
        )
        try:
            source_root = Path(self.evidence_source_root).expanduser()
        except (TypeError, ValueError) as exc:
            raise ValueError("evidence_source_root must be an absolute path") from exc
        if not source_root.is_absolute():
            raise ValueError("evidence_source_root must be an absolute path")
        try:
            source_root = source_root.resolve(strict=True)
        except OSError as exc:
            raise ValueError("evidence_source_root does not exist") from exc
        bridge = source_root / "src" / "evidence_engine" / "bridge.py"
        if not bridge.is_file():
            raise ValueError("evidence_source_root does not contain src/evidence_engine/bridge.py")
        database = _absolute_existing_file(self.database_path, "database")
        if (
            isinstance(self.timeout_seconds, bool)
            or not isinstance(self.timeout_seconds, (int, float))
            or not 0.1 <= float(self.timeout_seconds) <= 120.0
        ):
            raise ValueError("timeout_seconds must be from 0.1 to 120 seconds")
        if (
            isinstance(self.max_response_bytes, bool)
            or not isinstance(self.max_response_bytes, int)
            or not 256 <= self.max_response_bytes <= 4 * 1024 * 1024
        ):
            raise ValueError("max_response_bytes must be from 256 bytes to 4 MiB")
        object.__setattr__(self, "python_executable", python_path)
        object.__setattr__(self, "evidence_source_root", source_root)
        object.__setattr__(self, "database_path", database)
        object.__setattr__(self, "timeout_seconds", float(self.timeout_seconds))


class EvidenceStdioTransport:
    """Start one fixed, read-only bridge process for each request."""

    def __init__(self, config: EvidenceStdioConfig) -> None:
        if not isinstance(config, EvidenceStdioConfig):
            raise TypeError("config must be an EvidenceStdioConfig")
        self.config = config

    @staticmethod
    def _validate_call(method: str, params: Mapping[str, Any], request_id: str) -> dict[str, Any]:
        if method != "get_task_report":
            raise ValueError("Evidence stdio transport permits only get_task_report")
        if not isinstance(params, Mapping) or set(params) != {
            "task_id", "evidence_limit", "evidence_offset"
        }:
            raise ValueError("get_task_report parameters are invalid")
        task_id = params.get("task_id")
        if not isinstance(task_id, str):
            raise ValueError("task_id must be a canonical UUID")
        try:
            if str(uuid.UUID(task_id)) != task_id:
                raise ValueError("task_id must be a canonical UUID")
        except ValueError as exc:
            raise ValueError("task_id must be a canonical UUID") from exc
        limit = params.get("evidence_limit")
        offset = params.get("evidence_offset")
        if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= 100:
            raise ValueError("evidence_limit must be an integer from 1 to 100")
        if isinstance(offset, bool) or not isinstance(offset, int) or offset != 0:
            raise ValueError("evidence_offset must be zero")
        if not isinstance(request_id, str) or not request_id.strip() or len(request_id) > 256:
            raise ValueError("request_id must be a non-empty string of at most 256 characters")
        return {
            "jsonrpc": "2.0",
            "id": request_id,
            "method": "get_task_report",
            "params": {
                "task_id": task_id,
                "evidence_limit": limit,
                "evidence_offset": 0,
            },
        }

    async def _stop_process(self, process: asyncio.subprocess.Process) -> None:
        if process.returncode is None:
            try:
                process.kill()
            except ProcessLookupError:
                pass
        try:
            await process.wait()
        except (ProcessLookupError, OSError):
            pass

    async def call(
        self, method: str, params: Mapping[str, Any], *, request_id: str
    ) -> Mapping[str, Any]:
        """Send one bounded JSON-RPC request; never invoke a shell or arbitrary command."""
        request = self._validate_call(method, params, request_id)
        request_line = json.dumps(
            request, ensure_ascii=False, allow_nan=False, separators=(",", ":")
        ).encode("utf-8")
        if len(request_line) > _MAX_REQUEST_BYTES:
            raise ValueError("Evidence request exceeds the maximum size")

        environment = {
            "PYTHONPATH": str(self.config.evidence_source_root / "src"),
            "PYTHONNOUSERSITE": "1",
            "PYTHONUNBUFFERED": "1",
        }
        process: asyncio.subprocess.Process | None = None
        try:
            async with asyncio.timeout(self.config.timeout_seconds):
                process = await asyncio.create_subprocess_exec(
                    str(self.config.python_executable),
                    "-m",
                    "evidence_engine.bridge",
                    "--db",
                    str(self.config.database_path),
                    "--max-line-bytes",
                    str(_MAX_REQUEST_BYTES),
                    stdin=asyncio.subprocess.PIPE,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.DEVNULL,
                    env=environment,
                    limit=self.config.max_response_bytes + 1,
                )
                assert process.stdin is not None and process.stdout is not None
                process.stdin.write(request_line + b"\n")
                await process.stdin.drain()
                process.stdin.close()
                response_line = await process.stdout.readline()
                if (
                    not response_line
                    or len(response_line) > self.config.max_response_bytes
                    or not response_line.endswith(b"\n")
                ):
                    raise EvidenceTransportError("Evidence bridge response is empty or oversized")
                await process.wait()
                if await process.stdout.read(1):
                    raise EvidenceTransportError("Evidence bridge returned extra stdout data")
                if process.returncode != 0:
                    raise EvidenceTransportError("Evidence bridge exited unsuccessfully")
                try:
                    decoded = json.loads(response_line.decode("utf-8"))
                except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                    raise EvidenceTransportError("Evidence bridge returned invalid JSON-RPC") from exc
                if not isinstance(decoded, dict):
                    raise EvidenceTransportError("Evidence bridge returned invalid JSON-RPC")
                return decoded
        except TimeoutError as exc:
            if process is not None:
                await self._stop_process(process)
            raise EvidenceTransportError("Evidence bridge timed out") from exc
        except asyncio.CancelledError:
            if process is not None:
                await self._stop_process(process)
            raise
        except EvidenceTransportError:
            if process is not None:
                await self._stop_process(process)
            raise
        except (OSError, ValueError, asyncio.LimitOverrunError) as exc:
            if process is not None:
                await self._stop_process(process)
            raise EvidenceTransportError("Evidence bridge could not be started or read") from exc
        finally:
            if process is not None and process.returncode is None:
                await self._stop_process(process)
