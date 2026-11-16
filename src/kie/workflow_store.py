"""Durable workflow execution state, separate from engine execution.

The store persists declared state and transitions. It never starts an engine,
calls Hermes, retries work automatically, or executes historical transitions.
It can share a SQLite file with SQLiteEventStore.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from enum import Enum
import json
from pathlib import Path
import sqlite3
from threading import RLock
from typing import Any
from uuid import UUID, uuid4

from .workflows import StepPlanStatus, WorkflowDefinition, WorkflowPlan

_SCHEMA_VERSION = 1
_MAX_RETRIES = 10


class WorkflowStoreError(RuntimeError):
    """Base exception for workflow persistence failures."""


class RunNotFoundError(WorkflowStoreError):
    """The requested workflow run does not exist."""


class StepNotFoundError(WorkflowStoreError):
    """The requested step does not exist in this run."""


class InvalidWorkflowTransition(WorkflowStoreError):
    """A requested state transition is not permitted."""


class RetryLimitExceeded(WorkflowStoreError):
    """The configured retry limit has been reached."""


class RunStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    PARTIAL = "partial"
    FAILED = "failed"
    BLOCKED = "blocked"
    CANCELLED = "cancelled"


class StepStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    SKIPPED = "skipped"
    UNAVAILABLE = "unavailable"
    NO_DATA = "no_data"
    FAILED = "failed"
    TIMED_OUT = "timed_out"
    CANCELLED = "cancelled"
    BLOCKED = "blocked"


@dataclass(frozen=True, slots=True)
class WorkflowStepRecord:
    step_id: str
    engine_id: str
    depends_on: tuple[str, ...]
    required: bool
    timeout_seconds: int
    status: StepStatus
    attempt_count: int
    max_retries: int
    started_at: datetime | None
    deadline_at: datetime | None
    finished_at: datetime | None
    result_event_id: UUID | None
    reason: str | None


@dataclass(frozen=True, slots=True)
class WorkflowRunRecord:
    run_id: UUID
    workflow_id: str
    workflow_version: str
    status: RunStatus
    created_at: datetime
    updated_at: datetime
    reason: str | None
    max_retries: int
    steps: tuple[WorkflowStepRecord, ...]


@dataclass(frozen=True, slots=True)
class WorkflowTransitionRecord:
    sequence: int
    run_id: UUID
    step_id: str | None
    from_status: str | None
    to_status: str
    reason: str | None
    occurred_at: datetime
    attempt_count: int


def _aware(value: datetime, field: str) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field} must be a timezone-aware datetime")
    return value


def _iso(value: datetime) -> str:
    return _aware(value, "timestamp").astimezone(timezone.utc).isoformat(timespec="microseconds")


def _parse_datetime(value: str | None) -> datetime | None:
    if value is None:
        return None
    parsed = datetime.fromisoformat(value)
    return _aware(parsed, "stored timestamp")


def _json_list(values: tuple[str, ...]) -> str:
    return json.dumps(list(values), ensure_ascii=False, separators=(",", ":"))


def _parse_dependencies(value: str) -> tuple[str, ...]:
    decoded = json.loads(value)
    if not isinstance(decoded, list) or any(not isinstance(item, str) for item in decoded):
        raise WorkflowStoreError("stored workflow dependency list is invalid")
    return tuple(decoded)


class SQLiteWorkflowStore:
    """Persist workflow runs and an append-only transition history.

    The database file may be the same file used by SQLiteEventStore. This
    component tracks its schema in its own metadata table and does not change
    SQLite's global PRAGMA user_version.
    """

    def __init__(self, database: str | Path, *, timeout_seconds: float = 5.0) -> None:
        if isinstance(timeout_seconds, bool) or not isinstance(timeout_seconds, (int, float)) or not 0 < timeout_seconds <= 60:
            raise ValueError("timeout_seconds must be greater than 0 and at most 60 seconds")
        database_value = str(database)
        if not database_value:
            raise ValueError("database path must not be empty")
        if database_value != ":memory:" and not database_value.startswith("file:"):
            path = Path(database_value).expanduser()
            path.parent.mkdir(parents=True, exist_ok=True)
            database_value = str(path)

        self.database = database_value
        self.timeout_seconds = float(timeout_seconds)
        self._lock = RLock()
        self._closed = False
        self._connection = sqlite3.connect(
            database_value,
            timeout=self.timeout_seconds,
            isolation_level=None,
            check_same_thread=False,
            uri=database_value.startswith("file:"),
        )
        self._connection.row_factory = sqlite3.Row
        try:
            self._initialize()
        except Exception:
            self._connection.close()
            self._closed = True
            raise

    def _initialize(self) -> None:
        connection = self._connection
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute(f"PRAGMA busy_timeout = {int(self.timeout_seconds * 1000)}")
        connection.execute("PRAGMA synchronous = FULL")
        if self.database != ":memory:":
            connection.execute("PRAGMA journal_mode = WAL")
        connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS kie_workflow_meta (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS kie_workflow_runs (
                run_id TEXT PRIMARY KEY,
                workflow_id TEXT NOT NULL,
                workflow_version TEXT NOT NULL,
                status TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                reason TEXT,
                max_retries INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS kie_workflow_steps (
                run_id TEXT NOT NULL,
                step_id TEXT NOT NULL,
                engine_id TEXT NOT NULL,
                depends_on_json TEXT NOT NULL,
                required INTEGER NOT NULL CHECK (required IN (0, 1)),
                timeout_seconds INTEGER NOT NULL,
                status TEXT NOT NULL,
                attempt_count INTEGER NOT NULL DEFAULT 0,
                max_retries INTEGER NOT NULL,
                started_at TEXT,
                deadline_at TEXT,
                finished_at TEXT,
                result_event_id TEXT,
                reason TEXT,
                PRIMARY KEY (run_id, step_id),
                FOREIGN KEY (run_id) REFERENCES kie_workflow_runs(run_id)
            );

            CREATE TABLE IF NOT EXISTS kie_workflow_transitions (
                sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id TEXT NOT NULL,
                step_id TEXT,
                from_status TEXT,
                to_status TEXT NOT NULL,
                reason TEXT,
                occurred_at TEXT NOT NULL,
                attempt_count INTEGER NOT NULL,
                FOREIGN KEY (run_id) REFERENCES kie_workflow_runs(run_id)
            );

            CREATE INDEX IF NOT EXISTS kie_workflow_runs_status_updated
                ON kie_workflow_runs(status, updated_at);

            CREATE INDEX IF NOT EXISTS kie_workflow_steps_status_deadline
                ON kie_workflow_steps(status, deadline_at);

            CREATE INDEX IF NOT EXISTS kie_workflow_transitions_run_sequence
                ON kie_workflow_transitions(run_id, sequence);

            CREATE TRIGGER IF NOT EXISTS kie_workflow_transitions_no_update
            BEFORE UPDATE ON kie_workflow_transitions
            BEGIN
                SELECT RAISE(ABORT, 'kie_workflow_transitions is append-only: UPDATE rejected');
            END;

            CREATE TRIGGER IF NOT EXISTS kie_workflow_transitions_no_delete
            BEFORE DELETE ON kie_workflow_transitions
            BEGIN
                SELECT RAISE(ABORT, 'kie_workflow_transitions is append-only: DELETE rejected');
            END;
            """
        )
        row = connection.execute(
            "SELECT value FROM kie_workflow_meta WHERE key = 'schema_version'"
        ).fetchone()
        if row is None:
            connection.execute(
                "INSERT INTO kie_workflow_meta(key, value) VALUES('schema_version', ?)",
                (str(_SCHEMA_VERSION),),
            )
        else:
            version = int(row["value"])
            if version > _SCHEMA_VERSION:
                raise WorkflowStoreError(
                    f"workflow schema version {version} is newer than supported version {_SCHEMA_VERSION}"
                )
            if version < _SCHEMA_VERSION:
                raise WorkflowStoreError("workflow schema migration is required")

    def _assert_open(self) -> None:
        if self._closed:
            raise RuntimeError("workflow store is closed")

    def _run_row(self, run_id: UUID | str) -> sqlite3.Row:
        try:
            normalized = str(run_id if isinstance(run_id, UUID) else UUID(str(run_id)))
        except (TypeError, ValueError) as exc:
            raise ValueError("run_id must be a UUID") from exc
        row = self._connection.execute(
            "SELECT * FROM kie_workflow_runs WHERE run_id = ?", (normalized,)
        ).fetchone()
        if row is None:
            raise RunNotFoundError(f"workflow run {normalized} does not exist")
        return row

    def _step_row(self, run_id: UUID | str, step_id: str) -> sqlite3.Row:
        row = self._connection.execute(
            "SELECT * FROM kie_workflow_steps WHERE run_id = ? AND step_id = ?",
            (str(run_id), step_id),
        ).fetchone()
        if row is None:
            raise StepNotFoundError(f"workflow step {step_id!r} does not exist in this run")
        return row

    def _transition(
        self, connection: sqlite3.Connection, *, run_id: str, step_id: str | None,
        from_status: str | None, to_status: str, reason: str | None,
        now: datetime, attempt_count: int,
    ) -> None:
        connection.execute(
            """
            INSERT INTO kie_workflow_transitions(
                run_id, step_id, from_status, to_status, reason, occurred_at, attempt_count
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (run_id, step_id, from_status, to_status, reason, _iso(now), attempt_count),
        )

    def _set_run_status(
        self, connection: sqlite3.Connection, run_row: sqlite3.Row,
        new_status: RunStatus, *, reason: str | None, now: datetime,
    ) -> None:
        old_status = run_row["status"]
        if old_status == new_status.value and run_row["reason"] == reason:
            return
        connection.execute(
            "UPDATE kie_workflow_runs SET status = ?, updated_at = ?, reason = ? WHERE run_id = ?",
            (new_status.value, _iso(now), reason, run_row["run_id"]),
        )
        self._transition(
            connection, run_id=run_row["run_id"], step_id=None,
            from_status=old_status, to_status=new_status.value, reason=reason,
            now=now, attempt_count=0,
        )

    def _refresh_run_status(self, connection: sqlite3.Connection, run_id: str, now: datetime) -> None:
        run_row = connection.execute(
            "SELECT * FROM kie_workflow_runs WHERE run_id = ?", (run_id,)
        ).fetchone()
        current = RunStatus(run_row["status"])
        if current in {RunStatus.CANCELLED, RunStatus.BLOCKED}:
            return
        steps = connection.execute(
            "SELECT required, status FROM kie_workflow_steps WHERE run_id = ?", (run_id,)
        ).fetchall()
        bad = {
            StepStatus.UNAVAILABLE.value, StepStatus.NO_DATA.value, StepStatus.FAILED.value,
            StepStatus.TIMED_OUT.value, StepStatus.BLOCKED.value, StepStatus.CANCELLED.value,
        }
        if any(bool(row["required"]) and row["status"] in bad | {StepStatus.SKIPPED.value} for row in steps):
            target, reason = RunStatus.FAILED, "required_step_did_not_complete"
        elif any(row["status"] == StepStatus.RUNNING.value for row in steps):
            target, reason = RunStatus.RUNNING, None
        elif any(row["status"] == StepStatus.PENDING.value for row in steps):
            target, reason = RunStatus.PENDING, None
        elif any(row["status"] in bad for row in steps):
            target, reason = RunStatus.PARTIAL, "optional_step_did_not_complete"
        else:
            target, reason = RunStatus.COMPLETED, None
        self._set_run_status(connection, run_row, target, reason=reason, now=now)

    def create_run(
        self, workflow: WorkflowDefinition, plan: WorkflowPlan, *,
        run_id: UUID | None = None, max_retries: int = 1,
        now: datetime | None = None,
    ) -> WorkflowRunRecord:
        if not isinstance(workflow, WorkflowDefinition) or not isinstance(plan, WorkflowPlan):
            raise TypeError("workflow and plan must be WorkflowDefinition and WorkflowPlan values")
        timestamp = _aware(now or datetime.now(timezone.utc), "now")
        if isinstance(max_retries, bool) or not isinstance(max_retries, int) or not 0 <= max_retries <= _MAX_RETRIES:
            raise ValueError(f"max_retries must be an integer from 0 to {_MAX_RETRIES}")
        selected_run_id = run_id or uuid4()
        if not isinstance(selected_run_id, UUID):
            raise ValueError("run_id must be a UUID")
        if plan.workflow_id != workflow.workflow_id or plan.version != workflow.version:
            raise ValueError("workflow plan does not match workflow identity/version")

        definitions = {step.step_id: step for step in workflow.steps}
        planned = {step.step_id: step for step in plan.steps}
        if set(definitions) != set(planned):
            raise ValueError("workflow plan steps do not match workflow definition")
        for step_id, definition in definitions.items():
            item = planned[step_id]
            if (item.engine_id, item.required, item.timeout_seconds) != (
                definition.engine_id, definition.required, definition.timeout_seconds
            ):
                raise ValueError(f"workflow plan metadata mismatch for step {step_id}")
        expected = all(not item.required or item.status is StepPlanStatus.READY for item in plan.steps)
        if plan.can_start is not expected:
            raise ValueError("workflow plan can_start flag is inconsistent with its steps")

        initial = {
            StepPlanStatus.READY: StepStatus.PENDING,
            StepPlanStatus.SKIPPED: StepStatus.SKIPPED,
            StepPlanStatus.BLOCKED: StepStatus.BLOCKED,
        }
        has_blocked = any(item.status is StepPlanStatus.BLOCKED for item in plan.steps)
        has_pending = any(item.status is StepPlanStatus.READY for item in plan.steps)
        run_status = RunStatus.BLOCKED if has_blocked else RunStatus.PENDING if has_pending else RunStatus.PARTIAL
        run_reason = "required_engine_unavailable" if has_blocked else (
            "no_ready_workflow_steps" if not has_pending else None
        )

        with self._lock:
            self._assert_open()
            connection = self._connection
            connection.execute("BEGIN IMMEDIATE")
            try:
                run_text = str(selected_run_id)
                connection.execute(
                    """
                    INSERT INTO kie_workflow_runs(
                        run_id, workflow_id, workflow_version, status, created_at,
                        updated_at, reason, max_retries
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (run_text, workflow.workflow_id, workflow.version, run_status.value,
                     _iso(timestamp), _iso(timestamp), run_reason, max_retries),
                )
                self._transition(
                    connection, run_id=run_text, step_id=None, from_status=None,
                    to_status=run_status.value, reason=run_reason, now=timestamp, attempt_count=0,
                )
                for item in plan.steps:
                    definition = definitions[item.step_id]
                    status = initial[item.status]
                    connection.execute(
                        """
                        INSERT INTO kie_workflow_steps(
                            run_id, step_id, engine_id, depends_on_json, required,
                            timeout_seconds, status, attempt_count, max_retries,
                            started_at, deadline_at, finished_at, result_event_id, reason
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, 0, ?, NULL, NULL, NULL, NULL, ?)
                        """,
                        (run_text, definition.step_id, definition.engine_id,
                         _json_list(definition.depends_on), int(definition.required),
                         definition.timeout_seconds, status.value, max_retries, item.reason),
                    )
                    self._transition(
                        connection, run_id=run_text, step_id=item.step_id, from_status=None,
                        to_status=status.value, reason=item.reason, now=timestamp, attempt_count=0,
                    )
                connection.execute("COMMIT")
            except sqlite3.IntegrityError as exc:
                if connection.in_transaction:
                    connection.execute("ROLLBACK")
                raise WorkflowStoreError(f"could not create workflow run: {exc}") from exc
            except Exception:
                if connection.in_transaction:
                    connection.execute("ROLLBACK")
                raise
            return self.get_run(selected_run_id)

    def get_run(self, run_id: UUID | str) -> WorkflowRunRecord:
        with self._lock:
            self._assert_open()
            row = self._run_row(run_id)
            step_rows = self._connection.execute(
                "SELECT * FROM kie_workflow_steps WHERE run_id = ? ORDER BY rowid",
                (row["run_id"],),
            ).fetchall()
            return WorkflowRunRecord(
                run_id=UUID(row["run_id"]), workflow_id=row["workflow_id"],
                workflow_version=row["workflow_version"], status=RunStatus(row["status"]),
                created_at=_parse_datetime(row["created_at"]),
                updated_at=_parse_datetime(row["updated_at"]), reason=row["reason"],
                max_retries=int(row["max_retries"]),
                steps=tuple(self._row_to_step(item) for item in step_rows),
            )

    @staticmethod
    def _row_to_step(row: sqlite3.Row) -> WorkflowStepRecord:
        return WorkflowStepRecord(
            step_id=row["step_id"], engine_id=row["engine_id"],
            depends_on=_parse_dependencies(row["depends_on_json"]),
            required=bool(row["required"]), timeout_seconds=int(row["timeout_seconds"]),
            status=StepStatus(row["status"]), attempt_count=int(row["attempt_count"]),
            max_retries=int(row["max_retries"]), started_at=_parse_datetime(row["started_at"]),
            deadline_at=_parse_datetime(row["deadline_at"]), finished_at=_parse_datetime(row["finished_at"]),
            result_event_id=UUID(row["result_event_id"]) if row["result_event_id"] else None,
            reason=row["reason"],
        )

    def _update_step(
        self, connection: sqlite3.Connection, row: sqlite3.Row, new_status: StepStatus, *,
        now: datetime, reason: str | None, attempt_count: int | None = None,
        started_at: datetime | None = None, deadline_at: datetime | None = None,
        finished_at: datetime | None = None, result_event_id: UUID | None = None,
        clear_result: bool = False,
    ) -> None:
        attempts = int(row["attempt_count"]) if attempt_count is None else attempt_count
        if started_at is None and new_status in {StepStatus.PENDING, StepStatus.SKIPPED, StepStatus.BLOCKED}:
            started_value = None
        else:
            started_value = _iso(started_at) if started_at else row["started_at"]
        deadline_value = _iso(deadline_at) if deadline_at else (
            None if new_status is not StepStatus.RUNNING else row["deadline_at"]
        )
        terminal = {
            StepStatus.COMPLETED, StepStatus.SKIPPED, StepStatus.UNAVAILABLE,
            StepStatus.NO_DATA, StepStatus.FAILED, StepStatus.TIMED_OUT,
            StepStatus.CANCELLED, StepStatus.BLOCKED,
        }
        finished_value = _iso(finished_at or now) if new_status in terminal else None
        result_value = None if clear_result else (
            str(result_event_id) if result_event_id is not None else row["result_event_id"]
        )
        connection.execute(
            """
            UPDATE kie_workflow_steps SET
                status = ?, attempt_count = ?, started_at = ?, deadline_at = ?,
                finished_at = ?, result_event_id = ?, reason = ?
            WHERE run_id = ? AND step_id = ?
            """,
            (new_status.value, attempts, started_value, deadline_value,
             finished_value, result_value, reason, row["run_id"], row["step_id"]),
        )
        self._transition(
            connection, run_id=row["run_id"], step_id=row["step_id"],
            from_status=row["status"], to_status=new_status.value, reason=reason,
            now=now, attempt_count=attempts,
        )

    def start_step(self, run_id: UUID | str, step_id: str, *, now: datetime | None = None) -> WorkflowStepRecord:
        timestamp = _aware(now or datetime.now(timezone.utc), "now")
        with self._lock:
            self._assert_open()
            connection = self._connection
            connection.execute("BEGIN IMMEDIATE")
            try:
                run = self._run_row(run_id)
                if RunStatus(run["status"]) not in {RunStatus.PENDING, RunStatus.RUNNING}:
                    raise InvalidWorkflowTransition(f"workflow run is {run['status']}, not startable")
                row = self._step_row(run["run_id"], step_id)
                if StepStatus(row["status"]) is not StepStatus.PENDING:
                    raise InvalidWorkflowTransition(f"step {step_id} is {row['status']}, not pending")
                dependencies = _parse_dependencies(row["depends_on_json"])
                if dependencies:
                    placeholders = ",".join("?" for _ in dependencies)
                    dep_rows = connection.execute(
                        f"SELECT step_id, status FROM kie_workflow_steps WHERE run_id = ? AND step_id IN ({placeholders})",
                        (run["run_id"], *dependencies),
                    ).fetchall()
                    dependency_states = {item["step_id"]: StepStatus(item["status"]) for item in dep_rows}
                    failed = tuple(name for name in dependencies if dependency_states[name] is not StepStatus.COMPLETED)
                    if failed:
                        permanently_unready = any(
                            dependency_states[name] in {
                                StepStatus.SKIPPED, StepStatus.UNAVAILABLE, StepStatus.NO_DATA,
                                StepStatus.FAILED, StepStatus.TIMED_OUT, StepStatus.CANCELLED,
                                StepStatus.BLOCKED,
                            }
                            for name in failed
                        )
                        if permanently_unready:
                            new_status = StepStatus.BLOCKED if bool(row["required"]) else StepStatus.SKIPPED
                            self._update_step(
                                connection, row, new_status, now=timestamp,
                                reason="dependency_not_completed:" + ",".join(failed),
                            )
                            self._refresh_run_status(connection, run["run_id"], timestamp)
                            result = self._step_row(run["run_id"], step_id)
                            connection.execute("COMMIT")
                            return self._row_to_step(result)
                        raise InvalidWorkflowTransition(
                            f"step {step_id} dependencies are not completed: {', '.join(failed)}"
                        )
                attempts = int(row["attempt_count"]) + 1
                deadline = timestamp + timedelta(seconds=int(row["timeout_seconds"]))
                self._update_step(
                    connection, row, StepStatus.RUNNING, now=timestamp, reason=None,
                    attempt_count=attempts, started_at=timestamp, deadline_at=deadline,
                    clear_result=True,
                )
                self._refresh_run_status(connection, run["run_id"], timestamp)
                result = self._step_row(run["run_id"], step_id)
                connection.execute("COMMIT")
                return self._row_to_step(result)
            except Exception:
                if connection.in_transaction:
                    connection.execute("ROLLBACK")
                raise

    def _finish_step(
        self, run_id: UUID | str, step_id: str, status: StepStatus, *,
        reason: str | None, now: datetime | None,
        result_event_id: UUID | str | None = None,
    ) -> WorkflowStepRecord:
        timestamp = _aware(now or datetime.now(timezone.utc), "now")
        event_id: UUID | None = None
        if result_event_id is not None:
            try:
                event_id = result_event_id if isinstance(result_event_id, UUID) else UUID(str(result_event_id))
            except (TypeError, ValueError) as exc:
                raise ValueError("result_event_id must be a UUID") from exc
        if status is not StepStatus.COMPLETED and (not isinstance(reason, str) or not reason.strip()):
            raise ValueError("a non-completed terminal step requires a non-empty reason")
        if reason is not None and (not isinstance(reason, str) or not reason.strip()):
            raise ValueError("reason must be None or a non-empty string")
        with self._lock:
            self._assert_open()
            connection = self._connection
            connection.execute("BEGIN IMMEDIATE")
            try:
                run = self._run_row(run_id)
                if RunStatus(run["status"]) in {RunStatus.CANCELLED, RunStatus.BLOCKED}:
                    raise InvalidWorkflowTransition(f"workflow run is {run['status']}")
                row = self._step_row(run["run_id"], step_id)
                if StepStatus(row["status"]) is not StepStatus.RUNNING:
                    raise InvalidWorkflowTransition(f"step {step_id} is not running")
                self._update_step(
                    connection, row, status, now=timestamp, reason=reason,
                    finished_at=timestamp, result_event_id=event_id,
                    clear_result=(status is not StepStatus.COMPLETED),
                )
                self._refresh_run_status(connection, run["run_id"], timestamp)
                result = self._step_row(run["run_id"], step_id)
                connection.execute("COMMIT")
                return self._row_to_step(result)
            except Exception:
                if connection.in_transaction:
                    connection.execute("ROLLBACK")
                raise

    def complete_step(
        self, run_id: UUID | str, step_id: str, *,
        result_event_id: UUID | str | None = None, now: datetime | None = None,
    ) -> WorkflowStepRecord:
        return self._finish_step(
            run_id, step_id, StepStatus.COMPLETED, reason=None,
            now=now, result_event_id=result_event_id,
        )

    def fail_step(self, run_id: UUID | str, step_id: str, *, reason: str, now: datetime | None = None) -> WorkflowStepRecord:
        return self._finish_step(run_id, step_id, StepStatus.FAILED, reason=reason, now=now)

    def mark_no_data(self, run_id: UUID | str, step_id: str, *, reason: str, now: datetime | None = None) -> WorkflowStepRecord:
        return self._finish_step(run_id, step_id, StepStatus.NO_DATA, reason=reason, now=now)

    def mark_unavailable(self, run_id: UUID | str, step_id: str, *, reason: str, now: datetime | None = None) -> WorkflowStepRecord:
        timestamp = _aware(now or datetime.now(timezone.utc), "now")
        if not isinstance(reason, str) or not reason.strip():
            raise ValueError("reason must be a non-empty string")
        with self._lock:
            self._assert_open()
            connection = self._connection
            connection.execute("BEGIN IMMEDIATE")
            try:
                run = self._run_row(run_id)
                row = self._step_row(run["run_id"], step_id)
                if StepStatus(row["status"]) not in {StepStatus.PENDING, StepStatus.RUNNING}:
                    raise InvalidWorkflowTransition("only pending or running steps can become unavailable")
                self._update_step(connection, row, StepStatus.UNAVAILABLE, now=timestamp, reason=reason)
                self._refresh_run_status(connection, run["run_id"], timestamp)
                result = self._step_row(run["run_id"], step_id)
                connection.execute("COMMIT")
                return self._row_to_step(result)
            except Exception:
                if connection.in_transaction:
                    connection.execute("ROLLBACK")
                raise

    def skip_step(
        self, run_id: UUID | str, step_id: str, *,
        reason: str = "not_required", now: datetime | None = None,
    ) -> WorkflowStepRecord:
        timestamp = _aware(now or datetime.now(timezone.utc), "now")
        if not isinstance(reason, str) or not reason.strip():
            raise ValueError("reason must be a non-empty string")
        with self._lock:
            self._assert_open()
            connection = self._connection
            connection.execute("BEGIN IMMEDIATE")
            try:
                run = self._run_row(run_id)
                row = self._step_row(run["run_id"], step_id)
                if bool(row["required"]):
                    raise InvalidWorkflowTransition("required workflow step cannot be skipped")
                if StepStatus(row["status"]) is not StepStatus.PENDING:
                    raise InvalidWorkflowTransition("only pending optional steps can be skipped")
                self._update_step(connection, row, StepStatus.SKIPPED, now=timestamp, reason=reason)
                self._refresh_run_status(connection, run["run_id"], timestamp)
                result = self._step_row(run["run_id"], step_id)
                connection.execute("COMMIT")
                return self._row_to_step(result)
            except Exception:
                if connection.in_transaction:
                    connection.execute("ROLLBACK")
                raise

    def retry_step(self, run_id: UUID | str, step_id: str, *, now: datetime | None = None) -> WorkflowStepRecord:
        timestamp = _aware(now or datetime.now(timezone.utc), "now")
        retryable = {StepStatus.UNAVAILABLE, StepStatus.NO_DATA, StepStatus.FAILED, StepStatus.TIMED_OUT}
        with self._lock:
            self._assert_open()
            connection = self._connection
            connection.execute("BEGIN IMMEDIATE")
            try:
                run = self._run_row(run_id)
                if RunStatus(run["status"]) is RunStatus.CANCELLED:
                    raise InvalidWorkflowTransition("cancelled workflow run cannot be retried")
                row = self._step_row(run["run_id"], step_id)
                status = StepStatus(row["status"])
                if status not in retryable:
                    raise InvalidWorkflowTransition(f"step {step_id} with status {status.value} is not retryable")
                attempts = int(row["attempt_count"])
                if attempts > int(row["max_retries"]):
                    raise RetryLimitExceeded(f"retry limit reached for step {step_id}")
                self._update_step(
                    connection, row, StepStatus.PENDING, now=timestamp,
                    reason="retry_scheduled", clear_result=True,
                )
                self._refresh_run_status(connection, run["run_id"], timestamp)
                result = self._step_row(run["run_id"], step_id)
                connection.execute("COMMIT")
                return self._row_to_step(result)
            except Exception:
                if connection.in_transaction:
                    connection.execute("ROLLBACK")
                raise

    def expire_timed_out_steps(
        self, *, now: datetime | None = None,
        run_id: UUID | str | None = None,
    ) -> tuple[tuple[UUID, str], ...]:
        """Mark overdue RUNNING steps, optionally scoped to one workflow run.

        Omitting run_id performs an explicit store-wide timeout sweep. Callers
        handling one run should pass its ID to avoid changing unrelated runs.
        """
        timestamp = _aware(now or datetime.now(timezone.utc), "now")
        with self._lock:
            self._assert_open()
            connection = self._connection
            connection.execute("BEGIN IMMEDIATE")
            try:
                run_filter = None if run_id is None else self._run_row(run_id)["run_id"]
                query = (
                    "SELECT * FROM kie_workflow_steps "
                    "WHERE status = 'running' AND deadline_at IS NOT NULL AND deadline_at <= ?"
                )
                parameters: list[Any] = [_iso(timestamp)]
                if run_filter is not None:
                    query += " AND run_id = ?"
                    parameters.append(run_filter)
                query += " ORDER BY run_id, step_id"
                rows = connection.execute(query, parameters).fetchall()
                changed: list[tuple[UUID, str]] = []
                affected: set[str] = set()
                for row in rows:
                    self._update_step(connection, row, StepStatus.TIMED_OUT, now=timestamp, reason="execution_timeout")
                    changed.append((UUID(row["run_id"]), row["step_id"]))
                    affected.add(row["run_id"])
                for run_text in sorted(affected):
                    self._refresh_run_status(connection, run_text, timestamp)
                connection.execute("COMMIT")
                return tuple(changed)
            except Exception:
                if connection.in_transaction:
                    connection.execute("ROLLBACK")
                raise

    def cancel_run(
        self, run_id: UUID | str, *, reason: str = "cancelled",
        now: datetime | None = None,
    ) -> WorkflowRunRecord:
        timestamp = _aware(now or datetime.now(timezone.utc), "now")
        if not isinstance(reason, str) or not reason.strip():
            raise ValueError("reason must be a non-empty string")
        with self._lock:
            self._assert_open()
            connection = self._connection
            connection.execute("BEGIN IMMEDIATE")
            try:
                run = self._run_row(run_id)
                if RunStatus(run["status"]) not in {RunStatus.PENDING, RunStatus.RUNNING}:
                    raise InvalidWorkflowTransition(f"workflow run is {run['status']} and cannot be cancelled")
                rows = connection.execute(
                    """
                    SELECT * FROM kie_workflow_steps
                    WHERE run_id = ? AND status IN ('pending', 'running')
                    ORDER BY rowid
                    """,
                    (run["run_id"],),
                ).fetchall()
                for row in rows:
                    self._update_step(connection, row, StepStatus.CANCELLED, now=timestamp, reason=reason)
                self._set_run_status(connection, run, RunStatus.CANCELLED, reason=reason, now=timestamp)
                connection.execute("COMMIT")
            except Exception:
                if connection.in_transaction:
                    connection.execute("ROLLBACK")
                raise
            return self.get_run(run["run_id"])

    def read_transitions(
        self, run_id: UUID | str, *, step_id: str | None = None,
    ) -> tuple[WorkflowTransitionRecord, ...]:
        with self._lock:
            self._assert_open()
            run = self._run_row(run_id)
            if step_id is None:
                rows = self._connection.execute(
                    "SELECT * FROM kie_workflow_transitions WHERE run_id = ? ORDER BY sequence",
                    (run["run_id"],),
                ).fetchall()
            else:
                rows = self._connection.execute(
                    "SELECT * FROM kie_workflow_transitions WHERE run_id = ? AND step_id = ? ORDER BY sequence",
                    (run["run_id"], step_id),
                ).fetchall()
            return tuple(
                WorkflowTransitionRecord(
                    sequence=int(row["sequence"]), run_id=UUID(row["run_id"]),
                    step_id=row["step_id"], from_status=row["from_status"],
                    to_status=row["to_status"], reason=row["reason"],
                    occurred_at=_parse_datetime(row["occurred_at"]),
                    attempt_count=int(row["attempt_count"]),
                )
                for row in rows
            )

    def list_runs(self, *, status: RunStatus | None = None, limit: int = 100) -> tuple[WorkflowRunRecord, ...]:
        if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= 1000:
            raise ValueError("limit must be an integer from 1 to 1000")
        if status is not None and not isinstance(status, RunStatus):
            raise ValueError("status must be a RunStatus or None")
        with self._lock:
            self._assert_open()
            if status is None:
                rows = self._connection.execute(
                    "SELECT run_id FROM kie_workflow_runs ORDER BY created_at DESC, rowid DESC LIMIT ?",
                    (limit,),
                ).fetchall()
            else:
                rows = self._connection.execute(
                    "SELECT run_id FROM kie_workflow_runs WHERE status = ? ORDER BY created_at DESC, rowid DESC LIMIT ?",
                    (status.value, limit),
                ).fetchall()
            return tuple(self.get_run(row["run_id"]) for row in rows)

    def close(self) -> None:
        with self._lock:
            if not self._closed:
                self._connection.close()
                self._closed = True

    def __enter__(self) -> SQLiteWorkflowStore:
        with self._lock:
            self._assert_open()
        return self

    def __exit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
        self.close()
