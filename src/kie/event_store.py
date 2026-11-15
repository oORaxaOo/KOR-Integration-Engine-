"""Append-only SQLite event ledger for the KOR Integration Engine.

The ledger provides transactional deduplication, ordered reads, and a
SHA-256 hash chain for tamper-evidence. It is not a security boundary against
an attacker who controls the database file or can replace the entire file.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import sqlite3
from threading import RLock
from typing import Any
from uuid import UUID

from .events import IntegrationEvent, event_from_dict, event_to_dict

GENESIS_HASH = "0" * 64
_SCHEMA_VERSION = 1
_MAX_PAGE_SIZE = 1000


class EventStoreError(RuntimeError):
    """Base error for event-ledger failures."""


class EventConflictError(EventStoreError):
    """An existing event ID was submitted with different content."""


class IdempotencyConflictError(EventStoreError):
    """An idempotency key was reused for a different logical event."""


@dataclass(frozen=True, slots=True)
class AppendResult:
    event_id: UUID
    sequence: int
    inserted: bool
    envelope_sha256: str
    entry_hash: str


@dataclass(frozen=True, slots=True)
class EventRecord:
    sequence: int
    event: IntegrationEvent
    envelope_sha256: str
    previous_hash: str
    entry_hash: str
    stored_at: datetime


@dataclass(frozen=True, slots=True)
class IntegrityReport:
    ok: bool
    checked_events: int
    errors: tuple[str, ...]


def _canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def _sha256(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _event_fingerprint(event: IntegrationEvent) -> str:
    """Ignore generated event ID and recording time when matching retries."""
    envelope = event_to_dict(event)
    envelope.pop("event_id")
    envelope.pop("recorded_at")
    return _sha256(_canonical_json(envelope).encode("utf-8"))


def _entry_hash(sequence: int, previous_hash: str, envelope_sha256: str, stored_at: str) -> str:
    material = f"{sequence}\n{previous_hash}\n{envelope_sha256}\n{stored_at}".encode("utf-8")
    return _sha256(material)


def _parse_stored_at(value: str) -> datetime:
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("stored_at is not timezone-aware")
    return parsed


class SQLiteEventStore:
    """Persistent, append-only event history backed by SQLite.

    The class serializes writes within one instance and uses BEGIN IMMEDIATE
    so concurrent SQLite writers coordinate through the database lock.
    Each event is written atomically with its sequence and hash-chain link.
    """

    def __init__(self, database: str | Path, *, timeout_seconds: float = 5.0) -> None:
        if isinstance(timeout_seconds, bool) or not isinstance(timeout_seconds, (int, float)) or not 0 < timeout_seconds <= 60:
            raise ValueError("timeout_seconds must be greater than 0 and at most 60 seconds")
        database_value = str(database)
        if not database_value:
            raise ValueError("database path must not be empty")
        if database_value != ":memory:" and not database_value.startswith("file:"):
            database_path = Path(database_value).expanduser()
            database_path.parent.mkdir(parents=True, exist_ok=True)
            database_value = str(database_path)

        self.database = database_value
        self.timeout_seconds = float(timeout_seconds)
        self._lock = RLock()
        self._closed = False
        self._connection = sqlite3.connect(
            database_value,
            timeout=float(timeout_seconds),
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

        version = int(connection.execute("PRAGMA user_version").fetchone()[0])
        if version > _SCHEMA_VERSION:
            raise EventStoreError(
                f"database schema version {version} is newer than supported version {_SCHEMA_VERSION}"
            )

        connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS kie_events (
                sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                event_id TEXT NOT NULL UNIQUE,
                producer_engine TEXT NOT NULL,
                event_type TEXT NOT NULL,
                task_id TEXT,
                correlation_id TEXT,
                idempotency_key TEXT,
                idempotency_fingerprint TEXT,
                envelope_json TEXT NOT NULL,
                envelope_sha256 TEXT NOT NULL,
                previous_hash TEXT NOT NULL,
                entry_hash TEXT NOT NULL,
                stored_at TEXT NOT NULL
            );

            CREATE UNIQUE INDEX IF NOT EXISTS kie_events_idempotency
                ON kie_events(producer_engine, idempotency_key)
                WHERE idempotency_key IS NOT NULL;

            CREATE INDEX IF NOT EXISTS kie_events_task_sequence
                ON kie_events(task_id, sequence);

            CREATE INDEX IF NOT EXISTS kie_events_correlation_sequence
                ON kie_events(correlation_id, sequence);

            CREATE TRIGGER IF NOT EXISTS kie_events_no_update
            BEFORE UPDATE ON kie_events
            BEGIN
                SELECT RAISE(ABORT, 'kie_events is append-only: UPDATE rejected');
            END;

            CREATE TRIGGER IF NOT EXISTS kie_events_no_delete
            BEFORE DELETE ON kie_events
            BEGIN
                SELECT RAISE(ABORT, 'kie_events is append-only: DELETE rejected');
            END;
            """
        )
        connection.execute(f"PRAGMA user_version = {_SCHEMA_VERSION}")

    def _assert_open(self) -> None:
        if self._closed:
            raise RuntimeError("event store is closed")

    @staticmethod
    def _append_result(row: sqlite3.Row, *, inserted: bool) -> AppendResult:
        return AppendResult(
            event_id=UUID(row["event_id"]),
            sequence=int(row["sequence"]),
            inserted=inserted,
            envelope_sha256=row["envelope_sha256"],
            entry_hash=row["entry_hash"],
        )

    def append(self, event: IntegrationEvent) -> AppendResult:
        """Append once, or return the original record for an exact retry."""
        if not isinstance(event, IntegrationEvent):
            raise TypeError("event must be an IntegrationEvent")
        envelope_json = _canonical_json(event_to_dict(event))
        envelope_sha256 = _sha256(envelope_json.encode("utf-8"))
        fingerprint = _event_fingerprint(event)
        now = datetime.now(timezone.utc).isoformat(timespec="microseconds")

        with self._lock:
            self._assert_open()
            connection = self._connection
            connection.execute("BEGIN IMMEDIATE")
            try:
                existing = connection.execute(
                    "SELECT * FROM kie_events WHERE event_id = ?",
                    (str(event.event_id),),
                ).fetchone()
                if existing is not None:
                    if existing["envelope_json"] != envelope_json:
                        raise EventConflictError(
                            f"event_id {event.event_id} already exists with different content"
                        )
                    connection.execute("COMMIT")
                    return self._append_result(existing, inserted=False)

                if event.idempotency_key is not None:
                    existing = connection.execute(
                        """
                        SELECT * FROM kie_events
                        WHERE producer_engine = ? AND idempotency_key = ?
                        """,
                        (event.producer_engine, event.idempotency_key),
                    ).fetchone()
                    if existing is not None:
                        if existing["idempotency_fingerprint"] != fingerprint:
                            raise IdempotencyConflictError(
                                "idempotency key was reused with different event content"
                            )
                        connection.execute("COMMIT")
                        return self._append_result(existing, inserted=False)

                previous = connection.execute(
                    "SELECT sequence, entry_hash FROM kie_events ORDER BY sequence DESC LIMIT 1"
                ).fetchone()
                sequence = 1 if previous is None else int(previous["sequence"]) + 1
                previous_hash = GENESIS_HASH if previous is None else previous["entry_hash"]
                entry_hash = _entry_hash(sequence, previous_hash, envelope_sha256, now)

                connection.execute(
                    """
                    INSERT INTO kie_events (
                        sequence, event_id, producer_engine, event_type, task_id,
                        correlation_id, idempotency_key, idempotency_fingerprint,
                        envelope_json, envelope_sha256, previous_hash, entry_hash, stored_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        sequence,
                        str(event.event_id),
                        event.producer_engine,
                        event.event_type,
                        event.task_id,
                        event.correlation_id,
                        event.idempotency_key,
                        fingerprint if event.idempotency_key is not None else None,
                        envelope_json,
                        envelope_sha256,
                        previous_hash,
                        entry_hash,
                        now,
                    ),
                )
                row = connection.execute(
                    "SELECT * FROM kie_events WHERE sequence = ?", (sequence,)
                ).fetchone()
                connection.execute("COMMIT")
                return self._append_result(row, inserted=True)
            except Exception:
                if connection.in_transaction:
                    connection.execute("ROLLBACK")
                raise

    def get_event(self, event_id: UUID | str) -> EventRecord | None:
        try:
            normalized_id = str(event_id if isinstance(event_id, UUID) else UUID(event_id))
        except (TypeError, ValueError) as exc:
            raise ValueError("event_id must be a UUID or UUID string") from exc
        with self._lock:
            self._assert_open()
            row = self._connection.execute(
                "SELECT * FROM kie_events WHERE event_id = ?", (normalized_id,)
            ).fetchone()
            return None if row is None else self._row_to_record(row)

    def read_events(
        self,
        *,
        after_sequence: int = 0,
        limit: int = 100,
        producer_engine: str | None = None,
        event_type: str | None = None,
        task_id: str | None = None,
        correlation_id: str | None = None,
    ) -> tuple[EventRecord, ...]:
        """Read an ordered page; after_sequence is an exclusive cursor."""
        if isinstance(after_sequence, bool) or not isinstance(after_sequence, int) or after_sequence < 0:
            raise ValueError("after_sequence must be a non-negative integer")
        if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= _MAX_PAGE_SIZE:
            raise ValueError(f"limit must be an integer from 1 to {_MAX_PAGE_SIZE}")

        filters = (
            ("producer_engine", producer_engine),
            ("event_type", event_type),
            ("task_id", task_id),
            ("correlation_id", correlation_id),
        )
        clauses = ["sequence > ?"]
        parameters: list[Any] = [after_sequence]
        for column, value in filters:
            if value is not None:
                if not isinstance(value, str) or not value.strip():
                    raise ValueError(f"{column} filter must be None or a non-empty string")
                clauses.append(f"{column} = ?")
                parameters.append(value)
        parameters.append(limit)
        query = (
            "SELECT * FROM kie_events WHERE "
            + " AND ".join(clauses)
            + " ORDER BY sequence ASC LIMIT ?"
        )

        with self._lock:
            self._assert_open()
            rows = self._connection.execute(query, parameters).fetchall()
            return tuple(self._row_to_record(row) for row in rows)

    def replay(self, *, after_sequence: int = 0, limit: int = 100) -> tuple[EventRecord, ...]:
        """Read historical events in order; does not dispatch or execute them."""
        return self.read_events(after_sequence=after_sequence, limit=limit)

    def count(self) -> int:
        with self._lock:
            self._assert_open()
            return int(self._connection.execute("SELECT COUNT(*) FROM kie_events").fetchone()[0])

    def verify_integrity(self) -> IntegrityReport:
        """Check SQLite structure, envelopes, indexed metadata, and hash chain."""
        with self._lock:
            self._assert_open()
            errors: list[str] = []
            integrity_rows = self._connection.execute("PRAGMA integrity_check").fetchall()
            if len(integrity_rows) != 1 or integrity_rows[0][0] != "ok":
                errors.extend(f"sqlite integrity_check: {row[0]}" for row in integrity_rows)

            rows = self._connection.execute(
                "SELECT * FROM kie_events ORDER BY sequence ASC"
            ).fetchall()
            previous_hash = GENESIS_HASH
            expected_sequence = 1
            for row in rows:
                sequence = int(row["sequence"])
                if sequence != expected_sequence:
                    errors.append(
                        f"sequence gap: expected {expected_sequence}, found {sequence}"
                    )
                expected_sequence = sequence + 1
                if row["previous_hash"] != previous_hash:
                    errors.append(f"previous hash mismatch at sequence {sequence}")

                try:
                    envelope = json.loads(row["envelope_json"])
                    canonical = _canonical_json(envelope)
                    calculated_envelope_hash = _sha256(canonical.encode("utf-8"))
                    if calculated_envelope_hash != row["envelope_sha256"]:
                        errors.append(f"envelope hash mismatch at sequence {sequence}")
                    try:
                        event = event_from_dict(envelope)
                    except (TypeError, ValueError) as exc:
                        errors.append(f"invalid event envelope at sequence {sequence}: {exc}")
                    else:
                        if (
                            str(event.event_id) != row["event_id"]
                            or event.producer_engine != row["producer_engine"]
                            or event.event_type != row["event_type"]
                            or event.task_id != row["task_id"]
                            or event.correlation_id != row["correlation_id"]
                            or event.idempotency_key != row["idempotency_key"]
                        ):
                            errors.append(f"indexed metadata mismatch at sequence {sequence}")
                except (TypeError, ValueError, json.JSONDecodeError) as exc:
                    calculated_envelope_hash = _sha256(row["envelope_json"].encode("utf-8"))
                    if calculated_envelope_hash != row["envelope_sha256"]:
                        errors.append(f"envelope hash mismatch at sequence {sequence}")
                    errors.append(f"invalid event envelope at sequence {sequence}: {exc}")

                expected_entry_hash = _entry_hash(
                    sequence,
                    row["previous_hash"],
                    calculated_envelope_hash,
                    row["stored_at"],
                )
                if expected_entry_hash != row["entry_hash"]:
                    errors.append(f"entry hash mismatch at sequence {sequence}")
                previous_hash = row["entry_hash"]

            return IntegrityReport(
                ok=not errors,
                checked_events=len(rows),
                errors=tuple(errors),
            )

    def _row_to_record(self, row: sqlite3.Row) -> EventRecord:
        envelope = json.loads(row["envelope_json"])
        event = event_from_dict(envelope)
        return EventRecord(
            sequence=int(row["sequence"]),
            event=event,
            envelope_sha256=row["envelope_sha256"],
            previous_hash=row["previous_hash"],
            entry_hash=row["entry_hash"],
            stored_at=_parse_stored_at(row["stored_at"]),
        )

    def close(self) -> None:
        with self._lock:
            if not self._closed:
                self._connection.close()
                self._closed = True

    def __enter__(self) -> SQLiteEventStore:
        with self._lock:
            self._assert_open()
        return self

    def __exit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
        self.close()
