"""Persistent storage for the event outbox.

A single SQLite database (WAL mode) on a shared volume backs both the
ingestion API and the independent worker. SQLite with a busy timeout is
sufficient here: the API serializes short writes and exactly one worker
process claims events. Every state transition is one committed transaction,
so a worker crash at any point leaves an event either unclaimed or
in_flight (reclaimed by lease timeout) -- an event can never silently
disappear.

Every event is bound to the target's *current delivery generation* in the
same transaction as request-id de-duplication and insertion. Retries,
crash recovery and dead-letter replay all preserve that generation; the
worker resolves the URL and secret for the stored generation, never the
generation that happens to be current later.

Delivery guarantee: **at-least-once**. Duplicate sends after a crash or
lease expiry are explicitly permitted; consumers must dedupe on
``X-Event-Id``/``X-Request-Id``.
"""

from __future__ import annotations

import json
import sqlite3
import threading
import time
import uuid
from typing import Any, Optional

SCHEMA = """
CREATE TABLE IF NOT EXISTS events (
    id             TEXT PRIMARY KEY,
    request_id     TEXT NOT NULL UNIQUE,
    target         TEXT NOT NULL,
    generation_id  TEXT,
    payload        BLOB NOT NULL,
    status         TEXT NOT NULL,
    not_before     REAL NOT NULL,
    seq            INTEGER NOT NULL UNIQUE,
    attempts       INTEGER NOT NULL DEFAULT 0,
    max_attempts   INTEGER NOT NULL DEFAULT 4,
    created_at     REAL NOT NULL,
    locked_at      REAL,
    delivered_at   REAL,
    dead_at        REAL,
    replayed_count INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_events_target_status
    ON events(target, status, not_before);
CREATE INDEX IF NOT EXISTS idx_events_status ON events(status);

CREATE TABLE IF NOT EXISTS attempts (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id    TEXT NOT NULL REFERENCES events(id),
    attempt_no  INTEGER NOT NULL,
    started_at  REAL NOT NULL,
    finished_at REAL,
    ok          INTEGER,
    status_code INTEGER,
    error       TEXT
);
CREATE INDEX IF NOT EXISTS idx_attempts_event ON attempts(event_id, attempt_no);

CREATE TABLE IF NOT EXISTS target_state (
    target        TEXT PRIMARY KEY,
    generation_id TEXT NOT NULL,
    updated_at    REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS kv (
    k TEXT PRIMARY KEY,
    v TEXT NOT NULL
);
"""

# Retry backoff in seconds after attempts 1..3 fail. A 4th failure dead-letters.
BACKOFF_AFTER = {1: 1.0, 2: 2.0, 3: 4.0}

VALID_STATUSES = ("scheduled", "in_flight", "delivered", "dead")


class Storage:
    """Thread-safe wrapper around one SQLite connection (one per process)."""

    def __init__(self, path: str):
        self.path = path
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(
            path, check_same_thread=False, isolation_level=None, timeout=30
        )
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL;")
        self._conn.execute("PRAGMA synchronous=NORMAL;")
        self._conn.execute("PRAGMA busy_timeout=30000;")
        self._conn.execute("PRAGMA foreign_keys=ON;")
        self._conn.executescript(SCHEMA)
        self._migrate()

    def _migrate(self) -> None:
        """Add the generation column to databases created by one-gen builds."""
        columns = {
            row["name"]
            for row in self._conn.execute("PRAGMA table_info(events)")
        }
        if "generation_id" not in columns:
            self._conn.execute(
                "ALTER TABLE events ADD COLUMN generation_id TEXT"
            )
            # Backfill rows written by the original single-generation schema
            # so an in-place upgrade keeps their existing delivery contract.
            self._conn.execute(
                "UPDATE events SET generation_id=? WHERE generation_id IS NULL",
                ("default",),
            )

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    def reset_test_state(self) -> None:
        """Wipe events, attempts and mutable target state.

        Used by the one-shot verify service so repeated acceptance runs
        against a persistent named volume start from a clean slate. The API
        immediately reseeds target state from its deployment configuration.
        """
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            self._conn.execute("DELETE FROM attempts")
            self._conn.execute("DELETE FROM events")
            self._conn.execute("DELETE FROM target_state")
            self._conn.execute("DELETE FROM kv")
            self._conn.execute("COMMIT")

    # ------------------------------------------------------- target gen ops

    def ensure_targets(self, targets: dict[str, dict], now: float) -> None:
        """Seed whitelisted targets that do not yet have persisted state.

        Existing current-generation state is never changed. This makes an
        old single-generation database compatible when it is restarted with
        the equivalent normalized legacy configuration.
        """
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                for name, spec in targets.items():
                    initial = spec["initial_generation_id"]
                    self._conn.execute(
                        "INSERT INTO target_state(target, generation_id, "
                        "updated_at) VALUES(?,?,?) ON CONFLICT(target) DO "
                        "NOTHING",
                        (name, initial, now),
                    )
                self._conn.execute("COMMIT")
            except BaseException:
                self._conn.execute("ROLLBACK")
                raise

    def get_target_state(self, target: str) -> Optional[sqlite3.Row]:
        with self._lock:
            return self._conn.execute(
                "SELECT * FROM target_state WHERE target=?", (target,)
            ).fetchone()

    def list_target_state(self) -> list[sqlite3.Row]:
        with self._lock:
            return list(
                self._conn.execute(
                    "SELECT * FROM target_state ORDER BY target"
                )
            )

    def activate_generation(
        self,
        target: str,
        generation_id: str,
        expected_current: Optional[str],
        now: float,
    ) -> tuple[Optional[sqlite3.Row], Optional[str]]:
        """Compare-and-set the current generation.

        Returns ``(row, conflict_current)``. A missing target or unknown
        generation is represented by ``None``; an expected-version mismatch
        returns the persisted current id without changing anything. The
        durable update commits before the admin response is released, so any
        later event insertion sees the committed state.
        """
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                row = self._conn.execute(
                    "SELECT * FROM target_state WHERE target=?", (target,)
                ).fetchone()
                if row is None:
                    self._conn.execute("ROLLBACK")
                    return None, None
                if expected_current is not None and \
                        row["generation_id"] != expected_current:
                    conflict = row["generation_id"]
                    self._conn.execute("ROLLBACK")
                    return None, conflict
                self._conn.execute(
                    "UPDATE target_state SET generation_id=?, updated_at=? "
                    "WHERE target=?",
                    (generation_id, now, target),
                )
                updated = self._conn.execute(
                    "SELECT * FROM target_state WHERE target=?", (target,)
                ).fetchone()
                self._conn.execute("COMMIT")
                return updated, None
            except BaseException:
                self._conn.execute("ROLLBACK")
                raise

    # ---------------------------------------------------------------- clock

    def get_clock(self) -> dict[str, Any]:
        with self._lock:
            row = self._conn.execute(
                "SELECT v FROM kv WHERE k='clock'"
            ).fetchone()
        if row is None:
            return {"mode": "real", "virtual_now": None}
        return json.loads(row["v"])

    def set_clock(self, mode: str, virtual_now: Optional[float] = None) -> dict:
        assert mode in ("real", "virtual")
        if mode == "virtual" and virtual_now is None:
            virtual_now = time.time()
        state = {"mode": mode, "virtual_now": virtual_now}
        with self._lock:
            self._conn.execute(
                "INSERT INTO kv(k,v) VALUES('clock', ?) "
                "ON CONFLICT(k) DO UPDATE SET v=excluded.v",
                (json.dumps(state),),
            )
        return state

    def advance_clock(self, delta: float) -> dict:
        """Atomically add ``delta`` seconds to virtual time (virtual mode)."""
        with self._lock:
            row = self._conn.execute(
                "SELECT v FROM kv WHERE k='clock'"
            ).fetchone()
            state = json.loads(row["v"]) if row else {
                "mode": "virtual", "virtual_now": time.time()
            }
            state["mode"] = "virtual"
            state["virtual_now"] = float(
                state.get("virtual_now") or 0
            ) + float(delta)
            self._conn.execute(
                "INSERT INTO kv(k,v) VALUES('clock', ?) "
                "ON CONFLICT(k) DO UPDATE SET v=excluded.v",
                (json.dumps(state),),
            )
        return state

    # --------------------------------------------------------------- events

    def insert_event(
        self,
        request_id: str,
        target: str,
        payload: bytes,
        now: float,
        configured_generation_ids: Optional[set[str]] = None,
    ) -> dict[str, Any]:
        """Insert an event and bind the current generation atomically.

        The unique request-id check, current-generation lookup and insert
        happen in one ``IMMEDIATE`` transaction. This closes the
        activation/ingestion ordering gap: after activation commits, an
        insertion cannot bind the old generation, and vice versa.

        Result kinds:
        * ``new``: inserted;
        * ``duplicate``: request id already has this payload;
        * ``conflict``: request id already has another payload;
        * ``missing_generation``: target exists but its persisted current
          generation has no deployment configuration (the API treats this as
          a retryable 503 and inserts nothing).
        """
        event_id = uuid.uuid4().hex
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                existing = self._conn.execute(
                    "SELECT id, payload, status, generation_id FROM events "
                    "WHERE request_id=?",
                    (request_id,),
                ).fetchone()
                if existing is not None:
                    same = bytes(existing["payload"]) == payload
                    self._conn.execute("COMMIT")
                    return {
                        "kind": "duplicate" if same else "conflict",
                        "event_id": existing["id"],
                        "status": existing["status"],
                        "generation_id": existing["generation_id"],
                    }

                state = self._conn.execute(
                    "SELECT generation_id FROM target_state WHERE target=?",
                    (target,),
                ).fetchone()
                if state is None:
                    self._conn.execute("ROLLBACK")
                    return {"kind": "unknown_target"}
                generation_id = state["generation_id"]
                if configured_generation_ids is not None and \
                        generation_id not in configured_generation_ids:
                    self._conn.execute("ROLLBACK")
                    return {
                        "kind": "missing_generation",
                        "event_id": None,
                        "status": None,
                        "generation_id": generation_id,
                    }

                # Insertion happens only after both durable state and the
                # process's deployment contract agree on the current generation.
                # URLs/secrets deliberately do not live in the DB.
                self._conn.execute(
                    "INSERT INTO events(id, request_id, target, generation_id, "
                    "payload, status, not_before, seq, attempts, max_attempts, "
                    "created_at) VALUES (?,?,?,?,?, 'scheduled', ?, "
                    "(SELECT COALESCE(MAX(seq),0)+1 FROM events), 0, 4, ?)",
                    (event_id, request_id, target, generation_id, payload,
                     now, now),
                )
                self._conn.execute("COMMIT")
                return {
                    "kind": "new",
                    "event_id": event_id,
                    "status": "scheduled",
                    "generation_id": generation_id,
                }
            except BaseException:
                self._conn.execute("ROLLBACK")
                raise

    def get_event(self, event_id: str) -> Optional[sqlite3.Row]:
        with self._lock:
            return self._conn.execute(
                "SELECT * FROM events WHERE id=?", (event_id,)
            ).fetchone()

    def get_event_by_request(self, request_id: str) -> Optional[sqlite3.Row]:
        with self._lock:
            return self._conn.execute(
                "SELECT * FROM events WHERE request_id=?", (request_id,)
            ).fetchone()

    def list_dead(self, limit: int = 100) -> list[sqlite3.Row]:
        with self._lock:
            return list(
                self._conn.execute(
                    "SELECT * FROM events WHERE status='dead' "
                    "ORDER BY dead_at LIMIT ?",
                    (limit,),
                )
            )

    def list_events(self, limit: int = 100) -> list[sqlite3.Row]:
        with self._lock:
            return list(
                self._conn.execute(
                    "SELECT * FROM events ORDER BY seq LIMIT ?", (limit,)
                )
            )

    def list_attempts(self, event_id: str) -> list[sqlite3.Row]:
        with self._lock:
            return list(
                self._conn.execute(
                    "SELECT * FROM attempts WHERE event_id=? ORDER BY attempt_no",
                    (event_id,),
                )
            )

    # ----------------------------------------------------------- worker ops

    def requeue_inflight(self, now: float) -> int:
        """Called at (single) worker startup: every in_flight row belonged
        to the dead predecessor process and is requeued immediately.

        ``now`` is logical (controllable) time so not_before behaves like a
        fresh retry; the wall-clock lock column is cleared too. Half-open
        attempt rows (finished_at NULL) remain as honest crash history.
        Duplicate delivery is therefore possible, but no event is lost. The
        bound generation is intentionally untouched.
        """
        with self._lock:
            cur = self._conn.execute(
                "UPDATE events SET status='scheduled', not_before=?, "
                "locked_at=NULL WHERE status='in_flight'",
                (now,),
            )
            return cur.rowcount

    def peek_due_head(self, target: str, now: float) -> Optional[sqlite3.Row]:
        """Return the earliest due scheduled row without claiming it.

        A missing generation contract must not create an attempt or alter the
        event, so the worker performs this read-only FIFO-gate check before
        ``claim_next``. An in-flight row is intentionally not reported as
        blocked: it already belongs to an HTTP attempt (or a predecessor whose
        startup recovery will requeue it).
        """
        with self._lock:
            head = self._conn.execute(
                "SELECT * FROM events WHERE target=? AND status IN "
                "('scheduled','in_flight') ORDER BY seq LIMIT 1",
                (target,),
            ).fetchone()
            if head is None or head["status"] != "scheduled" \
                    or head["not_before"] > now:
                return None
            return head

    def claim_next(
        self, target: str, now: float, wall_now: Optional[float] = None
    ) -> Optional[dict]:
        """Claim the head eligible event for one target.

        Ordering is strictly ``seq`` -- events enter the table in ingestion
        order. The earliest still-active row is the gate: if it is in
        flight or not yet due (not_before), no younger row may be taken.
        ``now`` is logical (controllable) time; ``wall_now`` is the real
        wall clock stamped into ``locked_at`` for crash diagnostics.
        """
        if wall_now is None:
            wall_now = now
        with self._lock:
            cur = self._conn.execute("BEGIN IMMEDIATE")
            try:
                # Earliest still-active row for this target; if it is not
                # due yet, nothing younger may be taken either.
                head = self._conn.execute(
                    "SELECT * FROM events WHERE target=? AND status IN "
                    "('scheduled','in_flight') ORDER BY seq LIMIT 1",
                    (target,),
                ).fetchone()
                if head is None:
                    cur.close()
                    self._conn.execute("COMMIT")
                    return None
                if head["status"] != "scheduled" or head["not_before"] > now:
                    cur.close()
                    self._conn.execute("COMMIT")
                    return None

                event_id = head["id"]
                attempt_no = head["attempts"] + 1
                self._conn.execute(
                    "UPDATE events SET status='in_flight', locked_at=?, "
                    "attempts=? WHERE id=?",
                    (wall_now, attempt_no, event_id),
                )
                self._conn.execute(
                    "INSERT INTO attempts(event_id, attempt_no, started_at) "
                    "VALUES (?,?,?)",
                    (event_id, attempt_no, now),
                )
                cur.close()
                self._conn.execute("COMMIT")
                return {
                    "id": event_id,
                    "request_id": head["request_id"],
                    "target": target,
                    "generation_id": head["generation_id"],
                    "payload": bytes(head["payload"]),
                    "attempt_no": attempt_no,
                    "max_attempts": head["max_attempts"],
                }
            except BaseException:
                self._conn.execute("ROLLBACK")
                raise

    def cancel_fresh_claim(
        self, event_id: str, attempt_no: int, now: float
    ) -> bool:
        """Cancel a claim before any HTTP byte has been sent.

        This is used only when the bound generation contract disappears from
        process configuration in the tiny window between the claim and URL
        resolution. The tentative attempt row is deleted and the event
        remains scheduled at the head; it is neither an attempted delivery
        nor a failure.
        """
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                cur = self._conn.execute(
                    "DELETE FROM attempts WHERE event_id=? AND attempt_no=? "
                    "AND finished_at IS NULL",
                    (event_id, attempt_no),
                )
                cancelled = cur.rowcount > 0
                if cancelled:
                    self._conn.execute(
                        "UPDATE events SET status='scheduled', not_before=?, "
                        "locked_at=NULL, attempts=? WHERE id=? AND "
                        "status='in_flight'",
                        (now, attempt_no - 1, event_id),
                    )
                self._conn.execute("COMMIT")
                return cancelled
            except BaseException:
                self._conn.execute("ROLLBACK")
                raise

    def finish_attempt(
        self,
        event_id: str,
        attempt_no: int,
        now: float,
        claim_now: float,
        ok: bool,
        status_code: Optional[int],
        error: Optional[str],
    ) -> str:
        """Record an attempt outcome and transition the event.

        Retry scheduling is anchored to the attempt's *start* (claim) time
        so the 1/2/4 s schedule is exact regardless of request duration or
        clock progress during the (blocking) HTTP call; a long attempt can
        simply make its retry immediately due.

        Returns the new event status.
        """
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                row = self._conn.execute(
                    "SELECT attempts, max_attempts FROM events WHERE id=?",
                    (event_id,),
                ).fetchone()
                if ok:
                    self._conn.execute(
                        "UPDATE events SET status='delivered', delivered_at=?, "
                        "locked_at=NULL WHERE id=?",
                        (now, event_id),
                    )
                elif row["attempts"] >= row["max_attempts"]:
                    self._conn.execute(
                        "UPDATE events SET status='dead', dead_at=?, "
                        "locked_at=NULL WHERE id=?",
                        (now, event_id),
                    )
                else:
                    backoff = BACKOFF_AFTER.get(row["attempts"], 4.0)
                    self._conn.execute(
                        "UPDATE events SET status='scheduled', "
                        "not_before=?, locked_at=NULL WHERE id=?",
                        (claim_now + backoff, event_id),
                    )
                self._conn.execute(
                    "UPDATE attempts SET finished_at=?, ok=?, status_code=?, "
                    "error=? WHERE event_id=? AND attempt_no=?",
                    (
                        now,
                        1 if ok else 0,
                        status_code,
                        error,
                        event_id,
                        attempt_no,
                    ),
                )
                new = self._conn.execute(
                    "SELECT status FROM events WHERE id=?", (event_id,)
                ).fetchone()
                self._conn.execute("COMMIT")
                return new["status"]
            except BaseException:
                self._conn.execute("ROLLBACK")
                raise

    def replay_dead(self, event_id: str, now: float) -> Optional[sqlite3.Row]:
        """Bring a dead event back with a *fresh* 4-attempt budget.

        Identity (event id, request id, payload, seq, generation) and the
        full attempt history are preserved; subsequent attempt numbers
        continue monotonically (5, 6, ...) and ``replayed_count`` grows.
        """
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM events WHERE id=? AND status='dead'", (event_id,)
            ).fetchone()
            if row is None:
                return None
            self._conn.execute(
                "UPDATE events SET status='scheduled', not_before=?, "
                "locked_at=NULL, max_attempts=max_attempts+4, "
                "replayed_count=replayed_count+1 WHERE id=?",
                (now, event_id),
            )
            return self._conn.execute(
                "SELECT * FROM events WHERE id=?", (event_id,)
            ).fetchone()
