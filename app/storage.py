"""Persistent storage for the event outbox.

A single SQLite database (WAL mode) on a shared volume backs both the
ingestion API and the independent worker. SQLite with a busy timeout is
sufficient here: the API serializes short writes and exactly one worker
process claims events. Every state transition is one committed transaction,
so a worker crash at any point leaves an event either unclaimed or
in_flight (reclaimed by lease timeout) -- an event can never silently
disappear.

Delivery guarantee: **at-least-once**. Duplicate sends after a crash or
lease expiry are explicitly permitted; consumers must dedupe on
``X-Event-Id``/``X-Request-Id``.
"""

from __future__ import annotations

import json
import os
import sqlite3
import threading
import time
import uuid
from typing import Any, Iterable, Optional

SCHEMA = """
CREATE TABLE IF NOT EXISTS events (
    id             TEXT PRIMARY KEY,
    request_id     TEXT NOT NULL UNIQUE,
    target         TEXT NOT NULL,
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

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    def reset_test_state(self) -> None:
        """Wipe events/attempts and return the clock to real time.

        Used by the one-shot verify service so repeated acceptance runs
        against a persistent named volume start from a clean slate.
        """
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            self._conn.execute("DELETE FROM attempts")
            self._conn.execute("DELETE FROM events")
            self._conn.execute("DELETE FROM kv")
            self._conn.execute("COMMIT")

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
        self, request_id: str, target: str, payload: bytes, now: float
    ) -> tuple[str, bool]:
        """Insert an event. Returns (event_id, is_new).

        If ``request_id`` already exists the original event id is returned
        with ``is_new=False`` (producer-side dedup). The payload must match
        the original; the API layer checks that beforehand.
        """
        event_id = uuid.uuid4().hex
        with self._lock:
            try:
                self._conn.execute(
                    "INSERT INTO events(id, request_id, target, payload, "
                    "status, not_before, seq, attempts, max_attempts, "
                    "created_at) VALUES (?,?,?,?, 'scheduled', ?, "
                    "(SELECT COALESCE(MAX(seq),0)+1 FROM events), 0, 4, ?)",
                    (event_id, request_id, target, payload, now, now),
                )
                return event_id, True
            except sqlite3.IntegrityError:
                row = self._conn.execute(
                    "SELECT id FROM events WHERE request_id=?", (request_id,)
                ).fetchone()
                return row["id"], False

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
        Duplicate delivery is therefore possible, but no event is lost.
        """
        with self._lock:
            cur = self._conn.execute(
                "UPDATE events SET status='scheduled', not_before=?, "
                "locked_at=NULL WHERE status='in_flight'",
                (now,),
            )
            return cur.rowcount

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
                    "payload": bytes(head["payload"]),
                    "attempt_no": attempt_no,
                    "max_attempts": head["max_attempts"],
                }
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

        Identity (event id, request id, payload, seq) and the full attempt
        history are preserved; subsequent attempt numbers continue
        monotonically (5, 6, ...) and ``replayed_count`` grows.
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
