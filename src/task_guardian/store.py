from __future__ import annotations
import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional, Sequence

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

@dataclass
class Task:
    id: str
    name: str
    tool: str
    params_json: str
    schedule: str
    expected: str
    verify: str
    output_path: str
    enabled: bool
    created_at: str
    updated_at: str
    next_run_at: Optional[str]

@dataclass
class RunRecord:
    run_id: str
    task_id: str
    status: str
    started_at: str
    finished_at: Optional[str]
    message: str
    output_path: str
    meta_json: str

SCHEMA = r"""
CREATE TABLE IF NOT EXISTS tasks (
  id TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  tool TEXT NOT NULL,
  params_json TEXT NOT NULL,
  schedule TEXT NOT NULL,
  expected TEXT NOT NULL,
  verify TEXT NOT NULL,
  output_path TEXT NOT NULL,
  enabled INTEGER NOT NULL DEFAULT 1,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  next_run_at TEXT
);

CREATE TABLE IF NOT EXISTS runs (
  run_id TEXT PRIMARY KEY,
  task_id TEXT NOT NULL,
  status TEXT NOT NULL,
  started_at TEXT NOT NULL,
  finished_at TEXT,
  message TEXT NOT NULL,
  output_path TEXT NOT NULL,
  meta_json TEXT NOT NULL,
  FOREIGN KEY(task_id) REFERENCES tasks(id)
);

CREATE INDEX IF NOT EXISTS idx_runs_task_id_started_at ON runs(task_id, started_at);
CREATE INDEX IF NOT EXISTS idx_tasks_next_run_at ON tasks(next_run_at);

CREATE TABLE IF NOT EXISTS markers (
  marker_id TEXT PRIMARY KEY,
  name TEXT NOT NULL UNIQUE,
  status TEXT NOT NULL DEFAULT 'active',
  created_at TEXT NOT NULL,
  closed_at TEXT
);

CREATE TABLE IF NOT EXISTS marker_tasks (
  marker_id TEXT NOT NULL,
  task_id TEXT NOT NULL,
  added_at TEXT NOT NULL,
  PRIMARY KEY(marker_id, task_id),
  FOREIGN KEY(marker_id) REFERENCES markers(marker_id),
  FOREIGN KEY(task_id) REFERENCES tasks(id)
);

CREATE INDEX IF NOT EXISTS idx_marker_tasks_marker ON marker_tasks(marker_id);
CREATE INDEX IF NOT EXISTS idx_marker_tasks_task ON marker_tasks(task_id);

"""

class TaskStore:
    def __init__(self, db_path: Path):
        self.db_path = db_path
        self._init()

    def _conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _init(self) -> None:
        with self._conn() as c:
            c.executescript(SCHEMA)

    def upsert_task(
        self, *, task_id: str, name: str, tool: str, params: dict[str, Any],
        schedule: str, expected: str, verify: str, output_path: str,
        enabled: bool = True, next_run_at: Optional[str] = None
    ) -> None:
        now = utc_now_iso()
        with self._conn() as c:
            c.execute(
                """
                INSERT INTO tasks (id,name,tool,params_json,schedule,expected,verify,output_path,enabled,created_at,updated_at,next_run_at)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(id) DO UPDATE SET
                  name=excluded.name,
                  tool=excluded.tool,
                  params_json=excluded.params_json,
                  schedule=excluded.schedule,
                  expected=excluded.expected,
                  verify=excluded.verify,
                  output_path=excluded.output_path,
                  enabled=excluded.enabled,
                  updated_at=excluded.updated_at,
                  next_run_at=excluded.next_run_at
                """,
                (task_id, name, tool, json.dumps(params, ensure_ascii=False), schedule, expected, verify,
                 output_path, 1 if enabled else 0, now, now, next_run_at)
            )

    def list_tasks(self, *, enabled_only: bool = False, limit: int = 50) -> list[Task]:
        q = "SELECT * FROM tasks "
        params: Sequence[Any] = ()
        if enabled_only:
            q += "WHERE enabled=1 "
        q += "ORDER BY updated_at DESC LIMIT ?"
        params = (*params, limit)
        with self._conn() as c:
            rows = c.execute(q, params).fetchall()
            return [Task(**dict(r)) for r in rows]

    def due_tasks(self, *, now_iso: str, limit: int = 25) -> list[Task]:
        with self._conn() as c:
            rows = c.execute(
                """
                SELECT * FROM tasks
                WHERE enabled=1
                  AND (next_run_at IS NULL OR next_run_at <= ?)
                ORDER BY COALESCE(next_run_at, created_at) ASC
                LIMIT ?
                """,
                (now_iso, limit),
            ).fetchall()
            return [Task(**dict(r)) for r in rows]

    def set_next_run(self, task_id: str, next_run_at: Optional[str]) -> None:
        now = utc_now_iso()
        with self._conn() as c:
            c.execute("UPDATE tasks SET next_run_at=?, updated_at=? WHERE id=?", (next_run_at, now, task_id))

    def set_enabled(self, task_id: str, enabled: bool) -> None:
        now = utc_now_iso()
        with self._conn() as c:
            c.execute("UPDATE tasks SET enabled=?, updated_at=? WHERE id=?", (1 if enabled else 0, now, task_id))

    def create_run(self, *, run_id: str, task_id: str, started_at: str, output_path: str, meta: dict[str, Any]) -> None:
        with self._conn() as c:
            c.execute(
                "INSERT INTO runs (run_id,task_id,status,started_at,finished_at,message,output_path,meta_json) VALUES (?,?,?,?,?,?,?,?)",
                (run_id, task_id, "running", started_at, None, "", output_path, json.dumps(meta, ensure_ascii=False)),
            )

    def finish_run(self, *, run_id: str, status: str, message: str, finished_at: str, meta_updates: dict[str, Any] | None = None) -> None:
        with self._conn() as c:
            row = c.execute("SELECT meta_json FROM runs WHERE run_id=?", (run_id,)).fetchone()
            meta = json.loads(row["meta_json"]) if row else {}
            if meta_updates:
                meta.update(meta_updates)
            c.execute(
                "UPDATE runs SET status=?, message=?, finished_at=?, meta_json=? WHERE run_id=?",
                (status, message, finished_at, json.dumps(meta, ensure_ascii=False), run_id),
            )

    def list_runs(self, *, limit: int = 50) -> list[RunRecord]:
        with self._conn() as c:
            rows = c.execute("SELECT * FROM runs ORDER BY started_at DESC LIMIT ?", (limit,)).fetchall()
            return [RunRecord(**dict(r)) for r in rows]

    def runs_in_window(self, *, since_iso: str) -> list[RunRecord]:
        with self._conn() as c:
            rows = c.execute("SELECT * FROM runs WHERE started_at >= ? ORDER BY started_at DESC", (since_iso,)).fetchall()
            return [RunRecord

    # ---------- markers ----------
    def create_marker(self, *, marker_id: str, name: str) -> None:
        now = utc_now_iso()
        with self._conn() as c:
            c.execute(
                "INSERT INTO markers (marker_id,name,status,created_at,closed_at) VALUES (?,?,?,?,?)",
                (marker_id, name, "active", now, None),
            )

    def list_markers(self, *, limit: int = 50):
        with self._conn() as c:
            rows = c.execute("SELECT * FROM markers ORDER BY created_at DESC LIMIT ?", (limit,)).fetchall()
            return [dict(r) for r in rows]

    def get_marker(self, *, name: str):
        with self._conn() as c:
            row = c.execute("SELECT * FROM markers WHERE name=?", (name,)).fetchone()
            return dict(row) if row else None

    def close_marker(self, *, name: str) -> None:
        now = utc_now_iso()
        with self._conn() as c:
            c.execute("UPDATE markers SET status='closed', closed_at=? WHERE name=?", (now, name))

    def add_task_to_marker(self, *, marker_name: str, task_id: str) -> None:
        m = self.get_marker(name=marker_name)
        if not m:
            raise ValueError(f"Marker not found: {marker_name}")
        now = utc_now_iso()
        with self._conn() as c:
            c.execute(
                "INSERT OR IGNORE INTO marker_tasks (marker_id,task_id,added_at) VALUES (?,?,?)",
                (m["marker_id"], task_id, now),
            )

    def remove_task_from_marker(self, *, marker_name: str, task_id: str) -> None:
        m = self.get_marker(name=marker_name)
        if not m:
            raise ValueError(f"Marker not found: {marker_name}")
        with self._conn() as c:
            c.execute("DELETE FROM marker_tasks WHERE marker_id=? AND task_id=?", (m["marker_id"], task_id))

    def marker_tasks(self, *, marker_name: str):
        m = self.get_marker(name=marker_name)
        if not m:
            raise ValueError(f"Marker not found: {marker_name}")
        with self._conn() as c:
            rows = c.execute("SELECT task_id FROM marker_tasks WHERE marker_id=? ORDER BY added_at ASC", (m["marker_id"],)).fetchall()
            return [r["task_id"] for r in rows]

    def due_tasks_for_marker(self, *, marker_name: str, now_iso: str, limit: int = 25):
        m = self.get_marker(name=marker_name)
        if not m or m["status"] != "active":
            return []
        with self._conn() as c:
            rows = c.execute(
                """
                SELECT t.* FROM tasks t
                JOIN marker_tasks mt ON mt.task_id = t.id
                WHERE mt.marker_id = ?
                  AND t.enabled=1
                  AND (t.next_run_at IS NULL OR t.next_run_at <= ?)
                ORDER BY COALESCE(t.next_run_at, t.created_at) ASC
                LIMIT ?
                """,
                (m["marker_id"], now_iso, limit),
            ).fetchall()
            return [Task(**dict(r)) for r in rows]

    def due_tasks_active_markers_only(self, *, now_iso: str, limit: int = 25):
        with self._conn() as c:
            rows = c.execute(
                """
                SELECT DISTINCT t.* FROM tasks t
                JOIN marker_tasks mt ON mt.task_id = t.id
                JOIN markers m ON m.marker_id = mt.marker_id
                WHERE m.status='active'
                  AND t.enabled=1
                  AND (t.next_run_at IS NULL OR t.next_run_at <= ?)
                ORDER BY COALESCE(t.next_run_at, t.created_at) ASC
                LIMIT ?
                """,
                (now_iso, limit),
            ).fetchall()
            return [Task(**dict(r)) for r in rows]

    def marker_status(self, *, marker_name: str):
        m = self.get_marker(name=marker_name)
        if not m:
            raise ValueError(f"Marker not found: {marker_name}")
        task_ids = self.marker_tasks(marker_name=marker_name)
        ok_all = True
        rows_out = []
        with self._conn() as c:
            for tid in task_ids:
                r = c.execute(
                    """
                    SELECT * FROM runs
                    WHERE task_id=? AND started_at >= ?
                    ORDER BY started_at DESC
                    LIMIT 1
                    """,
                    (tid, m["created_at"]),
                ).fetchone()
                if not r:
                    ok_all = False
                    rows_out.append({"task_id": tid, "state": "missing"})
                    continue
                rr = dict(r)
                if rr["status"] != "success":
                    ok_all = False
                rows_out.append({"task_id": tid, "state": rr["status"], "latest": {"started_at": rr["started_at"], "message": rr["message"]}})
        return {"marker": m, "green": bool(ok_all) if task_ids else False, "tasks": rows_out}

(**dict(r)) for r in rows]
