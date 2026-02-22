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
            return [RunRecord(**dict(r)) for r in rows]
