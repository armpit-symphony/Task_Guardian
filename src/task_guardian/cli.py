from __future__ import annotations

import argparse
import json
import sys
import uuid
from datetime import datetime, timezone

from .config import load_config
from .scheduler import next_run_iso, parse_window
from .store import TaskStore
from .task_runner import run_due, run_task_id


def _print(obj) -> None:
    print(json.dumps(obj, indent=2, ensure_ascii=False))


def cmd_init(_a) -> int:
    cfg = load_config()
    TaskStore(cfg.db_path)
    _print({"ok": True, "db_path": str(cfg.db_path)})
    return 0


def cmd_add(a) -> int:
    cfg = load_config()
    store = TaskStore(cfg.db_path)

    if a.tool in ("exec", "gh"):
        if not a.command:
            raise SystemExit("--command required")
        params = {"command": a.command, "timeout": a.timeout}
    elif a.tool == "http_request":
        if not a.url:
            raise SystemExit("--url required")
        params = {"url": a.url, "timeout": a.timeout}
    else:
        raise SystemExit("Unknown tool")

    output_path = a.output or f"runs/{a.id}.out.txt"
    nxt = next_run_iso(a.schedule)

    store.upsert_task(
        task_id=a.id,
        name=a.name,
        tool=a.tool,
        params=params,
        schedule=a.schedule,
        expected=a.expected,
        verify=a.verify,
        output_path=output_path,
        enabled=(not a.disabled),
        next_run_at=nxt,
    )
    _print({"ok": True, "task_id": a.id, "next_run_at": nxt})
    return 0


def cmd_run_due(a) -> int:
    cfg = load_config()
    out = run_due(cfg=cfg, limit=a.limit, marker_name=a.marker, active_markers_only=a.active_markers_only)
    _print(out)
    return 0


def cmd_run_task(a) -> int:
    cfg = load_config()
    out = run_task_id(cfg=cfg, task_id=a.id)
    _print(out)
    return 0 if out.get('ok') else 2


def cmd_runs(a) -> int:
    cfg = load_config()
    store = TaskStore(cfg.db_path)
    runs = store.list_runs(limit=a.limit)
    _print({"count": len(runs), "runs": [r.__dict__ for r in runs]})
    return 0


def cmd_report(a) -> int:
    cfg = load_config()
    store = TaskStore(cfg.db_path)
    td = parse_window(a.window)
    since = (datetime.now(timezone.utc) - td).isoformat()
    runs = store.runs_in_window(since_iso=since)
    ok = sum(1 for r in runs if r.status == "success")
    fail = sum(1 for r in runs if r.status == "fail")
    _print({"window": a.window, "since": since, "runs": len(runs), "success": ok, "fail": fail})
    return 0


# ---- marker commands (will work once store has marker methods) ----

def cmd_marker_create(a) -> int:
    cfg = load_config()
    store = TaskStore(cfg.db_path)
    store.create_marker(marker_id=uuid.uuid4().hex, name=a.name)
    _print({"ok": True, "marker": a.name})
    return 0


def cmd_marker_list(a) -> int:
    cfg = load_config()
    store = TaskStore(cfg.db_path)
    _print({"markers": store.list_markers(limit=a.limit)})
    return 0


def cmd_marker_add_task(a) -> int:
    cfg = load_config()
    store = TaskStore(cfg.db_path)
    store.add_task_to_marker(marker_name=a.name, task_id=a.task_id)
    _print({"ok": True, "marker": a.name, "task_id": a.task_id})
    return 0


def cmd_marker_status(a) -> int:
    cfg = load_config()
    store = TaskStore(cfg.db_path)
    _print(store.marker_status(marker_name=a.name))
    return 0


def cmd_marker_close(a) -> int:
    cfg = load_config()
    store = TaskStore(cfg.db_path)
    store.close_marker(name=a.name)
    _print({"ok": True, "marker": a.name, "status": "closed"})
    return 0



def cmd_marker_reset(a) -> int:
    cfg = load_config()
    store = TaskStore(cfg.db_path)
    store.reset_marker(name=a.name)
    _print({"ok": True, "marker": a.name, "reset": True})
    return 0

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="tg", description="Task Guardian CLI")
    sub = p.add_subparsers(dest="cmd", required=True)

    s = sub.add_parser("init")
    s.set_defaults(fn=cmd_init)

    s = sub.add_parser("add")
    s.add_argument("--id", required=True)
    s.add_argument("--name", required=True)
    s.add_argument("--tool", required=True, choices=["exec", "gh", "http_request"])
    s.add_argument("--command")
    s.add_argument("--url")
    s.add_argument("--timeout", type=int, default=300)
    s.add_argument("--schedule", required=True)
    s.add_argument("--expected", default="task completes successfully")
    s.add_argument("--verify", default="file_exists", choices=["file_exists", "exit_code_0"])
    s.add_argument("--output")
    s.add_argument("--disabled", action="store_true")
    s.set_defaults(fn=cmd_add)

    s = sub.add_parser("run-task")
    s.add_argument("--id", required=True)
    s.set_defaults(fn=cmd_run_task)

    s = sub.add_parser("run-due")
    s.add_argument("--limit", type=int, default=25)
    s.add_argument("--marker", help="Run only tasks in this active marker")
    s.add_argument("--active-markers-only", action="store_true", help="Run only tasks belonging to any active marker")
    s.set_defaults(fn=cmd_run_due)

    s = sub.add_parser("runs")
    s.add_argument("--limit", type=int, default=50)
    s.set_defaults(fn=cmd_runs)

    s = sub.add_parser("report")
    s.add_argument("--window", default="24h")
    s.set_defaults(fn=cmd_report)

    m = sub.add_parser("marker", help="marker operations")
    ms = m.add_subparsers(dest="marker_cmd", required=True)

    mc = ms.add_parser("create")
    mc.add_argument("--name", required=True)
    mc.set_defaults(fn=cmd_marker_create)

    ml = ms.add_parser("list")
    ml.add_argument("--limit", type=int, default=50)
    ml.set_defaults(fn=cmd_marker_list)

    ma = ms.add_parser("add-task")
    ma.add_argument("--name", required=True)
    ma.add_argument("--task-id", required=True)
    ma.set_defaults(fn=cmd_marker_add_task)

    mst = ms.add_parser("status")
    mst.add_argument("--name", required=True)
    mst.set_defaults(fn=cmd_marker_status)

    mrst = ms.add_parser("reset")
    mrst.add_argument("--name", required=True)
    mrst.set_defaults(fn=cmd_marker_reset)

    mcl = ms.add_parser("close")
    mcl.add_argument("--name", required=True)
    mcl.set_defaults(fn=cmd_marker_close)

    return p


def main(argv=None) -> int:
    argv = argv if argv is not None else sys.argv[1:]
    args = build_parser().parse_args(argv)
    return int(args.fn(args))


if __name__ == "__main__":
    raise SystemExit(main())
