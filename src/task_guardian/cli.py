from __future__ import annotations
import argparse, json, sys
from datetime import datetime, timezone
from .config import load_config
from .scheduler import next_run_iso, parse_window
from .store import TaskStore
from .task_runner import run_due

def _print(obj): print(json.dumps(obj, indent=2, ensure_ascii=False))

def cmd_init(_):
    cfg = load_config()
    TaskStore(cfg.db_path)
    _print({"ok": True, "db_path": str(cfg.db_path), "data_dir": str(cfg.data_dir), "log_dir": str(cfg.log_dir)})
    return 0

def cmd_add(a):
    cfg = load_config()
    store = TaskStore(cfg.db_path)

    if a.tool in ("exec", "gh"):
        if not a.command: raise SystemExit("--command required")
        params = {"command": a.command, "timeout": a.timeout}
    elif a.tool == "http_request":
        if not a.url: raise SystemExit("--url required")
        params = {"url": a.url, "timeout": a.timeout}
    else:
        raise SystemExit("Unknown tool")

    output_path = a.output or f"runs/{a.id}.out.txt"
    nxt = next_run_iso(a.schedule)

    store.upsert_task(
        task_id=a.id, name=a.name, tool=a.tool, params=params,
        schedule=a.schedule, expected=a.expected, verify=a.verify,
        output_path=output_path, enabled=(not a.disabled), next_run_at=nxt
    )
    _print({"ok": True, "task_id": a.id, "next_run_at": nxt})
    return 0

def cmd_list(a):
    cfg = load_config()
    store = TaskStore(cfg.db_path)
    tasks = store.list_tasks(enabled_only=a.enabled_only, limit=a.limit)
    _print({"count": len(tasks), "tasks": [t.__dict__ for t in tasks]})
    return 0

def cmd_enable(a):
    cfg = load_config()
    TaskStore(cfg.db_path).set_enabled(a.id, True)
    _print({"ok": True, "id": a.id, "enabled": True})
    return 0

def cmd_disable(a):
    cfg = load_config()
    TaskStore(cfg.db_path).set_enabled(a.id, False)
    _print({"ok": True, "id": a.id, "enabled": False})
    return 0

def cmd_run_due(a):
    cfg = load_config()
    out = run_due(cfg=cfg, limit=a.limit)
    _print(out)
    return 0 if all(r.get("status") == "success" for r in out["results"]) else 2

def cmd_runs(a):
    cfg = load_config()
    runs = TaskStore(cfg.db_path).list_runs(limit=a.limit)
    _print({"count": len(runs), "runs": [r.__dict__ for r in runs]})
    return 0

def cmd_report(a):
    cfg = load_config()
    td = parse_window(a.window)
    since = (datetime.now(timezone.utc) - td).isoformat()
    runs = TaskStore(cfg.db_path).runs_in_window(since_iso=since)
    ok = sum(1 for r in runs if r.status == "success")
    fail = sum(1 for r in runs if r.status == "fail")
    _print({"window": a.window, "since": since, "runs": len(runs), "success": ok, "fail": fail})
    return 0

def cmd_import_task_queue(a):
    cfg = load_config()
    store = TaskStore(cfg.db_path)
    q = json.load(open(a.path, "r", encoding="utf-8"))
    imported = []
    for t in q.get("tasks", []):
        task_id = t["id"]
        nxt = next_run_iso(t.get("schedule", "cron:0 * * * *"))
        store.upsert_task(
            task_id=task_id,
            name=t.get("name", task_id),
            tool=t.get("tool", "exec"),
            params=t.get("params", {}),
            schedule=t.get("schedule", "cron:0 * * * *"),
            expected=t.get("description", "task output exists"),
            verify=t.get("verify", "file_exists"),
            output_path=t.get("output", f"runs/{task_id}.out.txt"),
            enabled=True,
            next_run_at=nxt
        )
        imported.append(task_id)
    _print({"ok": True, "imported": len(imported), "task_ids": imported})
    return 0

def build_parser():
    p = argparse.ArgumentParser(prog="tg", description="Task Guardian CLI")
    sub = p.add_subparsers(dest="cmd", required=True)

    s = sub.add_parser("init"); s.set_defaults(fn=cmd_init)

    s = sub.add_parser("add")
    s.add_argument("--id", required=True)
    s.add_argument("--name", required=True)
    s.add_argument("--tool", required=True, choices=["exec","gh","http_request"])
    s.add_argument("--command")
    s.add_argument("--url")
    s.add_argument("--timeout", type=int, default=300)
    s.add_argument("--schedule", required=True)
    s.add_argument("--expected", default="task completes successfully")
    s.add_argument("--verify", default="file_exists", choices=["file_exists","exit_code_0"])
    s.add_argument("--output")
    s.add_argument("--disabled", action="store_true")
    s.set_defaults(fn=cmd_add)

    s = sub.add_parser("list")
    s.add_argument("--limit", type=int, default=50)
    s.add_argument("--enabled-only", action="store_true")
    s.set_defaults(fn=cmd_list)

    s = sub.add_parser("enable"); s.add_argument("--id", required=True); s.set_defaults(fn=cmd_enable)
    s = sub.add_parser("disable"); s.add_argument("--id", required=True); s.set_defaults(fn=cmd_disable)

    s = sub.add_parser("run-due")
    s.add_argument("--limit", type=int, default=25)
    s.set_defaults(fn=cmd_run_due)

    s = sub.add_parser("runs")
    s.add_argument("--limit", type=int, default=50)
    s.set_defaults(fn=cmd_runs)

    s = sub.add_parser("report")
    s.add_argument("--window", default="24h")
    s.set_defaults(fn=cmd_report)

    s = sub.add_parser("import-task-queue")
    s.add_argument("--path", required=True)
    s.set_defaults(fn=cmd_import_task_queue)

    return p

def main(argv=None):
    argv = argv if argv is not None else sys.argv[1:]
    args = build_parser().parse_args(argv)
    return int(args.fn(args))

if __name__ == "__main__":
    raise SystemExit(main())
