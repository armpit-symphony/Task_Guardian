from __future__ import annotations
import json
import os
from pathlib import Path
from typing import Any, Dict, Tuple
from .config import TGConfig
from .executor import verifier, new_run_id, now_iso, _write_output, run_subprocess_and_capture
from .logging_utils import log_line
from .scheduler import next_run_iso, utc_now_iso
from .store import TaskStore, Task
from .integrations.executive import exec_with_guard_if_available
from .integrations.memory import remember_if_available

def _tier(exec_ok: bool, verify_ok: bool) -> tuple[str, dict[str, Any]]:
    if exec_ok and verify_ok:
        return "SUCCESS", {"ok": True}
    return "FAIL", {"ok": False, "exec_ok": exec_ok, "verify_ok": verify_ok}

def run_task_once(*, cfg: TGConfig, store: TaskStore, task: Task, lane: str = "task_guardian") -> dict[str, Any]:
    log_file = cfg.log_dir / "task_guardian.log"
    params = json.loads(task.params_json)

    output_path = (cfg.data_dir / task.output_path).resolve() if not Path(task.output_path).is_absolute() else Path(task.output_path)

    run_id = new_run_id()
    started_at = now_iso()

    store.create_run(run_id=run_id, task_id=task.id, started_at=started_at, output_path=str(output_path),
                     meta={"tool": task.tool, "schedule": task.schedule, "expected": task.expected})

    remember_if_available(session_id="task_guardian", role="SYSTEM",
                         content=f"Task started: {task.id} | {task.name} | tool={task.tool} | run_id={run_id}",
                         data_dir=str(cfg.data_dir))

    log_line(log_file, f"--- Task start {task.id} ({task.name}) run_id={run_id} ---")

    def perform() -> dict[str, Any]:
        tool = task.tool
        timeout = int(params.get("timeout", 300))

        if tool in ("exec", "gh"):
            cmd = params.get("command", "")
            ok, text, meta = run_subprocess_and_capture(cmd=cmd, timeout=timeout, log_file=log_file)
            _write_output(output_path, text)
            return {"ok": ok, "message": f"Exit code: {meta.get('returncode')}", "meta": meta, "output_path": str(output_path)}

        if tool == "http_request":
            url = params.get("url", "")
            cmd = f"curl -sS --max-time {timeout} {json.dumps(url)}"
            ok, text, meta = run_subprocess_and_capture(cmd=cmd, timeout=timeout+5, log_file=log_file)
            _write_output(output_path, text)
            return {"ok": ok, "message": f"HTTP exit code: {meta.get('returncode')}", "meta": meta, "output_path": str(output_path)}

        _write_output(output_path, f"Unknown tool: {tool}")
        return {"ok": False, "message": f"Unknown tool: {tool}", "meta": {"tool": tool}, "output_path": str(output_path)}

    def validate(result: dict[str, Any]) -> Tuple[str, Dict[str, Any]]:
        v = verifier(task.verify)
        verify_ok, verify_msg = v(output_path)
        tier, meta = _tier(bool(result.get("ok")), verify_ok)
        meta.update({"verify_msg": verify_msg})
        return tier, meta

    result = exec_with_guard_if_available(
        task_id=task.id,
        lane=lane,
        action_type="command_exec" if task.tool in ("exec", "gh") else "http_request",
        expected_outcome=task.expected,
        confidence_pre=0.7,
        perform_fn=perform,
        validate_fn=validate,
        metadata={"run_id": run_id, "task_name": task.name, "tg_lane": lane, "tg_agent": "task_guardian"},
    )

    tier, vmeta = validate(result)
    finished_at = now_iso()
    final_ok = (tier.upper() == "SUCCESS")
    status = "success" if final_ok else "fail"
    message = f"{status.upper()} | {result.get('message','')}".strip()

    store.finish_run(run_id=run_id, status=status, message=message, finished_at=finished_at, meta_updates=vmeta)

    try:
        nxt = next_run_iso(task.schedule)
    except Exception:
        nxt = None
    store.set_next_run(task.id, nxt)

    remember_if_available(session_id="task_guardian", role="SYSTEM",
                         content=f"Task finished: {task.id} | {task.name} | status={status} | run_id={run_id} | output={output_path}",
                         data_dir=str(cfg.data_dir))

    log_line(log_file, f"--- Task end {task.id} status={status} next_run_at={nxt} ---")

    return {"run_id": run_id, "task_id": task.id, "name": task.name, "status": status,
            "message": message, "output_path": str(output_path), "finished_at": finished_at, "next_run_at": nxt}

def run_due(*, cfg: TGConfig, limit: int = 25, marker_name: str | None = None, active_markers_only: bool = False) -> dict[str, Any]:
    store = TaskStore(cfg.db_path)
    now = utc_now_iso()
    fail_fast = os.getenv('TG_FAIL_FAST','0') == '1'
    if marker_name:
        due = store.due_tasks_for_marker(marker_name=marker_name, now_iso=now, limit=limit)
    elif active_markers_only:
        if fail_fast:
            # only run tasks in GREEN active markers
            due = []
            for m in store.list_markers(limit=200):
                if m.get('status') != 'active':
                    continue
                st = store.marker_status(marker_name=m['name'])
                if st.get('green'):
                    due.extend(store.due_tasks_for_marker(marker_name=m['name'], now_iso=now, limit=limit))
            due = due[:limit]
        else:
            due = store.due_tasks_active_markers_only(now_iso=now, limit=limit)
    else:
        due = store.due_tasks(now_iso=now, limit=limit)

    log_file = cfg.log_dir / "task_guardian.log"
    log_line(log_file, f"RUN_DUE: {len(due)} tasks due (limit={limit})")

    results = []
    for t in due:
        try:
            results.append(run_task_once(cfg=cfg, store=store, task=t))
        except Exception as e:
            try:
                store.set_next_run(t.id, next_run_iso(t.schedule))
            except Exception:
                store.set_next_run(t.id, None)
            results.append({"task_id": t.id, "name": t.name, "status": "fail", "message": str(e)})

    return {"now": now, "count": len(results), "results": results}
