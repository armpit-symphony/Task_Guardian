from __future__ import annotations
import subprocess
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable
from .logging_utils import log_line

@dataclass
class ExecResult:
    ok: bool
    message: str
    meta: dict[str, Any]

def _write_output(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")

def verify_file_exists(output_path: Path) -> tuple[bool, str]:
    ok = output_path.exists() and output_path.stat().st_size > 0
    return ok, f"Output file {'exists' if ok else 'missing'}: {output_path}"

def verify_exit_code_0(output_path: Path) -> tuple[bool, str]:
    ok = output_path.exists() and output_path.stat().st_size > 0
    return ok, f"Exit code verification: {'passed' if ok else 'failed'}"

def verifier(name: str) -> Callable[[Path], tuple[bool, str]]:
    name = (name or "file_exists").strip().lower()
    if name == "file_exists":
        return verify_file_exists
    if name == "exit_code_0":
        return verify_exit_code_0
    return lambda p: (False, f"Unknown verify type: {name}")

def new_run_id() -> str:
    return uuid.uuid4().hex

def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def run_subprocess_and_capture(*, cmd: str, timeout: int, log_file: Path) -> tuple[bool, str, dict]:
    log_line(log_file, f"  run: {cmd[:140]}")
    r = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=timeout)
    text = (r.stdout or "")
    if r.stderr:
        text += "\n--- STDERR ---\n" + r.stderr
    ok = (r.returncode == 0)
    return ok, text, {"returncode": r.returncode, "stdout_len": len(r.stdout or ""), "stderr_len": len(r.stderr or "")}
