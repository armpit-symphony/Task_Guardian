from __future__ import annotations
from datetime import datetime, timezone, timedelta
from typing import Optional
from croniter import croniter

def parse_schedule(schedule: str) -> tuple[str, str]:
    if ":" not in schedule:
        raise ValueError("schedule must be cron:<expr> or every:<seconds>")
    kind, val = schedule.split(":", 1)
    kind = kind.strip().lower()
    val = val.strip()
    if kind not in ("cron", "every"):
        raise ValueError("schedule kind must be cron or every")
    return kind, val

def next_run_iso(schedule: str, *, base: Optional[datetime] = None) -> str:
    base = base or datetime.now(timezone.utc)
    kind, val = parse_schedule(schedule)
    if kind == "every":
        return (base + timedelta(seconds=int(val))).isoformat()
    it = croniter(val, base)
    return it.get_next(datetime).replace(tzinfo=timezone.utc).isoformat()

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def parse_window(window: str) -> timedelta:
    w = window.strip().lower()
    if w.endswith("h"): return timedelta(hours=int(w[:-1]))
    if w.endswith("d"): return timedelta(days=int(w[:-1]))
    if w.endswith("m"): return timedelta(minutes=int(w[:-1]))
    raise ValueError("window must end with m/h/d (e.g. 90m, 24h, 7d)")
