from __future__ import annotations
import os
from dataclasses import dataclass
from pathlib import Path

@dataclass(frozen=True)
class TGConfig:
    data_dir: Path
    log_dir: Path
    db_path: Path

def load_config() -> TGConfig:
    cwd = Path.cwd()
    default_data = cwd / "data"
    default_logs = cwd / "logs"

    data_dir = Path(os.getenv(
        "TG_DATA_DIR",
        str(default_data if default_data.exists() else Path.home() / ".task-guardian" / "data")
    )).expanduser()

    log_dir = Path(os.getenv(
        "TG_LOG_DIR",
        str(default_logs if default_logs.exists() else Path.home() / ".task-guardian" / "logs")
    )).expanduser()

    data_dir.mkdir(parents=True, exist_ok=True)
    log_dir.mkdir(parents=True, exist_ok=True)

    db_path = data_dir / "task_guardian.db"
    return TGConfig(data_dir=data_dir, log_dir=log_dir, db_path=db_path)
