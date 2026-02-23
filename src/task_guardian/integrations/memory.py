from __future__ import annotations

import os
from typing import Optional

def _load_memory():
    """
    Support both historical layouts:
      - memory_os.*     (older / alternate)
      - memory_guardian.* (current repo layout)
    """
    # Try memory_os first
    try:
        from memory_os.api import MemoryGuardian  # type: ignore
        from memory_os.config import Config  # type: ignore
        return MemoryGuardian, Config
    except Exception:
        pass

    # Try memory_guardian
    from memory_guardian.api import MemoryGuardian  # type: ignore
    from memory_guardian.config import Config  # type: ignore
    return MemoryGuardian, Config

def remember_if_available(*, session_id: str, role: str, content: str, data_dir: Optional[str] = None) -> bool:
    """
    Writes a message to Memory Guardian if available.

    Priority for data dir:
      1) TG_MEMORY_DIR env var (recommended: central memory_guardian/data)
      2) data_dir argument
      3) ./data
    """
    try:
        MemoryGuardian, Config = _load_memory()
        chosen = os.getenv("TG_MEMORY_DIR") or data_dir or "./data"
        mem = MemoryGuardian(Config(data_dir=chosen))
        mem.remember_message(role=role, content=content, session_id=session_id)
        return True
    except Exception:
        return False
