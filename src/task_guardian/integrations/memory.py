from __future__ import annotations
from typing import Optional

def remember_if_available(*, session_id: str, role: str, content: str, data_dir: Optional[str] = None) -> bool:
    try:
        from memory_os.api import MemoryGuardian  # type: ignore
        from memory_os.config import Config  # type: ignore
        mem = MemoryGuardian(Config(data_dir=data_dir or "./data"))
        mem.remember_message(role=role, content=content, session_id=session_id)
        return True
    except Exception:
        return False
