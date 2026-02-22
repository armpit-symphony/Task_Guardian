from __future__ import annotations
from typing import Any, Callable, Dict, Tuple

def exec_with_guard_if_available(
    *, task_id: str, lane: str, action_type: str,
    expected_outcome: str, confidence_pre: float,
    perform_fn: Callable[[], Any],
    validate_fn: Callable[[Any], Tuple[str, Dict[str, Any]]],
    metadata: Dict[str, Any] | None = None,
) -> Any:
    try:
        from executive_guardian import guardian as eg  # type: ignore
        return eg.exec_with_guard(
            task_id=task_id,
            lane=lane,
            action_type=action_type,
            expected_outcome=expected_outcome,
            confidence_pre=confidence_pre,
            perform_fn=perform_fn,
            validate_fn=validate_fn,
            metadata=metadata,
        )
    except Exception:
        return perform_fn()
