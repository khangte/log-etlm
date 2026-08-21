from __future__ import annotations

import yaml


def read_eps_from_profile(path: str) -> int | None:
    """profiles.yml에서 eps 값을 추출한다."""
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
        value = data.get("eps")
        return int(float(value)) if value is not None else None
    except (FileNotFoundError, ValueError, TypeError):
        return None
