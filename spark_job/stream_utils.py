from __future__ import annotations

import re


def read_eps_from_profile(path: str) -> int | None:
    """profiles.yml에서 eps 값을 추출한다. (간단 파서)"""
    try:
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                stripped = line.strip()
                if not stripped or stripped.startswith("#"):
                    continue
                if stripped.startswith("eps:"):
                    value = stripped.split(":", 1)[1].strip().strip("'\"")
                    if not value:
                        return None
                    try:
                        return int(float(value))
                    except ValueError:
                        return None
        return None
    except FileNotFoundError:
        return None


def parse_duration_seconds(value: str | None) -> float | None:
    """문자열 기간(예: '3 seconds', '5s', '1500ms')을 초로 변환한다."""
    if not value:
        return None
    raw = value.strip().lower()
    if not raw:
        return None
    if raw.isdigit():
        return float(raw)

    match = re.match(r"^([0-9]+(?:\.[0-9]+)?)\s*([a-z]+)$", raw)
    if not match:
        parts = raw.split()
        if len(parts) == 2 and parts[0].replace(".", "", 1).isdigit():
            number = float(parts[0])
            unit = parts[1]
        else:
            return None
    else:
        number = float(match.group(1))
        unit = match.group(2)

    unit_map = {
        "ms": 1 / 1000,
        "millisecond": 1 / 1000,
        "milliseconds": 1 / 1000,
        "s": 1,
        "sec": 1,
        "secs": 1,
        "second": 1,
        "seconds": 1,
        "m": 60,
        "min": 60,
        "mins": 60,
        "minute": 60,
        "minutes": 60,
        "h": 3600,
        "hr": 3600,
        "hour": 3600,
        "hours": 3600,
    }
    if unit not in unit_map:
        return None
    return number * unit_map[unit]
