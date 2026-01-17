from __future__ import annotations

from typing import Mapping, Optional


def get_env_str(
    env: Mapping[str, str],
    key: str,
    default: Optional[str] = None,
) -> Optional[str]:
    """환경 변수 문자열을 가져온다."""
    value = env.get(key)
    if value is None:
        return default
    value = value.strip()
    return value if value else default


def get_env_int(
    env: Mapping[str, str],
    key: str,
    default: Optional[int] = None,
) -> Optional[int]:
    """환경 변수 정수를 가져온다."""
    value = get_env_str(env, key)
    if value is None:
        return default
    try:
        return int(value)
    except ValueError as exc:
        raise ValueError(f"{key} must be an integer (got: {value})") from exc


def get_env_float(
    env: Mapping[str, str],
    key: str,
    default: Optional[float] = None,
) -> Optional[float]:
    """환경 변수 실수를 가져온다."""
    value = get_env_str(env, key)
    if value is None:
        return default
    try:
        return float(value)
    except ValueError as exc:
        raise ValueError(f"{key} must be a float (got: {value})") from exc


def get_env_bool(env: Mapping[str, str], key: str, default: bool) -> bool:
    """환경 변수 불리언을 가져온다."""
    value = get_env_str(env, key)
    if value is None:
        return default
    return value.lower() in ("1", "true", "yes", "y")
