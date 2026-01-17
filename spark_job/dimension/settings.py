from __future__ import annotations

from dataclasses import dataclass
import os
from typing import Mapping, Optional

from common.get_env import get_env_int, get_env_str


@dataclass(frozen=True)
class DimensionBatchSettings:
    """디멘전 배치 설정 값을 담는다."""
    lookback_days: int
    service_map_path: Optional[str]


_DEFAULT_LOOKBACK_DAYS = 1


def load_dimension_batch_settings(
    env: Mapping[str, str] | None = None,
) -> DimensionBatchSettings:
    """환경 변수에서 디멘전 배치 설정을 로드한다."""
    source = env or os.environ
    return DimensionBatchSettings(
        lookback_days=get_env_int(
            source,
            "DIM_BATCH_LOOKBACK_DAYS",
            _DEFAULT_LOOKBACK_DAYS,
        ),
        service_map_path=get_env_str(source, "DIM_SERVICE_MAP_PATH"),
    )


_settings_cache: DimensionBatchSettings | None = None


def get_dimension_batch_settings() -> DimensionBatchSettings:
    """캐시된 디멘전 배치 설정을 반환한다."""
    global _settings_cache
    if _settings_cache is None:
        _settings_cache = load_dimension_batch_settings()
    return _settings_cache
