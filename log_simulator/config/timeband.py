# -----------------------------------------------------------------------------
# 파일명 : log_simulator/config/timeband.py
# 목적   : profiles/*.yaml의 time_weights 설정을 Band 객체로 변환하고 multiplier 계산
# 사용   : generator가 현재 시각(Asia/Seoul)에 맞는 배수를 선택해 EPS 조절
# 설명   : range "0-7" + weight [min,max] 형태를 지원
# -----------------------------------------------------------------------------

from __future__ import annotations
from typing import List, Dict, Tuple
from dataclasses import dataclass
from zoneinfo import ZoneInfo
from datetime import datetime
import random


KST = ZoneInfo("Asia/Seoul")


@dataclass
class Band:
    """시간대 범위와 [min,max] 가중치."""
    start: int
    end: int
    w_min: float
    w_max: float

    def contains(self, hour: int) -> bool:
        """해당 band가 hour(KST)를 포함하는지 확인."""
        return self.start <= hour <= self.end


def _parse_range(r: str) -> Tuple[int, int]:
    """
    "8-11" 같은 범위를 (8, 11)로 파싱.
    Args: r: "start-end" 형태 문자열(0~23 범위)
    Returns: (start, end)
    """
    s, e = r.split("-")
    return int(s), int(e)


def load_bands(raw_bands: List[Dict]) -> List[Band]:
    """
    프로파일의 time_weights 배열을 Band 리스트로 변환한다.
    Args: raw_bands: [{range:"0-7", weight:[0.2,0.4]}, ...]
    Returns: List[Band]: 사용 가능한 band 목록
    """
    bands: List[Band] = []
    for item in raw_bands or []:
        start, end = _parse_range(str(item.get("range", "0-23")))
        w = item.get("weight", [0.1, 0.3])
        w_min = float(w[0])
        w_max = float(w[1]) if len(w) > 1 else w_min
        bands.append(Band(start, end, w_min, w_max))
    return bands


def current_hour_kst(now: datetime | None = None) -> int:
    """
    현재 KST(Asia/Seoul) 시간의 시(hour)를 반환.
    Args: now: 테스트/주입용 시각(UTC 또는 naive). None이면 현재 시각 사용.
    Returns: int: 0 ~ 23
    """
    if now is None:
        now = datetime.now(tz=KST)
    else:
        # naive거나 TZ가 없으면 KST로 가정
        if now.tzinfo is None:
            now = now.replace(tzinfo=KST)
        else:
            now = now.astimezone(KST)
    return now.hour


def pick_multiplier(bands: List[Band], hour_kst: int) -> float:
    """
    현재 시각(KST)에 해당하는 band에서 multiplier를 선택한다.
    Args:
        bands: Band 리스트(load_bands 결과)
        hour_kst: 0~23
    Returns: float: multiplier (기본 1.0)
    """
    for b in bands:
        if b.contains(hour_kst):
            return random.uniform(b.w_min, b.w_max)
    return 1.0  # 매칭되는 band가 없으면 1.0
