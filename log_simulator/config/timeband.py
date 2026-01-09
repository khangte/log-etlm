# -----------------------------------------------------------------------------
# 파일명 : log_simulator/config/timeband.py
# 목적   : profiles/*.yaml의 time_weights 설정을 Band 객체로 변환하고 multiplier 계산
# 사용   : generator가 현재 시각(Asia/Seoul)에 맞는 배수를 선택해 EPS 조절
# 설명   : range "0-7" + weight [min,max], weight_mode(uniform/mid/low/high) 지원
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
    """시간 범위를 시작/끝 값으로 파싱한다."""
    s, e = r.split("-")
    return int(s), int(e)


def load_bands(raw_bands: List[Dict]) -> List[Band]:
    """time_weights 설정을 Band 목록으로 변환한다."""
    bands: List[Band] = []
    for item in raw_bands or []:
        start, end = _parse_range(str(item.get("range", "0-23")))
        w = item.get("weight", [0.1, 0.3])
        w_min = float(w[0])
        w_max = float(w[1]) if len(w) > 1 else w_min
        bands.append(Band(start, end, w_min, w_max))
    return bands


def current_hour_kst(now: datetime | None = None) -> int:
    """현재 KST 기준 시각의 hour를 반환한다."""
    if now is None:
        now = datetime.now(tz=KST)
    else:
        # naive거나 TZ가 없으면 KST로 가정
        if now.tzinfo is None:
            now = now.replace(tzinfo=KST)
        else:
            now = now.astimezone(KST)
    return now.hour


def pick_multiplier(bands: List[Band], hour_kst: int, mode: str = "uniform") -> float:
    """현재 시간대에 맞는 multiplier를 균등 확률로 선택한다."""
    for b in bands:
        if b.contains(hour_kst):
            return random.uniform(b.w_min, b.w_max)
    return 1.0  # 매칭되는 band가 없으면 1.0
