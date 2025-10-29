import random
from datetime import datetime
try:
    from zoneinfo import ZoneInfo  # Python 3.9+
except ImportError:
    from backports.zoneinfo import ZoneInfo  # 필요 시 설치: pip install backports.zoneinfo

# 1) 시간대별 루프 간격(초) 범위
TIME_INTERVAL_CONFIG = {
    (0, 7):   (0.20, 0.40),  # 심야: 천천히
    (8, 11):  (0.10, 0.30),  # 오전: 보통
    (12, 13): (0.05, 0.15),  # 점심 피크: 빠르게
    (14, 18): (0.10, 0.30),  # 오후: 보통
    (19, 23): (0.15, 0.35),  # 저녁: 약간 느리게
}

# 2) 서비스별 1루프당 생성 개수 기본값
SERVICE_BATCH_DEFAULT = {
    "auth":    (120, 240),
    "notify":  (100, 200),
    "order":   (140, 260),
    "payment": (120, 220),
}

# 3) (선택) 시간대별 서비스별 오버라이드
SERVICE_HOURLY_OVERRIDES = {
    (12, 13): {  # 점심 피크에 주문/결제 증폭
        "order":   (220, 380),
        "payment": (200, 360),
    },
    (14, 18): {  # 오후에도 살짝 높임
        "order":   (180, 320),
        "payment": (160, 300),
    },
}

SEOUL = ZoneInfo("Asia/Seoul")

def _match_range(hour: int, table: dict):
    for (start, end), val in table.items():
        if start <= hour <= end:
            return val
    return None

def interval_for_now(seoul_now: datetime) -> float:
    """현재(서울 기준) 시간대에 맞는 루프 간격(초)을 랜덤으로 반환"""
    rng = _match_range(seoul_now.hour, TIME_INTERVAL_CONFIG)
    if rng:
        mn, mx = rng
        return random.uniform(mn, mx)
    return 0.20

def batch_for_service(seoul_now: datetime, service: str) -> int:
    """현재 시간대 + 서비스에 맞는 1루프당 생성 개수를 랜덤으로 반환"""
    override = _match_range(seoul_now.hour, SERVICE_HOURLY_OVERRIDES) or {}
    mn, mx = override.get(service, SERVICE_BATCH_DEFAULT.get(service, (100, 200)))
    return random.randint(mn, mx)
