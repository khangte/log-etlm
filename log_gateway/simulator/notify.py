# -----------------------------------------------------------------------------
# 파일명 : log_gateway/simulator/notify.py
# 목적   : 알림 서비스(notify)의 로그 패턴을 생성하는 시뮬레이터 구현
# 설명   : routes.yml 라우트를 기반으로 NotificationSent/Failed 이벤트를 만들어 Kafka 발행용 payload 생성
# -----------------------------------------------------------------------------

from __future__ import annotations
import random
from typing import Any, Dict, List
from .base import BaseServiceSimulator

class NotifySimulator(BaseServiceSimulator):
    """
    알림(notify) 도메인 로그 시뮬레이터.

    - 성공 시 NotificationSent, 실패 시 NotificationFailed로 단순화.
    - 알림은 외부 연동 특성상 429/500 등으로 실패 분포를 약간 포함.
    """

    service_name = "notify"

    def __init__(self, routes: List[Dict[str, Any]], profile: Dict[str, Any]):
        """
        Args:
            routes: notify용 라우트 목록
            profile: 시뮬레이터 프로파일(dict)
        """
        super().__init__(routes, profile)

    def generate_log_one(self) -> Dict[str, Any]:
        """
        notify 로그 1건 생성.

        Returns:
            Dict[str, Any]: {"ts","svc","lvl","rid","met","path","st","lat","evt"}
        """
        r = self.pick_route(self.routes)
        m = self.pick_method(r)
        is_err = (random.random() < self.error_rate)

        status = random.choice([500, 429, 400]) if is_err else random.choice([200, 202, 204])
        latency = round(random.uniform(60, 300) if is_err else random.uniform(20, 120), 2)
        event = "NotificationFailed" if is_err else "NotificationSent"

        return {
            "ts":  self.now_kst_iso(),
            "svc": self.service_name,
            "lvl": "E" if is_err else "I",
            "rid": self.new_request_id(),
            "met": m,
            "path": r["path"],
            "st":  status,
            "lat": latency,
            "evt": event,
        }
