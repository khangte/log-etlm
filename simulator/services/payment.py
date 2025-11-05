from __future__ import annotations
import random
from typing import Any, Dict, List
from .common import BaseServiceSimulator

class PaymentSimulator(BaseServiceSimulator):
    """
    결제(payment) 도메인 로그 시뮬레이터.

    - 성공 시 PaymentAuthorized, 실패 시 PaymentFailed 이벤트로 요약.
    - 결제는 실패/타임아웃 분포를 조금 더 넓게 잡아 장애 체감을 높인다.
    """

    service_name = "payment"

    def __init__(self, routes: List[Dict[str, Any]], profile: Dict[str, Any]):
        """
        Args:
            routes: payment용 라우트 목록
            profile: 시뮬레이터 프로파일(dict)
        """
        super().__init__(routes, profile)

    def generate_one(self) -> Dict[str, Any]:
        """
        payment 로그 1건 생성.

        Returns:
            Dict[str, Any]: {"ts","svc","lvl","rid","met","path","st","lat","evt"}
        """
        r = self.pick_route(self.routes)
        m = self.pick_method(r)
        is_err = (random.random() < self.error_rate)

        status = random.choice([400, 402, 408, 500]) if is_err else random.choice([200, 201, 204])
        latency = round(random.uniform(100, 400) if is_err else random.uniform(40, 200), 2)
        event = "PaymentFailed" if is_err else "PaymentAuthorized"

        return {
            "ts":  self.now_utc_iso(),
            "svc": self.service_name,
            "lvl": "E" if is_err else "I",
            "rid": self.new_request_id(),
            "met": m,
            "path": r["path"],
            "st":  status,
            "lat": latency,
            "evt": event,
        }
