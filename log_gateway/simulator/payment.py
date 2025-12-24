# -----------------------------------------------------------------------------
# 파일명 : log_gateway/simulator/payment.py
# 목적   : 결제 도메인 트랜잭션 로그를 모사하는 시뮬레이터 클래스
# 설명   : profile.error_rate에 따른 실패 분포와 routes.yml에 따른 경로/메서드 조합으로 이벤트 생성
# -----------------------------------------------------------------------------

from __future__ import annotations
import random
from typing import Any, Dict, List
from .base import BaseServiceSimulator

class PaymentSimulator(BaseServiceSimulator):
    """
    결제(payment) 도메인 로그 시뮬레이터.

    - 성공 시 PaymentAuthorized, 실패 시 PaymentFailed 이벤트로 요약.
    - 결제는 실패/타임아웃 분포를 조금 더 넓게 잡아 장애 체감을 높인다.
    """

    service = "payment"

    def __init__(self, routes: List[Dict[str, Any]], profile: Dict[str, Any]):
        """
        Args:
            routes: payment용 라우트 목록
            profile: 시뮬레이터 프로파일(dict)
        """
        super().__init__(routes, profile)

    def generate_log_one(self) -> Dict[str, Any]:
        """
        payment 로그 1건 생성.
        """
        is_err = (random.random() < self.error_rate)
        route = self.pick_route(self.routes)
        method = self.pick_method(route)

        log = {
            "timestamp": self.now_utc_iso(),
            "timestamp_ms": self.now_utc_ms(),
            "service": self.service,
            "level": "ERROR" if is_err else "INFO",
            "request_id": self.generate_request_id(),
            "method": method,
            "path": route["path"],
            "status_code":  random.choice([400, 402, 408, 500]) if is_err else random.choice([200, 201, 204]),
            "event": "PaymentFailed" if is_err else "PaymentAuthorized",

            "user_id": self.generate_user_id(),
            "amount": random.randint(1000, 500000),
        }

        return log
