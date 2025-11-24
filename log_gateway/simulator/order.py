# -----------------------------------------------------------------------------
# 파일명 : log_gateway/simulator/order.py
# 목적   : 주문 도메인의 주요 API 패턴을 기반으로 로그를 생성하는 시뮬레이터
# 설명   : routes.yml 라우트와 profile.error_rate 를 이용해 성공/실패 분기, 이벤트명 추상화
# -----------------------------------------------------------------------------

from __future__ import annotations
import random
from typing import Any, Dict, List
from .base import BaseServiceSimulator

class OrderSimulator(BaseServiceSimulator):
    """
    주문(order) 도메인 로그 시뮬레이터.

    - POST면 보통 생성(Create), GET이면 조회(Query)로 간주해 이벤트명을 단순화.
    - 실패 시 상태코드/지연을 넓게 분포시켜 장애 느낌을 준다.
    """

    service_name = "order"

    def __init__(self, routes: List[Dict[str, Any]], profile: Dict[str, Any]):
        """
        Args:
            routes: order용 라우트 목록
            profile: 시뮬레이터 프로파일(dict)
        """
        super().__init__(routes, profile)

    def generate_log_one(self) -> Dict[str, Any]:
        """
        order 로그 1건 생성.

        Returns:
            Dict[str, Any]: {"ts","svc","lvl","rid","met","path","st","lat","evt"}
        """
        r = self.pick_route(self.routes)
        m = self.pick_method(r)
        is_err = (random.random() < self.error_rate)

        # 메서드 기반 간단 이벤트명
        evt = "OrderCreated" if (m == "POST" and not is_err) else ("OrderQuery" if m == "GET" else "OrderOp")

        status = random.choice([500, 422, 409]) if is_err else random.choice([200, 201, 204])
        latency = round(random.uniform(80, 320) if is_err else random.uniform(30, 180), 2)

        return {
            "ts":  self.now_kst_iso(),
            "svc": self.service_name,
            "lvl": "E" if is_err else "I",
            "rid": self.new_request_id(),
            "met": m,
            "path": r["path"],
            "st":  status,
            "lat": latency,
            "evt": evt,
        }
