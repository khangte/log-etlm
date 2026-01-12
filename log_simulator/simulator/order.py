# -----------------------------------------------------------------------------
# 파일명 : log_simulator/simulator/order.py
# 목적   : 주문 도메인의 주요 API 패턴 기반 이벤트 생성(개선 버전)
# 설명   :
#   - 요청 1건당 도메인 이벤트 1개 생성(POST 중심, 설정에 따라 GET도 가능)
#   - 도메인 이벤트명은 routes.yml의 domain_events.success/fail를 사용
#   - 실패 시 reason_code를 정규화 코드로 기록
# -----------------------------------------------------------------------------

from __future__ import annotations

from typing import Any, Dict, List, Optional
from .base import BaseServiceSimulator


class OrderSimulator(BaseServiceSimulator):
    service = "order"
    domain = "order"

    ORDER_REASON_CODES = (
        "OUT_OF_STOCK",
        "INVALID_STATE",
        "CONFLICT",
        "INTERNAL_ERROR",
    )

    def _pick_reason_code(self) -> str:
        """주문 실패 원인 코드를 무작위로 선택한다."""
        return self._rng.choice(list(self.ORDER_REASON_CODES))

    def _infer_ids_for_route(self, route_path: str) -> Dict[str, Optional[str]]:
        """
        route_template에 따라 order_id 필요 여부를 판단해 채운다.
        """
        order_id: Optional[str] = None

        if "{order_id}" in route_path:
            order_id = self.generate_order_id()

        return {"order_id": order_id}

    def generate_events_one(self) -> List[Dict[str, Any]]:
        """요청 1건에 대한 도메인 이벤트 리스트를 생성한다."""
        route = self.pick_route()
        method = self.pick_method(route)

        now_ms = self.now_utc_ms()
        request_id = self.generate_request_id()

        is_err = self._is_err()
        # 공통 엔티티 필드(주문은 user/product가 의미있음)
        user_id = self.generate_user_id()
        product_id = int(self._rng.randint(100000, 999999))

        ids = self._infer_ids_for_route(route["path"])
        order_id = ids.get("order_id")

        events: List[Dict[str, Any]] = []
        if self._should_emit_domain_event(method, route, is_err):
            dom_name = self._domain_event_name(route, is_err)
            if dom_name:
                # 주문 생성(POST /v2/orders) 같은 경우는 order_id가 없으면 만들어 주는 편이 좋음
                if method == "POST" and route["path"] == "/v2/orders" and not order_id:
                    order_id = self.generate_order_id()

                dom_ev = self.make_domain_event(
                    ts_ms=now_ms,
                    request_id=request_id,
                    event_name=dom_name,
                    result="fail" if is_err else "success",
                    reason_code=self._pick_reason_code() if is_err else None,
                    user_id=user_id,
                    order_id=order_id,
                    api_group=route.get("api_group"),
                    route_template=route["path"],
                    extra={
                        "timestamp_ms": now_ms,   # 레거시 호환
                        "product_id": product_id,
                    },
                )
                events.append(dom_ev)

        return events
