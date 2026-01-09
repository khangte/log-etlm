# -----------------------------------------------------------------------------
# 파일명 : log_simulator/simulator/order.py
# 목적   : 주문 도메인의 주요 API 패턴 기반 이벤트 생성(개선 버전)
# 설명   :
#   - 요청 1건당 order 도메인 이벤트 생성(POST 중심, 설정에 따라 GET도 가능)
#   - 도메인 이벤트명은 routes.yml의 domain_events.success/fail를 사용
#   - 실패 시 reason_code를 정규화 코드로 기록
# -----------------------------------------------------------------------------

from __future__ import annotations

import time
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
        """에러 사유 코드를 선택한다."""
        return self._rng.choice(list(self.ORDER_REASON_CODES))

    def _infer_ids_for_route(self, route_path: str) -> Dict[str, Optional[str]]:
        """라우트에 따라 필요한 ID를 보완한다."""
        order_id: Optional[str] = None

        if "{order_id}" in route_path:
            order_id = self.generate_order_id()

        return {"order_id": order_id}

    def generate_events_one(self) -> List[Dict[str, Any]]:
        """요청 1건의 주문 이벤트 목록을 생성한다."""
        profile = self._profile_enabled
        if profile:
            t_start = time.perf_counter()
            self._profile_reset_id_timings()

        route = self.pick_route()
        method = self.pick_method(route)
        if profile:
            t_pick = time.perf_counter()

        now_ms = self.now_utc_ms()
        request_id = self.generate_request_id()
        user_id = self.generate_user_id()
        ids = self._infer_ids_for_route(route["path"])
        order_id = ids.get("order_id")
        if profile:
            t_ids = time.perf_counter()

        is_err = self._is_err()
        product_id = int(self._rng.randint(100000, 999999))
        if profile:
            t_rand = time.perf_counter()

        # 공통 엔티티 필드(주문은 user/product가 의미있음)
        events: List[Dict[str, Any]] = []
        # 도메인 이벤트(조건부)
        if self._emit_domain_event() and self._should_emit_domain_event(method, route, is_err):
            dom_name = self._domain_event_name(route, method, is_err)
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
                    extra={
                        "product_id": product_id,
                    },
                )
                events.append(dom_ev)

        if profile:
            t_done = time.perf_counter()
            idgen_ms, idgen_count = self._profile_get_id_timings()
            self._maybe_log_gen_profile(
                service=self.service,
                event_count=len(events),
                pick_ms=(t_pick - t_start) * 1000.0,
                id_ms=(t_ids - t_pick) * 1000.0,
                rand_ms=(t_rand - t_ids) * 1000.0,
                event_ms=(t_done - t_rand) * 1000.0,
                total_ms=(t_done - t_start) * 1000.0,
                idgen_ms=idgen_ms,
                idgen_count=idgen_count,
            )

        return events
