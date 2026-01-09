# -----------------------------------------------------------------------------
# 파일명 : log_simulator/simulator/payment.py
# 목적   : 결제 도메인 트랜잭션 이벤트 생성(개선 버전)
# 설명   :
#   - 요청 1건당 payment 도메인 이벤트 생성(POST 중심)
#   - 도메인 이벤트명은 routes.yml의 domain_events.success/fail를 사용
#   - 실패 시 reason_code를 정규화 코드로 기록
# -----------------------------------------------------------------------------

from __future__ import annotations

import time
from typing import Any, Dict, List, Optional

from .base import BaseServiceSimulator


class PaymentSimulator(BaseServiceSimulator):
    service = "payment"
    domain = "payment"

    PAYMENT_REASON_CODES = (
        "PG_TIMEOUT",
        "INSUFFICIENT_FUNDS",
        "FRAUD_SUSPECTED",
        "INTERNAL_ERROR",
    )

    def _pick_reason_code(self) -> str:
        """에러 사유 코드를 선택한다."""
        return self._rng.choice(list(self.PAYMENT_REASON_CODES))

    def _infer_ids_for_route(self, route_path: str, method: str) -> Dict[str, Optional[str]]:
        """라우트에 따라 필요한 ID를 보완한다."""
        payment_id: Optional[str] = None

        if "{payment_id}" in route_path:
            payment_id = self.generate_payment_id()
        elif method == "POST" and route_path == "/v2/payments":
            payment_id = self.generate_payment_id()

        return {"payment_id": payment_id}

    def generate_events_one(self) -> List[Dict[str, Any]]:
        """요청 1건의 결제 이벤트 목록을 생성한다."""
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
        ids = self._infer_ids_for_route(route["path"], method)
        payment_id = ids.get("payment_id")
        # 결제는 order_id와 연계되면 좋은데(전환/리드타임 KPI),
        # 아직 공유 state를 안 넣는 단계라 일단 랜덤 생성(필요 시 추후 state로 연결)
        order_id = self.generate_order_id()
        if profile:
            t_ids = time.perf_counter()

        is_err = self._is_err()
        amount = int(self._rng.randint(1000, 500000))
        if profile:
            t_rand = time.perf_counter()

        events: List[Dict[str, Any]] = []
        # 도메인 이벤트(조건부)
        if self._emit_domain_event() and self._should_emit_domain_event(method, route, is_err):
            dom_name = self._domain_event_name(route, method, is_err)
            if dom_name:
                dom_ev = self.make_domain_event(
                    ts_ms=now_ms,
                    request_id=request_id,
                    event_name=dom_name,
                    result="fail" if is_err else "success",
                    reason_code=self._pick_reason_code() if is_err else None,
                    user_id=user_id,
                    order_id=order_id,
                    payment_id=payment_id,
                    amount=amount,
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
