# -----------------------------------------------------------------------------
# 파일명 : log_simulator/simulator/services/payment.py
# 목적   : 결제 도메인 트랜잭션 이벤트 생성(개선 버전)
# 설명   :
#   - 요청 1건당 이벤트 1~2개 생성
#       1) http_request_completed (항상)
#       2) payment 도메인 이벤트(POST 중심)
#   - 도메인 이벤트명은 routes.yml의 domain_events.success/fail를 사용
#   - 실패 시 reason_code를 정규화 코드로 기록
# -----------------------------------------------------------------------------

from __future__ import annotations

from typing import Any, Dict, List, Optional
from ..base import BaseServiceSimulator


class PaymentSimulator(BaseServiceSimulator):
    service = "payment"
    domain = "payment"

    PAYMENT_REASON_CODES = (
        "PG_TIMEOUT",
        "INSUFFICIENT_FUNDS",
        "FRAUD_SUSPECTED",
        "INTERNAL_ERROR",
    )

    def _pick_status_code(self, is_err: bool) -> int:
        # 기존 분포 유지: 실패(400/402/408/500), 성공(200/201/204)
        """pick_status_code 처리를 수행한다."""
        if is_err:
            return self._rng.choice([400, 402, 408, 500])
        return self._rng.choice([200, 201, 204])

    def _pick_reason_code(self) -> str:
        """pick_reason_code 처리를 수행한다."""
        return self._rng.choice(list(self.PAYMENT_REASON_CODES))

    def _infer_ids_for_route(self, route_path: str, method: str) -> Dict[str, Optional[str]]:
        """
        route_template에 따라 payment_id 필요 여부를 판단해 채운다.
        """
        payment_id: Optional[str] = None

        if "{payment_id}" in route_path:
            payment_id = self.generate_payment_id()
        elif method == "POST" and route_path == "/v2/payments":
            payment_id = self.generate_payment_id()

        return {"payment_id": payment_id}

    def generate_events_one(self) -> List[Dict[str, Any]]:
        """generate_events_one 처리를 수행한다."""
        route = self.pick_route()
        method = self.pick_method(route)

        now_ms = self.now_utc_ms()
        request_id = self.generate_request_id()

        is_err = self._is_err()
        status_code = self._pick_status_code(is_err)
        duration_ms = self.sample_duration_ms()

        user_id = self.generate_user_id()
        amount = int(self._rng.randint(1000, 500000))

        ids = self._infer_ids_for_route(route["path"], method)
        payment_id = ids.get("payment_id")

        # 결제는 order_id와 연계되면 좋은데(전환/리드타임 KPI),
        # 아직 공유 state를 안 넣는 단계라 일단 랜덤 생성(필요 시 추후 state로 연결)
        order_id = self.generate_order_id()

        events: List[Dict[str, Any]] = []
        if self._emit_http_event():
            # 1) HTTP 이벤트(조건부)
            http_ev = self.make_http_event(
                ts_ms=now_ms,
                request_id=request_id,
                method=method,
                route_template=route["path"],
                status_code=status_code,
                duration_ms=duration_ms,
                user_id=user_id,
                order_id=order_id,
                payment_id=payment_id,
                api_group=route.get("api_group"),
                extra={
                    "timestamp_ms": now_ms,  # 레거시 호환
                    "amount": amount,
                },
            )
            events.append(http_ev)

        # 2) 도메인 이벤트(조건부)
        if self._emit_domain_event() and self._should_emit_domain_event(method, route, is_err):
            dom_name = self._domain_event_name(route, is_err)
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
                    api_group=route.get("api_group"),
                    route_template=route["path"],
                    extra={
                        "timestamp_ms": now_ms,  # 레거시 호환
                    },
                )
                events.append(dom_ev)

        return events
