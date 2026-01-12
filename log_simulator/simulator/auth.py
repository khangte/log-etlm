from __future__ import annotations
from typing import Any, Dict, List
from .base import BaseServiceSimulator

class AuthSimulator(BaseServiceSimulator):
    service = "auth"
    domain = "auth"

    AUTH_REASON_CODES = [
        "INVALID_CREDENTIALS",
        "TOKEN_EXPIRED",
        "RATE_LIMITED",
        "INTERNAL_ERROR",
    ]

    def _pick_reason_code(self) -> str:
        """인증 실패 원인 코드를 무작위로 선택한다."""
        return self._rng.choice(self.AUTH_REASON_CODES)

    def generate_events_one(self) -> List[Dict[str, Any]]:
        """요청 1건에 대한 도메인 이벤트 리스트를 생성한다."""
        route = self.pick_route()
        method = self.pick_method(route)

        now_ms = self.now_utc_ms()
        request_id = self.generate_request_id()
        user_id = self.generate_user_id()

        is_err = self._is_err()
        events: List[Dict[str, Any]] = []
        if self._should_emit_domain_event(method, route, is_err):
            dom_name = self._domain_event_name(route, is_err)
            if dom_name:
                dom_ev = self.make_domain_event(
                    ts_ms=now_ms,
                    request_id=request_id,
                    event_name=dom_name,
                    result="fail" if is_err else "success",
                    reason_code=self._pick_reason_code() if is_err else None,
                    user_id=user_id,
                    api_group=route.get("api_group"),
                    route_template=route["path"],
                )
                events.append(dom_ev)

        return events  # late 이벤트 없음
