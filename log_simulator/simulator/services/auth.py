from __future__ import annotations
from typing import Any, Dict, List
from ..base import BaseServiceSimulator

class AuthSimulator(BaseServiceSimulator):
    service = "auth"
    domain = "auth"

    AUTH_REASON_CODES = [
        "INVALID_CREDENTIALS",
        "TOKEN_EXPIRED",
        "RATE_LIMITED",
        "INTERNAL_ERROR",
    ]

    def _pick_status_code(self, is_err: bool) -> int:
        if is_err:
            return self._rng.choice([401, 403, 429, 500])
        return self._rng.choice([200, 200, 204])

    def _pick_reason_code(self) -> str:
        return self._rng.choice(self.AUTH_REASON_CODES)

    def generate_events_one(self) -> List[Dict[str, Any]]:
        route = self.pick_route()
        method = self.pick_method(route)

        now_ms = self.now_utc_ms()
        request_id = self.generate_request_id()
        user_id = self.generate_user_id()

        is_err = self._is_err()
        status_code = self._pick_status_code(is_err)
        duration_ms = self.sample_duration_ms()

        events: List[Dict[str, Any]] = []
        if self._emit_http_event():
            http_ev = self.make_http_event(
                ts_ms=now_ms,
                request_id=request_id,
                method=method,
                route_template=route["path"],
                status_code=status_code,
                duration_ms=duration_ms,
                user_id=user_id,
                api_group=route.get("api_group"),
            )
            events.append(http_ev)

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
                    api_group=route.get("api_group"),
                    route_template=route["path"],
                )
                events.append(dom_ev)

        return events  # late 이벤트 없음
