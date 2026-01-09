from __future__ import annotations

import time
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
        """에러 사유 코드를 선택한다."""
        return self._rng.choice(self.AUTH_REASON_CODES)

    def generate_events_one(self) -> List[Dict[str, Any]]:
        """요청 1건의 인증 이벤트 목록을 생성한다."""
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
        if profile:
            t_ids = time.perf_counter()

        is_err = self._is_err()
        if profile:
            t_rand = time.perf_counter()

        events: List[Dict[str, Any]] = []
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

        return events  # late 이벤트 없음
