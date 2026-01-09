# -----------------------------------------------------------------------------
# 파일명 : log_simulator/simulator/base.py
# 목적   : 서비스별 시뮬레이터가 공통으로 사용하는 베이스 클래스/유틸 정의(최적화 버전)
# 설명   : 라우트/메서드 선택, 에러율 처리, request_id/event_id 생성, UTC ms 생성,
#          도메인 이벤트 생성, 렌더링 등을 제공
# -----------------------------------------------------------------------------

from __future__ import annotations

from typing import Any, Dict, List, Optional, Sequence, Tuple
import bisect
import json
import logging
import os
import random
import time
import gc

_PROFILE_LOGGER = logging.getLogger("log_simulator.gen_profile")


class BaseServiceSimulator:
    """
    성능 최적화 포인트
    - routes 전처리(누적 weight)로 pick_route 비용 절감
    - Faker 제거: randbits 기반 ID 생성으로 속도 향상
    - time.time_ns 기반 now_utc_ms
    - 요청 1건 -> 이벤트 리스트(1~2개) 생성 패턴 지원
    """

    service: str = "base"
    domain: str = "base"

    __slots__ = (
        "routes",               # 원본 라우트 목록
        "profile",              # 프로파일 설정 딕셔너리
        "error_rate",           # 서비스별 에러율
        "domain_event_policy",  # 도메인 이벤트 정책
        "event_mode",           # all|domain
        "domain_event_rate",    # 도메인 이벤트 추정 비율
        "_rng",                 # 인스턴스 전용 RNG
        "_route_prefix_sums",   # 라우트 가중치 누적합
        "_route_total_weight",  # 라우트 가중치 합계
        "_profile_enabled",     # 프로파일 로그 활성화
        "_profile_every",       # 프로파일 로그 주기
        "_profile_counter",     # 프로파일 로그 카운터
        "_profile_id_ms",       # id 생성 누적 시간(ms)
        "_profile_id_count",    # id 생성 측정 횟수
    )

    def __init__(self, routes: List[Dict[str, Any]], profile: Dict[str, Any]):
        """라우트/프로파일을 검증하고 초기 상태를 준비한다."""
        if not isinstance(routes, list):
            raise ValueError("routes must be a list")
        if not isinstance(profile, dict):
            raise ValueError("profile must be a dict")

        self.routes = routes
        self.profile = profile
        mode = str(profile.get("event_mode", "all")).lower()
        if mode not in ("all", "domain"):
            mode = "domain"
        self.event_mode = mode

        # error_rate: dict면 서비스 키 우선, 아니면 공통값
        er = profile.get("error_rate", 0.01)
        if isinstance(er, dict):
            self.error_rate = float(er.get(self.service, 0.01))
        else:
            self.error_rate = float(er)

        # 정책들
        dep = profile.get("domain_event_policy", {}) or {}
        if not isinstance(dep, dict):
            dep = {}
        self.domain_event_policy = {
            "emit_for_get_routes": bool(dep.get("emit_for_get_routes", False)),
            "emit_on_fail": bool(dep.get("emit_on_fail", True)),
        }

        # self.funnel = profile.get("funnel", {}) or {}
        # self.entity_pool = profile.get("entity_pool", {}) or {}

        # RNG(테스트 재현성 필요하면 seed 넣기)
        seed = profile.get("seed")
        self._rng = random.Random(seed) if seed is not None else random.Random()

        # -------- routes 전처리(핵심 최적화) --------
        # 1) methods 대문자화(매번 upper 하지 않게)
        # 2) weight 누적합(prefix sums) 만들기
        prefix: List[int] = []
        total = 0
        for r in self.routes:
            # methods 정규화
            ms = r.get("methods") or ["GET"]
            if isinstance(ms, list):
                r["methods"] = [str(m).upper() for m in ms] if ms else ["GET"]
            else:
                r["methods"] = ["GET"]

            # weight 정규화
            w = r.get("weight", 1)
            try:
                wi = int(w)
            except Exception:
                wi = 1
            if wi < 1:
                wi = 1
            r["weight"] = wi

            total += wi
            prefix.append(total)

        if total <= 0:
            raise ValueError("routes total weight must be > 0")

        self._route_prefix_sums = prefix
        self._route_total_weight = total
        self.domain_event_rate = self._estimate_domain_event_rate()
        self._profile_enabled = os.getenv("SIM_GEN_PROFILE", "0").strip().lower() in ( # 현재는 비활성화
            "1",
            "true",
            "yes",
            "y",
        )
        self._profile_every = max(int(os.getenv("SIM_GEN_PROFILE_EVERY", "500")), 1)
        self._profile_counter = 0
        self._profile_id_ms = 0.0
        self._profile_id_count = 0
        if self._profile_enabled and not _PROFILE_LOGGER.handlers:
            handler = logging.StreamHandler()
            handler.setFormatter(
                logging.Formatter("%(asctime)s [%(levelname)s] %(name)s - %(message)s")
            )
            _PROFILE_LOGGER.addHandler(handler)
            _PROFILE_LOGGER.setLevel(logging.INFO)


    # ---------- 공통 유틸 ----------

    def _rand_hex(self, width: int) -> str:
        """지정 길이의 hex 문자열을 빠르게 생성한다."""
        bits = width * 4
        return f"{self._rng.getrandbits(bits):0{width}x}"

    @staticmethod
    def now_utc_ms() -> int:
        """현재 UTC epoch ms를 반환한다."""
        return time.time_ns() // 1_000_000

    def generate_request_id(self) -> str:
        """요청 ID를 생성한다."""
        if self._profile_enabled:
            started = time.perf_counter()
            value = self._rand_hex(12)
            self._profile_id_ms += (time.perf_counter() - started) * 1000.0
            self._profile_id_count += 1
            return "req_" + value
        return "req_" + self._rand_hex(12)

    def generate_event_id(self) -> str:
        """이벤트 ID를 생성한다."""
        if self._profile_enabled:
            started = time.perf_counter()
            value = self._rand_hex(32)
            self._profile_id_ms += (time.perf_counter() - started) * 1000.0
            self._profile_id_count += 1
            return "evt_" + value
        return "evt_" + self._rand_hex(16)

    def generate_user_id(self) -> str:
        """사용자 ID를 생성한다."""
        if self._profile_enabled:
            started = time.perf_counter()
            value = self._rand_hex(8)
            self._profile_id_ms += (time.perf_counter() - started) * 1000.0
            self._profile_id_count += 1
            return value
        return self._rand_hex(8)

    def generate_order_id(self) -> str:
        """주문 ID를 생성한다."""
        if self._profile_enabled:
            started = time.perf_counter()
            value = self._rand_hex(12)
            self._profile_id_ms += (time.perf_counter() - started) * 1000.0
            self._profile_id_count += 1
            return "o_" + value
        return "o_" + self._rand_hex(12)

    def generate_payment_id(self) -> str:
        """결제 ID를 생성한다."""
        if self._profile_enabled:
            started = time.perf_counter()
            value = self._rand_hex(12)
            self._profile_id_ms += (time.perf_counter() - started) * 1000.0
            self._profile_id_count += 1
            return "p_" + value
        return "p_" + self._rand_hex(12)

    def _profile_reset_id_timings(self) -> None:
        """ID 생성 프로파일 누적치를 초기화한다."""
        self._profile_id_ms = 0.0
        self._profile_id_count = 0

    def _profile_get_id_timings(self) -> tuple[float, int]:
        """ID 생성 프로파일 누적치를 반환한다."""
        return self._profile_id_ms, self._profile_id_count

    def _maybe_log_gen_profile(
        self,
        *,
        service: str,
        event_count: int,
        pick_ms: float,
        id_ms: float,
        rand_ms: float,
        event_ms: float,
        total_ms: float,
        idgen_ms: float,
        idgen_count: int,
    ) -> None:
        """프로파일 조건을 만족하면 생성 성능 로그를 남긴다."""
        if not self._profile_enabled:
            return
        self._profile_counter += 1
        if self._profile_counter % self._profile_every != 0:
            return
        gc_counts = gc.get_count()
        gc_gen2 = gc.get_stats()[2]["collections"] if gc.isenabled() else 0
        _PROFILE_LOGGER.info(
            "[gen-profile] service=%s events=%d pick_ms=%.3f id_ms=%.3f "
            "rand_ms=%.3f event_ms=%.3f total_ms=%.3f idgen_ms=%.3f idgen_n=%d "
            "gc_count=%s gc_gen2=%d",
            service,
            event_count,
            pick_ms,
            id_ms,
            rand_ms,
            event_ms,
            total_ms,
            idgen_ms,
            idgen_count,
            gc_counts,
            gc_gen2,
        )

    # ---------- route/method 선택 ----------

    def pick_route(self, routes: Optional[List[Dict[str, Any]]] = None) -> Dict[str, Any]:
        """가중치 기반으로 라우트를 선택한다."""
        if routes is None or routes is self.routes:
            # 1..total
            x = self._rng.randrange(1, self._route_total_weight + 1)
            idx = bisect.bisect_left(self._route_prefix_sums, x)
            return self.routes[idx]

        # (드문 케이스) 외부 routes 리스트가 들어오면 간단 처리
        if not routes:
            raise ValueError("routes is empty")
        weights = [int(r.get("weight", 1)) for r in routes]
        return self._rng.choices(routes, weights=weights, k=1)[0]

    def pick_method(self, route: Dict[str, Any]) -> str:
        """라우트의 HTTP 메서드를 선택한다."""
        methods = route.get("methods") or ["GET"]
        if len(methods) == 1:
            return methods[0]
        return methods[self._rng.randrange(0, len(methods))]

    def _is_err(self) -> bool:
        """에러 여부를 확률로 결정한다."""
        return self._rng.random() < self.error_rate

    def _should_emit_domain_event(self, method: str, route: Dict[str, Any], is_err: bool) -> bool:
        """도메인 이벤트 발행 여부를 판단한다."""
        if method == "GET" and not self.domain_event_policy["emit_for_get_routes"]:
            return False
        if is_err and not self.domain_event_policy["emit_on_fail"]:
            return False
        return True

    def _fallback_event_name(self, route: Dict[str, Any], method: str) -> str:
        path = str(route.get("path", "")).strip().lower()
        if path.startswith("/"):
            path = path[1:]
        for ch in ("/", "-", "{", "}"):
            path = path.replace(ch, "_")
        path = path.strip("_") or "unknown"
        return f"{self.service}_{method.lower()}_{path}"

    def _domain_event_name(self, route: Dict[str, Any], method: str, is_err: bool) -> Optional[str]:
        """도메인 이벤트명을 결정한다."""
        de = route.get("domain_events")
        if isinstance(de, dict):
            name = de.get("fail" if is_err else "success")
            if name:
                return name
        return self._fallback_event_name(route, method)

    def _estimate_domain_event_rate(self) -> float:
        """도메인 이벤트 비율 추정값을 계산한다."""
        total_weight = float(self._route_total_weight or 0)
        if total_weight <= 0:
            return 0.0

        emit_get = self.domain_event_policy["emit_for_get_routes"]
        emit_on_fail = self.domain_event_policy["emit_on_fail"]
        err_rate = max(0.0, min(1.0, float(self.error_rate)))
        rate = 0.0

        for route in self.routes:
            de = route.get("domain_events")
            methods = route.get("methods") or ["GET"]
            if not isinstance(methods, list) or not methods:
                methods = ["GET"]
            per_method = 1.0 / float(len(methods))

            for method in methods:
                if method == "GET" and not emit_get:
                    continue
                p = 1.0 if emit_on_fail else (1.0 - err_rate)
                rate += (route.get("weight", 1) / total_weight) * per_method * p

        return rate

    def _emit_domain_event(self) -> bool:
        """도메인 이벤트 발행 여부를 반환한다."""
        return self.event_mode in ("all", "domain")


    # ---------- 공통 이벤트 생성 ----------

    def make_domain_event(
        self,
        *,
        ts_ms: int,
        request_id: str,
        event_name: str,
        result: str,
        user_id: Optional[str] = None,
        order_id: Optional[str] = None,
        payment_id: Optional[str] = None,
        reason_code: Optional[str] = None,
        amount: Optional[float] = None,
        extra: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """도메인 이벤트 페이로드를 생성한다."""
        ev: Dict[str, Any] = {
            "eid": self.generate_event_id(),
            "evt": event_name,
            "ts": ts_ms,
            "svc": self.service,
            "rid": request_id,
            "res": result,
        }
        if user_id:
            ev["uid"] = user_id
        if order_id:
            ev["oid"] = order_id
        if payment_id:
            ev["pid"] = payment_id
        if reason_code:
            ev["rc"] = reason_code
        if amount is not None:
            ev["amt"] = float(amount)
        if extra:
            ev.update(extra)
        return ev

    # ---------- 생성 템플릿 ----------

    def generate_events_one(self) -> List[Dict[str, Any]]:
        """요청 1건의 이벤트 목록을 생성한다."""
        raise NotImplementedError

    def generate_log_one(self) -> Dict[str, Any]:
        """첫 번째 이벤트만 반환한다."""
        events = self.generate_events_one()
        if not events:
            raise RuntimeError("generate_events_one() returned empty list")
        return events[0]

    def generate_events(self, count: int) -> List[Dict[str, Any]]:
        """여러 요청의 이벤트를 생성해 평탄화한다."""
        out: List[Dict[str, Any]] = []
        for _ in range(count):
            out.extend(self.generate_events_one())
        return out

    # ---------- 출력 ----------

    def render(self, log: Dict[str, Any]) -> str:
        """이벤트를 JSON 문자열로 변환한다."""
        return json.dumps(log, ensure_ascii=False)

    def render_bytes(self, log: Dict[str, Any]) -> bytes:
        """이벤트를 JSON 바이트로 변환한다."""
        return json.dumps(log, ensure_ascii=False).encode("utf-8")


    # # ---------- late 이벤트(옵션) ----------

    # def maybe_delay_some(self, events: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    #     if not self.late_events.get("enabled"):
    #         return list(events)

    #     rate = float(self.late_events.get("rate", 0.0))
    #     max_delay = int(self.late_events.get("max_delay_seconds", 0))
    #     if rate <= 0.0 or max_delay <= 0:
    #         return list(events)

    #     out: List[Dict[str, Any]] = []
    #     rng = self._rng
    #     for ev in events:
    #         if rng.random() < rate:
    #             delay_ms = rng.randint(1, max_delay) * 1000
    #             ev2 = ev.copy()
    #             ev2["ts_ms"] = int(ev2["ts_ms"]) - delay_ms
    #             out.append(ev2)
    #         else:
    #             out.append(ev)
    #     return out
