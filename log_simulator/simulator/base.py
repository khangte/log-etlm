# -----------------------------------------------------------------------------
# 파일명 : log_simulator/simulator/base.py
# 목적   : 서비스별 시뮬레이터가 공통으로 사용하는 베이스 클래스/유틸 정의(최적화 버전)
# 설명   : 라우트/메서드 선택, 에러율 처리, request_id/event_id 생성, UTC ms 생성,
#          공통 이벤트 생성(HTTP/도메인), 렌더링 등을 제공
# -----------------------------------------------------------------------------

from __future__ import annotations

from typing import Any, Dict, List, Optional
import bisect
import json
import random
import time


class BaseServiceSimulator:
    """
    성능 최적화 포인트
    - routes 전처리(누적 weight)로 pick_route 비용 절감
    - Faker 제거: 빠른 난수 기반 ID 생성
    - time.time_ns 기반 now_utc_ms
    - 요청 1건 -> 이벤트 리스트(1~2개) 생성 패턴 지원
    """

    service: str = "base"
    domain: str = "base"

    __slots__ = (
        "routes",
        "profile",
        "error_rate",
        "domain_event_policy",
        "event_mode",
        "domain_event_rate",
        "http_event_rate",
        "_rng",
        "_route_prefix_sums",
        "_route_total_weight",
    )

    def __init__(self, routes: List[Dict[str, Any]], profile: Dict[str, Any]):
        if not isinstance(routes, list):
            raise ValueError("routes must be a list")
        if not isinstance(profile, dict):
            raise ValueError("profile must be a dict")

        self.routes = routes
        self.profile = profile
        mode = str(profile.get("event_mode", "all")).lower()
        if mode not in ("all", "domain", "http"):
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
        self.http_event_rate = self._estimate_http_event_rate()


    # ---------- 공통 유틸 ----------

    @staticmethod
    def now_utc_ms() -> int:
        """현재 UTC epoch ms (빠른 구현)"""
        return time.time_ns() // 1_000_000

    def _rand_hex(self, length: int) -> str:
        """고정 길이 hex 문자열 생성."""
        length = max(int(length), 1)
        bits = length * 4
        return f"{self._rng.getrandbits(bits):0{length}x}"

    def generate_request_id(self) -> str:
        """generate_request_id 처리를 수행한다."""
        return "req_" + self._rand_hex(12)

    def generate_event_id(self) -> str:
        """generate_event_id 처리를 수행한다."""
        return "evt_" + self._rand_hex(32)

    def generate_user_id(self) -> str:
        """간단 user id(8 hex)"""
        return self._rand_hex(8)

    def generate_order_id(self) -> str:
        """generate_order_id 처리를 수행한다."""
        return "o_" + self._rand_hex(12)

    def generate_payment_id(self) -> str:
        """generate_payment_id 처리를 수행한다."""
        return "p_" + self._rand_hex(12)

    # ---------- route/method 선택 ----------

    def pick_route(self, routes: Optional[List[Dict[str, Any]]] = None) -> Dict[str, Any]:
        """
        최적화: 기본은 self.routes + prefix sums로 선택.
        레거시 호환: routes가 self.routes가 아니면 느린 방식으로 처리.
        """
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
        """pick_method 처리를 수행한다."""
        methods = route.get("methods") or ["GET"]
        if len(methods) == 1:
            return methods[0]
        return methods[self._rng.randrange(0, len(methods))]

    def sample_duration_ms(self) -> int:
        """간단 지연 샘플(override는 나중에)"""
        return self._rng.randint(5, 300)

    def _is_err(self) -> bool:
        """is_err 처리를 수행한다."""
        return self._rng.random() < self.error_rate

    def _should_emit_domain_event(self, method: str, route: Dict[str, Any], is_err: bool) -> bool:
        """should_emit_domain_event 처리를 수행한다."""
        if not route.get("domain_events"):
            return False
        if method == "GET" and not self.domain_event_policy["emit_for_get_routes"]:
            return False
        if is_err and not self.domain_event_policy["emit_on_fail"]:
            return False
        return True

    def _domain_event_name(self, route: Dict[str, Any], is_err: bool) -> Optional[str]:
        """domain_event_name 처리를 수행한다."""
        de = route.get("domain_events")
        if not isinstance(de, dict):
            return None
        return de.get("fail" if is_err else "success")

    def _fallback_domain_event_name(self, route: Dict[str, Any]) -> str:
        """fallback 도메인 이벤트명을 생성한다."""
        api_group = route.get("api_group")
        if api_group:
            return f"{str(api_group).lower()}_event"
        return f"{self.service}_event"

    def _estimate_http_event_rate(self) -> float:
        """estimate_http_event_rate 처리를 수행한다."""
        return 1.0 if self.event_mode in ("all", "http") else 0.0

    def _estimate_domain_event_rate(self) -> float:
        """estimate_domain_event_rate 처리를 수행한다."""
        total_weight = float(self._route_total_weight or 0)
        if total_weight <= 0:
            return 0.0

        emit_get = self.domain_event_policy["emit_for_get_routes"]
        emit_on_fail = self.domain_event_policy["emit_on_fail"]
        err_rate = max(0.0, min(1.0, float(self.error_rate)))
        rate = 0.0

        for route in self.routes:
            de = route.get("domain_events")
            if not isinstance(de, dict):
                continue
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

    def _emit_http_event(self) -> bool:
        """emit_http_event 처리를 수행한다."""
        return self.event_mode in ("all", "http")

    def _emit_domain_event(self) -> bool:
        """emit_domain_event 처리를 수행한다."""
        return self.event_mode in ("all", "domain")


    # ---------- 공통 이벤트 생성 ----------

    def make_http_event(
        self,
        *,
        ts_ms: int,
        request_id: str,
        method: str,
        route_template: str,
        status_code: int,
        duration_ms: int,
        user_id: Optional[str] = None,
        order_id: Optional[str] = None,
        payment_id: Optional[str] = None,
        api_group: Optional[str] = None,
        extra: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """make_http_event 처리를 수행한다."""
        ev: Dict[str, Any] = {
            "event_id": self.generate_event_id(),
            "event_name": "http_request_completed",
            "domain": "http",
            "ts_ms": ts_ms,
            "service": self.service,
            "request_id": request_id,
            "method": method,
            "route_template": route_template,
            "status_code": int(status_code),
            "duration_ms": int(duration_ms),
        }
        if api_group:
            ev["api_group"] = api_group
        if user_id:
            ev["user_id"] = user_id
        if order_id:
            ev["order_id"] = order_id
        if payment_id:
            ev["payment_id"] = payment_id
        if extra:
            ev.update(extra)
        return ev

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
        api_group: Optional[str] = None,
        route_template: Optional[str] = None,
        extra: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """make_domain_event 처리를 수행한다."""
        ev: Dict[str, Any] = {
            "event_id": self.generate_event_id(),
            "event_name": event_name,
            "domain": self.domain,
            "ts_ms": ts_ms,
            "service": self.service,
            "request_id": request_id,
            "result": result,
        }
        if api_group:
            ev["api_group"] = api_group
        if route_template:
            ev["route_template"] = route_template
        if user_id:
            ev["user_id"] = user_id
        if order_id:
            ev["order_id"] = order_id
        if payment_id:
            ev["payment_id"] = payment_id
        if reason_code:
            ev["reason_code"] = reason_code
        if amount is not None:
            ev["amount"] = float(amount)
        if extra:
            ev.update(extra)
        return ev

    # ---------- 생성 템플릿 ----------

    def generate_events_one(self) -> List[Dict[str, Any]]:
        """요청 1건 -> 이벤트 리스트(서브클래스 구현)"""
        raise NotImplementedError

    def generate_events(self, count: int) -> List[Dict[str, Any]]:
        """count번 요청 -> 이벤트 평탄화"""
        out: List[Dict[str, Any]] = []
        for _ in range(count):
            out.extend(self.generate_events_one())
        return out

    # ---------- 출력 ----------

    def render_bytes(self, log: Dict[str, Any]) -> bytes:
        """render_bytes 처리를 수행한다."""
        return json.dumps(log, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
