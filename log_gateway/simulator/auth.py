# -----------------------------------------------------------------------------
# 파일명 : log_gateway/simulator/auth.py
# 목적   : 인증 도메인 요청 패턴을 흉내 내는 시뮬레이터 구현
# 설명   : routes.yml 기반으로 경로/메서드를 선택하고 profile.error_rate 에 맞춰 성공/실패 로그 생성
# -----------------------------------------------------------------------------

from __future__ import annotations
import random
from typing import Any, Dict, List
from .base import BaseServiceSimulator

class AuthSimulator(BaseServiceSimulator):
    """
    인증(auth) 도메인 로그 시뮬레이터.

    - 최소 9키 스키마에 맞춰 단일 이벤트를 생성한다.
    - 에러율은 profile.error_rate의 서비스별 값(또는 공통값)을 따른다.
    - 경로/메서드는 templates/routes.yml의 가중치(weight) 기반으로 선택한다.
    """

    service = "auth"

    def __init__(self, routes: List[Dict[str, Any]], profile: Dict[str, Any]):
        """
        Args:
            routes: auth용 라우트 목록 (path/methods/weight)
            profile: 시뮬레이터 프로파일(dict)
        """
        super().__init__(routes, profile)

    def generate_log_one(self) -> Dict[str, Any]:
        """
        auth 로그 1건 생성.
        """
        is_err = (random.random() < self.error_rate)
        route = self.pick_route(self.routes)     # 가중치 기반 경로 선택
        method = self.pick_method(route)         # 해당 경로의 HTTP 메서드 선택

        log = {
            "timestamp": self.now_utc_iso(),
            "timestamp_ms": self.now_utc_ms(),
            "service": self.service,
            "level": "ERROR" if is_err else "INFO",
            "request_id": self.generate_request_id(),
            "method": method,
            "path": route["path"],
            "status_code":  random.choice([401, 403, 429, 500]) if is_err else random.choice([200, 200, 204]),
            "latency": round(random.uniform(60, 250) if is_err else random.uniform(20, 120), 2),
            "event": "LoginFailed" if is_err else "LoginSucceeded",

            "user_id": self.generate_user_id(),
        }

        return log
