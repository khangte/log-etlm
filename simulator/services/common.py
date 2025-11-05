# simulator/services/common.py
from __future__ import annotations
import random, uuid
from typing import Any, Dict, List
from datetime import datetime, timezone


class BaseServiceSimulator:
    """
    서비스별 로그 시뮬레이터의 공통 베이스 클래스.

    역할
    - 라우트/HTTP 메서드 선택 로직 제공
    - 공통 유틸(UTC ISO 시각, request_id 생성) 제공
    - 배치 생성 템플릿(generate_batch) 제공
    - 에러율(error_rate) 기본 처리: profile.error_rate가 dict면 서비스명 키를 우선 사용,
      아니면 숫자 공통값을 사용

    사용 패턴
    - 서브클래스에서 service_name을 설정하고, generate_one()만 구현하면 됨.
      예) class AuthSimulator(BaseServiceSimulator): service_name = "auth"
    """

    service_name: str = "base"

    def __init__(self, routes: List[Dict[str, Any]], profile: Dict[str, Any]):
        """
        Args:
            routes: templates/routes.yml에서 서비스별로 로드한 라우트 리스트
                예) [{"path": "/v1/login", "methods": ["POST"], "weight": 4}, ...]
            profile: profiles/*.yaml에서 로드한 시뮬레이션 프로파일(dict)

        Raises:
            ValueError: routes가 리스트가 아닐 때
        """
        if not isinstance(routes, list):
            raise ValueError("routes must be a list")
        self.routes = routes
        self.profile = profile

        # error_rate 설정: dict면 서비스명 키, 숫자면 공통값
        er = profile.get("error_rate", 0.01)
        if isinstance(er, dict):
            self.error_rate = float(er.get(self.service_name, 0.01))
        else:
            self.error_rate = float(er)

    # ---------- 공통 유틸 ----------

    @staticmethod
    def pick_route(routes: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        routes에서 weight 기반으로 1개 라우트를 선택한다.

        Args:
            routes: 라우트 사전 목록. 각 항목은 {"path", "methods", "weight"} 필드를 가짐

        Returns:
            Dict[str, Any]: 선택된 라우트 한 건

        Raises:
            ValueError: routes가 비어있을 때
        """
        if not routes:
            raise ValueError("routes is empty")
        weights = [int(r.get("weight", 1)) for r in routes]
        return random.choices(routes, weights=weights, k=1)[0]

    @staticmethod
    def pick_method(route: Dict[str, Any]) -> str:
        """
        라우트에 정의된 methods 중 1개를 선택한다. 없으면 기본값은 "GET".

        Args:
            route: {"methods": ["GET","POST",...]} 형태의 라우트 사전

        Returns:
            str: 선택된 HTTP 메서드 (예: "GET", "POST")
        """
        methods = route.get("methods") or ["GET"]
        return random.choice(methods)

    @staticmethod
    def now_utc_iso() -> str:
        """
        현재 UTC 시각을 ISO8601 문자열로 반환한다. (밀리초 없음, 접미사 Z)

        Returns:
            str: 예) "2025-11-05T07:55:10Z"
        """
        return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    @staticmethod
    def new_request_id() -> str:
        """
        요청 추적용 짧은 request_id를 생성한다.

        Returns:
            str: 예) "req_a1b2c3d4"
        """
        return f"req_{uuid.uuid4().hex[:8]}"

    # ---------- 생성 템플릿 ----------

    def generate_one(self) -> Dict[str, Any]:
        """
        단일 로그 이벤트를 생성한다.
        서브클래스에서 서비스 특화 로직으로 구현해야 한다.

        Returns:
            Dict[str, Any]: 최소 스키마(9필드)를 만족하는 이벤트 딕셔너리
                {"ts","svc","lvl","rid","met","path","st","lat","evt"}
        """
        raise NotImplementedError

    def generate_batch(self, count: int) -> List[Dict[str, Any]]:
        """
        지정된 개수만큼 단일 이벤트를 생성해 리스트로 반환한다.

        Args:
            count: 생성할 이벤트 개수

        Returns:
            List[Dict[str, Any]]: 이벤트 목록
        """
        return [self.generate_one() for _ in range(count)]
