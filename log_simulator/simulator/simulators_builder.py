# log_simulator/simulator/simulators_builder.py

from __future__ import annotations
from typing import Any, Dict

from . import REGISTRY
from ..config.profile_route_settings import load_routes


def build_simulators(profile: Dict[str, Any]) -> Dict[str, Any]:
    """프로파일과 routes.yml을 기반으로 서비스별 시뮬레이터 인스턴스 생성."""
    routes_cfg = load_routes()
    simulators: Dict[str, Any] = {}

    for svc, cls in REGISTRY.items():
        svc_routes = routes_cfg.get(svc, [])
        if not svc_routes:
            continue
        simulators[svc] = cls(routes=svc_routes, profile=profile)

    if not simulators:
        raise RuntimeError("생성된 시뮬레이터가 없습니다. routes.yml 을 확인하세요.")

    return simulators
