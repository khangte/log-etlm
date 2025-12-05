# -----------------------------------------------------------------------------
# 파일명 : log_gateway/generator.py
# 목적   : 서비스별 시뮬레이터를 인스턴스화하고 FastAPI 앱 생명주기 동안 지속적으로 로그를 생성/Kafka로 전송
# 사용   : main.py startup 이벤트에서 run_generator()를 asyncio task로 띄워 백그라운드 발행
# -----------------------------------------------------------------------------

from __future__ import annotations
from typing import Any, Dict, List
import asyncio

from .core.config import PROFILES_DIR, load_profile, load_routes
from .core.timeband import load_bands
from .core.stats import stats_reporter
from .simulator import REGISTRY
from .runtime_pipeline import start_pipeline

PROFILE_NAME = "baseline"

# -----------------------------------------------------------------------------
# 새로운 비동기 제너레이터: 앱 살아있는 동안 계속 로그 생성
# -----------------------------------------------------------------------------

def _build_simulators(profile: Dict[str, Any]) -> Dict[str, Any]:
    """
    profile + routes.yml 을 기반으로 서비스별 시뮬레이터 인스턴스를 만든다.
    """
    routes_cfg = load_routes()  # {"auth":[...], "order":[...], ...}
    simulators: Dict[str, Any] = {}

    for svc, cls in REGISTRY.items():
        svc_routes = routes_cfg.get(svc, [])
        if not svc_routes:
            continue # routes.yml에 정의되지 않은 서비스는 스킵
        simulators[svc] = cls(routes=svc_routes, profile=profile)

    if not simulators:
        raise RuntimeError("생성된 시뮬레이터가 없습니다. routes.yml 을 확인하세요.")

    return simulators


def _compute_service_rps(base_rps: float, mix: Dict[str, Any], services: List[str]) -> Dict[str, float]:
    """mix 비중을 기반으로 서비스별 목표 RPS를 계산."""
    if not services:
        return {}

    weights = {svc: float(mix.get(svc, 1.0)) for svc in services}
    weight_sum = sum(weights.values())
    if weight_sum <= 0:
        weight_sum = float(len(services))
        weights = {svc: 1.0 for svc in services}

    return {svc: base_rps * (weights[svc] / weight_sum) for svc in services}


async def run_generator() -> None:
    """
    앱이 떠 있는 동안 계속 로그를 생성해서 Kafka로 발행하는 비동기 제너레이터.

    FastAPI main.py 예시:

        @app.on_event("startup")
        async def start_generator():
            asyncio.create_task(run_generator())
    """
    # logger.info("[generator] starting run_generator()")

    # 1) 프로파일/라우트 로딩
    profile_path = PROFILES_DIR / f"{PROFILE_NAME}.yaml"
    profile: Dict[str, Any] = load_profile(profile_path)

    # 기본 RPS, 서비스 믹스, 시간대 가중치 등 읽기
    base_rps: float = float(profile.get("rps", 10.0))
    mix: Dict[str, Any] = profile.get("mix", {})
    raw_time_weights = profile.get("time_weights", [])
    weight_mode: str = profile.get("weight_mode", "uniform")

    # 시간대 밴드 파싱
    bands = load_bands(raw_time_weights)

    # 2) 서비스별 시뮬레이터 인스턴스 생성
    simulators = _build_simulators(profile)
    available_services = list(simulators.keys())
    service_rps = _compute_service_rps(base_rps, mix, available_services)

    (
        publish_queue, stats_queue, service_tasks, publisher_tasks,
    ) = start_pipeline(
        simulators=simulators,
        service_rps=service_rps,
        base_rps=base_rps,
        bands=bands,
        weight_mode=weight_mode,
        profile=profile,
    )

    stats_task = asyncio.create_task(
        stats_reporter(stats_queue=stats_queue, services=available_services),
        name="stats-reporter",
    )

    tasks = service_tasks + publisher_tasks + [stats_task]
    try:
        await asyncio.gather(*tasks)
    finally:
        for task in tasks:
            task.cancel()


# -----------------------------------------------------------------------------
# 이전 batch 기반 인터페이스(필요 시 사용) - 기존 코드 유지
# -----------------------------------------------------------------------------

# def generate_batch(
#     service: str,
#     count: int,
#     routes: List[Dict[str, Any]],
#     profile: Dict[str, Any],
# ) -> List[Dict[str, Any]]:
#     """
#     서비스명으로 등록된 시뮬레이터 클래스를 찾아 인스턴스를 만들고,
#     지정된 개수만큼 이벤트를 생성한다.
#     Args:
#         service: "auth" | "order" | "payment" | "notify"
#         count: 생성할 이벤트 개수
#         routes: 해당 서비스의 라우트 리스트 (templates/routes.yml)
#         profile: 실행 프로파일(dict). error_rate 등 포함.
#     Returns:
#         List[Dict[str, Any]]: 최소 9-키 스키마의 이벤트 리스트
#     Raises:
#         ValueError: 알 수 없는 서비스명일 때
#     """
#     cls = REGISTRY.get(service)
#     if cls is None:
#         raise ValueError(f"unknown service: {service}")
#     simulator = cls(routes=routes, profile=profile)
#     return simulator.generate_logs(count)
# -----------------------------------------------------------------------------
