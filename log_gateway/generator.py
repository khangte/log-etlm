# -----------------------------------------------------------------------------
# 파일명 : log_gateway/generator.py
# 목적   : 서비스별 시뮬레이터를 인스턴스화하고 FastAPI 앱 생명주기 동안 지속적으로 로그를 생성/Kafka로 전송
# 사용   : main.py startup 이벤트에서 run_generator()를 asyncio task로 띄워 백그라운드 발행
# -----------------------------------------------------------------------------

from __future__ import annotations
from typing import Any, Dict, List
import asyncio
import random
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from .core.config import PROFILES_DIR, load_profile, load_routes
from .core.timeband import load_bands, current_hour_kst, pick_multiplier
from .simulator import REGISTRY
from . import producer

import logging
logger = logging.getLogger("log_gateway.generator")
logger.setLevel(logging.INFO)

if not logger.handlers:
    _handler = logging.StreamHandler()
    _handler.setFormatter(
        logging.Formatter("%(asctime)s [%(levelname)s] %(name)s - %(message)s")
    )
    logger.addHandler(_handler)

KST = ZoneInfo("Asia/Seoul")

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


def _pick_service(mix: Dict[str, Any], available_services: List[str]) -> str:
    """
    프로파일의 mix 비율을 기반으로 서비스 하나를 랜덤 선택.
    mix에 없는 서비스는 weight=1 로 취급.
    """
    services: List[str] = []
    weights: List[float] = []

    for svc in available_services:
        w = mix.get(svc, 1)
        services.append(svc)
        weights.append(float(w))

    return random.choices(services, weights=weights, k=1)[0]


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
    mix: Dict[str, Any] = profile.get("mix", {})  # 예: {"auth":1, "order":3, ...}
    raw_time_weights = profile.get("time_weights", [])
    weight_mode: str = profile.get("weight_mode", "uniform")

    # 시간대 밴드 파싱
    bands = load_bands(raw_time_weights)

    # 2) 서비스별 시뮬레이터 인스턴스 생성
    simulators = _build_simulators(profile)
    available_services = list(simulators.keys())

     # --- 통계용 변수 ---
    total = 0
    by_svc: Dict[str, int] = {svc: 0 for svc in available_services}
    last_report = datetime.now(KST)
    report_interval = timedelta(seconds=5)

    # 3) 무한 루프: 앱이 살아있는 동안 계속 생성
    while True:
        now = datetime.now(KST)

        # 3-1) 어떤 서비스 로그를 만들지 선택
        service = _pick_service(mix, available_services)
        simulator = simulators[service]

        # 3-2) 로그 1건 생성 (dict)
        event: Dict[str, Any] = simulator.generate_log_one()

        # 3-3) JSON 직렬화 (simulator.render() 가 있다면 그걸 써도 무방)
        payload = simulator.render(event)

        # logger.info("[debug] generating and publishing service=%s", service)

        # 3-4) Kafka 비동기 발행 (실제로는 thread pool에서 publish_sync 실행)
        await producer.publish(service, payload)

        # 통계 업데이트
        total += 1
        by_svc[service] += 1

        # 일정 주기마다 [stats] 로그 출력
        if now - last_report >= report_interval:
            elapsed = (now - last_report).total_seconds()
            avg_rps = total / elapsed if elapsed > 0 else 0.0
            logger.info(
                "[stats] total=%d avg_rps=%.1f by_svc=(%s)",
                total,
                avg_rps,
                ", ".join(f"{svc}:{cnt}" for svc, cnt in by_svc.items()),
            )
            # 리셋
            total = 0
            by_svc = {svc: 0 for svc in available_services}
            last_report = now

        # 3-5) 시간대별 multiplier 를 적용해 interval 계산
        hour = current_hour_kst(now)
        multiplier = pick_multiplier(bands, hour_kst=hour, mode=weight_mode) if bands else 1.0
        effective_rps = max(base_rps * multiplier, 0.01)  # 보호용 최소값
        interval_sec = 1.0 / effective_rps

        await asyncio.sleep(interval_sec)


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
#     return simulator.generate_logs_with_count(count)
# -----------------------------------------------------------------------------
