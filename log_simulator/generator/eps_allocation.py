# -----------------------------------------------------------------------------
# 파일명 : log_simulator/generator/eps_allocation.py
# 목적   : EPS 분배 정책만 담당
# -----------------------------------------------------------------------------

from __future__ import annotations

from typing import Dict, List, Tuple


def compute_service_eps(
    total_eps: float,
    mix: Dict[str, float],
    services: List[str],
    simulator_share: float,
) -> Tuple[float, Dict[str, float]]:
    """전체 EPS를 share로 조정한 뒤 mix 비중으로 서비스별 목표 EPS 계산."""
    if not services:
        return 0.0, {}

    simulator_share = max(simulator_share, 0.0)
    base_eps = total_eps * simulator_share

    weights = {service: float(mix.get(service, 1.0)) for service in services}
    weight_sum = sum(weights.values())
    if weight_sum <= 0:
        # mix가 비정상이면 서비스 수 기준 균등 분배로 fallback.
        weight_sum = float(len(services))
        weights = {service: 1.0 for service in services}

    service_eps = {
        service: base_eps * (weights[service] / weight_sum) for service in services
    }
    return base_eps, service_eps
