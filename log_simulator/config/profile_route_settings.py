# -----------------------------------------------------------------------------
# 파일명 : log_simulator/config/profile_route_settings.py
# 목적   : log_simulator 앱의 정적 리소스 경로(config) 및 YAML 로더 제공
# 사용   : generator/API가 load_profile(), load_routes()로 시뮬레이션 설정을 읽어옴
# -----------------------------------------------------------------------------

from __future__ import annotations
from dataclasses import dataclass
from typing import Dict, Any, List
from pathlib import Path
import os
import yaml

from .timeband import load_bands, Band

THIS_DIR = Path(__file__).resolve().parent
CONFIG_DIR = THIS_DIR
ROUTES_FILE = CONFIG_DIR / "routes.yml"
PROFILES_FILE = CONFIG_DIR / "profiles.yml"


def load_routes() -> Dict[str, Any]:
    with ROUTES_FILE.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    return data.get("routes", {})


def load_profile() -> Dict[str, Any]:
    with PROFILES_FILE.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    return data


@dataclass(frozen=True)
class ProfileContext:
    profile: Dict[str, Any]

    total_eps: float
    mix: Dict[str, Any]
    weight_mode: str
    bands: List[Band]

    error_rate: Dict[str, float]
    funnel: Dict[str, float]
    entity_pool: Dict[str, Any]
    domain_event_policy: Dict[str, Any]


def load_profile_context() -> ProfileContext:
    """
    프로파일 파일명을 기준으로 실행에 필요한 기본 컨텍스트를 로드한다.
    """
    profile = load_profile()

    total_eps = float(profile.get("eps", 10000))
    mix = profile.get("mix", {})
    weight_mode = str(profile.get("weight_mode", "uniform"))
    bands = load_bands(profile.get("time_weights", []))
    error_rate = profile.get("error_rate", {})
    funnel = profile.get("funnel", {})
    entity_pool = profile.get("entity_pool", {})
    domain_event_policy = profile.get("domain_event_policy", {})

    return ProfileContext(
        profile=profile,
        total_eps=total_eps,
        mix=mix,
        weight_mode=weight_mode,
        bands=bands,
        error_rate=error_rate,
        funnel=funnel,
        entity_pool=entity_pool,
        domain_event_policy=domain_event_policy,
    )
