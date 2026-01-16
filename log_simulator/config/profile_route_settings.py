# -----------------------------------------------------------------------------
# 파일명 : log_simulator/config/profile_route_settings.py
# 목적   : log_simulator 앱의 정적 리소스 경로(config) 및 YAML 로더 제공
# 사용   : generator/API가 load_profile(), load_routes()로 시뮬레이션 설정을 읽어옴
# -----------------------------------------------------------------------------

from __future__ import annotations
from dataclasses import dataclass
from typing import Dict, Any, List
from pathlib import Path
import yaml

from .timeband import load_bands, Band

THIS_DIR = Path(__file__).resolve().parent
CONFIG_DIR = THIS_DIR
ROUTES_FILE = CONFIG_DIR / "routes.yml"
PROFILES_FILE = CONFIG_DIR / "profiles.yml"


def load_routes() -> Dict[str, Any]:
    """load_routes 처리를 수행한다."""
    with ROUTES_FILE.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    return data.get("routes", {})


def load_profile() -> Dict[str, Any]:
    """load_profile 처리를 수행한다."""
    with PROFILES_FILE.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    return data


@dataclass(frozen=True)
class ProfileContext:
    profile: Dict[str, Any]

    total_eps: float
    mix: Dict[str, Any]
    bands: List[Band]


def load_profile_context() -> ProfileContext:
    """
    프로파일 파일명을 기준으로 실행에 필요한 기본 컨텍스트를 로드한다.
    """
    profile = load_profile()

    total_eps = float(profile.get("eps", 10000))
    mix = profile.get("mix", {})
    bands = load_bands(profile.get("time_weights", []))

    return ProfileContext(
        profile=profile,
        total_eps=total_eps,
        mix=mix,
        bands=bands,
    )
