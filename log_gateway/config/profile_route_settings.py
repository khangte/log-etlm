# -----------------------------------------------------------------------------
# 파일명 : log_gateway/config/profile_route_settings.py
# 목적   : log_gateway 앱의 정적 리소스 경로(templates/profiles) 및 YAML 로더 제공
# 사용   : generator/API가 load_profile(), load_routes()로 시뮬레이션 설정을 읽어옴
# -----------------------------------------------------------------------------

from __future__ import annotations
from dataclasses import dataclass
from typing import Dict, Any, List
from pathlib import Path
import os
import yaml

from .timeband import load_bands, Band

# ===== 리소스 파일 경로 =====
# 본 파일 위치 기준
THIS_FILE = Path(__file__).resolve()
ROOT_DIR = THIS_FILE.parents[2] # repo 루트
APP_DIR = THIS_FILE.parents[1] # 앱 루트: log_gateway/
TEMPLATES_DIR = ROOT_DIR / APP_DIR / "templates"
PROFILES_DIR  = ROOT_DIR / APP_DIR / "profiles"

ROUTES_FILE = TEMPLATES_DIR / "routes.yml"

def load_profile(profile_path: str | Path) -> Dict[str, Any]:
    """
    YAML 프로파일을 로드한다.

    Args:
        profile_path: profiles/*.yaml 경로

    Returns:
        Dict[str, Any]: duration_sec, eps, mix, error_rate, bursts, time_weights 등 포함
    """
    path = Path(profile_path)
    with path.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    return data

def load_routes() -> Dict[str, Any]:
    """
    routes.yml을 로드하여 서비스별 라우트 리스트 맵을 반환한다.

    Returns:
        Dict[str, Any]: {"auth": [...], "order":[...], "payment":[...], "notify":[...]}
    """
    with ROUTES_FILE.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    return data.get("routes", {})


@dataclass(frozen=True)
class ProfileContext:
    profile: Dict[str, Any]
    total_eps: float
    mix: Dict[str, Any]
    weight_mode: str
    bands: List[Band]


def load_profile_context(profile_name: str) -> ProfileContext:
    """
    프로파일 파일명을 기준으로 실행에 필요한 기본 컨텍스트를 로드한다.
    """
    profile_path = PROFILES_DIR / f"{profile_name}.yaml"
    profile = load_profile(profile_path)
    total_eps = float(profile.get("eps", 10000))

    mix = profile.get("mix", {})
    weight_mode = profile.get("weight_mode", "uniform")
    raw_time_weights = profile.get("time_weights", [])
    bands = load_bands(raw_time_weights)
    
    return ProfileContext(
        profile=profile,
        total_eps=total_eps,
        mix=mix,
        weight_mode=weight_mode,
        bands=bands,
    )
