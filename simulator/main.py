# -----------------------------------------------------------------------------
# 파일명 : simulator/main.py
# 목적   : CLI → 프로파일/라우트 로드 → 오버라이드 적용 → 러너 실행(또는 dry-run)
# 사용   :
#   python -m simulator.main --profile simulator/profiles/baseline.yaml
#   python -m simulator.main --dry-run --rps 200 --duration 30
# -----------------------------------------------------------------------------

from __future__ import annotations
from pathlib import Path
from typing import Any, Dict
import yaml

from simulator.cli.args import build_parser, parse_args, normalize_args
from simulator.core.config import load_profile, load_routes, ROUTES_FILE
from simulator.core.runner import run


def _apply_overrides(profile: Dict[str, Any], overrides: Dict[str, Any]) -> Dict[str, Any]:
    """
    CLI에서 받은 오버라이드를 프로파일 dict에 병합한다.
    - 단순 값은 덮어쓰기
    - mix/error_rate는 dict 또는 float로 들어올 수 있음
    """
    merged = dict(profile)
    for k, v in overrides.items():
        # 단순 덮어쓰기(필요 시 병합 로직 추가 가능)
        merged[k] = v
    return merged


def _load_routes_from(path: Path) -> Dict[str, Any]:
    """
    routes.yml을 path에서 로드한다.
    - 기본 경로(ROUTES_FILE)와 다를 때 사용.
    """
    with path.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    return data.get("routes", {})


def main(argv: list[str] | None = None) -> None:
    """
    엔트리 포인트.
    1) CLI 인자 파싱
    2) 프로파일/라우트 로드
    3) 오버라이드 적용
    4) 러너 실행
    """
    ns = parse_args(argv)
    profile_path, routes_path, overrides = normalize_args(ns)

    # 1) 프로파일 로드
    profile = load_profile(profile_path)

    # 2) 라우트 로드 (경로가 기본과 다르면 직접 로드)
    if routes_path == ROUTES_FILE:
        routes_map = load_routes()
    else:
        routes_map = _load_routes_from(routes_path)

    # 3) 오버라이드 적용
    profile = _apply_overrides(profile, overrides)

    # 4) 러너 실행
    run(
        profile=profile,
        routes_map=routes_map,
        replicate_error_to_topic=bool(overrides.get("replicate_error_to_topic", True)),
        dry_run=bool(overrides.get("dry_run", False)),
        log_interval_sec=int(overrides.get("log_interval_sec", 5)),
    )


if __name__ == "__main__":
    main()
