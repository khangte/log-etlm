from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List

from .timeband import Band, load_bands


@dataclass(frozen=True)
class ProfileContext:
    profile: Dict[str, Any]
    total_eps: float
    mix: Dict[str, Any]
    bands: List[Band]

    @staticmethod
    def from_profile(profile: Dict[str, Any]) -> "ProfileContext":
        total_eps = float(profile.get("eps", 10000))
        mix = profile.get("mix", {})
        bands = load_bands(profile.get("time_weights", []))
        return ProfileContext(
            profile=profile,
            total_eps=total_eps,
            mix=mix,
            bands=bands,
        )
