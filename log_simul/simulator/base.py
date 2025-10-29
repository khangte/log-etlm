from abc import ABC, abstractmethod
from datetime import datetime, timezone

class SimulatorBase(ABC):
    @abstractmethod
    def generate(self, now: datetime | None = None) -> dict:
        ...

    @staticmethod
    def now_iso(now: datetime | None = None) -> str:
        ts = now or datetime.now(timezone.utc)
        return ts.isoformat().replace("+00:00", "Z")
    