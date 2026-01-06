# -----------------------------------------------------------------------------
# 패키지 : log_simulator/simulator
# 목적   : 도메인별 시뮬레이터(Auth/Order/Payment/Notify)와 REGISTRY 매핑 제공
# 설명   : generator/api 가 REGISTRY를 참조해 각 서비스를 즉시 인스턴스화할 수 있도록 함
# -----------------------------------------------------------------------------

from .auth import AuthSimulator
from .order import OrderSimulator
from .payment import PaymentSimulator

# 서비스명 → 시뮬레이터 클래스 매핑
REGISTRY = {
    "auth": AuthSimulator,
    "order": OrderSimulator,
    "payment": PaymentSimulator,
}

__all__ = ["AuthSimulator", "OrderSimulator", "PaymentSimulator", "REGISTRY"]
