"""
services 패키지 초기화 및 클래스 레지스트리.

- 각 도메인 시뮬레이터(Auth/Order/Payment/Notify)를 가져와 REGISTRY로 노출한다.
- generator.py는 REGISTRY에서 클래스를 찾아 인스턴스를 생성하고,
  generate_batch()를 호출해 배치를 만든다.
"""

from .auth import AuthSimulator
from .order import OrderSimulator
from .payment import PaymentSimulator
from .notify import NotifySimulator

# 서비스명 → 시뮬레이터 클래스 매핑
REGISTRY = {
    "auth": AuthSimulator,
    "order": OrderSimulator,
    "payment": PaymentSimulator,
    "notify": NotifySimulator,
}

__all__ = ["AuthSimulator", "OrderSimulator", "PaymentSimulator", "NotifySimulator", "REGISTRY"]
