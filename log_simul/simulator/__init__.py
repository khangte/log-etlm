# simulator/__init__.py
from .auth import AuthSimulator
from .notify import NotifySimulator
from .order import OrderSimulator
from .payment import PaymentSimulator

SIMULATORS = {
    "auth": AuthSimulator,
    "notify": NotifySimulator,
    "order": OrderSimulator,
    "payment": PaymentSimulator,
}

def get_all_simulators(enabled: list[str] | None = None) -> dict[str, object]:
    names = enabled or list(SIMULATORS.keys())
    return {n: SIMULATORS[n]() for n in names if n in SIMULATORS}
