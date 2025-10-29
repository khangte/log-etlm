from datetime import datetime, timezone
from faker import Faker
import ulid
from .base import SimulatorBase
from .thread_name import next_thread

fake = Faker()

class OrderSimulator(SimulatorBase):
    """
    주문 관련 단순 로그.
    """
    def generate(self, now: datetime | None = None) -> dict:
        level = fake.random_element(["INFO"] * 9 + ["ERROR"] * 1)
        return {
            "event_id": ulid.new().str,
            "timestamp": self.now_iso(now),
            "service": "order",
            "level": level,
            "pid": fake.random_int(10000, 99999),
            "thread": next_thread("order"),
            "logger": "c.ecommerce.order.OrderService",
            "user_id": fake.uuid4().hex,
            "order_id": fake.uuid4().hex,
            "message": "order created" if level == "INFO" else "order creation failed"
        }
