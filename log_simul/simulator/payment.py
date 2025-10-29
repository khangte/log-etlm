from datetime import datetime, timezone
from faker import Faker
import ulid
from .base import SimulatorBase
from .thread_name import next_thread

fake = Faker()

class PaymentSimulator(SimulatorBase):
    """
    결제 관련 단순 로그.
    """
    def generate(self, now: datetime | None = None) -> dict:
        level = fake.random_element(["INFO"] * 8 + ["ERROR"] * 2)
        return {
            "event_id": ulid.new().str,
            "timestamp": self.now_iso(now),
            "service": "payment",
            "level": level,
            "pid": fake.random_int(10000, 99999),
            "thread": next_thread("payment"),
            "logger": "c.ecommerce.payment.PaymentService",
            "user_id": fake.uuid4().hex,
            "order_id": fake.uuid4().hex,
            "payment_id": fake.uuid4().hex,
            "quantity": fake.random_int(1000, 200000),
            "message": "payment approved" if level == "INFO" else "payment failed"
        }
