from datetime import datetime, timezone
from faker import Faker
import ulid
from .base import SimulatorBase
from .thread_name import next_thread

fake = Faker()

class AuthSimulator(SimulatorBase):
    """
    인증 관련 단순 로그.
    """
    def generate(self, now: datetime | None = None) -> dict:
        level = fake.random_element(["INFO"] * 8 + ["ERROR"] * 2)
        return {
            "event_id": ulid.new().str,
            "timestamp": self.now_iso(now),
            "service": "auth",
            "level": level,
            "pid": fake.random_int(10000, 99999),
            "thread": next_thread("auth"),
            "logger": "c.ecommerce.auth.AuthService",
            "user_id": fake.uuid4().hex,    
            "message": "authentication successful" if level == "INFO" else "authentication failed"
        }
