# -----------------------------------------------------------------------------
# 패키지 : log_simulator/producer
# 목적   : Kafka Producer 래퍼 및 토픽 매핑
# -----------------------------------------------------------------------------

from .kafka_client import close_producer
from .topic import get_topic

__all__ = [
    "close_producer",
    "get_topic",
]
