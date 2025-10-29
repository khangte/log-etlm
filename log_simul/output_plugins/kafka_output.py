from kafka import KafkaProducer

from .base import OutputPluginBase
from ..config import KAFKA_BOOTSTRAP_SERVERS

class KafkaOutput(OutputPluginBase):
    def __init__(self):
        self.producer = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)

    async def write(self, message: str, topic: str):
        self.producer.send(topic, message.encode("utf-8"))