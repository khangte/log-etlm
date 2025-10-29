from abc import ABC, abstractmethod

class OutputPluginBase(ABC):
    @abstractmethod
    async def write(self, message: str, topic: str):
        pass
