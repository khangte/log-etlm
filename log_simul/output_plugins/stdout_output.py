from .base import OutputPluginBase

class StdoutOutput(OutputPluginBase):
    async def write(self, message: str, topic: str):
        print(f"[{topic}] {message}")
