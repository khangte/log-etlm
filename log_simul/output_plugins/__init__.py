from ..config import OUTPUT_PLUGIN
from .stdout_output import StdoutOutput
from .kafka_output import KafkaOutput

def get_output_plugin():
    if OUTPUT_PLUGIN == "kafka":
        return KafkaOutput()
    else:
        return StdoutOutput()
