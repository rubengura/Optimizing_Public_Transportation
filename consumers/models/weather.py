"""Contains functionality related to Weather"""
import logging
import json


logger = logging.getLogger(__name__)


class Weather:
    """Defines the Weather model"""

    def __init__(self):
        """Creates the weather model"""
        self.temperature = 70.0
        self.status = "sunny"

    def process_message(self, message):
        """Handles incoming weather data"""
        try:
            value = message.value()
            # Avro topics reach here already decoded into a dict; a JSON topic
            # would hand over the raw bytes.
            if isinstance(value, (bytes, bytearray, str)):
                value = json.loads(value)
            self.temperature = value["temperature"]
            self.status = value["status"]
        except Exception as e:
            logger.fatal(f"unable to process weather message: {message}, {e}")

    def to_dict(self):
        """Serializes the weather for the status API"""
        return {"temperature": self.temperature, "status": self.status}
