"""Contains functionality related to Lines"""
import json
import logging

from models import Line
import topics


logger = logging.getLogger(__name__)


class Lines:
    """Contains all train lines"""

    def __init__(self):
        """Creates the Lines object"""
        self.red_line = Line("red")
        self.green_line = Line("green")
        self.blue_line = Line("blue")

    def process_message(self, message):
        """Processes a station message"""
        topic = message.topic()

        if topic == topics.TURNSTILE_SUMMARY:
            # Turnstile counts are keyed by station, not by line, so every
            # line has to inspect them to find its own stations.
            self.green_line.process_message(message)
            self.red_line.process_message(message)
            self.blue_line.process_message(message)
            return

        if topic == topics.STATIONS_TABLE:
            value = json.loads(message.value())
        elif topic == topics.ARRIVALS:
            value = message.value()
        else:
            logger.info("ignoring non-lines message %s", topic)
            return

        line = value.get("line")
        if line == "green":
            self.green_line.process_message(message)
        elif line == "red":
            self.red_line.process_message(message)
        elif line == "blue":
            self.blue_line.process_message(message)
        else:
            logger.debug("discarding unknown line msg %s", line)

    def to_dict(self):
        """Serializes every line, in the order the dashboard draws them"""
        return [
            line.to_dict()
            for line in (self.blue_line, self.green_line, self.red_line)
        ]
