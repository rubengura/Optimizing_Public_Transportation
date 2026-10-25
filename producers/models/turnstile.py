"""Creates a turnstile data producer"""
import logging
from pathlib import Path

from confluent_kafka import avro

from models.producer import Producer
from models.turnstile_hardware import TurnstileHardware


logger = logging.getLogger(__name__)


class Turnstile(Producer):
    key_schema = avro.load(f"{Path(__file__).parents[0]}/schemas/turnstile_key.json")

    value_schema = avro.load(
       f"{Path(__file__).parents[0]}/schemas/turnstile_value.json"
    )

    # One shared topic for every turnstile; ksqlDB aggregates it per station
    topic_name = "chicago.transport.turnstile"

    def __init__(self, station):
        """Create the Turnstile"""
        super().__init__(
            Turnstile.topic_name,
            key_schema=Turnstile.key_schema,
            value_schema=Turnstile.value_schema,
            num_partitions=5,
            num_replicas=1,
        )
        self.station = station
        self.turnstile_hardware = TurnstileHardware(station)

    def run(self, timestamp, time_step):
        """Simulates riders entering through the turnstile."""
        num_entries = self.turnstile_hardware.get_entries(timestamp, time_step)
        logger.debug(f"{num_entries} riders have entered at {self.station.name} station at {timestamp.isoformat()}")

        for _ in range(num_entries):
            try:
                self.produce(
                    key={"timestamp": self.time_millis()},
                    value={
                        "station_id": self.station.station_id,
                        "station_name": self.station.name,
                        "line": self.station.color.name,
                    },
                )
            except Exception as e:
                logger.fatal(e)
