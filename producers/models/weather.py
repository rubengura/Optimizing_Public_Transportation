"""Methods pertaining to weather data"""
from enum import IntEnum
import json
import logging
import os
import random

import requests

from models.producer import Producer, load_schema


logger = logging.getLogger(__name__)


class Weather(Producer):
    """Defines a simulated weather model"""

    status = IntEnum(
        "status", "sunny partly_cloudy cloudy windy precipitation", start=0
    )

    rest_proxy_url = os.getenv("REST_PROXY_URL", "http://localhost:8082")

    # Loaded at import time: the base class needs them before __init__ runs
    key_schema = load_schema("weather_key.json")
    value_schema = load_schema("weather_value.json")

    topic_name = "chicago.transport.weather"

    winter_months = set((0, 1, 2, 3, 10, 11))
    summer_months = set((6, 7, 8))

    def __init__(self, month):
        super().__init__(
            Weather.topic_name,
            key_schema=Weather.key_schema,
            value_schema=Weather.value_schema,
            num_partitions=5,
            num_replicas=1
        )

        self.status = Weather.status.sunny
        self.temp = 70.0
        if month in Weather.winter_months:
            self.temp = 40.0
        elif month in Weather.summer_months:
            self.temp = 85.0

    def _set_weather(self, month):
        """Returns the current weather"""
        mode = 0.0
        if month in Weather.winter_months:
            mode = -1.0
        elif month in Weather.summer_months:
            mode = 1.0
        self.temp += min(max(-20.0, random.triangular(-10.0, 10.0, mode)), 100.0)
        self.status = random.choice(list(Weather.status))

    def run(self, month):
        """Posts a weather event through REST Proxy.

        The CTA weather hardware cannot run the Kafka client library, so it
        speaks HTTP instead. The Avro schemas travel inline with every request.
        """
        self._set_weather(month)

        resp = requests.post(
           f"{Weather.rest_proxy_url}/topics/{self.topic_name}",
           headers={"Content-Type": "application/vnd.kafka.avro.v2+json"},
           timeout=30,
           data=json.dumps(
               {
                   "key_schema": Weather.key_schema,
                   "value_schema": Weather.value_schema,
                   "records": [
                       {
                           "key": {"timestamp": self.time_millis()},
                           "value": {
                               "temperature": self.temp,
                               "status": self.status.name
                           }
                       }
                   ]
               }
           ),
        )
        resp.raise_for_status()
        logger.debug(
            "sent weather data to kafka, temp: %s, status: %s",
            self.temp,
            self.status.name,
        )
