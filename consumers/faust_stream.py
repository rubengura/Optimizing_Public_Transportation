"""Defines trends calculations for stations"""
import logging
import os

import faust

import topics


logger = logging.getLogger(__name__)


FAUST_BROKER_URL = os.getenv("FAUST_BROKER_URL", "kafka://localhost:9092")


# Faust will ingest records from Kafka in this format
class Station(faust.Record):
    stop_id: int
    direction_id: str
    stop_name: str
    station_name: str
    station_descriptive_name: str
    station_id: int
    order: int
    red: bool
    blue: bool
    green: bool


# Faust will produce records to Kafka in this format
class TransformedStation(faust.Record):
    station_id: int
    station_name: str
    order: int
    line: str


app = faust.App("stations-stream", broker=FAUST_BROKER_URL, store="memory://")

# Raw rows as Kafka Connect dumped them out of the `stations` Postgres table
in_topic = app.topic(topics.CONNECT_STATIONS, value_type=Station)

# The trimmed-down view the web server consumes
out_topic = app.topic(topics.STATIONS_TABLE, partitions=1)

# Writing into the table publishes onto its changelog, which is out_topic
table = app.Table(
    "stations-table",
    partitions=1,
    changelog_topic=out_topic,
)


@app.agent(in_topic)
async def transform_stations(stations):
    """Keeps only the fields the dashboard needs, with the line as a color"""
    async for station in stations:
        table[station.station_id] = TransformedStation(
            station_id=station.station_id,
            station_name=station.station_name,
            order=station.order,
            line="red" if station.red else "blue" if station.blue else "green",
        )


if __name__ == "__main__":
    app.main()
