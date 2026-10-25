"""Canonical Kafka topic names for the transit status application.

They live in one place because four modules have to agree on them, and because
each stage of the pipeline writes into the next stage's input.
"""

# Avro, produced by producers/models/station.py
ARRIVALS = "chicago.transport.arrivals"

# Avro, produced by producers/models/turnstile.py and aggregated by ksqlDB
TURNSTILE = "chicago.transport.turnstile"

# Avro, posted by producers/models/weather.py through REST Proxy
WEATHER = "chicago.transport.weather"

# JSON, written by the Kafka Connect JDBC source connector. The connector's
# `topic.prefix` plus the `stations` table name produce this name.
CONNECT_STATIONS = "chicago.transport.stations"

# JSON, the Faust table changelog derived from CONNECT_STATIONS
STATIONS_TABLE = "chicago.transport.stations.table"

# JSON, the ksqlDB table aggregating turnstile events per station. ksqlDB
# uppercases unquoted identifiers, so the topic name is uppercase as well.
TURNSTILE_SUMMARY = "TURNSTILE_SUMMARY"
