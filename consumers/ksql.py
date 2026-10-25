"""Configures KSQL to combine station and turnstile data"""
import json
import logging
import os

import requests

import topic_check
import topics


logger = logging.getLogger(__name__)


KSQL_URL = os.getenv("KSQL_URL", "http://localhost:8088")

# The turnstile topic is keyed by timestamp, not by station, so it cannot be
# read as a table directly: ksqlDB requires a table's primary key to be the
# Kafka message key. Reading it as a stream and aggregating produces a table
# keyed by station, which is what the dashboard needs.
#
# Two details that are easy to get wrong:
#
# * A GROUP BY column lands in the Kafka message *key*, not in the value. The
#   web server only reads values, so `AS_VALUE` copies the station id into the
#   value as well; `station_key` is the key column it leaves behind.
# * `COUNT` is a reserved word and would need backquoting as an alias, hence
#   TOTAL_ENTRIES.
KSQL_STATEMENT = f"""
CREATE STREAM turnstile (
    station_id INT,
    station_name VARCHAR,
    line VARCHAR
) WITH (
    KAFKA_TOPIC = '{topics.TURNSTILE}',
    VALUE_FORMAT = 'AVRO'
);

CREATE TABLE turnstile_summary
WITH (VALUE_FORMAT = 'JSON') AS
    SELECT station_id AS station_key,
           AS_VALUE(station_id) AS station_id,
           COUNT(*) AS total_entries
    FROM turnstile
    GROUP BY station_id;
"""


def execute_statement():
    """Executes the KSQL statement against the KSQL API"""
    if topic_check.topic_exists(topics.TURNSTILE_SUMMARY) is True:
        return

    logging.debug("executing ksql statement...")

    resp = requests.post(
        f"{KSQL_URL}/ksql",
        headers={"Content-Type": "application/vnd.ksql.v1+json"},
        timeout=60,
        data=json.dumps(
            {
                "ksql": KSQL_STATEMENT,
                "streamsProperties": {"ksql.streams.auto.offset.reset": "earliest"},
            }
        ),
    )

    # Ensure that a 2XX status code was returned
    resp.raise_for_status()


if __name__ == "__main__":
    execute_statement()
