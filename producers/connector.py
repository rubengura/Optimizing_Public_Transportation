"""Configures a Kafka Connector for Postgres Station data"""
import json
import logging
import os

import requests


logger = logging.getLogger(__name__)


KAFKA_CONNECT_URL = os.getenv("KAFKA_CONNECT_URL", "http://localhost:8083") + "/connectors"
CONNECTOR_NAME = "stations"

# Kafka Connect runs inside the compose network, so the database host is the
# `postgres` service name. Using localhost here would point Connect at itself.
DB_URL = os.getenv("CONNECT_DB_URL", "jdbc:postgresql://postgres:5432/cta")
DB_USER = os.getenv("POSTGRES_USER", "cta_admin")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD", "chicago")

# The connector appends the table name, producing `chicago.transport.stations`,
# which is the topic the Faust stream processor reads from.
TOPIC_PREFIX = "chicago.transport."


def configure_connector():
    """Starts and configures the Kafka Connect connector"""
    logging.debug("creating or updating kafka connect connector...")

    resp = requests.get(f"{KAFKA_CONNECT_URL}/{CONNECTOR_NAME}", timeout=10)
    if resp.status_code == 200:
        logging.debug("connector already created skipping recreation")
        return

    resp = requests.post(
        KAFKA_CONNECT_URL,
        headers={"Content-Type": "application/json"},
        timeout=30,
        data=json.dumps({
            "name": CONNECTOR_NAME,
            "config": {
                "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
                "key.converter": "org.apache.kafka.connect.json.JsonConverter",
                "key.converter.schemas.enable": "false",
                "value.converter": "org.apache.kafka.connect.json.JsonConverter",
                "value.converter.schemas.enable": "false",
                "batch.max.rows": "500",
                "connection.url": DB_URL,
                "connection.user": DB_USER,
                "connection.password": DB_PASSWORD,
                "table.whitelist": "stations",
                "mode": "incrementing",
                "incrementing.column.name": "stop_id",
                "topic.prefix": TOPIC_PREFIX,
                # The station table is static, so there is no point polling it
                # more than once a minute.
                "poll.interval.ms": "60000",
            }
        }),
    )

    ## Ensure a healthy response was given
    resp.raise_for_status()
    logging.debug("connector created successfully")


if __name__ == "__main__":
    configure_connector()
