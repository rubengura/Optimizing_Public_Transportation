"""Characterises the bytes the arrival producer puts on the wire.

This test exists to survive the migration from the deprecated AvroProducer to
the AvroSerializer API unchanged. Everything it asserts is part of the
Confluent Avro wire contract, not of the client API used to produce, so if the
migration altered the framing, the subject naming or the payload, this is where
it would show.
"""
import json
import struct
import time
import uuid

import pytest
from confluent_kafka import Consumer, TopicPartition
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext

from models import Line, Station, Train


pytestmark = pytest.mark.integration

ARRIVALS = "chicago.transport.arrivals"

# Jackson on the blue line, and the stop before it in the b direction
JACKSON = 40070
MONROE = 40790


def _end_offsets(consumer, topic, timeout=30):
    """Maps each partition of `topic` to its current high watermark.

    Topic creation is asynchronous, so cluster metadata can lag behind the
    CreateTopics call that Station.__init__ makes.
    """
    deadline = time.monotonic() + timeout
    while True:
        metadata = consumer.list_topics(topic, timeout=10).topics.get(topic)
        if metadata is not None and metadata.error is None and metadata.partitions:
            return {
                partition: consumer.get_watermark_offsets(
                    TopicPartition(topic, partition), timeout=15
                )[1]
                for partition in metadata.partitions
            }
        if time.monotonic() > deadline:
            pytest.fail(f"{topic} never appeared in cluster metadata")
        time.sleep(0.5)


@pytest.fixture
def produced_arrival(broker_url, schema_registry_url):
    """Emits one arrival through the real producer and returns its raw bytes.

    Partitions are assigned at their current high watermarks before producing,
    so the test reads its own message instead of replaying the whole topic.
    """
    train_id = f"IT{uuid.uuid4().hex[:4].upper()}"

    # Build the producer first. Station.__init__ is what creates the topic,
    # with its five partitions, and on a fresh cluster there is nothing to
    # assign before that happens.
    station = Station(JACKSON, "Jackson", Line.colors.blue)

    consumer = Consumer(
        {
            "bootstrap.servers": broker_url,
            "group.id": f"avro-roundtrip-{uuid.uuid4()}",
            "enable.auto.commit": False,
            # Never paper over a missing topic with a one-partition stand-in
            "allow.auto.create.topics": False,
        }
    )
    try:
        assignment = [
            TopicPartition(ARRIVALS, partition, high)
            for partition, high in _end_offsets(consumer, ARRIVALS).items()
        ]
        assert assignment, f"no partitions to watch on {ARRIVALS}"
        consumer.assign(assignment)

        station.run(Train(train_id, Train.status.in_service), "b", MONROE, "b")
        station.close()

        deadline = 200
        while deadline:
            message = consumer.poll(0.5)
            deadline -= 1
            if message is None or message.error() is not None:
                continue
            payload = message.value()
            # The train id is unique per run, so this skips simulation traffic
            if train_id.encode() in payload:
                yield train_id, message
                return
        pytest.fail(f"arrival for {train_id} never came back")
    finally:
        consumer.close()


def test_the_value_uses_the_confluent_avro_framing(produced_arrival, schema_registry_url):
    """Magic byte 0, then a big-endian schema id the registry knows."""
    _, message = produced_arrival
    payload = message.value()

    assert payload[0] == 0

    schema_id = struct.unpack(">I", payload[1:5])[0]
    registry = SchemaRegistryClient({"url": schema_registry_url})
    schema = json.loads(registry.get_schema(schema_id).schema_str)

    # arrival_value.json declares namespace "com.udacity" and name
    # "arrival.value", but a dotted name *is* a fullname in Avro, so the
    # declared namespace is discarded and the record normalises to
    # arrival.value. Pinning the normalised form catches any change in how the
    # schema text reaches the registry.
    assert f"{schema['namespace']}.{schema['name']}" == "arrival.value"


def test_the_schema_is_registered_under_the_topic_name(schema_registry_url):
    """TopicNameStrategy: {topic}-key and {topic}-value."""
    registry = SchemaRegistryClient({"url": schema_registry_url})
    subjects = registry.get_subjects()
    assert f"{ARRIVALS}-value" in subjects
    assert f"{ARRIVALS}-key" in subjects


def test_the_modern_deserializer_reads_it(produced_arrival, schema_registry_url):
    train_id, message = produced_arrival
    registry = SchemaRegistryClient({"url": schema_registry_url})
    deserializer = AvroDeserializer(registry)

    value = deserializer(
        message.value(), SerializationContext(ARRIVALS, MessageField.VALUE)
    )

    assert value == {
        "station_id": JACKSON,
        "train_id": train_id,
        "direction": "b",
        "line": "blue",
        "train_status": "in_service",
        "prev_station_id": MONROE,
        "prev_direction": "b",
    }


def test_producing_registers_no_new_schema_version(produced_arrival, schema_registry_url):
    """The guard rail for the migration.

    Changing the subject strategy, the record name or the schema text would
    register a second version here. Staying on one version is what proves the
    messages already in the topics are still readable.
    """
    registry = SchemaRegistryClient({"url": schema_registry_url})
    for subject in (f"{ARRIVALS}-key", f"{ARRIVALS}-value"):
        assert len(registry.get_versions(subject)) == 1, subject
