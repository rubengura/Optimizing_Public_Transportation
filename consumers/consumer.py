"""Defines core consumer functionality"""
import logging
import os
import re

import confluent_kafka
from confluent_kafka import Consumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext
from tornado import gen


logger = logging.getLogger(__name__)


BROKER_URL = os.getenv("BROKER_URL", "PLAINTEXT://localhost:9092")
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")

_schema_registry = None


def schema_registry():
    """Returns the shared Schema Registry client, creating it on first use"""
    global _schema_registry
    if _schema_registry is None:
        _schema_registry = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
    return _schema_registry


class DecodedMessage:
    """A consumed message whose value has already been deserialized.

    The view models only read `topic()` and `value()`, so wrapping the message
    keeps Avro decoding inside the consumer rather than spreading it across
    every model. Everything else is delegated to the original message.
    """

    __slots__ = ("_message", "_value")

    def __init__(self, message, value):
        self._message = message
        self._value = value

    def value(self):
        return self._value

    def __getattr__(self, name):
        return getattr(self._message, name)


class KafkaConsumer:
    """Defines the base kafka consumer class"""

    def __init__(
        self,
        topic_name_pattern,
        message_handler,
        is_avro=True,
        offset_earliest=False,
        sleep_secs=1.0,
        consume_timeout=0.1,
        group_id=None,
        max_batch=100,
    ):
        """Creates a consumer object for asynchronous use"""
        self.topic_name_pattern = topic_name_pattern
        self.message_handler = message_handler
        self.sleep_secs = sleep_secs
        self.consume_timeout = consume_timeout
        self.offset_earliest = offset_earliest
        self.max_batch = max_batch

        # Each consumer needs its own group. Sharing one would make them join
        # the same rebalance and split the partitions between themselves, so
        # each handler would only ever see part of its topic.
        if group_id is None:
            group_id = "transit-status." + re.sub(
                r"[^A-Za-z0-9._-]", "", topic_name_pattern
            )

        self.broker_properties = {
            "group.id": group_id,
            "bootstrap.servers": BROKER_URL,
            "auto.offset.reset": "earliest" if offset_earliest else "latest",
        }

        # Avro topics are decoded in _consume; JSON ones reach the handler raw
        self.deserializer = AvroDeserializer(schema_registry()) if is_avro else None

        self.consumer = Consumer(self.broker_properties)
        self.consumer.subscribe([self.topic_name_pattern], on_assign=self.on_assign)

    def on_assign(self, consumer, partitions):
        """Callback for when topic assignment takes place"""
        if self.offset_earliest is True:
            for partition in partitions:
                partition.offset = confluent_kafka.OFFSET_BEGINNING

        logger.info("partitions assigned for %s", self.topic_name_pattern)
        consumer.assign(partitions)

    async def consume(self):
        """Asynchronously consumes data from kafka topic"""
        while True:
            # Bounded so that a busy topic cannot starve the Tornado IOLoop,
            # which also serves the web page.
            for _ in range(self.max_batch):
                if self._consume() == 0:
                    await gen.sleep(self.sleep_secs)
                    break
            else:
                await gen.sleep(0)

    def _consume(self):
        """Polls for a message. Returns 1 if a message was received, 0 otherwise"""
        message = self.consumer.poll(self.consume_timeout)

        if message is None:
            return 0

        if message.error() is not None:
            logger.error(
                "consumer error on %s: %s", self.topic_name_pattern, message.error()
            )
            return 0

        if self.deserializer is not None:
            try:
                value = self.deserializer(
                    message.value(),
                    SerializationContext(message.topic(), MessageField.VALUE),
                )
            except Exception as e:
                logger.error(
                    "unable to decode message on %s: %s", message.topic(), e
                )
                return 0
            message = DecodedMessage(message, value)

        self.message_handler(message)
        return 1

    def close(self):
        """Cleans up any open kafka consumers"""
        logger.debug("closing consumer")
        self.consumer.close()
