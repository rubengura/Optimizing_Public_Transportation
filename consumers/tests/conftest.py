"""Shared fixtures for the consumer tests."""
import pytest


class FakeMessage:
    """The slice of confluent_kafka.Message the view models actually use.

    `value` is handed over exactly as the real consumer would: a decoded dict
    for Avro topics, raw JSON text for the JSON ones.
    """

    def __init__(self, topic, value, key=None):
        self._topic = topic
        self._value = value
        self._key = key

    def topic(self):
        return self._topic

    def value(self):
        return self._value

    def key(self):
        return self._key


@pytest.fixture
def message():
    return FakeMessage
