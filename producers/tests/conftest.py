"""Shared fixtures for the producer tests."""
from unittest.mock import MagicMock

import pytest


@pytest.fixture(autouse=True)
def fake_kafka(monkeypatch):
    """Keeps the models off the network.

    Line and TurnstileHardware are pure simulation logic, but Line builds
    Station objects and Station.__init__ reaches Producer.__init__, which
    creates topics through an AdminClient, opens a producer connection and
    builds serializers against the Schema Registry.
    """
    import models.producer as producer_module

    monkeypatch.setattr(producer_module, "AdminClient", MagicMock())
    monkeypatch.setattr(producer_module, "KafkaProducer", MagicMock())
    monkeypatch.setattr(producer_module, "AvroSerializer", MagicMock())
    monkeypatch.setattr(producer_module.Producer, "_schema_registry", MagicMock())
    # Topic creation is deduplicated through a class attribute shared by every
    # Producer instance; reset it so tests do not depend on each other.
    monkeypatch.setattr(producer_module.Producer, "existing_topics", set())
