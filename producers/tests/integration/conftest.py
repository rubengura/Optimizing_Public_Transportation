"""Fixtures for the tests that talk to a real broker and Schema Registry."""
import os

import pytest
import requests

from models.producer import BROKER_URL, SCHEMA_REGISTRY_URL


# The fake_kafka fixture in the parent conftest is autouse, and these tests
# need the real thing.
@pytest.fixture(autouse=True)
def fake_kafka():
    """Cancels the unit-test patching for this package."""
    return None


@pytest.fixture(scope="session")
def schema_registry_url():
    url = os.getenv("SCHEMA_REGISTRY_URL", SCHEMA_REGISTRY_URL)
    try:
        requests.get(f"{url}/subjects", timeout=5).raise_for_status()
    except Exception as exc:  # noqa: BLE001 - any failure means "not available"
        pytest.skip(f"Schema Registry not reachable at {url}: {exc}")
    return url


@pytest.fixture(scope="session")
def broker_url(schema_registry_url):
    """Depends on the registry fixture so one skip covers the whole stack."""
    return os.getenv("BROKER_URL", BROKER_URL)
