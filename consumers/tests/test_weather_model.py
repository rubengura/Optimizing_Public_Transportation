"""Tests for the weather reading shown in the dashboard header."""
import json

from models import Weather


def test_defaults_before_any_message_arrives():
    weather = Weather()
    assert (weather.temperature, weather.status) == (70.0, "sunny")


def test_an_avro_message_arrives_already_decoded(message):
    """The Avro deserializer hands over a dict, not bytes."""
    weather = Weather()
    weather.process_message(message("weather", {"temperature": 42.5, "status": "cloudy"}))
    assert (weather.temperature, weather.status) == (42.5, "cloudy")


def test_raw_json_is_also_accepted(message):
    """A plain consumer would hand over the bytes instead."""
    weather = Weather()
    payload = json.dumps({"temperature": 31.0, "status": "windy"}).encode()
    weather.process_message(message("weather", payload))
    assert (weather.temperature, weather.status) == (31.0, "windy")


def test_a_malformed_reading_leaves_the_last_good_one(message):
    weather = Weather()
    weather.process_message(message("weather", {"temperature": 55.0, "status": "sunny"}))
    weather.process_message(message("weather", "nonsense"))
    assert (weather.temperature, weather.status) == (55.0, "sunny")
