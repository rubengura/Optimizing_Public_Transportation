"""Tests for how a single line dispatches the four kinds of message."""
import json

import pytest

import topics
from models import Line


BLUE_STATION = {"station_id": 40070, "station_name": "Jackson", "order": 19, "line": "blue"}
RED_STATION = {"station_id": 40560, "station_name": "Jackson", "order": 21, "line": "red"}


@pytest.fixture
def line():
    return Line("blue")


def stations_table(message, payload):
    """Faust writes its table changelog as JSON text."""
    return message(topics.STATIONS_TABLE, json.dumps(payload))


def test_a_station_on_this_line_is_added(line, message):
    line.process_message(stations_table(message, BLUE_STATION))
    assert 40070 in line.stations
    assert line.stations[40070].station_name == "Jackson"


def test_a_station_on_another_line_is_ignored(line, message):
    line.process_message(stations_table(message, RED_STATION))
    assert line.stations == {}


def test_an_arrival_places_the_train_and_clears_the_previous_station(line, message):
    line.process_message(stations_table(message, BLUE_STATION))
    line.process_message(stations_table(message, {**BLUE_STATION, "station_id": 40790, "station_name": "Monroe", "order": 18}))
    line.stations[40790].handle_arrival("b", "BL008", "in_service")

    # Avro topics arrive already decoded, so the value is a dict
    line.process_message(
        message(
            topics.ARRIVALS,
            {
                "station_id": 40070,
                "train_id": "BL008",
                "direction": "b",
                "line": "blue",
                "train_status": "in_service",
                "prev_station_id": 40790,
                "prev_direction": "b",
            },
        )
    )

    assert line.stations[40070].dir_b == {"train_id": "BL008", "status": "in service"}
    assert line.stations[40790].dir_b is None


def test_a_turnstile_summary_updates_the_counter(line, message):
    line.process_message(stations_table(message, BLUE_STATION))
    line.process_message(
        message(topics.TURNSTILE_SUMMARY, json.dumps({"STATION_ID": 40070, "TOTAL_ENTRIES": 147}))
    )
    assert line.stations[40070].num_turnstile_entries == 147


def test_a_turnstile_summary_for_an_unknown_station_is_dropped(line, message):
    line.process_message(
        message(topics.TURNSTILE_SUMMARY, json.dumps({"STATION_ID": 99999, "TOTAL_ENTRIES": 5}))
    )
    assert line.stations == {}


def test_an_unknown_topic_changes_nothing(line, message):
    line.process_message(message("some.other.topic", json.dumps(BLUE_STATION)))
    assert line.stations == {}


def test_a_malformed_station_payload_does_not_propagate(line, message):
    """A bad message must not take the Tornado IOLoop down with it."""
    line.process_message(message(topics.STATIONS_TABLE, "not json at all"))
    assert line.stations == {}
