"""Tests for the fan-out across the three lines."""
import json

import pytest

import topics
from models import Lines


@pytest.fixture
def lines():
    return Lines()


def station_payload(line, station_id=40070):
    return {"station_id": station_id, "station_name": "Jackson", "order": 19, "line": line}


@pytest.mark.parametrize("color", ["blue", "red", "green"])
def test_a_station_reaches_only_its_own_line(lines, message, color):
    lines.process_message(
        message(topics.STATIONS_TABLE, json.dumps(station_payload(color)))
    )
    by_color = {"blue": lines.blue_line, "red": lines.red_line, "green": lines.green_line}
    assert 40070 in by_color[color].stations
    for other, line in by_color.items():
        if other != color:
            assert line.stations == {}


def test_an_arrival_is_routed_by_its_line_field(lines, message):
    lines.process_message(
        message(topics.STATIONS_TABLE, json.dumps(station_payload("green")))
    )
    lines.process_message(
        message(
            topics.ARRIVALS,
            {
                "station_id": 40070,
                "train_id": "GL001",
                "direction": "a",
                "line": "green",
                "train_status": "in_service",
                "prev_station_id": None,
                "prev_direction": None,
            },
        )
    )
    assert lines.green_line.stations[40070].dir_a == {
        "train_id": "GL001",
        "status": "in service",
    }


def test_a_turnstile_summary_goes_to_every_line(lines, message):
    """The summary is keyed by station, not by line, so each line has to look."""
    lines.process_message(
        message(topics.STATIONS_TABLE, json.dumps(station_payload("red")))
    )
    lines.process_message(
        message(topics.TURNSTILE_SUMMARY, json.dumps({"STATION_ID": 40070, "TOTAL_ENTRIES": 88}))
    )
    assert lines.red_line.stations[40070].num_turnstile_entries == 88


def test_an_unknown_line_is_discarded(lines, message):
    lines.process_message(
        message(topics.STATIONS_TABLE, json.dumps(station_payload("brown")))
    )
    assert all(
        line.stations == {}
        for line in (lines.blue_line, lines.red_line, lines.green_line)
    )


def test_an_unknown_topic_is_ignored(lines, message):
    lines.process_message(message("org.chicago.cta.station", json.dumps(station_payload("blue"))))
    assert lines.blue_line.stations == {}
