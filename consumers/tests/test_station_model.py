"""Tests for the station view model the dashboard renders."""
from models import Station


def a_station():
    return Station.from_message(
        {"station_id": 40070, "station_name": "Jackson", "order": 19}
    )


def test_from_message_takes_only_the_fields_the_board_shows():
    station = a_station()
    assert (station.station_id, station.station_name, station.order) == (
        40070,
        "Jackson",
        19,
    )
    assert station.dir_a is None
    assert station.dir_b is None
    assert station.num_turnstile_entries == 0


def test_an_arrival_fills_the_matching_direction():
    station = a_station()
    station.handle_arrival("b", "BL008", "in_service")
    assert station.dir_b == {"train_id": "BL008", "status": "in service"}
    assert station.dir_a is None


def test_a_departure_empties_the_matching_direction():
    station = a_station()
    station.handle_arrival("a", "BL007", "in_service")
    station.handle_departure("a")
    assert station.dir_a is None


def test_turnstile_entries_come_from_the_ksqldb_column():
    """ksqlDB uppercases unquoted identifiers, so the column is TOTAL_ENTRIES."""
    station = a_station()
    station.process_message({"STATION_ID": 40070, "TOTAL_ENTRIES": 147})
    assert station.num_turnstile_entries == 147
