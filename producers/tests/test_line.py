"""Tests for the train simulation along a line."""
from pathlib import Path

import pandas as pd
import pytest

from models import Line


STATIONS_CSV = Path(__file__).parents[1] / "data" / "cta_stations.csv"
NUM_TRAINS = 10


@pytest.fixture(scope="module")
def blue_line_data():
    """The blue line, loaded the way simulation.py loads it."""
    stations = pd.read_csv(STATIONS_CSV).sort_values("order")
    return stations[stations["blue"]]


@pytest.fixture
def line(blue_line_data):
    return Line(Line.colors.blue, blue_line_data)


def placed_trains(line):
    return [t for s in line.stations for t in (s.a_train, s.b_train) if t is not None]


def test_stations_are_chained_in_platform_order(line):
    assert len(line.stations) == 32
    # Deliberately uneven: the pairs are consecutive stations, so the last one
    # has nothing to pair with.
    for current, following in zip(line.stations, line.stations[1:], strict=False):
        assert current.dir_b is following
        assert following.dir_a is current


def test_the_terminals_have_nothing_beyond_them(line):
    assert line.stations[0].dir_a is None
    assert line.stations[-1].dir_b is None


def test_num_stations_discounts_the_terminal(line):
    """Line.num_stations is an index bound, not a station count."""
    assert line.num_stations == len(line.stations) - 1


def test_every_train_is_placed_on_the_line(line):
    assert len(line.trains) == NUM_TRAINS
    assert {id(t) for t in placed_trains(line)} == {id(t) for t in line.trains}


def test_train_ids_are_unique_and_carry_the_line_initial(line):
    ids = [train.train_id for train in line.trains]
    assert len(set(ids)) == NUM_TRAINS
    assert all(train_id.startswith("BL") for train_id in ids)


def test_advancing_never_loses_or_duplicates_a_train(line):
    for _ in range(5):
        line._advance_trains()
        trains = placed_trains(line)
        assert len(trains) == NUM_TRAINS
        assert len({id(t) for t in trains}) == NUM_TRAINS


@pytest.mark.parametrize(
    "index, b_direction, expected",
    [
        (0, True, (1, True)),       # heading b, room to continue
        (30, True, (31, False)),    # reaches the far terminal and turns round
        (1, False, (0, True)),      # heading a, reaches the near terminal
    ],
)
def test_direction_reverses_at_the_terminals(line, index, b_direction, expected):
    assert line._get_next_idx(index, b_direction, step_size=1) == expected
