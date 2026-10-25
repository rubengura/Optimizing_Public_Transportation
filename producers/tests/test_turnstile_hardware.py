"""Tests for the ridership simulation behind the turnstiles."""
import datetime

import pytest

from models.turnstile_hardware import TurnstileHardware


# Clark/Lake: a real CTA station, and the busiest row in ridership_seed.csv
STATION_ID = 40380

# 08:00, where the hourly curve peaks at 0.125
MONDAY = datetime.datetime(2026, 1, 5, 8)
SATURDAY = datetime.datetime(2026, 1, 10, 8)
SUNDAY = datetime.datetime(2026, 1, 11, 8)

FIVE_MINUTES = datetime.timedelta(minutes=5)


class FakeStation:
    """get_entries only ever reads the station id."""

    def __init__(self, station_id):
        self.station_id = station_id


@pytest.fixture
def hardware(monkeypatch):
    """Hardware with round ridership figures and the random jitter pinned.

    get_entries adds random.choice(range(-5, 5)) to every result; removing it
    leaves the curve arithmetic as the only thing under test.
    """
    hw = TurnstileHardware(FakeStation(STATION_ID))
    hw.weekday_ridership = 24000
    hw.saturday_ridership = 12000
    hw.sunday_ridership = 6000
    monkeypatch.setattr("models.turnstile_hardware.random.choice", lambda seq: 0)
    return hw


def test_the_fixture_dates_are_the_days_they_claim():
    """weekday() counts Monday as 0 and Saturday as 5, not 6."""
    assert MONDAY.weekday() == 0
    assert SATURDAY.weekday() == 5
    assert SUNDAY.weekday() == 6


def test_weekday_uses_the_weekday_ridership(hardware):
    # 24000 riders * 0.125 of the day at 08:00, spread over 300 steps
    assert hardware.get_entries(MONDAY, FIVE_MINUTES) == 10


def test_saturday_uses_the_saturday_ridership(hardware):
    assert hardware.get_entries(SATURDAY, FIVE_MINUTES) == 5


def test_sunday_uses_the_sunday_ridership(hardware):
    assert hardware.get_entries(SUNDAY, FIVE_MINUTES) == 2


@pytest.mark.parametrize(
    "hour, ratio",
    [
        (3, 0.01),      # middle of the night
        (8, 0.125),     # morning peak
        (12, 0.0225),   # midday lull
        (18, 0.125),    # evening peak
    ],
)
def test_entries_follow_the_hourly_curve(hardware, hour, ratio):
    expected = int(24000 * ratio / 300)
    assert hardware.get_entries(MONDAY.replace(hour=hour), FIVE_MINUTES) == expected


def test_entries_are_never_negative():
    """The jitter subtracts up to five riders, which a quiet hour cannot afford."""
    hw = TurnstileHardware(FakeStation(STATION_ID))
    hw.weekday_ridership = 0
    quiet_hour = MONDAY.replace(hour=3)
    assert all(hw.get_entries(quiet_hour, FIVE_MINUTES) >= 0 for _ in range(50))


def test_ridership_is_read_from_the_seed_file():
    hw = TurnstileHardware(FakeStation(STATION_ID))
    assert hw.weekday_ridership == 22812
    assert hw.saturday_ridership == 6902
    assert hw.sunday_ridership == 6234
