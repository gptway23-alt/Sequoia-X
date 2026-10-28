"""Tests for resolving the latest completed Shanghai weekday session."""

from datetime import datetime, date
from zoneinfo import ZoneInfo

import pytest

from sequoia_x.core.daily_schedule import latest_completed_weekday


SHANGHAI = ZoneInfo("Asia/Shanghai")


@pytest.mark.parametrize(
    ("now", "expected"),
    [
        (datetime(2026, 10, 19, 14, 0, tzinfo=SHANGHAI), date(2026, 10, 16)),
        (datetime(2026, 10, 19, 15, 5, tzinfo=SHANGHAI), date(2026, 10, 19)),
        (datetime(2026, 10, 20, 1, 20, tzinfo=SHANGHAI), date(2026, 10, 19)),
        (datetime(2026, 10, 24, 1, 20, tzinfo=SHANGHAI), date(2026, 10, 23)),
        (datetime(2026, 10, 26, 0, 37, tzinfo=SHANGHAI), date(2026, 10, 23)),
    ],
)
def test_latest_completed_weekday_handles_market_close_midnight_and_weekends(
    now: datetime, expected: date
) -> None:
    assert latest_completed_weekday(now) == expected


def test_latest_completed_weekday_normalizes_other_timezones() -> None:
    utc = datetime.fromisoformat("2026-10-19T17:20:00+00:00")
    assert latest_completed_weekday(utc) == date(2026, 10, 19)


def test_latest_completed_weekday_treats_naive_input_as_shanghai_time() -> None:
    naive = datetime(2026, 10, 20, 1, 20)
    assert latest_completed_weekday(naive) == date(2026, 10, 19)
