"""Helpers for recovering scheduled runs after GitHub delays."""

from datetime import date, datetime, time, timedelta
from zoneinfo import ZoneInfo


SHANGHAI = ZoneInfo("Asia/Shanghai")
MARKET_CLOSE_CHECK = time(15, 5)


def latest_completed_weekday(now: datetime | None = None) -> date:
    """Return the latest completed weekday session by Shanghai wall clock.

    A delayed run after midnight belongs to the preceding session. Before
    today's close check, use the previous weekday so a backup schedule never
    treats an in-progress session as complete.
    """
    current = now or datetime.now(SHANGHAI)
    if current.tzinfo is None:
        current = current.replace(tzinfo=SHANGHAI)
    else:
        current = current.astimezone(SHANGHAI)

    candidate = current.date()
    if candidate.weekday() < 5 and current.time() < MARKET_CLOSE_CHECK:
        candidate -= timedelta(days=1)
    while candidate.weekday() >= 5:
        candidate -= timedelta(days=1)
    return candidate
