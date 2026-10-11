"""Read-only BaoStock on-demand UNADJUSTED daily OHLCV adapter.

This module deliberately has NO SQLite, raw full-universe backfill, Pages export,
email attachment, transaction, or filesystem write functions. The historical
strategy DataEngine in engine.py remains separate and unchanged.
"""
from __future__ import annotations

import math
from datetime import date
from typing import Any

SOURCE = "baostock_adjustflag_3"
FIELDS = "date,open,high,low,close,volume,amount"


class MarketApiError(RuntimeError):
    """BaoStock returned untrusted, incomplete, or invalid historical bars."""


def iso_day(value: object) -> str:
    text = str(value)
    try:
        if date.fromisoformat(text).isoformat() == text:
            return text
    except ValueError:
        pass
    raise MarketApiError(f"Invalid ISO date: {text!r}")


def bs_code(symbol: str) -> str:
    if len(symbol) != 6 or not symbol.isascii() or not symbol.isdigit():
        raise MarketApiError(f"Invalid symbol: {symbol!r}")
    if symbol.startswith(("0", "3")):
        return "sz." + symbol
    if symbol.startswith("6"):
        return "sh." + symbol
    if symbol.startswith(("4", "8", "9")):
        return "bj." + symbol
    raise MarketApiError(f"Unknown symbol exchange: {symbol}")


def parse_bar(symbol: str, fields: list[str], start: str, end: str) -> tuple:
    if len(fields) != 7:
        raise MarketApiError(f"{symbol}: wrong BaoStock row length {len(fields)}")
    day = iso_day(fields[0])
    if not start <= day <= end:
        raise MarketApiError(f"{symbol}: unexpected market date {day}")
    try:
        o, h, low, c, volume, turnover = (float(x) for x in fields[1:])
    except (ValueError, TypeError) as exc:
        raise MarketApiError(f"{symbol}: non-numeric price on {day}") from exc
    if not (all(math.isfinite(x) for x in (o, h, low, c, volume, turnover))
            and low > 0 and low <= min(o, c) and max(o, c) <= h
            and volume >= 0 and turnover >= 0):
        raise MarketApiError(f"{symbol}: invalid raw OHLC on {day}")
    return symbol, day, o, h, low, c, volume, turnover


def fetch_raw(bs: Any, symbol: str, start: str, end: str) -> list[tuple]:
    """Direct provider request. NO database; raw prices use adjustflag='3'."""
    start, end = iso_day(start), iso_day(end)
    if start > end:
        raise MarketApiError("start must be <= end")
    response = bs.query_history_k_data_plus(
        bs_code(symbol), FIELDS,
        start_date=start, end_date=end,
        frequency="d", adjustflag="3",
    )
    if getattr(response, "error_code", "ERR") != "0":
        raise MarketApiError(
            f"{symbol}: BaoStock failed: {getattr(response, 'error_msg', '')}"
        )
    bars: list[tuple] = []
    seen: set[str] = set()
    while response.next():
        bar = parse_bar(symbol, list(response.get_row_data()), start, end)
        if bar[1] in seen:
            raise MarketApiError(f"{symbol}: duplicate raw OHLC {bar[1]}")
        seen.add(bar[1])
        bars.append(bar)
    if getattr(response, "error_code", "ERR") != "0":
        raise MarketApiError(
            f"{symbol}: stream failed: {getattr(response, 'error_msg', '')}"
        )
    bars.sort(key=lambda row: row[1])
    return bars
