
"""Publish a read-only static market-data API without emailing database ZIPs.

The existing stock_daily is post-adjusted (BaoStock adjustflag=1).
This publisher makes fresh, unadjusted requests (adjustflag=3)
for stocks that actually appear in verified selection history.

Private selection/strategy rows and full SQLite databases are
never exposed on GitHub Pages.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import sqlite3

from collections import defaultdict
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any


class MarketApiError(RuntimeError):
    """The data cannot be accurately or safely published."""


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()

    with path.open("rb") as f:
        for chunk in iter(
            lambda: f.read(1 << 20), b""
        ):
            digest.update(chunk)

    return digest.hexdigest()


def _day(value: str) -> str:
    value = str(value)

    try:
        d = date.fromisoformat(value)
    except ValueError as exc:
        raise MarketApiError(
            f"Invalid trade date: {value}"
        ) from exc

    if d.isoformat() != value:
        raise MarketApiError(
            f"Non-canonical trade date: {value}"
        )

    return value


def _bs_symbol(code: str) -> str | None:
    if (
        len(code) != 6
        or not code.isascii()
        or not code.isdigit()
    ):
        return None

    if code.startswith(("0", "3")):
        return f"sz.{code}"

    if code.startswith("6"):
        return f"sh.{code}"

    return None


def _raw_row(
    symbol: str,
    fields: list[str],
    *,
    start: str,
    end: str,
) -> dict[str, Any]:

    if len(fields) != 7:
        raise MarketApiError(
            f"Invalid BaoStock row length: {symbol}"
        )

    trading_day = _day(fields[0])

    if not (start <= trading_day <= end):
        raise MarketApiError(
            f"Out-of-range raw price: "
            f"{symbol}, {trading_day}"
        )

    try:
        o, h, l, c = (
            float(value)
            for value in fields[1:5]
        )

        volume, turnover = (
            float(value)
            for value in fields[5:7]
        )

    except (ValueError, TypeError) as exc:
        raise MarketApiError(
            f"Invalid raw price: "
            f"{symbol}, {trading_day}"
        ) from exc

    if not (
        all(
            math.isfinite(value)
            for value in (
                o, h, l, c, volume, turnover
            )
        )
        and 0 < l <= min(o, c)
        and max(o, c) <= h
        and volume >= 0
        and turnover >= 0
    ):
        raise MarketApiError(
            f"OHLC validation failed: "
            f"{symbol}, {trading_day}"
        )

    return {
        "symbol": symbol,
        "date": trading_day,
        "open": o,
        "high": h,
        "low": l,
        "close": c,
        "volume": volume,
        "turnover": turnover,
    }


def _query_raw(
    bs: Any,
    symbol: str,
    start: str,
    end: str,
) -> list[dict[str, Any]]:

    bs_code = _bs_symbol(symbol)

    if bs_code is None:
        raise MarketApiError(
            f"Unsupported stock exchange: {symbol}"
        )

    response = bs.query_history_k_data_plus(
        bs_code,
        "date,open,high,low,close,volume,amount",
        start_date=start,
        end_date=end,
        frequency="d",
        adjustflag="3",
    )

    if response.error_code != "0":
        raise MarketApiError(
            f"BaoStock query failed: "
            f"{symbol}: {response.error_msg}"
        )

    result: list[dict[str, Any]] = []
    seen: set[str] = set()

    while response.next():
        record = _raw_row(
            symbol,
            list(response.get_row_data()),
            start=start,
            end=end,
        )

        if record["date"] in seen:
            raise MarketApiError(
                f"Duplicate raw date: "
                f"{symbol}, {record['date']}"
            )

        seen.add(record["date"])
        result.append(record)

    if response.error_code != "0":
        raise MarketApiError(
            f"BaoStock iteration failed: "
            f"{symbol}: {response.error_msg}"
        )

    return sorted(
        result,
        key=lambda x: x["date"],
    )


def _dump(path: Path, obj: Any) -> None:
    path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    path.write_text(
        json.dumps(
            obj,
            ensure_ascii=False,
            separators=(",", ":"),
            allow_nan=False,
        ) + "\n",
        encoding="utf-8",
    )


def export_market_api(
    snapshot: Path,
    manifest_path: Path,
    output: Path,
    market_date: str,
    *,
    bs: Any | None = None,
) -> dict[str, Any]:

    """Export selected stocks' raw OHLC as static JSON."""

    market_date = _day(market_date)

    if not snapshot.is_file():
        raise MarketApiError(
            "Verified GPT snapshot is missing"
        )

    if not manifest_path.is_file():
        raise MarketApiError(
            "Verified manifest is missing"
        )

    manifest = json.loads(
        manifest_path.read_text(
            encoding="utf-8"
        )
    )

    digest = _sha256(snapshot)

    if manifest.get("database_sha256") != digest:
        raise MarketApiError(
            "Snapshot SHA-256 does not match verified manifest"
        )

    if manifest.get("required_market_date") != market_date:
        raise MarketApiError(
            "Snapshot market date differs from requested publication"
        )

    verification = (
        manifest.get("verification") or {}
    )

    if verification.get("status") != "complete":
        raise MarketApiError(
            "Snapshot verification status not complete: "
            f"{verification}"
        )

    uri = (
        f"{snapshot.resolve().as_uri()}?mode=ro"
    )

    with sqlite3.connect(
        uri,
        uri=True,
    ) as conn:

        if conn.execute(
            "PRAGMA quick_check"
        ).fetchone() != ("ok",):
            raise MarketApiError(
                "Snapshot SQLite integrity check failed"
            )

        records = conn.execute(
            "SELECT DISTINCT symbol, market_date "
            "FROM selection_results "
            "WHERE market_date <= ? "
            "ORDER BY symbol, market_date",
            (market_date,),
        ).fetchall()

    signals: dict[str, set[str]] = defaultdict(set)

    for symbol, signal_date in records:
        signals[str(symbol)].add(
            _day(str(signal_date))
        )

    if not signals:
        raise MarketApiError(
            "No published selection history to build a market API"
        )

    if bs is None:
        import baostock as bs

    login = bs.login()

    if login.error_code != "0":
        raise MarketApiError(
            f"BaoStock login failed: {login.error_msg}"
        )

    by_day: dict[
        str, list[dict[str, Any]]
    ] = defaultdict(list)

    missing: dict[str, str] = {}
    published = 0

    try:
        for symbol, signal_dates in sorted(
            signals.items()
        ):
            try:
                prices = _query_raw(
                    bs,
                    symbol,
                    min(signal_dates),
                    market_date,
                )

                if not prices:
                    missing[symbol] = (
                        "NO_RAW_PRICE_IN_RANGE"
                    )
                    continue

                available_dates = {
                    row["date"]
                    for row in prices
                }

                absent = sorted(
                    signal_dates - available_dates
                )

                if absent:
                    missing[symbol] = (
                        "MISSING_SIGNAL_DATE_RAW: "
                        + ",".join(absent[:5])
                    )
                    continue

                _dump(
                    output
                    / "stock"
                    / f"{symbol}.json",
                    {
                        "symbol": symbol,
                        "adjustment": "NONE",
                        "source": "baostock_adjustflag_3",
                        "data": prices,
                    },
                )

                for row in prices:
                    by_day[
                        row["date"]
                    ].append(row)

                published += 1

            except Exception as exc:
                missing[symbol] = (
                    f"{type(exc).__name__}: {exc}"
                )[:200]

    finally:
        bs.logout()

    if not published:
        raise MarketApiError(
            "No valid unadjusted price history was retrieved"
        )

    for day, rows in sorted(
        by_day.items()
    ):
        _dump(
            output / "eod" / f"{day}.json",
            {
                "date": day,
                "adjustment": "NONE",
                "source": "baostock_adjustflag_3",
                "count": len(rows),
                "data": sorted(
                    rows,
                    key=lambda x: x["symbol"],
                ),
            },
        )

    last_raw_date = max(by_day)

    if market_date not in by_day:
        raise MarketApiError(
            "Requested market date has no raw EOD prices"
        )

    metadata = {
        "schema_version": 1,
        "source": "baostock_adjustflag_3",
        "adjustment": "NONE",
        "scope": "verified_selection_symbols_only",
        "snapshot_market_date": market_date,
        "latest_raw_market_date": last_raw_date,
        "snapshot_sha256": digest,
        "generated_at_utc": datetime.now(
            timezone.utc
        ).isoformat(),
        "requested_symbols": len(signals),
        "published_symbols": published,
        "missing_symbols": len(missing),
        "data_status": (
            "COMPLETE"
            if not missing
            else "PARTIAL"
        ),
        "available_dates": sorted(by_day),
        "note": (
            "Raw OHLC only; prices are not "
            "evidence of executable fills, "
            "T+1, or dividends."
        ),
    }

    _dump(
        output / "meta.json",
        metadata,
    )

    _dump(
        output / "missing.json",
        {"missing": missing},
    )

    public_dir = output.parent.parent

    public_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    (
        public_dir / "index.html"
    ).write_text(
        "<!doctype html>"
        "<meta charset='utf-8'>"
        "<title>Sequoia-X Market Data</title>\n",
        encoding="utf-8",
    )

    return metadata


def main() -> int:

    parser = argparse.ArgumentParser(
        description=(
            "Export Sequoia-X "
            "unadjusted JSON market API"
        )
    )

    parser.add_argument(
        "--snapshot",
        type=Path,
        required=True,
    )

    parser.add_argument(
        "--manifest",
        type=Path,
        required=True,
    )

    parser.add_argument(
        "--output",
        type=Path,
        default=Path("public/api/v1"),
    )

    parser.add_argument(
        "--market-date",
        required=True,
    )

    args = parser.parse_args()

    try:
        result = export_market_api(
            args.snapshot,
            args.manifest,
            args.output,
            args.market_date,
        )

    except (
        MarketApiError,
        sqlite3.Error,
        OSError,
        ValueError,
    ) as exc:
        parser.exit(
            1,
            f"Market API export failed: {exc}\n",
        )

    print(
        json.dumps(
            result,
            ensure_ascii=False,
        )
    )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
