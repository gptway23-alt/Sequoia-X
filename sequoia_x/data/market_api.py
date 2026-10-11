
"""Full-universe Sequoia-X raw daily OHLC cache and public static API.

Core stock_daily remains BaoStock adjustflag=1 (back-adjusted).  This module
creates an independent stock_daily_raw table using adjustflag=3 (unadjusted).
No strategy history, SMTP secrets or the original GPT SQLite are published.
Full backfill is resumable; a PARTIAL state must never be presented as complete.
"""
from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import math
import os
import sqlite3
import time
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterator

SCHEMA = 2
SOURCE = "baostock_adjustflag_3"
FIELDS = "date,open,high,low,close,volume,amount"
CREATE_RAW = """CREATE TABLE IF NOT EXISTS stock_daily_raw (
  symbol TEXT NOT NULL, date TEXT NOT NULL,
  open REAL NOT NULL, high REAL NOT NULL, low REAL NOT NULL,
  close REAL NOT NULL, volume REAL NOT NULL, turnover REAL NOT NULL,
  PRIMARY KEY (symbol, date)
)"""
CREATE_SYNC = """CREATE TABLE IF NOT EXISTS stock_daily_raw_sync (
  symbol TEXT PRIMARY KEY, history_start TEXT NOT NULL,
  checked_through TEXT, checked_at_utc TEXT, status TEXT NOT NULL,
  error TEXT NOT NULL DEFAULT ''
)"""
UPSERT_RAW = """INSERT INTO stock_daily_raw
(symbol,date,open,high,low,close,volume,turnover) VALUES (?,?,?,?,?,?,?,?)
ON CONFLICT(symbol,date) DO UPDATE SET
open=excluded.open,high=excluded.high,low=excluded.low,
close=excluded.close,volume=excluded.volume,turnover=excluded.turnover"""
UPSERT_SYNC = """INSERT INTO stock_daily_raw_sync
(symbol,history_start,checked_through,checked_at_utc,status,error)
VALUES (?,?,?,?,?,?) ON CONFLICT(symbol) DO UPDATE SET
history_start=excluded.history_start, checked_through=excluded.checked_through,
checked_at_utc=excluded.checked_at_utc,status=excluded.status,error=excluded.error"""


class MarketApiError(RuntimeError):
    """Market data incomplete, unverified or unusable."""


def iso_day(value: object) -> str:
    s = str(value)
    try:
        if date.fromisoformat(s).isoformat() == s:
            return s
    except ValueError:
        pass
    raise MarketApiError(f"Invalid ISO date: {s!r}")


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for part in iter(lambda: fh.read(1024 * 1024), b""):
            digest.update(part)
    return digest.hexdigest()


def atomic_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_name(path.name + ".tmp")
    temp.write_text(
        json.dumps(
            value,
            ensure_ascii=False,
            separators=(",", ":"),
            allow_nan=False
        ) + "\n",
        encoding="utf-8",
    )
    os.replace(temp, path)


def bs_code(symbol: str) -> str:
    if len(symbol) != 6 or not symbol.isascii() or not symbol.isdigit():
        raise MarketApiError(f"Invalid symbol: {symbol!r}")
    if symbol.startswith(("0", "3")):
        return "sz." + symbol
    if symbol.startswith("6"):
        return "sh." + symbol

    # BaoStock's Beijing Stock Exchange support depends on data version.
    # Never silently replace BJ prices with a different exchange's data.
    if symbol.startswith(("4", "8", "9")):
        return "bj." + symbol
    raise MarketApiError(f"Unknown symbol exchange: {symbol}")


def parse_bar(
    symbol: str,
    fields: list[str],
    start: str,
    end: str
) -> tuple[Any, ...]:
    if len(fields) != 7:
        raise MarketApiError(
            f"{symbol}: wrong BaoStock row length {len(fields)}"
        )

    d = iso_day(fields[0])
    if not start <= d <= end:
        raise MarketApiError(f"{symbol}: unexpected market date {d}")

    try:
        o, h, lo, c, volume, turnover = [
            float(x) for x in fields[1:]
        ]
    except (ValueError, TypeError) as exc:
        raise MarketApiError(
            f"{symbol}: non-numeric price on {d}"
        ) from exc

    if not (
        all(
            math.isfinite(v)
            for v in (o, h, lo, c, volume, turnover)
        )
        and lo > 0
        and lo <= min(o, c)
        and max(o, c) <= h
        and volume >= 0
        and turnover >= 0
    ):
        raise MarketApiError(
            f"{symbol}: invalid raw OHLC on {d}"
        )

    return (symbol, d, o, h, lo, c, volume, turnover)


def fetch_raw(
    bs: Any,
    symbol: str,
    start: str,
    end: str
) -> list[tuple[Any, ...]]:
    code = bs_code(symbol)

    response = bs.query_history_k_data_plus(
        code,
        FIELDS,
        start_date=start,
        end_date=end,
        frequency="d",
        adjustflag="3",
    )

    if getattr(response, "error_code", "ERR") != "0":
        raise MarketApiError(
            f"{symbol}: BaoStock failed: "
            f"{getattr(response, 'error_msg', '')}"
        )

    bars = []
    seen = set()

    while response.next():
        bar = parse_bar(
            symbol,
            list(response.get_row_data()),
            start,
            end,
        )

        if bar[1] in seen:
            raise MarketApiError(
                f"{symbol}: duplicate raw OHLC {bar[1]}"
            )

        seen.add(bar[1])
        bars.append(bar)

    if response.error_code != "0":
        raise MarketApiError(
            f"{symbol}: stream failed: {response.error_msg}"
        )

    return bars


def open_db(db_path: Path) -> sqlite3.Connection:
    if not db_path.is_file() or db_path.stat().st_size == 0:
        raise MarketApiError(
            f"Sequoia-X database absent or empty: {db_path}"
        )

    conn = sqlite3.connect(db_path, timeout=45)

    if conn.execute(
        "PRAGMA quick_check"
    ).fetchone() != ("ok",):
        conn.close()
        raise MarketApiError(
            "Sequoia-X SQLite quick_check failed"
        )

    if not conn.execute(
        "SELECT 1 FROM sqlite_master "
        "WHERE type='table' AND name='stock_daily'"
    ).fetchone():
        conn.close()
        raise MarketApiError(
            "stock_daily table absent: "
            "cannot enumerate all existing market symbols"
        )

    return conn


def initialize_raw(conn: sqlite3.Connection) -> None:
    conn.execute(CREATE_RAW)
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_raw_date_symbol "
        "ON stock_daily_raw(date,symbol)"
    )
    conn.execute(CREATE_SYNC)
    conn.commit()


def get_universe(
    conn: sqlite3.Connection,
    asof: str
) -> dict[str, str]:
    symbols = {}

    for code, first_date in conn.execute(
        "SELECT symbol, MIN(date) FROM stock_daily WHERE date <= ? "
        "GROUP BY symbol ORDER BY symbol",
        (asof,),
    ):
        s = str(code)
        iso_day(first_date)

        # Don't silently drop unsupported exchanges:
        # record sync error later.
        symbols[s] = str(first_date)

    if not symbols:
        raise MarketApiError(
            "No pre-existing full-universe stock_daily symbols"
        )

    return symbols


def _connect_baostock(bs: Any) -> None:
    resp = bs.login()

    if getattr(resp, "error_code", "ERR") != "0":
        raise MarketApiError(
            f"BaoStock login failed: "
            f"{getattr(resp, 'error_msg', '')}"
        )


def sync_full_universe(
    db_path: Path,
    asof: str,
    *,
    max_symbols: int = 0,
    budget_seconds: int = 5400,
    bs: Any | None = None,
) -> dict[str, Any]:
    """Sync all original database symbols, not just selected stocks.

    max_symbols=0 means no item cap. A wall-clock budget prevents
    losing the entire Actions job. Successful rows commit per symbol.
    """

    asof = iso_day(asof)

    if max_symbols < 0 or budget_seconds < 1:
        raise MarketApiError("Invalid sync limits")

    if date.fromisoformat(asof) > datetime.now(timezone.utc).date():
        raise MarketApiError(
            "Cannot fetch future market dates"
        )

    with open_db(db_path) as conn:
        initialize_raw(conn)
        universe = get_universe(conn, asof)

        previous = {
            row[0]: row[1:]
            for row in conn.execute(
                "SELECT symbol,history_start,checked_through,status "
                "FROM stock_daily_raw_sync"
            )
        }

        due = []

        for symbol, history_start in universe.items():
            p = previous.get(symbol)

            if (
                p
                and p[0] <= history_start
                and p[1] == asof
                and p[2] == "OK"
            ):
                continue

            # Previously failed symbols have priority over
            # already-checked symbols.
            due.append((symbol, history_start, p))

        due.sort(
            key=lambda x: (
                1 if x[2] and x[2][2] in {"OK", "NO_BARS"} else 0,
                x[0],
            )
        )

        if bs is None:
            import baostock as bs

        _connect_baostock(bs)

        scanned, failed, added = 0, 0, 0
        started = time.monotonic()

        try:
            for symbol, historical_start, p in due:
                if (
                    (max_symbols and scanned >= max_symbols)
                    or (time.monotonic() - started > budget_seconds)
                ):
                    break

                scanned += 1

                # Correct recent corporate-action quotes
                # within a configurable 10-day window.
                if (
                    p
                    and p[0] <= historical_start
                    and p[1]
                    and p[2] == "OK"
                ):
                    tail = (
                        date.fromisoformat(p[1])
                        - timedelta(days=10)
                    )

                    fetch_from = max(
                        historical_start,
                        tail.isoformat()
                    )
                else:
                    fetch_from = historical_start

                error = ""
                bars = []
                success = False

                for retry in range(3):
                    try:
                        bars = fetch_raw(
                            bs, symbol, fetch_from, asof
                        )
                        success = True
                        error = ""
                        break

                    except Exception as exc:
                        error = (
                            f"{type(exc).__name__}: {exc}"
                        )[:300]

                        if retry < 2:
                            try:
                                bs.logout()
                            except Exception:
                                pass

                            time.sleep(
                                0.25 * (retry + 1)
                            )

                            try:
                                _connect_baostock(bs)
                            except Exception as login_err:
                                error = (
                                    f"Login: {login_err}"
                                )[:300]

                if not success:
                    failed += 1

                    state = (
                        symbol,
                        historical_start,
                        p[1] if p else None,
                        utc_now(),
                        "ERROR",
                        error,
                    )

                else:
                    # Suspensions can legitimately have no raw bar.
                    # Never substitute adjusted close as raw close.
                    with conn:
                        conn.executemany(
                            UPSERT_RAW, bars
                        )

                    added += len(bars)

                    expected = {
                        r[0]
                        for r in conn.execute(
                            "SELECT date FROM stock_daily "
                            "WHERE symbol=? AND date BETWEEN ? AND ?",
                            (
                                symbol,
                                historical_start,
                                asof,
                            ),
                        )
                    }

                    received = {
                        r[0]
                        for r in conn.execute(
                            "SELECT date FROM stock_daily_raw "
                            "WHERE symbol=? AND date BETWEEN ? AND ?",
                            (
                                symbol,
                                historical_start,
                                asof,
                            ),
                        )
                    }

                    missing_dates = (
                        expected - received
                    )

                    status = (
                        "GAP" if missing_dates
                        else ("OK" if received else "NO_BARS")
                    )

                    msg = (
                        f"{len(missing_dates)} raw dates absent; "
                        f"first={min(missing_dates)}"
                        if missing_dates
                        else ""
                    )

                    # GAP is not equivalent to COMPLETE.
                    state = (
                        symbol,
                        historical_start,
                        asof,
                        utc_now(),
                        status,
                        msg,
                    )

                with conn:
                    conn.execute(
                        UPSERT_SYNC,
                        state,
                    )

                if scanned % 100 == 0:
                    print(
                        f"raw-synced {scanned}/{len(due)}; "
                        f"failed={failed}; last={symbol}",
                        flush=True,
                    )

        finally:
            try:
                bs.logout()
            except Exception:
                pass

        stats = sync_stats(
            conn,
            universe,
            asof,
        )

        stats.update({
            "attempted_this_run": scanned,
            "failed_this_run": failed,
            "fetched_rows_this_run": added,
            "asof": asof,
            "updated_at_utc": utc_now(),
        })

        print(
            json.dumps(
                stats,
                ensure_ascii=False,
            ),
            flush=True,
        )

        return stats


def sync_stats(
    conn: sqlite3.Connection,
    universe: dict[str, str],
    asof: str,
) -> dict[str, Any]:

    ok = conn.execute(
        "SELECT COUNT(*) FROM stock_daily_raw_sync "
        "WHERE checked_through=? AND status='OK'",
        (asof,),
    ).fetchone()[0]

    # Cached historical delisted symbols are preserved.
    raw_symbols, raw_bars, lo, hi = conn.execute(
        "SELECT COUNT(DISTINCT symbol), COUNT(*), "
        "MIN(date), MAX(date) "
        "FROM stock_daily_raw WHERE date<=?",
        (asof,),
    ).fetchone()

    return {
        "source_symbols": len(universe),
        "fully_checked_symbols": ok,
        "pending_symbols": max(
            0, len(universe) - ok
        ),
        "raw_symbols": raw_symbols,
        "raw_rows": raw_bars,
        "earliest_raw_date": lo,
        "latest_raw_date": hi,
        "data_status": (
            "COMPLETE"
            if ok == len(universe)
            else "PARTIAL"
        ),
    }


def iter_rows(
    conn: sqlite3.Connection,
    order: str,
    asof: str,
) -> Iterator[tuple[Any, ...]]:

    if order == "symbol":
        sort = "symbol,date"
    elif order == "date":
        sort = "date,symbol"
    else:
        raise MarketApiError(
            "Invalid SQLite export order"
        )

    return iter(
        conn.execute(
            "SELECT symbol,date,open,high,low,close,volume,turnover "
            f"FROM stock_daily_raw WHERE date<=? ORDER BY {sort}",
            (asof,),
        )
    )


def entry(
    row: tuple[Any, ...]
) -> dict[str, Any]:

    symbol, d, op, hi, lo, cl, vol, amt = row

    return {
        "symbol": symbol,
        "date": d,
        "open": op,
        "high": hi,
        "low": lo,
        "close": cl,
        "volume": vol,
        "turnover": amt,
    }


def verify_snapshot(
    snapshot: Path,
    manifest_file: Path,
    asof: str,
) -> str:

    if (
        not snapshot.is_file()
        or not manifest_file.is_file()
    ):
        raise MarketApiError(
            "Verified GPT snapshot/manifest missing"
        )

    manifest = json.loads(
        manifest_file.read_text(
            encoding="utf-8"
        )
    )

    digest = sha256(snapshot)

    if manifest.get(
        "database_sha256"
    ) != digest:
        raise MarketApiError(
            "Verified GPT snapshot SHA-256 mismatch"
        )

    if (
        manifest.get("required_market_date") != asof
        or (
            manifest.get("verification") or {}
        ).get("status") != "complete"
    ):
        raise MarketApiError(
            "Snapshot not verified for requested trade date"
        )

    return digest


def publish_full_history(
    db_path: Path,
    snapshot: Path,
    manifest: Path,
    output: Path,
    asof: str,
    *,
    limit_mb: int = 900,
) -> dict[str, Any]:
    """Publish only unadjusted OHLC, across all cached symbols/dates.

    Output includes stock JSON, date JSON and yearly JSONL.gz.
    An incomplete sync remains explicitly PARTIAL.
    """

    asof = iso_day(asof)

    fingerprint = verify_snapshot(
        snapshot,
        manifest,
        asof,
    )

    with open_db(db_path) as conn:
        initialize_raw(conn)
        universe = get_universe(conn, asof)
        stats = sync_stats(
            conn,
            universe,
            asof,
        )

        if stats["raw_rows"] == 0:
            raise MarketApiError(
                "Unadjusted database empty; "
                "sync before publishing"
            )

        # Keep historical delisted symbols.
        if (
            stats["raw_symbols"]
            > stats["source_symbols"]
        ):
            print(
                "WARNING: historic delisted symbols "
                "remain in raw cache"
            )

        import shutil

        if output.exists():
            shutil.rmtree(output)

        output.mkdir(parents=True)

        stock_index = []
        current_code = None
        items: list[dict[str, Any]] = []

        def flush_stock() -> None:
            if current_code is None:
                return

            atomic_json(
                output
                / "stock"
                / f"{current_code}.json",
                {
                    "symbol": current_code,
                    "adjustment": "NONE",
                    "source": SOURCE,
                    "data": items,
                },
            )

            stock_index.append({
                "symbol": current_code,
                "first_date": items[0]["date"],
                "last_date": items[-1]["date"],
                "bars": len(items),
            })

        for row in iter_rows(
            conn,
            "symbol",
            asof,
        ):
            code = row[0]

            if (
                current_code is not None
                and code != current_code
            ):
                flush_stock()
                items = []

            current_code = code
            items.append(entry(row))

        flush_stock()

        atomic_json(
            output / "symbols.json",
            {
                "scope": "all_local_market_symbols",
                "stocks": stock_index,
            },
        )

        current_day, day_items = None, []
        year_name, year_stream = None, None

        year_stats: dict[
            str, dict[str, Any]
        ] = defaultdict(
            lambda: {"rows": 0}
        )

        bundles = []

        try:
            for row in iter_rows(
                conn,
                "date",
                asof,
            ):
                item = entry(row)
                d = item["date"]

                if current_day and d != current_day:
                    atomic_json(
                        output
                        / "eod"
                        / f"{current_day}.json",
                        {
                            "date": current_day,
                            "adjustment": "NONE",
                            "source": SOURCE,
                            "count": len(day_items),
                            "data": day_items,
                        },
                    )
                    day_items = []

                current_day = d
                day_items.append(item)

                year = d[:4]

                if year_name != year:
                    if year_stream:
                        year_stream.close()

                        file = (
                            output
                            / "download"
                            / f"raw-{year_name}.jsonl.gz"
                        )

                        bundles.append({
                            "year": year_name,
                            "path": (
                                f"download/{file.name}"
                            ),
                            "bytes": file.stat().st_size,
                            "sha256": sha256(file),
                            "rows": (
                                year_stats[
                                    year_name
                                ]["rows"]
                            ),
                        })

                    target = (
                        output
                        / "download"
                        / f"raw-{year}.jsonl.gz"
                    )

                    target.parent.mkdir(
                        parents=True,
                        exist_ok=True,
                    )

                    year_stream = gzip.open(
                        target,
                        "wt",
                        encoding="utf-8",
                        compresslevel=6,
                    )

                    year_name = year

                year_stream.write(
                    json.dumps(
                        item,
                        separators=(",", ":"),
                        allow_nan=False,
                    ) + "\n"
                )

                year_stats[year]["rows"] += 1

            if current_day:
                atomic_json(
                    output
                    / "eod"
                    / f"{current_day}.json",
                    {
                        "date": current_day,
                        "adjustment": "NONE",
                        "source": SOURCE,
                        "count": len(day_items),
                        "data": day_items,
                    },
                )

        finally:
            if year_stream:
                year_stream.close()

        if year_name:
            file = (
                output
                / "download"
                / f"raw-{year_name}.jsonl.gz"
            )

            bundles.append({
                "year": year_name,
                "path": (
                    f"download/{file.name}"
                ),
                "bytes": file.stat().st_size,
                "sha256": sha256(file),
                "rows": (
                    year_stats[year_name]["rows"]
                ),
            })

        status_rows = {
            row[0]: (
                row[1],
                row[2],
                row[3],
            )
            for row in conn.execute(
                "SELECT symbol,status,error,checked_through "
                "FROM stock_daily_raw_sync"
            )
        }

        problems = []

        for code in sorted(universe):
            status, error, checked = (
                status_rows.get(
                    code,
                    ("NOT_STARTED", "", None),
                )
            )

            if (
                checked != asof
                or status != "OK"
            ):
                problems.append({
                    "symbol": code,
                    "status": status,
                    "checked_through": checked,
                    "error": error,
                })

        atomic_json(
            output / "missing.json",
            {
                "asof": asof,
                "pending_or_failed": problems,
                "note": (
                    "Suspensions do not necessarily "
                    "generate rows"
                ),
            },
        )

        meta = {
            "schema_version": SCHEMA,
            "source": SOURCE,
            "adjustment": "NONE",
            "scope": "full_local_stock_daily_universe",
            "snapshot_market_date": asof,
            "snapshot_sha256": fingerprint,
            "generated_at_utc": utc_now(),
            "requested_symbols": (
                stats["source_symbols"]
            ),
            "published_symbols": (
                stats["raw_symbols"]
            ),
            "fully_checked_symbols": (
                stats["fully_checked_symbols"]
            ),
            "pending_symbols": (
                stats["pending_symbols"]
            ),
            "raw_rows": stats["raw_rows"],
            "earliest_raw_date": (
                stats["earliest_raw_date"]
            ),
            "latest_raw_market_date": (
                stats["latest_raw_date"]
            ),
            "available_dates": [
                d[0]
                for d in conn.execute(
                    "SELECT DISTINCT date "
                    "FROM stock_daily_raw "
                    "WHERE date<=? ORDER BY date",
                    (asof,),
                )
            ],
            "data_status": (
                stats["data_status"]
            ),
            "download_bundles": bundles,
            "note": (
                "Independent raw OHLC cache; "
                "excludes private signals/transactions. "
                "Incomplete batches remain PARTIAL. "
                "No future T+1 fills implied."
            ),
        }

        atomic_json(
            output / "meta.json",
            meta,
        )

        public_root = output.parent.parent

        public_root.mkdir(
            exist_ok=True
        )

        (
            public_root / "index.html"
        ).write_text(
            "<!doctype html>"
            "<meta charset='utf-8'>"
            "<title>Sequoia-X Raw Market API</title>",
            encoding="utf-8",
        )

        size = sum(
            f.stat().st_size
            for f in public_root.rglob("*")
            if f.is_file()
        )

        meta["published_site_bytes"] = size

        atomic_json(
            output / "meta.json",
            meta,
        )

        size = sum(
            f.stat().st_size
            for f in public_root.rglob("*")
            if f.is_file()
        )

        if size > limit_mb * 1024 * 1024:
            raise MarketApiError(
                f"Pages site {size} bytes exceeds "
                f"configured {limit_mb} MB; "
                "use object storage instead"
            )

        print(
            json.dumps(
                meta,
                ensure_ascii=False,
            ),
            flush=True,
        )

        return meta


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Full Sequoia-X unadjusted OHLC API"
        )
    )

    parser.add_argument(
        "--mode",
        required=True,
        choices=["sync", "publish"],
    )
    parser.add_argument(
        "--db",
        type=Path,
        default=Path("data/sequoia_v2.db"),
    )
    parser.add_argument(
        "--market-date",
        required=True,
    )
    parser.add_argument(
        "--snapshot",
        type=Path,
    )
    parser.add_argument(
        "--manifest",
        type=Path,
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("public/api/v1"),
    )
    parser.add_argument(
        "--max-symbols",
        type=int,
        default=0,
    )
    parser.add_argument(
        "--time-budget-seconds",
        type=int,
        default=5400,
    )
    parser.add_argument(
        "--pages-limit-mb",
        type=int,
        default=900,
    )

    args = parser.parse_args()

    try:
        if args.mode == "sync":
            result = sync_full_universe(
                args.db,
                args.market_date,
                max_symbols=args.max_symbols,
                budget_seconds=args.time_budget_seconds,
            )

        else:
            if (
                not args.snapshot
                or not args.manifest
            ):
                raise MarketApiError(
                    "--snapshot and --manifest "
                    "required for public export"
                )

            result = publish_full_history(
                args.db,
                args.snapshot,
                args.manifest,
                args.output,
                args.market_date,
                limit_mb=args.pages_limit_mb,
            )

    except (
        MarketApiError,
        sqlite3.Error,
        OSError,
        ValueError,
    ) as exc:
        parser.exit(
            1,
            f"Sequoia-X market API failed: {exc}\n",
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
