"""Create a safe, self-contained SQLite snapshot for analysis by GPT.

The exporter copies published ``stock_daily`` data and, when both are present,
validated ``selection_runs`` and ``selection_results``. Staging data and every
other source table are excluded by construction.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import sqlite3
import tempfile
from contextlib import closing
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Iterator, Sequence


DATABASE_FILENAME = "sequoia-x-verified.sqlite3"
MANIFEST_FILENAME = "manifest.json"
README_FILENAME = "README.txt"
EXPORT_SCHEMA_VERSION = 1

_EXPORTED_COLUMNS = (
    "symbol",
    "date",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "turnover",
)

_SELECTION_RUN_COLUMNS = (
    "market_date",
    "recorded_at",
    "expected_symbols",
    "verified_symbols",
    "verified_symbols_json",
    "coverage",
    "strategies_json",
    "result_rows",
)

_SELECTION_RESULT_COLUMNS = (
    "market_date",
    "strategy",
    "symbol",
)

_CREATE_EXPORTED_TABLE_SQL = """
CREATE TABLE stock_daily (
    symbol   TEXT NOT NULL,
    date     TEXT NOT NULL,
    open     REAL,
    high     REAL,
    low      REAL,
    close    REAL,
    volume   REAL,
    turnover REAL,
    PRIMARY KEY (symbol, date)
)
"""

_CREATE_SELECTION_TABLES_SQL = """
CREATE TABLE selection_runs (
    market_date      TEXT PRIMARY KEY,
    recorded_at      TEXT NOT NULL,
    expected_symbols INTEGER NOT NULL,
    verified_symbols INTEGER NOT NULL,
    verified_symbols_json TEXT NOT NULL,
    coverage         REAL NOT NULL,
    strategies_json  TEXT NOT NULL,
    result_rows      INTEGER NOT NULL
);
CREATE TABLE selection_results (
    market_date TEXT NOT NULL,
    strategy    TEXT NOT NULL,
    symbol      TEXT NOT NULL,
    PRIMARY KEY (market_date, strategy, symbol),
    FOREIGN KEY (market_date) REFERENCES selection_runs (market_date) ON DELETE CASCADE
);
CREATE INDEX idx_selection_results_symbol_date
ON selection_results (symbol, market_date);
"""

_README_TEXT = """Sequoia-X GPT 数据快照

文件：sequoia-x-verified.sqlite3
完成标志：manifest.json

内容范围
1. 快照始终包含已经发布的 stock_daily 行情表。
2. 如果源库已有完整的 selection_runs 和 selection_results，则同时包含选股历史。
3. 两张选股历史表必须成套存在，否则导出失败。
4. stock_daily_staging、交易订单、资金、持仓、NAV 和源库中的其他表均未复制。

读取要求
1. 先核对 manifest.json 中的 database_sha256，再读取数据库。
2. 以 SQLite 只读模式打开，例如：file:sequoia-x-verified.sqlite3?mode=ro。
3. latest_market_date 是快照内真实的最新行情日期，不要把更早数据描述成当日行情。
4. 行情表是源库已发布行的完整副本；manifest 不宣称每个历史日期都覆盖全市场。
5. 选股历史只从部署 selection_runs 功能后的首个成功交易日开始。

示例查询
SELECT * FROM stock_daily WHERE symbol = '000001' ORDER BY date DESC LIMIT 30;
SELECT date, COUNT(DISTINCT symbol) AS symbols FROM stock_daily GROUP BY date ORDER BY date DESC;
SELECT * FROM selection_runs ORDER BY market_date DESC;
SELECT * FROM selection_results WHERE market_date = '2026-10-09' ORDER BY strategy, symbol;
"""


class GptExportError(RuntimeError):
    """The requested snapshot could not be proven complete and safe."""


@dataclass(frozen=True)
class GptExportResult:
    """Paths and evidence for one completed export."""

    database_path: Path
    manifest_path: Path
    readme_path: Path
    row_count: int
    symbol_count: int
    latest_market_date: str
    database_sha256: str


def _readonly_connection(path: Path) -> sqlite3.Connection:
    uri = f"{path.resolve().as_uri()}?mode=ro"
    connection = sqlite3.connect(uri, uri=True, timeout=30)
    connection.execute("PRAGMA query_only = ON")
    return connection


def _quick_check(connection: sqlite3.Connection, label: str) -> None:
    result = connection.execute("PRAGMA quick_check").fetchone()
    if not result or result[0] != "ok":
        raise GptExportError(f"{label} SQLite quick_check failed: {result}")


def _require_source_schema(connection: sqlite3.Connection) -> None:
    table = connection.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'stock_daily'"
    ).fetchone()
    if table is None:
        raise GptExportError("source database has no stock_daily table")

    columns = {
        str(row[1])
        for row in connection.execute("PRAGMA table_info(stock_daily)").fetchall()
    }
    missing = sorted(set(_EXPORTED_COLUMNS) - columns)
    if missing:
        raise GptExportError(f"stock_daily is missing required columns: {', '.join(missing)}")


def _selection_tables_available(connection: sqlite3.Connection) -> bool:
    source_tables = {
        str(row[0])
        for row in connection.execute(
            """
            SELECT name
            FROM sqlite_master
            WHERE type = 'table' AND name IN ('selection_runs', 'selection_results')
            """
        ).fetchall()
    }
    if not source_tables:
        return False
    required_tables = {"selection_runs", "selection_results"}
    if source_tables != required_tables:
        missing = ", ".join(sorted(required_tables - source_tables))
        raise GptExportError(f"selection history is incomplete; missing table: {missing}")

    required_columns = {
        "selection_runs": set(_SELECTION_RUN_COLUMNS),
        "selection_results": set(_SELECTION_RESULT_COLUMNS),
    }
    for table_name, expected_columns in required_columns.items():
        actual_columns = {
            str(row[1])
            for row in connection.execute(f"PRAGMA table_info({table_name})").fetchall()
        }
        missing_columns = sorted(expected_columns - actual_columns)
        if missing_columns:
            raise GptExportError(
                f"{table_name} is missing required columns: {', '.join(missing_columns)}"
            )
    return True


def _strict_market_date(value: object, label: str) -> str:
    text = str(value)
    try:
        parsed = date.fromisoformat(text)
    except ValueError as exc:
        raise GptExportError(f"{label} must be YYYY-MM-DD: {text!r}") from exc
    if parsed.isoformat() != text:
        raise GptExportError(f"{label} must be YYYY-MM-DD: {text!r}")
    return text


def _non_negative_integer(value: object, label: str) -> int:
    text = str(value)
    if not text.isascii() or not text.isdigit():
        raise GptExportError(f"{label} must be a non-negative integer: {value!r}")
    try:
        return int(text)
    except ValueError as exc:
        raise GptExportError(
            f"{label} must be a non-negative integer: {value!r}"
        ) from exc


def _is_ascii_stock_symbol(value: object) -> bool:
    symbol = str(value)
    return len(symbol) == 6 and symbol.isascii() and symbol.isdigit()


def _verified_universe_for_run(
    connection: sqlite3.Connection,
    market_date: str,
    raw_json: object,
    claimed_count: int,
) -> frozenset[str]:
    try:
        decoded = json.loads(str(raw_json))
    except (TypeError, json.JSONDecodeError) as exc:
        raise GptExportError(
            f"selection run {market_date} verified_symbols_json is invalid"
        ) from exc
    if not isinstance(decoded, list):
        raise GptExportError(
            f"selection run {market_date} verified_symbols_json must be a list"
        )
    if any(
        not isinstance(symbol, str)
        or not _is_ascii_stock_symbol(symbol)
        for symbol in decoded
    ):
        raise GptExportError(
            f"selection run {market_date} has an invalid verified stock symbol"
        )
    if decoded != sorted(decoded) or len(decoded) != len(set(decoded)):
        raise GptExportError(
            f"selection run {market_date} verified symbols must be sorted and unique"
        )
    if len(decoded) != claimed_count:
        raise GptExportError(
            f"selection run {market_date} verified_symbols={claimed_count} "
            f"but verified_symbols_json has {len(decoded)} symbols"
        )

    valid_symbols: set[str] = set()
    for offset in range(0, len(decoded), 500):
        chunk = decoded[offset : offset + 500]
        placeholders = ", ".join("?" for _ in chunk)
        rows = connection.execute(
            f"""
            SELECT symbol, open, high, low, close, volume, turnover
            FROM stock_daily
            WHERE date = ? AND symbol IN ({placeholders})
            """,
            (market_date, *chunk),
        ).fetchall()
        valid_symbols.update(
            str(row[0])
            for row in rows
            if _is_valid_market_row(tuple(row))
        )
    missing = sorted(set(decoded) - valid_symbols)
    if missing:
        preview = ", ".join(missing[:10])
        raise GptExportError(
            f"selection run {market_date} has {len(missing)} verified symbol(s) "
            f"without valid same-day published prices: {preview}"
        )
    return frozenset(decoded)


def _validate_selection_history(
    connection: sqlite3.Connection,
    minimum_coverage: float,
) -> tuple[bool, dict[str, dict[str, object]], dict[str, object]]:
    """Validate both optional selection tables and return per-day evidence."""

    available = _selection_tables_available(connection)
    empty_stats: dict[str, object] = {
        "available": False,
        "run_count": 0,
        "result_count": 0,
        "first_market_date": None,
        "latest_market_date": None,
    }
    if not available:
        return False, {}, empty_stats

    orphan = connection.execute(
        """
        SELECT result.market_date
        FROM selection_results AS result
        LEFT JOIN selection_runs AS run ON run.market_date = result.market_date
        WHERE run.market_date IS NULL
        LIMIT 1
        """
    ).fetchone()
    if orphan is not None:
        raise GptExportError(
            f"selection_results has no parent run for market date {orphan[0]}"
        )

    result_counts = {
        str(market_date): int(row_count)
        for market_date, row_count in connection.execute(
            """
            SELECT market_date, COUNT(*)
            FROM selection_results
            GROUP BY market_date
            """
        ).fetchall()
    }
    run_rows = connection.execute(
        """
        SELECT market_date, recorded_at, expected_symbols, verified_symbols,
               verified_symbols_json, coverage, strategies_json, result_rows
        FROM selection_runs
        ORDER BY market_date
        """
    ).fetchall()

    evidence: dict[str, dict[str, object]] = {}
    allowed_strategies: dict[str, frozenset[str]] = {}
    verified_universes: dict[str, frozenset[str]] = {}
    for row in run_rows:
        market_date = _strict_market_date(row[0], "selection_runs.market_date")
        recorded_at = str(row[1]).strip()
        if not recorded_at:
            raise GptExportError(f"selection run {market_date} has an empty recorded_at")
        try:
            datetime.fromisoformat(recorded_at.replace("Z", "+00:00"))
        except ValueError as exc:
            raise GptExportError(
                f"selection run {market_date} has invalid recorded_at: {recorded_at!r}"
            ) from exc
        expected_count = _non_negative_integer(
            row[2], f"selection run {market_date} expected_symbols"
        )
        verified_count = _non_negative_integer(
            row[3], f"selection run {market_date} verified_symbols"
        )
        result_count = _non_negative_integer(
            row[7], f"selection run {market_date} result_rows"
        )
        if expected_count <= 0:
            raise GptExportError(f"selection run {market_date} expected_symbols must be positive")
        if verified_count > expected_count:
            raise GptExportError(
                f"selection run {market_date} verified_symbols exceeds expected_symbols"
            )
        if verified_count <= 0:
            raise GptExportError(
                f"selection run {market_date} verified_symbols must be positive"
            )
        try:
            coverage = float(row[5])
        except (TypeError, ValueError) as exc:
            raise GptExportError(
                f"selection run {market_date} has invalid coverage: {row[5]!r}"
            ) from exc
        calculated_coverage = verified_count / expected_count
        if not math.isfinite(coverage) or not 0 <= coverage <= 1:
            raise GptExportError(f"selection run {market_date} coverage is outside [0, 1]")
        if not math.isclose(coverage, calculated_coverage, rel_tol=0, abs_tol=1e-12):
            raise GptExportError(
                f"selection run {market_date} coverage does not match verified/expected"
            )
        if coverage < minimum_coverage:
            raise GptExportError(
                f"selection run {market_date} coverage gate failed: "
                f"{coverage:.2%} < {minimum_coverage:.2%}"
            )
        actual_results = result_counts.get(market_date, 0)
        if result_count != actual_results:
            raise GptExportError(
                f"selection run {market_date} result_rows={result_count} "
                f"but selection_results has {actual_results} rows"
            )
        try:
            strategies = json.loads(str(row[6]))
        except (TypeError, json.JSONDecodeError) as exc:
            raise GptExportError(
                f"selection run {market_date} strategies_json is invalid"
            ) from exc
        if (
            not isinstance(strategies, list)
            or any(not isinstance(strategy, str) or not strategy for strategy in strategies)
            or len(strategies) != len(set(strategies))
        ):
            raise GptExportError(
                f"selection run {market_date} strategies_json must be unique non-empty strings"
            )

        verified_universe = _verified_universe_for_run(
            connection,
            market_date,
            row[4],
            verified_count,
        )
        verified_universes[market_date] = verified_universe
        allowed_strategies[market_date] = frozenset(strategies)
        evidence[market_date] = {
            "status": "complete",
            "market_date": market_date,
            "recorded_at": recorded_at,
            "verified_symbol_count": verified_count,
            "expected_symbol_count": expected_count,
            "coverage": coverage,
            "minimum_coverage": minimum_coverage,
            "result_count": result_count,
            "verified_symbols_sha256": hashlib.sha256(
                "\n".join(sorted(verified_universe)).encode("ascii")
            ).hexdigest(),
        }

    for row in connection.execute(
        """
        SELECT result.market_date, result.strategy, result.symbol,
               daily.symbol, daily.open, daily.high, daily.low, daily.close,
               daily.volume, daily.turnover
        FROM selection_results AS result
        LEFT JOIN stock_daily AS daily
          ON daily.symbol = result.symbol AND daily.date = result.market_date
        ORDER BY result.market_date, result.strategy, result.symbol
        """
    ):
        market_date = str(row[0])
        strategy = str(row[1])
        symbol = str(row[2])
        if not _is_ascii_stock_symbol(symbol):
            raise GptExportError(
                f"selection result has invalid stock symbol on {market_date}: {row[2]!r}"
            )
        if symbol not in verified_universes.get(market_date, frozenset()):
            raise GptExportError(
                f"selection result {market_date}/{strategy}/{symbol} "
                "is not in verified_symbols_json"
            )
        same_day_market_row = (row[3], *row[4:])
        if row[3] is None or not _is_valid_market_row(same_day_market_row):
            raise GptExportError(
                f"selection result {market_date}/{strategy}/{symbol} has no valid same-day price"
            )
        if not strategy or strategy not in allowed_strategies.get(market_date, frozenset()):
            raise GptExportError(
                f"selection result {market_date}/{strategy}/{symbol} "
                "is not listed in strategies_json"
            )

    dates = sorted(evidence)
    stats: dict[str, object] = {
        "available": True,
        "run_count": len(evidence),
        "result_count": sum(result_counts.values()),
        "first_market_date": dates[0] if dates else None,
        "latest_market_date": dates[-1] if dates else None,
    }
    return True, evidence, stats


def _is_valid_market_row(row: tuple[object, ...]) -> bool:
    try:
        values = tuple(float(value) for value in row[1:])
    except (TypeError, ValueError):
        return False
    open_price, high, low, close, volume, turnover = values
    if not all(math.isfinite(value) for value in values):
        return False
    if min(open_price, high, low, close) <= 0:
        return False
    if high < max(open_price, low, close):
        return False
    if low > min(open_price, high, close):
        return False
    return volume >= 0 and turnover >= 0


def _is_valid_published_row(row: tuple[object, ...]) -> bool:
    if len(row) != len(_EXPORTED_COLUMNS):
        return False
    symbol = str(row[0])
    if not _is_ascii_stock_symbol(symbol):
        return False
    try:
        market_date = date.fromisoformat(str(row[1]))
    except ValueError:
        return False
    if market_date.isoformat() != str(row[1]):
        return False
    return _is_valid_market_row((symbol, *row[2:]))


def _iter_published_rows(
    connection: sqlite3.Connection,
    batch_size: int = 10_000,
) -> Iterator[list[tuple[object, ...]]]:
    cursor = connection.execute(
        """
        SELECT symbol, date, open, high, low, close, volume, turnover
        FROM stock_daily
        ORDER BY symbol, date
        """
    )
    while True:
        rows = cursor.fetchmany(batch_size)
        if not rows:
            return
        yield [tuple(row) for row in rows]


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _fsync(path: Path) -> None:
    # Windows requires a writable file descriptor for FlushFileBuffers.
    with path.open("r+b") as stream:
        os.fsync(stream.fileno())


def _atomic_write_text(path: Path, text: str) -> None:
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.",
        suffix=".tmp",
        dir=path.parent,
    )
    temporary_path = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8", newline="\n") as stream:
            stream.write(text)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary_path, path)
    finally:
        temporary_path.unlink(missing_ok=True)


def export_verified_stock_database(
    source_db: str | Path,
    output_dir: str | Path,
    *,
    required_market_date: str | None = None,
    minimum_coverage: float = 0.98,
) -> GptExportResult:
    """Export every published source row and any validated selection history.

    When ``required_market_date`` is supplied, that date must have a complete,
    internally consistent ``selection_runs`` record and all of its selected
    symbols must have valid same-day rows in ``stock_daily``. The source is
    opened read-only. The manifest is replaced last and therefore acts as the
    export's completion marker.
    """

    source_path = Path(source_db)
    destination_dir = Path(output_dir)
    destination_dir.mkdir(parents=True, exist_ok=True)
    database_path = destination_dir / DATABASE_FILENAME
    manifest_path = destination_dir / MANIFEST_FILENAME
    readme_path = destination_dir / README_FILENAME
    # manifest.json is the completion marker. Remove any prior marker before
    # starting so a failed refresh cannot be mistaken for a current snapshot.
    manifest_path.unlink(missing_ok=True)
    if required_market_date is not None:
        required_market_date = _strict_market_date(
            required_market_date, "required_market_date"
        )
    if not source_path.is_file() or source_path.stat().st_size <= 0:
        raise GptExportError(f"source database is missing or empty: {source_path}")
    try:
        minimum_coverage = float(minimum_coverage)
    except (TypeError, ValueError) as exc:
        raise GptExportError(
            "minimum_coverage must be a finite number in (0, 1]"
        ) from exc
    if not math.isfinite(minimum_coverage) or not 0 < minimum_coverage <= 1:
        raise GptExportError("minimum_coverage must be a finite number in (0, 1]")
    if source_path.resolve() == database_path.resolve():
        raise GptExportError("source database and exported database must be different files")
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{DATABASE_FILENAME}.",
        suffix=".tmp",
        dir=destination_dir,
    )
    os.close(descriptor)
    temporary_database = Path(temporary_name)

    try:
        with closing(_readonly_connection(source_path)) as source:
            source.execute("BEGIN")
            _quick_check(source, "source")
            _require_source_schema(source)
            (
                selection_history_available,
                selection_evidence,
                selection_stats,
            ) = _validate_selection_history(source, minimum_coverage)
            if required_market_date is not None and required_market_date not in selection_evidence:
                raise GptExportError(
                    f"selection_runs has no complete record for required market date "
                    f"{required_market_date}"
                )

            source_stats = source.execute(
                """
                SELECT COUNT(*), COUNT(DISTINCT symbol), MIN(date), MAX(date)
                FROM stock_daily
                """
            ).fetchone()
            if source_stats is None or int(source_stats[0]) <= 0:
                raise GptExportError("stock_daily contains no published rows")
            source_row_count = int(source_stats[0])
            source_symbol_count = int(source_stats[1])
            first_market_date = str(source_stats[2])
            latest_market_date = str(source_stats[3])
            if (
                required_market_date is not None
                and latest_market_date < required_market_date
            ):
                raise GptExportError(
                    f"source latest date {latest_market_date} is older than "
                    f"required date {required_market_date}"
                )

            with closing(sqlite3.connect(temporary_database)) as exported:
                exported.execute("PRAGMA foreign_keys = ON")
                exported.execute(_CREATE_EXPORTED_TABLE_SQL)
                exported.execute(
                    "CREATE INDEX idx_stock_daily_date ON stock_daily (date, symbol)"
                )
                insert_sql = """
                    INSERT INTO stock_daily
                        (symbol, date, open, high, low, close, volume, turnover)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """
                for rows in _iter_published_rows(source):
                    invalid_rows = [row for row in rows if not _is_valid_published_row(row)]
                    if invalid_rows:
                        preview = ", ".join(
                            f"{row[0]!r}/{row[1]!r}" for row in invalid_rows[:5]
                        )
                        raise GptExportError(
                            f"stock_daily contains {len(invalid_rows)} invalid row(s) "
                            f"in the current batch: {preview}"
                        )
                    exported.executemany(insert_sql, rows)

                if selection_history_available:
                    exported.executescript(_CREATE_SELECTION_TABLES_SQL)
                    run_columns = ", ".join(_SELECTION_RUN_COLUMNS)
                    run_placeholders = ", ".join("?" for _ in _SELECTION_RUN_COLUMNS)
                    run_rows = source.execute(
                        f"SELECT {run_columns} FROM selection_runs ORDER BY market_date"
                    ).fetchall()
                    exported.executemany(
                        f"INSERT INTO selection_runs ({run_columns}) VALUES ({run_placeholders})",
                        run_rows,
                    )
                    result_columns = ", ".join(_SELECTION_RESULT_COLUMNS)
                    result_placeholders = ", ".join("?" for _ in _SELECTION_RESULT_COLUMNS)
                    result_rows = source.execute(
                        f"SELECT {result_columns} FROM selection_results "
                        "ORDER BY market_date, strategy, symbol"
                    ).fetchall()
                    exported.executemany(
                        f"INSERT INTO selection_results ({result_columns}) "
                        f"VALUES ({result_placeholders})",
                        result_rows,
                    )
                exported.commit()
                _quick_check(exported, "exported")
                exported_stats = exported.execute(
                    """
                    SELECT COUNT(*), COUNT(DISTINCT symbol), MIN(date), MAX(date)
                    FROM stock_daily
                    """
                ).fetchone()
                exported_selection_counts: tuple[int, int] | None = None
                if selection_history_available:
                    exported_selection_counts = (
                        int(exported.execute("SELECT COUNT(*) FROM selection_runs").fetchone()[0]),
                        int(
                            exported.execute(
                                "SELECT COUNT(*) FROM selection_results"
                            ).fetchone()[0]
                        ),
                    )

            if exported_stats != source_stats:
                raise GptExportError(
                    f"exported row statistics differ from source: "
                    f"source={source_stats}, exported={exported_stats}"
                )
            if selection_history_available and exported_selection_counts != (
                int(selection_stats["run_count"]),
                int(selection_stats["result_count"]),
            ):
                raise GptExportError(
                    "exported selection row counts differ from validated source counts"
                )

        _fsync(temporary_database)
        database_sha256 = _sha256(temporary_database)
        os.replace(temporary_database, database_path)

        _atomic_write_text(readme_path, _README_TEXT)
        manifest = {
            "schema_version": EXPORT_SCHEMA_VERSION,
            "created_at_utc": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "database_file": DATABASE_FILENAME,
            "database_sha256": database_sha256,
            "included_tables": (
                ["stock_daily", "selection_runs", "selection_results"]
                if selection_history_available
                else ["stock_daily"]
            ),
            "stock_daily_columns": list(_EXPORTED_COLUMNS),
            "row_count": source_row_count,
            "symbol_count": source_symbol_count,
            "first_market_date": first_market_date,
            "latest_market_date": latest_market_date,
            "integrity": {
                "quick_check": "ok",
                "source_quick_check": "ok",
                "export_quick_check": "ok",
                "invalid_rows": 0,
                "copied_row_count": source_row_count,
            },
            "required_market_date": required_market_date,
            "verification": (
                selection_evidence[required_market_date]
                if required_market_date is not None
                else {"status": "not_requested", "market_date": None}
            ),
            "content_policy": {
                "published_market_data_only": True,
                "staging_excluded": True,
                "orders_funds_positions_nav_excluded": True,
                "unknown_source_tables_excluded": True,
            },
            "scope": {
                "all_published_source_rows_copied": True,
                "historical_full_market_coverage_proven": False,
                "selection_history_starts_when_feature_was_enabled": True,
            },
            "selection_history": selection_stats,
        }
        _atomic_write_text(
            manifest_path,
            json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        )
    finally:
        temporary_database.unlink(missing_ok=True)

    return GptExportResult(
        database_path=database_path.resolve(),
        manifest_path=manifest_path.resolve(),
        readme_path=readme_path.resolve(),
        row_count=source_row_count,
        symbol_count=source_symbol_count,
        latest_market_date=latest_market_date,
        database_sha256=database_sha256,
    )


def _cli_market_date(value: str) -> str:
    try:
        return _strict_market_date(value, "--required-market-date")
    except GptExportError as exc:
        raise argparse.ArgumentTypeError(str(exc)) from exc


def _argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="导出仅含已发布行情和已核验选股历史的 GPT SQLite 快照"
    )
    parser.add_argument(
        "--source",
        default=os.environ.get("DB_PATH", "data/sequoia_v2.db"),
        help="源 SQLite 数据库（默认读取 DB_PATH）",
    )
    parser.add_argument(
        "--output",
        default=os.environ.get("GPT_EXPORT_DIR", "gpt-export"),
        help="导出目录（默认读取 GPT_EXPORT_DIR 或使用 gpt-export）",
    )
    parser.add_argument(
        "--required-market-date",
        type=_cli_market_date,
        help="必须存在并通过完整性门禁的选股日期，格式 YYYY-MM-DD",
    )
    parser.add_argument(
        "--minimum-coverage",
        type=float,
        default=os.environ.get("MIN_DAILY_COVERAGE", "0.98"),
        help="选股记录最低行情覆盖率（默认读取 MIN_DAILY_COVERAGE）",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = _argument_parser()
    args = parser.parse_args(argv)
    try:
        result = export_verified_stock_database(
            args.source,
            args.output,
            required_market_date=args.required_market_date,
            minimum_coverage=args.minimum_coverage,
        )
    except (GptExportError, OSError, sqlite3.Error) as exc:
        parser.exit(1, f"导出失败：{exc}\n")

    print(
        json.dumps(
            {
                "database": str(result.database_path),
                "manifest": str(result.manifest_path),
                "readme": str(result.readme_path),
                "row_count": result.row_count,
                "symbol_count": result.symbol_count,
                "latest_market_date": result.latest_market_date,
                "database_sha256": result.database_sha256,
            },
            ensure_ascii=False,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
