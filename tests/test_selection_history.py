"""已核验选股历史的持久化与隔离测试。"""

from __future__ import annotations

import json
import sqlite3

import pytest

from sequoia_x.core.config import Settings
from sequoia_x.data.engine import (
    SYNC_COMPLETE,
    SYNC_INCOMPLETE,
    DataEngine,
    DataIntegrityError,
    SyncReport,
)


def _engine(tmp_path) -> DataEngine:
    engine = DataEngine(
        Settings(
            db_path=str(tmp_path / "selection-history.db"),
            start_date="2024-01-01",
            min_daily_coverage=0.98,
        )
    )
    with sqlite3.connect(engine.db_path) as connection:
        connection.executemany(
            """
            INSERT INTO stock_daily
                (symbol, date, open, high, low, close, volume, turnover)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                ("000001", "2026-10-08", 10, 12, 9, 11, 100, 1_100),
                ("600519", "2026-10-08", 20, 23, 19, 22, 200, 4_400),
            ],
        )
    return engine


def _complete_report() -> SyncReport:
    return SyncReport(
        status=SYNC_COMPLETE,
        target_date="2026-10-08",
        is_trade_day=True,
        expected_symbols=2,
        verified_symbols=frozenset({"000001", "600519"}),
    )


def test_records_and_atomically_replaces_complete_selection_date(tmp_path) -> None:
    engine = _engine(tmp_path)
    report = _complete_report()

    written = engine.record_verified_selections(
        sync_report=report,
        strategy_results={"StrategyA": ["000001"], "StrategyB": ["600519"]},
        executed_strategies=["StrategyA", "StrategyB", "StrategyC"],
    )
    assert written == 2

    with sqlite3.connect(engine.db_path) as connection:
        run = connection.execute(
            """
            SELECT market_date, expected_symbols, verified_symbols, coverage,
                   verified_symbols_json, strategies_json, result_rows
            FROM selection_runs
            """
        ).fetchone()
        results = connection.execute(
            """
            SELECT strategy, symbol
            FROM selection_results
            ORDER BY strategy, symbol
            """
        ).fetchall()

    assert run[:4] == ("2026-10-08", 2, 2, 1.0)
    assert json.loads(run[4]) == ["000001", "600519"]
    assert json.loads(run[5]) == ["StrategyA", "StrategyB", "StrategyC"]
    assert run[6] == 2
    assert results == [("StrategyA", "000001"), ("StrategyB", "600519")]

    replaced = engine.record_verified_selections(
        sync_report=report,
        strategy_results={"StrategyA": ["600519"]},
        executed_strategies=["StrategyA", "StrategyB", "StrategyC"],
    )
    assert replaced == 1

    with sqlite3.connect(engine.db_path) as connection:
        results = connection.execute(
            "SELECT strategy, symbol FROM selection_results"
        ).fetchall()
        result_rows = connection.execute(
            "SELECT result_rows FROM selection_runs"
        ).fetchone()[0]
    assert results == [("StrategyA", "600519")]
    assert result_rows == 1


def test_rejects_incomplete_or_unverified_results_without_writing(tmp_path) -> None:
    engine = _engine(tmp_path)
    incomplete = SyncReport(
        status=SYNC_INCOMPLETE,
        target_date="2026-10-08",
        is_trade_day=True,
        expected_symbols=2,
        verified_symbols=frozenset({"000001"}),
    )

    with pytest.raises(DataIntegrityError, match="只能记录"):
        engine.record_verified_selections(
            sync_report=incomplete,
            strategy_results={"StrategyA": ["000001"]},
            executed_strategies=["StrategyA"],
        )

    with pytest.raises(DataIntegrityError, match="未通过"):
        engine.record_verified_selections(
            sync_report=_complete_report(),
            strategy_results={"StrategyA": ["300001"]},
            executed_strategies=["StrategyA"],
        )

    with sqlite3.connect(engine.db_path) as connection:
        assert connection.execute("SELECT COUNT(*) FROM selection_runs").fetchone()[0] == 0
        assert connection.execute("SELECT COUNT(*) FROM selection_results").fetchone()[0] == 0


def test_rejects_report_symbol_without_same_day_published_quote(tmp_path) -> None:
    engine = _engine(tmp_path)
    with sqlite3.connect(engine.db_path) as connection:
        connection.execute(
            "DELETE FROM stock_daily WHERE symbol = '600519' AND date = '2026-10-08'"
        )

    with pytest.raises(DataIntegrityError, match="缺少同日有效已发布行情"):
        engine.record_verified_selections(
            sync_report=_complete_report(),
            strategy_results={"StrategyA": ["000001"]},
            executed_strategies=["StrategyA"],
        )

    with sqlite3.connect(engine.db_path) as connection:
        assert connection.execute("SELECT COUNT(*) FROM selection_runs").fetchone()[0] == 0


def test_selection_schema_contains_no_trading_ledger_tables(tmp_path) -> None:
    engine = _engine(tmp_path)
    with sqlite3.connect(engine.db_path) as connection:
        tables = {
            row[0]
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            )
        }

    assert {"stock_daily", "stock_daily_staging", "selection_runs", "selection_results"} <= tables
    assert tables.isdisjoint({"orders", "funds", "positions", "holdings", "nav", "accounts"})
