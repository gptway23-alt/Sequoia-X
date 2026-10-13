"""数据引擎属性测试。"""

import sqlite3
import tempfile
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import pytest
from hypothesis import given, settings as h_settings
from hypothesis import strategies as st

from sequoia_x.core.config import Settings
from sequoia_x.data import engine as engine_module
from sequoia_x.data.engine import BatchResult, DataEngine, DataIntegrityError


def make_engine_in(tmp_dir: str) -> tuple[DataEngine, Settings]:
    """创建使用临时数据库的 DataEngine 实例。"""
    settings = Settings(
        db_path=str(Path(tmp_dir) / "test.db"),
        start_date="2024-01-01",
        feishu_webhook_url="https://example.com/hook",
    )
    engine = DataEngine(settings)
    return engine, settings


# Property 4: (symbol, date) 唯一约束防止重复写入
@given(
    symbol=st.text(min_size=6, max_size=6, alphabet="0123456789"),
    trade_date=st.dates(min_value=date(2024, 1, 1), max_value=date(2025, 12, 31)),
)
@h_settings(max_examples=50, deadline=None)
def test_unique_symbol_date_constraint(symbol: str, trade_date: date) -> None:
    """相同 (symbol, date) 插入两次，数据库中该组合记录数应保持为 1。"""
    with tempfile.TemporaryDirectory() as tmp_dir:
        engine, _ = make_engine_in(tmp_dir)
        row = {
            "symbol": symbol, "date": str(trade_date),
            "open": 10.0, "high": 11.0, "low": 9.0, "close": 10.5,
            "volume": 1000.0, "turnover": 10500.0,
        }
        df = pd.DataFrame([row])
        with sqlite3.connect(engine.db_path) as conn:
            df.to_sql("stock_daily", conn, if_exists="append", index=False, method="multi")
            try:
                df.to_sql("stock_daily", conn, if_exists="append", index=False, method="multi")
            except sqlite3.IntegrityError:
                pass
            count = conn.execute(
                "SELECT COUNT(*) FROM stock_daily WHERE symbol=? AND date=?",
                (symbol, str(trade_date)),
            ).fetchone()[0]
        assert count == 1


def _seed_daily_rows(engine: DataEngine, symbols: list[str], trade_date: date) -> None:
    previous = (trade_date - timedelta(days=1)).isoformat()
    today = trade_date.isoformat()
    with sqlite3.connect(engine.db_path) as conn:
        conn.executemany(
            """
            INSERT INTO stock_daily
                (symbol, date, open, high, low, close, volume, turnover)
            VALUES (?, ?, 10, 11, 9, 10.5, 1000, 10500)
            """,
            [(symbol, previous) for symbol in symbols]
            + [(symbol, today) for symbol in symbols],
        )


def _fake_executor(monkeypatch, returned_symbols: set[str], failed: set[str] | None = None) -> None:
    class FakeFuture:
        def __init__(self, result: BatchResult):
            self._result = result

        def result(self) -> BatchResult:
            return self._result

    class FakeExecutor:
        def __init__(self, max_workers: int):
            assert max_workers <= 2

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def submit(self, _function, payload):
            tasks, _attempts, _backoff = payload
            rows = []
            for symbol, _code, _start, end in tasks:
                if symbol in returned_symbols:
                    rows.append([symbol, end, "10", "11", "9", "10.5", "1000", "10500"])
            failed_in_batch = [symbol for symbol, *_ in tasks if failed and symbol in failed]
            return FakeFuture(BatchResult(rows, failed_in_batch, []))

    monkeypatch.setattr(engine_module, "ProcessPoolExecutor", FakeExecutor)
    monkeypatch.setattr(engine_module, "as_completed", lambda futures: list(futures))


def test_incomplete_market_data_does_not_publish_partial_rows(monkeypatch) -> None:
    with tempfile.TemporaryDirectory() as tmp_dir:
        engine, _ = make_engine_in(tmp_dir)
        trade_date = date(2026, 10, 13)
        symbols = [f"{index:06d}" for index in range(100)]
        _seed_daily_rows(engine, symbols, trade_date)
        _fake_executor(monkeypatch, set(symbols[:50]), set(symbols[50:]))

        with pytest.raises(DataIntegrityError, match="覆盖率"):
            engine.sync_today_bulk(expected_symbols=symbols, target_date=trade_date)

        with sqlite3.connect(engine.db_path) as conn:
            today_count = conn.execute(
                "SELECT COUNT(*) FROM stock_daily WHERE date=?", (trade_date.isoformat(),)
            ).fetchone()[0]
        assert today_count == len(symbols)
        assert engine.verified_symbols is None


def test_published_market_data_excludes_unverified_symbols(monkeypatch) -> None:
    with tempfile.TemporaryDirectory() as tmp_dir:
        engine, _ = make_engine_in(tmp_dir)
        trade_date = date(2026, 10, 13)
        symbols = [f"{index:06d}" for index in range(100)]
        _seed_daily_rows(engine, symbols, trade_date)
        _fake_executor(monkeypatch, set(symbols[:99]), {symbols[-1]})

        count = engine.sync_today_bulk(expected_symbols=symbols, target_date=trade_date)

        assert count == 99
        assert engine.verified_date == trade_date.isoformat()
        assert engine.verified_symbols == set(symbols[:99])
        assert set(engine.get_local_symbols()) == set(symbols[:99])
        assert engine.get_ohlcv(symbols[-1]).empty
        with sqlite3.connect(engine.db_path) as conn:
            today_symbols = {
                row[0]
                for row in conn.execute(
                    "SELECT symbol FROM stock_daily WHERE date=?", (trade_date.isoformat(),)
                )
            }
        assert today_symbols == set(symbols[:99])
