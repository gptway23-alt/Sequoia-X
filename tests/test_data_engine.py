"""数据引擎属性测试。"""

import socket
import sqlite3
import sys
import tempfile
from datetime import date, timedelta
from pathlib import Path
from types import SimpleNamespace

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


def test_daily_run_completion_marker_persists_and_is_idempotent() -> None:
    target_date = date(2026, 10, 27)
    with tempfile.TemporaryDirectory() as tmp_dir:
        engine, settings = make_engine_in(tmp_dir)
        assert engine.daily_run_completed(target_date) is False

        engine.mark_daily_run_completed(target_date)
        engine.mark_daily_run_completed(target_date)

        restored_engine = DataEngine(settings)
        assert restored_engine.daily_run_completed(target_date) is True
        with sqlite3.connect(engine.db_path) as conn:
            count = conn.execute(
                "SELECT COUNT(*) FROM daily_runs WHERE trade_date = ?",
                (target_date.isoformat(),),
            ).fetchone()[0]
        assert count == 1


def test_get_ohlcv_excludes_rows_after_target_trade_date() -> None:
    with tempfile.TemporaryDirectory() as tmp_dir:
        engine, _ = make_engine_in(tmp_dir)
        _seed_daily_rows(engine, ["000001"], date(2026, 10, 15))
        engine.verified_date = "2026-10-15"

        history = engine.get_ohlcv("000001")

        assert history["date"].tolist() == ["2026-10-14", "2026-10-15"]


def _fake_executor(monkeypatch, returned_symbols: set[str], failed: set[str] | None = None) -> None:
    class FakeFuture:
        def __init__(self, result: BatchResult):
            self._result = result

        def result(self) -> BatchResult:
            return self._result

        def cancel(self) -> bool:
            return False

        def cancelled(self) -> bool:
            return False

    class FakeExecutor:
        def __init__(self, max_workers: int):
            assert max_workers <= 2

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def submit(self, _function, payload, _timeout):
            tasks, _attempts, _backoff, _socket_timeout = payload
            rows = []
            for symbol, _code, _start, end in tasks:
                if symbol in returned_symbols:
                    rows.append([symbol, end, "10", "11", "9", "10.5", "1000", "10500"])
            failed_in_batch = [symbol for symbol, *_ in tasks if failed and symbol in failed]
            return FakeFuture(BatchResult(rows, failed_in_batch, []))

    monkeypatch.setattr(engine_module, "ThreadPoolExecutor", FakeExecutor)
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


def test_trading_calendar_retries_transient_login_failure(monkeypatch) -> None:
    class CalendarResult:
        error_code = "0"
        error_msg = ""

        def __init__(self) -> None:
            self._read = False

        def next(self) -> bool:
            if self._read:
                return False
            self._read = True
            return True

        def get_row_data(self) -> list[str]:
            return ["2026-10-14", "1"]

    class FakeBaoStock:
        login_calls = 0
        logout_calls = 0

        def login(self):
            self.login_calls += 1
            if self.login_calls == 1:
                return SimpleNamespace(error_code="1", error_msg="temporary timeout")
            return SimpleNamespace(error_code="0", error_msg="")

        def query_trade_dates(self, **_kwargs):
            return CalendarResult()

        def logout(self):
            self.logout_calls += 1

    fake_bs = FakeBaoStock()
    monkeypatch.setitem(sys.modules, "baostock", fake_bs)
    monkeypatch.setattr(socket, "setdefaulttimeout", lambda _timeout: None)
    monkeypatch.setattr(engine_module.time, "sleep", lambda _seconds: None)

    with tempfile.TemporaryDirectory() as tmp_dir:
        engine, _ = make_engine_in(tmp_dir)
        assert engine.is_trading_day(date(2026, 10, 14)) is True

    assert fake_bs.login_calls == 2
    assert fake_bs.logout_calls == 1


def test_trading_calendar_fails_closed_after_bounded_login_retries(monkeypatch) -> None:
    class FakeBaoStock:
        login_calls = 0

        def login(self):
            self.login_calls += 1
            return SimpleNamespace(error_code="1", error_msg="network receive error")

    fake_bs = FakeBaoStock()
    monkeypatch.setitem(sys.modules, "baostock", fake_bs)
    monkeypatch.setattr(socket, "setdefaulttimeout", lambda _timeout: None)
    monkeypatch.setattr(engine_module.time, "sleep", lambda _seconds: None)
    monkeypatch.setattr(
        engine_module,
        "_run_akshare_calendar_with_timeout",
        lambda _timeout: (_ for _ in ()).throw(RuntimeError("calendar offline")),
    )

    with tempfile.TemporaryDirectory() as tmp_dir:
        engine, _ = make_engine_in(tmp_dir)
        with pytest.raises(DataIntegrityError, match="AKShare 均无法核验"):
            engine.is_trading_day(date(2026, 10, 14))

    assert fake_bs.login_calls == 2


def test_trading_calendar_uses_akshare_fallback_after_baostock_failure(monkeypatch) -> None:
    class FakeBaoStock:
        login_calls = 0

        def login(self):
            self.login_calls += 1
            return SimpleNamespace(error_code="1", error_msg="network timeout")

    fake_bs = FakeBaoStock()
    monkeypatch.setitem(sys.modules, "baostock", fake_bs)
    monkeypatch.setattr(socket, "setdefaulttimeout", lambda _timeout: None)
    monkeypatch.setattr(engine_module.time, "sleep", lambda _seconds: None)
    monkeypatch.setattr(
        engine_module,
        "_run_akshare_calendar_with_timeout",
        lambda _timeout: ["2026-10-13", "2026-10-14", "2026-10-15"],
    )

    with tempfile.TemporaryDirectory() as tmp_dir:
        engine, _ = make_engine_in(tmp_dir)
        assert engine.is_trading_day(date(2026, 10, 14)) is True
        assert engine.is_trading_day(date(2026, 10, 15)) is True

    assert fake_bs.login_calls == 4


def test_trading_calendar_rejects_stale_akshare_fallback(monkeypatch) -> None:
    class FakeBaoStock:
        def login(self):
            return SimpleNamespace(error_code="1", error_msg="network timeout")

    monkeypatch.setitem(sys.modules, "baostock", FakeBaoStock())
    monkeypatch.setattr(socket, "setdefaulttimeout", lambda _timeout: None)
    monkeypatch.setattr(engine_module.time, "sleep", lambda _seconds: None)
    monkeypatch.setattr(
        engine_module,
        "_run_akshare_calendar_with_timeout",
        lambda _timeout: ["2026-10-10", "2026-10-11"],
    )

    with tempfile.TemporaryDirectory() as tmp_dir:
        engine, _ = make_engine_in(tmp_dir)
        with pytest.raises(DataIntegrityError, match="无法确认 2026-10-14"):
            engine.is_trading_day(date(2026, 10, 14))


def test_stock_universe_uses_akshare_after_baostock_login_failure(monkeypatch) -> None:
    class FakeBaoStock:
        def login(self):
            return SimpleNamespace(error_code="1", error_msg="network receive error")

    monkeypatch.setitem(sys.modules, "baostock", FakeBaoStock())
    monkeypatch.setattr(socket, "setdefaulttimeout", lambda _timeout: None)
    monkeypatch.setattr(
        engine_module,
        "_run_akshare_symbols_with_timeout",
        lambda _timeout: ["600000", "000001", "000001"],
    )

    with tempfile.TemporaryDirectory() as tmp_dir:
        engine, _ = make_engine_in(tmp_dir)
        assert engine.get_all_symbols() == ["000001", "600000"]


def test_akshare_fallback_fills_missing_current_day_bars(monkeypatch) -> None:
    class FakeFuture:
        def __init__(self, result: BatchResult):
            self._result = result

        def result(self) -> BatchResult:
            return self._result

        def cancel(self) -> bool:
            return False

        def cancelled(self) -> bool:
            return False

    class FakeExecutor:
        def __init__(self, max_workers: int):
            assert max_workers <= 2

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def submit(self, function, payload, _timeout):
            tasks = payload[0]
            if function is engine_module._run_batch_with_timeout:
                rows = [
                    [symbol, end, "10", "11", "9", "10.5", "1000", "10500"]
                    for symbol, _code, _start, end in tasks
                    if symbol not in missing
                ]
                return FakeFuture(BatchResult(rows, sorted(missing), []))
            symbol, _code, _start, end = tasks[0]
            return FakeFuture(
                BatchResult(
                    [[symbol, end, "10", "11", "9", "10.5", "1000", "10500"]],
                    [],
                    [],
                )
            )

    with tempfile.TemporaryDirectory() as tmp_dir:
        engine, _ = make_engine_in(tmp_dir)
        trade_date = date(2026, 10, 14)
        symbols = [f"{index:06d}" for index in range(100)]
        missing = set(symbols[-5:])
        _seed_daily_rows(engine, symbols, trade_date)
        monkeypatch.setattr(engine_module, "ThreadPoolExecutor", FakeExecutor)
        monkeypatch.setattr(engine_module, "as_completed", lambda futures: list(futures))

        count = engine.sync_today_bulk(expected_symbols=symbols, target_date=trade_date)

        assert count == 100
        assert engine.verified_symbols == set(symbols)
        assert engine.verified_date == trade_date.isoformat()


def test_stale_and_invalid_akshare_rows_do_not_count_toward_daily_coverage(monkeypatch) -> None:
    class FakeFuture:
        def __init__(self, result: BatchResult):
            self._result = result

        def result(self) -> BatchResult:
            return self._result

        def cancel(self) -> bool:
            return False

        def cancelled(self) -> bool:
            return False

    class FakeExecutor:
        def __init__(self, max_workers: int):
            assert max_workers <= 2

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def submit(self, function, payload, _timeout):
            tasks = payload[0]
            if function is engine_module._run_batch_with_timeout:
                rows = [
                    [symbol, end, "10", "11", "9", "10.5", "1000", "10500"]
                    for symbol, _code, _start, end in tasks
                    if symbol not in missing
                ]
                return FakeFuture(BatchResult(rows, sorted(missing), []))
            symbol, _code, start, _end = tasks[0]
            if symbol == symbols[-1]:
                stale_date = (date.fromisoformat(start) - timedelta(days=1)).isoformat()
                row = [symbol, stale_date, "10", "11", "9", "10.5", "1000", "10500"]
            else:
                row = [symbol, start, "10", "9", "9", "10.5", "1000", "10500"]
            return FakeFuture(
                BatchResult(
                    [row],
                    [],
                    [],
                )
            )

    with tempfile.TemporaryDirectory() as tmp_dir:
        engine, _ = make_engine_in(tmp_dir)
        trade_date = date(2026, 10, 14)
        symbols = [f"{index:06d}" for index in range(100)]
        missing = set(symbols[-3:])
        _seed_daily_rows(engine, symbols, trade_date)
        monkeypatch.setattr(engine_module, "ThreadPoolExecutor", FakeExecutor)
        monkeypatch.setattr(engine_module, "as_completed", lambda futures: list(futures))

        with pytest.raises(DataIntegrityError, match="覆盖率"):
            engine.sync_today_bulk(expected_symbols=symbols, target_date=trade_date)

        assert engine.verified_symbols is None
