"""BaoStock 故障、批次隔离和 SQLite 发布门禁的离线回归测试。"""

from __future__ import annotations

import socket
import sqlite3
import sys
from concurrent.futures import Future
from datetime import date
from types import SimpleNamespace

import pytest

import sequoia_x.data.engine as engine_module
from sequoia_x.core.config import Settings
from sequoia_x.data.engine import BatchFetchResult, DataEngine, SYNC_INCOMPLETE, _bs_fetch_batch


class _Response:
    def __init__(self, rows: list[list[str]] | None = None, fail_next: bool = False) -> None:
        self.error_code = "0"
        self.error_msg = ""
        self._rows = list(rows or [])
        self._index = 0
        self._fail_next = fail_next

    def next(self) -> bool:
        if self._fail_next:
            raise IndexError("short BaoStock response")
        if self._index >= len(self._rows):
            return False
        self._index += 1
        return True

    def get_row_data(self) -> list[str]:
        return self._rows[self._index - 1]


def _engine(tmp_path, *, coverage: float = 1.0) -> DataEngine:
    return DataEngine(
        Settings(
            db_path=str(tmp_path / "test.db"),
            start_date="2024-01-01",
            min_daily_coverage=coverage,
            baostock_max_workers=1,
            baostock_max_attempts=2,
            baostock_backoff_seconds=0,
        )
    )


def test_batch_login_failure_never_queries_and_restores_timeout(monkeypatch) -> None:
    calls = {"login": 0, "query": 0}

    def login():
        calls["login"] += 1
        return SimpleNamespace(error_code="1", error_msg="login failed")

    def query(*_args, **_kwargs):
        calls["query"] += 1
        raise AssertionError("query must not run after a failed login")

    fake_bs = SimpleNamespace(login=login, logout=lambda: None, query_history_k_data_plus=query)
    monkeypatch.setitem(sys.modules, "baostock", fake_bs)
    original_timeout = socket.getdefaulttimeout()

    result = _bs_fetch_batch(
        ([('000001', 'sz.000001', '2026-10-08', '2026-10-08')], 2, 0, 7)
    )

    assert calls == {"login": 2, "query": 0}
    assert result.login_error == "login failed"
    assert set(result.failed_symbols) == {"000001"}
    assert socket.getdefaulttimeout() == original_timeout


def test_query_index_error_is_bounded_and_next_symbol_isolated(monkeypatch) -> None:
    query_calls: dict[str, int] = {}

    def query(code: str, *_args, **_kwargs):
        query_calls[code] = query_calls.get(code, 0) + 1
        if code == "sz.000001":
            return _Response(fail_next=True)
        return _Response(
            [["2026-10-08", "10", "11", "9", "10.5", "100", "1050"]]
        )

    fake_bs = SimpleNamespace(
        login=lambda: SimpleNamespace(error_code="0", error_msg=""),
        logout=lambda: None,
        query_history_k_data_plus=query,
    )
    monkeypatch.setitem(sys.modules, "baostock", fake_bs)
    result = _bs_fetch_batch(
        (
            [
                ("000001", "sz.000001", "2026-10-08", "2026-10-08"),
                ("000002", "sz.000002", "2026-10-08", "2026-10-08"),
            ],
            2,
            0,
            7,
        )
    )

    assert query_calls == {"sz.000001": 2, "sz.000002": 1}
    assert "IndexError" in result.failed_symbols["000001"]
    assert result.current_symbols == {"000002"}
    assert len(result.rows) == 1


def test_all_worker_login_failures_stop_unscheduled_batches(monkeypatch, tmp_path) -> None:
    engine = _engine(tmp_path)
    submitted: list[list[tuple[str, str, str, str]]] = []

    class _Executor:
        def __init__(self, *args, **kwargs) -> None:
            pass

        def __enter__(self):
            return self

        def __exit__(self, *args) -> None:
            return None

        def submit(self, function, payload):
            batch = payload[0]
            submitted.append(batch)
            future = Future()
            future.set_result(function(payload))
            return future

    def failed_worker(payload):
        batch = payload[0]
        return BatchFetchResult(
            failed_symbols={task[0]: "login failed" for task in batch},
            login_error="login failed",
        )

    monkeypatch.setattr("concurrent.futures.ProcessPoolExecutor", _Executor)
    monkeypatch.setattr(engine_module, "_bs_fetch_batch", failed_worker)
    batches = [
        [(f"{index:06d}", f"sz.{index:06d}", "2026-10-08", "2026-10-08")]
        for index in range(5)
    ]

    results = engine._run_fetch_batches(batches)

    assert len(submitted) == engine.max_workers == 1
    assert len(results) == len(batches)
    assert all(result.login_error for result in results)
    assert {symbol for result in results for symbol in result.failed_symbols} == {
        f"{index:06d}" for index in range(5)
    }


def test_partial_coverage_is_not_published(monkeypatch, tmp_path) -> None:
    engine = _engine(tmp_path, coverage=1.0)
    engine._upsert_rows(
        [("000001", "2026-10-07", 10.0, 11.0, 9.0, 10.5, 100.0, 1050.0)]
    )
    monkeypatch.setattr(engine, "_is_trade_day", lambda _target: True)
    monkeypatch.setattr(
        engine,
        "_iter_fetch_batches",
        lambda _batches: iter(
            [
                BatchFetchResult(
                    rows=[
                        [
                            "000002",
                            "2026-10-08",
                            "20",
                            "21",
                            "19",
                            "20.5",
                            "200",
                            "4100",
                        ]
                    ],
                    attempted_symbols={"000001", "000002"},
                    current_symbols={"000002"},
                    failed_symbols={"000001": "simulated failure"},
                )
            ]
        ),
    )

    report = engine.sync_today_bulk(
        expected_symbols=["000001", "000002"],
        target_date=date(2026, 10, 8),
    )

    assert report.status == SYNC_INCOMPLETE
    assert report.verified_symbols == frozenset({"000002"})
    assert report.coverage == 0.5
    with sqlite3.connect(engine.db_path) as connection:
        rows = connection.execute(
            "SELECT symbol, date FROM stock_daily ORDER BY symbol, date"
        ).fetchall()
        staged = connection.execute(
            """
            SELECT symbol, date
            FROM stock_daily_staging
            WHERE target_date = ?
            ORDER BY symbol, date
            """,
            ("2026-10-08",),
        ).fetchall()
    assert rows == [("000001", "2026-10-07")]
    assert staged == [("000002", "2026-10-08")]


def test_daily_staging_resumes_only_missing_symbols_and_publishes_atomically(
    monkeypatch,
    tmp_path,
) -> None:
    engine = _engine(tmp_path, coverage=1.0)
    monkeypatch.setattr(engine, "_is_trade_day", lambda _target: True)
    calls: list[list[str]] = []

    def first_run(batches):
        calls.append([task[0] for batch in batches for task in batch])
        return iter(
            [
                BatchFetchResult(
                    rows=[
                        ["000001", "2026-10-08", "10", "11", "9", "10.5", "100", "1050"]
                    ],
                    attempted_symbols={"000001", "000002"},
                    failed_symbols={"000002": "simulated timeout"},
                )
            ]
        )

    monkeypatch.setattr(engine, "_iter_fetch_batches", first_run)
    first = engine.sync_today_bulk(
        expected_symbols=["000001", "000002"],
        target_date=date(2026, 10, 8),
    )

    assert first.status == SYNC_INCOMPLETE
    assert first.verified_symbols == frozenset({"000001"})
    assert first.rows_written == 0
    assert calls == [["000001", "000002"]]
    assert engine.get_ohlcv("000001").empty

    def resumed_run(batches):
        calls.append([task[0] for batch in batches for task in batch])
        return iter(
            [
                BatchFetchResult(
                    rows=[
                        ["000002", "2026-10-08", "20", "21", "19", "20.5", "200", "4100"]
                    ],
                    attempted_symbols={"000002"},
                )
            ]
        )

    monkeypatch.setattr(engine, "_iter_fetch_batches", resumed_run)
    second = engine.sync_today_bulk(
        expected_symbols=["000001", "000002"],
        target_date=date(2026, 10, 8),
    )

    assert calls[1] == ["000002"]
    assert second.complete
    assert second.verified_symbols == frozenset({"000001", "000002"})
    assert second.rows_written == 2
    with sqlite3.connect(engine.db_path) as connection:
        published = connection.execute(
            "SELECT symbol, date FROM stock_daily ORDER BY symbol, date"
        ).fetchall()
        staged_count = connection.execute(
            "SELECT COUNT(*) FROM stock_daily_staging WHERE target_date = ?",
            ("2026-10-08",),
        ).fetchone()[0]
    assert published == [("000001", "2026-10-08"), ("000002", "2026-10-08")]
    assert staged_count == 0


def test_completed_daily_batch_survives_later_fetch_crash(monkeypatch, tmp_path) -> None:
    engine = _engine(tmp_path, coverage=1.0)
    monkeypatch.setattr(engine, "_is_trade_day", lambda _target: True)

    def interrupted(_batches):
        yield BatchFetchResult(
            rows=[
                ["000001", "2026-10-08", "10", "11", "9", "10.5", "100", "1050"]
            ],
            attempted_symbols={"000001"},
        )
        raise RuntimeError("simulated runner interruption")

    monkeypatch.setattr(engine, "_iter_fetch_batches", interrupted)

    with pytest.raises(RuntimeError, match="runner interruption"):
        engine.sync_today_bulk(
            expected_symbols=["000001", "000002"],
            target_date=date(2026, 10, 8),
        )

    with sqlite3.connect(engine.db_path) as connection:
        published_count = connection.execute("SELECT COUNT(*) FROM stock_daily").fetchone()[0]
        staged = connection.execute(
            "SELECT symbol FROM stock_daily_staging WHERE target_date = ?",
            ("2026-10-08",),
        ).fetchall()
    assert published_count == 0
    assert staged == [("000001",)]


def test_new_target_prunes_old_staging_and_never_publishes_unexpected_symbol(
    monkeypatch,
    tmp_path,
) -> None:
    engine = _engine(tmp_path, coverage=1.0)
    monkeypatch.setattr(engine, "_is_trade_day", lambda _target: True)
    engine._stage_rows(
        [("000002", "2026-10-07", 20.0, 21.0, 19.0, 20.5, 200.0, 4100.0)],
        "2026-10-07",
    )
    engine._stage_rows(
        [
            ("000001", "2026-10-08", 10.0, 11.0, 9.0, 10.5, 100.0, 1050.0),
            ("999999", "2026-10-08", 30.0, 31.0, 29.0, 30.5, 300.0, 9150.0),
        ],
        "2026-10-08",
    )

    report = engine.sync_today_bulk(
        expected_symbols=["000001"],
        target_date=date(2026, 10, 8),
    )

    assert report.complete
    with sqlite3.connect(engine.db_path) as connection:
        published = connection.execute(
            "SELECT symbol, date FROM stock_daily ORDER BY symbol, date"
        ).fetchall()
        staged_count = connection.execute(
            "SELECT COUNT(*) FROM stock_daily_staging"
        ).fetchone()[0]
    assert published == [("000001", "2026-10-08")]
    assert staged_count == 0


def test_same_date_upsert_does_not_delete_existing_symbols(tmp_path) -> None:
    engine = _engine(tmp_path)
    engine._upsert_rows(
        [("000001", "2026-10-08", 10.0, 11.0, 9.0, 10.5, 100.0, 1050.0)]
    )
    engine._upsert_rows(
        [("000002", "2026-10-08", 20.0, 21.0, 19.0, 20.5, 200.0, 4100.0)]
    )

    with sqlite3.connect(engine.db_path) as connection:
        symbols = connection.execute(
            "SELECT symbol FROM stock_daily WHERE date = ? ORDER BY symbol",
            ("2026-10-08",),
        ).fetchall()
    assert symbols == [("000001",), ("000002",)]
