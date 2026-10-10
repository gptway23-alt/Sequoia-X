"""历史回填的流式并发和批次隔离回归测试。"""

from __future__ import annotations

import sqlite3
from datetime import date

from sequoia_x.core.config import Settings
from sequoia_x.data.engine import BatchFetchResult, DataEngine, DataIntegrityError


def _engine(tmp_path, *, batch_size: int = 1) -> DataEngine:
    return DataEngine(
        Settings(
            db_path=str(tmp_path / "backfill.db"),
            start_date="2024-01-01",
            baostock_max_workers=2,
            baostock_batch_size=batch_size,
            baostock_max_attempts=2,
            baostock_backoff_seconds=0,
        )
    )


def _row(symbol: str, day: str, price: str = "10") -> list[str]:
    return [symbol, day, price, price, price, price, "100", "1000"]


def test_backfill_writes_each_result_before_requesting_next(monkeypatch, tmp_path) -> None:
    """第一批应先提交到 SQLite，生成器才会被请求第二批。"""
    engine = _engine(tmp_path, batch_size=1)
    monkeypatch.setattr(
        engine,
        "_latest_trade_date",
        lambda _today: date(2026, 10, 8),
    )

    def streamed_results(batches):
        assert len(batches) == 3
        assert all(len(batch) == 1 for batch in batches)
        yield BatchFetchResult(
            rows=[_row("000001", "2026-10-08")],
            attempted_symbols={"000001"},
        )

        # 恢复生成器代表 backfill 已处理上一个 yield；此时首批必须已经提交。
        with sqlite3.connect(engine.db_path) as conn:
            first_count = conn.execute(
                "SELECT COUNT(*) FROM stock_daily WHERE symbol = ?",
                ("000001",),
            ).fetchone()[0]
        assert first_count == 1

        yield BatchFetchResult(
            attempted_symbols={"000002"},
            failed_symbols={"000002": "simulated query failure"},
        )
        yield BatchFetchResult(
            rows=[_row("000003", "2026-10-08", "30")],
            attempted_symbols={"000003"},
        )

    monkeypatch.setattr(engine, "_iter_fetch_batches", streamed_results)

    report = engine.backfill(["000001", "000002", "000003"])

    assert report.requested == 3
    assert report.succeeded == 2
    assert report.skipped == 0
    assert report.failed_symbols == {"000002": "simulated query failure"}
    with sqlite3.connect(engine.db_path) as conn:
        symbols = conn.execute(
            "SELECT DISTINCT symbol FROM stock_daily ORDER BY symbol"
        ).fetchall()
    assert symbols == [("000001",), ("000003",)]


def test_backfill_invalid_symbol_does_not_discard_valid_sibling(
    monkeypatch,
    tmp_path,
) -> None:
    engine = _engine(tmp_path, batch_size=20)
    monkeypatch.setattr(
        engine,
        "_latest_trade_date",
        lambda _today: date(2026, 10, 8),
    )
    monkeypatch.setattr(
        engine,
        "_iter_fetch_batches",
        lambda _batches: iter(
            [
                BatchFetchResult(
                    rows=[
                        _row("000001", "2026-10-08"),
                        # high 小于 open/close，整只股票必须被拒绝。
                        [
                            "000002",
                            "2026-10-08",
                            "20",
                            "19",
                            "18",
                            "20",
                            "100",
                            "2000",
                        ],
                    ],
                    attempted_symbols={"000001", "000002"},
                )
            ]
        ),
    )

    report = engine.backfill(["000001", "000002"])

    assert report.succeeded == 1
    assert "OHLC bounds" in report.failed_symbols["000002"]
    assert engine.get_local_symbols() == ["000001"]


def test_backfill_database_failure_rolls_back_only_that_batch(
    monkeypatch,
    tmp_path,
) -> None:
    engine = _engine(tmp_path, batch_size=1)
    monkeypatch.setattr(
        engine,
        "_latest_trade_date",
        lambda _today: date(2026, 10, 8),
    )
    monkeypatch.setattr(
        engine,
        "_iter_fetch_batches",
        lambda _batches: iter(
            [
                BatchFetchResult(
                    rows=[_row("000001", "2026-10-08")],
                    attempted_symbols={"000001"},
                ),
                BatchFetchResult(
                    rows=[_row("000002", "2026-10-08", "20")],
                    attempted_symbols={"000002"},
                ),
            ]
        ),
    )

    original_upsert = engine._upsert_rows
    calls = 0

    def fail_first_batch(rows, **kwargs):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise DataIntegrityError("simulated transaction failure")
        return original_upsert(rows, **kwargs)

    monkeypatch.setattr(engine, "_upsert_rows", fail_first_batch)

    report = engine.backfill(["000001", "000002"])

    assert report.succeeded == 1
    assert "batch database write failed" in report.failed_symbols["000001"]
    assert engine.get_local_symbols() == ["000002"]
    assert engine.database_quick_check()
