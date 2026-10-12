"""仅使用本地SQLite进行回归，不访问网络、不触及模拟交易账本。"""
from datetime import date
import json
import sqlite3

import pytest

from sequoia_x.core.config import Settings
from sequoia_x.data.engine import (
    BatchFetchResult, DataEngine, DataIntegrityError,
    SyncReport, SYNC_COMPLETE, SYNC_INCOMPLETE,
)


@pytest.fixture
def engine(tmp_path):
    return DataEngine(Settings(db_path=str(tmp_path / 'test.db'), min_daily_coverage=1.0))


def row(code, day='2026-10-08'):
    return (code, day, 10., 11., 9., 10.5, 100., 1050.)


def complete_report():
    return SyncReport(
        status=SYNC_COMPLETE, target_date='2026-10-08',
        is_trade_day=True, expected_symbols=2,
        verified_symbols=frozenset({'000001', '600519'}),
    )


def test_staging_accepts_old_and_new_argument_order(engine):
    engine._stage_rows([row('000001')], '2026-10-08')
    engine._stage_rows('2026-10-08', [row('600519')])
    with sqlite3.connect(engine.db_path) as db:
        assert db.execute('SELECT COUNT(*) FROM stock_daily_staging').fetchone()[0] == 2
        assert db.execute('SELECT COUNT(*) FROM stock_daily').fetchone()[0] == 0


def test_incomplete_report_never_claims_staged_as_published(engine, monkeypatch):
    monkeypatch.setattr(engine, '_is_trade_day', lambda _: True)
    monkeypatch.setattr(engine, '_iter_fetch_batches', lambda _: iter([
        BatchFetchResult(rows=[list(row('000001'))], attempted_symbols={'000001', '600519'})
    ]))
    res = engine.sync_today_bulk(['000001', '600519'], date(2026, 10, 8))
    assert res.status == SYNC_INCOMPLETE
    assert res.verified_symbols == frozenset()
    assert res.candidate_verified_count == 1
    assert res.candidate_coverage == .5
    with sqlite3.connect(engine.db_path) as db:
        assert db.execute('SELECT COUNT(*) FROM stock_daily').fetchone()[0] == 0


def test_old_stage_pruned_on_new_trade_day(engine, monkeypatch):
    engine._stage_rows([row('000001','2026-10-07')], '2026-10-07')
    monkeypatch.setattr(engine, '_is_trade_day', lambda _: True)
    monkeypatch.setattr(engine, '_iter_fetch_batches', lambda _: iter(()))
    engine.sync_today_bulk(['000001', '600519'], date(2026, 10, 8))
    with sqlite3.connect(engine.db_path) as db:
        assert db.execute('SELECT COUNT(*) FROM stock_daily_staging').fetchone()[0] == 0


def test_selection_schema_only_research(engine):
    with sqlite3.connect(engine.db_path) as db:
        tables = {x[0] for x in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    assert {'stock_daily', 'stock_daily_staging', 'selection_runs', 'selection_results'} <= tables
    assert not {'orders', 'positions', 'cashflows', 'nav'} & tables


def test_record_verified_selections_atomic_and_idempotent(engine):
    engine._upsert_rows([row('000001'), row('600519')])
    result=engine.record_verified_selections(
        complete_report(), {'StrategyA':['000001'], 'StrategyB':['600519']},
        ['StrategyA','StrategyB','StrategyC'])
    assert result == 2
    with sqlite3.connect(engine.db_path) as db:
        original = db.execute("SELECT recorded_at FROM selection_runs WHERE selection_date='2026-10-08'").fetchone()[0]
        assert db.execute('SELECT COUNT(*) FROM selection_results').fetchone()[0] == 2
    assert engine.record_verified_selections(
        complete_report(), {'StrategyB':['600519'], 'StrategyA':['000001']},
        ['StrategyC','StrategyB','StrategyA']) == 2
    with sqlite3.connect(engine.db_path) as db:
        assert db.execute('SELECT recorded_at FROM selection_runs').fetchone()[0] == original
        assert db.execute('SELECT COUNT(*) FROM selection_results').fetchone()[0] == 2
    assert engine.record_verified_selections(
        complete_report(), {'StrategyA':['000001']}, ['StrategyA']) == 1
    with sqlite3.connect(engine.db_path) as db:
        results=db.execute('SELECT symbol, strategy_hit_count FROM selection_results').fetchall()
        assert results == [('000001',1)]
        assert db.execute('SELECT result_count FROM selection_runs').fetchone()[0] == 1


def test_selection_incomplete_blocked(engine):
    incomplete = SyncReport(status=SYNC_INCOMPLETE,target_date='2026-10-08',
        is_trade_day=True,expected_symbols=2,verified_symbols=frozenset({'000001'}))
    with pytest.raises(DataIntegrityError, match='只能记录'):
        engine.record_verified_selections(incomplete, {'A':['000001']}, ['A'])
    with sqlite3.connect(engine.db_path) as db:
        assert db.execute('SELECT COUNT(*) FROM selection_runs').fetchone()[0] == 0


def test_selection_rejects_missing_published_quote(engine):
    engine._upsert_rows([row('000001')])
    with pytest.raises(DataIntegrityError, match='缺少同日有效已发布行情'):
        engine.record_verified_selections(complete_report(), {'A':['600519']}, ['A'])
    with sqlite3.connect(engine.db_path) as db:
        assert db.execute('SELECT COUNT(*) FROM selection_runs').fetchone()[0] == 0
