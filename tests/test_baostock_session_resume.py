"""离线验证 BaoStock 连接复用、断点续传和98%完整性门禁。"""
import sqlite3
import sys
from datetime import date
from types import SimpleNamespace

import sequoia_x.data.engine as module
from sequoia_x.core.config import Settings
from sequoia_x.data.engine import DataEngine, SYNC_COMPLETE


class _Result:
    error_code = "0"
    error_msg = ""

    def __init__(self, day):
        self._day = day
        self._reads = 0

    def next(self):
        self._reads += 1
        return self._reads == 1

    def get_row_data(self):
        return [self._day, "10", "11", "9", "10.5", "1000", "10500"]


class _FakeBaoStock:
    def __init__(self):
        self.logins = 0
        self.logouts = 0
        self.requests = 0

    def login(self):
        self.logins += 1
        return SimpleNamespace(error_code="0", error_msg="")

    def logout(self):
        self.logouts += 1
        return SimpleNamespace(error_code="0", error_msg="")

    def query_history_k_data_plus(self, code, fields, *, start_date, end_date, frequency, adjustflag):
        self.requests += 1
        assert code.startswith("sh.")
        return _Result(end_date)


def test_worker_reuses_one_login_over_multiple_batches(monkeypatch):
    fake = _FakeBaoStock()
    monkeypatch.setitem(sys.modules, "baostock", fake)
    monkeypatch.setattr(module, "_WORKER_BS", None)
    monkeypatch.setattr(module, "_WORKER_LOGGED_IN", False)
    monkeypatch.setattr(module, "_WORKER_SHUTDOWN_REGISTERED", False)
    day = date.today().isoformat()
    try:
        one = module._bs_fetch_batch(([("600059", "sh.600059", day, day)], 1, 0, 10))
        two = module._bs_fetch_batch(([("600060", "sh.600060", day, day)], 1, 0, 10))
        assert one.current_symbols == {"600059"}
        assert two.current_symbols == {"600060"}
        assert fake.requests == 2
        assert fake.logins == 1, "同一个worker处理两个批次不应重复登录"
        assert fake.logouts == 0, "小批次完成时不得反复退出"
    finally:
        module._worker_disconnect()
    assert fake.logouts == 1


def test_98_percent_staging_releases_without_fetch(monkeypatch, tmp_path):
    day = date.today()
    day_text = day.isoformat()
    e = DataEngine(Settings(db_path=str(tmp_path / "sequoia_v2.db"), min_daily_coverage=0.98))
    symbols = [f"60{n:04d}" for n in range(50)]
    rows = [(s, day_text, 10, 11, 9, 10.5, 1000, 10500) for s in symbols[:49]]
    e._stage_rows(day_text, rows)
    monkeypatch.setattr(e, "_is_trade_day", lambda day: True)

    def unexpected_fetch(_):
        raise AssertionError("暂存已达到98%，不应重新请求BaoStock")

    monkeypatch.setattr(e, "_iter_fetch_batches", unexpected_fetch)
    result = e.sync_today_bulk(symbols, target_date=day)
    assert result.status == SYNC_COMPLETE
    assert result.coverage == 49 / 50
    assert result.rows_written == 49
    with sqlite3.connect(e.db_path) as db:
        assert db.execute("SELECT COUNT(*) FROM stock_daily").fetchone()[0] == 49
        assert db.execute("SELECT COUNT(*) FROM stock_daily_staging").fetchone()[0] == 0


def test_less_than_98_percent_staging_does_not_publish(monkeypatch, tmp_path):
    day = date.today()
    day_text = day.isoformat()
    e = DataEngine(Settings(db_path=str(tmp_path / "sequoia_v2.db"), min_daily_coverage=0.98))
    symbols = [f"60{n:04d}" for n in range(50)]
    rows = [(s, day_text, 10, 11, 9, 10.5, 1000, 10500) for s in symbols[:48]]
    e._stage_rows(day_text, rows)
    monkeypatch.setattr(e, "_is_trade_day", lambda _: True)
    monkeypatch.setattr(e, "_iter_fetch_batches", lambda _: iter(()))
    result = e.sync_today_bulk(symbols, target_date=day)
    assert result.candidate_coverage == 48/50
    assert result.coverage == 0
    with sqlite3.connect(e.db_path) as db:
        assert db.execute("SELECT COUNT(*) FROM stock_daily").fetchone()[0] == 0
        assert db.execute("SELECT COUNT(*) FROM stock_daily_staging").fetchone()[0] == 48
