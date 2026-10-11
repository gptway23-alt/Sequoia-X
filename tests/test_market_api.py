"""Direct BaoStock adapter tests. No database or network dependencies."""
import pytest
from sequoia_x.data.market_api import MarketApiError, bs_code, fetch_raw, parse_bar


class FakeResult:
    error_code = "0"
    error_msg = ""
    def __init__(self, rows):
        self.rows = rows
        self.idx = -1
    def next(self):
        self.idx += 1
        return self.idx < len(self.rows)
    def get_row_data(self):
        return self.rows[self.idx]


class FakeBaoStock:
    def __init__(self, rows, error_code="0"):
        self.rows = rows
        self.error_code = error_code
        self.calls = []
    def query_history_k_data_plus(self, *args, **kwargs):
        self.calls.append((args, kwargs))
        r = FakeResult(self.rows)
        r.error_code = self.error_code
        return r


def row(day='2026-10-09'):
    return [day, '10', '11', '9', '10.5', '12345', '123456']


def test_fetch_raw_adjustflag_and_code():
    bs = FakeBaoStock([row()])
    bars = fetch_raw(bs, '301363', '2026-10-01', '2026-10-09')
    assert len(bars) == 1
    assert bars[0][0:2] == ('301363', '2026-10-09')
    args, kw = bs.calls[0]
    assert args[0] == 'sz.301363'
    assert kw['frequency'] == 'd'
    assert kw['adjustflag'] == '3'


def test_exchanges():
    assert bs_code('600519') == 'sh.600519'
    assert bs_code('000001') == 'sz.000001'
    assert bs_code('301363') == 'sz.301363'
    with pytest.raises(MarketApiError):
        bs_code('../bad')


def test_duplicate_rejected():
    with pytest.raises(MarketApiError, match='duplicate'):
        fetch_raw(FakeBaoStock([row(), row()]), '301363', '2026-10-01', '2026-10-09')


def test_bad_price_rejected():
    with pytest.raises(MarketApiError, match='invalid raw OHLC'):
        parse_bar('301363', ['2026-10-09', '10', '11', '12', '10.5', '1', '1'], '2026-10-01', '2026-10-09')


def test_provider_error_rejected():
    with pytest.raises(MarketApiError, match='BaoStock failed'):
        fetch_raw(FakeBaoStock([], error_code='1'), '301363', '2026-10-01', '2026-10-09')


def test_bad_dates_rejected():
    with pytest.raises(MarketApiError):
        fetch_raw(FakeBaoStock([]), '301363', '2026-10-10', '2026-10-09')


def test_old_raw_db_export_removed():
    import sequoia_x.data.market_api as market_api
    for obsolete in ('sync_full_universe', 'open_db', 'export', 'build_static_api'):
        assert not hasattr(market_api, obsolete)
