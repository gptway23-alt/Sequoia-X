"""策略引擎属性测试。"""

import sys
import tempfile
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pandas as pd
from hypothesis import given, settings as h_settings
from hypothesis import strategies as st

from sequoia_x.core.config import Settings
from sequoia_x.data.engine import DataEngine
from sequoia_x.strategy.ma_volume import MaVolumeStrategy
from sequoia_x.strategy.turtle_trade import TurtleTradeStrategy


# Feature: sequoia-x-v2, Property 9: 策略 run() 返回值类型正确
@given(
    symbols=st.lists(
        st.text(min_size=6, max_size=6, alphabet="0123456789"),
        min_size=0, max_size=3, unique=True,
    )
)
@h_settings(max_examples=30, deadline=None)
def test_strategy_run_returns_list_of_str(symbols: list[str]) -> None:
    """属性 9：run() 应返回 list[str]，每个元素为非空字符串。"""
    with tempfile.TemporaryDirectory() as tmp_dir:
        settings = Settings(
            db_path=str(Path(tmp_dir) / "test.db"),
            start_date="2024-01-01",
            feishu_webhook_url="https://example.com/hook",
        )
        engine = DataEngine(settings)

        with patch.object(engine, "get_all_symbols", return_value=symbols):
            with patch.object(engine, "get_ohlcv", return_value=pd.DataFrame()):
                strategy = MaVolumeStrategy(engine=engine, settings=settings)
                result = strategy.run()

    assert isinstance(result, list)
    assert all(isinstance(s, str) and len(s) > 0 for s in result)


def test_turtle_market_cap_falls_back_to_tencent_after_baostock_login_failure(
    monkeypatch,
) -> None:
    class FakeBaoStock:
        def login(self):
            return SimpleNamespace(error_code="1", error_msg="network receive error")

    fake_ak = SimpleNamespace(
        stock_zh_a_hist_tx=lambda **_kwargs: pd.DataFrame(
            [
                {
                    "date": "2026-10-14",
                    "close": 10.5,
                    "volume": 1000,
                    "turnover": 0.05,
                }
            ]
        )
    )
    settings = Settings(feishu_webhook_url="https://example.com/hook")
    engine = SimpleNamespace(verified_date="2026-10-14", _to_baostock_code=lambda s: s)
    strategy = TurtleTradeStrategy(engine=engine, settings=settings)
    monkeypatch.setitem(sys.modules, "baostock", FakeBaoStock())
    monkeypatch.setitem(sys.modules, "akshare", fake_ak)

    assert strategy._get_market_caps(["000001"]) == {"000001": 210000.0}
