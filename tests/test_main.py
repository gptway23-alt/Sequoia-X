"""主程序入口属性测试。"""

import sys
from datetime import datetime
from unittest.mock import MagicMock, patch
from zoneinfo import ZoneInfo

import pytest
from hypothesis import given, settings as h_settings
from hypothesis import strategies as st

# 预先导入 main 模块，避免在 @given 循环中重复导入
import main as main_module


def test_market_data_run_waits_until_after_close() -> None:
    shanghai = ZoneInfo("Asia/Shanghai")
    assert not main_module._after_market_close(datetime(2026, 10, 13, 14, 59, tzinfo=shanghai))
    assert main_module._after_market_close(datetime(2026, 10, 13, 15, 5, tzinfo=shanghai))
    assert not main_module._after_market_close(datetime(2026, 10, 17, 19, 15, tzinfo=shanghai))


# Feature: sequoia-x-v2, Property 13: 主程序异常以非零退出码终止
@given(error_msg=st.text(min_size=1, max_size=100))
@h_settings(max_examples=30, deadline=None)
def test_main_exits_nonzero_on_exception(error_msg: str) -> None:
    """属性 13：main() 中任意未捕获异常应导致 sys.exit(1)。"""
    # patch main 模块中直接引用的 get_settings
    with patch.object(main_module, "get_settings", side_effect=RuntimeError(error_msg)):
        with pytest.raises(SystemExit) as exc_info:
            main_module.main()
        assert exc_info.value.code != 0


def test_email_failure_does_not_stop_later_strategies(monkeypatch) -> None:
    """A failed email is reported after every strategy has had a chance to run."""
    monkeypatch.setattr(sys, "argv", ["main.py"])

    class FakeEngine:
        def __init__(self, settings) -> None:
            self.verified_symbols = {"000001"}
            self.verified_date = None

        def get_all_symbols(self) -> list[str]:
            return ["000001"]

        def get_local_symbols(self) -> list[str]:
            return ["000001"]

        def daily_run_completed(self, trade_date) -> bool:
            return False

        def mark_daily_run_completed(self, trade_date) -> None:
            self.completed_date = trade_date

        def is_trading_day(self, target_date=None) -> bool:
            return True

        def sync_today_bulk(
            self,
            expected_symbols: list[str] | None = None,
            target_date=None,
        ) -> int:
            self.verified_date = target_date.isoformat()
            return 1

    strategy_runs: list[str] = []

    class FakeStrategy:
        webhook_key = "test"

        def __init__(self, engine, settings) -> None:
            pass

        def run(self) -> list[str]:
            strategy_runs.append("run")
            return ["000001", "000002"]

    notifier = MagicMock()
    notifier.send.return_value = False

    with (
        patch.object(main_module, "get_settings", return_value=object()),
        patch.object(main_module, "_after_market_close", return_value=True),
        patch.object(main_module, "DataEngine", FakeEngine),
        patch.object(main_module, "EmailNotifier", return_value=notifier),
        patch.object(main_module, "MaVolumeStrategy", FakeStrategy),
        patch.object(main_module, "TurtleTradeStrategy", FakeStrategy),
        patch.object(main_module, "HighTightFlagStrategy", FakeStrategy),
        patch.object(main_module, "LimitUpShakeoutStrategy", FakeStrategy),
        patch.object(main_module, "UptrendLimitDownStrategy", FakeStrategy),
        patch.object(main_module, "RpsBreakoutStrategy", FakeStrategy),
        patch.object(main_module, "PrivatePlacementStrategy", FakeStrategy),
    ):
        with pytest.raises(SystemExit) as exc_info:
            main_module.main()

    assert exc_info.value.code == 2
    assert len(strategy_runs) == 7
    notifier.send.assert_called_once_with(
        symbols=["000001"],
        strategy_name="DailySummary",
        strategy_results={"FakeStrategy": ["000001"]},
        verified_symbols={"000001"},
        trade_date=datetime.now(ZoneInfo("Asia/Shanghai")).date(),
    )


def test_catch_up_skips_a_trade_date_already_completed(monkeypatch) -> None:
    target_date = datetime(2026, 10, 27, tzinfo=ZoneInfo("Asia/Shanghai")).date()
    monkeypatch.setattr(sys, "argv", ["main.py", "--catch-up"])

    class CompletedEngine:
        def __init__(self, settings) -> None:
            self.verified_date = None

        def daily_run_completed(self, trade_date) -> bool:
            assert trade_date == target_date
            return True

    with (
        patch.object(main_module, "get_settings", return_value=object()),
        patch.object(main_module, "latest_completed_weekday", return_value=target_date),
        patch.object(main_module, "DataEngine", CompletedEngine),
        patch.object(main_module, "EmailNotifier") as notifier,
        patch.object(main_module, "MaVolumeStrategy") as strategy,
    ):
        main_module.main()

    notifier.assert_not_called()
    strategy.assert_not_called()
