"""日常编排的数据门禁与通知隔离测试。"""

from __future__ import annotations

import json
import sys
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch
from zoneinfo import ZoneInfo

import pytest

import main as main_module
from sequoia_x.data.engine import SYNC_COMPLETE, SYNC_INCOMPLETE, SYNC_NON_TRADING_DAY, SyncReport


class _Engine:
    def __init__(self, report: SyncReport) -> None:
        self.report = report
        self.pinned = None
        self.recorded = None
        self.sync_target_date = None

    def get_all_symbols(self):
        return ["000001", "000002"]

    def get_local_symbols(self):
        return ["000001", "000002"]

    def sync_today_bulk(self, expected_symbols, target_date=None):
        assert expected_symbols == ["000001", "000002"]
        self.sync_target_date = target_date
        return self.report

    def pin_strategy_snapshot(self, trade_date, symbols):
        self.pinned = (trade_date, set(symbols))

    def record_verified_selections(
        self,
        sync_report,
        strategy_results,
        executed_strategies,
    ):
        self.recorded = (
            sync_report,
            strategy_results,
            tuple(executed_strategies),
        )
        return sum(len(symbols) for symbols in strategy_results.values())


def _run_with_engine(
    engine: _Engine,
    *,
    argv: list[str] | None = None,
    shanghai_now: datetime | None = None,
    **extra_patches,
) -> None:
    patches = {
        "get_settings": Mock(return_value=SimpleNamespace()),
        "DataEngine": Mock(return_value=engine),
        "_clear_gpt_export_state": Mock(),
        "_write_gpt_export_state": Mock(return_value=Path("reports/gpt-export-state.json")),
        "_shanghai_now": Mock(
            return_value=shanghai_now
            or datetime(2026, 10, 8, 19, 15, tzinfo=ZoneInfo("Asia/Shanghai"))
        ),
        **extra_patches,
    }
    with patch.object(sys, "argv", argv or ["main.py"]), patch.multiple(
        main_module, **patches
    ):
        main_module.main()


@pytest.mark.parametrize("status", [SYNC_INCOMPLETE])
def test_incomplete_quote_snapshot_stops_before_strategy_and_email(status) -> None:
    report = SyncReport(
        status=status,
        target_date="2026-10-08",
        is_trade_day=True,
        expected_symbols=2,
        verified_symbols=frozenset({"000001"}),
        failed_symbols={"000002": "simulated"},
        stale_symbols=frozenset({"000002"}),
    )
    engine = _Engine(report)
    strategy = Mock(side_effect=AssertionError("strategy must not be constructed"))
    notifier = Mock(side_effect=AssertionError("email must not be constructed"))

    with pytest.raises(SystemExit) as exc_info:
        _run_with_engine(engine, MaVolumeStrategy=strategy, EmailNotifier=notifier)

    assert exc_info.value.code == 1
    strategy.assert_not_called()
    notifier.assert_not_called()
    assert engine.pinned is None
    assert engine.recorded is None


def test_non_trading_day_skips_strategy_and_email_without_error() -> None:
    report = SyncReport(
        status=SYNC_NON_TRADING_DAY,
        target_date="2026-10-03",
        is_trade_day=False,
        expected_symbols=2,
    )
    engine = _Engine(report)
    strategy = Mock(side_effect=AssertionError("strategy must not be constructed"))
    notifier = Mock(side_effect=AssertionError("email must not be constructed"))

    state_writer = Mock(return_value=Path("reports/gpt-export-state.json"))
    _run_with_engine(
        engine,
        MaVolumeStrategy=strategy,
        EmailNotifier=notifier,
        _write_gpt_export_state=state_writer,
    )

    strategy.assert_not_called()
    notifier.assert_not_called()
    assert engine.pinned is None
    assert engine.recorded is None
    assert state_writer.call_args.kwargs == {
        "status": "non_trading_day",
        "market_date": "2026-10-03",
        "expected_symbols": 2,
        "verified_symbols": 0,
        "coverage": 0.0,
    }


def test_only_verified_strategy_results_reach_one_aggregate_email() -> None:
    report = SyncReport(
        status=SYNC_COMPLETE,
        target_date="2026-10-08",
        is_trade_day=True,
        expected_symbols=2,
        verified_symbols=frozenset({"000001"}),
    )
    engine = _Engine(report)

    strategy_types = {}
    for name in (
        "MaVolumeStrategy",
        "TurtleTradeStrategy",
        "HighTightFlagStrategy",
        "LimitUpShakeoutStrategy",
        "UptrendLimitDownStrategy",
        "RpsBreakoutStrategy",
        "PrivatePlacementStrategy",
    ):
        strategy_types[name] = type(
            name,
            (),
            {
                "__init__": lambda self, engine, settings: None,
                "run": lambda self: ["000001", "000002"],
            },
        )

    captured = {}

    class _Notifier:
        def __init__(self, settings):
            pass

        def send_report(self, **kwargs):
            captured.update(kwargs)
            return SimpleNamespace(
                status="sent",
                symbol_count=1,
                report_path=Path("reports/test.txt"),
            )

    state_writer = Mock(return_value=Path("reports/gpt-export-state.json"))
    _run_with_engine(
        engine,
        EmailNotifier=_Notifier,
        _write_gpt_export_state=state_writer,
        **strategy_types,
    )

    assert engine.pinned == ("2026-10-08", {"000001"})
    assert engine.sync_target_date.isoformat() == "2026-10-08"
    assert captured["verified_symbols"] == {"000001"}
    assert captured["market_date"] == "2026-10-08"
    assert set(captured["strategy_results"]) == set(strategy_types)
    assert all(symbols == ["000001"] for symbols in captured["strategy_results"].values())
    assert engine.recorded is not None
    recorded_report, recorded_results, recorded_strategies = engine.recorded
    assert recorded_report is report
    assert recorded_results == captured["strategy_results"]
    assert set(recorded_strategies) == set(strategy_types)
    assert state_writer.call_args.kwargs == {
        "status": "complete",
        "market_date": "2026-10-08",
        "expected_symbols": 2,
        "verified_symbols": 1,
        "coverage": 0.5,
        "selection_result_rows": 7,
    }


def test_email_failure_happens_after_selection_commit_and_export_state() -> None:
    report = SyncReport(
        status=SYNC_COMPLETE,
        target_date="2026-10-08",
        is_trade_day=True,
        expected_symbols=2,
        verified_symbols=frozenset({"000001", "000002"}),
    )
    engine = _Engine(report)
    events: list[str] = []

    strategy_types = {}
    for name in (
        "MaVolumeStrategy",
        "TurtleTradeStrategy",
        "HighTightFlagStrategy",
        "LimitUpShakeoutStrategy",
        "UptrendLimitDownStrategy",
        "RpsBreakoutStrategy",
        "PrivatePlacementStrategy",
    ):
        strategy_types[name] = type(
            name,
            (),
            {
                "__init__": lambda self, engine, settings: None,
                "run": lambda self: ["000001"],
            },
        )

    def _write_state(*args, **kwargs):
        events.append("state")
        return Path("reports/gpt-export-state.json")

    class _FailingNotifier:
        def __init__(self, settings):
            pass

        def send_report(self, **kwargs):
            events.append("email")
            raise RuntimeError("simulated SMTP failure")

    with pytest.raises(SystemExit) as exc_info:
        _run_with_engine(
            engine,
            EmailNotifier=_FailingNotifier,
            _write_gpt_export_state=_write_state,
            **strategy_types,
        )

    assert exc_info.value.code == 1
    assert engine.recorded is not None
    assert events == ["state", "email"]


def test_before_shanghai_cutoff_stops_before_engine_and_network() -> None:
    engine_factory = Mock(side_effect=AssertionError("engine must not be constructed"))
    strategy = Mock(side_effect=AssertionError("strategy must not be constructed"))
    notifier = Mock(side_effect=AssertionError("email must not be constructed"))
    state_writer = Mock(return_value=Path("reports/gpt-export-state.json"))

    with (
        patch.object(sys, "argv", ["main.py"]),
        patch.multiple(
            main_module,
            get_settings=Mock(
                return_value=SimpleNamespace(daily_sync_not_before="15:30")
            ),
            DataEngine=engine_factory,
            _clear_gpt_export_state=Mock(),
            _write_gpt_export_state=state_writer,
            _shanghai_now=Mock(
                return_value=datetime(
                    2026,
                    10,
                    9,
                    14,
                    59,
                    tzinfo=ZoneInfo("Asia/Shanghai"),
                )
            ),
            MaVolumeStrategy=strategy,
            EmailNotifier=notifier,
        ),
    ):
        main_module.main()

    engine_factory.assert_not_called()
    strategy.assert_not_called()
    notifier.assert_not_called()
    assert state_writer.call_args.kwargs == {
        "status": "before_cutoff",
        "market_date": "2026-10-09",
    }


def test_explicit_past_date_bypasses_today_cutoff() -> None:
    report = SyncReport(
        status=SYNC_NON_TRADING_DAY,
        target_date="2026-10-09",
        is_trade_day=False,
        expected_symbols=2,
    )
    engine = _Engine(report)
    strategy = Mock(side_effect=AssertionError("strategy must not be constructed"))
    notifier = Mock(side_effect=AssertionError("email must not be constructed"))

    _run_with_engine(
        engine,
        argv=["main.py", "--target-date", "2026-10-09"],
        shanghai_now=datetime(
            2026,
            10,
            10,
            10,
            0,
            tzinfo=ZoneInfo("Asia/Shanghai"),
        ),
        MaVolumeStrategy=strategy,
        EmailNotifier=notifier,
    )

    assert engine.sync_target_date.isoformat() == "2026-10-09"
    strategy.assert_not_called()
    notifier.assert_not_called()


def test_backfill_and_target_date_are_mutually_exclusive() -> None:
    with (
        patch.object(
            sys,
            "argv",
            ["main.py", "--backfill", "--target-date", "2026-10-09"],
        ),
        pytest.raises(SystemExit) as exc_info,
    ):
        main_module.main()

    assert exc_info.value.code == 2


def test_gpt_export_state_is_bound_to_current_workflow_attempt(tmp_path, monkeypatch) -> None:
    state_path = tmp_path / "reports" / "gpt-export-state.json"
    settings = SimpleNamespace(gpt_export_state_path=str(state_path))
    monkeypatch.setenv("GITHUB_RUN_ID", "12345")
    monkeypatch.setenv("GITHUB_RUN_ATTEMPT", "2")

    written = main_module._write_gpt_export_state(
        settings,
        status="complete",
        market_date="2026-10-08",
        expected_symbols=2,
        verified_symbols=2,
        coverage=1.0,
        selection_result_rows=3,
    )

    payload = json.loads(state_path.read_text(encoding="utf-8"))
    assert written == state_path.resolve()
    assert payload["source_run_id"] == "12345"
    assert payload["source_run_attempt"] == "2"
    assert payload["schema_version"] == main_module.GPT_EXPORT_STATE_SCHEMA_VERSION
    assert payload["market_date"] == "2026-10-08"
    assert payload["selection_result_rows"] == 3
    assert not state_path.with_suffix(".json.tmp").exists()

    main_module._clear_gpt_export_state(settings)
    assert not state_path.exists()


def test_gpt_export_state_rejects_non_finite_coverage(tmp_path) -> None:
    state_path = tmp_path / "reports" / "gpt-export-state.json"
    settings = SimpleNamespace(gpt_export_state_path=str(state_path))

    with pytest.raises(ValueError, match="Out of range float values"):
        main_module._write_gpt_export_state(
            settings,
            status="complete",
            market_date="2026-10-08",
            expected_symbols=2,
            verified_symbols=2,
            coverage=float("nan"),
            selection_result_rows=1,
        )

    assert not state_path.exists()
