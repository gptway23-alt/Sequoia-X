"""历史重跑不得读取策略快照日期之后的定增数据。"""

from __future__ import annotations

import sys
from types import SimpleNamespace

import pandas as pd

from sequoia_x.strategy.private_placement import PrivatePlacementStrategy


def test_private_placement_excludes_future_events_for_historical_snapshot(monkeypatch) -> None:
    frame = pd.DataFrame(
        [
            {"发行方式": "定向增发", "发行日期": "2026-10-05", "股票代码": "000001"},
            {"发行方式": "定向增发", "发行日期": "2026-10-09", "股票代码": "600519"},
            {"发行方式": "定向增发", "发行日期": "2026-09-20", "股票代码": "300001"},
        ]
    )
    monkeypatch.setitem(
        sys.modules,
        "akshare",
        SimpleNamespace(stock_qbzf_em=lambda: frame.copy()),
    )
    engine = SimpleNamespace(
        strategy_snapshot_date="2026-10-08",
        get_local_symbols=lambda: ["000001", "600519", "300001"],
    )

    selected = PrivatePlacementStrategy(
        engine=engine,
        settings=SimpleNamespace(),
    ).run()

    assert selected == ["000001"]

