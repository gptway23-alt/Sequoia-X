"""Sequoia-X V2 主程序入口。

两种运行模式：
  python main.py               # 日常模式：增量补数据 + 跑策略 + 邮件推送
  python main.py --backfill    # 回填模式：baostock 拉全市场历史K线
"""

import argparse
import socket
import sys

from dotenv import load_dotenv

load_dotenv()
socket.setdefaulttimeout(10.0)

from sequoia_x.core.config import get_settings
from sequoia_x.core.logger import get_logger
from sequoia_x.data.engine import DataEngine
from sequoia_x.notify.email import EmailNotifier
from sequoia_x.strategy.base import BaseStrategy
from sequoia_x.strategy.high_tight_flag import HighTightFlagStrategy
from sequoia_x.strategy.limit_up_shakeout import LimitUpShakeoutStrategy
from sequoia_x.strategy.ma_volume import MaVolumeStrategy
from sequoia_x.strategy.private_placement import PrivatePlacementStrategy
from sequoia_x.strategy.rps_breakout import RpsBreakoutStrategy
from sequoia_x.strategy.turtle_trade import TurtleTradeStrategy
from sequoia_x.strategy.uptrend_limit_down import UptrendLimitDownStrategy


SUPPORTED_PREFIXES = (
    "000", "001", "002", "003",      # 深市主板
    "300", "301",                    # 创业板
    "600", "601", "603", "605",      # 沪市主板
)


def main() -> None:
    parser = argparse.ArgumentParser(description="Sequoia-X V2 选股系统")
    parser.add_argument(
        "--backfill",
        action="store_true",
        help="回填模式：通过 baostock 拉取全市场历史 K 线",
    )
    args = parser.parse_args()

    try:
        # 1. 初始化配置
        settings = get_settings()

        # 2. 初始化日志
        logger = get_logger(__name__)
        logger.info("Sequoia-X V2 启动")

        # 3. 初始化数据引擎
        engine = DataEngine(settings)

        if args.backfill:
            logger.info("进入回填模式...")

            all_symbols = [
                str(symbol).zfill(6)
                for symbol in engine.get_all_symbols()
                if str(symbol).zfill(6).startswith(SUPPORTED_PREFIXES)
            ]

            chinext_count = sum(
                symbol.startswith(("300", "301"))
                for symbol in all_symbols
            )

            logger.info(
                f"支持股票池共 {len(all_symbols)} 只，"
                f"其中创业板 {chinext_count} 只"
            )

            engine.backfill(all_symbols)

            logger.info("Sequoia-X V2 回填模式运行完成")
            return

        # 4. 日常模式：检查并补齐股票池
        logger.info("检查本地股票池完整性...")

        all_symbols = [
            str(symbol).zfill(6)
            for symbol in engine.get_all_symbols()
            if str(symbol).zfill(6).startswith(SUPPORTED_PREFIXES)
        ]

        local_symbols = {
            str(symbol).zfill(6)
            for symbol in engine.get_local_symbols()
        }

        missing_symbols = [
            symbol
            for symbol in all_symbols
            if symbol not in local_symbols
        ]

        if missing_symbols:
            chinext_count = sum(
                symbol.startswith(("300", "301"))
                for symbol in missing_symbols
            )

            logger.info(
                f"检测到 {len(missing_symbols)} 只缺失股票，"
                f"其中创业板 {chinext_count} 只，开始回填..."
            )

            engine.backfill(missing_symbols)

            logger.info(
                f"股票池补齐完成，本轮补齐 "
                f"{len(missing_symbols)} 只股票"
            )
        else:
            logger.info("本地股票池已完整，无需补齐")

        # 5. 增量同步
        logger.info("开始拉取最新快照...")
        count = engine.sync_today_bulk()
        logger.info(f"快照同步完成，写入 {count} 只股票")

        # 6. 策略列表
        strategies: list[BaseStrategy] = [
            MaVolumeStrategy(engine=engine, settings=settings),
            TurtleTradeStrategy(engine=engine, settings=settings),
            HighTightFlagStrategy(engine=engine, settings=settings),
            LimitUpShakeoutStrategy(engine=engine, settings=settings),
            UptrendLimitDownStrategy(engine=engine, settings=settings),
            RpsBreakoutStrategy(engine=engine, settings=settings),
            PrivatePlacementStrategy(engine=engine, settings=settings),
        ]

        notifier = EmailNotifier(settings)

        # 7. 执行策略
        for strategy in strategies:
            strategy_name = type(strategy).__name__

            logger.info(f"执行策略：{strategy_name}")

            selected: list[str] = strategy.run()

            logger.info(
                f"{strategy_name} 选出 {len(selected)} 只股票"
            )

            if selected:
                notifier.send(
                    symbols=selected,
                    strategy_name=strategy_name,
                    webhook_key=strategy.webhook_key,
                )
            else:
                logger.info(
                    f"{strategy_name} 无选股结果，跳过推送"
                )

    except Exception:
        try:
            _logger = get_logger(__name__)
            _logger.exception(
                "主流程发生未捕获异常，程序终止"
            )
        except Exception:
            import traceback
            traceback.print_exc()

        sys.exit(1)

    logger.info("Sequoia-X V2 运行完成")


if __name__ == "__main__":
    main()
