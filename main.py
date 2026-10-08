"""Sequoia-X V2 主程序入口。

两种运行模式：
  python main.py               # 日常模式：增量补数据 + 跑策略 + 邮件推送
  python main.py --backfill    # 回填模式：baostock 拉全市场历史K线
"""

import argparse
import sys

from dotenv import load_dotenv

load_dotenv()

from sequoia_x.core.config import get_settings
from sequoia_x.core.logger import get_logger
from sequoia_x.data import engine as data_engine
from sequoia_x.notify.email import EmailNotifier
from sequoia_x.strategy.base import BaseStrategy
from sequoia_x.strategy.high_tight_flag import HighTightFlagStrategy
from sequoia_x.strategy.limit_up_shakeout import LimitUpShakeoutStrategy
from sequoia_x.strategy.ma_volume import MaVolumeStrategy
from sequoia_x.strategy.private_placement import PrivatePlacementStrategy
from sequoia_x.strategy.rps_breakout import RpsBreakoutStrategy
from sequoia_x.strategy.turtle_trade import TurtleTradeStrategy
from sequoia_x.strategy.uptrend_limit_down import UptrendLimitDownStrategy


REQUIRED_ENGINE_API_VERSION = 2
if getattr(data_engine, "ENGINE_API_VERSION", 0) != REQUIRED_ENGINE_API_VERSION:
    raise ImportError(
        "Sequoia-X 源码版本不一致：main.py 需要 engine API 2。"
        "请同时覆盖 main.py 与 sequoia_x/data/engine.py，不能只更新其中一个文件。"
    )

SYNC_COMPLETE = data_engine.SYNC_COMPLETE
SYNC_NON_TRADING_DAY = data_engine.SYNC_NON_TRADING_DAY
DataEngine = data_engine.DataEngine
DataIntegrityError = data_engine.DataIntegrityError


SUPPORTED_PREFIXES = (
    "000", "001", "002", "003",      # 深市主板
    "300", "301",                    # 创业板
    "600", "601", "603", "605",      # 沪市主板
)


def _supported_symbols(symbols: list[str]) -> list[str]:
    """规范化并去重受支持的 A 股代码。"""
    return sorted(
        {
            str(symbol).zfill(6)
            for symbol in symbols
            if str(symbol).zfill(6).startswith(SUPPORTED_PREFIXES)
        }
    )


def _require_complete_local_universe(
    engine: DataEngine,
    expected_symbols: list[str],
) -> None:
    """回填后重新检查本地股票池，禁止把部分成功记录成完整。"""
    local_symbols = {str(symbol).zfill(6) for symbol in engine.get_local_symbols()}
    missing = sorted(set(expected_symbols) - local_symbols)
    if missing:
        preview = ", ".join(missing[:10])
        raise DataIntegrityError(
            f"本地股票池仍缺失 {len(missing)} 只股票"
            + (f"（示例：{preview}）" if preview else "")
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

            all_symbols = _supported_symbols(engine.get_all_symbols())
            if not all_symbols:
                raise DataIntegrityError("远端股票池经过支持范围过滤后为空")

            chinext_count = sum(
                symbol.startswith(("300", "301"))
                for symbol in all_symbols
            )

            logger.info(
                f"支持股票池共 {len(all_symbols)} 只，"
                f"其中创业板 {chinext_count} 只"
            )

            report = engine.backfill(all_symbols)
            _require_complete_local_universe(engine, all_symbols)
            if not report.complete:
                raise DataIntegrityError(
                    f"历史回填仍有 {len(report.failed_symbols)} 只股票失败"
                )

            logger.info("Sequoia-X V2 回填模式运行完成")
            return

        # 4. 日常模式：检查并补齐股票池
        logger.info("检查本地股票池完整性...")

        all_symbols = _supported_symbols(engine.get_all_symbols())
        if not all_symbols:
            raise DataIntegrityError("远端股票池经过支持范围过滤后为空")

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

            backfill_report = engine.backfill(missing_symbols)
            _require_complete_local_universe(engine, all_symbols)
            if not backfill_report.complete:
                raise DataIntegrityError(
                    f"股票池补齐仍有 {len(backfill_report.failed_symbols)} 只失败"
                )

            logger.info(
                f"股票池补齐完成，本轮补齐 "
                f"{len(missing_symbols)} 只股票"
            )
        else:
            logger.info("本地股票池已完整，无需补齐")

        # 5. 增量同步
        logger.info("开始拉取最新快照...")
        sync_report = engine.sync_today_bulk(expected_symbols=all_symbols)
        if sync_report.status == SYNC_NON_TRADING_DAY:
            logger.info(
                f"{sync_report.target_date} 不是交易日；不运行策略，不生成或发送 TXT"
            )
            return
        if sync_report.status != SYNC_COMPLETE:
            raise DataIntegrityError(
                f"{sync_report.target_date} 行情未通过门禁："
                f"状态={sync_report.status}，覆盖率={sync_report.coverage:.2%}，"
                f"失败={len(sync_report.failed_symbols)}，陈旧={len(sync_report.stale_symbols)}"
            )

        verified_symbols = set(sync_report.verified_symbols)
        if not verified_symbols:
            raise DataIntegrityError("本轮没有任何经过当日行情核验的股票")
        engine.pin_strategy_snapshot(sync_report.target_date, verified_symbols)
        logger.info(
            f"快照核验完成：{len(verified_symbols)}/{sync_report.expected_symbols} 只，"
            f"覆盖率 {sync_report.coverage:.2%}，写入 {sync_report.rows_written} 条"
        )

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

        # 7. 执行策略
        strategy_results: dict[str, list[str]] = {}
        for strategy in strategies:
            strategy_name = type(strategy).__name__

            logger.info(f"执行策略：{strategy_name}")

            raw_selected: list[str] = strategy.run()
            normalized_selected = {
                str(symbol).strip().zfill(6)
                for symbol in raw_selected
            }
            selected = sorted(normalized_selected & verified_symbols)
            rejected = len(normalized_selected - verified_symbols)
            if rejected > 0:
                logger.warning(
                    f"{strategy_name} 丢弃 {rejected} 个未通过当日行情核验的结果"
                )

            logger.info(
                f"{strategy_name} 选出 {len(selected)} 只股票"
            )

            if selected:
                strategy_results[strategy_name] = selected
            else:
                logger.info(
                    f"{strategy_name} 无已核验选股结果"
                )

        if strategy_results:
            notifier = EmailNotifier(settings)
            send_result = notifier.send_report(
                strategy_results=strategy_results,
                verified_symbols=verified_symbols,
                market_date=sync_report.target_date,
            )
            logger.info(
                f"TXT 邮件处理完成：状态={send_result.status}，"
                f"股票数={send_result.symbol_count}，文件={send_result.report_path}"
            )
        else:
            logger.info("所有策略均无已核验结果，不生成或发送 TXT")

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
