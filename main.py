"""Sequoia-X V2 主程序入口。

两种运行模式：
  python main.py               # 日常模式：增量补数据 + 跑策略 + 邮件推送
  python main.py --backfill    # 回填模式：baostock 拉全市场历史K线
"""

import argparse
import socket
import sys
from datetime import date, datetime, time
from zoneinfo import ZoneInfo

from dotenv import load_dotenv

load_dotenv()
socket.setdefaulttimeout(10.0)

from sequoia_x.core.config import get_settings
from sequoia_x.core.daily_schedule import latest_completed_weekday
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


def _after_market_close(now: datetime | None = None) -> bool:
    """Only run daily stock selection after the Shanghai market has closed."""
    shanghai = ZoneInfo("Asia/Shanghai")
    current = now or datetime.now(shanghai)
    if current.tzinfo is None:
        current = current.replace(tzinfo=shanghai)
    else:
        current = current.astimezone(shanghai)
    return current.weekday() < 5 and current.time() >= time(15, 5)


def main() -> None:
    parser = argparse.ArgumentParser(description="Sequoia-X V2 选股系统")
    parser.add_argument(
        "--backfill",
        action="store_true",
        help="回填模式：通过 baostock 拉取全市场历史 K 线",
    )
    parser.add_argument(
        "--catch-up",
        action="store_true",
        help="处理最近已收盘的工作日（用于延迟的定时任务）",
    )
    parser.add_argument(
        "--trade-date",
        help="明确指定要处理的交易日，格式 YYYY-MM-DD",
    )
    args = parser.parse_args()
    if args.catch_up and args.trade_date:
        parser.error("--catch-up 和 --trade-date 不能同时使用")

    notification_failures: list[str] = []

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

        shanghai_now = datetime.now(ZoneInfo("Asia/Shanghai"))
        if args.trade_date:
            try:
                trade_date = date.fromisoformat(args.trade_date)
            except ValueError:
                parser.error("--trade-date 必须是有效的 YYYY-MM-DD 日期")
            if trade_date > shanghai_now.date():
                parser.error("--trade-date 不能是未来日期")
        elif args.catch_up:
            trade_date = latest_completed_weekday(shanghai_now)
        elif _after_market_close(shanghai_now):
            trade_date = shanghai_now.date()
        else:
            logger.info("当前不在上海时间工作日 15:05 后的运行时段，跳过行情同步和选股")
            return

        engine.verified_date = trade_date.isoformat()
        logger.info("本次目标交易日：%s", trade_date.isoformat())

        if engine.daily_run_completed(trade_date):
            logger.info("目标交易日 %s 已成功完成，跳过重复运行", trade_date.isoformat())
            return

        if not engine.is_trading_day(trade_date):
            if args.catch_up:
                # A weekday may still be an exchange holiday. Walk back to
                # the most recent actual trading session so holidays do not
                # prevent a delayed run from recovering the prior session.
                candidate = trade_date
                for _ in range(14):
                    candidate = date.fromordinal(candidate.toordinal() - 1)
                    while candidate.weekday() >= 5:
                        candidate = date.fromordinal(candidate.toordinal() - 1)
                    if engine.is_trading_day(candidate):
                        trade_date = candidate
                        engine.verified_date = trade_date.isoformat()
                        logger.info("休市日回退后，本次目标交易日：%s", trade_date)
                        break
                else:
                    logger.info("最近 14 个工作日内未找到可补跑的 A 股交易日，跳过")
                    return
            else:
                logger.info("目标日期 %s 不是 A 股交易日，跳过行情同步和选股", trade_date)
                return

            if engine.daily_run_completed(trade_date):
                logger.info("目标交易日 %s 已成功完成，跳过重复运行", trade_date.isoformat())
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
        logger.info("开始刷新当日收盘日线...")
        count = engine.sync_today_bulk(expected_symbols=all_symbols, target_date=trade_date)
        logger.info(f"行情校验完成，写入 {count} 条日线记录")

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
        strategy_results: dict[str, list[str]] = {}
        verified_symbols = getattr(engine, "verified_symbols", None)

        # 7. 执行策略
        for strategy in strategies:
            strategy_name = type(strategy).__name__

            logger.info(f"执行策略：{strategy_name}")

            selected: list[str] = strategy.run()

            if verified_symbols is not None:
                before_filter = len(selected)
                selected = [symbol for symbol in selected if symbol in verified_symbols]
                if len(selected) < before_filter:
                    logger.warning(
                        "%s 移除 %d 只当日行情未核验股票",
                        strategy_name,
                        before_filter - len(selected),
                    )

            logger.info(
                f"{strategy_name} 选出 {len(selected)} 只股票"
            )

            if selected:
                strategy_results[strategy_name] = selected
            else:
                logger.info(
                    f"{strategy_name} 无选股结果，跳过推送"
                )

        if strategy_results:
            selected_symbols = list(
                dict.fromkeys(
                    symbol
                    for symbols in strategy_results.values()
                    for symbol in symbols
                )
            )
            sent = notifier.send(
                symbols=selected_symbols,
                strategy_name="DailySummary",
                strategy_results=strategy_results,
                verified_symbols=verified_symbols,
                trade_date=trade_date,
            )
            if not sent:
                notification_failures.append("DailySummary")
        else:
            logger.info("所有策略均无已核验选股结果，跳过邮件")

        if not notification_failures:
            engine.mark_daily_run_completed(trade_date)
            logger.info("目标交易日 %s 已标记完成", trade_date.isoformat())

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

    if notification_failures:
        logger.error(
            "行情与策略流程已完成，但邮件发送失败：%s",
            ", ".join(notification_failures),
        )
        sys.exit(2)

    logger.info("Sequoia-X V2 运行完成")


if __name__ == "__main__":
    main()
