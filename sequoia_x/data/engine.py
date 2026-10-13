"""数据引擎模块：负责 SQLite 行情数据存储与 baostock 增量同步。"""

import sqlite3
import multiprocessing
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd

from sequoia_x.core.config import Settings
from sequoia_x.core.logger import get_logger

logger = get_logger(__name__)


_CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS stock_daily (
    id       INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol   TEXT    NOT NULL,
    date     TEXT    NOT NULL,
    open     REAL,
    high     REAL,
    low      REAL,
    close    REAL,
    volume   REAL,
    turnover REAL,
    UNIQUE (symbol, date)
);
"""

_CREATE_INDEX_SQL = """
CREATE INDEX IF NOT EXISTS idx_symbol_date ON stock_daily (symbol, date);
"""


class DataIntegrityError(RuntimeError):
    """Raised when current-day market data is incomplete or cannot be verified."""


@dataclass
class BatchResult:
    rows: list[list[str]]
    failed_symbols: list[str]
    errors: list[str]


def _bs_fetch_batch(payload: tuple) -> BatchResult:
    """Fetch one isolated BaoStock batch, reconnecting and retrying boundedly."""
    tasks, max_attempts, backoff_seconds, socket_timeout_seconds = payload
    import time

    import socket

    # BaoStock performs blocking socket I/O. Bound each connect/read so retries
    # can run even when the remote endpoint stops responding.
    socket.setdefaulttimeout(socket_timeout_seconds)
    import baostock as bs

    rows: list[list[str]] = []
    failed: list[str] = []
    errors: list[str] = []

    def login() -> bool:
        result = bs.login()
        if result.error_code != "0":
            errors.append(f"login: {result.error_msg}")
            return False
        return True

    if not login():
        return BatchResult(rows, [task[0] for task in tasks], errors)

    try:
        consecutive_failures = 0
        for index, (symbol, bs_code, start, end) in enumerate(tasks):
            for attempt in range(max_attempts):
                try:
                    result = bs.query_history_k_data_plus(
                        bs_code,
                        "date,open,high,low,close,volume,amount",
                        start_date=start,
                        end_date=end,
                        frequency="d",
                        adjustflag="1",  # 后复权
                    )
                    if result.error_code != "0":
                        raise RuntimeError(result.error_msg)

                    symbol_rows: list[list[str]] = []
                    while result.next():
                        row = result.get_row_data()
                        if len(row) != 7:
                            raise ValueError(f"unexpected BaoStock row width: {len(row)}")
                        symbol_rows.append([symbol] + row)
                    rows.extend(symbol_rows)
                    consecutive_failures = 0
                    break
                except Exception as exc:
                    if attempt + 1 >= max_attempts:
                        failed.append(symbol)
                        errors.append(f"{symbol}: {exc}")
                        consecutive_failures += 1
                        if consecutive_failures >= 5:
                            failed.extend(task[0] for task in tasks[index + 1 :])
                            errors.append("batch stopped after 5 consecutive query failures")
                            return BatchResult(rows, failed, errors)
                        break

                    try:
                        bs.logout()
                    except Exception:
                        pass
                    time.sleep(backoff_seconds * (2**attempt))
                    if not login():
                        failed.append(symbol)
                        failed.extend(task[0] for task in tasks[index + 1 :])
                        return BatchResult(rows, failed, errors)
    finally:
        try:
            bs.logout()
        except Exception:
            pass

    return BatchResult(rows, failed, errors)


def _batch_process_entry(payload: tuple, sender) -> None:
    """Run one batch in a killable process and return its result to the parent."""
    try:
        result = _bs_fetch_batch(payload)
    except Exception as exc:
        tasks = payload[0]
        result = BatchResult(
            [],
            [task[0] for task in tasks],
            [f"batch worker failed: {type(exc).__name__}: {exc}"],
        )
    try:
        sender.send(result)
    except (BrokenPipeError, EOFError, OSError):
        pass
    finally:
        sender.close()


def _stop_process(process) -> None:
    """Terminate a batch worker and reap it, escalating if necessary."""
    if not process.is_alive():
        process.join()
        return
    process.terminate()
    process.join(timeout=3)
    if process.is_alive():
        process.kill()
        process.join(timeout=3)


def _run_batch_with_timeout(payload: tuple, timeout_seconds: int) -> BatchResult:
    """Run one BaoStock batch with a hard wall-clock timeout."""
    tasks = payload[0]
    context = multiprocessing.get_context("spawn")
    receiver, sender = context.Pipe(duplex=False)
    process = context.Process(target=_batch_process_entry, args=(payload, sender))
    started = False
    try:
        process.start()
        started = True
        sender.close()
        deadline = time.monotonic() + timeout_seconds
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                _stop_process(process)
                return BatchResult(
                    [],
                    [task[0] for task in tasks],
                    [f"batch timed out after {timeout_seconds} seconds"],
                )
            if receiver.poll(min(0.25, remaining)):
                try:
                    result = receiver.recv()
                except EOFError:
                    process.join(timeout=1)
                    return BatchResult(
                        [],
                        [task[0] for task in tasks],
                        ["batch worker exited without returning a result"],
                    )
                process.join(timeout=1)
                if process.is_alive():
                    _stop_process(process)
                return result
            if not process.is_alive():
                process.join()
                return BatchResult(
                    [],
                    [task[0] for task in tasks],
                    [f"batch worker exited with code {process.exitcode} without a result"],
                )
    except Exception as exc:
        if started:
            _stop_process(process)
        return BatchResult(
            [],
            [task[0] for task in tasks],
            [f"batch supervisor failed: {type(exc).__name__}: {exc}"],
        )
    finally:
        receiver.close()
        sender.close()
        if started and process.is_alive():
            _stop_process(process)


class DataEngine:
    """行情数据引擎，负责 SQLite 存储和 baostock 数据同步。"""

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.db_path: str = settings.db_path
        self.start_date: str = settings.start_date
        self.verified_symbols: set[str] | None = None
        self.verified_date: str | None = None
        self._init_db()

    def _init_db(self) -> None:
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(_CREATE_TABLE_SQL)
            conn.execute(_CREATE_INDEX_SQL)
            conn.commit()
        logger.info(f"数据库初始化完成：{self.db_path}")

    def _get_last_date(self, symbol: str) -> str | None:
        with sqlite3.connect(self.db_path) as conn:
            row = conn.execute(
                "SELECT MAX(date) FROM stock_daily WHERE symbol = ?",
                (symbol,),
            ).fetchone()
        return row[0] if row and row[0] else None

    def get_ohlcv(self, symbol: str) -> pd.DataFrame:
        if self.verified_symbols is not None and symbol not in self.verified_symbols:
            return pd.DataFrame(
                columns=[
                    "id",
                    "symbol",
                    "date",
                    "open",
                    "high",
                    "low",
                    "close",
                    "volume",
                    "turnover",
                ]
            )
        with sqlite3.connect(self.db_path) as conn:
            df = pd.read_sql(
                "SELECT * FROM stock_daily WHERE symbol = ? ORDER BY date",
                conn,
                params=(symbol,),
            )
        return df

    @staticmethod
    def _to_baostock_code(symbol: str) -> str:
        """将纯数字代码转为 baostock 格式：6/9开头 -> sh，其余 -> sz。"""
        prefix = "sh" if symbol.startswith(("6", "9")) else "sz"
        return f"{prefix}.{symbol}"

    # ── 数据同步 ──

    def sync_today_bulk(
        self,
        expected_symbols: list[str] | None = None,
        target_date: date | None = None,
    ) -> int:
        """Refresh current-day rows and publish only after full-batch validation."""
        today = target_date or datetime.now(ZoneInfo("Asia/Shanghai")).date()
        today_str = today.isoformat()

        with sqlite3.connect(self.db_path) as conn:
            rows = conn.execute(
                "SELECT symbol, MAX(date) FROM stock_daily GROUP BY symbol"
            ).fetchall()

        last_dates = {symbol: last_date for symbol, last_date in rows}
        symbols = sorted(set(expected_symbols) if expected_symbols is not None else last_dates)
        if not symbols:
            raise DataIntegrityError("股票池为空，停止行情同步")

        tasks = []
        for symbol in symbols:
            last_date = last_dates.get(symbol)
            if last_date and last_date < today_str:
                start = (date.fromisoformat(last_date) + timedelta(days=1)).isoformat()
            else:
                # Refresh today's records even if a previous run saved an
                # intraday/incomplete bar before market close.
                start = today_str
            tasks.append((symbol, self._to_baostock_code(symbol), start, today_str))

        expected_count = len(symbols)
        batch_size = self.settings.baostock_batch_size
        batches = [tasks[i : i + batch_size] for i in range(0, len(tasks), batch_size)]
        payloads = [
            (
                batch,
                self.settings.baostock_max_attempts,
                self.settings.baostock_backoff_seconds,
                self.settings.baostock_socket_timeout_seconds,
            )
            for batch in batches
        ]

        logger.info(
            "刷新 %d 只股票的 %s 日行情；批次=%d，并发上限=%d，单股最多尝试=%d 次",
            len(tasks),
            today_str,
            len(batches),
            min(self.settings.baostock_max_workers, len(batches)),
            self.settings.baostock_max_attempts,
        )
        logger.info(
            "行情抓取超时限制：单次网络请求 %d 秒，单批次 %d 秒",
            self.settings.baostock_socket_timeout_seconds,
            self.settings.baostock_batch_timeout_seconds,
        )

        results: list[BatchResult] = []
        failed_symbols: set[str] = set()
        batch_errors: list[str] = []
        failed_batches = 0
        max_workers = min(self.settings.baostock_max_workers, len(batches))
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_batches = {
                executor.submit(
                    _run_batch_with_timeout,
                    payload,
                    self.settings.baostock_batch_timeout_seconds,
                ): (index, batch)
                for index, (payload, batch) in enumerate(zip(payloads, batches), start=1)
            }

            def cancel_queued_batches() -> None:
                for pending, (_index, pending_batch) in future_batches.items():
                    if pending.cancel():
                        failed_symbols.update(task[0] for task in pending_batch)

            completed_batches = 0
            for future in as_completed(future_batches):
                batch_index, batch = future_batches[future]
                completed_batches += 1
                try:
                    result = future.result()
                except Exception as exc:
                    failed_symbols.update(task[0] for task in batch)
                    batch_errors.append(str(exc))
                    logger.warning(
                        "行情批次进度 %d/%d：批次 %d/%d 异常：%s",
                        completed_batches,
                        len(batches),
                        batch_index,
                        len(batches),
                        exc,
                    )
                    if not future.cancelled():
                        failed_batches += 1
                        if failed_batches >= max_workers:
                            logger.error("多个行情批次全部失败，取消尚未开始的批次")
                            cancel_queued_batches()
                    continue
                results.append(result)
                failed_symbols.update(result.failed_symbols)
                batch_errors.extend(result.errors[:3])
                logger.info(
                    "行情批次进度 %d/%d：批次 %d/%d 完成，有效行=%d，失败股票=%d",
                    completed_batches,
                    len(batches),
                    batch_index,
                    len(batches),
                    len(result.rows),
                    len(result.failed_symbols),
                )
                if len(result.failed_symbols) == len(batch):
                    failed_batches += 1
                    if failed_batches >= max_workers:
                        logger.error("多个行情批次全部失败，取消尚未开始的批次")
                        cancel_queued_batches()

        all_rows = [row for result in results for row in result.rows]
        df = pd.DataFrame(
            all_rows,
            columns=["symbol", "date", "open", "high", "low", "close", "volume", "turnover"],
        )
        if not df.empty:
            for col in ["open", "high", "low", "close", "volume", "turnover"]:
                df[col] = pd.to_numeric(df[col], errors="coerce")
            df["date"] = df["date"].astype(str)
            df = df.dropna(subset=["close", "volume"])
            df = df[(df["close"] > 0) & (df["volume"] > 0) & (df["date"] <= today_str)]
            df = df.drop_duplicates(subset=["symbol", "date"], keep="last")

        received_current = (
            set(df.loc[df["date"] == today_str, "symbol"].astype(str))
            if not df.empty
            else set()
        )
        # Validate only rows returned by this run. Cached today's rows are not
        # trusted because an earlier workflow may have run before market close.
        coverage = len(received_current) / expected_count
        minimum = self.settings.min_market_data_coverage
        if coverage < minimum:
            error_summary = "; ".join(batch_errors[:5])
            logger.error(
                "%s 行情覆盖率 %.2f%% 低于门槛 %.2f%%；失败股票=%d/%d；本轮数据不发布",
                today_str,
                coverage * 100,
                minimum * 100,
                len(failed_symbols),
                expected_count,
            )
            raise DataIntegrityError(
                f"{today_str} 行情覆盖率 {coverage:.2%} 低于门槛 {minimum:.2%}；"
                f"失败股票={len(failed_symbols)}；{error_summary}"
            )

        # Atomic publication: replace only after validation. Never delete an
        # entire date and then leave it with a partial set of symbols.
        columns = ["symbol", "date", "open", "high", "low", "close", "volume", "turnover"]
        values = [
            tuple(None if pd.isna(value) else value for value in row)
            for row in df[columns].itertuples(index=False, name=None)
        ]
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("DELETE FROM stock_daily WHERE date = ?", (today_str,))
            conn.executemany(
                """
                INSERT INTO stock_daily
                    (symbol, date, open, high, low, close, volume, turnover)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(symbol, date) DO UPDATE SET
                    open=excluded.open, high=excluded.high, low=excluded.low,
                    close=excluded.close, volume=excluded.volume,
                    turnover=excluded.turnover
                """,
                values,
            )
            conn.commit()

        self.verified_symbols = received_current
        self.verified_date = today_str
        logger.info(
            "sync_today_bulk: 发布 %d 条有效数据，%s 覆盖率 %.2f%%，失败股票=%d",
            len(df),
            today_str,
            coverage * 100,
            len(failed_symbols),
        )
        return len(df)

    def is_trading_day(self, target_date: date | None = None) -> bool:
        """Check BaoStock's exchange calendar; fail closed if unavailable."""
        import baostock as bs

        target = (target_date or datetime.now(ZoneInfo("Asia/Shanghai")).date()).isoformat()
        login = bs.login()
        if login.error_code != "0":
            raise DataIntegrityError(f"BaoStock 登录失败，无法读取交易日历：{login.error_msg}")
        try:
            result = bs.query_trade_dates(start_date=target, end_date=target)
            if result.error_code != "0":
                raise DataIntegrityError(f"BaoStock 交易日历查询失败：{result.error_msg}")
            while result.next():
                row = result.get_row_data()
                if row and row[0] == target:
                    return row[1] == "1"
            raise DataIntegrityError(f"BaoStock 未返回 {target} 的交易日状态")
        finally:
            bs.logout()

    def backfill(self, symbols: list[str]) -> None:
        """通过 baostock 批量回填历史日 K 线数据（后复权）。

        容错机制：
        - 单只股票失败自动重试 3 次，间隔递增（2s/4s/8s）
        - 每 200 只股票自动重连 baostock（防止长连接超时）
        - 已入库的自动 skip，中断后可重跑续传
        """
        import time
        from datetime import date, timedelta

        import baostock as bs

        today_str = date.today().strftime("%Y-%m-%d")
        max_retries = 3
        reconnect_interval = 200  # 每处理 N 只股票重连一次

        def _login():
            lg = bs.login()
            if lg.error_code != "0":
                logger.error(f"baostock 登录失败: {lg.error_msg}")
                return False
            return True

        if not _login():
            return

        success = 0
        skipped = 0
        failed = 0
        since_reconnect = 0

        try:
            for i, symbol in enumerate(symbols):
                last_date = self._get_last_date(symbol)
                if last_date and last_date >= today_str:
                    skipped += 1
                    if (i + 1) % 500 == 0:
                        logger.info(
                            f"已处理 {i + 1}/{len(symbols)}，"
                            f"成功 {success} 跳过 {skipped} 失败 {failed}"
                        )
                    continue

                # 定期重连，防止长连接超时
                since_reconnect += 1
                if since_reconnect >= reconnect_interval:
                    bs.logout()
                    time.sleep(1)
                    if not _login():
                        logger.error("重连失败，终止回填")
                        return
                    since_reconnect = 0

                start = last_date or self.start_date
                if last_date:
                    start = (date.fromisoformat(last_date) + timedelta(days=1)).strftime("%Y-%m-%d")

                bs_code = self._to_baostock_code(symbol)

                # 带重试的查询
                rows = []
                query_ok = False
                for attempt in range(max_retries):
                    try:
                        rs = bs.query_history_k_data_plus(
                            bs_code,
                            "date,open,high,low,close,volume,amount",
                            start_date=start,
                            end_date=today_str,
                            frequency="d",
                            adjustflag="1",  # 后复权
                        )

                        if rs.error_code != "0":
                            raise RuntimeError(rs.error_msg)

                        rows = []
                        while rs.next():
                            rows.append(rs.get_row_data())
                        query_ok = True
                        break

                    except Exception as exc:
                        if attempt < max_retries - 1:
                            wait = 2 ** (attempt + 1)
                            logger.warning(
                                f"[{symbol}] 第{attempt + 1}次失败: {exc}，{wait}s 后重试"
                            )
                            time.sleep(wait)
                            # 重连 baostock
                            bs.logout()
                            time.sleep(1)
                            _login()
                        else:
                            logger.warning(f"[{symbol}] {max_retries}次重试均失败，跳过")

                if not query_ok:
                    failed += 1
                    continue

                if not rows:
                    skipped += 1
                    continue

                df = pd.DataFrame(rows, columns=rs.fields)
                for col in ["open", "high", "low", "close", "volume", "amount"]:
                    df[col] = pd.to_numeric(df[col], errors="coerce")
                df = df.dropna(subset=["close"])
                df = df[df["volume"] > 0]

                if df.empty:
                    skipped += 1
                    continue

                df["symbol"] = symbol
                df = df.rename(columns={"amount": "turnover"})
                df = df[["symbol", "date", "open", "high", "low", "close", "volume", "turnover"]]

                try:
                    with sqlite3.connect(self.db_path) as conn:
                        df.to_sql(
                            "stock_daily", conn, if_exists="append",
                            index=False, method="multi", chunksize=500,
                        )
                except sqlite3.IntegrityError:
                    pass

                success += 1

                if (i + 1) % 500 == 0:
                    logger.info(
                        f"已处理 {i + 1}/{len(symbols)}，"
                        f"成功 {success} 跳过 {skipped} 失败 {failed}"
                    )

        finally:
            bs.logout()

        logger.info(f"回填完成 — 成功: {success} | 跳过: {skipped} | 失败: {failed}")

    # ── 股票列表 ──

    def get_all_symbols(self) -> list[str]:
        """通过 baostock 获取全市场 A 股代码列表。"""
        import baostock as bs

        lg = bs.login()
        if lg.error_code != "0":
            logger.error(f"baostock 登录失败: {lg.error_msg}")
            return []

        try:
            rs = bs.query_stock_basic(code_name="", code="")
            symbols = []
            while rs.next():
                row = rs.get_row_data()
                code = row[0]           # "sh.600000" or "sz.000001"
                status = row[4]         # "1" = 上市
                stock_type = row[5]     # "1" = 股票
                if status == "1" and stock_type == "1":
                    symbols.append(code.split(".")[1])  # 提取纯数字代码
            logger.info(f"获取股票列表完成，共 {len(symbols)} 只")
            return symbols
        except Exception as e:
            logger.error(f"获取股票列表失败: {e}")
            return []
        finally:
            bs.logout()

    def get_local_symbols(self) -> list[str]:
        with sqlite3.connect(self.db_path) as conn:
            rows = conn.execute(
                "SELECT DISTINCT symbol FROM stock_daily"
            ).fetchall()
        symbols = [row[0] for row in rows]
        if self.verified_symbols is not None:
            return [symbol for symbol in symbols if symbol in self.verified_symbols]
        return symbols
