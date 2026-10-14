"""SQLite market data engine with BaoStock primary and AKShare fallback."""

import multiprocessing
import sqlite3
import time
from concurrent.futures import CancelledError, ThreadPoolExecutor, as_completed
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


def _akshare_fetch_batch(payload: tuple) -> BatchResult:
    """Fetch bounded daily-history fallback rows from Tencent through AKShare."""
    tasks, max_attempts, backoff_seconds, request_timeout_seconds = payload
    import akshare as ak

    rows: list[list[str]] = []
    failed: list[str] = []
    errors: list[str] = []

    for symbol, _bs_code, start, end in tasks:
        for attempt in range(max_attempts):
            try:
                frame = ak.stock_zh_a_hist_tx(
                    symbol=symbol,
                    start_date=start.replace("-", ""),
                    end_date=end.replace("-", ""),
                    adjust="hfq",
                    timeout=request_timeout_seconds,
                )
                if frame is None or frame.empty:
                    raise RuntimeError("Tencent history returned no rows")
                required = {"date", "open", "high", "low", "close", "volume", "amount"}
                if not required.issubset(frame.columns):
                    raise ValueError(
                        f"Tencent history missing columns: {sorted(required - set(frame.columns))}"
                    )
                for row in frame[
                    ["date", "open", "high", "low", "close", "volume", "amount"]
                ].itertuples(index=False, name=None):
                    raw_date, open_, high, low, close, volume, amount = row
                    parsed_date = pd.to_datetime(raw_date, errors="coerce")
                    if pd.isna(parsed_date):
                        continue
                    rows.append(
                        [
                            symbol,
                            parsed_date.date().isoformat(),
                            str(open_),
                            str(high),
                            str(low),
                            str(close),
                            str(volume),
                            str(amount),
                        ]
                    )
                break
            except Exception as exc:
                if attempt + 1 >= max_attempts:
                    failed.append(symbol)
                    errors.append(f"{symbol}: {type(exc).__name__}: {exc}")
                    break
                delay = backoff_seconds * (2**attempt)
                if delay:
                    time.sleep(delay)

    return BatchResult(rows, failed, errors)


def _akshare_calendar_dates(_payload: object = None) -> list[str]:
    """Read the Sina exchange calendar via AKShare and return ISO date strings."""
    import akshare as ak

    frame = ak.tool_trade_date_hist_sina()
    if frame is None or frame.empty or "trade_date" not in frame.columns:
        raise RuntimeError("Sina trade calendar is empty or has no trade_date column")
    parsed = pd.to_datetime(frame["trade_date"], errors="coerce").dropna()
    dates = sorted({value.date().isoformat() for value in parsed})
    if not dates:
        raise RuntimeError("Sina trade calendar contains no valid dates")
    return dates


def _akshare_stock_symbols(_payload: object = None) -> list[str]:
    """Read the current A-share code list through AKShare."""
    import akshare as ak

    frame = ak.stock_info_a_code_name()
    if frame is None or frame.empty or "code" not in frame.columns:
        raise RuntimeError("AKShare stock list is empty or has no code column")
    symbols = frame["code"].astype(str).str.extract(r"(\d{6})", expand=False).dropna()
    result = sorted(set(symbols.tolist()))
    if not result:
        raise RuntimeError("AKShare stock list contains no valid six-digit codes")
    return result


def _isolated_task_entry(target, payload: object, sender) -> None:
    """Run an AKShare task in a process that the caller can stop on timeout."""
    try:
        sender.send((True, target(payload)))
    except Exception as exc:
        try:
            sender.send((False, f"{type(exc).__name__}: {exc}"))
        except (BrokenPipeError, EOFError, OSError):
            pass
    finally:
        sender.close()


def _run_isolated_task(target, payload: object, timeout_seconds: int):
    """Run one potentially blocking AKShare operation with a hard deadline."""
    context = multiprocessing.get_context("spawn")
    receiver, sender = context.Pipe(duplex=False)
    process = context.Process(
        target=_isolated_task_entry,
        args=(target, payload, sender),
    )
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
                raise TimeoutError(f"AKShare task timed out after {timeout_seconds} seconds")
            if receiver.poll(min(0.25, remaining)):
                try:
                    succeeded, result = receiver.recv()
                except EOFError as exc:
                    raise RuntimeError("AKShare worker exited without returning a result") from exc
                process.join(timeout=1)
                if process.is_alive():
                    _stop_process(process)
                if not succeeded:
                    raise RuntimeError(f"AKShare worker failed: {result}")
                return result
            if not process.is_alive():
                process.join()
                raise RuntimeError(
                    f"AKShare worker exited with code {process.exitcode} without a result"
                )
    finally:
        receiver.close()
        sender.close()
        if started and process.is_alive():
            _stop_process(process)


def _run_akshare_batch_with_timeout(payload: tuple, timeout_seconds: int) -> BatchResult:
    """Run one AKShare fallback batch in a killable worker process."""
    tasks = payload[0]
    try:
        result = _run_isolated_task(_akshare_fetch_batch, payload, timeout_seconds)
        if not isinstance(result, BatchResult):
            raise TypeError("AKShare history worker returned an invalid result")
        return result
    except Exception as exc:
        return BatchResult(
            [],
            [task[0] for task in tasks],
            [f"AKShare fallback batch failed: {type(exc).__name__}: {exc}"],
        )


def _run_akshare_calendar_with_timeout(timeout_seconds: int) -> list[str]:
    return _run_isolated_task(_akshare_calendar_dates, None, timeout_seconds)


def _run_akshare_symbols_with_timeout(timeout_seconds: int) -> list[str]:
    return _run_isolated_task(_akshare_stock_symbols, None, timeout_seconds)


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


def _normalize_daily_rows(rows: list[list[str]], end_date: str) -> pd.DataFrame:
    """Normalize provider rows and discard dates or bars that cannot be verified."""
    columns = ["symbol", "date", "open", "high", "low", "close", "volume", "turnover"]
    frame = pd.DataFrame(rows, columns=columns)
    if frame.empty:
        return frame

    frame["symbol"] = frame["symbol"].astype(str).str.zfill(6)
    parsed_dates = pd.to_datetime(frame["date"], errors="coerce")
    frame["date"] = parsed_dates.dt.strftime("%Y-%m-%d")
    numeric_columns = ["open", "high", "low", "close", "volume", "turnover"]
    for column in numeric_columns:
        frame[column] = pd.to_numeric(frame[column], errors="coerce")

    frame = frame.replace([float("inf"), float("-inf")], pd.NA)
    frame = frame.dropna(subset=["symbol", "date", *numeric_columns])
    frame = frame[
        (frame["date"] <= end_date)
        & (frame["open"] > 0)
        & (frame["high"] > 0)
        & (frame["low"] > 0)
        & (frame["close"] > 0)
        & (frame["volume"] > 0)
        & (frame["turnover"] > 0)
        & (frame["high"] >= frame[["open", "close", "low"]].max(axis=1))
        & (frame["low"] <= frame[["open", "close", "high"]].min(axis=1))
    ]
    return frame.drop_duplicates(subset=["symbol", "date"], keep="last")


class DataEngine:
    """SQLite data engine for BaoStock sync, AKShare fallback, and backfill."""

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
        requested_start_dates = {task[0]: task[2] for task in tasks}
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
        batch_errors: list[str] = []
        failed_batch_streak = 0
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

            scheduling_stopped = False

            def cancel_queued_batches() -> None:
                nonlocal scheduling_stopped
                if scheduling_stopped:
                    return
                scheduling_stopped = True
                for pending in future_batches:
                    pending.cancel()

            completed_batches = 0
            cancelled_batches = 0

            def note_full_batch_failure() -> None:
                nonlocal failed_batch_streak
                failed_batch_streak += 1
                if failed_batch_streak >= max_workers:
                    logger.error(
                        "连续 %d 个行情批次全部失败，取消尚未开始的批次",
                        failed_batch_streak,
                    )
                    cancel_queued_batches()

            for future in as_completed(future_batches):
                batch_index, batch = future_batches[future]
                if future.cancelled():
                    cancelled_batches += 1
                    continue
                try:
                    result = future.result()
                except CancelledError:
                    cancelled_batches += 1
                    continue
                except Exception as exc:
                    completed_batches += 1
                    batch_errors.append(str(exc))
                    logger.warning(
                        "行情批次进度 %d/%d：批次 %d/%d 异常：%s",
                        completed_batches,
                        len(batches),
                        batch_index,
                        len(batches),
                        exc,
                    )
                    note_full_batch_failure()
                    continue

                completed_batches += 1
                results.append(result)
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
                    note_full_batch_failure()
                else:
                    # A successful or partially successful batch breaks the
                    # global-outage streak; isolated failed batches stay isolated.
                    failed_batch_streak = 0

            if cancelled_batches:
                logger.warning(
                    "行情抓取实际完成 %d/%d 批，另有 %d 批因连续失败取消",
                    completed_batches,
                    len(batches),
                    cancelled_batches,
                )

        all_rows = [row for result in results for row in result.rows]
        df = _normalize_daily_rows(all_rows, today_str)
        df = df[df["symbol"].isin(symbols)]
        if not df.empty:
            df["requested_start"] = df["symbol"].map(requested_start_dates)
            df = df[df["date"] >= df["requested_start"]].drop(columns=["requested_start"])
        received_current = set(df.loc[df["date"] == today_str, "symbol"].astype(str))

        missing_symbols = set(symbols) - received_current
        if missing_symbols:
            logger.warning(
                "BaoStock 行情缺少 %d/%d 只目标日有效数据，改用 AKShare 腾讯日线补取",
                len(missing_symbols),
                expected_count,
            )
            fallback_tasks = [task for task in tasks if task[0] in missing_symbols]
            fallback_payloads = []
            fallback_timeout_by_symbol: dict[str, int] = {}
            request_timeout = min(
                15.0, float(self.settings.baostock_socket_timeout_seconds)
            )
            for task in fallback_tasks:
                start_date = date.fromisoformat(task[2])
                end_date = date.fromisoformat(task[3])
                year_span = end_date.year - start_date.year + 1
                attempts = self.settings.baostock_max_attempts
                backoff_budget = sum(
                    self.settings.baostock_backoff_seconds * (2**attempt)
                    for attempt in range(max(0, attempts - 1))
                )
                estimated_timeout = int(year_span * request_timeout * attempts + backoff_budget + 5)
                task_timeout = min(
                    1800,
                    max(self.settings.baostock_batch_timeout_seconds, estimated_timeout),
                )
                fallback_payloads.append(
                    (
                        [task],
                        attempts,
                        self.settings.baostock_backoff_seconds,
                        request_timeout,
                    )
                )
                fallback_timeout_by_symbol[task[0]] = task_timeout

            fallback_results: list[BatchResult] = []
            fallback_workers = min(self.settings.baostock_max_workers, len(fallback_payloads))
            with ThreadPoolExecutor(max_workers=fallback_workers) as executor:
                future_batches = {
                    executor.submit(
                        _run_akshare_batch_with_timeout,
                        payload,
                        fallback_timeout_by_symbol[payload[0][0][0]],
                    ): payload[0][0][0]
                    for payload in fallback_payloads
                }
                fallback_failure_streak = 0
                for future in as_completed(future_batches):
                    if future.cancelled():
                        continue
                    symbol = future_batches[future]
                    try:
                        result = future.result()
                    except Exception as exc:
                        result = BatchResult(
                            [],
                            [symbol],
                            [f"{symbol}: {type(exc).__name__}: {exc}"],
                        )
                    fallback_results.append(result)
                    batch_errors.extend(result.errors[:3])
                    errors_text = " ".join(result.errors).lower()
                    fatal_markers = (
                        "timeout",
                        "timed out",
                        "connection",
                        "network",
                        "proxy",
                        "remote",
                        "refused",
                        "unreachable",
                        "no module named",
                        "has no attribute",
                        "worker failed",
                        "worker exited",
                        "接收",
                        "连接",
                        "远程主机",
                    )
                    if not result.rows and any(marker in errors_text for marker in fatal_markers):
                        fallback_failure_streak += 1
                    else:
                        fallback_failure_streak = 0
                    completed_fallbacks = len(fallback_results)
                    if (
                        completed_fallbacks == 1
                        or completed_fallbacks % 25 == 0
                        or completed_fallbacks == len(fallback_payloads)
                    ):
                        logger.info(
                            "AKShare 备用行情进度 %d/%d：最近=%s，有效行=%d，失败=%d",
                            completed_fallbacks,
                            len(fallback_payloads),
                            symbol,
                            len(result.rows),
                            len(result.failed_symbols),
                        )
                    if fallback_failure_streak >= fallback_workers:
                        logger.error(
                            "连续 %d 个 AKShare 请求出现网络故障，停止排队中的备用批次",
                            fallback_failure_streak,
                        )
                        for pending in future_batches:
                            if pending is not future:
                                pending.cancel()

            fallback_rows = [row for result in fallback_results for row in result.rows]
            fallback_df = _normalize_daily_rows(fallback_rows, today_str)
            fallback_df = fallback_df[fallback_df["symbol"].isin(missing_symbols)]
            if not fallback_df.empty:
                fallback_df["requested_start"] = fallback_df["symbol"].map(requested_start_dates)
                fallback_df = fallback_df[
                    fallback_df["date"] >= fallback_df["requested_start"]
                ].drop(columns=["requested_start"])
            if not fallback_df.empty:
                # Keep valid primary rows when both providers returned the same date.
                df = pd.concat([fallback_df, df], ignore_index=True)
                df = df.drop_duplicates(subset=["symbol", "date"], keep="last")
            received_current = set(df.loc[df["date"] == today_str, "symbol"].astype(str))

        unresolved_symbols = set(symbols) - received_current
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
                len(unresolved_symbols),
                expected_count,
            )
            raise DataIntegrityError(
                f"{today_str} 行情覆盖率 {coverage:.2%} 低于门槛 {minimum:.2%}；"
                f"失败股票={len(unresolved_symbols)}；{error_summary}"
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
            len(unresolved_symbols),
        )
        return len(df)

    def is_trading_day(self, target_date: date | None = None) -> bool:
        """Check BaoStock's calendar, then use a bounded AKShare calendar fallback."""
        import socket

        socket.setdefaulttimeout(self.settings.baostock_socket_timeout_seconds)
        import baostock as bs

        target = (target_date or datetime.now(ZoneInfo("Asia/Shanghai")).date()).isoformat()
        max_attempts = self.settings.baostock_max_attempts
        backoff_seconds = self.settings.baostock_backoff_seconds
        errors: list[str] = []

        for attempt in range(max_attempts):
            logged_in = False
            try:
                login = bs.login()
                if login.error_code != "0":
                    raise RuntimeError(f"登录失败：{login.error_msg}")
                logged_in = True

                result = bs.query_trade_dates(start_date=target, end_date=target)
                if result.error_code != "0":
                    raise RuntimeError(f"交易日历查询失败：{result.error_msg}")

                while result.next():
                    row = result.get_row_data()
                    if row and row[0] == target:
                        if len(row) < 2 or row[1] not in {"0", "1"}:
                            raise RuntimeError(f"交易日历返回了无效状态：{row!r}")
                        return row[1] == "1"
                raise RuntimeError(f"交易日历未返回 {target} 的交易日状态")
            except Exception as exc:
                errors.append(f"{type(exc).__name__}: {exc}")
                if attempt + 1 < max_attempts:
                    delay = backoff_seconds * (2**attempt)
                    logger.warning(
                        "BaoStock 交易日历检查失败（%d/%d）：%s；%.1f 秒后重试",
                        attempt + 1,
                        max_attempts,
                        exc,
                        delay,
                    )
                    if delay:
                        time.sleep(delay)
            finally:
                if logged_in:
                    try:
                        bs.logout()
                    except Exception as exc:
                        logger.warning("BaoStock 交易日历检查后登出失败：%s", exc)

        fallback_errors: list[str] = []
        timeout_seconds = max(10, min(30, self.settings.baostock_socket_timeout_seconds * 2))
        for attempt in range(max_attempts):
            try:
                calendar_dates = _run_akshare_calendar_with_timeout(timeout_seconds)
                normalized_dates = sorted(
                    {date.fromisoformat(str(value)).isoformat() for value in calendar_dates}
                )
                if not normalized_dates:
                    raise RuntimeError("AKShare 交易日历为空")
                if normalized_dates[-1] < target:
                    raise RuntimeError(
                        f"AKShare 日历只更新到 {normalized_dates[-1]}，无法确认 {target}"
                    )
                logger.warning(
                    "BaoStock 交易日历不可用，已切换 AKShare/Sina；目标日期=%s",
                    target,
                )
                return target in set(normalized_dates)
            except Exception as exc:
                fallback_errors.append(f"{type(exc).__name__}: {exc}")
                if attempt + 1 < max_attempts:
                    delay = backoff_seconds * (2**attempt)
                    logger.warning(
                        "AKShare 备用交易日历失败（%d/%d）：%s；%.1f 秒后重试",
                        attempt + 1,
                        max_attempts,
                        exc,
                        delay,
                    )
                    if delay:
                        time.sleep(delay)

        raise DataIntegrityError(
            f"BaoStock 与 AKShare 均无法核验 {target} 的交易日历；"
            f"BaoStock={errors[-1]}；AKShare={fallback_errors[-1]}"
        )

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
        """Get the A-share universe from BaoStock, falling back to AKShare."""
        import socket

        socket.setdefaulttimeout(self.settings.baostock_socket_timeout_seconds)
        logged_in = False
        try:
            import baostock as bs

            lg = bs.login()
            if lg.error_code != "0":
                raise RuntimeError(f"BaoStock 登录失败：{lg.error_msg}")
            logged_in = True
            rs = bs.query_stock_basic(code_name="", code="")
            if rs.error_code != "0":
                raise RuntimeError(f"BaoStock 股票列表查询失败：{rs.error_msg}")
            symbols = []
            while rs.next():
                row = rs.get_row_data()
                code = row[0]           # "sh.600000" or "sz.000001"
                status = row[4]         # "1" = 上市
                stock_type = row[5]     # "1" = 股票
                if status == "1" and stock_type == "1":
                    symbols.append(code.split(".")[1])  # 提取纯数字代码
            if not symbols:
                raise RuntimeError("BaoStock 股票列表为空")
            logger.info(f"获取股票列表完成，共 {len(symbols)} 只")
            return sorted(set(symbols))
        except Exception as exc:
            logger.warning("BaoStock 股票列表不可用，尝试 AKShare 备用名单：%s", exc)
        finally:
            if logged_in:
                try:
                    bs.logout()
                except Exception as exc:
                    logger.warning("BaoStock 股票列表查询后登出失败：%s", exc)

        errors: list[str] = []
        timeout_seconds = max(10, min(30, self.settings.baostock_socket_timeout_seconds * 2))
        for attempt in range(self.settings.baostock_max_attempts):
            try:
                symbols = _run_akshare_symbols_with_timeout(timeout_seconds)
                normalized = sorted(
                    {
                        str(symbol).strip().zfill(6)
                        for symbol in symbols
                        if str(symbol).strip().isdigit()
                        and len(str(symbol).strip()) == 6
                    }
                )
                if not normalized:
                    raise RuntimeError("AKShare 股票列表没有有效的六位代码")
                logger.warning("AKShare 备用股票列表完成，共 %d 只", len(normalized))
                return normalized
            except Exception as exc:
                errors.append(f"{type(exc).__name__}: {exc}")
                if attempt + 1 < self.settings.baostock_max_attempts:
                    delay = self.settings.baostock_backoff_seconds * (2**attempt)
                    logger.warning(
                        "AKShare 备用股票列表失败（%d/%d）：%s；%.1f 秒后重试",
                        attempt + 1,
                        self.settings.baostock_max_attempts,
                        exc,
                        delay,
                    )
                    if delay:
                        time.sleep(delay)
        raise DataIntegrityError(
            f"BaoStock 与 AKShare 均无法获取股票列表：{errors[-1]}"
        )

    def get_local_symbols(self) -> list[str]:
        with sqlite3.connect(self.db_path) as conn:
            rows = conn.execute(
                "SELECT DISTINCT symbol FROM stock_daily"
            ).fetchall()
        symbols = [row[0] for row in rows]
        if self.verified_symbols is not None:
            return [symbol for symbol in symbols if symbol in self.verified_symbols]
        return symbols
