"""SQLite 行情存储和受控的 BaoStock 同步。"""

from __future__ import annotations

import atexit
import json
import math
import socket
import sqlite3
import time
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo
from typing import Iterable, Iterator

import pandas as pd

from sequoia_x.core.config import Settings
from sequoia_x.core.logger import get_logger

logger = get_logger(__name__)


# main.py、测试与工作流依赖这一版结构化同步接口。修改接口时必须同步升级该值。
ENGINE_API_VERSION = 3


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

# 验证通过前的独立暂存区；不向策略查询的 stock_daily 表发布不完整快照。
_CREATE_STAGING_SQL = """
CREATE TABLE IF NOT EXISTS stock_daily_staging (
    target_date TEXT NOT NULL,
    symbol TEXT NOT NULL,
    date TEXT NOT NULL,
    open REAL,
    high REAL,
    low REAL,
    close REAL,
    volume REAL,
    turnover REAL,
    UNIQUE(target_date, symbol, date)
);
"""

_STAGE_UPSERT_SQL = """
INSERT INTO stock_daily_staging
    (target_date, symbol, date, open, high, low, close, volume, turnover)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(target_date, symbol, date) DO UPDATE SET
    open = excluded.open,
    high = excluded.high,
    low = excluded.low,
    close = excluded.close,
    volume = excluded.volume,
    turnover = excluded.turnover
"""

# 研究信号历史只记录已发布行情对应的策略输出；不包含模拟交易账本。
_CREATE_SELECTION_RUNS_SQL = """
CREATE TABLE IF NOT EXISTS selection_runs (
    market_date TEXT NOT NULL PRIMARY KEY,
    recorded_at TEXT NOT NULL,
    expected_symbols INTEGER NOT NULL CHECK (expected_symbols >= 0),
    verified_symbols INTEGER NOT NULL CHECK (verified_symbols >= 0),
    coverage REAL NOT NULL CHECK (coverage >= 0.0 AND coverage <= 1.0),
    verified_symbols_json TEXT NOT NULL,
    strategies_json TEXT NOT NULL,
    result_rows INTEGER NOT NULL CHECK (result_rows >= 0)
);
"""

_CREATE_SELECTION_RESULTS_SQL = """
CREATE TABLE IF NOT EXISTS selection_results (
    market_date TEXT NOT NULL,
    strategy TEXT NOT NULL,
    symbol TEXT NOT NULL,
    PRIMARY KEY (market_date, strategy, symbol),
    FOREIGN KEY (market_date)
        REFERENCES selection_runs(market_date) ON DELETE CASCADE
);
"""


_UPSERT_SQL = """
INSERT INTO stock_daily
    (symbol, date, open, high, low, close, volume, turnover)
VALUES (?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(symbol, date) DO UPDATE SET
    open = excluded.open,
    high = excluded.high,
    low = excluded.low,
    close = excluded.close,
    volume = excluded.volume,
    turnover = excluded.turnover
"""

SYNC_COMPLETE = "complete"
SYNC_INCOMPLETE = "incomplete"
SYNC_NON_TRADING_DAY = "non_trading_day"
SYNC_NO_LOCAL_DATA = "no_local_data"


class DataSourceUnavailable(RuntimeError):
    """BaoStock 无法建立或维持一个已认证会话。"""


class DataIntegrityError(RuntimeError):
    """远端数据或本地数据未通过完整性检查。"""


@dataclass
class BatchFetchResult:
    """一个隔离批次的结果；worker 异常不得逃逸到进程池。"""

    rows: list[list[str]] = field(default_factory=list)
    attempted_symbols: set[str] = field(default_factory=set)
    current_symbols: set[str] = field(default_factory=set)
    stale_symbols: set[str] = field(default_factory=set)
    failed_symbols: dict[str, str] = field(default_factory=dict)
    login_error: str | None = None


@dataclass
class SyncReport:
    """日行情同步的可审计结果。"""

    status: str
    target_date: str
    is_trade_day: bool
    expected_symbols: int
    verified_symbols: frozenset[str] = field(default_factory=frozenset)
    failed_symbols: dict[str, str] = field(default_factory=dict)
    stale_symbols: frozenset[str] = field(default_factory=frozenset)
    rows_written: int = 0
    # 包含暂存但尚未正式发布的合格日线；与 coverage 严格分离。
    candidate_verified_count: int = 0

    @property
    def candidate_coverage(self) -> float:
        if self.expected_symbols <= 0:
            return 0.0
        return self.candidate_verified_count / self.expected_symbols

    @property
    def coverage(self) -> float:
        if self.expected_symbols <= 0:
            return 0.0
        return len(self.verified_symbols) / self.expected_symbols

    @property
    def complete(self) -> bool:
        return self.status == SYNC_COMPLETE


@dataclass
class BackfillReport:
    requested: int
    succeeded: int
    skipped: int
    failed_symbols: dict[str, str] = field(default_factory=dict)

    @property
    def complete(self) -> bool:
        return not self.failed_symbols


def _safe_logout(bs) -> None:
    try:
        bs.logout()
    except Exception:
        pass


# 进程隔离的 BaoStock 会话。ProcessPoolExecutor 会重复向同一工作进程
# 分配多个小批次，因此不要在每个批次结束后反复 login/logout。
# 每个工作进程最多持有自己的连接，禁止跨进程共享 BaoStock 对象。
_WORKER_BS = None
_WORKER_LOGGED_IN = False
_WORKER_SHUTDOWN_REGISTERED = False


def _worker_disconnect() -> None:
    global _WORKER_LOGGED_IN
    if _WORKER_LOGGED_IN and _WORKER_BS is not None:
        _safe_logout(_WORKER_BS)
    _WORKER_LOGGED_IN = False


def _worker_connect(bs, max_attempts: int, backoff_seconds: float) -> str | None:
    global _WORKER_BS, _WORKER_LOGGED_IN, _WORKER_SHUTDOWN_REGISTERED
    if _WORKER_BS is not bs:
        _worker_disconnect()
        _WORKER_BS = bs
    if not _WORKER_SHUTDOWN_REGISTERED:
        atexit.register(_worker_disconnect)
        _WORKER_SHUTDOWN_REGISTERED = True
    if _WORKER_LOGGED_IN:
        return None
    error = _login_with_retry(bs, max_attempts, backoff_seconds)
    if error is None:
        _WORKER_LOGGED_IN = True
    return error


def _login_with_retry(bs, max_attempts: int, backoff_seconds: float) -> str | None:
    """登录成功返回 ``None``，失败返回最后一条错误。"""
    last_error = "unknown login error"
    attempts = max(1, max_attempts)
    for attempt in range(attempts):
        try:
            result = bs.login()
            if getattr(result, "error_code", None) == "0":
                return None
            last_error = str(getattr(result, "error_msg", "unknown login error"))
        except Exception as exc:
            last_error = f"{type(exc).__name__}: {exc}"

        if attempt < attempts - 1:
            _safe_logout(bs)
            time.sleep(backoff_seconds * (2**attempt))
    return last_error


def _query_symbol(
    bs,
    task: tuple[str, str, str, str],
    max_attempts: int,
    backoff_seconds: float,
) -> tuple[list[list[str]], str | None, bool]:
    """查询一只股票，并在重试前重建会话。

    返回 ``(rows, error, session_healthy)``。响应解析也属于重试范围，
    因而 BaoStock 的短响应 ``IndexError`` 不会击穿整个批次。
    """
    symbol, bs_code, start, end = task
    last_error = "unknown query error"
    attempts = max(1, max_attempts)

    try:
        query_start = date.fromisoformat(start)
        query_end = date.fromisoformat(end)
    except ValueError as exc:
        return [], f"invalid query date: {exc}", True
    if query_start > query_end:
        return [], f"invalid query range: {start} > {end}", True

    for attempt in range(attempts):
        try:
            rs = bs.query_history_k_data_plus(
                bs_code,
                "date,open,high,low,close,volume,amount",
                start_date=start,
                end_date=end,
                frequency="d",
                adjustflag="1",
            )
            if getattr(rs, "error_code", None) != "0":
                raise RuntimeError(str(getattr(rs, "error_msg", "unknown query error")))

            rows: list[list[str]] = []
            while rs.next():
                row = list(rs.get_row_data())
                if len(row) != 7:
                    raise ValueError(f"unexpected BaoStock row length: {len(row)}")
                row_date = date.fromisoformat(str(row[0]))
                if not query_start <= row_date <= query_end:
                    raise ValueError(f"BaoStock returned an out-of-range date: {row_date}")
                rows.append([symbol, *row])
            if getattr(rs, "error_code", None) != "0":
                raise RuntimeError(str(getattr(rs, "error_msg", "query iteration failed")))
            return rows, None, True
        except Exception as exc:
            last_error = f"{type(exc).__name__}: {exc}"
            if attempt >= attempts - 1:
                break

            time.sleep(backoff_seconds * (2**attempt))
            _safe_logout(bs)
            login_error = _login_with_retry(bs, max_attempts, backoff_seconds)
            if login_error is not None:
                return [], f"{last_error}; reconnect failed: {login_error}", False

    # 查询连续失败后先恢复会话，避免把一个坏连接传给批次中的下一只股票。
    _safe_logout(bs)
    login_error = _login_with_retry(bs, max_attempts, backoff_seconds)
    if login_error is not None:
        return [], f"{last_error}; reconnect failed: {login_error}", False
    return [], last_error, True


def _bs_fetch_batch(
    payload: tuple[list[tuple[str, str, str, str]], int, float, float],
) -> BatchFetchResult:
    """进程 worker：单批独立登录，逐只隔离错误，始终返回结构化结果。"""
    tasks, max_attempts, backoff_seconds, socket_timeout = payload
    global _WORKER_LOGGED_IN
    result = BatchFetchResult()
    if not tasks:
        return result

    import baostock as bs

    previous_timeout = socket.getdefaulttimeout()
    socket.setdefaulttimeout(socket_timeout)
    try:
        login_error = _worker_connect(bs, max_attempts, backoff_seconds)
        if login_error is not None:
            result.login_error = login_error
            result.failed_symbols.update({task[0]: login_error for task in tasks})
            return result

        for index, task in enumerate(tasks):
            symbol, _bs_code, _start, target_date = task
            result.attempted_symbols.add(symbol)
            rows, error, session_healthy = _query_symbol(
                bs,
                task,
                max_attempts=max_attempts,
                backoff_seconds=backoff_seconds,
            )
            if error is not None:
                result.failed_symbols[symbol] = error
            else:
                result.rows.extend(rows)
                if any(row[1] == target_date for row in rows):
                    result.current_symbols.add(symbol)
                else:
                    result.stale_symbols.add(symbol)

            if not session_healthy:
                # 下个分配到该 worker 的批次必须重新登录；不可继续使用坏会话。
                _WORKER_LOGGED_IN = False
                result.login_error = error or "BaoStock session unavailable"
                for remaining in tasks[index + 1 :]:
                    result.failed_symbols[remaining[0]] = result.login_error
                break
        return result
    except Exception as exc:
        # 兜底保护：任何未知 worker 错误只影响当前小批次。
        _WORKER_LOGGED_IN = False
        error = f"{type(exc).__name__}: {exc}"
        for task in tasks:
            result.failed_symbols.setdefault(task[0], error)
        return result
    finally:
        # 只恢复网络参数；BaoStock 会话交给工作进程退出时统一释放。
        socket.setdefaulttimeout(previous_timeout)


class DataEngine:
    """行情数据引擎，负责 SQLite 存储和 BaoStock 数据同步。"""

    def __init__(self, settings: Settings) -> None:
        self.db_path = str(settings.db_path)
        self.start_date = str(settings.start_date)
        try:
            self._start_date_value = date.fromisoformat(self.start_date)
        except ValueError as exc:
            raise DataIntegrityError(f"START_DATE 不是有效 ISO 日期: {self.start_date}") from exc
        self.max_workers = max(1, min(int(settings.baostock_max_workers), 4))
        self.batch_size = max(1, min(int(settings.baostock_batch_size), 200))
        self.max_attempts = max(1, min(int(settings.baostock_max_attempts), 5))
        self.backoff_seconds = max(0.0, min(float(settings.baostock_backoff_seconds), 60.0))
        self.socket_timeout = max(
            5.0,
            min(float(settings.baostock_socket_timeout_seconds), 120.0),
        )
        self.min_daily_coverage = max(0.0, min(float(settings.min_daily_coverage), 1.0))
        self._snapshot_date: str | None = None
        self._snapshot_symbols: tuple[str, ...] | None = None
        self._init_db()

    def _init_db(self) -> None:
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(_CREATE_TABLE_SQL)
            conn.execute(_CREATE_INDEX_SQL)
            conn.execute(_CREATE_STAGING_SQL)
            conn.execute(_CREATE_SELECTION_RUNS_SQL)
            conn.execute(_CREATE_SELECTION_RESULTS_SQL)
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
        with sqlite3.connect(self.db_path) as conn:
            if self._snapshot_date is None:
                return pd.read_sql(
                    "SELECT * FROM stock_daily WHERE symbol = ? ORDER BY date",
                    conn,
                    params=(symbol,),
                )
            return pd.read_sql(
                "SELECT * FROM stock_daily WHERE symbol = ? AND date <= ? ORDER BY date",
                conn,
                params=(symbol, self._snapshot_date),
            )

    def pin_strategy_snapshot(self, trade_date: str, symbols: Iterable[str]) -> None:
        """把本轮策略固定到已经核验的日期和股票集合。"""
        date.fromisoformat(trade_date)
        clean = sorted(self._normalise_symbols(symbols))
        self._snapshot_date = trade_date
        self._snapshot_symbols = tuple(clean)

    @property
    def strategy_snapshot_date(self) -> str | None:
        return self._snapshot_date

    @staticmethod
    def _to_baostock_code(symbol: str) -> str:
        prefix = "sh" if symbol.startswith(("6", "9")) else "sz"
        return f"{prefix}.{symbol}"

    @staticmethod
    def _normalise_symbols(symbols: Iterable[str]) -> set[str]:
        clean: set[str] = set()
        for raw_symbol in symbols:
            symbol = str(raw_symbol).strip().zfill(6)
            if len(symbol) != 6 or not symbol.isdigit():
                raise DataIntegrityError(f"无效股票代码: {raw_symbol!r}")
            clean.add(symbol)
        return clean

    def _with_socket_timeout(self):
        """设置临时超时并返回旧值；调用方负责在 finally 中恢复。"""
        previous = socket.getdefaulttimeout()
        socket.setdefaulttimeout(self.socket_timeout)
        return previous

    def _query_trade_calendar(self, start_date: str, end_date: str) -> dict[str, bool]:
        import baostock as bs

        start_value = date.fromisoformat(start_date)
        end_value = date.fromisoformat(end_date)
        if start_value > end_value:
            raise DataIntegrityError(f"交易日历查询区间无效: {start_date} > {end_date}")

        previous_timeout = self._with_socket_timeout()
        try:
            login_error = _login_with_retry(bs, self.max_attempts, self.backoff_seconds)
            if login_error is not None:
                raise DataSourceUnavailable(f"baostock 登录失败: {login_error}")

            last_error = "unknown trade calendar error"
            for attempt in range(self.max_attempts):
                try:
                    rs = bs.query_trade_dates(start_date=start_date, end_date=end_date)
                    if getattr(rs, "error_code", None) != "0":
                        raise RuntimeError(
                            str(getattr(rs, "error_msg", "trade calendar query failed"))
                        )
                    rows: list[list[str]] = []
                    while rs.next():
                        rows.append(list(rs.get_row_data()))
                    if getattr(rs, "error_code", None) != "0":
                        raise RuntimeError(
                            str(getattr(rs, "error_msg", "trade calendar iteration failed"))
                        )
                    calendar: dict[str, bool] = {}
                    for row in rows:
                        if len(row) < 2:
                            raise ValueError(f"baostock 交易日响应字段不足: {row!r}")
                        row_date = date.fromisoformat(str(row[0]))
                        if not start_value <= row_date <= end_value or row[1] not in {"0", "1"}:
                            raise ValueError(f"baostock 交易日响应内容异常: {row!r}")
                        date_text = row_date.isoformat()
                        trade_status = row[1] == "1"
                        if date_text in calendar and calendar[date_text] != trade_status:
                            raise ValueError(f"baostock 交易日响应冲突: {row!r}")
                        calendar[date_text] = trade_status
                    if not calendar:
                        raise ValueError("baostock 未返回可验证的交易日状态")
                    return calendar
                except Exception as exc:
                    last_error = f"{type(exc).__name__}: {exc}"
                    if attempt >= self.max_attempts - 1:
                        break
                    time.sleep(self.backoff_seconds * (2**attempt))
                    _safe_logout(bs)
                    login_error = _login_with_retry(
                        bs,
                        self.max_attempts,
                        self.backoff_seconds,
                    )
                    if login_error is not None:
                        raise DataSourceUnavailable(
                            f"baostock 交易日查询重连失败: {login_error}"
                        ) from exc
            raise DataSourceUnavailable(f"baostock 交易日查询失败: {last_error}")
        finally:
            _safe_logout(bs)
            socket.setdefaulttimeout(previous_timeout)

    def _is_trade_day(self, target_date: str) -> bool:
        calendar = self._query_trade_calendar(target_date, target_date)
        if target_date not in calendar:
            raise DataSourceUnavailable(f"baostock 未返回 {target_date} 的交易日状态")
        return calendar[target_date]

    def _latest_trade_date(self, on_or_before: date) -> date:
        start = on_or_before - timedelta(days=31)
        calendar = self._query_trade_calendar(start.isoformat(), on_or_before.isoformat())
        trade_dates = [
            date.fromisoformat(day)
            for day, is_trade_day in calendar.items()
            if is_trade_day and date.fromisoformat(day) <= on_or_before
        ]
        if not trade_dates:
            raise DataSourceUnavailable(
                f"baostock 未返回 {start.isoformat()} 至 {on_or_before.isoformat()} 的交易日"
            )
        return max(trade_dates)

    @staticmethod
    def _get_verified_symbols_from_connection(
        conn: sqlite3.Connection,
        target_date: str,
    ) -> set[str]:
        rows = conn.execute(
            """
            SELECT symbol, open, high, low, close, volume, turnover
            FROM stock_daily
            WHERE date = ?
            """,
            (target_date,),
        ).fetchall()
        verified: set[str] = set()
        for symbol, open_price, high, low, close, volume, turnover in rows:
            try:
                values = tuple(
                    float(value)
                    for value in (open_price, high, low, close, volume, turnover)
                )
            except (TypeError, ValueError):
                continue
            open_value, high_value, low_value, close_value, volume_value, turnover_value = (
                values
            )
            if not all(math.isfinite(value) for value in values):
                continue
            if min(open_value, high_value, low_value, close_value) <= 0:
                continue
            if high_value < max(open_value, low_value, close_value):
                continue
            if low_value > min(open_value, high_value, close_value):
                continue
            if volume_value < 0 or turnover_value < 0:
                continue
            verified.add(str(symbol).zfill(6))
        return verified

    def _get_verified_symbols(self, target_date: str) -> set[str]:
        with sqlite3.connect(self.db_path) as conn:
            return self._get_verified_symbols_from_connection(conn, target_date)

    def _normalise_rows(
        self,
        rows: list[list[str]],
        expected_symbols: set[str],
        target_date: str,
    ) -> tuple[list[tuple], set[str], dict[str, str]]:
        grouped: dict[str, list[list[str]]] = {}
        failures: dict[str, str] = {}
        for row in rows:
            if not row:
                continue
            symbol = str(row[0]).zfill(6)
            grouped.setdefault(symbol, []).append(row)

        normalised: list[tuple] = []
        current_symbols: set[str] = set()
        minimum_date = self._start_date_value
        maximum_date = date.fromisoformat(target_date)
        if maximum_date < minimum_date:
            raise DataIntegrityError(
                f"目标日期 {target_date} 早于配置起始日期 {self.start_date}"
            )

        for symbol, symbol_rows in grouped.items():
            if symbol not in expected_symbols:
                failures[symbol] = "symbol was not in the validated universe"
                continue

            per_date: dict[str, tuple] = {}
            try:
                for row in symbol_rows:
                    if len(row) != 8:
                        raise ValueError(f"unexpected row length: {len(row)}")
                    row_date = date.fromisoformat(str(row[1]))
                    if not minimum_date <= row_date <= maximum_date:
                        raise ValueError(f"date out of range: {row_date}")

                    open_price, high, low, close = (float(value) for value in row[2:6])
                    volume = float(row[6] or 0)
                    turnover = float(row[7] or 0)
                    numeric_values = (open_price, high, low, close, volume, turnover)
                    if not all(math.isfinite(value) for value in numeric_values):
                        raise ValueError("行情包含 NaN 或无穷值")
                    if min(open_price, high, low, close) <= 0:
                        raise ValueError("OHLC contains a non-positive value")
                    if high < max(open_price, low, close) or low > min(open_price, high, close):
                        raise ValueError("OHLC bounds are inconsistent")
                    if volume < 0 or turnover < 0:
                        raise ValueError("volume or turnover is negative")

                    date_text = row_date.isoformat()
                    record = (
                        symbol,
                        date_text,
                        open_price,
                        high,
                        low,
                        close,
                        volume,
                        turnover,
                    )
                    previous = per_date.get(date_text)
                    if previous is not None and previous != record:
                        raise ValueError(f"同一交易日返回冲突记录: {date_text}")
                    per_date[date_text] = record
            except (TypeError, ValueError) as exc:
                failures[symbol] = str(exc)
                continue

            normalised.extend(per_date.values())
            if target_date in per_date:
                current_symbols.add(symbol)

        return normalised, current_symbols, failures

    def _upsert_rows(
        self,
        rows: list[tuple],
        *,
        target_date: str | None = None,
        expected_symbols: set[str] | None = None,
        minimum_coverage: float | None = None,
        run_quick_check: bool = True,
    ) -> set[str]:
        if not rows:
            return set()
        if (target_date is None) != (expected_symbols is None):
            raise ValueError("target_date 与 expected_symbols 必须同时提供")

        with sqlite3.connect(self.db_path) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                conn.executemany(_UPSERT_SQL, rows)

                verified: set[str] = set()
                if target_date is not None and expected_symbols is not None:
                    verified = (
                        self._get_verified_symbols_from_connection(conn, target_date)
                        & expected_symbols
                    )
                    threshold = 0.0 if minimum_coverage is None else minimum_coverage
                    coverage = len(verified) / len(expected_symbols) if expected_symbols else 0.0
                    if coverage < threshold:
                        raise DataIntegrityError(
                            f"事务内覆盖率异常: {coverage:.2%} < {threshold:.2%}"
                        )

                if run_quick_check:
                    integrity = conn.execute("PRAGMA quick_check").fetchone()
                    if not integrity or integrity[0] != "ok":
                        raise DataIntegrityError(f"SQLite quick_check failed: {integrity}")
                conn.commit()
                return verified
            except Exception:
                if conn.in_transaction:
                    conn.rollback()
                raise

    def _stage_rows(
        self,
        target_date: str | list[tuple],
        rows: list[tuple] | str,
    ) -> None:
        """持久化暂存；兼容历史调用 _stage_rows(rows, target_date)。

        暂存记录绝不出现在正式发布集合 verified_symbols 中；
        直到通过 min_daily_coverage 门禁后才允许迁移至 stock_daily。
        """
        if not isinstance(target_date, str) and isinstance(rows, str):
            target_date, rows = rows, target_date
        if not isinstance(target_date, str) or isinstance(rows, str):
            raise TypeError("_stage_rows 需要交易日字符串与行情行序列")
        date.fromisoformat(target_date)
        if not rows:
            return
        validated_rows: list[tuple] = []
        for row in rows:
            if len(row) != 8:
                raise DataIntegrityError("暂存行情必须包含 symbol/date/OHLC/volume/turnover 共8列")
            row_date = date.fromisoformat(str(row[1]))
            if row_date > date.fromisoformat(target_date):
                raise DataIntegrityError("暂存行情日期晚于目标交易日")
            validated_rows.append(tuple(row))
        with sqlite3.connect(self.db_path) as conn:
            conn.executemany(
                _STAGE_UPSERT_SQL,
                [(target_date, *row) for row in validated_rows],
            )
            # 每个短事务由 SQLite 保证原子性；全库 quick_check 放到备份/发布阶段，
            # 避免数百个批次重复全库扫描。

    def _read_staged_rows(self, target_date: str) -> list[list]:
        """只读取目标日期对应的暂存记录，防止跨日期混用。"""
        with sqlite3.connect(self.db_path) as conn:
            return [
                list(row)
                for row in conn.execute(
                    """SELECT symbol, date, open, high, low, close, volume, turnover
                       FROM stock_daily_staging WHERE target_date = ?""",
                    (target_date,),
                ).fetchall()
            ]

    def _publish_staging(
        self, target_date: str, expected_symbols: set[str],
    ) -> tuple[set[str], int]:
        """覆盖率和 SQLite 检查同一事务内完成；成功后才发布并清空该日暂存。"""
        with sqlite3.connect(self.db_path) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                staged = conn.execute(
                    """SELECT symbol, date, open, high, low, close, volume, turnover
                       FROM stock_daily_staging WHERE target_date = ?""",
                    (target_date,),
                ).fetchall()
                # 防止暂存区被意外改写或跨股票池污染。
                clean_rows, _, invalid = self._normalise_rows(
                    [list(row) for row in staged], expected_symbols, target_date,
                )
                if invalid:
                    raise DataIntegrityError(f"暂存行情未通过校验: {invalid}")
                conn.executemany(_UPSERT_SQL, clean_rows)
                verified = (
                    self._get_verified_symbols_from_connection(conn, target_date)
                    & expected_symbols
                )
                coverage = len(verified) / len(expected_symbols) if expected_symbols else 0.0
                if coverage < self.min_daily_coverage:
                    raise DataIntegrityError(
                        f"发布事务覆盖率不足: {coverage:.2%} < {self.min_daily_coverage:.2%}"
                    )
                integrity = conn.execute("PRAGMA quick_check").fetchone()
                if not integrity or integrity[0] != "ok":
                    raise DataIntegrityError(f"SQLite quick_check failed: {integrity}")
                conn.execute(
                    "DELETE FROM stock_daily_staging WHERE target_date = ?",
                    (target_date,),
                )
                conn.commit()
                return verified, len(clean_rows)
            except Exception:
                if conn.in_transaction:
                    conn.rollback()
                raise

    def _iter_fetch_batches(
        self,
        batches: list[list[tuple[str, str, str, str]]],
    ) -> Iterator[BatchFetchResult]:
        """受控并行抓取，并逐波产出结果。

        调用方可在下一波网络请求完成前处理并落库上一波结果，内存占用因此
        只与 ``max_workers * batch_size`` 成正比。一个 worker 异常只会标记
        它负责的小批次；若整波均无法登录，则熔断尚未提交的批次。
        """
        if not batches:
            return

        import multiprocessing
        from concurrent.futures import ProcessPoolExecutor, as_completed

        workers = min(self.max_workers, len(batches))
        context = multiprocessing.get_context("spawn")
        with ProcessPoolExecutor(max_workers=workers, mp_context=context) as executor:
            for offset in range(0, len(batches), workers):
                wave = batches[offset : offset + workers]
                future_to_batch = {
                    executor.submit(
                        _bs_fetch_batch,
                        (
                            batch,
                            self.max_attempts,
                            self.backoff_seconds,
                            self.socket_timeout,
                        ),
                    ): batch
                    for batch in wave
                }
                wave_results: list[BatchFetchResult] = []
                for future in as_completed(future_to_batch):
                    batch = future_to_batch[future]
                    try:
                        wave_results.append(future.result())
                    except Exception as exc:
                        error = f"worker failed: {type(exc).__name__}: {exc}"
                        wave_results.append(
                            BatchFetchResult(
                                attempted_symbols={task[0] for task in batch},
                                failed_symbols={task[0]: error for task in batch},
                            )
                        )
                for result in wave_results:
                    yield result

                login_errors = [result.login_error for result in wave_results]
                if wave_results and all(login_errors):
                    error = f"BaoStock 登录或会话失效，停止剩余批次: {login_errors[0]}"
                    for remaining_batch in batches[offset + len(wave) :]:
                        yield BatchFetchResult(
                            failed_symbols={task[0]: error for task in remaining_batch},
                            login_error=error,
                        )
                    break

    def _run_fetch_batches(
        self,
        batches: list[list[tuple[str, str, str, str]]],
    ) -> list[BatchFetchResult]:
        """兼容日行情同步：收集流式批次结果。"""
        return list(self._iter_fetch_batches(batches))

    def sync_today_bulk(
        self,
        expected_symbols: Iterable[str] | None = None,
        target_date: date | None = None,
    ) -> SyncReport:
        """同步并发布一个经过交易日、数据质量和覆盖率验证的日快照。"""
        target_value = target_date or date.today()
        if target_value > date.today():
            raise DataIntegrityError(f"目标日期不能晚于今天: {target_value.isoformat()}")
        if target_value < self._start_date_value:
            raise DataIntegrityError(
                f"目标日期 {target_value.isoformat()} 早于配置起始日期 {self.start_date}"
            )
        target_text = target_value.isoformat()
        with sqlite3.connect(self.db_path) as conn:
            last_rows = conn.execute(
                "SELECT symbol, MAX(date) FROM stock_daily GROUP BY symbol"
            ).fetchall()
        last_by_symbol = {str(symbol).zfill(6): last_date for symbol, last_date in last_rows}

        if expected_symbols is None:
            expected = set(last_by_symbol)
        else:
            expected = self._normalise_symbols(expected_symbols)

        if not expected:
            return SyncReport(
                status=SYNC_NO_LOCAL_DATA,
                target_date=target_text,
                is_trade_day=False,
                expected_symbols=0,
            )

        if not self._is_trade_day(target_text):
            verified_before = self._get_verified_symbols(target_text) & expected
            logger.info(f"{target_text} 不是交易日，跳过行情同步和选股邮件")
            return SyncReport(
                status=SYNC_NON_TRADING_DAY,
                target_date=target_text,
                is_trade_day=False,
                expected_symbols=len(expected),
                verified_symbols=frozenset(verified_before),
                stale_symbols=frozenset(expected - verified_before),
            )

        # 仅保留本目标日期的待发布快照；严格禁止跨交易日复用陈旧暂存。
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                "DELETE FROM stock_daily_staging WHERE target_date < ?",
                (target_text,),
            )

        verified_before = self._get_verified_symbols(target_text) & expected
        # 从上一次失败运行的独立暂存区恢复，正式库中的行情仍不受影响。
        staged_raw = self._read_staged_rows(target_text)
        staged_rows, staged_current, invalid_staged = self._normalise_rows(
            staged_raw, expected_symbols=expected, target_date=target_text,
        )
        if invalid_staged:
            # 忽略旧版本或损坏暂存中的异常记录，避免一条意外代码阻断
            # 其余经过校验行情的续传；正式发布仍只接收 expected 集合。
            with sqlite3.connect(self.db_path) as conn:
                conn.executemany(
                    "DELETE FROM stock_daily_staging WHERE target_date = ? AND symbol = ?",
                    [(target_text, symbol) for symbol in invalid_staged],
                )
        if verified_before == expected:
            return SyncReport(
                status=SYNC_COMPLETE,
                target_date=target_text,
                is_trade_day=True,
                expected_symbols=len(expected),
                verified_symbols=frozenset(verified_before),
                candidate_verified_count=len(expected),
            )

        # 关键续传修复：如果上一次失败已暂存足够的当日合格行情，
        # 直接执行同事务门禁+正式发布，而不再请求剩余的约2%股票。
        restored = verified_before | staged_current
        if len(restored) / len(expected) >= self.min_daily_coverage:
            verified_after, rows_written = self._publish_staging(target_text, expected)
            logger.info(
                "恢复暂存后已达发布门槛：核验 %s/%s，正式写入 %s 条",
                len(verified_after), len(expected), rows_written,
            )
            return SyncReport(
                status=SYNC_COMPLETE,
                target_date=target_text,
                is_trade_day=True,
                expected_symbols=len(expected),
                verified_symbols=frozenset(verified_after),
                stale_symbols=frozenset(expected - verified_after),
                rows_written=rows_written,
                candidate_verified_count=len(verified_after),
            )

        tasks: list[tuple[str, str, str, str]] = []
        preflight_failures: dict[str, str] = {}
        for symbol in sorted(expected - verified_before - staged_current):
            last_date_text = last_by_symbol.get(symbol)
            last_date_value: date | None = None
            if last_date_text:
                try:
                    last_date_value = date.fromisoformat(str(last_date_text))
                except ValueError:
                    preflight_failures[symbol] = (
                        f"invalid local date: {last_date_text!r}"
                    )
                    continue
                if last_date_value > target_value:
                    preflight_failures[symbol] = f"future local date: {last_date_text}"
                    continue
            start = target_text
            if last_date_value is not None and last_date_value < target_value:
                start_value = max(
                    last_date_value + timedelta(days=1),
                    self._start_date_value,
                )
                start = start_value.isoformat()
            tasks.append((symbol, self._to_baostock_code(symbol), start, target_text))

        if tasks:
            batches = [
                tasks[index : index + self.batch_size]
                for index in range(0, len(tasks), self.batch_size)
            ]
            logger.info(
                f"需要更新 {len(tasks)} 只股票，已恢复暂存 {len(staged_current)} 只，"
                f"使用最多 {self.max_workers} 个进程，拆分为 {len(batches)} 个隔离批次"
            )
        else:
            batches = []

        failed_symbols = dict(preflight_failures)
        stale_symbols: set[str] = set()
        newly_staged: set[str] = set()
        # 使用流式批次处理，避免一次性收集几千只股票的原始响应。
        for batch in self._iter_fetch_batches(batches):
            failed_symbols.update(batch.failed_symbols)
            stale_symbols.update(batch.stale_symbols)
            normalised, current, invalid = self._normalise_rows(
                batch.rows,
                expected_symbols=expected,
                target_date=target_text,
            )
            failed_symbols.update(invalid)
            if normalised:
                self._stage_rows(target_text, normalised)
                newly_staged.update(current)

        candidate_verified = verified_before | staged_current | newly_staged
        candidate_coverage = len(candidate_verified) / len(expected)
        stale_symbols.update(expected - candidate_verified)

        if candidate_coverage < self.min_daily_coverage:
            logger.error(
                f"{target_text} 行情候选覆盖率 {candidate_coverage:.2%} 低于门槛 "
                f"{self.min_daily_coverage:.2%}；已保存 {len(staged_current | newly_staged)} "
                "只核验通过的暂存行情，但未发布正式行情"
            )
            return SyncReport(
                status=SYNC_INCOMPLETE,
                target_date=target_text,
                is_trade_day=True,
                expected_symbols=len(expected),
                verified_symbols=frozenset(candidate_verified),
                failed_symbols=failed_symbols,
                stale_symbols=frozenset(stale_symbols),
                candidate_verified_count=len(candidate_verified),
            )

        verified_after, rows_written = self._publish_staging(target_text, expected)
        final_coverage = len(verified_after) / len(expected)
        logger.info(
            f"sync_today_bulk: 发布 {rows_written} 条，"
            f"核验 {len(verified_after)}/{len(expected)} 只，覆盖率 {final_coverage:.2%}"
        )
        return SyncReport(
            status=SYNC_COMPLETE,
            target_date=target_text,
            is_trade_day=True,
            expected_symbols=len(expected),
            verified_symbols=frozenset(verified_after),
            failed_symbols=failed_symbols,
            stale_symbols=frozenset(expected - verified_after),
            rows_written=rows_written,
            candidate_verified_count=len(verified_after),
        )

    def backfill(self, symbols: list[str]) -> BackfillReport:
        """并行抓取历史数据，并在主进程中按批校验及原子落库。

        BaoStock 会话只存在于受限数量的 worker 中；SQLite 只由主进程写入。
        每个完成批次会立即释放原始响应，避免首次全量回填累积全市场历史行。
        """
        clean_symbols = sorted(self._normalise_symbols(symbols))
        if not clean_symbols:
            return BackfillReport(requested=0, succeeded=0, skipped=0)

        target_value = self._latest_trade_date(date.today())
        target_text = target_value.isoformat()
        verified_at_target = self._get_verified_symbols(target_text)

        # 一次查询全部本地游标，避免为约千只股票反复建立 SQLite 连接。
        with sqlite3.connect(self.db_path) as conn:
            last_rows = conn.execute(
                "SELECT symbol, MAX(date) FROM stock_daily GROUP BY symbol"
            ).fetchall()
        last_by_symbol = {
            str(symbol).zfill(6): str(last_date)
            for symbol, last_date in last_rows
            if last_date
        }

        skipped_symbols: set[str] = set()
        failures: dict[str, str] = {}
        tasks: list[tuple[str, str, str, str]] = []
        for symbol in clean_symbols:
            last_date_text = last_by_symbol.get(symbol)
            last_date_value: date | None = None
            if last_date_text:
                try:
                    last_date_value = date.fromisoformat(last_date_text)
                except ValueError:
                    failures[symbol] = f"invalid local date: {last_date_text!r}"
                    continue
                if last_date_value > target_value:
                    failures[symbol] = f"future local date: {last_date_text}"
                    continue
                if last_date_value == target_value and symbol in verified_at_target:
                    skipped_symbols.add(symbol)
                    continue

            start_value = self._start_date_value
            if last_date_value is not None and last_date_value < target_value:
                start_value = max(
                    last_date_value + timedelta(days=1),
                    self._start_date_value,
                )
            elif last_date_value == target_value:
                start_value = target_value
            tasks.append(
                (
                    symbol,
                    self._to_baostock_code(symbol),
                    start_value.isoformat(),
                    target_text,
                )
            )

        # 历史响应远大于单日响应；进一步限制每批大小来约束峰值内存。
        backfill_batch_size = min(self.batch_size, 20)
        batches = [
            tasks[index : index + backfill_batch_size]
            for index in range(0, len(tasks), backfill_batch_size)
        ]
        logger.info(
            f"历史回填待抓取 {len(tasks)} 只，使用最多 {self.max_workers} 个进程，"
            f"拆分为 {len(batches)} 个流式批次（每批最多 {backfill_batch_size} 只）"
        )

        succeeded_symbols: set[str] = set()
        processed = len(skipped_symbols) + len(failures)
        for batch_result in self._iter_fetch_batches(batches):
            batch_symbols = (
                set(batch_result.attempted_symbols)
                | set(batch_result.failed_symbols)
                | {str(row[0]).zfill(6) for row in batch_result.rows if row}
            )
            failures.update(batch_result.failed_symbols)

            normalised, _current, invalid = self._normalise_rows(
                batch_result.rows,
                expected_symbols=batch_symbols,
                target_date=target_text,
            )
            failures.update(invalid)
            valid_symbols = {str(row[0]).zfill(6) for row in normalised}

            for symbol in batch_symbols - valid_symbols - set(failures):
                failures[symbol] = (
                    f"baostock 未返回截至 {target_text} 的预期历史数据"
                )

            if normalised:
                try:
                    # 每批一个独立事务；完整性扫描延后到全部批次结束，仅执行一次。
                    self._upsert_rows(normalised, run_quick_check=False)
                    succeeded_symbols.update(valid_symbols)
                    for symbol in valid_symbols:
                        failures.pop(symbol, None)
                except (sqlite3.Error, DataIntegrityError) as exc:
                    error = f"batch database write failed: {type(exc).__name__}: {exc}"
                    for symbol in valid_symbols:
                        failures[symbol] = error

            processed += len(batch_symbols)
            if processed == len(clean_symbols) or processed % 100 < len(batch_symbols):
                logger.info(
                    f"已处理 {min(processed, len(clean_symbols))}/{len(clean_symbols)}，"
                    f"成功 {len(succeeded_symbols)} 跳过 {len(skipped_symbols)} "
                    f"失败 {len(failures)}"
                )

        accounted_symbols = succeeded_symbols | skipped_symbols | set(failures)
        for symbol in set(clean_symbols) - accounted_symbols:
            failures[symbol] = "batch worker returned no auditable result"

        if not self.database_quick_check():
            raise DataIntegrityError("历史回填后 SQLite quick_check 失败")

        report = BackfillReport(
            requested=len(clean_symbols),
            succeeded=len(succeeded_symbols),
            skipped=len(skipped_symbols),
            failed_symbols=failures,
        )
        logger.info(
            f"回填完成 — 成功: {report.succeeded} | 跳过: {report.skipped} | "
            f"失败: {len(report.failed_symbols)}"
        )
        return report

    def record_verified_selections(
        self,
        sync_report: SyncReport,
        strategy_results: dict[str, Iterable[str]],
        executed_strategies: Iterable[str],
    ) -> int:
        """按完整信号日原子替换已核验的研究选股历史。

        只接收 SYNC_COMPLETE、已发布且当日价格核验的策略结果；
        对相同日期的完全一致重跑，保留首次 recorded_at。
        与订单、流水、持仓、NAV 无关。
        """
        if sync_report.status != SYNC_COMPLETE or not sync_report.is_trade_day:
            raise DataIntegrityError("只能记录完整交易日、已发布行情对应的研究选股")
        try:
            date.fromisoformat(sync_report.target_date)
        except (TypeError, ValueError) as exc:
            raise DataIntegrityError("研究选股日期非法") from exc
        if (
            sync_report.expected_symbols <= 0
            or sync_report.coverage < self.min_daily_coverage
        ):
            raise DataIntegrityError("只能记录已通过完整性覆盖率门禁的研究选股")

        strategies = sorted({str(x).strip() for x in executed_strategies})
        if any(not name for name in strategies):
            raise DataIntegrityError("执行策略名称为空")
        if not isinstance(strategy_results, dict):
            raise TypeError("strategy_results 应为 strategy -> symbols 的字典")

        selected: dict[str, set[str]] = {}
        for strategy, symbols in strategy_results.items():
            if strategy not in strategies:
                raise DataIntegrityError(f"结果包含未执行的策略: {strategy!r}")
            for original in symbols:
                symbol = str(original).strip().zfill(6)
                if len(symbol) != 6 or not symbol.isdigit():
                    raise DataIntegrityError(f"选股代码不合法: {original!r}")
                if symbol not in sync_report.verified_symbols:
                    raise DataIntegrityError(f"{strategy} 包含未通过同日行情核验的股票: {symbol}")
                selected.setdefault(symbol, set()).add(strategy)

        now = datetime.now(ZoneInfo("Asia/Shanghai")).isoformat(timespec="seconds")
        report_date = sync_report.target_date
        with sqlite3.connect(self.db_path) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                # 必须在同一事务里验证真实同日价，不能只信任报告的字符串集合。
                published = self._get_verified_symbols_from_connection(conn, report_date)
                missing = set(sync_report.verified_symbols) - published
                if missing:
                    raise DataIntegrityError(
                        "缺少同日有效已发布行情: " + ",".join(sorted(missing))
                    )

                current = conn.execute(
                    "SELECT expected_symbols, verified_symbols, coverage, "
                    "verified_symbols_json, strategies_json, result_rows, recorded_at "
                    "FROM selection_runs WHERE market_date = ?",
                    (report_date,),
                ).fetchone()
                existing_results = conn.execute(
                    "SELECT market_date, strategy, symbol FROM selection_results "
                    "WHERE market_date = ? ORDER BY strategy, symbol",
                    (report_date,),
                ).fetchall()
                target_results = sorted(
                    (report_date, strategy, symbol)
                    for symbol, names in selected.items()
                    for strategy in names
                )
                exact_rerun = bool(
                    current
                    and current[0] == sync_report.expected_symbols
                    and current[1] == len(sync_report.verified_symbols)
                    and math.isclose(current[2], sync_report.coverage, rel_tol=0, abs_tol=1e-12)
                    and json.loads(current[3]) == sorted(sync_report.verified_symbols)
                    and json.loads(current[4]) == strategies
                    and current[5] == len(target_results)
                    and existing_results == target_results
                )
                if exact_rerun:
                    conn.rollback()
                    return len(target_results)

                conn.execute(
                    "DELETE FROM selection_results WHERE market_date = ?",
                    (report_date,),
                )
                conn.execute("DELETE FROM selection_runs WHERE market_date = ?", (report_date,))
                conn.execute(
                    """INSERT INTO selection_runs
                       (market_date, recorded_at, expected_symbols, verified_symbols,
                        coverage, verified_symbols_json, strategies_json, result_rows)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        report_date, now, sync_report.expected_symbols,
                        len(sync_report.verified_symbols), sync_report.coverage,
                        json.dumps(sorted(sync_report.verified_symbols), ensure_ascii=False),
                        json.dumps(strategies, ensure_ascii=False), len(target_results),
                    ),
                )
                conn.executemany(
                    "INSERT INTO selection_results (market_date, strategy, symbol) VALUES (?, ?, ?)",
                    target_results,
                )
                conn.commit()
                return len(target_results)
            except Exception:
                if conn.in_transaction:
                    conn.rollback()
                raise

    def get_all_symbols(self) -> list[str]:
        """获取全市场 A 股代码；远端失败与合法空结果严格区分。"""
        import baostock as bs

        previous_timeout = self._with_socket_timeout()
        last_error = "unknown stock universe error"
        try:
            login_error = _login_with_retry(bs, self.max_attempts, self.backoff_seconds)
            if login_error is not None:
                raise DataSourceUnavailable(f"baostock 登录失败: {login_error}")

            for attempt in range(self.max_attempts):
                try:
                    rs = bs.query_stock_basic(code_name="", code="")
                    if getattr(rs, "error_code", None) != "0":
                        raise RuntimeError(str(getattr(rs, "error_msg", "unknown query error")))
                    symbols: list[str] = []
                    while rs.next():
                        row = list(rs.get_row_data())
                        if len(row) < 6:
                            raise ValueError(f"unexpected stock basic row length: {len(row)}")
                        code, stock_type, status = row[0], row[4], row[5]
                        if status == "1" and stock_type == "1":
                            if not isinstance(code, str) or "." not in code:
                                raise ValueError(f"unexpected stock code: {code!r}")
                            market, raw_symbol = code.split(".", 1)
                            if market not in {"sh", "sz", "bj"}:
                                raise ValueError(f"unexpected stock market: {code!r}")
                            symbol = raw_symbol.zfill(6)
                            if len(symbol) != 6 or not symbol.isdigit():
                                raise ValueError(f"unexpected stock code: {code!r}")
                            symbols.append(symbol)
                    if getattr(rs, "error_code", None) != "0":
                        raise RuntimeError(
                            str(getattr(rs, "error_msg", "stock universe iteration failed"))
                        )
                    symbols = sorted(set(symbols))
                    if not symbols:
                        raise ValueError("baostock 返回了空股票池")
                    logger.info(f"获取股票列表完成，共 {len(symbols)} 只")
                    return symbols
                except Exception as exc:
                    last_error = f"{type(exc).__name__}: {exc}"
                    if attempt >= self.max_attempts - 1:
                        break
                    time.sleep(self.backoff_seconds * (2**attempt))
                    _safe_logout(bs)
                    login_error = _login_with_retry(
                        bs,
                        self.max_attempts,
                        self.backoff_seconds,
                    )
                    if login_error is not None:
                        raise DataSourceUnavailable(
                            f"baostock 股票池查询重连失败: {login_error}"
                        ) from exc
            raise DataSourceUnavailable(f"baostock 股票池查询失败: {last_error}")
        finally:
            _safe_logout(bs)
            socket.setdefaulttimeout(previous_timeout)

    def get_local_symbols(self) -> list[str]:
        if self._snapshot_symbols is not None:
            return list(self._snapshot_symbols)
        with sqlite3.connect(self.db_path) as conn:
            rows = conn.execute(
                "SELECT DISTINCT symbol FROM stock_daily ORDER BY symbol"
            ).fetchall()
        return [str(row[0]).zfill(6) for row in rows]

    def database_quick_check(self) -> bool:
        with sqlite3.connect(self.db_path) as conn:
            result = conn.execute("PRAGMA quick_check").fetchone()
        return bool(result and result[0] == "ok")
