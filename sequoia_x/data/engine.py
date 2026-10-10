"""SQLite 行情存储和受控的 BaoStock 同步。"""

from __future__ import annotations

import json
import math
import socket
import sqlite3
import time
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
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

_CREATE_STAGING_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS stock_daily_staging (
    target_date TEXT NOT NULL,
    symbol      TEXT NOT NULL,
    date        TEXT NOT NULL,
    open        REAL,
    high        REAL,
    low         REAL,
    close       REAL,
    volume      REAL,
    turnover    REAL,
    PRIMARY KEY (target_date, symbol, date)
);
"""

_CREATE_STAGING_INDEX_SQL = """
CREATE INDEX IF NOT EXISTS idx_stock_daily_staging_target
ON stock_daily_staging (target_date, symbol, date);
"""

_CREATE_SELECTION_RUNS_SQL = """
CREATE TABLE IF NOT EXISTS selection_runs (
    market_date      TEXT    PRIMARY KEY,
    recorded_at      TEXT    NOT NULL,
    expected_symbols INTEGER NOT NULL CHECK (expected_symbols >= 0),
    verified_symbols INTEGER NOT NULL CHECK (verified_symbols >= 0),
    coverage         REAL    NOT NULL CHECK (coverage >= 0.0 AND coverage <= 1.0),
    verified_symbols_json TEXT NOT NULL,
    strategies_json  TEXT    NOT NULL,
    result_rows      INTEGER NOT NULL CHECK (result_rows >= 0)
);
"""

_CREATE_SELECTION_RESULTS_SQL = """
CREATE TABLE IF NOT EXISTS selection_results (
    market_date TEXT NOT NULL,
    strategy    TEXT NOT NULL,
    symbol      TEXT NOT NULL,
    PRIMARY KEY (market_date, strategy, symbol),
    FOREIGN KEY (market_date)
        REFERENCES selection_runs (market_date)
        ON DELETE CASCADE
);
"""

_CREATE_SELECTION_RESULTS_INDEX_SQL = """
CREATE INDEX IF NOT EXISTS idx_selection_results_symbol_date
ON selection_results (symbol, market_date);
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

_STAGING_UPSERT_SQL = """
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
    result = BatchFetchResult()
    if not tasks:
        return result

    import baostock as bs

    previous_timeout = socket.getdefaulttimeout()
    socket.setdefaulttimeout(socket_timeout)
    try:
        login_error = _login_with_retry(bs, max_attempts, backoff_seconds)
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
                result.login_error = error or "BaoStock session unavailable"
                for remaining in tasks[index + 1 :]:
                    result.failed_symbols[remaining[0]] = result.login_error
                break
        return result
    except Exception as exc:
        # 兜底保护：任何未知 worker 错误只影响当前小批次。
        error = f"{type(exc).__name__}: {exc}"
        for task in tasks:
            result.failed_symbols.setdefault(task[0], error)
        return result
    finally:
        _safe_logout(bs)
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
        self.min_daily_coverage = float(settings.min_daily_coverage)
        if (
            not math.isfinite(self.min_daily_coverage)
            or not 0.0 < self.min_daily_coverage <= 1.0
        ):
            raise DataIntegrityError("MIN_DAILY_COVERAGE 必须是 (0, 1] 范围内的有限数值")
        self._snapshot_date: str | None = None
        self._snapshot_symbols: tuple[str, ...] | None = None
        self._init_db()

    def _init_db(self) -> None:
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(_CREATE_TABLE_SQL)
            conn.execute(_CREATE_INDEX_SQL)
            conn.execute(_CREATE_STAGING_TABLE_SQL)
            conn.execute(_CREATE_STAGING_INDEX_SQL)
            conn.execute(_CREATE_SELECTION_RUNS_SQL)
            conn.execute(_CREATE_SELECTION_RESULTS_SQL)
            conn.execute(_CREATE_SELECTION_RESULTS_INDEX_SQL)
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

    def record_verified_selections(
        self,
        sync_report: SyncReport,
        strategy_results: dict[str, list[str]],
        executed_strategies: Iterable[str],
    ) -> int:
        """原子保存一次完整策略运行，只接受同步门禁核验过的股票。

        同一市场日期重跑时会完整替换该日记录。该表仅保存选股审计数据，
        不包含订单、资金、持仓或 NAV，也不会读取未发布的 staging 行情。
        """
        if not sync_report.complete or not sync_report.is_trade_day:
            raise DataIntegrityError("只能记录已完成交易日的选股结果")

        market_date = date.fromisoformat(sync_report.target_date).isoformat()
        if sync_report.expected_symbols <= 0:
            raise DataIntegrityError("选股记录的预期股票数必须大于零")

        verified = self._normalise_symbols(sync_report.verified_symbols)
        if not verified:
            raise DataIntegrityError("选股记录缺少已核验股票")
        if len(verified) != len(sync_report.verified_symbols):
            raise DataIntegrityError("已核验股票存在重复或无效值")
        if len(verified) > sync_report.expected_symbols:
            raise DataIntegrityError("已核验股票数不能超过预期股票数")
        if not 0.0 <= sync_report.coverage <= 1.0:
            raise DataIntegrityError("选股记录覆盖率必须在 0 到 1 之间")
        if sync_report.coverage < self.min_daily_coverage:
            raise DataIntegrityError(
                f"选股记录覆盖率 {sync_report.coverage:.2%} 低于门槛 "
                f"{self.min_daily_coverage:.2%}"
            )

        requested_strategies = tuple(
            str(value).strip()
            for value in executed_strategies
        )
        strategies = tuple(dict.fromkeys(requested_strategies))
        if not strategies or any(not value for value in strategies):
            raise DataIntegrityError("选股记录缺少有效策略名称")
        if len(strategies) != len(requested_strategies):
            raise DataIntegrityError("选股记录包含重复策略名称")

        unknown_strategies = set(strategy_results) - set(strategies)
        if unknown_strategies:
            raise DataIntegrityError(
                "选股结果包含未执行策略: " + ", ".join(sorted(unknown_strategies))
            )

        result_rows: list[tuple[str, str, str]] = []
        for strategy in strategies:
            symbols = self._normalise_symbols(strategy_results.get(strategy, []))
            unverified = symbols - verified
            if unverified:
                preview = ", ".join(sorted(unverified)[:10])
                raise DataIntegrityError(
                    f"{strategy} 包含未通过 {market_date} 行情核验的股票: {preview}"
                )
            result_rows.extend(
                (market_date, strategy, symbol)
                for symbol in sorted(symbols)
            )

        recorded_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
        strategies_json = json.dumps(
            strategies,
            ensure_ascii=False,
            separators=(",", ":"),
        )
        verified_symbols_json = json.dumps(
            sorted(verified),
            ensure_ascii=False,
            separators=(",", ":"),
        )

        with sqlite3.connect(self.db_path) as conn:
            conn.execute("PRAGMA foreign_keys = ON")
            conn.execute("BEGIN IMMEDIATE")
            published_for_date = self._get_verified_symbols_from_connection(
                conn,
                market_date,
            )
            missing_published = sorted(verified - published_for_date)
            if missing_published:
                preview = ", ".join(missing_published[:10])
                raise DataIntegrityError(
                    f"{market_date} 已核验股票缺少同日有效已发布行情: {preview}"
                )
            conn.execute(
                "DELETE FROM selection_results WHERE market_date = ?",
                (market_date,),
            )
            conn.execute(
                "DELETE FROM selection_runs WHERE market_date = ?",
                (market_date,),
            )
            conn.execute(
                """
                INSERT INTO selection_runs (
                    market_date,
                    recorded_at,
                    expected_symbols,
                    verified_symbols,
                    coverage,
                    verified_symbols_json,
                    strategies_json,
                    result_rows
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    market_date,
                    recorded_at,
                    sync_report.expected_symbols,
                    len(verified),
                    sync_report.coverage,
                    verified_symbols_json,
                    strategies_json,
                    len(result_rows),
                ),
            )
            if result_rows:
                conn.executemany(
                    """
                    INSERT INTO selection_results (market_date, strategy, symbol)
                    VALUES (?, ?, ?)
                    """,
                    result_rows,
                )
            conn.commit()

        return len(result_rows)

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
            if len(symbol) != 6 or not symbol.isascii() or not symbol.isdigit():
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
    def _verified_symbols_from_rows(rows: Iterable[tuple]) -> set[str]:
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

    @classmethod
    def _get_verified_symbols_from_connection(
        cls,
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
        return cls._verified_symbols_from_rows(rows)

    @classmethod
    def _get_staged_verified_symbols_from_connection(
        cls,
        conn: sqlite3.Connection,
        target_date: str,
    ) -> set[str]:
        rows = conn.execute(
            """
            SELECT symbol, open, high, low, close, volume, turnover
            FROM stock_daily_staging
            WHERE target_date = ? AND date = ?
            """,
            (target_date, target_date),
        ).fetchall()
        return cls._verified_symbols_from_rows(rows)

    def _get_verified_symbols(self, target_date: str) -> set[str]:
        with sqlite3.connect(self.db_path) as conn:
            return self._get_verified_symbols_from_connection(conn, target_date)

    def _get_staged_verified_symbols(self, target_date: str) -> set[str]:
        with sqlite3.connect(self.db_path) as conn:
            return self._get_staged_verified_symbols_from_connection(conn, target_date)

    def _stage_rows(self, rows: list[tuple], target_date: str) -> int:
        """持久化一个已通过字段校验的批次，但不向策略可见表发布。"""
        if not rows:
            return 0
        staged_rows = [(target_date, *row) for row in rows]
        with sqlite3.connect(self.db_path) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                conn.executemany(_STAGING_UPSERT_SQL, staged_rows)
                conn.commit()
            except Exception:
                if conn.in_transaction:
                    conn.rollback()
                raise
        return len(staged_rows)

    def _prune_staging_before(self, target_date: str) -> int:
        """进入新交易日后删除无法再用于当日门禁的旧暂存批次。"""
        with sqlite3.connect(self.db_path) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                cursor = conn.execute(
                    "DELETE FROM stock_daily_staging WHERE target_date < ?",
                    (target_date,),
                )
                deleted = max(cursor.rowcount, 0)
                conn.commit()
                return deleted
            except Exception:
                if conn.in_transaction:
                    conn.rollback()
                raise

    def _promote_staged_rows(
        self,
        target_date: str,
        expected_symbols: set[str],
        minimum_coverage: float,
    ) -> tuple[set[str], int]:
        """在同一事务内复核覆盖率、发布暂存行情并清空当日暂存区。"""
        with sqlite3.connect(self.db_path) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                published_before = (
                    self._get_verified_symbols_from_connection(conn, target_date)
                    & expected_symbols
                )
                staged_verified = (
                    self._get_staged_verified_symbols_from_connection(conn, target_date)
                    & expected_symbols
                )
                candidate_verified = published_before | staged_verified
                coverage = (
                    len(candidate_verified) / len(expected_symbols)
                    if expected_symbols
                    else 0.0
                )
                if coverage < minimum_coverage:
                    raise DataIntegrityError(
                        f"暂存区覆盖率异常: {coverage:.2%} < {minimum_coverage:.2%}"
                    )

                staged = conn.execute(
                    """
                    SELECT symbol, date, open, high, low, close, volume, turnover
                    FROM stock_daily_staging
                    WHERE target_date = ?
                    """,
                    (target_date,),
                ).fetchall()
                publish_rows = [
                    tuple(row) for row in staged if str(row[0]).zfill(6) in expected_symbols
                ]
                if publish_rows:
                    conn.executemany(_UPSERT_SQL, publish_rows)

                verified_after = (
                    self._get_verified_symbols_from_connection(conn, target_date)
                    & expected_symbols
                )
                final_coverage = (
                    len(verified_after) / len(expected_symbols)
                    if expected_symbols
                    else 0.0
                )
                if final_coverage < minimum_coverage:
                    raise DataIntegrityError(
                        f"发布事务内覆盖率异常: {final_coverage:.2%} < "
                        f"{minimum_coverage:.2%}"
                    )

                conn.execute(
                    "DELETE FROM stock_daily_staging WHERE target_date = ?",
                    (target_date,),
                )
                integrity = conn.execute("PRAGMA quick_check").fetchone()
                if not integrity or integrity[0] != "ok":
                    raise DataIntegrityError(f"SQLite quick_check failed: {integrity}")
                conn.commit()
                return verified_after, len(publish_rows)
            except Exception:
                if conn.in_transaction:
                    conn.rollback()
                raise

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
        """同步并发布一个经过交易日、数据质量和覆盖率验证的日快照。

        每个成功批次先写入 ``stock_daily_staging``。失败或进程中断后可从
        暂存进度继续，但策略查询始终只读取 ``stock_daily``。只有合并覆盖率
        达到门槛时，暂存数据才会在一个 SQLite 事务内整体发布。
        """
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
            staged_last_rows = conn.execute(
                """
                SELECT symbol, MAX(date)
                FROM stock_daily_staging
                WHERE target_date = ?
                GROUP BY symbol
                """,
                (target_text,),
            ).fetchall()
        last_by_symbol = {str(symbol).zfill(6): last_date for symbol, last_date in last_rows}
        for symbol, last_date in staged_last_rows:
            clean_symbol = str(symbol).zfill(6)
            published_last = last_by_symbol.get(clean_symbol)
            if published_last is None or str(last_date) > str(published_last):
                last_by_symbol[clean_symbol] = last_date

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

        pruned_rows = self._prune_staging_before(target_text)
        if pruned_rows:
            logger.info(f"已清理 {pruned_rows} 条旧交易日暂存行情")

        published_before = self._get_verified_symbols(target_text) & expected
        staged_before = self._get_staged_verified_symbols(target_text) & expected
        candidate_before = published_before | staged_before
        initial_coverage = len(candidate_before) / len(expected)
        if initial_coverage >= self.min_daily_coverage:
            verified_after, promoted_rows = self._promote_staged_rows(
                target_text,
                expected,
                self.min_daily_coverage,
            )
            logger.info(
                f"{target_text} 已恢复到可发布进度："
                f"{len(verified_after)}/{len(expected)} 只，"
                f"覆盖率 {len(verified_after) / len(expected):.2%}"
            )
            return SyncReport(
                status=SYNC_COMPLETE,
                target_date=target_text,
                is_trade_day=True,
                expected_symbols=len(expected),
                verified_symbols=frozenset(verified_after),
                stale_symbols=frozenset(expected - verified_after),
                rows_written=promoted_rows,
            )

        tasks: list[tuple[str, str, str, str]] = []
        preflight_failures: dict[str, str] = {}
        for symbol in sorted(expected - candidate_before):
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
                f"需要更新 {len(tasks)} 只股票，使用最多 {self.max_workers} 个进程，"
                f"拆分为 {len(batches)} 个隔离批次"
            )
            batch_results: Iterable[BatchFetchResult] = self._iter_fetch_batches(batches)
        else:
            batches = []
            batch_results = []

        failed_symbols = dict(preflight_failures)
        staged_this_run = 0
        for batch_number, batch in enumerate(batch_results, start=1):
            failed_symbols.update(batch.failed_symbols)
            normalised, _fetched_current, invalid = self._normalise_rows(
                batch.rows,
                expected_symbols=expected,
                target_date=target_text,
            )
            failed_symbols.update(invalid)
            if normalised:
                try:
                    staged_this_run += self._stage_rows(normalised, target_text)
                except Exception as exc:
                    error = f"staging database write failed: {type(exc).__name__}: {exc}"
                    affected = {str(row[0]).zfill(6) for row in normalised}
                    failed_symbols.update({symbol: error for symbol in affected})

            if batch_number % 10 == 0 or batch_number == len(batches):
                logger.info(
                    f"日行情暂存进度：{batch_number}/{len(batches)} 批，"
                    f"本轮已校验暂存 {staged_this_run} 条"
                )

        staged_after = self._get_staged_verified_symbols(target_text) & expected
        candidate_verified = published_before | staged_after
        coverage = len(candidate_verified) / len(expected)
        stale_symbols = expected - candidate_verified
        failed_symbols = {
            symbol: error
            for symbol, error in failed_symbols.items()
            if symbol not in candidate_verified
        }

        if coverage < self.min_daily_coverage:
            if not self.database_quick_check():
                raise DataIntegrityError("暂存行情后 SQLite quick_check 失败")
            logger.error(
                f"{target_text} 行情覆盖率 {coverage:.2%} 低于门槛 "
                f"{self.min_daily_coverage:.2%}；已安全暂存，策略不可见"
            )
            return SyncReport(
                status=SYNC_INCOMPLETE,
                target_date=target_text,
                is_trade_day=True,
                expected_symbols=len(expected),
                verified_symbols=frozenset(candidate_verified),
                failed_symbols=failed_symbols,
                stale_symbols=frozenset(stale_symbols),
            )

        verified_after, promoted_rows = self._promote_staged_rows(
            target_text,
            expected,
            self.min_daily_coverage,
        )
        final_coverage = len(verified_after) / len(expected)

        logger.info(
            f"sync_today_bulk: 原子发布 {promoted_rows} 条，"
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
            rows_written=promoted_rows,
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
