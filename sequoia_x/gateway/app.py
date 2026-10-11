"""Sequoia-X V10 read-only BaoStock gateway.

Endpoints offer on-demand, raw daily OHLCV. No SQLite, trading or write endpoints.
Run one Uvicorn worker: BaoStock maintains process-level session state.
"""
from __future__ import annotations

import logging
import os
import re
import secrets
import threading
from datetime import date, datetime, timezone
from zoneinfo import ZoneInfo
from typing import Annotated

from fastapi import Depends, FastAPI, Header, HTTPException, Path, Query
from fastapi.concurrency import run_in_threadpool

from sequoia_x.data.market_api import MarketApiError, fetch_raw

logger = logging.getLogger(__name__)
app = FastAPI(
    title="Sequoia-X Read-only A-share Market Gateway",
    version="10.0.0",
    docs_url=None,
    redoc_url=None,
    openapi_url=None,  # distribute curated read-only OpenAPI spec instead
)

_session_lock = threading.Lock()
_session_connected = False
MAX_CALENDAR_DAYS = 400


def require_key(authorization: Annotated[str | None, Header()] = None) -> None:
    """Require a secret in the Authorization: Bearer header for every endpoint."""
    expected = os.environ.get("SEQUOIA_GATEWAY_API_KEY", "")
    if len(expected) < 24:
        raise HTTPException(status_code=503, detail="API key is not configured securely")
    prefix = "Bearer "
    token = authorization[len(prefix):] if authorization and authorization.startswith(prefix) else ""
    if not secrets.compare_digest(token, expected):
        raise HTTPException(status_code=401, detail="Unauthorized")


def _query_raw(symbol: str, start: str, end: str) -> list[tuple]:
    """Uses the repository's ORIGINAL fetch_raw and BaoStock adjustflag=3 path."""
    global _session_connected
    if not _session_lock.acquire(timeout=10):
        raise RuntimeError("BaoStock data provider is busy")
    try:
        import baostock as bs

        # A login is reused within the process. Re-login once on provider failure.
        for attempt in range(2):
            if not _session_connected:
                result = bs.login()
                if getattr(result, "error_code", "ERR") != "0":
                    raise RuntimeError("BaoStock login failed")
                _session_connected = True
            try:
                return fetch_raw(bs, symbol, start, end)
            except (MarketApiError, OSError, ConnectionError) as exc:
                logger.warning("BaoStock query failed for %s (attempt %s): %s", symbol, attempt + 1, exc)
                try:
                    bs.logout()
                except Exception:
                    logger.exception("BaoStock logout failed")
                _session_connected = False
                if attempt == 1:
                    raise RuntimeError("BaoStock price query failed") from exc
        raise RuntimeError("BaoStock price query failed")
    finally:
        _session_lock.release()


@app.get("/health", operation_id="healthCheck", dependencies=[Depends(require_key)])
def health() -> dict:
    return {
        "status": "OK",
        "gateway": "sequoia-x-v10",
        "readonly": True,
        "database_access": False,
        "mode": "ON_DEMAND_DAILY",
        "now_utc": datetime.now(timezone.utc).isoformat(),
    }


@app.get("/v1/stocks/{symbol}/daily", operation_id="getAshareDailyBars", dependencies=[Depends(require_key)])
async def daily_bars(
    symbol: Annotated[str, Path(description="Six-digit mainland stock code, e.g. 301363")],
    start: Annotated[date, Query(description="Start date YYYY-MM-DD")],
    end: Annotated[date, Query(description="End date YYYY-MM-DD")],
) -> dict:
    if not re.fullmatch(r"[036489][0-9]{5}", symbol, flags=re.ASCII):
        raise HTTPException(422, "Unsupported six-digit symbol")
    if end < start:
        raise HTTPException(422, "end must be >= start")
    if (end - start).days > MAX_CALENDAR_DAYS:
        raise HTTPException(422, "Maximum range is 400 calendar days per request")
    if end > datetime.now(ZoneInfo("Asia/Shanghai")).date():
        raise HTTPException(422, "Future dates are not allowed")
    try:
        rows = await run_in_threadpool(_query_raw, symbol, start.isoformat(), end.isoformat())
    except Exception as exc:
        logger.exception("Upstream price data unavailable for %s", symbol)
        raise HTTPException(status_code=502, detail="Upstream BaoStock data unavailable") from exc

    bars = [
        {"date": d, "open": o, "high": h, "low": lo, "close": c,
         "volume": vol, "turnover": turnover}
        for _, d, o, h, lo, c, vol, turnover in rows
    ]
    return {
        "schema_version": 1,
        "symbol": symbol,
        "source": "baostock_query_history_k_data_plus",
        "adjustment": "NONE",
        "frequency": "1d",
        "request": {"start": start.isoformat(), "end": end.isoformat()},
        "fetched_at_utc": datetime.now(timezone.utc).isoformat(),
        "price_asof": bars[-1]["date"] if bars else None,
        "is_realtime": False,
        "count": len(bars),
        "status": "OK" if bars else "NO_DATA",
        "bars": bars,
        "note": "Daily price observations only; not proof of live executable fills, liquidity or T+1 eligibility.",
    }
