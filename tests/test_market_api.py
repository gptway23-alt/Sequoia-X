
"""V9 full universe is based on original stock_daily, never selection results."""

import hashlib
import json
import sqlite3
from types import SimpleNamespace

import pytest

from sequoia_x.data.market_api import (
    MarketApiError,
    publish_full_history,
    sync_full_universe,
)


class Response:
    error_code = "0"
    error_msg = ""

    def __init__(self, rows):
        self.rows = list(rows)
        self.pos = 0

    def next(self):
        if self.pos >= len(self.rows):
            return False
        self.pos += 1
        return True

    def get_row_data(self):
        return self.rows[self.pos - 1]


def make_source(tmp_path):
    db = tmp_path / "sequoia.db"

    with sqlite3.connect(db) as c:
        c.executescript("""
        CREATE TABLE stock_daily(
            symbol TEXT,
            date TEXT,
            open REAL,
            high REAL,
            low REAL,
            close REAL,
            volume REAL,
            turnover REAL,
            PRIMARY KEY(symbol,date)
        );

        CREATE TABLE selection_results(
            market_date TEXT,
            strategy TEXT,
            symbol TEXT
        );

        INSERT INTO stock_daily VALUES
        ('000001','2026-10-08',10,11,9,10,10,100),
        ('000001','2026-10-09',10,12,9,11,12,120),
        ('600519','2026-10-08',18,19,17,18,10,180),
        ('600519','2026-10-09',19,20,18,19,11,200),
        ('300001','2026-10-09',5,6,4,5,14,140);

        INSERT INTO selection_results VALUES
        ('2026-10-09','TurtleTradeStrategy','600519');
        """)

    snapshot = tmp_path / "snapshot.sqlite3"
    snapshot.write_bytes(
        b"valid-test-snapshot"
    )

    manifest = tmp_path / "manifest.json"

    manifest.write_text(
        json.dumps({
            "database_sha256": hashlib.sha256(
                snapshot.read_bytes()
            ).hexdigest(),
            "required_market_date": "2026-10-09",
            "verification": {
                "status": "complete"
            },
        }),
        encoding="utf-8",
    )

    return db, snapshot, manifest


def fake_bs(missing_symbol=None):
    calls = []

    def query(code, fields, **kwargs):
        calls.append(
            (code, kwargs)
        )

        if code == missing_symbol:
            return Response([])

        start = kwargs["start_date"]

        prices = {
            "sz.000001": [
                ("2026-10-08", 10),
                ("2026-10-09", 11),
            ],
            "sh.600519": [
                ("2026-10-08", 18),
                ("2026-10-09", 19),
            ],
            "sz.300001": [
                ("2026-10-09", 5),
            ],
        }

        rows = [
            [
                d,
                str(p),
                str(p + 1),
                str(p - 1),
                str(p),
                "10",
                "120",
            ]
            for d, p in prices[code]
            if start <= d <= kwargs["end_date"]
        ]

        return Response(rows)

    bs = SimpleNamespace(
        query_history_k_data_plus=query,
        login=lambda: SimpleNamespace(
            error_code="0",
            error_msg="",
        ),
        logout=lambda: None,
    )

    return bs, calls


def test_full_universe_all_source_symbols_and_dates(
    tmp_path,
):
    db, snap, manifest = make_source(tmp_path)
    bs, calls = fake_bs()

    sync = sync_full_universe(
        db,
        "2026-10-09",
        bs=bs,
    )

    assert sync["data_status"] == "COMPLETE"
    assert sync["source_symbols"] == 3
    assert sync["raw_rows"] == 5

    assert all(
        a[1]["adjustflag"] == "3"
        for a in calls
    )

    assert {
        x[0] for x in calls
    } == {
        "sz.000001",
        "sh.600519",
        "sz.300001",
    }

    out = (
        tmp_path
        / "public"
        / "api"
        / "v1"
    )

    meta = publish_full_history(
        db,
        snap,
        manifest,
        out,
        "2026-10-09",
        limit_mb=20,
    )

    assert meta["data_status"] == "COMPLETE"
    assert meta["published_symbols"] == 3
    assert meta["raw_rows"] == 5

    # This stock is NOT in the selection history.
    assert (
        out / "stock/000001.json"
    ).exists()

    # The previous trading day is included.
    assert (
        out / "eod/2026-10-08.json"
    ).exists()

    assert len(
        json.loads(
            (
                out / "eod/2026-10-09.json"
            ).read_text()
        )["data"]
    ) == 3

    assert (
        out / "download/raw-2026.jsonl.gz"
    ).exists()

    assert "strategy" not in (
        out / "meta.json"
    ).read_text().lower()


def test_partial_can_resume_and_remains_honest(
    tmp_path,
):
    db, snap, manifest = make_source(tmp_path)
    bs, calls = fake_bs()

    first = sync_full_universe(
        db,
        "2026-10-09",
        bs=bs,
        max_symbols=1,
    )

    assert first["data_status"] == "PARTIAL"
    assert first["pending_symbols"] == 2

    second = sync_full_universe(
        db,
        "2026-10-09",
        bs=bs,
        max_symbols=1,
    )

    assert second["pending_symbols"] == 1

    third = sync_full_universe(
        db,
        "2026-10-09",
        bs=bs,
        max_symbols=1,
    )

    assert third["data_status"] == "COMPLETE"
    assert len(calls) == 3


def test_stock_with_no_raw_prices_stays_partial(
    tmp_path,
):
    db, snap, manifest = make_source(tmp_path)
    bs, _ = fake_bs(
        missing_symbol="sz.300001"
    )

    result = sync_full_universe(
        db,
        "2026-10-09",
        bs=bs,
    )

    assert result["pending_symbols"] == 1

    out = (
        tmp_path
        / "public"
        / "api"
        / "v1"
    )

    meta = publish_full_history(
        db,
        snap,
        manifest,
        out,
        "2026-10-09",
    )

    assert meta["data_status"] == "PARTIAL"

    probs = json.loads(
        (out / "missing.json").read_text()
    )["pending_or_failed"]

    assert probs[0]["symbol"] == "300001"
    assert probs[0]["status"] == "GAP"


def test_modified_manifest_fails(tmp_path):
    db, snap, manifest = make_source(tmp_path)
    bs, _ = fake_bs()

    sync_full_universe(
        db,
        "2026-10-09",
        bs=bs,
    )

    manifest.write_text(
        '{"database_sha256":"fake"}',
        encoding="utf-8",
    )

    with pytest.raises(
        MarketApiError,
        match="SHA-256",
    ):
        publish_full_history(
            db,
            snap,
            manifest,
            tmp_path / "public/api/v1",
            "2026-10-09",
        )


def test_no_publishing_post_adjusted_as_raw(
    tmp_path,
):
    db, snap, manifest = make_source(tmp_path)

    with pytest.raises(
        MarketApiError,
        match="Unadjusted database empty",
    ):
        publish_full_history(
            db,
            snap,
            manifest,
            tmp_path / "public/api/v1",
            "2026-10-09",
        )
