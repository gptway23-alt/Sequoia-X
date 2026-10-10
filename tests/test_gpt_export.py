"""Tests for the safe SQLite snapshot consumed by GPT."""

from __future__ import annotations

import hashlib
import json
import sqlite3
from pathlib import Path

import pytest

from sequoia_x.data.gpt_export import GptExportError, export_verified_stock_database, main


def _source_database(path: Path, *, with_selections: bool = False) -> None:
    with sqlite3.connect(path) as connection:
        connection.executescript(
            """
            CREATE TABLE stock_daily (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL,
                date TEXT NOT NULL,
                open REAL,
                high REAL,
                low REAL,
                close REAL,
                volume REAL,
                turnover REAL,
                UNIQUE(symbol, date)
            );
            CREATE TABLE stock_daily_staging (
                target_date TEXT,
                symbol TEXT,
                date TEXT,
                open REAL,
                high REAL,
                low REAL,
                close REAL,
                volume REAL,
                turnover REAL
            );
            CREATE TABLE orders (secret TEXT);
            CREATE TABLE positions (secret TEXT);
            CREATE TABLE nav (secret TEXT);
            """
        )
        connection.executemany(
            """
            INSERT INTO stock_daily
                (symbol, date, open, high, low, close, volume, turnover)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                ("000001", "2026-10-08", 10, 12, 9, 11, 100, 1_100),
                ("600519", "2026-10-08", 20, 23, 19, 22, 200, 4_400),
                ("000001", "2026-10-09", 11, 13, 10, 12, 110, 1_320),
                ("600519", "2026-10-09", 22, 24, 21, 23, 210, 4_830),
            ],
        )
        connection.execute(
            """
            INSERT INTO stock_daily_staging
                (target_date, symbol, date, open, high, low, close, volume, turnover)
            VALUES ('2026-10-10', '300001', '2026-10-10', 1, 2, 1, 2, 3, 4)
            """
        )
        connection.execute("INSERT INTO orders VALUES ('do-not-export')")
        connection.execute("INSERT INTO positions VALUES ('do-not-export')")
        connection.execute("INSERT INTO nav VALUES ('do-not-export')")
        if with_selections:
            connection.executescript(
                """
                CREATE TABLE selection_runs (
                    market_date TEXT PRIMARY KEY,
                    recorded_at TEXT NOT NULL,
                    expected_symbols INTEGER NOT NULL,
                    verified_symbols INTEGER NOT NULL,
                    coverage REAL NOT NULL,
                    verified_symbols_json TEXT NOT NULL,
                    strategies_json TEXT NOT NULL,
                    result_rows INTEGER NOT NULL
                );
                CREATE TABLE selection_results (
                    market_date TEXT NOT NULL,
                    strategy TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    PRIMARY KEY (market_date, strategy, symbol),
                    FOREIGN KEY (market_date) REFERENCES selection_runs ON DELETE CASCADE
                );
                INSERT INTO selection_runs VALUES
                    ('2026-10-09', '2026-10-09T12:00:00Z', 2, 2, 1.0,
                     '["000001","600519"]', '["MaVolumeStrategy"]', 2);
                INSERT INTO selection_results VALUES
                    ('2026-10-09', 'MaVolumeStrategy', '000001'),
                    ('2026-10-09', 'MaVolumeStrategy', '600519');
                """
            )


def _digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def test_exports_only_published_prices_when_selection_tables_do_not_exist(
    tmp_path: Path,
) -> None:
    source = tmp_path / "source.db"
    output = tmp_path / "gpt-export"
    _source_database(source)

    result = export_verified_stock_database(source, output)

    assert result.row_count == 4
    assert result.symbol_count == 2
    assert result.latest_market_date == "2026-10-09"
    assert result.database_path.name == "sequoia-x-verified.sqlite3"
    assert result.database_sha256 == _digest(result.database_path)
    assert result.readme_path.read_text(encoding="utf-8").startswith("Sequoia-X GPT")

    manifest = json.loads(result.manifest_path.read_text(encoding="utf-8"))
    assert manifest["database_sha256"] == result.database_sha256
    assert manifest["latest_market_date"] == "2026-10-09"
    assert manifest["verification"] == {"market_date": None, "status": "not_requested"}
    assert manifest["integrity"] == {
        "copied_row_count": 4,
        "export_quick_check": "ok",
        "invalid_rows": 0,
        "quick_check": "ok",
        "source_quick_check": "ok",
    }
    assert manifest["selection_history"] == {
        "available": False,
        "first_market_date": None,
        "latest_market_date": None,
        "result_count": 0,
        "run_count": 0,
    }

    with sqlite3.connect(result.database_path) as connection:
        tables = {
            row[0]
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            )
        }
        rows = connection.execute(
            "SELECT symbol, date FROM stock_daily ORDER BY symbol, date"
        ).fetchall()
        quick_check = connection.execute("PRAGMA quick_check").fetchone()

    assert tables == {"stock_daily"}
    assert rows == [
        ("000001", "2026-10-08"),
        ("000001", "2026-10-09"),
        ("600519", "2026-10-08"),
        ("600519", "2026-10-09"),
    ]
    assert quick_check == ("ok",)


def test_copies_complete_selection_history_and_proves_required_date(tmp_path: Path) -> None:
    source = tmp_path / "source.db"
    output = tmp_path / "gpt-export"
    _source_database(source, with_selections=True)

    result = export_verified_stock_database(
        source,
        output,
        required_market_date="2026-10-09",
        minimum_coverage=0.98,
    )

    manifest = json.loads(result.manifest_path.read_text(encoding="utf-8"))
    assert manifest["required_market_date"] == "2026-10-09"
    assert manifest["verification"]["status"] == "complete"
    assert manifest["verification"]["coverage"] == 1.0
    assert manifest["selection_history"] == {
        "available": True,
        "first_market_date": "2026-10-09",
        "latest_market_date": "2026-10-09",
        "result_count": 2,
        "run_count": 1,
    }
    with sqlite3.connect(result.database_path) as connection:
        tables = {
            row[0]
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            )
        }
        selections = connection.execute(
            """
            SELECT market_date, strategy, symbol
            FROM selection_results
            ORDER BY symbol
            """
        ).fetchall()
    assert tables == {"stock_daily", "selection_runs", "selection_results"}
    assert selections == [
        ("2026-10-09", "MaVolumeStrategy", "000001"),
        ("2026-10-09", "MaVolumeStrategy", "600519"),
    ]


def test_required_date_rejects_missing_run(tmp_path: Path) -> None:
    source = tmp_path / "source.db"
    _source_database(source, with_selections=True)

    with pytest.raises(GptExportError, match="no complete record"):
        export_verified_stock_database(
            source,
            tmp_path / "gpt-export",
            required_market_date="2026-10-08",
        )


def test_rejects_selection_run_with_inconsistent_coverage(tmp_path: Path) -> None:
    source = tmp_path / "source.db"
    _source_database(source, with_selections=True)
    with sqlite3.connect(source) as connection:
        connection.execute(
            "UPDATE selection_runs SET coverage = 0.99 WHERE market_date = '2026-10-09'"
        )

    with pytest.raises(GptExportError, match="coverage does not match"):
        export_verified_stock_database(
            source,
            tmp_path / "gpt-export",
            required_market_date="2026-10-09",
        )


def test_rejects_selection_run_with_wrong_result_count(tmp_path: Path) -> None:
    source = tmp_path / "source.db"
    _source_database(source, with_selections=True)
    with sqlite3.connect(source) as connection:
        connection.execute(
            "UPDATE selection_runs SET result_rows = 1 WHERE market_date = '2026-10-09'"
        )

    with pytest.raises(GptExportError, match="selection_results has 2 rows"):
        export_verified_stock_database(source, tmp_path / "gpt-export")


def test_rejects_partial_selection_history_schema(tmp_path: Path) -> None:
    source = tmp_path / "source.db"
    _source_database(source)
    with sqlite3.connect(source) as connection:
        connection.execute(
            """
            CREATE TABLE selection_runs (
                market_date TEXT PRIMARY KEY,
                recorded_at TEXT NOT NULL,
                expected_symbols INTEGER NOT NULL,
                verified_symbols INTEGER NOT NULL,
                coverage REAL NOT NULL,
                verified_symbols_json TEXT NOT NULL,
                strategies_json TEXT NOT NULL,
                result_rows INTEGER NOT NULL
            )
            """
        )

    with pytest.raises(GptExportError, match="selection history is incomplete"):
        export_verified_stock_database(source, tmp_path / "gpt-export")


@pytest.mark.parametrize(
    ("column", "bad_value"),
    [
        ("symbol", "1"),
        ("date", "not-a-date"),
        ("open", -1),
        ("high", 1),
        ("volume", -1),
    ],
)
def test_rejects_any_invalid_published_market_row(
    tmp_path: Path,
    column: str,
    bad_value: object,
) -> None:
    source = tmp_path / "source.db"
    _source_database(source)
    with sqlite3.connect(source) as connection:
        connection.execute(
            f"UPDATE stock_daily SET {column} = ? WHERE symbol = '000001' AND date = '2026-10-08'",
            (bad_value,),
        )

    with pytest.raises(GptExportError, match="stock_daily contains .* invalid row"):
        export_verified_stock_database(source, tmp_path / "gpt-export")


def test_rejects_selection_outside_verified_universe(tmp_path: Path) -> None:
    source = tmp_path / "source.db"
    _source_database(source, with_selections=True)
    with sqlite3.connect(source) as connection:
        connection.execute(
            """
            UPDATE selection_results SET symbol = '300001'
            WHERE symbol = '000001'
            """
        )

    with pytest.raises(GptExportError, match="is not in verified_symbols_json"):
        export_verified_stock_database(source, tmp_path / "gpt-export")


def test_rejects_verified_universe_without_same_day_price(tmp_path: Path) -> None:
    source = tmp_path / "source.db"
    _source_database(source, with_selections=True)
    with sqlite3.connect(source) as connection:
        connection.execute(
            """
            UPDATE selection_runs
            SET verified_symbols_json = '["000001","300001"]'
            WHERE market_date = '2026-10-09'
            """
        )

    with pytest.raises(GptExportError, match="without valid same-day published prices"):
        export_verified_stock_database(source, tmp_path / "gpt-export")


def test_rejects_unicode_digits_in_published_symbol(tmp_path: Path) -> None:
    source = tmp_path / "source.db"
    _source_database(source)
    with sqlite3.connect(source) as connection:
        connection.execute(
            "UPDATE stock_daily SET symbol = '１２３４５６' WHERE symbol = '000001'"
        )

    with pytest.raises(GptExportError, match="invalid row"):
        export_verified_stock_database(source, tmp_path / "gpt-export")


def test_rejects_unicode_digits_in_verified_universe(tmp_path: Path) -> None:
    source = tmp_path / "source.db"
    _source_database(source, with_selections=True)
    with sqlite3.connect(source) as connection:
        connection.execute(
            """
            UPDATE selection_runs
            SET verified_symbols_json = '["１２３４５６","600519"]'
            WHERE market_date = '2026-10-09'
            """
        )

    with pytest.raises(GptExportError, match="invalid verified stock symbol"):
        export_verified_stock_database(source, tmp_path / "gpt-export")


def test_rejects_unicode_digits_in_integer_fields(tmp_path: Path) -> None:
    source = tmp_path / "source.db"
    _source_database(source, with_selections=True)
    with sqlite3.connect(source) as connection:
        connection.execute(
            "UPDATE selection_runs SET expected_symbols = '²' "
            "WHERE market_date = '2026-10-09'"
        )

    with pytest.raises(GptExportError, match="non-negative integer"):
        export_verified_stock_database(source, tmp_path / "gpt-export")


def test_failed_refresh_removes_old_completion_manifest(tmp_path: Path) -> None:
    source = tmp_path / "source.db"
    output = tmp_path / "gpt-export"
    output.mkdir()
    manifest = output / "manifest.json"
    manifest.write_text("stale", encoding="utf-8")
    _source_database(source)
    with sqlite3.connect(source) as connection:
        connection.execute(
            "UPDATE stock_daily SET open = -1 WHERE symbol = '000001'"
        )

    with pytest.raises(GptExportError, match="invalid row"):
        export_verified_stock_database(source, output)

    assert not manifest.exists()


@pytest.mark.parametrize("failure", ["missing_source", "invalid_date"])
def test_preflight_failure_removes_old_completion_manifest(
    tmp_path: Path,
    failure: str,
) -> None:
    output = tmp_path / "gpt-export"
    output.mkdir()
    manifest = output / "manifest.json"
    manifest.write_text("stale", encoding="utf-8")

    if failure == "missing_source":
        source = tmp_path / "missing.db"
        required_market_date = None
    else:
        source = tmp_path / "source.db"
        _source_database(source)
        required_market_date = "not-a-date"

    with pytest.raises(GptExportError):
        export_verified_stock_database(
            source,
            output,
            required_market_date=required_market_date,
        )

    assert not manifest.exists()


def test_cli_supports_required_market_date(tmp_path: Path, capsys) -> None:
    source = tmp_path / "source.db"
    output = tmp_path / "gpt-export"
    _source_database(source, with_selections=True)

    status = main(
        [
            "--source",
            str(source),
            "--output",
            str(output),
            "--required-market-date",
            "2026-10-09",
            "--minimum-coverage",
            "0.98",
        ]
    )

    assert status == 0
    cli_output = json.loads(capsys.readouterr().out)
    assert cli_output["latest_market_date"] == "2026-10-09"
    assert (output / "sequoia-x-verified.sqlite3").exists()
