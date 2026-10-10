"""Package and email the verified GPT database snapshot.

This module never reads or sends the production ``data/sequoia_v2.db`` file.
It accepts only the three-file, whitelist-filtered snapshot produced by
``sequoia_x.data.gpt_export`` and revalidates that snapshot before any network
connection is opened.
"""

from __future__ import annotations

import argparse
import hashlib
import html
import json
import math
import os
import re
import shutil
import smtplib
import sqlite3
import tempfile
import zipfile
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from email.message import EmailMessage
from email.policy import SMTP
from enum import Enum
from pathlib import Path
from urllib.parse import urlsplit

from sequoia_x.data.gpt_export import (
    DATABASE_FILENAME,
    EXPORT_SCHEMA_VERSION,
    MANIFEST_FILENAME,
    README_FILENAME,
    _EXPORTED_COLUMNS as STOCK_DAILY_COLUMNS,
    _README_TEXT as VERIFIED_README_TEXT,
    _SELECTION_RESULT_COLUMNS as SELECTION_RESULT_COLUMNS,
    _SELECTION_RUN_COLUMNS as SELECTION_RUN_COLUMNS,
)
from sequoia_x.notify.email import (
    RECIPIENTS,
    EmailConfigurationError,
    EmailNotifier,
    EmailSendStatus,
    SentCheckError,
)


DEFAULT_PART_BYTES = 16 * 1024 * 1024
DEFAULT_MAX_PARTS = 24
MAX_PART_BYTES = 16 * 1024 * 1024
MAX_PARTS = 50
MAX_ENCODED_MESSAGE_BYTES = 24_000_000
MAX_MANIFEST_BYTES = 2 * 1024 * 1024
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_SYMBOL_RE = re.compile(r"^[0-9]{6}$")
_PACKAGE_ENTRIES = (DATABASE_FILENAME, MANIFEST_FILENAME, README_FILENAME)
_ALLOWED_TABLES = {"stock_daily", "selection_runs", "selection_results"}
_EXPECTED_INDEXES = {
    "idx_stock_daily_date": ("stock_daily", ("date", "symbol")),
    "idx_selection_results_symbol_date": (
        "selection_results",
        ("symbol", "market_date"),
    ),
}
_MANIFEST_KEYS = {
    "schema_version",
    "created_at_utc",
    "database_file",
    "database_sha256",
    "included_tables",
    "stock_daily_columns",
    "row_count",
    "symbol_count",
    "first_market_date",
    "latest_market_date",
    "integrity",
    "required_market_date",
    "verification",
    "content_policy",
    "scope",
    "selection_history",
}
_VERIFICATION_KEYS = {
    "status",
    "market_date",
    "recorded_at",
    "verified_symbol_count",
    "expected_symbol_count",
    "coverage",
    "minimum_coverage",
    "result_count",
    "verified_symbols_sha256",
}
_CONTENT_POLICY = {
    "published_market_data_only": True,
    "staging_excluded": True,
    "orders_funds_positions_nav_excluded": True,
    "unknown_source_tables_excluded": True,
}
_SCOPE = {
    "all_published_source_rows_copied": True,
    "historical_full_market_coverage_proven": False,
    "selection_history_starts_when_feature_was_enabled": True,
}
_EXPECTED_COLUMNS = {
    "stock_daily": tuple(STOCK_DAILY_COLUMNS),
    "selection_runs": tuple(SELECTION_RUN_COLUMNS),
    "selection_results": tuple(SELECTION_RESULT_COLUMNS),
}
_EXPECTED_COLUMN_SPECS = {
    "stock_daily": (
        ("symbol", "TEXT", 1, None, 1),
        ("date", "TEXT", 1, None, 2),
        ("open", "REAL", 0, None, 0),
        ("high", "REAL", 0, None, 0),
        ("low", "REAL", 0, None, 0),
        ("close", "REAL", 0, None, 0),
        ("volume", "REAL", 0, None, 0),
        ("turnover", "REAL", 0, None, 0),
    ),
    "selection_runs": (
        ("market_date", "TEXT", 0, None, 1),
        ("recorded_at", "TEXT", 1, None, 0),
        ("expected_symbols", "INTEGER", 1, None, 0),
        ("verified_symbols", "INTEGER", 1, None, 0),
        ("verified_symbols_json", "TEXT", 1, None, 0),
        ("coverage", "REAL", 1, None, 0),
        ("strategies_json", "TEXT", 1, None, 0),
        ("result_rows", "INTEGER", 1, None, 0),
    ),
    "selection_results": (
        ("market_date", "TEXT", 1, None, 1),
        ("strategy", "TEXT", 1, None, 2),
        ("symbol", "TEXT", 1, None, 3),
    ),
}
_SIDECAR_SUFFIXES = ("-wal", "-shm", "-journal")


class DatabaseEmailError(RuntimeError):
    """Raised when a verified database package cannot be prepared or sent."""


class DatabaseEmailStatus(str, Enum):
    """Aggregate result for a complete database delivery set."""

    SENT = "sent"
    ALREADY_SENT = "already_sent"
    RESUMED = "resumed"


@dataclass(frozen=True)
class DatabasePackage:
    """Validated deterministic ZIP and its delivery metadata."""

    path: Path
    archive_sha256: str
    database_sha256: str
    market_date: str
    size_bytes: int
    part_bytes: int
    part_count: int


@dataclass(frozen=True)
class DatabasePart:
    """One byte range of a database package."""

    number: int
    filename: str
    offset: int
    size_bytes: int
    sha256: str


@dataclass(frozen=True)
class DatabasePartDelivery:
    """SENT evidence for one recipient and one package part."""

    recipient: str
    part_number: int
    filename: str
    message_id: str
    status: EmailSendStatus
    attempts: int


@dataclass(frozen=True)
class DatabaseEmailResult:
    """Complete result for all fixed recipients and all package parts."""

    status: DatabaseEmailStatus
    package: DatabasePackage
    deliveries: tuple[DatabasePartDelivery, ...]

    @property
    def sent_messages(self) -> int:
        return sum(item.status is EmailSendStatus.SENT for item in self.deliveries)

    @property
    def already_sent_messages(self) -> int:
        return sum(
            item.status is EmailSendStatus.ALREADY_SENT for item in self.deliveries
        )

    @property
    def smtp_attempts(self) -> int:
        return sum(item.attempts for item in self.deliveries)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _strict_positive_integer(value: object, name: str, maximum: int) -> int:
    if isinstance(value, bool):
        raise DatabaseEmailError(f"{name} must be a positive integer")
    if isinstance(value, str):
        text = value.strip()
        if not text.isascii() or not text.isdigit():
            raise DatabaseEmailError(f"{name} must be a positive integer")
        number = int(text)
    elif isinstance(value, int):
        number = value
    else:
        raise DatabaseEmailError(f"{name} must be a positive integer")
    if not 1 <= number <= maximum:
        raise DatabaseEmailError(f"{name} must be between 1 and {maximum}")
    return number


def _strict_non_negative_integer(value: object, name: str) -> int:
    if isinstance(value, bool):
        raise DatabaseEmailError(f"{name} must be a non-negative integer")
    if isinstance(value, str):
        text = value.strip()
        if not text.isascii() or not text.isdigit():
            raise DatabaseEmailError(f"{name} must be a non-negative integer")
        number = int(text)
    elif isinstance(value, int):
        number = value
    else:
        raise DatabaseEmailError(f"{name} must be a non-negative integer")
    if number < 0:
        raise DatabaseEmailError(f"{name} must be a non-negative integer")
    return number


def _strict_coverage(value: object, name: str) -> float:
    if isinstance(value, bool):
        raise DatabaseEmailError(f"{name} must be a number in (0, 1]")
    try:
        number = float(value)
    except (TypeError, ValueError) as exc:
        raise DatabaseEmailError(f"{name} must be a number in (0, 1]") from exc
    if not math.isfinite(number) or not 0 < number <= 1:
        raise DatabaseEmailError(f"{name} must be a number in (0, 1]")
    return number


def _require_exact_keys(value: object, expected: set[str], name: str) -> dict[str, object]:
    if not isinstance(value, dict) or set(value) != expected:
        raise DatabaseEmailError(f"{name} does not have the exact expected fields")
    return value


def _strict_timestamp(value: object, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise DatabaseEmailError(f"{name} is invalid")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise DatabaseEmailError(f"{name} is invalid") from exc
    if parsed.tzinfo is None:
        raise DatabaseEmailError(f"{name} must include a timezone")
    return value


def _validated_download_url(value: str | None) -> str | None:
    if value is None or not value.strip():
        return None
    candidate = value.strip()
    if (
        len(candidate) > 2048
        or not candidate.isascii()
        or any(character.isspace() for character in candidate)
    ):
        raise DatabaseEmailError("database download URL is invalid")
    parsed = urlsplit(candidate)
    if (
        parsed.scheme != "https"
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
        or parsed.fragment
    ):
        raise DatabaseEmailError("database download URL must be a safe HTTPS URL")
    return candidate


def _reject_sqlite_sidecars(database_path: Path) -> None:
    for suffix in _SIDECAR_SUFFIXES:
        sidecar = Path(f"{database_path}{suffix}")
        if sidecar.exists():
            raise DatabaseEmailError(
                f"SQLite sidecar is present and cannot be emailed safely: {sidecar.name}"
            )


def _table_columns(connection: sqlite3.Connection, table: str) -> tuple[str, ...]:
    return tuple(str(row[1]) for row in connection.execute(f'PRAGMA table_info("{table}")'))


def _table_column_specs(
    connection: sqlite3.Connection,
    table: str,
) -> tuple[tuple[object, ...], ...]:
    return tuple(
        (str(row[1]), str(row[2]).upper(), int(row[3]), row[4], int(row[5]))
        for row in connection.execute(f'PRAGMA table_info("{table}")')
    )


def _validate_schema(connection: sqlite3.Connection) -> None:
    objects = connection.execute(
        "SELECT type, name, tbl_name FROM sqlite_master "
        "WHERE name NOT LIKE 'sqlite_%' ORDER BY type, name"
    ).fetchall()
    tables = {str(name) for kind, name, _ in objects if kind == "table"}
    if tables != _ALLOWED_TABLES:
        raise DatabaseEmailError(f"SQLite table whitelist mismatch: {sorted(tables)}")
    if any(kind in {"view", "trigger"} for kind, _, _ in objects):
        raise DatabaseEmailError("SQLite snapshot contains an unexpected view or trigger")

    for table, expected in _EXPECTED_COLUMNS.items():
        if _table_columns(connection, table) != expected:
            raise DatabaseEmailError(f"SQLite {table} column schema mismatch")
        if _table_column_specs(connection, table) != _EXPECTED_COLUMN_SPECS[table]:
            raise DatabaseEmailError(f"SQLite {table} column definitions mismatch")

    actual_indexes = {
        str(name): str(table)
        for kind, name, table in objects
        if kind == "index"
    }
    if set(actual_indexes) != set(_EXPECTED_INDEXES):
        raise DatabaseEmailError(
            f"SQLite explicit index whitelist mismatch: {sorted(actual_indexes)}"
        )
    for index, (expected_table, expected_columns) in _EXPECTED_INDEXES.items():
        if actual_indexes[index] != expected_table:
            raise DatabaseEmailError(f"SQLite index {index} is attached to the wrong table")
        columns = tuple(
            str(row[2]) for row in connection.execute(f'PRAGMA index_info("{index}")')
        )
        if columns != expected_columns:
            raise DatabaseEmailError(f"SQLite index {index} column schema mismatch")

    foreign_keys = connection.execute(
        'PRAGMA foreign_key_list("selection_results")'
    ).fetchall()
    expected_foreign_key = (
        "selection_runs",
        "market_date",
        "market_date",
        "NO ACTION",
        "CASCADE",
    )
    normalized_foreign_keys = tuple(
        (str(row[2]), str(row[3]), str(row[4]), str(row[5]), str(row[6]))
        for row in foreign_keys
    )
    if normalized_foreign_keys != (expected_foreign_key,):
        raise DatabaseEmailError("SQLite selection_results foreign key schema mismatch")
    for table in ("stock_daily", "selection_runs"):
        if connection.execute(f'PRAGMA foreign_key_list("{table}")').fetchall():
            raise DatabaseEmailError(f"SQLite {table} has an unexpected foreign key")


def _is_valid_market_values(row: tuple[object, ...]) -> bool:
    if len(row) != 7 or not _SYMBOL_RE.fullmatch(str(row[0])):
        return False
    try:
        open_price, high, low, close, volume, turnover = (
            float(value) for value in row[1:]
        )
    except (TypeError, ValueError):
        return False
    values = (open_price, high, low, close, volume, turnover)
    return (
        all(math.isfinite(value) for value in values)
        and min(open_price, high, low, close) > 0
        and high >= max(open_price, low, close)
        and low <= min(open_price, high, close)
        and volume >= 0
        and turnover >= 0
    )


def _read_manifest(path: Path) -> dict[str, object]:
    if path.is_symlink() or not path.is_file():
        raise DatabaseEmailError(f"missing regular export file: {path.name}")
    if path.stat().st_size <= 0 or path.stat().st_size > MAX_MANIFEST_BYTES:
        raise DatabaseEmailError("manifest.json has an invalid size")
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise DatabaseEmailError("manifest.json is not valid UTF-8 JSON") from exc
    if not isinstance(payload, dict):
        raise DatabaseEmailError("manifest.json must contain a JSON object")
    return payload


def _validate_export(
    export_dir: Path,
    market_date: str,
    minimum_coverage: float,
) -> tuple[dict[str, object], Path, str, bytes, bytes]:
    try:
        normalized_date = EmailNotifier._validate_market_date(market_date)
    except (TypeError, ValueError) as exc:
        raise DatabaseEmailError(str(exc)) from exc
    trusted_minimum = _strict_coverage(minimum_coverage, "minimum_coverage")
    if export_dir.is_symlink() or not export_dir.is_dir():
        raise DatabaseEmailError(f"export directory is unavailable: {export_dir}")

    paths = {name: export_dir / name for name in _PACKAGE_ENTRIES}
    for name, path in paths.items():
        if path.is_symlink() or not path.is_file() or path.stat().st_size <= 0:
            raise DatabaseEmailError(f"missing or empty regular export file: {name}")

    readme_bytes = paths[README_FILENAME].read_bytes()
    if readme_bytes != VERIFIED_README_TEXT.encode("utf-8"):
        raise DatabaseEmailError("README.txt is not the trusted exporter README")

    manifest = _require_exact_keys(
        _read_manifest(paths[MANIFEST_FILENAME]),
        _MANIFEST_KEYS,
        "manifest.json",
    )
    if manifest.get("schema_version") != EXPORT_SCHEMA_VERSION:
        raise DatabaseEmailError("manifest.json has an unsupported schema_version")
    _strict_timestamp(manifest.get("created_at_utc"), "manifest created_at_utc")
    if manifest.get("database_file") != DATABASE_FILENAME:
        raise DatabaseEmailError("manifest.json names an unexpected database file")
    if manifest.get("required_market_date") != normalized_date:
        raise DatabaseEmailError("manifest.json does not match the requested market date")
    if manifest.get("stock_daily_columns") != list(STOCK_DAILY_COLUMNS):
        raise DatabaseEmailError("manifest.json stock_daily column schema mismatch")

    verification = _require_exact_keys(
        manifest.get("verification"),
        _VERIFICATION_KEYS,
        "manifest verification",
    )
    if (
        verification.get("status") != "complete"
        or verification.get("market_date") != normalized_date
    ):
        raise DatabaseEmailError("database snapshot has not passed the market-date gate")
    verification_recorded_at = _strict_timestamp(
        verification.get("recorded_at"),
        "manifest verification recorded_at",
    )

    if manifest.get("content_policy") != _CONTENT_POLICY:
        raise DatabaseEmailError("manifest.json does not prove the export content policy")
    if manifest.get("scope") != _SCOPE:
        raise DatabaseEmailError("manifest.json has an unexpected export scope")
    integrity = _require_exact_keys(
        manifest.get("integrity"),
        {
            "quick_check",
            "source_quick_check",
            "export_quick_check",
            "invalid_rows",
            "copied_row_count",
        },
        "manifest integrity",
    )
    if (
        integrity.get("quick_check") != "ok"
        or integrity.get("source_quick_check") != "ok"
        or integrity.get("export_quick_check") != "ok"
        or integrity.get("invalid_rows") != 0
    ):
        raise DatabaseEmailError("manifest.json integrity evidence is invalid")

    included_tables = manifest.get("included_tables")
    if included_tables != ["stock_daily", "selection_runs", "selection_results"]:
        raise DatabaseEmailError("database snapshot does not contain the exact table whitelist")

    expected_sha = manifest.get("database_sha256")
    if not isinstance(expected_sha, str) or not _SHA256_RE.fullmatch(expected_sha):
        raise DatabaseEmailError("manifest.json has an invalid database_sha256")
    database_path = paths[DATABASE_FILENAME]
    _reject_sqlite_sidecars(database_path)
    actual_sha = _sha256(database_path)
    if actual_sha != expected_sha:
        raise DatabaseEmailError("database SHA-256 does not match manifest.json")

    expected_rows = _strict_non_negative_integer(
        manifest.get("row_count"), "manifest row_count"
    )
    expected_symbols = _strict_non_negative_integer(
        manifest.get("symbol_count"), "manifest symbol_count"
    )
    if expected_rows <= 0 or expected_symbols <= 0:
        raise DatabaseEmailError("manifest stock_daily counts must be positive")
    if integrity.get("copied_row_count") != expected_rows:
        raise DatabaseEmailError("manifest copied_row_count does not match row_count")
    first_market_date = manifest.get("first_market_date")
    latest_market_date = manifest.get("latest_market_date")
    try:
        first_market_date = EmailNotifier._validate_market_date(first_market_date)  # type: ignore[arg-type]
        latest_market_date = EmailNotifier._validate_market_date(latest_market_date)
    except (TypeError, ValueError) as exc:
        raise DatabaseEmailError("manifest market-date range is invalid") from exc
    if first_market_date > latest_market_date or latest_market_date < normalized_date:
        raise DatabaseEmailError("database latest date is older than the requested market date")

    uri = f"{database_path.resolve().as_uri()}?mode=ro&immutable=1"
    try:
        with sqlite3.connect(uri, uri=True, timeout=30) as connection:
            connection.execute("PRAGMA query_only = ON")
            quick_check = connection.execute("PRAGMA quick_check").fetchone()
            if quick_check != ("ok",):
                raise DatabaseEmailError(f"SQLite quick_check failed: {quick_check}")
            foreign_key_rows = connection.execute("PRAGMA foreign_key_check").fetchall()
            if foreign_key_rows:
                raise DatabaseEmailError("SQLite foreign_key_check failed")
            _validate_schema(connection)
            row = connection.execute(
                "SELECT COUNT(*), COUNT(DISTINCT symbol), MIN(date), MAX(date) "
                "FROM stock_daily"
            ).fetchone()
            if row != (
                expected_rows,
                expected_symbols,
                first_market_date,
                latest_market_date,
            ):
                raise DatabaseEmailError(
                    "SQLite stock_daily counts do not match manifest.json"
                )
            run = connection.execute(
                "SELECT expected_symbols, verified_symbols, coverage, result_rows, "
                "verified_symbols_json, strategies_json, recorded_at "
                "FROM selection_runs WHERE market_date = ?",
                (normalized_date,),
            ).fetchone()
            same_day_rows = connection.execute(
                "SELECT symbol, open, high, low, close, volume, turnover "
                "FROM stock_daily WHERE date = ?",
                (normalized_date,),
            ).fetchall()
            same_day_symbols = {str(row[0]) for row in same_day_rows}
            selected_rows = connection.execute(
                "SELECT strategy, symbol FROM selection_results WHERE market_date = ?",
                (normalized_date,),
            ).fetchall()
            selection_counts = connection.execute(
                "SELECT COUNT(*), MIN(market_date), MAX(market_date) FROM selection_runs"
            ).fetchone()
            total_selection_results = int(
                connection.execute("SELECT COUNT(*) FROM selection_results").fetchone()[0]
            )
    except DatabaseEmailError:
        raise
    except sqlite3.Error as exc:
        raise DatabaseEmailError("unable to validate exported SQLite database") from exc

    if run is None:
        raise DatabaseEmailError("SQLite has no selection run for the requested market date")
    if any(not _is_valid_market_values(tuple(row)) for row in same_day_rows):
        raise DatabaseEmailError("SQLite has invalid same-day published market data")
    run_expected = _strict_non_negative_integer(run[0], "selection expected_symbols")
    run_verified = _strict_non_negative_integer(run[1], "selection verified_symbols")
    run_results = _strict_non_negative_integer(run[3], "selection result_rows")
    try:
        run_coverage = float(run[2])
    except (TypeError, ValueError) as exc:
        raise DatabaseEmailError("selection coverage is invalid") from exc
    if not math.isfinite(run_coverage):
        raise DatabaseEmailError("selection coverage is invalid")
    run_recorded_at = _strict_timestamp(run[6], "selection recorded_at")
    if (
        run_expected <= 0
        or run_verified <= 0
        or run_verified > run_expected
        or not 0 < run_coverage <= 1
        or not math.isclose(
            run_coverage, run_verified / run_expected, rel_tol=0, abs_tol=1e-12
        )
    ):
        raise DatabaseEmailError("selection run failed its coverage invariants")
    try:
        verified_universe = json.loads(str(run[4]))
        strategies = json.loads(str(run[5]))
    except json.JSONDecodeError as exc:
        raise DatabaseEmailError("selection run contains invalid JSON") from exc
    if (
        not isinstance(verified_universe, list)
        or len(verified_universe) != run_verified
        or any(
            not isinstance(symbol, str) or not _SYMBOL_RE.fullmatch(symbol)
            for symbol in verified_universe
        )
        or verified_universe != sorted(set(verified_universe))
    ):
        raise DatabaseEmailError("selection run has an invalid verified universe")
    if (
        not isinstance(strategies, list)
        or any(not isinstance(strategy, str) or not strategy for strategy in strategies)
        or len(strategies) != len(set(strategies))
    ):
        raise DatabaseEmailError("selection run has an invalid strategy list")
    verified_set = set(verified_universe)
    if not verified_set.issubset(same_day_symbols):
        raise DatabaseEmailError(
            "verified universe contains symbols without same-day published prices"
        )
    if len(selected_rows) != run_results or any(
        not isinstance(strategy, str)
        or strategy not in strategies
        or not isinstance(symbol, str)
        or symbol not in verified_set
        for strategy, symbol in selected_rows
    ):
        raise DatabaseEmailError("selection results do not match the verified run")
    manifest_expected = _strict_non_negative_integer(
        verification.get("expected_symbol_count"),
        "manifest verification expected_symbol_count",
    )
    manifest_verified = _strict_non_negative_integer(
        verification.get("verified_symbol_count"),
        "manifest verification verified_symbol_count",
    )
    manifest_results = _strict_non_negative_integer(
        verification.get("result_count"),
        "manifest verification result_count",
    )
    raw_manifest_coverage = verification.get("coverage")
    if isinstance(raw_manifest_coverage, bool):
        raise DatabaseEmailError("manifest verification coverage is invalid")
    try:
        manifest_coverage = float(raw_manifest_coverage)
    except (TypeError, ValueError) as exc:
        raise DatabaseEmailError("manifest verification coverage is invalid") from exc
    if not math.isfinite(manifest_coverage):
        raise DatabaseEmailError("manifest verification coverage is invalid")
    raw_minimum_coverage = verification.get("minimum_coverage")
    manifest_minimum = _strict_coverage(
        raw_minimum_coverage,
        "manifest minimum_coverage",
    )
    if (
        manifest_minimum + 1e-12 < trusted_minimum
        or run_coverage + 1e-12 < manifest_minimum
        or run_coverage + 1e-12 < trusted_minimum
    ):
        raise DatabaseEmailError("selection run is below the trusted coverage gate")
    universe_sha = hashlib.sha256(
        "\n".join(verified_universe).encode("ascii")
    ).hexdigest()
    if (
        manifest_expected != run_expected
        or manifest_verified != run_verified
        or manifest_results != run_results
        or verification.get("verified_symbols_sha256") != universe_sha
        or verification.get("recorded_at") != run_recorded_at
        or not math.isclose(
            manifest_coverage, run_coverage, rel_tol=0, abs_tol=1e-12
        )
    ):
        raise DatabaseEmailError("selection run does not match manifest verification")

    selection_history = _require_exact_keys(
        manifest.get("selection_history"),
        {
            "available",
            "run_count",
            "result_count",
            "first_market_date",
            "latest_market_date",
        },
        "manifest selection_history",
    )
    if selection_counts is None:
        raise DatabaseEmailError("SQLite selection history is unavailable")
    actual_run_count = int(selection_counts[0])
    if (
        selection_history.get("available") is not True
        or selection_history.get("run_count") != actual_run_count
        or selection_history.get("result_count") != total_selection_results
        or selection_history.get("first_market_date") != selection_counts[1]
        or selection_history.get("latest_market_date") != selection_counts[2]
        or actual_run_count <= 0
    ):
        raise DatabaseEmailError("selection history does not match manifest.json")

    _reject_sqlite_sidecars(database_path)
    canonical_manifest = dict(manifest)
    canonical_manifest["created_at_utc"] = verification_recorded_at
    canonical_manifest_bytes = (
        json.dumps(canonical_manifest, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    ).encode("utf-8")
    return manifest, database_path, actual_sha, canonical_manifest_bytes, readme_bytes


def _write_zip_entry(archive: zipfile.ZipFile, source: Path, name: str) -> None:
    info = zipfile.ZipInfo(name, date_time=(1980, 1, 1, 0, 0, 0))
    info.compress_type = zipfile.ZIP_DEFLATED
    info.create_system = 3
    info.external_attr = 0o100600 << 16
    with source.open("rb") as input_file, archive.open(
        info, "w", force_zip64=True
    ) as output_file:
        shutil.copyfileobj(input_file, output_file, length=1024 * 1024)


def _write_zip_bytes(archive: zipfile.ZipFile, content: bytes, name: str) -> None:
    info = zipfile.ZipInfo(name, date_time=(1980, 1, 1, 0, 0, 0))
    info.compress_type = zipfile.ZIP_DEFLATED
    info.create_system = 3
    info.external_attr = 0o100600 << 16
    archive.writestr(info, content)


def build_database_package(
    export_dir: str | Path,
    package_dir: str | Path,
    market_date: str,
    *,
    part_bytes: int = DEFAULT_PART_BYTES,
    max_parts: int = DEFAULT_MAX_PARTS,
    minimum_coverage: float = 0.98,
) -> tuple[DatabasePackage, tuple[DatabasePart, ...]]:
    """Validate an export, build an exact whitelist ZIP, and preflight all parts."""

    part_bytes = _strict_positive_integer(part_bytes, "part_bytes", MAX_PART_BYTES)
    max_parts = _strict_positive_integer(max_parts, "max_parts", MAX_PARTS)
    export_path = Path(export_dir)
    package_path = Path(package_dir)
    (
        _,
        database_path,
        database_sha,
        canonical_manifest_bytes,
        readme_bytes,
    ) = _validate_export(export_path, market_date, minimum_coverage)

    package_path.mkdir(parents=True, exist_ok=True)
    if package_path.is_symlink() or not package_path.is_dir():
        raise DatabaseEmailError(f"package directory is unavailable: {package_path}")

    database_descriptor, database_temporary_name = tempfile.mkstemp(
        prefix=".sequoia-x-database-",
        suffix=".sqlite3.tmp",
        dir=package_path,
    )
    os.close(database_descriptor)
    database_temporary_path = Path(database_temporary_name)
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=".sequoia-x-database-",
        suffix=".zip.tmp",
        dir=package_path,
    )
    os.close(descriptor)
    temporary_path = Path(temporary_name)
    try:
        with database_path.open("rb") as source, database_temporary_path.open(
            "wb"
        ) as destination:
            shutil.copyfileobj(source, destination, length=1024 * 1024)
            destination.flush()
            os.fsync(destination.fileno())
        _reject_sqlite_sidecars(database_path)
        if _sha256(database_temporary_path) != database_sha:
            raise DatabaseEmailError("database changed between validation and packaging")

        with zipfile.ZipFile(
            temporary_path,
            mode="w",
            compression=zipfile.ZIP_DEFLATED,
            compresslevel=9,
            allowZip64=True,
        ) as archive:
            _write_zip_entry(archive, database_temporary_path, DATABASE_FILENAME)
            _write_zip_bytes(archive, canonical_manifest_bytes, MANIFEST_FILENAME)
            _write_zip_bytes(archive, readme_bytes, README_FILENAME)

        with zipfile.ZipFile(temporary_path, mode="r") as archive:
            if archive.namelist() != list(_PACKAGE_ENTRIES):
                raise DatabaseEmailError("database ZIP entry whitelist mismatch")
            bad_entry = archive.testzip()
            if bad_entry is not None:
                raise DatabaseEmailError(f"database ZIP CRC failed: {bad_entry}")

        archive_sha = _sha256(temporary_path)
        final_path = package_path / (
            f"sequoia-x-gpt-database-{market_date}-{archive_sha[:12]}.zip"
        )
        os.replace(temporary_path, final_path)
    finally:
        temporary_path.unlink(missing_ok=True)
        database_temporary_path.unlink(missing_ok=True)

    size_bytes = final_path.stat().st_size
    part_count = max(1, (size_bytes + part_bytes - 1) // part_bytes)
    if part_count > max_parts:
        raise DatabaseEmailError(
            f"compressed database requires {part_count} email parts, "
            f"exceeding configured maximum {max_parts}; use the GitHub Artifact"
        )

    parts: list[DatabasePart] = []
    with final_path.open("rb") as package_file:
        for index in range(part_count):
            offset = index * part_bytes
            content = package_file.read(part_bytes)
            if not content:
                raise DatabaseEmailError("database ZIP ended before all parts were prepared")
            filename = (
                final_path.name
                if part_count == 1
                else f"{final_path.name}.part-{index + 1:03d}-of-{part_count:03d}"
            )
            parts.append(
                DatabasePart(
                    number=index + 1,
                    filename=filename,
                    offset=offset,
                    size_bytes=len(content),
                    sha256=hashlib.sha256(content).hexdigest(),
                )
            )

    package = DatabasePackage(
        path=final_path.resolve(),
        archive_sha256=archive_sha,
        database_sha256=database_sha,
        market_date=market_date,
        size_bytes=size_bytes,
        part_bytes=part_bytes,
        part_count=part_count,
    )
    return package, tuple(parts)


def _part_bytes(package: DatabasePackage, part: DatabasePart) -> bytes:
    with package.path.open("rb") as source:
        source.seek(part.offset)
        content = source.read(part.size_bytes)
    if len(content) != part.size_bytes or hashlib.sha256(content).hexdigest() != part.sha256:
        raise DatabaseEmailError(f"database package part {part.number} changed after preflight")
    return content


def _message_id(
    package: DatabasePackage,
    part: DatabasePart,
    recipient: str,
) -> str:
    material = (
        f"database-email-v1\n{package.market_date}\n{package.archive_sha256}\n"
        f"{part.number}\n{package.part_count}\n{recipient.strip().casefold()}"
    ).encode("ascii")
    digest = hashlib.sha256(material).hexdigest()
    return (
        f"<sequoia-x-db-{package.market_date.replace('-', '')}-"
        f"{digest[:40]}@sequoia-x.local>"
    )


def _build_part_message(
    notifier: EmailNotifier,
    package: DatabasePackage,
    part: DatabasePart,
    recipient: str,
    message_id: str,
    download_url: str | None = None,
) -> EmailMessage:
    content = _part_bytes(package, part)
    message = EmailMessage()
    part_suffix = (
        ""
        if package.part_count == 1
        else f" | {part.number}/{package.part_count}"
    )
    message["Subject"] = (
        f"Sequoia-X 已核验数据库 | {package.market_date} | "
        f"{package.archive_sha256[:12]}{part_suffix}"
    )
    message["From"] = notifier.user
    message["To"] = recipient
    message["Message-ID"] = message_id
    reassembly = (
        "附件本身就是完整 ZIP，可直接解压。"
        if package.part_count == 1
        else (
            "请下载同一批次全部 .part-* 附件，按文件名顺序以二进制方式拼接，"
            "恢复为 ZIP 后再解压。"
        )
    )
    link_text = (
        f"GitHub 已核验数据库下载页面：{download_url}\n"
        if download_url
        else "GitHub 下载链接未配置，请使用本邮件附件。\n"
    )
    plain_body = (
        "Sequoia-X 已核验 GPT 数据库见附件。\n"
        f"市场日期：{package.market_date}\n"
        f"分片：{part.number}/{package.part_count}\n"
        f"数据库 SHA-256：{package.database_sha256}\n"
        f"完整 ZIP SHA-256：{package.archive_sha256}\n"
        f"本分片 SHA-256：{part.sha256}\n"
        f"{reassembly}\n"
        f"{link_text}"
        "ZIP 只包含 sequoia-x-verified.sqlite3、manifest.json 和 README.txt。\n"
    )
    message.set_content(plain_body, charset="utf-8")
    if download_url:
        safe_url = html.escape(download_url, quote=True)
        message.add_alternative(
            "<html><body>"
            "<p>Sequoia-X 已核验 GPT 数据库见附件。</p>"
            f"<p>市场日期：{html.escape(package.market_date)}</p>"
            f'<p><a href="{safe_url}">打开 GitHub 已核验数据库下载页面</a></p>'
            f"<p>完整 ZIP SHA-256：<code>{package.archive_sha256}</code></p>"
            "<p>下载后请先核对 manifest.json 和数据库 SHA-256。</p>"
            "</body></html>",
            subtype="html",
            charset="utf-8",
        )
    message.add_attachment(
        content,
        maintype="application",
        subtype="zip" if package.part_count == 1 else "octet-stream",
        filename=part.filename,
    )
    encoded_size = len(message.as_bytes(policy=SMTP))
    if encoded_size >= MAX_ENCODED_MESSAGE_BYTES:
        raise DatabaseEmailError(
            f"encoded email part {part.number} is {encoded_size} bytes, "
            f"exceeding safety limit {MAX_ENCODED_MESSAGE_BYTES}"
        )
    return message


def send_database_package(
    export_dir: str | Path,
    package_dir: str | Path,
    market_date: str,
    *,
    part_bytes: int = DEFAULT_PART_BYTES,
    max_parts: int = DEFAULT_MAX_PARTS,
    minimum_coverage: float = 0.98,
    download_url: str | None = None,
    notifier: EmailNotifier | None = None,
) -> DatabaseEmailResult:
    """Send all ZIP parts separately to every fixed recipient.

    Every part is MIME-size checked before the first IMAP or SMTP connection.
    Each recipient/part pair has its own Message-ID, so a re-run resumes only
    missing messages after a partial failure.
    """

    package, parts = build_database_package(
        export_dir,
        package_dir,
        market_date,
        part_bytes=part_bytes,
        max_parts=max_parts,
        minimum_coverage=minimum_coverage,
    )
    selected_notifier = notifier or EmailNotifier(report_dir=package_dir)
    if tuple(selected_notifier.recipients) != RECIPIENTS:
        raise DatabaseEmailError("database email recipients do not match the fixed allowlist")
    download_url = _validated_download_url(download_url)

    # Complete all MIME-size checks before opening any network connection.
    longest_recipient = max(selected_notifier.recipients, key=len)
    for part in parts:
        preflight_id = _message_id(package, part, longest_recipient)
        _build_part_message(
            selected_notifier,
            package,
            part,
            longest_recipient,
            preflight_id,
            download_url,
        )

    deliveries: list[DatabasePartDelivery] = []
    failures: list[str] = []
    fatal_delivery_failure = False
    for part in parts:
        for recipient in selected_notifier.recipients:
            message_id = _message_id(package, part, recipient)
            try:
                status, attempts = selected_notifier._deliver_message(
                    message_id=message_id,
                    build_message=lambda part=part, recipient=recipient, message_id=message_id: (
                        _build_part_message(
                            selected_notifier,
                            package,
                            part,
                            recipient,
                            message_id,
                            download_url,
                        )
                    ),
                )
            except Exception as exc:
                failures.append(
                    f"recipient={recipient}, part={part.number}/{package.part_count}, "
                    f"error={type(exc).__name__}: {exc}"
                )
                if isinstance(exc, (SentCheckError, smtplib.SMTPAuthenticationError)):
                    fatal_delivery_failure = True
                    break
                continue
            deliveries.append(
                DatabasePartDelivery(
                    recipient=recipient,
                    part_number=part.number,
                    filename=part.filename,
                    message_id=message_id,
                    status=status,
                    attempts=attempts,
                )
            )
            print(
                f"数据库邮件：recipient={recipient}, "
                f"part={part.number}/{package.part_count}, status={status.value}, "
                f"attempts={attempts}"
            )
        if fatal_delivery_failure:
            break

    if failures:
        raise DatabaseEmailError(
            "database email delivery was incomplete; re-run to resume missing messages: "
            + " | ".join(failures)
        )

    sent = sum(item.status is EmailSendStatus.SENT for item in deliveries)
    skipped = sum(item.status is EmailSendStatus.ALREADY_SENT for item in deliveries)
    if sent and skipped:
        overall = DatabaseEmailStatus.RESUMED
    elif sent:
        overall = DatabaseEmailStatus.SENT
    else:
        overall = DatabaseEmailStatus.ALREADY_SENT
    return DatabaseEmailResult(
        status=overall,
        package=package,
        deliveries=tuple(deliveries),
    )


def _argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="压缩并邮件发送已通过门禁的 Sequoia-X GPT 数据库",
    )
    parser.add_argument(
        "--export-dir",
        default=os.environ.get("GPT_EXPORT_DIR", "gpt-export"),
        help="gpt_export 生成的三文件目录",
    )
    parser.add_argument(
        "--package-dir",
        default=os.environ.get("DATABASE_EMAIL_PACKAGE_DIR", "reports/database-email"),
        help="确定性 ZIP 的输出目录",
    )
    parser.add_argument("--market-date", required=True, help="已核验交易日 YYYY-MM-DD")
    parser.add_argument(
        "--part-bytes",
        type=int,
        default=os.environ.get("DATABASE_EMAIL_PART_BYTES", str(DEFAULT_PART_BYTES)),
        help=f"每封邮件的二进制附件上限，最大 {MAX_PART_BYTES} 字节",
    )
    parser.add_argument(
        "--max-parts",
        type=int,
        default=os.environ.get("DATABASE_EMAIL_MAX_PARTS", str(DEFAULT_MAX_PARTS)),
        help=f"最多分片数，硬上限 {MAX_PARTS}",
    )
    parser.add_argument(
        "--minimum-coverage",
        type=float,
        default=os.environ.get("MIN_DAILY_COVERAGE", "0.98"),
        help="可信的最低当日行情覆盖率门槛",
    )
    parser.add_argument(
        "--download-url",
        default=os.environ.get("DATABASE_DOWNLOAD_URL"),
        help="邮件正文中的已核验数据库 HTTPS 下载页面",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = _argument_parser()
    args = parser.parse_args(argv)
    try:
        result = send_database_package(
            args.export_dir,
            args.package_dir,
            args.market_date,
            part_bytes=args.part_bytes,
            max_parts=args.max_parts,
            minimum_coverage=args.minimum_coverage,
            download_url=args.download_url,
        )
    except (
        DatabaseEmailError,
        EmailConfigurationError,
        OSError,
        sqlite3.Error,
        zipfile.BadZipFile,
    ) as exc:
        parser.exit(1, f"数据库邮件失败：{exc}\n")

    print(
        json.dumps(
            {
                "status": result.status.value,
                "market_date": result.package.market_date,
                "package": str(result.package.path),
                "package_bytes": result.package.size_bytes,
                "package_sha256": result.package.archive_sha256,
                "database_sha256": result.package.database_sha256,
                "parts": result.package.part_count,
                "recipients": len({item.recipient for item in result.deliveries}),
                "sent_messages": result.sent_messages,
                "already_sent_messages": result.already_sent_messages,
                "smtp_attempts": result.smtp_attempts,
            },
            ensure_ascii=False,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
