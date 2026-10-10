"""Offline tests for verified database ZIP email delivery."""

from __future__ import annotations

import hashlib
import json
import smtplib
import sqlite3
import zipfile
from collections.abc import Iterable
from email.message import EmailMessage
from email.policy import SMTP
from email.utils import getaddresses
from pathlib import Path

import pytest

from sequoia_x.notify.database_email import (
    MAX_ENCODED_MESSAGE_BYTES,
    MAX_PART_BYTES,
    DatabaseEmailError,
    DatabaseEmailStatus,
    DatabasePackage,
    DatabasePart,
    _build_part_message,
    build_database_package,
    send_database_package,
)
from sequoia_x.data.gpt_export import _README_TEXT
from sequoia_x.notify.email import RECIPIENTS, EmailNotifier, SentCheckError


class FakeImapFactory:
    def __init__(self, outcomes: Iterable[bool | BaseException]) -> None:
        self.outcomes = list(outcomes)
        self.calls = 0
        self.message_ids: list[str] = []

    def __call__(self, host: str, port: int, **kwargs: object) -> "FakeImap":
        outcome = self.outcomes[self.calls]
        self.calls += 1
        return FakeImap(self, outcome)


class FakeImap:
    def __init__(self, factory: FakeImapFactory, outcome: bool | BaseException) -> None:
        self.factory = factory
        self.outcome = outcome

    def login(self, user: str, password: str) -> tuple[str, list[bytes]]:
        return "OK", [b"ok"]

    def list(self) -> tuple[str, list[bytes]]:
        return "OK", [b'(\\Sent) "/" "[Gmail]/Sent Mail"']

    def select(self, mailbox: str, readonly: bool) -> tuple[str, list[bytes]]:
        return "OK", [b"0"]

    def uid(self, *args: object) -> tuple[str, list[bytes]]:
        self.factory.message_ids.append(str(args[-1]))
        if isinstance(self.outcome, BaseException):
            raise self.outcome
        return "OK", [b"42" if self.outcome else b""]

    def logout(self) -> tuple[str, list[bytes]]:
        return "BYE", [b"bye"]


class FakeSmtpFactory:
    def __init__(self, outcomes: Iterable[BaseException | None]) -> None:
        self.outcomes = list(outcomes)
        self.calls = 0
        self.messages: list[EmailMessage] = []

    def __call__(self, host: str, port: int, **kwargs: object) -> "FakeSmtp":
        outcome = self.outcomes[self.calls]
        self.calls += 1
        return FakeSmtp(self, outcome)


class FakeSmtp:
    def __init__(self, factory: FakeSmtpFactory, outcome: BaseException | None) -> None:
        self.factory = factory
        self.outcome = outcome

    def __enter__(self) -> "FakeSmtp":
        return self

    def __exit__(self, *args: object) -> None:
        return None

    def login(self, user: str, password: str) -> tuple[int, bytes]:
        return 235, b"ok"

    def send_message(self, message: EmailMessage) -> dict[str, object]:
        self.factory.messages.append(message)
        if self.outcome is not None:
            raise self.outcome
        return {}


def _notifier(
    tmp_path: Path,
    imap: FakeImapFactory,
    smtp: FakeSmtpFactory,
) -> EmailNotifier:
    return EmailNotifier(
        user="sender@gmail.com",
        password="app-password",
        report_dir=tmp_path,
        imap_factory=imap,
        smtp_factory=smtp,
    )


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _valid_export(path: Path) -> None:
    path.mkdir()
    database = path / "sequoia-x-verified.sqlite3"
    with sqlite3.connect(database) as connection:
        connection.executescript(
            """
            PRAGMA foreign_keys = ON;
            CREATE TABLE stock_daily (
                symbol TEXT NOT NULL,
                date TEXT NOT NULL,
                open REAL,
                high REAL,
                low REAL,
                close REAL,
                volume REAL,
                turnover REAL,
                PRIMARY KEY(symbol, date)
            );
            CREATE INDEX idx_stock_daily_date ON stock_daily (date, symbol);
            CREATE TABLE selection_runs (
                market_date TEXT PRIMARY KEY,
                recorded_at TEXT NOT NULL,
                expected_symbols INTEGER NOT NULL,
                verified_symbols INTEGER NOT NULL,
                verified_symbols_json TEXT NOT NULL,
                coverage REAL NOT NULL,
                strategies_json TEXT NOT NULL,
                result_rows INTEGER NOT NULL
            );
            CREATE TABLE selection_results (
                market_date TEXT NOT NULL,
                strategy TEXT NOT NULL,
                symbol TEXT NOT NULL,
                PRIMARY KEY (market_date, strategy, symbol),
                FOREIGN KEY (market_date) REFERENCES selection_runs (market_date)
                    ON DELETE CASCADE
            );
            CREATE INDEX idx_selection_results_symbol_date
            ON selection_results (symbol, market_date);
            INSERT INTO stock_daily
                (symbol, date, open, high, low, close, volume, turnover)
            VALUES ('000001', '2026-10-09', 10, 12, 9, 11, 100, 1100);
            INSERT INTO selection_runs VALUES
                ('2026-10-09', '2026-10-09T12:00:00Z', 1, 1,
                 '["000001"]', 1.0, '["Strategy"]', 1);
            INSERT INTO selection_results VALUES
                ('2026-10-09', 'Strategy', '000001');
            """
        )
    manifest = {
        "schema_version": 1,
        "created_at_utc": "2026-10-09T12:00:00Z",
        "database_file": database.name,
        "database_sha256": _sha256(database),
        "included_tables": ["stock_daily", "selection_runs", "selection_results"],
        "stock_daily_columns": [
            "symbol", "date", "open", "high", "low", "close", "volume", "turnover"
        ],
        "row_count": 1,
        "symbol_count": 1,
        "first_market_date": "2026-10-09",
        "latest_market_date": "2026-10-09",
        "integrity": {
            "quick_check": "ok",
            "source_quick_check": "ok",
            "export_quick_check": "ok",
            "invalid_rows": 0,
            "copied_row_count": 1,
        },
        "required_market_date": "2026-10-09",
        "verification": {
            "status": "complete",
            "market_date": "2026-10-09",
            "recorded_at": "2026-10-09T12:00:00Z",
            "expected_symbol_count": 1,
            "verified_symbol_count": 1,
            "coverage": 1.0,
            "minimum_coverage": 0.98,
            "result_count": 1,
            "verified_symbols_sha256": hashlib.sha256(b"000001").hexdigest(),
        },
        "content_policy": {
            "published_market_data_only": True,
            "staging_excluded": True,
            "orders_funds_positions_nav_excluded": True,
            "unknown_source_tables_excluded": True,
        },
        "scope": {
            "all_published_source_rows_copied": True,
            "historical_full_market_coverage_proven": False,
            "selection_history_starts_when_feature_was_enabled": True,
        },
        "selection_history": {
            "available": True,
            "run_count": 1,
            "result_count": 1,
            "first_market_date": "2026-10-09",
            "latest_market_date": "2026-10-09",
        },
    }
    (path / "manifest.json").write_text(
        json.dumps(manifest, ensure_ascii=False, sort_keys=True),
        encoding="utf-8",
    )
    (path / "README.txt").write_bytes(_README_TEXT.encode("utf-8"))
    (path / "do-not-package.secret").write_text("secret", encoding="utf-8")


def test_builds_deterministic_zip_with_exact_file_whitelist(tmp_path: Path) -> None:
    export = tmp_path / "export"
    _valid_export(export)

    first, parts = build_database_package(
        export, tmp_path / "packages", "2026-10-09"
    )
    second, second_parts = build_database_package(
        export, tmp_path / "packages", "2026-10-09"
    )

    assert first.archive_sha256 == second.archive_sha256
    assert first.database_sha256 == _sha256(export / "sequoia-x-verified.sqlite3")
    assert parts == second_parts
    with zipfile.ZipFile(first.path) as archive:
        assert archive.namelist() == [
            "sequoia-x-verified.sqlite3",
            "manifest.json",
            "README.txt",
        ]
        assert archive.testzip() is None
        assert "do-not-package.secret" not in archive.namelist()


def test_new_export_timestamp_keeps_the_same_deterministic_package(tmp_path: Path) -> None:
    export = tmp_path / "export"
    _valid_export(export)
    first, _ = build_database_package(export, tmp_path / "first", "2026-10-09")
    manifest_path = export / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["created_at_utc"] = "2026-10-10T23:59:59Z"
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8", newline="\n")

    second, _ = build_database_package(export, tmp_path / "second", "2026-10-09")

    assert first.archive_sha256 == second.archive_sha256


def test_sends_one_validated_zip_separately_to_all_fixed_recipients(
    tmp_path: Path,
) -> None:
    export = tmp_path / "export"
    _valid_export(export)
    imap = FakeImapFactory([False] * len(RECIPIENTS))
    smtp = FakeSmtpFactory([None] * len(RECIPIENTS))

    result = send_database_package(
        export,
        tmp_path / "packages",
        "2026-10-09",
        download_url="https://github.com/example/sequoia/actions/runs/123",
        notifier=_notifier(tmp_path / "reports", imap, smtp),
    )

    assert result.status is DatabaseEmailStatus.SENT
    assert result.package.part_count == 1
    assert result.sent_messages == len(RECIPIENTS)
    assert result.already_sent_messages == 0
    assert smtp.calls == len(RECIPIENTS)
    recipients = [
        getaddresses(message.get_all("To", []))[0][1] for message in smtp.messages
    ]
    assert tuple(recipients) == RECIPIENTS
    assert len({message["Message-ID"] for message in smtp.messages}) == len(RECIPIENTS)
    for message in smtp.messages:
        plain = message.get_body(preferencelist=("plain",))
        assert plain is not None
        assert "https://github.com/example/sequoia/actions/runs/123" in plain.get_content()
        assert any(part.get_content_type() == "text/html" for part in message.walk())
        assert len(message.as_bytes(policy=SMTP)) < MAX_ENCODED_MESSAGE_BYTES
        attachments = list(message.iter_attachments())
        assert len(attachments) == 1
        assert hashlib.sha256(attachments[0].get_payload(decode=True)).hexdigest() == (
            result.package.archive_sha256
        )


def test_multipart_messages_reassemble_to_the_valid_zip(tmp_path: Path) -> None:
    export = tmp_path / "export"
    _valid_export(export)
    preview, _ = build_database_package(
        export,
        tmp_path / "preview",
        "2026-10-09",
        part_bytes=1024,
        max_parts=50,
    )
    assert preview.part_count > 1
    message_count = preview.part_count * len(RECIPIENTS)
    imap = FakeImapFactory([False] * message_count)
    smtp = FakeSmtpFactory([None] * message_count)

    result = send_database_package(
        export,
        tmp_path / "packages",
        "2026-10-09",
        part_bytes=1024,
        max_parts=50,
        notifier=_notifier(tmp_path / "reports", imap, smtp),
    )

    first_recipient_messages = [
        message
        for message in smtp.messages
        if getaddresses(message.get_all("To", []))[0][1] == RECIPIENTS[0]
    ]
    reconstructed = b"".join(
        list(message.iter_attachments())[0].get_payload(decode=True)
        for message in first_recipient_messages
    )
    assert hashlib.sha256(reconstructed).hexdigest() == result.package.archive_sha256
    reconstructed_path = tmp_path / "reconstructed.zip"
    reconstructed_path.write_bytes(reconstructed)
    with zipfile.ZipFile(reconstructed_path) as archive:
        assert archive.testzip() is None


def test_sent_parts_are_skipped_and_missing_recipients_are_sent(tmp_path: Path) -> None:
    export = tmp_path / "export"
    _valid_export(export)
    imap = FakeImapFactory([True, False, False])
    smtp = FakeSmtpFactory([None, None])

    result = send_database_package(
        export,
        tmp_path / "packages",
        "2026-10-09",
        notifier=_notifier(tmp_path / "reports", imap, smtp),
    )

    assert result.status is DatabaseEmailStatus.RESUMED
    assert result.already_sent_messages == 1
    assert result.sent_messages == 2
    assert smtp.calls == 2


def test_complete_database_is_sent_even_when_no_strategy_selected(tmp_path: Path) -> None:
    export = tmp_path / "export"
    _valid_export(export)
    database = export / "sequoia-x-verified.sqlite3"
    with sqlite3.connect(database) as connection:
        connection.execute("DELETE FROM selection_results")
        connection.execute(
            "UPDATE selection_runs SET result_rows = 0 "
            "WHERE market_date = '2026-10-09'"
        )
    manifest_path = export / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["database_sha256"] = _sha256(database)
    manifest["verification"]["result_count"] = 0
    manifest["selection_history"]["result_count"] = 0
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    imap = FakeImapFactory([False] * len(RECIPIENTS))
    smtp = FakeSmtpFactory([None] * len(RECIPIENTS))

    result = send_database_package(
        export,
        tmp_path / "packages",
        "2026-10-09",
        notifier=_notifier(tmp_path / "reports", imap, smtp),
    )

    assert result.status is DatabaseEmailStatus.SENT
    assert result.sent_messages == len(RECIPIENTS)
    assert smtp.calls == len(RECIPIENTS)


def test_transient_failure_rechecks_sent_and_retries_only_once(tmp_path: Path) -> None:
    export = tmp_path / "export"
    _valid_export(export)
    imap = FakeImapFactory([False, False, False, False])
    smtp = FakeSmtpFactory(
        [smtplib.SMTPServerDisconnected("lost"), None, None, None]
    )

    result = send_database_package(
        export,
        tmp_path / "packages",
        "2026-10-09",
        notifier=_notifier(tmp_path / "reports", imap, smtp),
    )

    assert result.smtp_attempts == 4
    assert imap.calls == 4
    assert smtp.calls == 4


def test_maximum_part_stays_below_encoded_message_safety_limit(tmp_path: Path) -> None:
    package_path = tmp_path / "maximum.zip"
    package_path.write_bytes(b"x" * MAX_PART_BYTES)
    part_sha = _sha256(package_path)
    package = DatabasePackage(
        path=package_path,
        archive_sha256=part_sha,
        database_sha256="0" * 64,
        market_date="2026-10-09",
        size_bytes=MAX_PART_BYTES,
        part_bytes=MAX_PART_BYTES,
        part_count=1,
    )
    part = DatabasePart(
        number=1,
        filename="maximum.zip",
        offset=0,
        size_bytes=MAX_PART_BYTES,
        sha256=part_sha,
    )
    notifier = _notifier(
        tmp_path / "reports", FakeImapFactory([]), FakeSmtpFactory([])
    )

    message = _build_part_message(
        notifier,
        package,
        part,
        RECIPIENTS[0],
        "<size-check@sequoia-x.local>",
    )

    assert len(message.as_bytes(policy=SMTP)) < MAX_ENCODED_MESSAGE_BYTES


@pytest.mark.parametrize(
    "corruption",
    [
        "sha",
        "status",
        "date",
        "database",
        "manifest_table",
        "actual_table",
        "missing_same_day",
        "selection_outside_universe",
        "coverage",
        "unicode_universe",
        "manifest_extra",
        "readme",
        "sidecar",
        "extra_column",
        "low_trusted_gate",
    ],
)
def test_invalid_export_fails_before_any_network(tmp_path: Path, corruption: str) -> None:
    export = tmp_path / "export"
    _valid_export(export)
    manifest_path = export / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if corruption == "sha":
        manifest["database_sha256"] = "0" * 64
    elif corruption == "status":
        manifest["verification"]["status"] = "incomplete"
    elif corruption == "date":
        manifest["required_market_date"] = "2026-10-08"
    elif corruption == "database":
        (export / "sequoia-x-verified.sqlite3").write_bytes(b"broken")
    elif corruption == "manifest_table":
        manifest["included_tables"] = ["stock_daily", "orders"]
    elif corruption == "actual_table":
        with sqlite3.connect(export / "sequoia-x-verified.sqlite3") as connection:
            connection.execute("CREATE TABLE orders (secret TEXT)")
        manifest["database_sha256"] = _sha256(
            export / "sequoia-x-verified.sqlite3"
        )
    elif corruption == "manifest_extra":
        manifest["secret"] = "must not be packaged"
    elif corruption == "readme":
        (export / "README.txt").write_text("secret", encoding="utf-8")
    elif corruption == "sidecar":
        Path(f"{export / 'sequoia-x-verified.sqlite3'}-wal").write_bytes(b"unsafe")
    elif corruption == "extra_column":
        with sqlite3.connect(export / "sequoia-x-verified.sqlite3") as connection:
            connection.execute("ALTER TABLE stock_daily ADD COLUMN secret TEXT")
        manifest["database_sha256"] = _sha256(
            export / "sequoia-x-verified.sqlite3"
        )
    elif corruption == "low_trusted_gate":
        manifest["verification"]["minimum_coverage"] = 0.5
    else:
        with sqlite3.connect(export / "sequoia-x-verified.sqlite3") as connection:
            if corruption == "missing_same_day":
                connection.execute(
                    "UPDATE stock_daily SET symbol = '600000' WHERE symbol = '000001'"
                )
            elif corruption == "selection_outside_universe":
                connection.execute(
                    "UPDATE selection_results SET symbol = '600000' "
                    "WHERE symbol = '000001'"
                )
            elif corruption == "coverage":
                connection.execute(
                    "UPDATE selection_runs SET coverage = 0.5 "
                    "WHERE market_date = '2026-10-09'"
                )
                manifest["verification"]["coverage"] = 0.5
            else:
                connection.execute(
                    "UPDATE selection_runs SET verified_symbols_json = ? "
                    "WHERE market_date = '2026-10-09'",
                    ('["１２３４５６"]',),
                )
        manifest["database_sha256"] = _sha256(
            export / "sequoia-x-verified.sqlite3"
        )
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    imap = FakeImapFactory([])
    smtp = FakeSmtpFactory([])

    with pytest.raises(DatabaseEmailError):
        send_database_package(
            export,
            tmp_path / "packages",
            "2026-10-09",
            notifier=_notifier(tmp_path / "reports", imap, smtp),
        )

    assert imap.calls == 0
    assert smtp.calls == 0


def test_rejects_non_allowlisted_recipient_and_unsafe_url_before_network(
    tmp_path: Path,
) -> None:
    export = tmp_path / "export"
    _valid_export(export)
    imap = FakeImapFactory([])
    smtp = FakeSmtpFactory([])
    notifier = _notifier(tmp_path / "reports", imap, smtp)
    notifier.recipients = ("attacker@example.com",)

    with pytest.raises(DatabaseEmailError, match="fixed allowlist"):
        send_database_package(
            export,
            tmp_path / "packages",
            "2026-10-09",
            notifier=notifier,
        )

    notifier.recipients = RECIPIENTS
    with pytest.raises(DatabaseEmailError, match="safe HTTPS URL"):
        send_database_package(
            export,
            tmp_path / "packages-2",
            "2026-10-09",
            download_url="http://example.com/database.zip",
            notifier=notifier,
        )
    assert imap.calls == 0
    assert smtp.calls == 0


def test_too_many_parts_fails_before_any_network(tmp_path: Path) -> None:
    export = tmp_path / "export"
    _valid_export(export)
    imap = FakeImapFactory([])
    smtp = FakeSmtpFactory([])

    with pytest.raises(DatabaseEmailError, match="use the GitHub Artifact"):
        send_database_package(
            export,
            tmp_path / "packages",
            "2026-10-09",
            part_bytes=100,
            max_parts=1,
            notifier=_notifier(tmp_path / "reports", imap, smtp),
        )

    assert imap.calls == 0
    assert smtp.calls == 0


def test_sent_lookup_failure_sends_nothing_to_that_recipient(tmp_path: Path) -> None:
    export = tmp_path / "export"
    _valid_export(export)
    imap = FakeImapFactory([OSError("offline"), False, False])
    smtp = FakeSmtpFactory([None, None])

    with pytest.raises(DatabaseEmailError, match="delivery was incomplete"):
        send_database_package(
            export,
            tmp_path / "packages",
            "2026-10-09",
            notifier=_notifier(tmp_path / "reports", imap, smtp),
        )

    assert imap.calls == 1
    assert smtp.calls == 0
