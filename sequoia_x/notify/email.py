"""Idempotent Gmail delivery for verified stock-selection reports."""

from __future__ import annotations

import hashlib
import imaplib
import os
import re
import smtplib
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from email.message import EmailMessage
from enum import Enum
from pathlib import Path
from typing import Any


RECIPIENTS: tuple[str, str] = (
    "1044757346@qq.com",
    "739152015@qq.com",
)
_SYMBOL_RE = re.compile(r"^[0-9]{6}$")
_SENT_FLAG_RE = re.compile(r"(?:^|[\s(])\\Sent(?:[\s)])", re.IGNORECASE)
_MAILBOX_NAME_RE = re.compile(r'("(?:[^"\\]|\\.)*"|\S+)\s*$')


class EmailConfigurationError(ValueError):
    """Raised when credentials or report output configuration are unusable."""


class SentCheckError(RuntimeError):
    """Raised when Gmail SENT cannot be checked safely."""


class EmailSendStatus(str, Enum):
    SENT = "sent"
    ALREADY_SENT = "already_sent"
    EMPTY = "empty"


@dataclass(frozen=True)
class EmailSendResult:
    """Outcome and evidence for one aggregate report delivery attempt."""

    status: EmailSendStatus
    message_id: str | None
    attempts: int
    symbols: tuple[str, ...]
    report_path: Path

    @property
    def symbol_count(self) -> int:
        return len(self.symbols)


class EmailNotifier:
    """Create and send one aggregate TXT report after a SENT preflight check.

    The injectable client factories keep network access outside report creation
    and make the delivery rules testable without contacting Gmail.
    """

    smtp_host = "smtp.gmail.com"
    smtp_port = 465
    imap_host = "imap.gmail.com"
    imap_port = 993

    def __init__(
        self,
        settings: Any = None,
        *,
        user: str | None = None,
        password: str | None = None,
        report_dir: str | Path | None = None,
        smtp_factory: Callable[..., Any] = smtplib.SMTP_SSL,
        imap_factory: Callable[..., Any] = imaplib.IMAP4_SSL,
        timeout: float = 10.0,
    ) -> None:
        self.user = self._credential(
            explicit=user,
            configured=getattr(settings, "gmail_user", None),
            environment_key="GMAIL_USER",
        )
        self.password = self._credential(
            explicit=password,
            configured=getattr(settings, "gmail_app_password", None),
            environment_key="GMAIL_APP_PASSWORD",
        )
        configured_report_dir = getattr(settings, "email_report_dir", None)
        selected_report_dir = report_dir if report_dir is not None else configured_report_dir
        self.report_dir = Path(selected_report_dir or "reports")
        self._smtp_factory = smtp_factory
        self._imap_factory = imap_factory
        self._timeout = timeout

    @staticmethod
    def _credential(
        *,
        explicit: Any,
        configured: Any,
        environment_key: str,
    ) -> str:
        value = explicit if explicit is not None else configured
        if value is None:
            value = os.environ.get(environment_key)
        if hasattr(value, "get_secret_value"):
            value = value.get_secret_value()
        if not isinstance(value, str) or not value.strip():
            raise EmailConfigurationError(f"missing required email credential: {environment_key}")
        return value

    def send_report(
        self,
        strategy_results: dict[str, list[str]],
        verified_symbols: Iterable[str],
        market_date: str,
    ) -> EmailSendResult:
        """Send one report containing only verified, globally deduplicated symbols.

        Gmail SENT is checked before any SMTP connection. A transient SMTP
        failure permits one retry, but only after SENT is checked again. If an
        IMAP check fails, :class:`SentCheckError` is raised and no SMTP attempt
        follows that failed check.
        """

        normalized_date = self._validate_market_date(market_date)
        verified = self._verified_set(verified_symbols)
        report_text, report_symbols = self._build_report(
            strategy_results,
            verified,
            normalized_date,
        )
        report_path = self._write_report(normalized_date, report_text)

        if not report_symbols:
            return EmailSendResult(
                status=EmailSendStatus.EMPTY,
                message_id=None,
                attempts=0,
                symbols=(),
                report_path=report_path,
            )

        message_id = self._message_id(normalized_date, report_text)
        if self._is_in_sent(message_id):
            return EmailSendResult(
                status=EmailSendStatus.ALREADY_SENT,
                message_id=message_id,
                attempts=0,
                symbols=report_symbols,
                report_path=report_path,
            )

        message = self._build_message(
            report_text=report_text,
            report_symbols=report_symbols,
            market_date=normalized_date,
            message_id=message_id,
        )

        attempts = 0
        while attempts < 2:
            attempts += 1
            try:
                self._send_smtp(message)
                return EmailSendResult(
                    status=EmailSendStatus.SENT,
                    message_id=message_id,
                    attempts=attempts,
                    symbols=report_symbols,
                    report_path=report_path,
                )
            except Exception as exc:
                if attempts >= 2 or not self._is_temporary_smtp_error(exc):
                    raise

                # SMTP may have accepted the message before the client saw an
                # error. Re-check SENT before the sole permitted retry.
                if self._is_in_sent(message_id):
                    return EmailSendResult(
                        status=EmailSendStatus.ALREADY_SENT,
                        message_id=message_id,
                        attempts=attempts,
                        symbols=report_symbols,
                        report_path=report_path,
                    )

        raise AssertionError("unreachable")

    @staticmethod
    def _validate_market_date(market_date: str) -> str:
        from datetime import date

        if not isinstance(market_date, str):
            raise TypeError("market_date must be an ISO date string")
        try:
            parsed = date.fromisoformat(market_date)
        except ValueError as exc:
            raise ValueError("market_date must use YYYY-MM-DD format") from exc
        if parsed.isoformat() != market_date:
            raise ValueError("market_date must use YYYY-MM-DD format")
        return market_date

    @staticmethod
    def _verified_set(verified_symbols: Iterable[str]) -> frozenset[str]:
        verified: set[str] = set()
        for symbol in verified_symbols:
            if not isinstance(symbol, str) or not _SYMBOL_RE.fullmatch(symbol):
                raise ValueError(f"invalid verified stock symbol: {symbol!r}")
            verified.add(symbol)
        return frozenset(verified)

    @staticmethod
    def _build_report(
        strategy_results: Mapping[str, list[str]],
        verified: frozenset[str],
        market_date: str,
    ) -> tuple[str, tuple[str, ...]]:
        symbol_strategies: dict[str, set[str]] = {}

        for strategy_name, symbols in strategy_results.items():
            if not isinstance(strategy_name, str) or not strategy_name.strip():
                raise ValueError("strategy names must be non-empty strings")
            for symbol in symbols:
                if symbol in verified:
                    symbol_strategies.setdefault(symbol, set()).add(strategy_name.strip())

        ordered_symbols = tuple(sorted(symbol_strategies))
        lines = [
            "Sequoia-X 已核验选股报告",
            f"市场日期：{market_date}",
            f"股票数量：{len(ordered_symbols)}",
            "",
        ]
        for symbol in ordered_symbols:
            strategies = ", ".join(sorted(symbol_strategies[symbol]))
            lines.append(f"{symbol}\t{strategies}")

        return "\n".join(lines) + "\n", ordered_symbols

    def _write_report(self, market_date: str, report_text: str) -> Path:
        self.report_dir.mkdir(parents=True, exist_ok=True)
        report_path = self.report_dir / f"sequoia-x-{market_date}.txt"
        temporary_path = report_path.with_suffix(".txt.tmp")
        temporary_path.write_bytes(report_text.encode("utf-8"))
        temporary_path.replace(report_path)
        return report_path.resolve()

    @staticmethod
    def _message_id(market_date: str, report_text: str) -> str:
        material = f"{market_date}\n{report_text}".encode("utf-8")
        digest = hashlib.sha256(material).hexdigest()
        return f"<sequoia-x-{market_date.replace('-', '')}-{digest[:32]}@sequoia-x.local>"

    def _build_message(
        self,
        *,
        report_text: str,
        report_symbols: tuple[str, ...],
        market_date: str,
        message_id: str,
    ) -> EmailMessage:
        message = EmailMessage()
        message["Subject"] = f"Sequoia-X | {market_date} | {len(report_symbols)}只"
        message["From"] = self.user
        message["To"] = ", ".join(RECIPIENTS)
        message["Message-ID"] = message_id
        message.set_content(
            "Sequoia-X 已核验选股结果见附件。\n"
            f"市场日期：{market_date}\n"
            f"股票数量：{len(report_symbols)}\n",
            charset="utf-8",
        )
        message.add_attachment(
            report_text,
            subtype="plain",
            charset="utf-8",
            filename=f"sequoia-x-{market_date}.txt",
        )
        return message

    def _is_in_sent(self, message_id: str) -> bool:
        client = None
        try:
            client = self._imap_factory(
                self.imap_host,
                self.imap_port,
                timeout=self._timeout,
            )
            status, _ = client.login(self.user, self.password)
            if status != "OK":
                raise SentCheckError("Gmail IMAP login failed")

            status, mailboxes = client.list()
            if status != "OK" or not mailboxes:
                raise SentCheckError("Gmail mailbox discovery failed")
            sent_mailbox = self._find_sent_mailbox(mailboxes)

            status, _ = client.select(sent_mailbox, readonly=True)
            if status != "OK":
                raise SentCheckError("Gmail SENT mailbox is unavailable")

            status, data = client.uid(
                "SEARCH",
                None,
                "HEADER",
                "Message-ID",
                f'"{message_id}"',
            )
            if status != "OK" or not data:
                raise SentCheckError("Gmail SENT search failed")
            if any(not isinstance(part, bytes) for part in data):
                raise SentCheckError("Gmail SENT returned an invalid search result")

            return any(part.strip() for part in data)
        except SentCheckError:
            raise
        except Exception as exc:
            raise SentCheckError("unable to verify Gmail SENT") from exc
        finally:
            if client is not None:
                try:
                    client.logout()
                except Exception:
                    pass

    @staticmethod
    def _find_sent_mailbox(mailboxes: Iterable[bytes | str | None]) -> str:
        for raw_line in mailboxes:
            if raw_line is None:
                continue
            if isinstance(raw_line, bytes):
                try:
                    line = raw_line.decode("ascii")
                except UnicodeDecodeError as exc:
                    raise SentCheckError("invalid Gmail mailbox listing") from exc
            elif isinstance(raw_line, str):
                line = raw_line
            else:
                raise SentCheckError("invalid Gmail mailbox listing")

            if not _SENT_FLAG_RE.search(line):
                continue
            match = _MAILBOX_NAME_RE.search(line)
            if not match:
                raise SentCheckError("invalid Gmail SENT mailbox entry")
            return match.group(1)

        raise SentCheckError("Gmail SENT mailbox was not advertised")

    def _send_smtp(self, message: EmailMessage) -> None:
        with self._smtp_factory(
            self.smtp_host,
            self.smtp_port,
            timeout=self._timeout,
        ) as smtp:
            smtp.login(self.user, self.password)
            refused = smtp.send_message(message)
            if refused:
                raise smtplib.SMTPRecipientsRefused(refused)

    @staticmethod
    def _is_temporary_smtp_error(exc: Exception) -> bool:
        if isinstance(exc, smtplib.SMTPAuthenticationError):
            return False
        if isinstance(exc, smtplib.SMTPRecipientsRefused):
            return False
        if isinstance(exc, smtplib.SMTPResponseException):
            return 400 <= exc.smtp_code < 500
        return isinstance(
            exc,
            (
                smtplib.SMTPServerDisconnected,
                TimeoutError,
                ConnectionError,
                OSError,
            ),
        )
