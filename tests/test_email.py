"""Offline tests for verified TXT report delivery."""

from __future__ import annotations

import smtplib
from collections.abc import Iterable
from email.message import EmailMessage
from email.utils import getaddresses
from types import SimpleNamespace

import pytest

from sequoia_x.notify.email import (
    RECIPIENTS,
    EmailConfigurationError,
    EmailNotifier,
    EmailSendStatus,
    SentCheckError,
)


class FakeImapFactory:
    def __init__(
        self,
        outcomes: Iterable[bool | BaseException | str],
        *,
        mailboxes: list[bytes] | None = None,
    ) -> None:
        self.outcomes = list(outcomes)
        self.calls = 0
        self.searches: list[tuple[object, ...]] = []
        self.selected_mailboxes: list[str] = []
        self.mailboxes = mailboxes or [
            b'(\\HasNoChildren) "/" "INBOX"',
            b'(\\Sent) "/" "[Google Mail]/Gesendet"',
        ]

    def __call__(self, host: str, port: int, **kwargs: object) -> "FakeImap":
        outcome = self.outcomes[self.calls]
        self.calls += 1
        return FakeImap(self, outcome)


class FakeImap:
    def __init__(
        self,
        factory: FakeImapFactory,
        outcome: bool | BaseException | str,
    ) -> None:
        self.factory = factory
        self.outcome = outcome

    def login(self, user: str, password: str) -> tuple[str, list[bytes]]:
        return "OK", [b"logged in"]

    def list(self) -> tuple[str, list[bytes]]:
        return "OK", self.factory.mailboxes

    def select(self, mailbox: str, readonly: bool) -> tuple[str, list[bytes]]:
        self.factory.selected_mailboxes.append(mailbox)
        return "OK", [b"0"]

    def uid(self, *args: object) -> tuple[str, list[bytes]]:
        self.factory.searches.append(args)
        if isinstance(self.outcome, BaseException):
            raise self.outcome
        if self.outcome == "search-error":
            return "NO", [b"search failed"]
        return "OK", [b"42" if self.outcome else b""]

    def logout(self) -> tuple[str, list[bytes]]:
        return "BYE", [b"logged out"]


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


def make_notifier(tmp_path, imap: FakeImapFactory, smtp: FakeSmtpFactory) -> EmailNotifier:
    return EmailNotifier(
        user="sender@gmail.com",
        password="app-password",
        report_dir=tmp_path,
        imap_factory=imap,
        smtp_factory=smtp,
    )


def test_sends_utf8_txt_with_only_verified_globally_deduplicated_symbols(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("GMAIL_TO", "attacker@example.com")
    imap = FakeImapFactory([False])
    smtp = FakeSmtpFactory([None])
    notifier = make_notifier(tmp_path, imap, smtp)

    result = notifier.send_report(
        {
            "Turtle": ["600519", "000001", "000001", "300001"],
            "Momentum": ["000001", "999999", "not-a-symbol"],
        },
        verified_symbols=["000001", "600519", "600519"],
        market_date="2026-10-08",
    )

    assert result.status is EmailSendStatus.SENT
    assert result.attempts == 1
    assert result.symbols == ("000001", "600519")
    assert result.symbol_count == 2
    assert result.report_path.is_absolute()

    report_bytes = result.report_path.read_bytes()
    report_text = report_bytes.decode("utf-8")
    assert report_text.count("000001") == 1
    assert report_text.count("600519") == 1
    assert "300001" not in report_text
    assert "999999" not in report_text
    assert "not-a-symbol" not in report_text

    message = smtp.messages[0]
    recipients = {address for _, address in getaddresses(message.get_all("To", []))}
    assert recipients == set(RECIPIENTS)
    assert notifier.recipients == RECIPIENTS
    assert message["Message-ID"] == result.message_id
    assert imap.selected_mailboxes == ['"[Google Mail]/Gesendet"']

    attachments = list(message.iter_attachments())
    assert len(attachments) == 1
    assert attachments[0].get_filename() == "sequoia-x-2026-10-08.txt"
    assert attachments[0].get_content_charset() == "utf-8"
    assert attachments[0].get_payload(decode=True) == report_bytes


def test_message_id_is_deterministic_for_same_logical_report(tmp_path) -> None:
    first = make_notifier(
        tmp_path / "first",
        FakeImapFactory([True]),
        FakeSmtpFactory([]),
    ).send_report(
        {"Beta": ["600519", "000001"], "Alpha": ["000001"]},
        verified_symbols={"600519", "000001"},
        market_date="2026-10-08",
    )
    second = make_notifier(
        tmp_path / "second",
        FakeImapFactory([True]),
        FakeSmtpFactory([]),
    ).send_report(
        {"Alpha": ["000001"], "Beta": ["000001", "600519"]},
        verified_symbols=["000001", "600519"],
        market_date="2026-10-08",
    )

    assert first.message_id == second.message_id
    assert first.report_path.read_bytes() == second.report_path.read_bytes()


def test_message_id_changes_when_recipient_set_changes() -> None:
    report_text = "verified report\n"
    first = EmailNotifier._message_id("2026-10-08", report_text, RECIPIENTS)
    second = EmailNotifier._message_id(
        "2026-10-08",
        report_text,
        (*RECIPIENTS, "another@gmail.com"),
    )

    assert first != second


def test_sender_is_not_implicitly_added_to_fixed_recipients(tmp_path) -> None:
    imap = FakeImapFactory([False])
    smtp = FakeSmtpFactory([None])
    notifier = EmailNotifier(
        user="another-sender@gmail.com",
        password="app-password",
        report_dir=tmp_path,
        imap_factory=imap,
        smtp_factory=smtp,
    )

    result = notifier.send_report(
        {"Strategy": ["000001"]},
        verified_symbols=["000001"],
        market_date="2026-10-08",
    )

    assert result.status is EmailSendStatus.SENT
    assert notifier.recipients == RECIPIENTS
    message_recipients = {
        address.casefold()
        for _, address in getaddresses(smtp.messages[0].get_all("To", []))
    }
    assert message_recipients == {recipient.casefold() for recipient in RECIPIENTS}


def test_existing_message_in_sent_skips_smtp(tmp_path) -> None:
    imap = FakeImapFactory([True])
    smtp = FakeSmtpFactory([])

    result = make_notifier(tmp_path, imap, smtp).send_report(
        {"Strategy": ["000001"]},
        verified_symbols=["000001"],
        market_date="2026-10-08",
    )

    assert result.status is EmailSendStatus.ALREADY_SENT
    assert result.attempts == 0
    assert smtp.calls == 0
    assert "Message-ID" in imap.searches[0]


@pytest.mark.parametrize("imap_outcome", [OSError("offline"), "search-error"])
def test_sent_check_failure_is_fail_closed(tmp_path, imap_outcome) -> None:
    imap = FakeImapFactory([imap_outcome])
    smtp = FakeSmtpFactory([])

    with pytest.raises(SentCheckError):
        make_notifier(tmp_path, imap, smtp).send_report(
            {"Strategy": ["000001"]},
            verified_symbols=["000001"],
            market_date="2026-10-08",
        )

    assert smtp.calls == 0


def test_missing_sent_special_use_mailbox_is_fail_closed(tmp_path) -> None:
    imap = FakeImapFactory(
        [False],
        mailboxes=[b'(\\HasNoChildren) "/" "[Gmail]/All Mail"'],
    )
    smtp = FakeSmtpFactory([])

    with pytest.raises(SentCheckError, match="not advertised"):
        make_notifier(tmp_path, imap, smtp).send_report(
            {"Strategy": ["000001"]},
            verified_symbols=["000001"],
            market_date="2026-10-08",
        )

    assert smtp.calls == 0


def test_transient_smtp_failure_rechecks_sent_then_retries_once(tmp_path) -> None:
    imap = FakeImapFactory([False, False])
    smtp = FakeSmtpFactory([smtplib.SMTPServerDisconnected("lost"), None])

    result = make_notifier(tmp_path, imap, smtp).send_report(
        {"Strategy": ["000001"]},
        verified_symbols=["000001"],
        market_date="2026-10-08",
    )

    assert result.status is EmailSendStatus.SENT
    assert result.attempts == 2
    assert imap.calls == 2
    assert smtp.calls == 2


def test_transient_smtp_failure_does_not_retry_when_sent_now_contains_message(
    tmp_path,
) -> None:
    imap = FakeImapFactory([False, True])
    smtp = FakeSmtpFactory([smtplib.SMTPServerDisconnected("ambiguous")])

    result = make_notifier(tmp_path, imap, smtp).send_report(
        {"Strategy": ["000001"]},
        verified_symbols=["000001"],
        market_date="2026-10-08",
    )

    assert result.status is EmailSendStatus.ALREADY_SENT
    assert result.attempts == 1
    assert smtp.calls == 1


def test_transient_smtp_failure_is_attempted_at_most_twice(tmp_path) -> None:
    imap = FakeImapFactory([False, False])
    smtp = FakeSmtpFactory(
        [
            smtplib.SMTPServerDisconnected("first"),
            smtplib.SMTPServerDisconnected("second"),
        ]
    )

    with pytest.raises(smtplib.SMTPServerDisconnected, match="second"):
        make_notifier(tmp_path, imap, smtp).send_report(
            {"Strategy": ["000001"]},
            verified_symbols=["000001"],
            market_date="2026-10-08",
        )

    assert imap.calls == 2
    assert smtp.calls == 2


def test_permanent_smtp_error_is_not_retried(tmp_path) -> None:
    imap = FakeImapFactory([False])
    smtp = FakeSmtpFactory([smtplib.SMTPAuthenticationError(535, b"bad credentials")])

    with pytest.raises(smtplib.SMTPAuthenticationError):
        make_notifier(tmp_path, imap, smtp).send_report(
            {"Strategy": ["000001"]},
            verified_symbols=["000001"],
            market_date="2026-10-08",
        )

    assert imap.calls == 1
    assert smtp.calls == 1


def test_empty_verified_intersection_writes_evidence_but_does_not_use_network(
    tmp_path,
) -> None:
    imap = FakeImapFactory([])
    smtp = FakeSmtpFactory([])

    result = make_notifier(tmp_path, imap, smtp).send_report(
        {"Strategy": ["600519"]},
        verified_symbols=["000001"],
        market_date="2026-10-08",
    )

    assert result.status is EmailSendStatus.EMPTY
    assert result.message_id is None
    assert result.symbol_count == 0
    assert result.report_path.exists()
    assert "600519" not in result.report_path.read_text(encoding="utf-8")
    assert imap.calls == 0
    assert smtp.calls == 0


def test_invalid_verified_symbol_fails_before_report_or_network(tmp_path) -> None:
    imap = FakeImapFactory([])
    smtp = FakeSmtpFactory([])

    with pytest.raises(ValueError, match="invalid verified stock symbol"):
        make_notifier(tmp_path, imap, smtp).send_report(
            {"Strategy": ["000001"]},
            verified_symbols=["000001", "bad"],
            market_date="2026-10-08",
        )

    assert list(tmp_path.iterdir()) == []
    assert imap.calls == 0
    assert smtp.calls == 0


def test_report_dir_uses_settings_unless_explicitly_overridden(tmp_path) -> None:
    settings_dir = tmp_path / "from-settings"
    explicit_dir = tmp_path / "explicit"
    settings = SimpleNamespace(
        gmail_user="sender@gmail.com",
        gmail_app_password="app-password",
        email_report_dir=str(settings_dir),
    )

    from_settings = EmailNotifier(
        settings,
        imap_factory=FakeImapFactory([]),
        smtp_factory=FakeSmtpFactory([]),
    ).send_report({}, [], "2026-10-08")
    explicit = EmailNotifier(
        settings,
        report_dir=explicit_dir,
        imap_factory=FakeImapFactory([]),
        smtp_factory=FakeSmtpFactory([]),
    ).send_report({}, [], "2026-10-08")

    assert from_settings.report_path.parent == settings_dir.resolve()
    assert explicit.report_path.parent == explicit_dir.resolve()


def test_missing_credentials_raise_clear_configuration_error(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("GMAIL_USER", raising=False)
    monkeypatch.delenv("GMAIL_APP_PASSWORD", raising=False)

    with pytest.raises(EmailConfigurationError, match="GMAIL_USER"):
        EmailNotifier(
            report_dir=tmp_path,
            imap_factory=FakeImapFactory([]),
            smtp_factory=FakeSmtpFactory([]),
        )
