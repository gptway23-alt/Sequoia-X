"""Offline tests for verified TXT report delivery."""

from __future__ import annotations

import smtplib
from collections.abc import Iterable, Mapping
from email.message import EmailMessage
from email.utils import getaddresses
from types import SimpleNamespace

import pytest

from sequoia_x.notify.email import (
    RECIPIENTS,
    EmailConfigurationError,
    EmailDeliveryError,
    EmailNotifier,
    EmailSendStatus,
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
    def __init__(
        self,
        outcomes: Iterable[BaseException | Mapping[str, tuple[int, bytes]] | None],
    ) -> None:
        self.outcomes = list(outcomes)
        self.calls = 0
        self.messages: list[EmailMessage] = []

    def __call__(self, host: str, port: int, **kwargs: object) -> "FakeSmtp":
        outcome = self.outcomes[self.calls]
        self.calls += 1
        return FakeSmtp(self, outcome)


class FakeSmtp:
    def __init__(
        self,
        factory: FakeSmtpFactory,
        outcome: BaseException | Mapping[str, tuple[int, bytes]] | None,
    ) -> None:
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
        if isinstance(self.outcome, BaseException):
            raise self.outcome
        if self.outcome is not None:
            return dict(self.outcome)
        return {}


def make_notifier(tmp_path, imap: FakeImapFactory, smtp: FakeSmtpFactory) -> EmailNotifier:
    return EmailNotifier(
        user="sender@gmail.com",
        password="app-password",
        report_dir=tmp_path,
        imap_factory=imap,
        smtp_factory=smtp,
    )


def message_recipient(message: EmailMessage) -> str:
    recipients = [address for _, address in getaddresses(message.get_all("To", []))]
    assert len(recipients) == 1
    return recipients[0]


def test_sends_utf8_txt_with_only_verified_globally_deduplicated_symbols(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("GMAIL_TO", "attacker@example.com")
    imap = FakeImapFactory([False, False, False])
    smtp = FakeSmtpFactory([None, None, None])
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
    assert result.attempts == 3
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

    assert notifier.recipients == RECIPIENTS
    expected_message_ids = tuple(
        EmailNotifier._message_id("2026-10-08", report_text, (recipient,))
        for recipient in RECIPIENTS
    )
    assert result.message_id is None
    assert result.message_ids == expected_message_ids
    assert tuple(delivery.recipient for delivery in result.deliveries) == RECIPIENTS
    assert tuple(delivery.message_id for delivery in result.deliveries) == expected_message_ids
    assert tuple(delivery.status for delivery in result.deliveries) == (
        EmailSendStatus.SENT,
        EmailSendStatus.SENT,
        EmailSendStatus.SENT,
    )
    assert tuple(delivery.attempts for delivery in result.deliveries) == (1, 1, 1)

    assert [message_recipient(message) for message in smtp.messages] == list(RECIPIENTS)
    assert tuple(message["Message-ID"] for message in smtp.messages) == expected_message_ids
    assert imap.searches == [
        ("SEARCH", None, "HEADER", "Message-ID", f'"{message_id}"')
        for message_id in expected_message_ids
    ]
    assert imap.selected_mailboxes == ['"[Google Mail]/Gesendet"'] * 3

    for message in smtp.messages:
        attachments = list(message.iter_attachments())
        assert len(attachments) == 1
        assert attachments[0].get_filename() == "sequoia-x-2026-10-08.txt"
        assert attachments[0].get_content_charset() == "utf-8"
        assert attachments[0].get_payload(decode=True) == report_bytes


def test_message_id_is_deterministic_for_same_logical_report(tmp_path) -> None:
    first = make_notifier(
        tmp_path / "first",
        FakeImapFactory([True, True, True]),
        FakeSmtpFactory([]),
    ).send_report(
        {"Beta": ["600519", "000001"], "Alpha": ["000001"]},
        verified_symbols={"600519", "000001"},
        market_date="2026-10-08",
    )
    second = make_notifier(
        tmp_path / "second",
        FakeImapFactory([True, True, True]),
        FakeSmtpFactory([]),
    ).send_report(
        {"Alpha": ["000001"], "Beta": ["000001", "600519"]},
        verified_symbols=["000001", "600519"],
        market_date="2026-10-08",
    )

    assert first.message_id is None
    assert second.message_id is None
    assert first.message_ids == second.message_ids
    assert len(set(first.message_ids)) == len(RECIPIENTS)
    assert tuple(delivery.recipient for delivery in first.deliveries) == RECIPIENTS
    assert tuple(delivery.message_id for delivery in first.deliveries) == first.message_ids
    assert tuple(delivery.status for delivery in first.deliveries) == (
        EmailSendStatus.ALREADY_SENT,
        EmailSendStatus.ALREADY_SENT,
        EmailSendStatus.ALREADY_SENT,
    )
    assert tuple(delivery.attempts for delivery in first.deliveries) == (0, 0, 0)
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
    imap = FakeImapFactory([False, False, False])
    smtp = FakeSmtpFactory([None, None, None])
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
    message_recipients = tuple(message_recipient(message) for message in smtp.messages)
    assert message_recipients == RECIPIENTS
    assert "another-sender@gmail.com" not in {
        recipient.casefold() for recipient in message_recipients
    }


def test_existing_message_in_sent_skips_smtp(tmp_path) -> None:
    imap = FakeImapFactory([True, True, True])
    smtp = FakeSmtpFactory([])

    result = make_notifier(tmp_path, imap, smtp).send_report(
        {"Strategy": ["000001"]},
        verified_symbols=["000001"],
        market_date="2026-10-08",
    )

    assert result.status is EmailSendStatus.ALREADY_SENT
    assert result.attempts == 0
    assert tuple(delivery.recipient for delivery in result.deliveries) == RECIPIENTS
    assert tuple(delivery.status for delivery in result.deliveries) == (
        EmailSendStatus.ALREADY_SENT,
        EmailSendStatus.ALREADY_SENT,
        EmailSendStatus.ALREADY_SENT,
    )
    assert tuple(delivery.attempts for delivery in result.deliveries) == (0, 0, 0)
    assert imap.calls == 3
    assert smtp.calls == 0
    assert [search[3] for search in imap.searches] == ["Message-ID"] * 3


@pytest.mark.parametrize("imap_outcome", [OSError("offline"), "search-error"])
def test_sent_query_failure_is_fail_closed(tmp_path, imap_outcome) -> None:
    imap = FakeImapFactory([imap_outcome])
    smtp = FakeSmtpFactory([])

    with pytest.raises(
        EmailDeliveryError,
        match=rf"recipient={RECIPIENTS[0]}.*error=SentCheckError",
    ):
        make_notifier(tmp_path, imap, smtp).send_report(
            {"Strategy": ["000001"]},
            verified_symbols=["000001"],
            market_date="2026-10-08",
        )

    assert imap.calls == 1
    assert smtp.calls == 0


def test_missing_sent_special_use_mailbox_is_fail_closed(tmp_path) -> None:
    imap = FakeImapFactory(
        [False],
        mailboxes=[b'(\\HasNoChildren) "/" "[Gmail]/All Mail"'],
    )
    smtp = FakeSmtpFactory([])

    with pytest.raises(
        EmailDeliveryError,
        match=rf"recipient={RECIPIENTS[0]}.*not advertised",
    ):
        make_notifier(tmp_path, imap, smtp).send_report(
            {"Strategy": ["000001"]},
            verified_symbols=["000001"],
            market_date="2026-10-08",
        )

    assert smtp.calls == 0


def test_transient_smtp_failure_rechecks_sent_then_retries_once(tmp_path) -> None:
    imap = FakeImapFactory([False, False, False, False])
    smtp = FakeSmtpFactory(
        [smtplib.SMTPServerDisconnected("lost"), None, None, None]
    )

    result = make_notifier(tmp_path, imap, smtp).send_report(
        {"Strategy": ["000001"]},
        verified_symbols=["000001"],
        market_date="2026-10-08",
    )

    assert result.status is EmailSendStatus.SENT
    assert result.attempts == 4
    assert tuple(delivery.attempts for delivery in result.deliveries) == (2, 1, 1)
    assert imap.calls == 4
    assert smtp.calls == 4
    assert [message_recipient(message) for message in smtp.messages] == [
        RECIPIENTS[0],
        RECIPIENTS[0],
        RECIPIENTS[1],
        RECIPIENTS[2],
    ]


def test_transient_smtp_failure_does_not_retry_when_sent_now_contains_message(
    tmp_path,
) -> None:
    imap = FakeImapFactory([False, True, False, False])
    smtp = FakeSmtpFactory(
        [smtplib.SMTPServerDisconnected("ambiguous"), None, None]
    )

    result = make_notifier(tmp_path, imap, smtp).send_report(
        {"Strategy": ["000001"]},
        verified_symbols=["000001"],
        market_date="2026-10-08",
    )

    assert result.status is EmailSendStatus.RESUMED
    assert result.attempts == 3
    assert tuple(delivery.status for delivery in result.deliveries) == (
        EmailSendStatus.ALREADY_SENT,
        EmailSendStatus.SENT,
        EmailSendStatus.SENT,
    )
    assert tuple(delivery.attempts for delivery in result.deliveries) == (1, 1, 1)
    assert imap.calls == 4
    assert smtp.calls == 3


def test_transient_smtp_failure_is_attempted_at_most_twice(tmp_path) -> None:
    imap = FakeImapFactory([False, False, False, False])
    smtp = FakeSmtpFactory(
        [
            smtplib.SMTPServerDisconnected("first"),
            smtplib.SMTPServerDisconnected("second"),
            None,
            None,
        ]
    )

    with pytest.raises(
        EmailDeliveryError,
        match=rf"recipient={RECIPIENTS[0]}.*SMTPServerDisconnected: second",
    ):
        make_notifier(tmp_path, imap, smtp).send_report(
            {"Strategy": ["000001"]},
            verified_symbols=["000001"],
            market_date="2026-10-08",
        )

    assert imap.calls == 4
    assert smtp.calls == 4
    assert [message_recipient(message) for message in smtp.messages] == [
        RECIPIENTS[0],
        RECIPIENTS[0],
        RECIPIENTS[1],
        RECIPIENTS[2],
    ]


def test_permanent_smtp_error_is_not_retried(tmp_path) -> None:
    imap = FakeImapFactory([False, False, False])
    smtp = FakeSmtpFactory([smtplib.SMTPDataError(550, b"rejected"), None, None])

    with pytest.raises(
        EmailDeliveryError,
        match=rf"recipient={RECIPIENTS[0]}.*SMTPDataError",
    ):
        make_notifier(tmp_path, imap, smtp).send_report(
            {"Strategy": ["000001"]},
            verified_symbols=["000001"],
            market_date="2026-10-08",
        )

    assert imap.calls == 3
    assert smtp.calls == 3
    assert [message_recipient(message) for message in smtp.messages] == list(RECIPIENTS)


def test_partial_recipient_refusal_is_recovered_on_rerun(tmp_path) -> None:
    refused_recipient = RECIPIENTS[1]
    first_imap = FakeImapFactory([False, False, False])
    first_smtp = FakeSmtpFactory(
        [None, {refused_recipient: (550, b"no such user")}, None]
    )
    notifier = make_notifier(tmp_path, first_imap, first_smtp)

    with pytest.raises(
        EmailDeliveryError,
        match=rf"recipient={refused_recipient}.*SMTPRecipientsRefused",
    ):
        notifier.send_report(
            {"Strategy": ["000001"]},
            verified_symbols=["000001"],
            market_date="2026-10-08",
        )

    first_message_ids = tuple(message["Message-ID"] for message in first_smtp.messages)
    assert len(set(first_message_ids)) == len(RECIPIENTS)
    assert [message_recipient(message) for message in first_smtp.messages] == list(RECIPIENTS)
    assert first_imap.calls == 3
    assert first_smtp.calls == 3

    rerun_imap = FakeImapFactory([True, False, True])
    rerun_smtp = FakeSmtpFactory([None])
    result = make_notifier(tmp_path, rerun_imap, rerun_smtp).send_report(
        {"Strategy": ["000001"]},
        verified_symbols=["000001"],
        market_date="2026-10-08",
    )

    assert result.status is EmailSendStatus.RESUMED
    assert result.message_ids == first_message_ids
    assert result.attempts == 1
    assert tuple(delivery.recipient for delivery in result.deliveries) == RECIPIENTS
    assert tuple(delivery.status for delivery in result.deliveries) == (
        EmailSendStatus.ALREADY_SENT,
        EmailSendStatus.SENT,
        EmailSendStatus.ALREADY_SENT,
    )
    assert tuple(delivery.attempts for delivery in result.deliveries) == (0, 1, 0)
    assert rerun_imap.calls == 3
    assert rerun_smtp.calls == 1
    assert message_recipient(rerun_smtp.messages[0]) == refused_recipient
    assert rerun_smtp.messages[0]["Message-ID"] == first_message_ids[1]


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
    assert result.message_ids == ()
    assert result.deliveries == ()
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
