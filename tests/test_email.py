"""Tests for Gmail notification failure handling."""

import smtplib
from unittest.mock import MagicMock, patch

from sequoia_x.notify.email import EmailNotifier


def _configured_notifier(monkeypatch) -> EmailNotifier:
    monkeypatch.setenv("GMAIL_USER", "sender@example.com")
    monkeypatch.setenv("GMAIL_APP_PASSWORD", "test-password")
    monkeypatch.setenv("GMAIL_TO", "one@example.com,two@example.com")
    return EmailNotifier()


def test_sent_lookup_uses_gmail_special_sent_folder(monkeypatch) -> None:
    notifier = _configured_notifier(monkeypatch)
    imap = MagicMock()
    imap.login.return_value = ("OK", [b"authenticated"])
    imap.list.return_value = (
        "OK",
        [b'* LIST (\\HasNoChildren \\Sent) "/" "[Gmail]/Sent Mail"'],
    )
    imap.select.return_value = ("OK", [b"1"])
    imap.search.return_value = ("OK", [b"7"])

    with patch("sequoia_x.notify.email.imaplib.IMAP4_SSL", return_value=imap) as imap_ssl:
        assert notifier._message_was_sent("<sequoia-x@example.com>") is True

    imap_ssl.assert_called_once_with("imap.gmail.com", timeout=notifier.timeout_seconds)
    imap.select.assert_called_once_with('"[Gmail]/Sent Mail"', readonly=True)
    imap.search.assert_called_once_with(
        None, "HEADER", "Message-ID", '"<sequoia-x@example.com>"'
    )


def test_login_disconnect_retries_once(monkeypatch) -> None:
    notifier = _configured_notifier(monkeypatch)
    first = MagicMock()
    first.__enter__.return_value = first
    first.login.side_effect = smtplib.SMTPServerDisconnected("closed")
    second = MagicMock()
    second.__enter__.return_value = second
    second.send_message.return_value = {}

    with (
        patch.object(notifier, "_message_was_sent", return_value=False),
        patch("sequoia_x.notify.email.smtplib.SMTP_SSL", return_value=first) as smtp_ssl,
        patch("sequoia_x.notify.email.smtplib.SMTP", return_value=second) as smtp_starttls,
        patch("sequoia_x.notify.email.time.sleep") as sleep,
    ):
        assert notifier.send(
            ["000001", "000002", "invalid"],
            "ExampleStrategy",
            verified_symbols={"000001"},
        ) is True

    smtp_ssl.assert_called_once_with("smtp.gmail.com", 465, timeout=notifier.timeout_seconds)
    smtp_starttls.assert_called_once_with(
        "smtp.gmail.com", 587, timeout=notifier.timeout_seconds
    )
    sleep.assert_called_once_with(notifier.retry_delay_seconds)
    first.send_message.assert_not_called()
    second.send_message.assert_called_once()
    message = second.send_message.call_args.args[0]
    assert message["To"] == "one@example.com, two@example.com"
    attachments = list(message.iter_attachments())
    assert len(attachments) == 1
    assert attachments[0].get_filename().endswith(".txt")
    assert attachments[0].get_content() == "000001\n"


def test_disconnect_after_submission_checks_sent_before_retry(monkeypatch) -> None:
    notifier = _configured_notifier(monkeypatch)
    smtp = MagicMock()
    smtp.__enter__.return_value = smtp
    smtp.send_message.side_effect = smtplib.SMTPServerDisconnected("closed")

    with (
        patch.object(notifier, "_message_was_sent", side_effect=[False, True]) as sent_check,
        patch("sequoia_x.notify.email.smtplib.SMTP_SSL", return_value=smtp) as smtp_ssl,
        patch("sequoia_x.notify.email.smtplib.SMTP") as smtp_starttls,
        patch("sequoia_x.notify.email.time.sleep") as sleep,
    ):
        assert notifier.send(["000001"], "ExampleStrategy", verified_symbols={"000001"}) is True

    smtp_ssl.assert_called_once()
    smtp_starttls.assert_not_called()
    sleep.assert_not_called()
    assert sent_check.call_count == 2


def test_post_submission_failure_retries_once_if_missing_from_sent(monkeypatch) -> None:
    notifier = _configured_notifier(monkeypatch)
    first = MagicMock()
    first.__enter__.return_value = first
    first.send_message.side_effect = smtplib.SMTPServerDisconnected("closed")
    second = MagicMock()
    second.__enter__.return_value = second
    second.send_message.return_value = {}

    with (
        patch.object(notifier, "_message_was_sent", side_effect=[False, False]) as sent_check,
        patch("sequoia_x.notify.email.smtplib.SMTP_SSL", return_value=first) as smtp_ssl,
        patch("sequoia_x.notify.email.smtplib.SMTP", return_value=second) as smtp_starttls,
        patch("sequoia_x.notify.email.time.sleep") as sleep,
    ):
        assert notifier.send(["000001"], "ExampleStrategy", verified_symbols={"000001"}) is True

    smtp_ssl.assert_called_once()
    smtp_starttls.assert_called_once()
    sleep.assert_called_once_with(notifier.retry_delay_seconds)
    assert sent_check.call_count == 2


def test_missing_mail_configuration_returns_failure(monkeypatch) -> None:
    monkeypatch.delenv("GMAIL_USER", raising=False)
    monkeypatch.delenv("GMAIL_APP_PASSWORD", raising=False)
    monkeypatch.delenv("GMAIL_TO", raising=False)
    notifier = EmailNotifier()

    with (
        patch("sequoia_x.notify.email.smtplib.SMTP_SSL") as smtp_ssl,
        patch("sequoia_x.notify.email.smtplib.SMTP") as smtp_starttls,
    ):
        with patch.object(notifier, "_message_was_sent", return_value=False):
            assert notifier.send(
                ["000001"], "ExampleStrategy", verified_symbols={"000001"}
            ) is False

    smtp_ssl.assert_not_called()
    smtp_starttls.assert_not_called()
