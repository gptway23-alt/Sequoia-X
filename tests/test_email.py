"""Tests for Gmail notification failure handling."""

import smtplib
from unittest.mock import MagicMock, patch

from sequoia_x.notify.email import EmailNotifier


def _configured_notifier(monkeypatch) -> EmailNotifier:
    monkeypatch.setenv("GMAIL_USER", "sender@example.com")
    monkeypatch.setenv("GMAIL_APP_PASSWORD", "test-password")
    monkeypatch.setenv("GMAIL_TO", "one@example.com,two@example.com")
    return EmailNotifier()


def test_login_disconnect_retries_once(monkeypatch) -> None:
    notifier = _configured_notifier(monkeypatch)
    first = MagicMock()
    first.__enter__.return_value = first
    first.login.side_effect = smtplib.SMTPServerDisconnected("closed")
    second = MagicMock()
    second.__enter__.return_value = second
    second.send_message.return_value = {}

    with (
        patch("sequoia_x.notify.email.smtplib.SMTP_SSL", return_value=first) as smtp_ssl,
        patch("sequoia_x.notify.email.smtplib.SMTP", return_value=second) as smtp_starttls,
        patch("sequoia_x.notify.email.time.sleep") as sleep,
    ):
        assert notifier.send(["000001"], "ExampleStrategy") is True

    smtp_ssl.assert_called_once_with("smtp.gmail.com", 465, timeout=notifier.timeout_seconds)
    smtp_starttls.assert_called_once_with(
        "smtp.gmail.com", 587, timeout=notifier.timeout_seconds
    )
    sleep.assert_called_once_with(notifier.retry_delay_seconds)
    first.send_message.assert_not_called()
    second.send_message.assert_called_once()
    assert second.send_message.call_args.args[0]["To"] == "one@example.com, two@example.com"


def test_disconnect_after_submission_starts_is_not_retried(monkeypatch) -> None:
    notifier = _configured_notifier(monkeypatch)
    smtp = MagicMock()
    smtp.__enter__.return_value = smtp
    smtp.send_message.side_effect = smtplib.SMTPServerDisconnected("closed")

    with (
        patch("sequoia_x.notify.email.smtplib.SMTP_SSL", return_value=smtp) as smtp_ssl,
        patch("sequoia_x.notify.email.smtplib.SMTP") as smtp_starttls,
        patch("sequoia_x.notify.email.time.sleep") as sleep,
    ):
        assert notifier.send(["000001"], "ExampleStrategy") is False

    smtp_ssl.assert_called_once()
    smtp_starttls.assert_not_called()
    sleep.assert_not_called()


def test_missing_mail_configuration_returns_failure(monkeypatch) -> None:
    monkeypatch.delenv("GMAIL_USER", raising=False)
    monkeypatch.delenv("GMAIL_APP_PASSWORD", raising=False)
    monkeypatch.delenv("GMAIL_TO", raising=False)
    notifier = EmailNotifier()

    with (
        patch("sequoia_x.notify.email.smtplib.SMTP_SSL") as smtp_ssl,
        patch("sequoia_x.notify.email.smtplib.SMTP") as smtp_starttls,
    ):
        assert notifier.send(["000001"], "ExampleStrategy") is False

    smtp_ssl.assert_not_called()
    smtp_starttls.assert_not_called()
