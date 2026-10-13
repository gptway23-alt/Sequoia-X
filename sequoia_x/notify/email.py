"""Email notifications for stock selections."""

import hashlib
import imaplib
import os
import re
import smtplib
import time
from datetime import datetime
from email.message import EmailMessage
from zoneinfo import ZoneInfo

from sequoia_x.core.logger import get_logger

logger = get_logger(__name__)


class EmailNotifier:
    """Send selection emails through Gmail with bounded connection recovery."""

    def __init__(self, settings=None):
        self.user = os.environ.get("GMAIL_USER", "").strip()
        self.password = os.environ.get("GMAIL_APP_PASSWORD", "").strip()
        raw_recipients = os.environ.get("GMAIL_TO", self.user)
        self.recipients = [
            address.strip()
            for address in raw_recipients.replace(";", ",").split(",")
            if address.strip()
        ]
        self.timeout_seconds = 20
        self.retry_delay_seconds = 2

    def _message_was_sent(self, message_id: str) -> bool:
        """Search Gmail's special-use Sent mailbox by deterministic Message-ID."""
        mailbox = imaplib.IMAP4_SSL("imap.gmail.com", timeout=self.timeout_seconds)
        try:
            status, _ = mailbox.login(self.user, self.password)
            if status != "OK":
                raise RuntimeError("Gmail IMAP login did not return OK")

            status, folders = mailbox.list()
            if status != "OK" or not folders:
                raise RuntimeError("Gmail IMAP could not list mailboxes")

            sent_folder = None
            for folder in folders:
                if not re.search(rb"\\Sent(?:[\s)])", folder):
                    continue
                match = re.search(rb'("(?:[^"\\]|\\.)*")$', folder)
                if match:
                    sent_folder = match.group(1).decode("ascii")
                    break
            if sent_folder is None:
                raise RuntimeError("Gmail IMAP did not expose a \\Sent mailbox")

            status, _ = mailbox.select(sent_folder, readonly=True)
            if status != "OK":
                raise RuntimeError("Gmail IMAP could not select Sent")
            status, data = mailbox.search(
                None, "HEADER", "Message-ID", f'"{message_id}"'
            )
            if status != "OK":
                raise RuntimeError("Gmail IMAP Sent search failed")
            return bool(data and data[0].strip())
        finally:
            try:
                mailbox.logout()
            except Exception:
                pass

    def send(
        self,
        symbols: list[str],
        strategy_name: str,
        webhook_key: str = "default",
        verified_symbols: set[str] | None = None,
    ) -> bool:
        """Send a verified stock list with one bounded retry and Sent checking.

        If submission becomes uncertain, Gmail's Sent mailbox is checked before
        retrying with the same deterministic Message-ID.
        """
        del webhook_key  # Kept for compatibility with existing strategy calls.
        if not symbols:
            return True
        if verified_symbols is None:
            logger.error("邮件未发送：缺少当日行情核验股票集合")
            return False
        symbols = list(
            dict.fromkeys(
                symbol.strip()
                for symbol in symbols
                if isinstance(symbol, str)
                and re.fullmatch(r"\d{6}", symbol.strip())
                and symbol.strip() in verified_symbols
            )
        )
        if not symbols:
            logger.info(
                "邮件跳过：策略结果中没有当日行情已核验股票（策略=%s）",
                strategy_name,
            )
            return True
        if not self.user or not self.password or not self.recipients:
            logger.error("邮件未发送：缺少 GMAIL_USER、GMAIL_APP_PASSWORD 或收件人配置")
            return False

        today = datetime.now(ZoneInfo("Asia/Shanghai")).date().isoformat()
        digest = hashlib.sha256(
            (strategy_name + "\n" + "\n".join(sorted(symbols))).encode("utf-8")
        ).hexdigest()[:16]
        domain = self.user.rsplit("@", 1)[-1]
        message_id = f"<sequoia-x-{today}-{digest}@{domain}>"

        msg = EmailMessage()
        msg["Subject"] = f"Sequoia-X | {strategy_name} | {len(symbols)}只"
        msg["From"] = self.user
        msg["To"] = ", ".join(self.recipients)
        msg["Message-ID"] = message_id
        txt_content = "\n".join(symbols) + "\n"
        msg.set_content(
            f"Sequoia-X 选股结果\n\n"
            f"策略：{strategy_name}\n"
            f"选出股票数量：{len(symbols)}\n\n"
            + txt_content
        )
        filename_strategy = re.sub(r"[^A-Za-z0-9_-]+", "_", strategy_name)
        msg.add_attachment(
            txt_content,
            subtype="plain",
            filename=f"Sequoia-X-{filename_strategy}-{today}.txt",
        )

        try:
            if self._message_was_sent(message_id):
                logger.info(
                    "邮件已存在于 Gmail Sent，跳过重复发送：策略=%s",
                    strategy_name,
                )
                return True
        except Exception as exc:
            logger.error(
                "Gmail Sent 核验失败，为避免重复本次不发送（策略=%s）：%s",
                strategy_name,
                exc,
            )
            return False

        transports = ((465, True), (587, False))
        for attempt, (port, implicit_tls) in enumerate(transports):
            delivery_started = False
            stage = "open connection"
            try:
                smtp_class = smtplib.SMTP_SSL if implicit_tls else smtplib.SMTP
                with smtp_class(
                    "smtp.gmail.com", port, timeout=self.timeout_seconds
                ) as smtp:
                    if not implicit_tls:
                        stage = "EHLO"
                        smtp.ehlo()
                        stage = "STARTTLS"
                        smtp.starttls()
                        stage = "EHLO after STARTTLS"
                        smtp.ehlo()
                    stage = "SMTP authentication"
                    smtp.login(self.user, self.password)
                    stage = "message submission"
                    delivery_started = True
                    refused = smtp.send_message(msg)
                    if refused:
                        logger.error(
                            "邮件被部分收件人拒收（策略=%s，拒收数=%d）",
                            strategy_name,
                            len(refused),
                        )
                        return False
                logger.info(
                    "邮件发送成功：策略=%s，收件人数=%d",
                    strategy_name,
                    len(self.recipients),
                )
                return True
            except smtplib.SMTPAuthenticationError as exc:
                logger.error("Gmail SMTP 身份验证失败；请检查应用专用密码：%s", exc)
                return False
            except (smtplib.SMTPException, OSError, TimeoutError) as exc:
                if delivery_started:
                    try:
                        if self._message_was_sent(message_id):
                            logger.info("邮件已在 Gmail Sent 核验到：策略=%s", strategy_name)
                            return True
                    except Exception as check_exc:
                        logger.error(
                            "邮件提交后连接异常且 Sent 核验失败；为避免重复不重发"
                            "（策略=%s）：%s；核验错误：%s",
                            strategy_name,
                            exc,
                            check_exc,
                        )
                        return False
                    if attempt == 0:
                        logger.warning(
                            "提交后连接异常，Sent 中未找到该 Message-ID，"
                            "将仅重试一次（策略=%s）：%s",
                            strategy_name,
                            exc,
                        )
                        time.sleep(self.retry_delay_seconds)
                        continue
                    logger.error(
                        "邮件重试后仍未确认进入 Gmail Sent（策略=%s）：%s",
                        strategy_name,
                        exc,
                    )
                    return False
                if attempt == 0:
                    logger.warning(
                        "Gmail SMTP 未提交邮件（策略=%s，端口=%d，阶段=%s），"
                        "将改用 587/STARTTLS 重试一次：%s",
                        strategy_name,
                        port,
                        stage,
                        exc,
                    )
                    time.sleep(self.retry_delay_seconds)
                else:
                    logger.error(
                        "Gmail SMTP 未提交邮件（策略=%s，端口=%d，阶段=%s）：%s",
                        strategy_name,
                        port,
                        stage,
                        exc,
                    )

        return False
