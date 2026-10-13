"""Email notifications for stock selections."""

import os
import smtplib
import time
from email.message import EmailMessage

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

    def send(
        self,
        symbols: list[str],
        strategy_name: str,
        webhook_key: str = "default",
    ) -> bool:
        """Send one selection email, retrying a transient pre-send failure once.

        A failure after ``send_message`` begins is treated as an uncertain
        delivery and is never retried, because Gmail may already have accepted
        the message.
        """
        del webhook_key  # Kept for compatibility with existing strategy calls.
        if not symbols:
            return True
        if not self.user or not self.password or not self.recipients:
            logger.error("邮件未发送：缺少 GMAIL_USER、GMAIL_APP_PASSWORD 或收件人配置")
            return False

        msg = EmailMessage()
        msg["Subject"] = f"Sequoia-X | {strategy_name} | {len(symbols)}只"
        msg["From"] = self.user
        msg["To"] = ", ".join(self.recipients)
        msg.set_content(
            f"Sequoia-X 选股结果\n\n"
            f"策略：{strategy_name}\n"
            f"选出股票数量：{len(symbols)}\n\n"
            + "\n".join(symbols)
        )

        for attempt in range(2):
            delivery_started = False
            try:
                with smtplib.SMTP_SSL(
                    "smtp.gmail.com", 465, timeout=self.timeout_seconds
                ) as smtp:
                    smtp.login(self.user, self.password)
                    delivery_started = True
                    refused = smtp.send_message(msg)
                    if refused:
                        logger.error(
                            "邮件被部分收件人拒收（策略=%s，拒收数=%d）",
                            strategy_name,
                            len(refused),
                        )
                        return False
                logger.info("邮件发送成功：策略=%s，收件人数=%d", strategy_name, len(self.recipients))
                return True
            except smtplib.SMTPAuthenticationError as exc:
                logger.error("Gmail SMTP 身份验证失败；请检查应用专用密码：%s", exc)
                return False
            except (smtplib.SMTPException, OSError, TimeoutError) as exc:
                if delivery_started:
                    logger.error(
                        "邮件提交后连接异常，投递状态不确定；为避免重复未重发（策略=%s）：%s",
                        strategy_name,
                        exc,
                    )
                    return False
                if attempt == 0:
                    logger.warning(
                        "Gmail SMTP 登录/连接失败，将重试一次（策略=%s）：%s",
                        strategy_name,
                        exc,
                    )
                    time.sleep(self.retry_delay_seconds)
                else:
                    logger.error(
                        "Gmail SMTP 登录/连接重试后仍失败（策略=%s）：%s",
                        strategy_name,
                        exc,
                    )

        return False
