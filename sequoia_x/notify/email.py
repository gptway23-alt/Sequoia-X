import os
import smtplib
from email.message import EmailMessage


class EmailNotifier:
    def __init__(self, settings=None):
        self.user = os.environ["GMAIL_USER"]
        self.password = os.environ["GMAIL_APP_PASSWORD"]
        self.to = os.environ.get("GMAIL_TO", self.user)

    def send(
        self,
        symbols: list[str],
        strategy_name: str,
        webhook_key: str = "default",
    ) -> None:
        if not symbols:
            return

        body = (
            f"Sequoia-X 选股结果\n\n"
            f"策略：{strategy_name}\n"
            f"选出股票数量：{len(symbols)}\n\n"
            + "\n".join(symbols)
        )

        msg = EmailMessage()
        msg["Subject"] = f"Sequoia-X | {strategy_name} | {len(symbols)}只"
        msg["From"] = self.user
        msg["To"] = self.to
        msg.set_content(body)

        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as smtp:
            smtp.login(self.user, self.password)
            smtp.send_message(msg)
