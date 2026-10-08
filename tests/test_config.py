"""配置管理属性测试。"""

import os
from hypothesis import HealthCheck, given, settings as h_settings
from hypothesis import strategies as st


# Feature: sequoia-x-v2, Property 1: 环境变量覆盖配置默认值
@given(
    db_path=st.text(
        min_size=1,
        max_size=100,
        alphabet=st.characters(
            whitelist_categories=("Lu", "Ll", "Nd"),
            whitelist_characters="/_.-",
        ),
    )
)
@h_settings(max_examples=100, suppress_health_check=[HealthCheck.function_scoped_fixture])
def test_env_overrides_default(db_path: str, monkeypatch) -> None:
    """属性 1：任意合法 db_path 通过环境变量设置后，Settings 实例应反映该值。"""
    import sequoia_x.core.config as cfg_module
    monkeypatch.setenv("DB_PATH", db_path)
    monkeypatch.setenv("FEISHU_WEBHOOK_URL", "https://example.com/hook")
    monkeypatch.setattr(cfg_module, "_settings", None)
    from sequoia_x.core.config import Settings
    s = Settings()
    assert s.db_path == db_path


# Feature: sequoia-x-v2, Property 2: 邮件与飞书配置按实际使用延迟校验
def test_notification_credentials_are_optional_at_startup() -> None:
    """回填和无选股运行不应依赖任何通知凭据。"""
    from sequoia_x.core.config import Settings

    keys = ("FEISHU_WEBHOOK_URL", "GMAIL_USER", "GMAIL_APP_PASSWORD")
    backup = {key: os.environ.pop(key, None) for key in keys}
    try:
        settings = Settings(_env_file=None)
        assert settings.feishu_webhook_url is None
        assert settings.gmail_user is None
        assert settings.gmail_app_password is None
    finally:
        for key, value in backup.items():
            if value is not None:
                os.environ[key] = value
