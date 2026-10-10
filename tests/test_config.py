"""配置管理属性测试。"""

import os

import pytest
from hypothesis import HealthCheck, given, settings as h_settings
from hypothesis import strategies as st
from pydantic import ValidationError


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


def test_daily_sync_cutoff_can_be_configured(monkeypatch) -> None:
    """上海收盘门禁时间必须能由 DAILY_SYNC_NOT_BEFORE 覆盖。"""
    from sequoia_x.core.config import Settings

    monkeypatch.setenv("DAILY_SYNC_NOT_BEFORE", "16:05")
    configured = Settings(_env_file=None)

    assert configured.daily_sync_not_before == "16:05"


@pytest.mark.parametrize("value", [0.0, -0.1, 1.01, float("nan")])
def test_invalid_daily_coverage_is_rejected(value: float) -> None:
    """覆盖率门禁配置错误时必须停止，不能静默放宽数据完整性要求。"""
    from sequoia_x.core.config import Settings

    with pytest.raises(ValidationError):
        Settings(min_daily_coverage=value, _env_file=None)
