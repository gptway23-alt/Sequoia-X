"""配置管理属性测试。"""

from hypothesis import given, settings as h_settings, HealthCheck
from hypothesis import strategies as st


# Feature: sequoia-x-v2, Property 1: 环境变量覆盖配置默认值
@given(db_path=st.text(min_size=1, max_size=100, alphabet=st.characters(whitelist_categories=("Lu", "Ll", "Nd"), whitelist_characters="/_.-")))
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


# The webhook remains optional for the Gmail-only Actions workflow.
def test_feishu_webhook_is_optional_for_email_workflow(monkeypatch) -> None:
    """Email-only deployments can start without the legacy Feishu webhook."""
    from sequoia_x.core.config import Settings

    monkeypatch.delenv("FEISHU_WEBHOOK_URL", raising=False)
    settings = Settings(_env_file=None)
    assert settings.feishu_webhook_url is None
