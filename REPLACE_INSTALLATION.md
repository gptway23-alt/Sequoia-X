# Sequoia-X V10 完整替换包：使用说明

## 包内内容

完整仓库源码（`Sequoia-X-master/`），而非局部覆盖。仅包含代码、配置模板、测试和说明，**不包含任何真实数据库或密码**。

## 推荐替换方法（已有 Git 克隆）

1. 备份 `data/`、`.env`、`reports/`（如有）以及自定义代码。
2. 将压缩包解压到其他目录。复制解压后 `Sequoia-X-master/` **里面的内容**到本地 Git 仓库根目录。
3. 按 `DELETE_LEGACY_FEATURES.txt` 删除旧模块：
   - `sequoia_x/data/gpt_export.py`
   - `sequoia_x/notify/database_email.py`
   - `tests/test_gpt_export.py`
   - `tests/test_database_email.py`
4. 查看 `git status` 和 `git diff`，确保真实 SQLite / `.env` 未被删除或提交。
5. 运行 `python -m pytest -q`（开发依赖完整安装时）。
6. 提交、推送到 `master`；部署 HTTPS 网关并配置 GPT 接口密钥和 `gpt-action-openapi.yaml`。
7. GPT 验证成功后，再按清理脚本谨慎处理旧 raw 表；不要直接删除策略主库。

## GitHub 网页上传注意事项

GitHub 的 Add file → Upload files **不会自动解压 ZIP**。需要先在电脑解压并上传内部文件，且旧文件需要在 GitHub 中单独删除；不要把 ZIP 本身上传到仓库后认为已替换。

## 最快的本机只读网关测试

```bash
python -m venv .venv-gateway
source .venv-gateway/bin/activate
pip install -r requirements-gateway.txt
export SEQUOIA_GATEWAY_API_KEY=请使用至少24位的随机密钥
uvicorn sequoia_x.gateway.app:app --host 127.0.0.1 --port 8000 --workers 1
```

独立终端测试：

```bash
curl -H 'Authorization: Bearer 你的密钥' http://127.0.0.1:8000/health
curl -H 'Authorization: Bearer 你的密钥' 'http://127.0.0.1:8000/v1/stocks/301363/daily?start=2026-07-01&end=2026-10-09'
```

只有成功返回真实 BaoStock 行情，再上线 GPT Action。不要把模拟测试当成真实联网证明。

## 本次删除 / 保留边界

移除：`market_api.py` 的 RAW 全市场 SQLite 同步及 Pages JSON 导出；旧 GPT 全库导出；数据库邮件。
保留：`sequoia_x/data/engine.py`、`data/sequoia_v2.db` 策略主库、策略与交易记录、TXT 报告。

注意：原策略 `main.py` 仍可能增量下载/补齐自己的股票历史数据库；GPT 个股日K查询则完全不打开数据库。
