# Sequoia-X V10：只读行情网关部署说明

## 文件变更

- `sequoia_x/gateway/app.py`：只有 GET `/health` 和 GET `/v1/stocks/{symbol}/daily`，复用现有 `sequoia_x.data.market_api.fetch_raw`，BaoStock `adjustflag=3`。完全不打开/修改 SQLite。
- `gpt-action-openapi.yaml`：供具备 GPT Actions 的自定义 GPT 导入的只读接口定义，只有两个 GET。
- `requirements-gateway.txt`：独立网关依赖，不修改主项目 uv.lock。
- `.github/workflows/sequoia.yml`：保留主策略运行、SQLite 基础缓存、TXT 产物；移除 GPT 数据库快照、全量 RAW 回填、Pages 全历史导出，统一为每轮一个缓存。缓存丢失时只报错、不自动触发完整市场回填；`main.py` 现有策略逻辑仍可能补齐缺失的股票历史。
- `scripts/cleanup_unused_raw_tables.py`：**默认只检查**。通过 `--apply --confirm-drop-raw` 才备份后删除 raw 缓存表。
- 已从源码包移除旧 `gpt_export.py`、`database_email.py` 及只针对其已弃用功能的测试。将 `market_api.py` 精简为只读的 `fetch_raw`、代码与价格验证函数；彻底移除该模块中的 raw SQLite 全量同步及静态导出代码，保留原策略主库与模拟交易账本。

## 运行只读网关

把代码部署到能访问 BaoStock 的 Linux/Windows 常驻服务器（**不能部署在 GitHub Pages 静态站**）：

```bash
python -m venv .venv-gateway
source .venv-gateway/bin/activate
pip install -r requirements-gateway.txt
export SEQUOIA_GATEWAY_API_KEY="$(python -c 'import secrets; print(secrets.token_urlsafe(32))')"
uvicorn sequoia_x.gateway.app:app --host 127.0.0.1 --port 8000 --workers 1
```

通过 Caddy/Nginx/托管服务 HTTPS 反向代理保护。不要在 `0.0.0.0` 无 TLS 情况下直接暴露 API key。把服务器密钥保存在 secrets 管理中，不提交 `.env`。

本机探针（把 `YOUR_KEY` 换为服务器的真实密钥）：

```bash
curl -H 'Authorization: Bearer YOUR_KEY' 'http://127.0.0.1:8000/health'
curl -H 'Authorization: Bearer YOUR_KEY' \
  'http://127.0.0.1:8000/v1/stocks/301363/daily?start=2026-07-01&end=2026-10-09'
```

## 连接 GPT

1. 先部署公网 HTTPS 域名。打开 `gpt-action-openapi.yaml`，把 `REPLACE-WITH-YOUR-HTTPS-DOMAIN` 替换为真实域名。
2. 对于仍支持 GPT Actions 的 GPT：编辑 GPT → Actions → Create new action → 导入这个 YAML。认证选 `API key` → `Bearer`，粘贴**同一份** `SEQUOIA_GATEWAY_API_KEY`。
3. 在 Preview 调用 `healthCheck`、`getAshareDailyBars`。查看日期、返回行数、数据来源和延迟。
4. 该 YAML 不声明任何 POST/PUT/PATCH/DELETE；GPT 没有数据库写入口或交易接口。
5. 如果你使用的是 ChatGPT 项目，而不是可配置 Actions 的自定义 GPT，则**仅粘贴 OpenAPI 文件不会自动添加工具**；需要通过支持的自定义插件/服务集成部署。不要把公网普通网页访问视为可保证的 API 访问。

## 必须保留

`data/sequoia_v2.db` 包含原策略使用的 `stock_daily` 及其他状态，**不可整体删除**；模拟交易订单、现金、持仓和 NAV 不做任何更改。
旧 raw 行情表可清理，但先部署新工作流、确认未来不再运行旧的全量回填，并保存备份。首次迁移前请保留可正常恢复的上一版 GitHub 缓存；不要删除唯一备份。

```bash
# 查看将要删的旧 raw 表（不会真的删）
python scripts/cleanup_unused_raw_tables.py --db data/sequoia_v2.db
# 确认库已备份、V10 已上线后才执行：
python scripts/cleanup_unused_raw_tables.py --db data/sequoia_v2.db --apply --confirm-drop-raw --vacuum
```

## 行情限制

`fetch_raw` 提供未复权历史日K，不是实时盘口，也不能证明成交。`price_asof` 为实际最新返回交易日。同步失败以 HTTP 502 告警、不使用捏造数值或 GPT 的记忆报价。单次查询不超过 400 自然日；超过则分段。

## 旧 GitHub Pages / Artifacts 的公开性

**GitHub Pages 先前发布的 `api/v1` 历史行情 JSON 不会因为修改 workflow 自动消失。** 要取消原静态 API 对外访问，请在 GitHub Settings → Pages 中手动取消发布，并验证原 `meta.json` 已不能访问。上线独立 HTTPS Gateway 后再执行该步骤，以免影响迁移期间的查询。清理历史 Artifacts/Caches 前保留独立备份。

## 从旧版仓库直接替换的提醒

1. **先备份**旧仓库 `data/`、`.env` 和需要保留的自定义文件；本安装包不包含真实数据库、密钥或历史记录。不要先删除旧仓库再解压。
2. 解压 ZIP 中的 `Sequoia-X-master/`，将里面的文件覆盖到 Git 工作树；旧版已弃用文件若原来存在，还需按 `DELETE_LEGACY_FEATURES.txt` 手动删除并提交删除记录。
3. 提交到 `master` 后检查 `.github/workflows/sequoia.yml` 的 `Run offline regression tests`；当前环境未执行 BaoStock 真实联网测试。
4. **上传代码不等于部署网关**：网关必须运行于持续在线、可以连接 BaoStock 的 HTTPS 服务器。配置 GPT 连接器后再验收。
5. 原 `data/sequoia_v2.db` 必须保留以支撑独立策略引擎；老 `stock_daily_raw` 缓存表可先 dry run 检查，备份后再申请删除。
6. GitHub Pages 历史站点如需撤销必须单独取消发布。

**禁止：** 把数据库文件、邮箱应用密码、网关 API Key 或 `.env` 上传至公开仓库。
