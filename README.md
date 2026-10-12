# Sequoia-X: 王者回归 | The King Returns

> A 股量化选股系统 V2 | A-Share Quantitative Stock Selection System V2

---

## 简介 | Introduction

Sequoia-X V2 是面向 A 股市场的量化选股系统，基于现代 Python 工程化标准从零重构。
系统以 OOP 架构、向量化计算和增量数据更新为核心设计原则，每日收盘后自动选股并发送 TXT 邮件。

当前股票池显式覆盖沪深主板与**创业板（300xxx / 301xxx）**。
原有策略引擎仍可能检测本地 SQLite 中缺失的受支持股票并补齐，因此旧缓存数据库升级后
无需删除重建即可补入创业板股票。

数据层使用 [baostock](http://baostock.com)（免费、无需注册、无限流）拉取历史及增量日 K 数据（后复权），
存储于本地 SQLite，彻底规避东方财富反爬问题。

---

## 三种运行方式

```bash
python main.py                           # 日常模式：受控增量同步 + 跑策略 + 已核验 TXT 邮件
python main.py --target-date 2026-10-09 # 指定历史交易日重跑
python main.py --backfill                # 回填模式：全市场历史 K 线一次性灌入
```

---

## 内置策略 | Strategies

| 策略 | 说明 |
|---|---|
| **TurtleTrade** | 海龟突破：20日新高 + 成交额过亿 + 阳线防诱多，按涨幅排序 |
| **MaVolume** | 均线+放量突破 |
| **HighTightFlag** | 高而窄的旗形整理突破 |
| **LimitUpShakeout** | 涨停洗盘回踩确认 |
| **UptrendLimitDown** | 上升趋势中的跌停反包 |
| **RpsBreakout** | 欧奈尔 RPS 相对强度突破 |
| **PrivatePlacement** | 定增事件筛选；历史重跑按目标日期截断事件，防止前视 |

---

## 快速开始 | Quick Start

### 环境要求

- Python >= 3.10

### 1. 安装依赖

```bash
# 推荐使用 uv（快速包管理器）
uv sync --locked

# 或者 pip
pip install .
```

### 2. 配置环境变量

```bash
cp .env.example .env
# 编辑 .env，填写 Gmail 发件账号和应用专用密码
```

### 3. 首次回填历史数据

```bash
python main.py --backfill
```

全市场首次回填可能持续较长时间；工作流会缓存已经通过完整性检查的进度，后续运行继续补齐。

### 4. 日常运行

```bash
python main.py
```

建议配合 crontab 每个交易日收盘后自动执行：

```cron
15 19 * * 1-5 cd /root/Sequoia-X && .venv/bin/python main.py >> log.txt 2>&1
```

---

## 目录结构 | Project Structure

```
Sequoia-X/
├── main.py                      # 入口：日常、指定日期重跑和历史回填
├── pyproject.toml               # 依赖声明 + ruff/pytest 配置
├── .env.example                 # 环境变量模板
├── data/                        # SQLite 数据库（运行时生成，不入 git）
├── sequoia_x/
│   ├── core/
│   │   ├── config.py            # Pydantic-settings 配置管理
│   │   └── logger.py            # rich 结构化日志
│   ├── data/
│   │   ├── engine.py            # 数据引擎（baostock 回填 + 增量同步 + SQLite）
│   │   └── market_api.py        # 未复权 BaoStock fetch_raw，供只读网关复用
│   ├── strategy/
│   │   ├── base.py              # 策略抽象基类
│   │   ├── turtle_trade.py      # 海龟交易策略
│   │   ├── ma_volume.py         # 均线放量策略
│   │   ├── high_tight_flag.py   # 高窄旗形策略
│   │   ├── limit_up_shakeout.py # 涨停洗盘策略
│   │   ├── uptrend_limit_down.py # 上升跌停策略
│   │   ├── rps_breakout.py      # RPS 突破策略
│   │   └── private_placement.py # 定增事件策略
│   └── notify/
│       ├── email.py             # TXT 报告、SENT 去重和有限重试
│       ├── ..                   # 不再包含数据库邮件功能
│       └── feishu.py            # 可选的飞书 Webhook 推送
└── tests/                       # 属性测试（hypothesis）
```

---

## 数据说明

- **数据源**：[baostock](http://baostock.com)（免费、无需注册、无限流）
- **复权方式**：后复权（hfq）— 历史价格不变，适合增量存储，避免除权导致数据错乱
- **存储**：本地 SQLite（`data/sequoia_v2.db`），可直接拷贝到其他机器使用
- **日常增量**：默认最多 2 个进程、小批次隔离、有限退避重试；覆盖率不达标时不发布
- **邮件安全**：每日主流程只发送 TXT 策略报告；手动按需流程只发送单只股票的历史行情 JSON，不发送数据库附件或全市场历史 ZIP。TXT 邮件发送前查询 Gmail SENT，临时失败最多重试一次
- **创业板支持**：300xxx / 301xxx 自动纳入股票池；涉及涨跌停的策略按 20% 常规限制判断

---

## V10：GPT 按需只读查询 A 股行情

从 V10 起停止生成 GPT 可下载的全市场 SQLite 快照、停止数据库分片邮件、停止工作流全市场 RAW 回填和 Pages 大规模行情发布。V10 的 `market_api.py` 现在**只包含 BaoStock 单股查询**，不再拥有全市场 SQLite 写入或静态 JSON 导出函数。GPT 仅通过服务器上运行的只读 HTTPS Gateway 请求 BaoStock 历史日K。

- 新建服务：`sequoia_x/gateway/app.py`；复用原 `market_api.fetch_raw()`，返回未复权 OHLCV。
- GPT 可用接口：GET `/health`、GET `/v1/stocks/{symbol}/daily?start=...&end=...`。
- 接入：`gpt-action-openapi.yaml`，以 API Key / Bearer 鉴权；完整步骤见 `V10-README-部署与安全说明.md`。
- 手动按需邮件：GitHub Actions 的 `Sequoia-X On-demand BaoStock` 只把单只股票的查询结果发送到 `gptway23@gmail.com` 并保存为短期 Artifact；不再向 Pages 或公开分支发布行情文件。
- 原策略 SQLite `data/sequoia_v2.db` 仍为程序内部必需；GPT 无法通过网关读写它。历史 raw 缓存表手动备份后才可清理。
- 旧 GitHub Pages 静态 API 可能仍在旧部署中公开；本版本不再部署 Pages。若要取消旧站点公开访问，需在 GitHub 仓库 Settings → Pages 中单独取消发布并检查旧 URL。

---

## 许可证 | License

MIT

> 提醒：**V10 只使 GPT 历史日K查询不依赖数据库**。原 `main.py` 策略扫描仍使用本地历史库及完整度检查；独立行情网关不等于全市场策略扫描的完整替代。
