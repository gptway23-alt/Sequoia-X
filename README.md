# Sequoia-X: 王者回归 | The King Returns

> A 股量化选股系统 V2 | A-Share Quantitative Stock Selection System V2

---

## 简介 | Introduction

Sequoia-X V2 是面向 A 股市场的量化选股系统，基于现代 Python 工程化标准从零重构。
系统以 OOP 架构、向量化计算和增量数据更新为核心设计原则，每日收盘后自动选股并发送 TXT 邮件。

当前股票池显式覆盖沪深主板与**创业板（300xxx / 301xxx）**。
日常运行会自动检测本地 SQLite 中缺失的受支持股票并回填，因此旧缓存数据库升级后
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
│   │   └── gpt_export.py        # 已发布行情与已核验选股历史的安全快照
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
│       └── feishu.py            # 可选的飞书 Webhook 推送
└── tests/                       # 属性测试（hypothesis）
```

---

## 数据说明

- **数据源**：[baostock](http://baostock.com)（免费、无需注册、无限流）
- **复权方式**：后复权（hfq）— 历史价格不变，适合增量存储，避免除权导致数据错乱
- **存储**：本地 SQLite（`data/sequoia_v2.db`），可直接拷贝到其他机器使用
- **日常增量**：默认最多 2 个进程、小批次隔离、有限退避重试；覆盖率不达标时不发布
- **邮件安全**：策略只读取目标交易日已核验股票；同步发送至两个 QQ 邮箱和 `gptway23@gmail.com`；发送前查询 SENT，失败最多重试一次
- **创业板支持**：300xxx / 301xxx 自动纳入股票池；涉及涨跌停的策略按 20% 常规限制判断

---

## 让 GPT 读取完整数据库

程序会从启用本版本后的每个完整交易日开始，将已核验选股历史写入：

- `selection_runs`：市场日期、覆盖率、已执行策略和结果行数。
- `selection_results`：市场日期、策略名称和股票代码。

GitHub Actions 只有在该市场日期已通过行情门禁时，才会生成
`sequoia-gpt-database-<market_date>-<run_id>-<run_attempt>` Artifact（保留 7 天）。压缩包包含：

- `sequoia-x-verified.sqlite3`：全部已发布日线行情和完整选股历史。
- `manifest.json`：数据日期、行数、覆盖率和数据库 SHA-256。
- `README.txt`：只读查询说明和示例。

`stock_daily_staging` 不会进入快照；订单、资金、持仓、NAV 和任何未知表也不会复制。
因此同步失败时不会把暂存数据或上一交易日行情标成当日行情。邮件发送失败不会删除已经
原子保存的选股历史，工作流仍可生成已核验数据库 Artifact，同时保留邮件失败状态。

使用方法：

1. 打开对应的 GitHub Actions 运行页面。
2. 在 **Artifacts** 下载 `sequoia-gpt-database-...` 并解压。
3. 将 `sequoia-x-verified.sqlite3` 和 `manifest.json` 一起上传给 GPT。
4. 先让 GPT 校验 `manifest.json` 的 SHA-256 和 `latest_market_date`，再以 SQLite 只读模式查询。

可以使用以下提示词：

```text
读取 manifest.json 和 sequoia-x-verified.sqlite3。先校验数据库 SHA-256，报告
latest_market_date、覆盖率与选股历史的最新日期；随后仅以只读方式查询。
若请求日期晚于 latest_market_date，明确说明数据尚不可用，不得用旧行情代替。
```

---

## 许可证 | License

MIT
