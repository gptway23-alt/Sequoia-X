# Sequoia-X: 王者回归 | The King Returns

> A 股量化选股系统 V2 | A-Share Quantitative Stock Selection System V2

---

## 简介 | Introduction

Sequoia-X V2 是面向 A 股市场的量化选股系统，基于现代 Python 工程化标准从零重构。
系统在上海时间 15:05 后校验收盘日线，再运行策略并通过 Gmail 发送选股结果 TXT。

当前股票池显式覆盖沪深主板、**创业板（300xxx / 301xxx）** 与科创板。
日常运行会自动检测本地 SQLite 中缺失的受支持股票并回填，因此旧缓存数据库升级后
无需删除重建即可补入创业板股票。

数据层优先使用 [BaoStock](http://baostock.com) 拉取历史及增量日 K 数据（后复权）；
BaoStock 不可用时，日线切换到 AKShare 的腾讯历史行情接口，交易日历使用新浪日历，股票名单使用 AKShare。
备用行情仍须通过目标日期、OHLC、成交量和成交额校验，并满足 98% 覆盖率门槛后才会写入 SQLite。

---

## 两种运行模式

```bash
python main.py               # 日常模式：收盘后同步行情、校验覆盖率、运行策略并邮件推送
python main.py --backfill     # 回填模式：通过 BaoStock 补充本地历史 K 线
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

---

## 快速开始 | Quick Start

### 环境要求

- Python >= 3.10

### 1. 安装依赖

```bash
# 推荐使用 uv（快速包管理器）
uv sync

# 或者 pip
pip install .
```

### 2. 配置环境变量

```bash
cp .env.example .env
# 编辑 .env，填写 Gmail 地址和应用专用密码
```

BaoStock 行情同步默认每批 25 只股票、最多 2 个并发批次；AKShare 备用行情逐股隔离并限制并发，
单次网络请求限时 15 秒，进程任务有硬超时。
批次完成会输出进度，超时批次会被终止并隔离；未达到 98% 覆盖率时不会发布当日行情。
这些值可通过 `.env` 中的 `BAOSTOCK_*` 参数调整；GitHub Actions 使用工作流中声明的参数。

### 3. 首次回填历史数据

```bash
python main.py --backfill
```

约 12 分钟完成 ~5200 只 A 股历史后复权日 K 数据回填。

### 4. 日常运行

```bash
python main.py
```

日常模式会在收盘确认时点前自动跳过。建议配合 crontab 每个交易日收盘后执行：

```cron
15 19 * * 1-5 cd /root/Sequoia-X && .venv/bin/python main.py >> log.txt 2>&1
```

---

## 目录结构 | Project Structure

```
Sequoia-X/
├── main.py                      # 入口：argparse 分发日常/回填模式
├── pyproject.toml               # 依赖声明 + ruff/pytest 配置
├── .env.example                 # 环境变量模板
├── data/                        # SQLite 数据库（运行时生成，不入 git）
├── sequoia_x/
│   ├── core/
│   │   ├── config.py            # Pydantic-settings 配置管理
│   │   └── logger.py            # rich 结构化日志
│   ├── data/
│   │   └── engine.py            # 数据引擎（baostock 回填 + 增量同步 + SQLite）
│   ├── strategy/
│   │   ├── base.py              # 策略抽象基类
│   │   ├── turtle_trade.py      # 海龟交易策略
│   │   ├── ma_volume.py         # 均线放量策略
│   │   ├── high_tight_flag.py   # 高窄旗形策略
│   │   ├── limit_up_shakeout.py # 涨停洗盘策略
│   │   ├── uptrend_limit_down.py # 上升跌停策略
│   │   └── rps_breakout.py      # RPS 突破策略
│   └── notify/
│       └── feishu.py            # 飞书 Webhook 推送
└── tests/                       # 属性测试（hypothesis）
```

---

## 数据说明

- **数据源**：主源 [BaoStock](http://baostock.com)，备用源 AKShare 腾讯历史日线 + 新浪交易日历
- **复权方式**：后复权（hfq）— 历史价格不变，适合增量存储，避免除权导致数据错乱
- **存储**：本地 SQLite（`data/sequoia_v2.db`），可直接拷贝到其他机器使用
- **日常同步**：最多 2 个进程、每批 100 只股票、单股最多重试 3 次
- **完整性门禁**：当日行情覆盖率低于 98% 时不发布数据、不运行策略；通过门禁后，策略和邮件只使用当日已核验股票
- **邮件通知**：每个非空策略结果附带仅含股票代码的 TXT；GitHub Actions 收件人为 `1044757346@qq.com` 和 `739152015@qq.com`
- **创业板支持**：300xxx / 301xxx 自动纳入股票池；涉及涨跌停的策略按 20% 常规限制判断

---

## 许可证 | License

MIT
