
import sqlite3
from pathlib import Path

DB_PATH = Path("data/sequoia_v2.db")

if not DB_PATH.exists():
    raise SystemExit(f"数据库不存在：{DB_PATH}，请检查实际路径及缓存恢复状态")

# 只读连接，不修改数据库
conn = sqlite3.connect(
    f"file:{DB_PATH.resolve().as_posix()}?mode=ro",
    uri=True
)

print("=== Sequoia-X 数据库检查 ===")
print(f"数据库文件：{DB_PATH}")
print(f"文件大小：{DB_PATH.stat().st_size / 1024**2:.2f} MB")

tables = [
    row[0]
    for row in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
    )
]

for table in ("stock_daily", "stock_daily_raw"):
    if table not in tables:
        print(f"\n{table}: 不存在，跳过")
        continue

    columns = [
        row[1] for row in conn.execute(f'PRAGMA table_info("{table}")')
    ]

    symbol_col = next(
        (c for c in ("symbol", "code", "stock_code") if c in columns),
        None
    )
    date_col = next(
        (c for c in ("date", "trade_date") if c in columns),
        None
    )

    if not symbol_col or not date_col:
        print(f"\n{table}: 无法识别股票或日期字段：{columns}")
        continue

    total = conn.execute(
        f'SELECT COUNT(*) FROM "{table}"'
    ).fetchone()[0]

    duplicate_groups = conn.execute(f"""
        SELECT COUNT(*) FROM (
            SELECT "{symbol_col}", "{date_col}"
            FROM "{table}"
            GROUP BY "{symbol_col}", "{date_col}"
            HAVING COUNT(*) > 1
        )
    """).fetchone()[0]

    print(f"\n表名：{table}")
    print(f"总记录：{total:,}")
    print(f"重复股票日期组合：{duplicate_groups:,}")

page_size = conn.execute("PRAGMA page_size").fetchone()[0]
page_count = conn.execute("PRAGMA page_count").fetchone()[0]
free_count = conn.execute("PRAGMA freelist_count").fetchone()[0]

print("\n=== SQLite 空间 ===")
print(f"数据库页面大小：{page_size * page_count / 1024**2:.2f} MB")
print(f"可复用空闲页：{page_size * free_count / 1024**2:.2f} MB")
print("\n检查完成：未执行任何删除或修改操作")

conn.close()
