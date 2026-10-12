"""只读查看 SQLite 行情暂存、发布覆盖。可独立运行：python scripts/check_staging.py。"""

import os
import sqlite3
import sys
from pathlib import Path
from urllib.parse import quote


def main() -> int:
    db_file = Path(os.getenv("DB_PATH", "data/sequoia_v2.db")).resolve()
    if not db_file.is_file():
        print(f"DB_NOT_FOUND: {db_file}")
        return 2
    # 只读打开，确保该诊断脚本不会新建、修改或重置数据库
    uri = "file:" + quote(db_file.as_posix(), safe="/") + "?mode=ro"
    with sqlite3.connect(uri, uri=True, timeout=20) as db:
        quick = db.execute("PRAGMA quick_check").fetchone()
        print(f"DB_PATH={db_file}")
        print(f"SQLITE_CHECK={quick[0] if quick else 'UNKNOWN'}")
        exists = db.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='stock_daily_staging'").fetchone()
        if not exists:
            print("STAGING_TABLE=NOT_FOUND (首次运行将由 DataEngine 初始化)")
            return 0
        for day, n in db.execute("SELECT target_date, COUNT(DISTINCT symbol) FROM stock_daily_staging GROUP BY target_date ORDER BY target_date DESC LIMIT 12"):
            published = db.execute("SELECT COUNT(DISTINCT symbol) FROM stock_daily WHERE date = ?", (day,)).fetchone()[0]
            print(f"DATE={day} STAGED={n} PUBLISHED={published}")
    return 0 if quick and quick[0] == 'ok' else 1


if __name__ == "__main__":
    sys.exit(main())
