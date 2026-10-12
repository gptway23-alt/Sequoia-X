"""Read-only integrity and duplicate checks for a Sequoia-X SQLite database."""

from __future__ import annotations

import os
import sqlite3
import sys
from pathlib import Path
from urllib.parse import quote


TABLES = ("stock_daily", "stock_daily_raw")


def main() -> int:
    path = Path(os.getenv("DB_PATH", "data/sequoia_v2.db")).resolve()
    if not path.is_file() or path.stat().st_size == 0:
        print(f"DB_NOT_FOUND_OR_EMPTY: {path}")
        return 2

    uri = f"file:{quote(path.as_posix(), safe='/')}?mode=ro"
    with sqlite3.connect(uri, uri=True, timeout=30) as connection:
        quick_check = connection.execute("PRAGMA quick_check").fetchone()
        if not quick_check or quick_check[0] != "ok":
            print(f"SQLITE_CHECK_FAILED: {quick_check}")
            return 1

        tables = {
            row[0]
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )
        }
        report = [
            "## Sequoia-X 数据库只读审计",
            f"- 数据库大小：{path.stat().st_size / 1024**2:.2f} MiB",
            "- SQLite quick_check：ok",
        ]
        print(f"DATABASE={path}")
        print("SQLITE_CHECK=ok")

        for table in TABLES:
            if table not in tables:
                report.append(f"- `{table}`：不存在")
                print(f"{table}: NOT_FOUND")
                if table == "stock_daily":
                    return 1
                continue

            columns = {
                row[1]
                for row in connection.execute(f'PRAGMA table_info("{table}")')
            }
            if not {"symbol", "date"} <= columns:
                print(f"{table}: missing symbol/date columns")
                return 1

            total = int(
                connection.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0]
            )
            duplicate_groups, extra_rows = connection.execute(
                f"""
                SELECT COUNT(*), COALESCE(SUM(n - 1), 0)
                FROM (
                    SELECT COUNT(*) AS n FROM "{table}"
                    GROUP BY symbol, date HAVING COUNT(*) > 1
                )
                """
            ).fetchone()
            print(
                f"{table}: rows={total:,}, duplicate_groups={duplicate_groups:,}, "
                f"extra_rows={extra_rows:,}"
            )
            report.append(
                f"- `{table}`：记录 {total:,}；重复股票日期组 {duplicate_groups:,}；"
                f"多余记录 {extra_rows:,}"
            )
            if duplicate_groups:
                print(f"::warning::{table} has duplicate symbol/date rows; no data changed")

        page_size = int(connection.execute("PRAGMA page_size").fetchone()[0])
        free_pages = int(connection.execute("PRAGMA freelist_count").fetchone()[0])
        reusable_mib = page_size * free_pages / 1024**2
        print(f"SQLITE_REUSABLE_SPACE={reusable_mib:.2f} MiB")
        report.extend(
            [
                f"- SQLite 可复用空间：{reusable_mib:.2f} MiB",
                "- 本次为只读检查，没有删除或修改数据库。",
            ]
        )

    summary_path = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary_path:
        with open(summary_path, "a", encoding="utf-8") as summary:
            summary.write("\n".join(report) + "\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
