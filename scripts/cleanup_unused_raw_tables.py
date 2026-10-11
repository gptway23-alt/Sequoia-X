"""Manual one-time V10 migration for unused raw historical cache tables.

By default this is a dry run. Never deletes the primary strategy database or orders.
Only use AFTER the V10 workflow is deployed and a standalone backup is verified.
"""
from __future__ import annotations

import argparse
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

REMOVABLE = ("stock_daily_raw", "stock_daily_raw_sync")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", default="data/sequoia_v2.db")
    parser.add_argument("--backup-dir", default="backups")
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--confirm-drop-raw", action="store_true")
    parser.add_argument("--vacuum", action="store_true")
    args = parser.parse_args()
    path = Path(args.db)
    if not path.is_file():
        parser.error(f"Database does not exist: {path}")
    if args.apply and not args.confirm_drop_raw:
        parser.error("--apply requires --confirm-drop-raw")

    with sqlite3.connect(f"file:{path}?mode=rw", uri=True) as source:
        integrity = source.execute("PRAGMA integrity_check").fetchone()[0]
        if integrity != "ok":
            raise SystemExit(f"Database failed integrity_check: {integrity}")
        tables = {r[0] for r in source.execute(
            "SELECT name FROM sqlite_master WHERE type='table'")}
        # Primary strategy table is retained. Do not touch any trade or signal tables.
        if "stock_daily" not in tables:
            raise SystemExit("Required stock_daily strategy table missing")
        present = [table for table in REMOVABLE if table in tables]
        for table in present:
            rows = source.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0]
            print(f"Candidate: {table}, rows={rows}")
        if not args.apply:
            print("DRY RUN ONLY: no tables dropped. Add --apply --confirm-drop-raw after backup validation.")
            return
        dest_dir = Path(args.backup_dir)
        dest_dir.mkdir(parents=True, exist_ok=True)
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        backup = dest_dir / f"sequoia-before-raw-cleanup-{stamp}.sqlite3"
        if backup.exists():
            raise SystemExit(f"Backup exists: {backup}")
        with sqlite3.connect(backup) as output:
            source.backup(output)
            assert output.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
        print(f"Verified pre-deletion backup: {backup}")
        for table in present:
            source.execute(f'DROP TABLE "{table}"')
        source.commit()
        if args.vacuum:
            # VACUUM needs free disk space approximately equal to the DB size.
            source.execute("VACUUM")
        assert source.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
        assert "stock_daily" in {r[0] for r in source.execute(
            "SELECT name FROM sqlite_master WHERE type='table'")}
        print("Legacy raw cache tables removed; strategy and transaction tables preserved")


if __name__ == "__main__":
    main()
