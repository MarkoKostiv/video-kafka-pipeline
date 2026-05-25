#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import shutil
import sqlite3
import tempfile
from datetime import datetime, timezone
from pathlib import Path

CHROME_EPOCH_DELTA_MICROSECONDS = 11_644_473_600_000_000

BROWSER_BASE_PATHS = {
    "chrome": Path("~/Library/Application Support/Google/Chrome").expanduser(),
}


def chrome_time_to_iso(value: int | None) -> str:
    if not value:
        return ""
    unix_microseconds = int(value) - CHROME_EPOCH_DELTA_MICROSECONDS
    return datetime.fromtimestamp(
        unix_microseconds / 1_000_000, tz=timezone.utc
    ).isoformat()


def resolve_history_db(browser: str, profile: str, explicit_path: str | None) -> Path:
    if explicit_path:
        return Path(explicit_path).expanduser()

    base_path = BROWSER_BASE_PATHS.get(browser)
    if not base_path:
        choices = ", ".join(sorted(BROWSER_BASE_PATHS))
        raise ValueError(f"Unknown browser '{browser}'. Choose one of: {choices}")

    return base_path / profile / "History"


def export_history(history_db: Path, output_csv: Path, limit: int | None) -> int:
    if not history_db.exists():
        raise FileNotFoundError(f"Chromium History database not found: {history_db}")

    output_csv.parent.mkdir(parents=True, exist_ok=True)

    # Chromium keeps the History database locked while the browser is open.
    # Copying first makes the export reliable and avoids touching the source DB.
    with tempfile.NamedTemporaryFile(suffix=".sqlite", delete=False) as temp_file:
        temp_path = Path(temp_file.name)

    try:
        shutil.copy2(history_db, temp_path)
        connection = sqlite3.connect(temp_path)
        cursor = connection.cursor()

        sql = """
            SELECT
                visits.visit_time,
                urls.title,
                urls.url
            FROM visits
            JOIN urls ON urls.id = visits.url
            ORDER BY visits.visit_time DESC
        """
        params: tuple[int, ...] = ()
        if limit is not None:
            sql += " LIMIT ?"
            params = (limit,)

        rows = cursor.execute(sql, params)
        count = 0
        with output_csv.open("w", encoding="utf-8", newline="") as file:
            writer = csv.DictWriter(file, fieldnames=["visit_time", "title", "url"])
            writer.writeheader()
            for visit_time, title, url in rows:
                writer.writerow(
                    {
                        "visit_time": chrome_time_to_iso(visit_time),
                        "title": title or "",
                        "url": url or "",
                    }
                )
                count += 1

        connection.close()
        return count
    finally:
        temp_path.unlink(missing_ok=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Export Chromium-based browser history to CSV."
    )
    parser.add_argument(
        "--browser", choices=sorted(BROWSER_BASE_PATHS), default="chrome"
    )
    parser.add_argument(
        "--profile",
        default="Default",
        help="Profile folder name, for example Default or Profile 1",
    )
    parser.add_argument(
        "--history-db", help="Explicit path to a Chromium History SQLite file"
    )
    parser.add_argument(
        "--output", default="data/browser_history.csv", help="Output CSV path"
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=1000,
        help="Maximum visits to export; use 0 for all visits",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    limit = None if args.limit == 0 else args.limit
    history_db = resolve_history_db(args.browser, args.profile, args.history_db)
    output_csv = Path(args.output)
    count = export_history(history_db, output_csv, limit)
    print(f"Exported {count} visits from {history_db} to {output_csv}")


if __name__ == "__main__":
    main()
