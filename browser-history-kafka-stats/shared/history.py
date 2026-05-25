from __future__ import annotations

import csv
import ipaddress
from pathlib import Path
from typing import Iterable
from urllib.parse import urlparse

URL_COLUMN_CANDIDATES = (
    "url",
    "URL",
    "Url",
    "address",
    "Address",
    "location",
    "Location",
    "href",
    "Href",
)

TITLE_COLUMN_CANDIDATES = ("title", "Title", "page_title", "Page Title", "name", "Name")
TIME_COLUMN_CANDIDATES = (
    "visit_time",
    "Visit Time",
    "last_visit_time",
    "Last Visit Time",
    "time",
    "Time",
    "date",
    "Date",
)


class HistoryCsvError(ValueError):
    """Raised when a browser-history CSV cannot be interpreted."""


def _clean_header(value: str | None) -> str:
    return (value or "").strip().lstrip("\ufeff")


def _pick_column(fieldnames: Iterable[str], candidates: Iterable[str], fuzzy_token: str | None = None) -> str | None:
    clean_names = [_clean_header(name) for name in fieldnames]
    by_lower = {name.lower(): name for name in clean_names}

    for candidate in candidates:
        match = by_lower.get(candidate.lower())
        if match:
            return match

    if fuzzy_token:
        token = fuzzy_token.lower()
        for name in clean_names:
            if token in name.lower():
                return name

    return None


def detect_url_column(fieldnames: Iterable[str]) -> str:
    column = _pick_column(fieldnames, URL_COLUMN_CANDIDATES, fuzzy_token="url")
    if not column:
        available = ", ".join(_clean_header(name) for name in fieldnames)
        raise HistoryCsvError(f"Could not find a URL column in CSV headers: {available}")
    return column


def _pick_value(row: dict[str, str], candidates: Iterable[str], fuzzy_token: str | None = None) -> str:
    column = _pick_column(row.keys(), candidates, fuzzy_token=fuzzy_token)
    return (row.get(column, "") if column else "").strip()


def normalize_url(raw_url: str) -> str:
    url = (raw_url or "").strip()
    if not url:
        return ""

    # Many exported CSVs contain bare hostnames. Treat them as web URLs.
    if "://" not in url and not url.startswith(("about:", "chrome:", "edge:", "brave:", "file:")):
        return f"https://{url}"

    return url


def hostname_from_url(raw_url: str) -> str | None:
    url = normalize_url(raw_url)
    if not url:
        return None

    parsed = urlparse(url)
    if parsed.scheme not in {"http", "https"}:
        return None

    hostname = parsed.hostname
    if not hostname:
        return None

    hostname = hostname.lower().strip(".")
    if not hostname:
        return None

    try:
        ipaddress.ip_address(hostname)
        return None
    except ValueError:
        return hostname


def extract_root_domain(raw_url: str) -> str | None:
    """Return the root domain/TLD part used by the task: com, ua, org, edu, etc."""
    hostname = hostname_from_url(raw_url)
    if not hostname:
        return None

    labels = [label for label in hostname.split(".") if label]
    if not labels:
        return None

    return labels[-1]


def iter_history_events(csv_path: str | Path, run_id: str) -> Iterable[dict[str, object]]:
    path = Path(csv_path)
    if not path.exists():
        raise FileNotFoundError(f"History CSV not found: {path}")

    with path.open("r", encoding="utf-8-sig", newline="") as file:
        reader = csv.DictReader(file)
        if not reader.fieldnames:
            raise HistoryCsvError(f"History CSV has no header row: {path}")

        url_column = detect_url_column(reader.fieldnames)

        for sequence, row in enumerate(reader, start=1):
            url = (row.get(url_column, "") or "").strip()
            hostname = hostname_from_url(url)
            root_domain = extract_root_domain(url)
            yield {
                "event_type": "visit",
                "run_id": run_id,
                "sequence": sequence,
                "url": url,
                "title": _pick_value(row, TITLE_COLUMN_CANDIDATES, fuzzy_token="title"),
                "visit_time": _pick_value(row, TIME_COLUMN_CANDIDATES, fuzzy_token="time"),
                "hostname": hostname,
                "root_domain": root_domain,
                "countable": root_domain is not None,
            }
