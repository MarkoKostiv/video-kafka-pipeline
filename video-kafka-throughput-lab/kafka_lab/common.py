from __future__ import annotations

import csv
import os
from pathlib import Path


DEFAULT_BOOTSTRAP = "localhost:19092"


def resolve_bootstrap(explicit: str | None) -> str:
    if explicit:
        return explicit
    return os.getenv("KAFKA_BOOTSTRAP_SERVERS", DEFAULT_BOOTSTRAP)


def ensure_parent_dir(path: str | Path) -> None:
    Path(path).expanduser().resolve().parent.mkdir(parents=True, exist_ok=True)


def ensure_dir(path: str | Path) -> Path:
    directory = Path(path).expanduser().resolve()
    directory.mkdir(parents=True, exist_ok=True)
    return directory


def init_csv(path: str | Path, headers: list[str]) -> None:
    ensure_parent_dir(path)
    with Path(path).open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=headers)
        writer.writeheader()
