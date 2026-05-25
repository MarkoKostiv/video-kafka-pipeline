from __future__ import annotations

from collections import Counter


def top_items(counter: Counter[str], limit: int) -> list[tuple[str, int]]:
    return sorted(counter.items(), key=lambda item: (-item[1], item[0]))[:limit]


def format_top(counter: Counter[str], limit: int = 5) -> str:
    rows = top_items(counter, limit)
    if not rows:
        return "No countable web visits received yet."

    width = max(len(domain) for domain, _ in rows)
    lines = []
    for index, (domain, visits) in enumerate(rows, start=1):
        lines.append(f"{index}. {domain:<{width}} {visits} visits")
    return "\n".join(lines)
