from __future__ import annotations

import json
import os
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from confluent_kafka import Producer

from shared.history import iter_history_events
from shared.kafka_admin import ensure_topic, wait_for_broker


def env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    return default if raw is None else float(raw)


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def produce_event(producer: Producer, topic: str, event: dict[str, Any], key: str) -> None:
    producer.produce(
        topic,
        key=key.encode("utf-8"),
        value=json.dumps(event, ensure_ascii=False).encode("utf-8"),
    )
    producer.poll(0)


def main() -> None:
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092")
    topic = os.getenv("HISTORY_TOPIC", "browser.history.visits")
    csv_path = Path(os.getenv("HISTORY_CSV", "data/sample_history.csv"))
    startup_sleep_seconds = env_float("GENERATOR_STARTUP_SLEEP_SECONDS", 0.0)
    row_delay_seconds = env_float("GENERATOR_ROW_DELAY_SECONDS", 0.0)
    flush_timeout_seconds = env_float("GENERATOR_FLUSH_TIMEOUT_SECONDS", 30.0)
    run_id = os.getenv("RUN_ID", uuid.uuid4().hex[:12])

    if startup_sleep_seconds > 0:
        print(f"[generator] Waiting {startup_sleep_seconds:.1f}s so the stream app can subscribe...", flush=True)
        time.sleep(startup_sleep_seconds)

    wait_for_broker(bootstrap_servers)
    ensure_topic(bootstrap_servers, topic, partitions=1, replication_factor=1)

    producer = Producer(
        {
            "bootstrap.servers": bootstrap_servers,
            "client.id": "browser-history-generator",
            "acks": "all",
        }
    )

    print(f"[generator] Starting run_id={run_id} from {csv_path}", flush=True)
    produce_event(
        producer,
        topic,
        {
            "event_type": "run_started",
            "run_id": run_id,
            "source_file": str(csv_path),
            "emitted_at": utc_now(),
        },
        key="__run__",
    )

    sent_count = 0
    countable_count = 0
    skipped_count = 0

    for event in iter_history_events(csv_path, run_id):
        sent_count += 1
        if event["countable"]:
            countable_count += 1
        else:
            skipped_count += 1

        key = str(event.get("root_domain") or "__uncountable__")
        produce_event(producer, topic, event, key=key)

        if row_delay_seconds > 0:
            time.sleep(row_delay_seconds)

    produce_event(
        producer,
        topic,
        {
            "event_type": "end_of_stream",
            "run_id": run_id,
            "source_file": str(csv_path),
            "sent_count": sent_count,
            "countable_count": countable_count,
            "skipped_count": skipped_count,
            "emitted_at": utc_now(),
        },
        key="__eos__",
    )

    remaining = producer.flush(flush_timeout_seconds)
    if remaining:
        raise RuntimeError(f"Producer flush timed out with {remaining} message(s) still queued")

    print(
        "[generator] Finished "
        f"run_id={run_id}: sent={sent_count}, countable={countable_count}, skipped={skipped_count}",
        flush=True,
    )


if __name__ == "__main__":
    main()
