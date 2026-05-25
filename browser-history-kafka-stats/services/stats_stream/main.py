from __future__ import annotations

import json
import os
from collections import Counter
from typing import Any

from confluent_kafka import Consumer, KafkaError, KafkaException

from shared.kafka_admin import ensure_topic, wait_for_broker
from shared.printing import format_top


def env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    return default if raw is None else int(raw)


def print_snapshot(counter: Counter[str], top_n: int, prefix: str) -> None:
    print(prefix, flush=True)
    print(format_top(counter, top_n), flush=True)


def parse_event(value: bytes) -> dict[str, Any]:
    return json.loads(value.decode("utf-8"))


def main() -> None:
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092")
    topic = os.getenv("HISTORY_TOPIC", "browser.history.visits")
    group_id = os.getenv("STATS_GROUP_ID", "browser-history-stats")
    auto_offset_reset = os.getenv("STATS_AUTO_OFFSET_RESET", "latest")
    top_n = env_int("TOP_N", 5)
    print_every_messages = env_int("PRINT_EVERY_MESSAGES", 25)

    wait_for_broker(bootstrap_servers)
    ensure_topic(bootstrap_servers, topic, partitions=1, replication_factor=1)

    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap_servers,
            "group.id": group_id,
            "client.id": "browser-history-stats-stream",
            "auto.offset.reset": auto_offset_reset,
            "enable.auto.commit": False,
        }
    )

    counter: Counter[str] = Counter()
    active_run_id: str | None = None
    processed_count = 0
    skipped_count = 0

    print(f"[stats-stream] Subscribing to topic={topic} group_id={group_id}", flush=True)
    consumer.subscribe([topic])

    try:
        while True:
            message = consumer.poll(1.0)
            if message is None:
                continue

            if message.error():
                if message.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise KafkaException(message.error())

            event = parse_event(message.value())
            event_type = event.get("event_type")
            run_id = event.get("run_id")

            if event_type == "run_started":
                active_run_id = str(run_id)
                counter.clear()
                processed_count = 0
                skipped_count = 0
                print(
                    f"[stats-stream] Run started: run_id={active_run_id}, source={event.get('source_file')}",
                    flush=True,
                )

            elif event_type == "visit":
                if active_run_id and run_id != active_run_id:
                    consumer.commit(message, asynchronous=False)
                    continue

                processed_count += 1
                root_domain = event.get("root_domain")
                if isinstance(root_domain, str) and root_domain:
                    counter[root_domain] += 1
                else:
                    skipped_count += 1

                if print_every_messages > 0 and processed_count % print_every_messages == 0:
                    print_snapshot(
                        counter,
                        top_n,
                        prefix=f"[stats-stream] Running top {top_n} after {processed_count} history rows:",
                    )

            elif event_type == "end_of_stream":
                if active_run_id and run_id != active_run_id:
                    consumer.commit(message, asynchronous=False)
                    continue

                print_snapshot(
                    counter,
                    top_n,
                    prefix=(
                        f"[stats-stream] FINAL top {top_n} root domains "
                        f"for run_id={run_id} after {processed_count} history rows:"
                    ),
                )
                print(
                    "[stats-stream] Summary: "
                    f"producer_sent={event.get('sent_count')}, "
                    f"producer_countable={event.get('countable_count')}, "
                    f"producer_skipped={event.get('skipped_count')}, "
                    f"consumer_skipped={skipped_count}",
                    flush=True,
                )
                consumer.commit(message, asynchronous=False)
                break

            consumer.commit(message, asynchronous=False)

    finally:
        consumer.close()


if __name__ == "__main__":
    main()
