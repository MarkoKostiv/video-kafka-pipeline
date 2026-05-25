from __future__ import annotations

import time

from confluent_kafka import KafkaException
from confluent_kafka.admin import AdminClient, NewTopic


def wait_for_broker(bootstrap_servers: str, timeout_seconds: float = 60.0) -> None:
    admin = AdminClient({"bootstrap.servers": bootstrap_servers})
    deadline = time.monotonic() + timeout_seconds
    last_error: Exception | None = None

    while time.monotonic() < deadline:
        try:
            admin.list_topics(timeout=3)
            return
        except Exception as exc:  # pragma: no cover - depends on broker timing
            last_error = exc
            time.sleep(1)

    raise TimeoutError(f"Kafka broker did not become ready within {timeout_seconds}s: {last_error}")


def ensure_topic(
    bootstrap_servers: str,
    topic: str,
    partitions: int = 1,
    replication_factor: int = 1,
) -> None:
    admin = AdminClient({"bootstrap.servers": bootstrap_servers})
    futures = admin.create_topics([NewTopic(topic, partitions, replication_factor)])

    for future in futures.values():
        try:
            future.result(timeout=15)
        except KafkaException as exc:
            if "TOPIC_ALREADY_EXISTS" not in str(exc):
                raise
