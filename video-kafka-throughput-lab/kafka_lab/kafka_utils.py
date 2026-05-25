from __future__ import annotations

import json
from typing import Any

from confluent_kafka import Consumer, Producer


def json_dumps(payload: dict[str, Any]) -> bytes:
    return json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


def json_loads(payload: bytes | str) -> dict[str, Any]:
    if isinstance(payload, bytes):
        payload = payload.decode("utf-8")
    value = json.loads(payload)
    if not isinstance(value, dict):
        raise ValueError("Kafka payload must be a JSON object")
    return value


def create_producer(bootstrap_servers: str) -> Producer:
    return Producer(
        {
            "bootstrap.servers": bootstrap_servers,
            "enable.idempotence": True,
            "acks": "all",
            "compression.type": "zstd",
            "linger.ms": 5,
        }
    )


def create_consumer(
    bootstrap_servers: str,
    topic: str,
    group_id: str,
    auto_offset_reset: str = "earliest",
) -> Consumer:
    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap_servers,
            "group.id": group_id,
            "auto.offset.reset": auto_offset_reset,
            "enable.auto.commit": False,
            "max.poll.interval.ms": 900000,
            "session.timeout.ms": 45000,
        }
    )
    consumer.subscribe([topic])
    return consumer
