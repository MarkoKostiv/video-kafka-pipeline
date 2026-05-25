from __future__ import annotations

import json
import logging
from typing import Any

from confluent_kafka import Consumer, KafkaError, Producer


logger = logging.getLogger(__name__)


def json_dumps(value: dict[str, Any]) -> bytes:
    return json.dumps(value, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


def json_loads(value: bytes | str) -> dict[str, Any]:
    if isinstance(value, bytes):
        value = value.decode("utf-8")
    data = json.loads(value)
    if not isinstance(data, dict):
        raise ValueError("Kafka JSON payload must be an object")
    return data


def create_producer(bootstrap_servers: str) -> Producer:
    return Producer(
        {
            "bootstrap.servers": bootstrap_servers,
            "enable.idempotence": True,
            "acks": "all",
            "compression.type": "zstd",
            "linger.ms": 10,
        }
    )


def create_consumer(
    bootstrap_servers: str,
    group_id: str,
    topics: list[str],
    *,
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
    consumer.subscribe(topics)
    return consumer


def delivery_report(err: KafkaError | None, msg: Any) -> None:
    if err is not None:
        logger.error("Kafka delivery failed topic=%s key=%r error=%s", msg.topic(), msg.key(), err)
        return
    logger.debug(
        "Kafka delivered topic=%s partition=%s offset=%s key=%r",
        msg.topic(),
        msg.partition(),
        msg.offset(),
        msg.key(),
    )


def produce_json(
    producer: Producer,
    topic: str,
    key: str,
    value: dict[str, Any],
    *,
    partition: int | None = None,
) -> None:
    kwargs: dict[str, Any] = {
        "topic": topic,
        "key": key.encode("utf-8"),
        "value": json_dumps(value),
        "callback": delivery_report,
    }
    if partition is not None:
        kwargs["partition"] = partition
    producer.produce(**kwargs)
    producer.poll(0)

