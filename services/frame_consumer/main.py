from __future__ import annotations

import logging
import sys
from queue import Queue
from typing import Any

from confluent_kafka import KafkaError, TopicPartition

from services.frame_consumer.triton_client import TritonSequenceClient
from services.frame_consumer.worker_pool import CommitRequest, WorkerPool
from shared.config import frame_render_settings, kafka_settings, triton_settings
from shared.kafka_utils import create_consumer, create_producer


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger("frame-consumer")


def _apply_commits(
    consumer: Any,
    requests: list[CommitRequest],
) -> None:
    if not requests:
        return
    # Each partition has exactly one worker, so commit requests for a partition
    # are produced in offset order. Collapse the batch to the highest offset.
    latest_per_partition: dict[tuple[str, int], CommitRequest] = {}
    for request in requests:
        key = (request.topic, request.partition)
        existing = latest_per_partition.get(key)
        if existing is None or request.next_offset > existing.next_offset:
            latest_per_partition[key] = request

    offsets = [
        TopicPartition(req.topic, req.partition, req.next_offset)
        for req in latest_per_partition.values()
    ]
    consumer.commit(offsets=offsets, asynchronous=False)
    for req in latest_per_partition.values():
        logger.info(
            "Committed offset topic=%s partition=%s next_offset=%s video_uuid=%s",
            req.topic,
            req.partition,
            req.next_offset,
            req.video_uuid,
        )


def main() -> int:
    kafka = kafka_settings()
    triton_cfg = triton_settings()
    render_cfg = frame_render_settings()

    consumer = create_consumer(
        kafka.bootstrap_servers,
        "frame-consumer-group",
        [kafka.raw_frames_topic],
    )
    producer = create_producer(kafka.bootstrap_servers)

    # The readiness client lives only on the main thread; each worker will
    # build its own Triton client in its own thread (gevent constraint).
    readiness_client = TritonSequenceClient(
        triton_cfg.url,
        triton_cfg.model_name,
        triton_cfg.request_timeout_seconds,
    )
    readiness_client.wait_until_ready()

    commit_queue: "Queue[CommitRequest]" = Queue()
    pool = WorkerPool(
        triton_settings=triton_cfg,
        frame_render_settings=render_cfg,
        producer=producer,
        detections_topic=kafka.detections_topic,
        commit_queue=commit_queue,
    )

    logger.info(
        "Consuming topic=%s group=frame-consumer-group with thread-per-partition worker pool render_enabled=%s render_output_dir=%s",
        kafka.raw_frames_topic,
        render_cfg.enabled,
        render_cfg.output_dir,
    )

    try:
        while True:
            _apply_commits(consumer, pool.drain_commit_queue())

            msg = consumer.poll(0.5)
            if msg is None:
                producer.poll(0)
                continue
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    logger.error("Kafka consumer error: %s", msg.error())
                continue

            pool.dispatch(msg)
    except KeyboardInterrupt:
        logger.info("Interrupted")
        return 130
    finally:
        pool.shutdown_all()
        _apply_commits(consumer, pool.drain_commit_queue())
        producer.flush(10)
        consumer.close()


if __name__ == "__main__":
    sys.exit(main())
