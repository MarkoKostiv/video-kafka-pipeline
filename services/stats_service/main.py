from __future__ import annotations

import logging
import sys
from dataclasses import dataclass, field
from typing import Any

from confluent_kafka import KafkaError

from shared.config import getenv, kafka_settings
from shared.kafka_utils import create_consumer, json_loads


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger("stats-service")

TRACKED_CLASSES = {"person", "car"}


@dataclass
class VideoStats:
    video_uuid: str
    video_name: str
    unique_people: set[str] = field(default_factory=set)
    unique_cars: set[str] = field(default_factory=set)
    last_sampled_frame_id: int = -1


@dataclass
class AggregateStats:
    unique_people: set[str] = field(default_factory=set)
    unique_cars: set[str] = field(default_factory=set)
    messages_processed: int = 0


def _fallback_global_track_id(video_uuid: str, detection: dict[str, Any], class_name: str) -> str:
    return f"{video_uuid}:{class_name}:{detection.get('track_id', 'unknown')}"


def _get_or_create_video_stats(
    states: dict[str, VideoStats],
    *,
    video_uuid: str,
    video_name: str,
) -> VideoStats:
    state = states.get(video_uuid)
    if state is not None:
        if state.video_name == "unknown" and video_name:
            state.video_name = video_name
        return state

    state = VideoStats(video_uuid=video_uuid, video_name=video_name or "unknown")
    states[video_uuid] = state
    return state


def _validate_detection_message(message: dict[str, Any]) -> None:
    required_fields = ["video_uuid", "video_name", "sampled_frame_id", "detections"]
    missing_fields = [field for field in required_fields if field not in message]
    if missing_fields:
        raise ValueError(f"Missing required fields in detection message: {', '.join(missing_fields)}")
    if not isinstance(message["detections"], list):
        raise ValueError("detections must be a list")


def process_detection_message(
    message: dict[str, Any],
    states: dict[str, VideoStats],
    aggregate: AggregateStats,
) -> None:
    _validate_detection_message(message)
    video_uuid = str(message["video_uuid"])
    video_name = str(message["video_name"])
    sampled_frame_id = int(message["sampled_frame_id"])

    state = _get_or_create_video_stats(states, video_uuid=video_uuid, video_name=video_name)
    state.last_sampled_frame_id = max(state.last_sampled_frame_id, sampled_frame_id)
    aggregate.messages_processed += 1

    for detection in message["detections"]:
        if not isinstance(detection, dict):
            continue
        class_name = str(detection.get("class_name", "")).lower()
        if class_name not in TRACKED_CLASSES:
            continue

        global_track_id = detection.get("global_track_id")
        if not isinstance(global_track_id, str) or not global_track_id:
            global_track_id = _fallback_global_track_id(video_uuid, detection, class_name)

        if class_name == "person":
            state.unique_people.add(global_track_id)
            aggregate.unique_people.add(global_track_id)
        elif class_name == "car":
            state.unique_cars.add(global_track_id)
            aggregate.unique_cars.add(global_track_id)

    logger.info(
        "Stats update video_uuid=%s video=%s frame=%s unique_people=%s unique_cars=%s total_unique_people=%s total_unique_cars=%s messages_processed=%s",
        video_uuid,
        state.video_name,
        sampled_frame_id,
        len(state.unique_people),
        len(state.unique_cars),
        len(aggregate.unique_people),
        len(aggregate.unique_cars),
        aggregate.messages_processed,
    )
    print(
        f"STATISTICS_UPDATE video_uuid={state.video_uuid} "
        f"video={state.video_name} "
        f"frame={sampled_frame_id} "
        f"unique_people={len(state.unique_people)} "
        f"unique_cars={len(state.unique_cars)} "
        f"total_unique_people={len(aggregate.unique_people)} "
        f"total_unique_cars={len(aggregate.unique_cars)}",
        flush=True,
    )


def main() -> int:
    kafka = kafka_settings()
    group_id = getenv("STATS_GROUP_ID", "stats-service-group")
    auto_offset_reset = getenv("STATS_AUTO_OFFSET_RESET", "earliest")

    consumer = create_consumer(
        kafka.bootstrap_servers,
        group_id,
        [kafka.detections_topic],
        auto_offset_reset=auto_offset_reset,
    )
    logger.info(
        "Consuming topic=%s group=%s",
        kafka.detections_topic,
        group_id,
    )

    states: dict[str, VideoStats] = {}
    aggregate = AggregateStats()

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    logger.error("Kafka consumer error: %s", msg.error())
                continue

            try:
                payload = json_loads(msg.value())
                process_detection_message(payload, states, aggregate)
            except Exception:  # noqa: BLE001
                logger.exception("Failed to process stats-service message; committing offset and continuing")
            finally:
                consumer.commit(msg, asynchronous=False)
    except KeyboardInterrupt:
        logger.info("Interrupted")
        return 130
    finally:
        consumer.close()


if __name__ == "__main__":
    sys.exit(main())
