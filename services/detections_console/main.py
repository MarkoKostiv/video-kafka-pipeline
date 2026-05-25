from __future__ import annotations

import logging
import sys

from confluent_kafka import KafkaError

from shared.config import kafka_settings
from shared.kafka_utils import create_consumer, json_loads


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger("detections-console")


def format_detection(detection: dict) -> str:
    bbox = detection.get("bbox_xyxy", [])
    bbox_text = "[" + ",".join(str(int(round(float(value)))) for value in bbox) + "]"
    confidence = float(detection.get("confidence", 0.0))
    return (
        "{"
        f"id={detection.get('track_id')}, "
        f"global_id={detection.get('global_track_id')}, "
        f"class={detection.get('class_name')}, "
        f"conf={confidence:.2f}, "
        f"bbox={bbox_text}"
        "}"
    )


def print_message(message: dict) -> None:
    detections = message.get("detections", [])
    if detections:
        formatted = ",\n  ".join(format_detection(item) for item in detections)
        detections_text = f"[\n  {formatted}\n]"
    else:
        detections_text = "[]"
    print(
        f"video_uuid={message.get('video_uuid')} "
        f"video={message.get('video_name')} "
        f"frame={message.get('sampled_frame_id')} "
        f"detections={detections_text}",
        flush=True,
    )


def main() -> int:
    kafka = kafka_settings()
    consumer = create_consumer(
        kafka.bootstrap_servers,
        "detections-console-group",
        [kafka.detections_topic],
    )
    logger.info("Consuming topic=%s group=detections-console-group", kafka.detections_topic)

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
                print_message(json_loads(msg.value()))
            except Exception:  # noqa: BLE001
                logger.exception("Failed to print detection message; committing offset and continuing")
            finally:
                consumer.commit(msg, asynchronous=False)
    except KeyboardInterrupt:
        logger.info("Interrupted")
        return 130
    finally:
        consumer.close()


if __name__ == "__main__":
    sys.exit(main())

