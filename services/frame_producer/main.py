from __future__ import annotations

import logging
import sys
import time
import uuid

import cv2

from shared.config import kafka_settings, producer_settings
from shared.image_utils import encode_frame_to_base64_jpeg
from shared.kafka_utils import create_producer, produce_json
from shared.schemas import END_OF_STREAM_EVENT, FRAME_EVENT


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger("frame-producer")


def timestamp_ms_for_frame(original_frame_id: int, fps: float) -> int:
    if fps <= 0:
        return 0
    return int(round((original_frame_id / fps) * 1000))


def main() -> int:
    kafka = kafka_settings()
    settings = producer_settings()

    if settings.startup_sleep_seconds > 0:
        logger.info("Sleeping %.1fs before starting producer", settings.startup_sleep_seconds)
        time.sleep(settings.startup_sleep_seconds)

    video_uuid = str(uuid.uuid4())
    logger.info("Generated video_uuid=%s for video=%s", video_uuid, settings.video_name)

    capture = cv2.VideoCapture(settings.video_path)
    if not capture.isOpened():
        logger.error("Could not open video path=%s", settings.video_path)
        return 1

    fps = float(capture.get(cv2.CAP_PROP_FPS) or 0.0)
    width = int(capture.get(cv2.CAP_PROP_FRAME_WIDTH) or 0)
    height = int(capture.get(cv2.CAP_PROP_FRAME_HEIGHT) or 0)
    sample_every = max(1, int(round(fps / settings.frame_rate_out))) if fps > 0 else 1

    logger.info(
        "Publishing video=%s uuid=%s fps=%.3f size=%sx%s sample_every=%s frame(s) target_partition=%s",
        settings.video_name,
        video_uuid,
        fps,
        width,
        height,
        sample_every,
        settings.target_partition if settings.target_partition is not None else "auto",
    )

    producer = create_producer(kafka.bootstrap_servers)
    original_frame_id = 0
    sampled_frame_id = 0
    last_original_frame_id = -1

    try:
        while True:
            ok, frame = capture.read()
            if not ok:
                break

            last_original_frame_id = original_frame_id
            if original_frame_id % sample_every == 0:
                image_base64 = encode_frame_to_base64_jpeg(frame, settings.jpeg_quality)
                message = {
                    "event_type": FRAME_EVENT,
                    "video_uuid": video_uuid,
                    "video_name": settings.video_name,
                    "source_path": settings.video_path,
                    "sampled_frame_id": sampled_frame_id,
                    "original_frame_id": original_frame_id,
                    "timestamp_ms": timestamp_ms_for_frame(original_frame_id, fps),
                    "fps": fps,
                    "width": int(frame.shape[1]),
                    "height": int(frame.shape[0]),
                    "image_format": "jpeg",
                    "image_base64": image_base64,
                }
                produce_json(
                    producer,
                    kafka.raw_frames_topic,
                    video_uuid,
                    message,
                    partition=settings.target_partition,
                )
                logger.info(
                    "Published frame video_uuid=%s sampled_frame_id=%s original_frame_id=%s",
                    video_uuid,
                    sampled_frame_id,
                    original_frame_id,
                )
                sampled_frame_id += 1

            original_frame_id += 1

        eos = {
            "event_type": END_OF_STREAM_EVENT,
            "video_uuid": video_uuid,
            "video_name": settings.video_name,
            "source_path": settings.video_path,
            "last_sampled_frame_id": sampled_frame_id - 1,
            "last_original_frame_id": last_original_frame_id,
            "timestamp_ms": timestamp_ms_for_frame(max(last_original_frame_id, 0), fps),
        }
        produce_json(
            producer,
            kafka.raw_frames_topic,
            video_uuid,
            eos,
            partition=settings.target_partition,
        )
        producer.flush()
        logger.info(
            "Published end_of_stream video_uuid=%s sampled_frames=%s",
            video_uuid,
            sampled_frame_id,
        )
        return 0
    except KeyboardInterrupt:
        logger.info("Interrupted")
        return 130
    finally:
        capture.release()
        producer.flush(10)


if __name__ == "__main__":
    sys.exit(main())
