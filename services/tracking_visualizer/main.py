from __future__ import annotations

import hashlib
import logging
import os
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import cv2
from confluent_kafka import KafkaError

from shared.config import kafka_settings
from shared.image_utils import decode_base64_jpeg_to_bytes, decode_jpeg_bytes_to_frame
from shared.kafka_utils import create_consumer, json_loads
from shared.schemas import (
    END_OF_STREAM_EVENT,
    FRAME_EVENT,
    validate_end_of_stream,
    validate_raw_frame,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger("tracking-visualizer")


def getenv(name: str, default: str) -> str:
    value = os.getenv(name, default)
    if not value:
        return default
    return value


def getenv_float(name: str, default: float) -> float:
    value = getenv(name, str(default))
    try:
        return float(value)
    except ValueError as exc:
        raise RuntimeError(f"{name} must be a float") from exc


@dataclass
class VideoState:
    video_uuid: str
    video_name: str
    width: int
    height: int
    output_fps: float
    output_path: Path
    writer: cv2.VideoWriter | None
    # Keep frames as encoded JPEG bytes (~50 KB each at 1080p) rather than
    # decoded RGB ndarrays (~6 MB each) so back-pressure from slow detections
    # does not blow up memory. Decoded just-in-time before drawing+writing.
    frame_buffer: dict[int, bytes] = field(default_factory=dict)
    detection_buffer: dict[int, list[dict[str, Any]]] = field(default_factory=dict)
    next_frame_to_write: int = 0
    eos_seen: bool = False
    last_sampled_frame_id: int = -1
    frames_written: int = 0
    highest_frame_id: int = -1
    highest_detection_frame_id: int = -1
    missing_frames: int = 0
    missing_detections: int = 0
    last_update_monotonic: float = field(default_factory=time.monotonic)


def color_for_track(global_track_id: str) -> tuple[int, int, int]:
    digest = hashlib.sha256(global_track_id.encode("utf-8")).digest()
    b = 80 + (digest[0] % 176)
    g = 80 + (digest[1] % 176)
    r = 80 + (digest[2] % 176)
    return int(b), int(g), int(r)


def draw_detections(frame, detections: list[dict[str, Any]]) -> None:
    for item in detections:
        bbox = item.get("bbox_xyxy", [])
        if not isinstance(bbox, list) or len(bbox) != 4:
            continue

        try:
            x1, y1, x2, y2 = [int(round(float(value))) for value in bbox]
        except (TypeError, ValueError):
            continue

        global_id = str(item.get("global_track_id", "unknown"))
        class_name = str(item.get("class_name", "unknown"))
        track_id = str(item.get("track_id", "?"))
        confidence = float(item.get("confidence", 0.0))

        color = color_for_track(global_id)
        cv2.rectangle(frame, (x1, y1), (x2, y2), color, 2)

        label = f"{class_name} id={track_id} {confidence:.2f}"
        label_y = max(y1 - 8, 16)
        cv2.putText(
            frame,
            label,
            (x1, label_y),
            cv2.FONT_HERSHEY_SIMPLEX,
            0.5,
            color,
            2,
            cv2.LINE_AA,
        )


def build_output_path(output_dir: Path, video_name: str, video_uuid: str) -> Path:
    stem = Path(video_name).stem or "video"
    safe_stem = "".join(ch if ch.isalnum() or ch in ("-", "_") else "_" for ch in stem)
    return output_dir / f"{safe_stem}__{video_uuid}__annotated.mp4"


def create_video_writer(
    path: Path, width: int, height: int, output_fps: float
) -> cv2.VideoWriter:
    path.parent.mkdir(parents=True, exist_ok=True)
    writer = cv2.VideoWriter(
        str(path),
        cv2.VideoWriter_fourcc(*"mp4v"),
        max(1.0, output_fps),
        (width, height),
    )
    if not writer.isOpened():
        raise RuntimeError(f"Could not open output writer: {path}")
    return writer


def render_header(
    frame, video_uuid: str, video_name: str, sampled_frame_id: int, detection_count: int
) -> None:
    header = (
        f"video={video_name} "
        f"uuid={video_uuid[:8]} "
        f"sampled_frame={sampled_frame_id} "
        f"dets={detection_count}"
    )
    cv2.rectangle(frame, (8, 8), (min(frame.shape[1] - 8, 780), 38), (0, 0, 0), -1)
    cv2.putText(
        frame,
        header,
        (14, 30),
        cv2.FONT_HERSHEY_SIMPLEX,
        0.55,
        (255, 255, 255),
        1,
        cv2.LINE_AA,
    )


def maybe_finalize(
    state: VideoState, states: dict[str, VideoState], *, allow_gaps: bool = False
) -> None:
    if state.writer is None:
        if state.eos_seen and allow_gaps:
            logger.warning(
                "Dropping state without writer video_uuid=%s (no frames rendered)",
                state.video_uuid,
            )
            states.pop(state.video_uuid, None)
        return
    if not state.eos_seen:
        return
    if state.last_sampled_frame_id < 0:
        return
    if allow_gaps:
        while state.next_frame_to_write <= state.last_sampled_frame_id:
            if state.next_frame_to_write in state.frame_buffer:
                break
            state.missing_frames += 1
            state.next_frame_to_write += 1
    elif state.next_frame_to_write <= state.last_sampled_frame_id:
        return
    if state.next_frame_to_write <= state.last_sampled_frame_id:
        return

    logger.info(
        "Finalized video_uuid=%s output=%s frames_written=%s missing_frames=%s missing_detections=%s",
        state.video_uuid,
        state.output_path,
        state.frames_written,
        state.missing_frames,
        state.missing_detections,
    )
    state.writer.release()
    states.pop(state.video_uuid, None)


def try_write_ready_frames(
    state: VideoState, states: dict[str, VideoState], *, allow_gaps: bool = False
) -> None:
    if state.writer is None:
        maybe_finalize(state, states, allow_gaps=allow_gaps)
        return

    while state.next_frame_to_write in state.frame_buffer:
        sampled_frame_id = state.next_frame_to_write
        frame_bytes = state.frame_buffer.pop(sampled_frame_id)
        detections = state.detection_buffer.pop(sampled_frame_id, None)
        if detections is None:
            detections_are_missing = (
                sampled_frame_id <= state.highest_detection_frame_id or allow_gaps
            )
            if not state.eos_seen or not detections_are_missing:
                state.frame_buffer[sampled_frame_id] = frame_bytes
                break
            state.missing_detections += 1
            detections = []

        frame = decode_jpeg_bytes_to_frame(frame_bytes)
        draw_detections(frame, detections)
        render_header(
            frame, state.video_uuid, state.video_name, sampled_frame_id, len(detections)
        )
        state.writer.write(frame)
        state.frames_written += 1
        state.next_frame_to_write += 1

        while (
            state.eos_seen
            and state.next_frame_to_write <= state.last_sampled_frame_id
            and state.next_frame_to_write not in state.frame_buffer
            and (state.next_frame_to_write <= state.highest_frame_id or allow_gaps)
        ):
            state.missing_frames += 1
            state.next_frame_to_write += 1

    maybe_finalize(state, states, allow_gaps=allow_gaps)


def finalize_idle_videos(states: dict[str, VideoState], idle_seconds: float) -> None:
    now = time.monotonic()
    for state in list(states.values()):
        if now - state.last_update_monotonic < idle_seconds:
            continue
        if not state.eos_seen:
            # Force-close stale videos even without explicit EOS so one broken
            # producer/consumer path does not leave state and writers hanging.
            state.eos_seen = True
            state.last_sampled_frame_id = max(
                state.last_sampled_frame_id,
                state.highest_frame_id,
                state.highest_detection_frame_id,
            )
            logger.warning(
                "Force-finalizing stale video_uuid=%s without EOS after %.1fs idle",
                state.video_uuid,
                idle_seconds,
            )
        else:
            logger.warning(
                "Finalizing idle video_uuid=%s with available frames/detections after %.1fs idle",
                state.video_uuid,
                idle_seconds,
            )
        try_write_ready_frames(state, states, allow_gaps=True)


def get_or_create_state(
    states: dict[str, VideoState],
    *,
    video_uuid: str,
    video_name: str,
    width: int,
    height: int,
    output_dir: Path,
    output_fps: float,
) -> VideoState:
    state = states.get(video_uuid)
    if state is not None:
        if state.writer is None:
            state.width = width
            state.height = height
            state.output_fps = output_fps
            state.writer = create_video_writer(
                state.output_path, width, height, output_fps
            )
            logger.info(
                "Created deferred writer video_uuid=%s output=%s",
                video_uuid,
                state.output_path,
            )
        return state

    output_path = build_output_path(output_dir, video_name, video_uuid)
    writer = create_video_writer(output_path, width, height, output_fps)
    state = VideoState(
        video_uuid=video_uuid,
        video_name=video_name,
        width=width,
        height=height,
        output_fps=output_fps,
        output_path=output_path,
        writer=writer,
    )
    states[video_uuid] = state
    logger.info("Created writer video_uuid=%s output=%s", video_uuid, output_path)
    return state


def process_raw_frame(
    *,
    message: dict[str, Any],
    states: dict[str, VideoState],
    output_dir: Path,
    output_fps: float,
) -> None:
    validate_raw_frame(message)
    video_uuid = str(message["video_uuid"])
    video_name = str(message["video_name"])
    sampled_frame_id = int(message["sampled_frame_id"])
    width = int(message["width"])
    height = int(message["height"])

    state = get_or_create_state(
        states,
        video_uuid=video_uuid,
        video_name=video_name,
        width=width,
        height=height,
        output_dir=output_dir,
        output_fps=output_fps,
    )

    state.frame_buffer[sampled_frame_id] = decode_base64_jpeg_to_bytes(
        str(message["image_base64"])
    )
    state.highest_frame_id = max(state.highest_frame_id, sampled_frame_id)
    state.last_update_monotonic = time.monotonic()
    try_write_ready_frames(state, states)


def validate_detection_message(message: dict[str, Any]) -> None:
    required = ["video_uuid", "video_name", "sampled_frame_id", "detections"]
    missing = [field for field in required if field not in message]
    if missing:
        raise ValueError(
            f"Missing required fields in detection message: {', '.join(missing)}"
        )
    if not isinstance(message["detections"], list):
        raise ValueError("detections must be a list")


def process_detection(
    *,
    message: dict[str, Any],
    states: dict[str, VideoState],
    output_dir: Path,
    output_fps: float,
) -> None:
    validate_detection_message(message)
    video_uuid = str(message["video_uuid"])
    video_name = str(message["video_name"])
    sampled_frame_id = int(message["sampled_frame_id"])
    detections = list(message["detections"])

    state = states.get(video_uuid)
    if state is None:
        # We do not know frame dimensions yet, so wait until raw frame arrives.
        placeholder_path = build_output_path(output_dir, video_name, video_uuid)
        logger.debug(
            "Detection arrived before frame video_uuid=%s sampled_frame_id=%s output=%s",
            video_uuid,
            sampled_frame_id,
            placeholder_path,
        )
        # Keep a lightweight in-memory placeholder state with deferred writer creation.
        state = VideoState(
            video_uuid=video_uuid,
            video_name=video_name,
            width=0,
            height=0,
            output_fps=output_fps,
            output_path=placeholder_path,
            writer=None,  # type: ignore[arg-type]
        )
        states[video_uuid] = state

    state.detection_buffer[sampled_frame_id] = detections
    state.highest_detection_frame_id = max(
        state.highest_detection_frame_id, sampled_frame_id
    )
    state.last_update_monotonic = time.monotonic()

    # If frame arrived first, state.writer exists and we can drain.
    if state.writer is not None:
        try_write_ready_frames(state, states)


def process_eos(
    message: dict[str, Any],
    states: dict[str, VideoState],
    *,
    output_dir: Path,
    output_fps: float,
) -> None:
    validate_end_of_stream(message)
    video_uuid = str(message["video_uuid"])
    video_name = str(message["video_name"])
    last_sampled = int(message["last_sampled_frame_id"])

    state = states.get(video_uuid)
    if state is None:
        state = VideoState(
            video_uuid=video_uuid,
            video_name=video_name,
            width=0,
            height=0,
            output_fps=output_fps,
            output_path=build_output_path(output_dir, video_name, video_uuid),
            writer=None,
        )
        states[video_uuid] = state

    state.eos_seen = True
    state.last_sampled_frame_id = last_sampled
    state.last_update_monotonic = time.monotonic()
    if state.writer is not None:
        try_write_ready_frames(state, states)


def main() -> int:
    kafka = kafka_settings()
    output_dir = Path(getenv("VISUALIZER_OUTPUT_DIR", "/app/output"))
    output_fps = getenv_float("VISUALIZER_OUTPUT_FPS", 1.0)
    finalize_idle_seconds = getenv_float("VISUALIZER_FINALIZE_IDLE_SECONDS", 60.0)
    group_id = getenv("VISUALIZER_GROUP_ID", "tracking-visualizer-group")
    auto_offset_reset = getenv("VISUALIZER_AUTO_OFFSET_RESET", "latest")

    consumer = create_consumer(
        kafka.bootstrap_servers,
        group_id,
        [kafka.raw_frames_topic, kafka.detections_topic],
        auto_offset_reset=auto_offset_reset,
    )
    logger.info(
        "Consuming topics=[%s,%s] group=%s output_dir=%s finalize_idle_seconds=%.1f",
        kafka.raw_frames_topic,
        kafka.detections_topic,
        group_id,
        output_dir,
        finalize_idle_seconds,
    )

    states: dict[str, VideoState] = {}

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                finalize_idle_videos(states, finalize_idle_seconds)
                continue
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    logger.error("Kafka consumer error: %s", msg.error())
                continue

            try:
                topic = msg.topic()
                payload = json_loads(msg.value())

                if topic == kafka.raw_frames_topic:
                    event_type = payload.get("event_type")
                    if event_type == FRAME_EVENT:
                        process_raw_frame(
                            message=payload,
                            states=states,
                            output_dir=output_dir,
                            output_fps=output_fps,
                        )
                    elif event_type == END_OF_STREAM_EVENT:
                        process_eos(
                            payload,
                            states,
                            output_dir=output_dir,
                            output_fps=output_fps,
                        )
                    else:
                        logger.warning("Skipping unknown raw event_type=%r", event_type)

                elif topic == kafka.detections_topic:
                    process_detection(
                        message=payload,
                        states=states,
                        output_dir=output_dir,
                        output_fps=output_fps,
                    )
                else:
                    logger.warning("Skipping message from unknown topic=%s", topic)
            except Exception:  # noqa: BLE001
                logger.exception(
                    "Failed to process visualizer message; committing offset and continuing"
                )
            finally:
                consumer.commit(msg, asynchronous=False)
    except KeyboardInterrupt:
        logger.info("Interrupted")
        return 130
    finally:
        for state in list(states.values()):
            if state.writer is not None:
                state.writer.release()
        consumer.close()


if __name__ == "__main__":
    sys.exit(main())
