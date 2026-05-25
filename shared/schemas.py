from __future__ import annotations

from typing import Any


FRAME_EVENT = "frame"
END_OF_STREAM_EVENT = "end_of_stream"


def require_fields(message: dict[str, Any], fields: list[str]) -> None:
    missing = [field for field in fields if field not in message]
    if missing:
        raise ValueError(f"Missing required fields: {', '.join(missing)}")


def validate_raw_frame(message: dict[str, Any]) -> None:
    require_fields(
        message,
        [
            "event_type",
            "video_uuid",
            "video_name",
            "source_path",
            "sampled_frame_id",
            "original_frame_id",
            "timestamp_ms",
            "fps",
            "width",
            "height",
            "image_format",
            "image_base64",
        ],
    )
    if message["event_type"] != FRAME_EVENT:
        raise ValueError(f"Expected event_type={FRAME_EVENT}")
    if message["image_format"] != "jpeg":
        raise ValueError("Only image_format=jpeg is supported")


def validate_end_of_stream(message: dict[str, Any]) -> None:
    require_fields(
        message,
        [
            "event_type",
            "video_uuid",
            "video_name",
            "source_path",
            "last_sampled_frame_id",
            "last_original_frame_id",
            "timestamp_ms",
        ],
    )
    if message["event_type"] != END_OF_STREAM_EVENT:
        raise ValueError(f"Expected event_type={END_OF_STREAM_EVENT}")


def detection_event(
    *,
    video_uuid: str,
    video_name: str,
    sampled_frame_id: int,
    original_frame_id: int,
    timestamp_ms: int,
    detections: list[dict[str, Any]],
) -> dict[str, Any]:
    return {
        "video_uuid": video_uuid,
        "video_name": video_name,
        "sampled_frame_id": sampled_frame_id,
        "original_frame_id": original_frame_id,
        "timestamp_ms": timestamp_ms,
        "detections": detections,
    }

