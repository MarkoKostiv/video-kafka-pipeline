from __future__ import annotations

import os
from dataclasses import dataclass


def getenv(name: str, default: str | None = None, *, required: bool = False) -> str:
    value = os.getenv(name, default)
    if required and not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    if value is None:
        raise RuntimeError(f"Missing environment variable: {name}")
    return value


def getenv_int(name: str, default: int) -> int:
    value = getenv(name, str(default))
    try:
        return int(value)
    except ValueError as exc:
        raise RuntimeError(f"Environment variable {name} must be an integer") from exc


def getenv_float(name: str, default: float) -> float:
    value = getenv(name, str(default))
    try:
        return float(value)
    except ValueError as exc:
        raise RuntimeError(f"Environment variable {name} must be a float") from exc


def getenv_bool(name: str, default: bool) -> bool:
    value = getenv(name, "true" if default else "false").strip().lower()
    if value in ("1", "true", "yes", "on"):
        return True
    if value in ("0", "false", "no", "off"):
        return False
    raise RuntimeError(f"Environment variable {name} must be a boolean")


@dataclass(frozen=True)
class KafkaSettings:
    bootstrap_servers: str
    raw_frames_topic: str
    detections_topic: str


@dataclass(frozen=True)
class ProducerSettings:
    video_path: str
    video_name: str
    frame_rate_out: int
    jpeg_quality: int
    target_partition: int | None
    startup_sleep_seconds: float


@dataclass(frozen=True)
class TritonSettings:
    url: str
    model_name: str
    request_timeout_seconds: float


@dataclass(frozen=True)
class FrameRenderSettings:
    enabled: bool
    output_dir: str
    output_fps: float
    image_quality: int


def kafka_settings() -> KafkaSettings:
    return KafkaSettings(
        bootstrap_servers=getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092"),
        raw_frames_topic=getenv("RAW_FRAMES_TOPIC", "raw.frames"),
        detections_topic=getenv("DETECTIONS_TOPIC", "detections.tracked"),
    )


def producer_settings() -> ProducerSettings:
    raw_partition = os.getenv("PRODUCER_PARTITION")
    if raw_partition in (None, ""):
        target_partition: int | None = None
    else:
        try:
            target_partition = int(raw_partition)
        except ValueError as exc:
            raise RuntimeError("PRODUCER_PARTITION must be an integer") from exc
        if target_partition < 0:
            raise RuntimeError("PRODUCER_PARTITION must be non-negative")
    settings = ProducerSettings(
        video_path=getenv("VIDEO_PATH", required=True),
        video_name=getenv("VIDEO_NAME", required=True),
        frame_rate_out=getenv_int("FRAME_RATE_OUT", 1),
        jpeg_quality=getenv_int("JPEG_QUALITY", 80),
        target_partition=target_partition,
        startup_sleep_seconds=getenv_float("PRODUCER_STARTUP_SLEEP_SECONDS", 0.0),
    )
    if settings.frame_rate_out <= 0:
        raise RuntimeError("FRAME_RATE_OUT must be greater than zero")
    if settings.startup_sleep_seconds < 0:
        raise RuntimeError("PRODUCER_STARTUP_SLEEP_SECONDS must be non-negative")
    return settings


def triton_settings() -> TritonSettings:
    return TritonSettings(
        url=getenv("TRITON_URL", "localhost:8001"),
        model_name=getenv("TRITON_MODEL_NAME", "yolo_tracker"),
        request_timeout_seconds=getenv_float("TRITON_REQUEST_TIMEOUT_SECONDS", 120.0),
    )


def frame_render_settings() -> FrameRenderSettings:
    settings = FrameRenderSettings(
        enabled=getenv_bool("FRAME_CONSUMER_RENDER_ENABLED", False),
        output_dir=getenv("FRAME_CONSUMER_RENDER_OUTPUT_DIR", "/app/output"),
        output_fps=getenv_float("FRAME_CONSUMER_RENDER_OUTPUT_FPS", 15.0),
        image_quality=getenv_int("FRAME_CONSUMER_RENDER_IMAGE_QUALITY", 90),
    )
    if settings.output_fps <= 0:
        raise RuntimeError("FRAME_CONSUMER_RENDER_OUTPUT_FPS must be greater than zero")
    if not 1 <= settings.image_quality <= 100:
        raise RuntimeError("FRAME_CONSUMER_RENDER_IMAGE_QUALITY must be in range 1..100")
    return settings
