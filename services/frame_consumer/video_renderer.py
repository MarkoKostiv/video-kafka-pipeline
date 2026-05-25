from __future__ import annotations

import hashlib
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import cv2
import numpy as np

from shared.config import FrameRenderSettings


logger = logging.getLogger("frame-consumer.renderer")


def _safe_stem(video_name: str) -> str:
    stem = Path(video_name).stem or "video"
    return "".join(ch if ch.isalnum() or ch in ("-", "_") else "_" for ch in stem)


def _color_for_track(global_track_id: str) -> tuple[int, int, int]:
    digest = hashlib.sha256(global_track_id.encode("utf-8")).digest()
    b = 80 + (digest[0] % 176)
    g = 80 + (digest[1] % 176)
    r = 80 + (digest[2] % 176)
    return int(b), int(g), int(r)


def _draw_detections(frame: np.ndarray, detections: list[dict[str, Any]]) -> None:
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
        color = _color_for_track(global_id)

        cv2.rectangle(frame, (x1, y1), (x2, y2), color, 2)
        label = f"{class_name} id={track_id} {confidence:.2f}"
        cv2.putText(
            frame,
            label,
            (x1, max(y1 - 8, 16)),
            cv2.FONT_HERSHEY_SIMPLEX,
            0.5,
            color,
            2,
            cv2.LINE_AA,
        )


def _render_header(
    frame: np.ndarray,
    *,
    video_uuid: str,
    video_name: str,
    sampled_frame_id: int,
    detection_count: int,
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


@dataclass
class RenderState:
    video_uuid: str
    video_name: str
    frames_dir: Path
    output_path: Path
    output_fps: float
    frame_ids: set[int] = field(default_factory=set)
    last_sampled_frame_id: int = -1
    finalized: bool = False


class VideoRenderManager:
    def __init__(self, settings: FrameRenderSettings) -> None:
        self._settings = settings
        self._states: dict[str, RenderState] = {}
        self._output_root = Path(settings.output_dir)
        self._output_root.mkdir(parents=True, exist_ok=True)

    @property
    def enabled(self) -> bool:
        return self._settings.enabled

    def render_frame(
        self,
        *,
        message: dict[str, Any],
        frame: np.ndarray,
        detections: list[dict[str, Any]],
    ) -> None:
        if not self._settings.enabled:
            return
        video_uuid = str(message["video_uuid"])
        video_name = str(message["video_name"])
        sampled_frame_id = int(message["sampled_frame_id"])
        state = self._get_or_create(video_uuid=video_uuid, video_name=video_name)
        if state.finalized:
            logger.warning(
                "Skipping frame render after finalization video_uuid=%s sampled_frame_id=%s",
                video_uuid,
                sampled_frame_id,
            )
            return

        annotated = frame.copy()
        _draw_detections(annotated, detections)
        _render_header(
            annotated,
            video_uuid=video_uuid,
            video_name=video_name,
            sampled_frame_id=sampled_frame_id,
            detection_count=len(detections),
        )
        frame_path = state.frames_dir / f"{sampled_frame_id:08d}.jpg"
        ok = cv2.imwrite(
            str(frame_path),
            annotated,
            [int(cv2.IMWRITE_JPEG_QUALITY), self._settings.image_quality],
        )
        if not ok:
            raise RuntimeError(f"Failed to write annotated frame: {frame_path}")
        state.frame_ids.add(sampled_frame_id)

    def mark_end_of_stream(self, message: dict[str, Any]) -> None:
        if not self._settings.enabled:
            return
        video_uuid = str(message["video_uuid"])
        video_name = str(message["video_name"])
        state = self._get_or_create(video_uuid=video_uuid, video_name=video_name)
        state.last_sampled_frame_id = int(message.get("last_sampled_frame_id", -1))
        self._finalize_video(state, reason="eos")

    def finalize_all(self) -> None:
        if not self._settings.enabled:
            return
        for state in list(self._states.values()):
            self._finalize_video(state, reason="shutdown")

    def _get_or_create(self, *, video_uuid: str, video_name: str) -> RenderState:
        state = self._states.get(video_uuid)
        if state is not None:
            return state
        safe = _safe_stem(video_name)
        video_root = self._output_root / f"{safe}__{video_uuid}"
        frames_dir = video_root / "frames"
        frames_dir.mkdir(parents=True, exist_ok=True)
        output_path = self._output_root / f"{safe}__{video_uuid}__annotated.mp4"
        state = RenderState(
            video_uuid=video_uuid,
            video_name=video_name,
            frames_dir=frames_dir,
            output_path=output_path,
            output_fps=self._settings.output_fps,
        )
        self._states[video_uuid] = state
        logger.info(
            "Created render state video_uuid=%s frames_dir=%s output=%s",
            video_uuid,
            frames_dir,
            output_path,
        )
        return state

    def _finalize_video(self, state: RenderState, *, reason: str) -> None:
        if state.finalized:
            return
        if not state.frame_ids:
            logger.warning(
                "Skipping video finalize without frames video_uuid=%s reason=%s",
                state.video_uuid,
                reason,
            )
            state.finalized = True
            self._states.pop(state.video_uuid, None)
            return

        frame_ids = sorted(state.frame_ids)
        first_frame = cv2.imread(str(state.frames_dir / f"{frame_ids[0]:08d}.jpg"))
        if first_frame is None:
            logger.error(
                "Could not read first frame for stitching video_uuid=%s reason=%s",
                state.video_uuid,
                reason,
            )
            return
        height, width = int(first_frame.shape[0]), int(first_frame.shape[1])
        writer = cv2.VideoWriter(
            str(state.output_path),
            cv2.VideoWriter_fourcc(*"mp4v"),
            max(1.0, state.output_fps),
            (width, height),
        )
        if not writer.isOpened():
            raise RuntimeError(f"Could not open output writer: {state.output_path}")

        missing_files = 0
        frames_written = 0
        try:
            for frame_id in frame_ids:
                path = state.frames_dir / f"{frame_id:08d}.jpg"
                frame = cv2.imread(str(path))
                if frame is None:
                    missing_files += 1
                    continue
                if frame.shape[0] != height or frame.shape[1] != width:
                    frame = cv2.resize(frame, (width, height))
                writer.write(frame)
                frames_written += 1
        finally:
            writer.release()

        state.finalized = True
        self._states.pop(state.video_uuid, None)
        logger.info(
            "Finalized annotated video video_uuid=%s output=%s reason=%s frames_written=%s missing_frame_files=%s last_sampled_frame_id=%s",
            state.video_uuid,
            state.output_path,
            reason,
            frames_written,
            missing_files,
            state.last_sampled_frame_id,
        )
