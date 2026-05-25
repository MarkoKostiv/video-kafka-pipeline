from __future__ import annotations

import ast
import base64
import hashlib
import io
import json
import logging
import os
import random
from types import SimpleNamespace
from typing import Any

import numpy as np
import torch
import triton_python_backend_utils as pb_utils
from ultralytics import YOLO
from ultralytics.trackers.basetrack import BaseTrack
from ultralytics.trackers.byte_tracker import BYTETracker


logger = logging.getLogger("triton-yolo-tracker")
logging.basicConfig(level=logging.INFO)
MAX_DETECTIONS = 100


def video_uuid_to_sequence_id(video_uuid: str) -> int:
    # Keep sequence IDs positive and non-zero for Triton sequence routing.
    return int(hashlib.sha256(video_uuid.encode("utf-8")).hexdigest()[:15], 16) + 1


def env_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


class TritonPythonModel:
    def initialize(self, args: dict[str, Any]) -> None:
        tracker_type = os.getenv("TRACKER_TYPE", "bytetrack").lower()
        if tracker_type != "bytetrack":
            raise RuntimeError("This model expects TRACKER_TYPE=bytetrack")

        self.deterministic_mode = env_bool("DETERMINISTIC_MODE", True)
        self.deterministic_seed = int(os.getenv("DETERMINISTIC_SEED", "12345"))
        self.deterministic_strict = env_bool("DETERMINISTIC_STRICT", True)
        self.yolo_device = os.getenv("YOLO_DEVICE", "cpu")
        self._configure_determinism()

        self.conf_threshold = float(os.getenv("YOLO_CONFIDENCE_THRESHOLD", "0.5"))
        self.model_path = os.getenv("YOLO_MODEL_PATH", "yolov8n.pt")
        self.model = YOLO(self.model_path)
        self.model.to(self.yolo_device)
        self.names = (
            self.model.names
            if isinstance(self.model.names, dict)
            else {index: name for index, name in enumerate(self.model.names)}
        )
        self.allowed_class_names = {"person", "car", "bus", "truck", "motorcycle"}
        self.allowed_class_ids = [
            int(class_id)
            for class_id, class_name in self.names.items()
            if class_name in self.allowed_class_names
        ]
        self.tracker_args = SimpleNamespace(
            tracker_type="bytetrack",
            track_high_thresh=self.conf_threshold,
            track_low_thresh=0.1,
            new_track_thresh=self.conf_threshold,
            track_buffer=30,
            match_thresh=0.8,
            fuse_score=True,
        )
        self.trackers_by_sequence_id: dict[int, dict[str, Any]] = {}
        logger.info(
            "Loaded YOLO model=%s conf_threshold=%.3f classes=%s deterministic=%s strict=%s seed=%s device=%s",
            self.model_path,
            self.conf_threshold,
            sorted(self.allowed_class_names),
            self.deterministic_mode,
            self.deterministic_strict,
            self.deterministic_seed,
            self.yolo_device,
        )

    def execute(self, requests: list[Any]) -> list[Any]:
        responses = []
        for request in requests:
            try:
                metadata = self._metadata_from_request(request)
                sequence_id, sequence_start, sequence_end = self._sequence_context_from_request(request, metadata)
                if sequence_start or sequence_id not in self.trackers_by_sequence_id:
                    self.trackers_by_sequence_id[sequence_id] = self._new_sequence_state()

                if metadata.get("event_type") == "end_of_stream" or sequence_end:
                    output = self._end_sequence(sequence_id, metadata)
                else:
                    output = self._track_frame(request, sequence_id, metadata)

                responses.append(self._response(output))
            except Exception as exc:  # noqa: BLE001
                logger.exception("Inference request failed")
                responses.append(pb_utils.InferenceResponse(error=pb_utils.TritonError(str(exc))))
        return responses

    def finalize(self) -> None:
        self.trackers_by_sequence_id.clear()
        logger.info("Cleared YOLO tracker state")

    def _configure_determinism(self) -> None:
        if not self.deterministic_mode:
            return

        random.seed(self.deterministic_seed)
        np.random.seed(self.deterministic_seed)
        torch.manual_seed(self.deterministic_seed)
        if torch.cuda.is_available():
            torch.cuda.manual_seed_all(self.deterministic_seed)
        torch.use_deterministic_algorithms(True, warn_only=not self.deterministic_strict)

        if hasattr(torch.backends, "cudnn"):
            torch.backends.cudnn.deterministic = True
            torch.backends.cudnn.benchmark = False
        if hasattr(torch.backends, "mkldnn"):
            torch.backends.mkldnn.enabled = False

        # Keep math kernels deterministic in shared containers.
        os.environ.setdefault("OMP_NUM_THREADS", "1")
        os.environ.setdefault("MKL_NUM_THREADS", "1")
        torch.set_num_threads(1)
        torch.set_num_interop_threads(1)

    def _sequence_context_from_request(self, request: Any, metadata: dict[str, Any]) -> tuple[int, bool, bool]:
        metadata_sequence_id = int(metadata.get("sequence_id") or video_uuid_to_sequence_id(metadata["video_uuid"]))
        sequence_id = metadata_sequence_id

        correlation_attr = getattr(request, "correlation_id", None)
        try:
            correlation_id = correlation_attr() if callable(correlation_attr) else correlation_attr
            if correlation_id is not None:
                sequence_id = int(correlation_id)
        except Exception:  # noqa: BLE001
            logger.debug("Could not read request correlation_id; falling back to metadata")

        flags_value = 0
        flags_attr = getattr(request, "flags", None)
        try:
            flags_value = int(flags_attr() if callable(flags_attr) else (flags_attr or 0))
        except Exception:  # noqa: BLE001
            flags_value = 0

        flag_start = bool(flags_value & pb_utils.TRITONSERVER_REQUEST_FLAG_SEQUENCE_START)
        flag_end = bool(flags_value & pb_utils.TRITONSERVER_REQUEST_FLAG_SEQUENCE_END)
        sequence_start = flag_start or bool(metadata.get("sequence_start"))
        sequence_end = flag_end or bool(metadata.get("sequence_end"))

        if sequence_id != metadata_sequence_id:
            logger.warning(
                "Sequence id mismatch: triton=%s metadata=%s video_uuid=%s",
                sequence_id,
                metadata_sequence_id,
                metadata.get("video_uuid"),
            )
        return sequence_id, sequence_start, sequence_end

    def _new_sequence_state(self) -> dict[str, Any]:
        previous_count = getattr(BaseTrack, "_count", 0)
        tracker = BYTETracker(args=self.tracker_args)

        # BYTETracker resets Ultralytics' global BaseTrack counter when it is
        # constructed. Restore the counter so concurrently active sequences do
        # not accidentally reuse raw tracker IDs.
        if getattr(BaseTrack, "_count", 0) < previous_count:
            BaseTrack._count = previous_count

        return {
            "tracker": tracker,
            "raw_to_local_track_id": {},
            "next_local_track_id": 1,
        }

    def _metadata_from_request(self, request: Any) -> dict[str, Any]:
        parameters = self._request_parameters(request)
        metadata_value = parameters.get("metadata_json")
        if isinstance(metadata_value, dict):
            metadata_value = (
                metadata_value.get("string_value")
                or metadata_value.get("string_param")
                or metadata_value.get("value")
            )
        if isinstance(metadata_value, str) and metadata_value:
            metadata = json.loads(metadata_value)
            if not isinstance(metadata, dict):
                raise ValueError("metadata_json parameter must decode to a JSON object")
            return metadata

        tensor = pb_utils.get_input_tensor_by_name(request, "METADATA_JSON")
        metadata_bytes = tensor.as_numpy().reshape(-1).astype(np.uint8, copy=False).tobytes().rstrip(b"\x00")
        metadata_text = metadata_bytes.decode("utf-8")
        try:
            metadata = json.loads(metadata_text)
        except json.JSONDecodeError:
            logger.warning("METADATA_JSON was not strict JSON; preview=%r", metadata_text[:160])
            metadata = ast.literal_eval(metadata_text)
        if not isinstance(metadata, dict):
            raise ValueError("METADATA_JSON must decode to a JSON object")
        return metadata

    def _frame_from_request(self, request: Any, metadata: dict[str, Any]) -> np.ndarray:
        frame_value = self._request_parameters(request).get("frame_npy_base64")
        if isinstance(frame_value, dict):
            frame_value = (
                frame_value.get("string_value")
                or frame_value.get("string_param")
                or frame_value.get("value")
            )
        if not isinstance(frame_value, str) or not frame_value:
            raise ValueError("frame_npy_base64 request parameter is required")
        frame_bytes = base64.b64decode(frame_value)
        raw_frame = np.load(io.BytesIO(frame_bytes), allow_pickle=False)
        logger.info(
            "Raw FRAME tensor dtype=%s shape=%s min=%.2f max=%.2f mean=%.2f",
            raw_frame.dtype,
            raw_frame.shape,
            float(raw_frame.min()),
            float(raw_frame.max()),
            float(raw_frame.mean()),
        )
        frame = raw_frame.astype(np.uint8, copy=False)
        if frame.ndim != 3 or frame.shape[2] != 3:
            raise ValueError("FRAME must have shape HxWx3")
        return frame

    def _request_parameters(self, request: Any) -> dict[str, Any]:
        parameters_method = getattr(request, "parameters", None)
        if not callable(parameters_method):
            return {}
        raw_parameters = parameters_method()
        if not raw_parameters:
            return {}
        parameters = json.loads(raw_parameters)
        return parameters if isinstance(parameters, dict) else {}

    def _track_frame(self, request: Any, sequence_id: int, metadata: dict[str, Any]) -> np.ndarray:
        frame = self._frame_from_request(request, metadata)

        results = self.model.predict(
            source=frame,
            device=self.yolo_device,
            conf=self.conf_threshold,
            classes=self.allowed_class_ids or None,
            half=False,
            augment=False,
            verbose=False,
        )
        result = results[0]
        state = self.trackers_by_sequence_id[sequence_id]
        raw_box_count = 0 if result.boxes is None else len(result.boxes)

        if result.boxes is None or len(result.boxes) == 0:
            tracks = np.empty((0, 8), dtype=np.float32)
        else:
            tracks = state["tracker"].update(result.boxes.cpu().numpy(), frame)

        detections = self._detections_from_tracks(tracks, state)
        logger.info(
            "Frame debug video_uuid=%s sampled_frame_id=%s frame_shape=%s frame_min=%s frame_max=%s frame_mean=%.2f raw_boxes=%s tracks=%s detections=%s",
            metadata.get("video_uuid"),
            metadata.get("sampled_frame_id"),
            frame.shape,
            int(frame.min()),
            int(frame.max()),
            float(frame.mean()),
            raw_box_count,
            0 if tracks is None else len(tracks),
            len(detections),
        )
        return detections

    def _detections_from_tracks(
        self,
        tracks: np.ndarray,
        state: dict[str, Any],
    ) -> np.ndarray:
        detections: list[list[float]] = []
        if tracks is None or len(tracks) == 0:
            return np.empty((0, 7), dtype=np.float32)

        rows = [row for row in np.asarray(tracks) if row.shape[0] >= 7]
        rows.sort(key=lambda row: (int(row[6]), int(row[4]), float(row[0]), float(row[1]), float(row[2]), float(row[3])))

        for row in rows:
            if row.shape[0] < 7:
                continue
            x1, y1, x2, y2 = [float(value) for value in row[:4]]
            raw_track_id = int(row[4])
            confidence = float(row[5])
            class_id = int(row[6])
            class_name = self.names.get(class_id, str(class_id))
            if confidence <= self.conf_threshold or class_name not in self.allowed_class_names:
                continue

            local_track_id = self._local_track_id(state, raw_track_id)
            detections.append([local_track_id, class_id, confidence, x1, y1, x2, y2])
        if not detections:
            return np.empty((0, 7), dtype=np.float32)
        detections.sort(key=lambda row: (int(row[1]), int(row[0]), float(row[3]), float(row[4]), float(row[5]), float(row[6])))
        return np.asarray(detections, dtype=np.float32)

    def _local_track_id(self, state: dict[str, Any], raw_track_id: int) -> int:
        track_map = state["raw_to_local_track_id"]
        if raw_track_id not in track_map:
            track_map[raw_track_id] = state["next_local_track_id"]
            state["next_local_track_id"] += 1
        return int(track_map[raw_track_id])

    def _end_sequence(self, sequence_id: int, metadata: dict[str, Any]) -> np.ndarray:
        self.trackers_by_sequence_id.pop(sequence_id, None)
        return np.empty((0, 7), dtype=np.float32)

    def _response(self, detections: np.ndarray) -> Any:
        detections_array = np.asarray(detections, dtype=np.float32).reshape(-1, 7)
        detection_count = min(len(detections_array), MAX_DETECTIONS)
        padded_detections = np.zeros((MAX_DETECTIONS, 7), dtype=np.float32)
        if detection_count:
            padded_detections[:detection_count] = detections_array[:detection_count]
        detections_tensor = pb_utils.Tensor(
            "DETECTIONS",
            padded_detections,
        )
        count_tensor = pb_utils.Tensor(
            "DETECTION_COUNT",
            np.array([detection_count], dtype=np.int32),
        )
        return pb_utils.InferenceResponse(output_tensors=[detections_tensor, count_tensor])
