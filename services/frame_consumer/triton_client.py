from __future__ import annotations

import base64
import hashlib
import io
import json
import logging
import time
from typing import Any

import numpy as np
import tritonclient.http as httpclient
from tritonclient.http import InferInput, InferRequestedOutput


logger = logging.getLogger(__name__)
MAX_METADATA_BYTES = 4096
CLASS_NAMES_BY_ID = {
    0: "person",
    2: "car",
    3: "motorcycle",
    5: "bus",
    7: "truck",
}


def video_uuid_to_sequence_id(video_uuid: str) -> int:
    # Keep sequence IDs positive and non-zero for Triton sequence routing.
    return int(hashlib.sha256(video_uuid.encode("utf-8")).hexdigest()[:15], 16) + 1


class TritonSequenceClient:
    def __init__(self, url: str, model_name: str, request_timeout_seconds: float = 120.0) -> None:
        self.url = url
        self.model_name = model_name
        self.request_timeout_seconds = request_timeout_seconds
        self.client = httpclient.InferenceServerClient(url=url, verbose=False)

    def wait_until_ready(self, timeout_seconds: float = 180.0) -> None:
        deadline = time.monotonic() + timeout_seconds
        last_error: Exception | None = None
        while time.monotonic() < deadline:
            try:
                if self.client.is_server_ready() and self.client.is_model_ready(self.model_name):
                    logger.info("Triton model is ready url=%s model=%s", self.url, self.model_name)
                    return
            except Exception as exc:  # noqa: BLE001
                last_error = exc
            time.sleep(2)
        raise TimeoutError(f"Triton model {self.model_name!r} was not ready") from last_error

    def infer(
        self,
        *,
        frame: np.ndarray | None,
        metadata: dict[str, Any],
        sequence_id: int,
        sequence_start: bool,
        sequence_end: bool,
        expect_output: bool = True,
    ) -> dict[str, Any]:
        if frame is None:
            if not sequence_end:
                raise ValueError("frame is required unless sequence_end=True")
            frame_array = np.zeros((1, 1, 3), dtype=np.uint8)
        else:
            frame_array = np.ascontiguousarray(frame, dtype=np.uint8)
            if frame_array.ndim != 3 or frame_array.shape[2] != 3:
                raise ValueError("frame must have shape HxWx3")
        frame_buffer = io.BytesIO()
        np.save(frame_buffer, frame_array, allow_pickle=False)
        frame_payload = base64.b64encode(frame_buffer.getvalue()).decode("ascii")
        frame_input_array = np.zeros((1, 1), dtype=np.uint8)

        metadata_json = json.dumps(metadata, separators=(",", ":"))
        metadata_bytes = metadata_json.encode("utf-8")
        if len(metadata_bytes) > MAX_METADATA_BYTES:
            raise ValueError(f"METADATA_JSON exceeds {MAX_METADATA_BYTES} bytes")
        metadata_array = np.zeros((1, MAX_METADATA_BYTES), dtype=np.uint8)
        metadata_array[0, : len(metadata_bytes)] = np.frombuffer(metadata_bytes, dtype=np.uint8)

        frame_input = InferInput("FRAME", frame_input_array.shape, "UINT8")
        frame_input.set_data_from_numpy(frame_input_array)

        metadata_input = InferInput("METADATA_JSON", metadata_array.shape, "UINT8")
        metadata_input.set_data_from_numpy(metadata_array)

        detections_output = InferRequestedOutput("DETECTIONS", binary_data=False)
        count_output = InferRequestedOutput("DETECTION_COUNT", binary_data=False)
        result = self.client.infer(
            model_name=self.model_name,
            inputs=[frame_input, metadata_input],
            outputs=[detections_output, count_output],
            sequence_id=sequence_id,
            sequence_start=sequence_start,
            sequence_end=sequence_end,
            parameters={
                "metadata_json": metadata_json,
                "frame_npy_base64": frame_payload,
            },
            timeout=int(self.request_timeout_seconds),
        )
        if not expect_output:
            return {}

        output_array = result.as_numpy("DETECTIONS")
        if output_array is None:
            raise RuntimeError("Triton response did not include DETECTIONS")
        count_array = result.as_numpy("DETECTION_COUNT")
        if count_array is None or count_array.size == 0:
            raise RuntimeError("Triton response did not include DETECTION_COUNT")
        detection_count = int(count_array.reshape(-1)[0])
        detections_array = np.asarray(output_array, dtype=np.float32).reshape(-1, 7)[:detection_count]
        detections = []
        video_uuid = str(metadata["video_uuid"])
        for row in detections_array:
            track_id = int(row[0])
            class_id = int(row[1])
            class_name = CLASS_NAMES_BY_ID.get(class_id, str(class_id))
            detections.append(
                {
                    "track_id": track_id,
                    "global_track_id": f"{video_uuid}:{class_name}:{track_id}",
                    "class_name": class_name,
                    "confidence": round(float(row[2]), 4),
                    "bbox_xyxy": [round(float(value), 2) for value in row[3:7]],
                }
            )
        return {
            "video_uuid": video_uuid,
            "video_name": metadata["video_name"],
            "sampled_frame_id": metadata["sampled_frame_id"],
            "original_frame_id": metadata["original_frame_id"],
            "timestamp_ms": metadata["timestamp_ms"],
            "detections": detections,
        }
