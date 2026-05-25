from __future__ import annotations

import base64

import cv2
import numpy as np


def encode_frame_to_base64_jpeg(frame: np.ndarray, quality: int = 80) -> str:
    quality = max(1, min(int(quality), 100))
    ok, buffer = cv2.imencode(
        ".jpg",
        frame,
        [int(cv2.IMWRITE_JPEG_QUALITY), quality],
    )
    if not ok:
        raise ValueError("OpenCV failed to JPEG-encode frame")
    return base64.b64encode(buffer.tobytes()).decode("ascii")


def decode_base64_jpeg_to_bytes(image_base64: str) -> bytes:
    return base64.b64decode(image_base64, validate=True)


def decode_jpeg_bytes_to_frame(image_bytes: bytes) -> np.ndarray:
    array = np.frombuffer(image_bytes, dtype=np.uint8)
    frame = cv2.imdecode(array, cv2.IMREAD_COLOR)
    if frame is None:
        raise ValueError("OpenCV failed to decode JPEG frame")
    return frame


def decode_base64_jpeg_to_frame(image_base64: str) -> np.ndarray:
    return decode_jpeg_bytes_to_frame(decode_base64_jpeg_to_bytes(image_base64))

