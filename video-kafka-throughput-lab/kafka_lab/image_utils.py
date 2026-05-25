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
