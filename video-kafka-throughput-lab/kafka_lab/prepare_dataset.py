from __future__ import annotations

import argparse
import csv
from pathlib import Path

import cv2

from kafka_lab.common import ensure_dir, ensure_parent_dir


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Extract video frames into a dataset CSV."
    )
    parser.add_argument(
        "--video-path",
        type=Path,
        required=True,
        help="Path to source video file (recommended around 30 minutes).",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data/video_dataset"),
        help="Directory to store extracted frames and metadata.",
    )
    parser.add_argument(
        "--sample-fps",
        type=float,
        default=1.0,
        help="How many frames to extract per second from the source video.",
    )
    parser.add_argument(
        "--max-frames",
        type=int,
        default=0,
        help="Optional cap for extracted frames (0 means no cap).",
    )
    parser.add_argument(
        "--jpeg-quality",
        type=int,
        default=80,
        help="JPEG quality (0-100) for stored frames.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    if not args.video_path.exists():
        raise FileNotFoundError(f"Video not found: {args.video_path}")

    if args.sample_fps <= 0:
        raise ValueError("--sample-fps must be > 0")

    output_dir = ensure_dir(args.output_dir)
    frames_dir = ensure_dir(output_dir / "frames")
    metadata_csv = output_dir / "frames.csv"
    ensure_parent_dir(metadata_csv)

    cap = cv2.VideoCapture(str(args.video_path))
    if not cap.isOpened():
        raise RuntimeError(f"Cannot open video file: {args.video_path}")

    source_fps = cap.get(cv2.CAP_PROP_FPS) or 0.0
    if source_fps <= 0:
        source_fps = 30.0

    frame_step = max(1, int(round(source_fps / args.sample_fps)))
    total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT) or 0)
    duration_s = (total_frames / source_fps) if source_fps > 0 else 0.0

    headers = [
        "row_id",
        "frame_number",
        "video_timestamp_ms",
        "frame_path",
        "frame_bytes",
    ]
    written = 0
    frame_idx = 0

    with metadata_csv.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=headers)
        writer.writeheader()

        while True:
            ok, frame = cap.read()
            if not ok:
                break

            if frame_idx % frame_step == 0:
                encoded_ok, encoded = cv2.imencode(
                    ".jpg",
                    frame,
                    [int(cv2.IMWRITE_JPEG_QUALITY), args.jpeg_quality],
                )
                if not encoded_ok:
                    frame_idx += 1
                    continue

                frame_file = frames_dir / f"frame_{written:07d}.jpg"
                frame_file.write_bytes(encoded.tobytes())

                writer.writerow(
                    {
                        "row_id": written,
                        "frame_number": frame_idx,
                        "video_timestamp_ms": int((frame_idx / source_fps) * 1000),
                        "frame_path": str(frame_file.resolve()),
                        "frame_bytes": frame_file.stat().st_size,
                    }
                )
                written += 1

                if args.max_frames > 0 and written >= args.max_frames:
                    break

            frame_idx += 1

    cap.release()
    print(
        f"Prepared frame dataset: {written} frames from {args.video_path} "
        f"(source_fps={source_fps:.2f}, duration_s={duration_s:.2f})."
    )
    print(f"Metadata: {metadata_csv.resolve()}")
    print(f"Frames dir: {frames_dir.resolve()}")


if __name__ == "__main__":
    main()
