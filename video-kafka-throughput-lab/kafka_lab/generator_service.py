from __future__ import annotations

import argparse
import base64
import csv
import time
from pathlib import Path

from kafka_lab.common import init_csv, resolve_bootstrap
from kafka_lab.kafka_utils import create_producer, json_dumps


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Video frame Kafka producer service.")
    parser.add_argument("--topic", required=True, help="Kafka topic name.")
    parser.add_argument(
        "--metadata-csv",
        type=Path,
        default=Path("data/video_dataset/frames.csv"),
        help="CSV produced by prepare_dataset.",
    )
    parser.add_argument("--producer-id", default="producer-1", help="Producer identifier.")
    parser.add_argument("--start-row", type=int, default=0, help="Start row index (inclusive).")
    parser.add_argument(
        "--end-row",
        type=int,
        default=-1,
        help="End row index (exclusive). Use -1 for all rows.",
    )
    parser.add_argument(
        "--sleep-between-ms",
        type=int,
        default=0,
        help="Optional sleep between sends (ms).",
    )
    parser.add_argument("--bootstrap-servers", default=None)
    parser.add_argument(
        "--log-file",
        type=Path,
        default=Path("logs/producers/producer.csv"),
        help="CSV file with producer send timestamps.",
    )
    return parser.parse_args()


def iter_metadata_rows(
    metadata_csv: Path, start_row: int, end_row: int
) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    with metadata_csv.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for row_idx, row in enumerate(reader):
            if row_idx < start_row:
                continue
            if end_row >= 0 and row_idx >= end_row:
                break
            rows.append(row)
    return rows


def main() -> None:
    args = parse_args()
    bootstrap = resolve_bootstrap(args.bootstrap_servers)
    producer = create_producer(bootstrap)

    rows = iter_metadata_rows(args.metadata_csv, args.start_row, args.end_row)
    if not rows:
        print("No rows to publish. Check --start-row/--end-row.")
        return

    init_csv(
        args.log_file,
        headers=[
            "producer_id",
            "row_id",
            "frame_number",
            "send_ts_ns",
            "payload_bytes",
            "frame_bytes",
        ],
    )

    sent = 0
    with args.log_file.open("a", encoding="utf-8", newline="") as log_handle:
        writer = csv.DictWriter(
            log_handle,
            fieldnames=[
                "producer_id",
                "row_id",
                "frame_number",
                "send_ts_ns",
                "payload_bytes",
                "frame_bytes",
            ],
        )

        for row in rows:
            frame_path = Path(row["frame_path"])
            frame_bytes = frame_path.read_bytes()
            send_ts_ns = time.time_ns()

            payload = {
                "row_id": int(row["row_id"]),
                "frame_number": int(row["frame_number"]),
                "video_timestamp_ms": int(row["video_timestamp_ms"]),
                "producer_id": args.producer_id,
                "send_ts_ns": send_ts_ns,
                "frame_bytes": len(frame_bytes),
                "image_base64": base64.b64encode(frame_bytes).decode("ascii"),
            }
            payload_bytes = json_dumps(payload)

            produced = False
            while not produced:
                try:
                    producer.produce(
                        topic=args.topic,
                        key=str(payload["frame_number"]).encode("utf-8"),
                        value=payload_bytes,
                    )
                    produced = True
                except BufferError:
                    producer.poll(0.05)
            producer.poll(0)

            writer.writerow(
                {
                    "producer_id": args.producer_id,
                    "row_id": payload["row_id"],
                    "frame_number": payload["frame_number"],
                    "send_ts_ns": send_ts_ns,
                    "payload_bytes": len(payload_bytes),
                    "frame_bytes": len(frame_bytes),
                }
            )
            log_handle.flush()
            sent += 1

            if args.sleep_between_ms > 0:
                time.sleep(args.sleep_between_ms / 1000.0)

    producer.flush()
    print(
        f"Producer {args.producer_id} published {sent} frames to {args.topic} "
        f"from {args.metadata_csv}."
    )


if __name__ == "__main__":
    main()
