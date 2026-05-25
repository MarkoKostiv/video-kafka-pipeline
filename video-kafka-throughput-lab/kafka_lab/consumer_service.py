from __future__ import annotations

import argparse
import csv
import time
from pathlib import Path

from confluent_kafka import KafkaError

from kafka_lab.common import init_csv, resolve_bootstrap
from kafka_lab.kafka_utils import create_consumer, json_loads


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Video frame Kafka consumer service.")
    parser.add_argument("--topic", required=True, help="Kafka topic name.")
    parser.add_argument("--group-id", required=True, help="Kafka consumer group id.")
    parser.add_argument("--consumer-id", required=True, help="Logical consumer id for logs.")
    parser.add_argument("--bootstrap-servers", default=None)
    parser.add_argument(
        "--sleep-seconds",
        type=float,
        default=1.0,
        help="Imitated processing delay per message.",
    )
    parser.add_argument(
        "--idle-timeout-seconds",
        type=float,
        default=120.0,
        help="Exit if no messages appear for this duration after consumption starts.",
    )
    parser.add_argument(
        "--log-file",
        type=Path,
        default=Path("logs/consumers/consumer.csv"),
        help="CSV output with processing timestamps and frame numbers.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    bootstrap = resolve_bootstrap(args.bootstrap_servers)

    init_csv(
        args.log_file,
        headers=[
            "consumer_id",
            "topic",
            "partition",
            "offset",
            "row_id",
            "frame_number",
            "video_timestamp_ms",
            "send_ts_ns",
            "process_start_ns",
            "process_end_ns",
            "payload_bytes",
        ],
    )

    consumer = create_consumer(
        bootstrap_servers=bootstrap,
        topic=args.topic,
        group_id=args.group_id,
        auto_offset_reset="earliest",
    )

    received_any = False
    last_activity = time.monotonic()

    with args.log_file.open("a", encoding="utf-8", newline="") as log_handle:
        writer = csv.DictWriter(
            log_handle,
            fieldnames=[
                "consumer_id",
                "topic",
                "partition",
                "offset",
                "row_id",
                "frame_number",
                "video_timestamp_ms",
                "send_ts_ns",
                "process_start_ns",
                "process_end_ns",
                "payload_bytes",
            ],
        )

        try:
            while True:
                msg = consumer.poll(1.0)
                if msg is None:
                    if (
                        received_any
                        and (time.monotonic() - last_activity) > args.idle_timeout_seconds
                    ):
                        break
                    continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    raise RuntimeError(f"Kafka consumer error: {msg.error()}")

                payload = json_loads(msg.value())
                process_start_ns = time.time_ns()
                time.sleep(args.sleep_seconds)
                process_end_ns = time.time_ns()

                writer.writerow(
                    {
                        "consumer_id": args.consumer_id,
                        "topic": msg.topic(),
                        "partition": msg.partition(),
                        "offset": msg.offset(),
                        "row_id": payload["row_id"],
                        "frame_number": payload["frame_number"],
                        "video_timestamp_ms": payload["video_timestamp_ms"],
                        "send_ts_ns": payload["send_ts_ns"],
                        "process_start_ns": process_start_ns,
                        "process_end_ns": process_end_ns,
                        "payload_bytes": len(msg.value()),
                    }
                )
                log_handle.flush()
                consumer.commit(message=msg, asynchronous=False)

                received_any = True
                last_activity = time.monotonic()
        finally:
            consumer.close()

    print(f"Consumer {args.consumer_id} finished. Log written to {args.log_file}.")


if __name__ == "__main__":
    main()
