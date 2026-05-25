from __future__ import annotations

import argparse
import json
from dataclasses import asdict, dataclass
from pathlib import Path

import pandas as pd

from kafka_lab.common import ensure_parent_dir


@dataclass
class Metrics:
    messages: int
    total_bytes: int
    start_send_ts_ns: int
    end_process_ts_ns: int
    duration_seconds: float
    throughput_mbps: float
    max_latency_seconds: float
    avg_latency_seconds: float


def compute_metrics(logs_dir: Path) -> Metrics:
    consumer_logs = sorted(logs_dir.glob("consumer_*.csv"))
    if not consumer_logs:
        raise FileNotFoundError(f"No consumer logs found in {logs_dir}")

    frames = [pd.read_csv(path) for path in consumer_logs]
    df = pd.concat(frames, ignore_index=True)
    if df.empty:
        raise RuntimeError("Consumer logs are empty.")

    numeric_columns = [
        "send_ts_ns",
        "process_end_ns",
        "payload_bytes",
    ]
    for column in numeric_columns:
        df[column] = pd.to_numeric(df[column], errors="coerce")
    df = df.dropna(subset=numeric_columns)
    if df.empty:
        raise RuntimeError("Consumer logs do not contain valid numeric data.")

    start_send_ts_ns = int(df["send_ts_ns"].min())
    end_process_ts_ns = int(df["process_end_ns"].max())
    duration_seconds = max((end_process_ts_ns - start_send_ts_ns) / 1_000_000_000, 1e-9)

    total_bytes = int(df["payload_bytes"].sum())
    throughput_mbps = (total_bytes * 8.0) / (duration_seconds * 1_000_000.0)

    latency_seconds = (df["process_end_ns"] - df["send_ts_ns"]) / 1_000_000_000
    max_latency_seconds = float(latency_seconds.max())
    avg_latency_seconds = float(latency_seconds.mean())

    return Metrics(
        messages=int(df.shape[0]),
        total_bytes=total_bytes,
        start_send_ts_ns=start_send_ts_ns,
        end_process_ts_ns=end_process_ts_ns,
        duration_seconds=duration_seconds,
        throughput_mbps=throughput_mbps,
        max_latency_seconds=max_latency_seconds,
        avg_latency_seconds=avg_latency_seconds,
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Aggregate consumer logs into a report.")
    parser.add_argument("--logs-dir", type=Path, required=True)
    parser.add_argument("--output-json", type=Path, required=True)
    parser.add_argument(
        "--summary-csv",
        type=Path,
        default=None,
        help="Optional CSV to append one row summary per run.",
    )
    parser.add_argument("--experiment-name", default="")
    parser.add_argument("--producers", type=int, default=0)
    parser.add_argument("--consumers", type=int, default=0)
    parser.add_argument("--partitions", type=int, default=0)
    parser.add_argument("--replicas", type=int, default=0)
    return parser.parse_args()


def append_summary(summary_csv: Path, row: dict[str, object]) -> None:
    ensure_parent_dir(summary_csv)
    df = pd.DataFrame([row])
    if summary_csv.exists():
        existing = pd.read_csv(summary_csv)
        combined = pd.concat([existing, df], ignore_index=True)
    else:
        combined = df
    combined.to_csv(summary_csv, index=False)


def main() -> None:
    args = parse_args()
    metrics = compute_metrics(args.logs_dir)

    report_payload = {
        "experiment_name": args.experiment_name,
        "producers": args.producers,
        "consumers": args.consumers,
        "partitions": args.partitions,
        "replicas": args.replicas,
        **asdict(metrics),
    }

    ensure_parent_dir(args.output_json)
    args.output_json.write_text(json.dumps(report_payload, indent=2), encoding="utf-8")

    if args.summary_csv is not None:
        append_summary(args.summary_csv, report_payload)

    print(f"Report written to {args.output_json}")
    print(
        f"throughput_mbps={metrics.throughput_mbps:.6f}, "
        f"max_latency_seconds={metrics.max_latency_seconds:.6f}, "
        f"messages={metrics.messages}"
    )


if __name__ == "__main__":
    main()
