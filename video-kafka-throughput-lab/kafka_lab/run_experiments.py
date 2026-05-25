from __future__ import annotations

import argparse
import csv
import json
import signal
import subprocess
import sys
import time
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path

import yaml

from kafka_lab.aggregate_report import Metrics, append_summary, compute_metrics
from kafka_lab.common import ensure_dir, resolve_bootstrap
from kafka_lab.topic_admin import create_topic, delete_topic


@dataclass
class ExperimentConfig:
    name: str
    producers: int
    consumers: int
    partitions: int
    replicas: int = 1


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run Kafka throughput experiments.")
    parser.add_argument(
        "--metadata-csv",
        type=Path,
        default=Path("data/video_dataset/frames.csv"),
        help="Frame metadata CSV from prepare_dataset.",
    )
    parser.add_argument(
        "--experiments-yaml",
        type=Path,
        default=Path("experiments/required_experiments.yaml"),
        help="Experiments configuration file.",
    )
    parser.add_argument("--bootstrap-servers", default=None)
    parser.add_argument("--topic-prefix", default="video-lab")
    parser.add_argument(
        "--messages-per-run",
        type=int,
        default=200,
        help="How many frames to process for each experiment.",
    )
    parser.add_argument(
        "--sleep-seconds",
        type=float,
        default=1.0,
        help="Consumer processing sleep duration.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("results"),
        help="Directory for logs and reports.",
    )
    parser.add_argument(
        "--delete-topics-after-run",
        action="store_true",
        help="Delete experiment topics after each run.",
    )
    return parser.parse_args()


def load_experiments(path: Path) -> list[ExperimentConfig]:
    raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    experiments: list[ExperimentConfig] = []
    for item in raw.get("experiments", []):
        experiments.append(
            ExperimentConfig(
                name=str(item["name"]),
                producers=int(item["producers"]),
                consumers=int(item["consumers"]),
                partitions=int(item["partitions"]),
                replicas=int(item.get("replicas", 1)),
            )
        )
    if not experiments:
        raise RuntimeError(f"No experiments found in {path}")
    return experiments


def count_dataset_rows(metadata_csv: Path) -> int:
    with metadata_csv.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.reader(handle)
        next(reader, None)
        return sum(1 for _ in reader)


def split_ranges(total_messages: int, producers: int) -> list[tuple[int, int]]:
    base = total_messages // producers
    remainder = total_messages % producers
    ranges: list[tuple[int, int]] = []
    start = 0
    for idx in range(producers):
        size = base + (1 if idx < remainder else 0)
        end = start + size
        ranges.append((start, end))
        start = end
    return ranges


def count_processed_messages(consumer_logs_dir: Path) -> int:
    total = 0
    for path in consumer_logs_dir.glob("consumer_*.csv"):
        with path.open("r", encoding="utf-8", newline="") as handle:
            lines = sum(1 for _ in handle)
            total += max(0, lines - 1)
    return total


def terminate_processes(processes: list[subprocess.Popen[bytes]]) -> None:
    for process in processes:
        if process.poll() is None:
            process.send_signal(signal.SIGTERM)
    for process in processes:
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            process.kill()


def run_experiment(
    exp: ExperimentConfig,
    project_root: Path,
    metadata_csv: Path,
    messages_for_run: int,
    bootstrap: str,
    topic_prefix: str,
    sleep_seconds: float,
    output_dir: Path,
    summary_csv: Path,
    delete_topics_after_run: bool,
) -> tuple[Metrics, Path]:
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    run_id = f"{exp.name}_{timestamp}"
    topic = f"{topic_prefix}-{run_id}".replace("_", "-")
    group_id = f"group-{run_id}"

    run_dir = ensure_dir(output_dir / run_id)
    consumers_log_dir = ensure_dir(run_dir / "logs")
    producers_log_dir = ensure_dir(run_dir / "producers")
    report_json = run_dir / "report.json"

    create_topic(
        bootstrap_servers=bootstrap,
        topic=topic,
        partitions=exp.partitions,
        replicas=exp.replicas,
    )

    consumer_processes: list[subprocess.Popen[bytes]] = []
    producer_processes: list[subprocess.Popen[bytes]] = []

    try:
        for idx in range(exp.consumers):
            consumer_log = consumers_log_dir / f"consumer_{idx+1}.csv"
            cmd = [
                sys.executable,
                "-m",
                "kafka_lab.consumer_service",
                "--bootstrap-servers",
                bootstrap,
                "--topic",
                topic,
                "--group-id",
                group_id,
                "--consumer-id",
                f"consumer-{idx+1}",
                "--sleep-seconds",
                str(sleep_seconds),
                "--idle-timeout-seconds",
                "180",
                "--log-file",
                str(consumer_log),
            ]
            consumer_processes.append(subprocess.Popen(cmd, cwd=str(project_root)))

        time.sleep(3)

        ranges = split_ranges(messages_for_run, exp.producers)
        for idx, (start, end) in enumerate(ranges):
            producer_log = producers_log_dir / f"producer_{idx+1}.csv"
            cmd = [
                sys.executable,
                "-m",
                "kafka_lab.generator_service",
                "--bootstrap-servers",
                bootstrap,
                "--topic",
                topic,
                "--metadata-csv",
                str(metadata_csv),
                "--producer-id",
                f"producer-{idx+1}",
                "--start-row",
                str(start),
                "--end-row",
                str(end),
                "--log-file",
                str(producer_log),
            ]
            producer_processes.append(subprocess.Popen(cmd, cwd=str(project_root)))

        for process in producer_processes:
            return_code = process.wait()
            if return_code != 0:
                raise RuntimeError(f"Producer exited with code {return_code}")

        effective_parallelism = max(1, min(exp.consumers, exp.partitions))
        expected_seconds = (messages_for_run * sleep_seconds) / effective_parallelism
        deadline = time.monotonic() + max(120.0, expected_seconds * 4.0)

        while time.monotonic() < deadline:
            processed = count_processed_messages(consumers_log_dir)
            if processed >= messages_for_run:
                break
            time.sleep(2.0)
    finally:
        terminate_processes(consumer_processes)

    metrics = compute_metrics(consumers_log_dir)
    report_payload = {
        "run_id": run_id,
        "experiment_name": exp.name,
        "topic": topic,
        "producers": exp.producers,
        "consumers": exp.consumers,
        "partitions": exp.partitions,
        "replicas": exp.replicas,
        **asdict(metrics),
    }
    report_json.write_text(json.dumps(report_payload, indent=2), encoding="utf-8")
    append_summary(summary_csv, report_payload)

    if delete_topics_after_run:
        delete_topic(bootstrap_servers=bootstrap, topic=topic)

    return metrics, run_dir


def main() -> None:
    args = parse_args()
    bootstrap = resolve_bootstrap(args.bootstrap_servers)
    project_root = Path(__file__).resolve().parents[1]

    experiments = load_experiments(args.experiments_yaml)
    dataset_rows = count_dataset_rows(args.metadata_csv)
    if dataset_rows <= 0:
        raise RuntimeError(f"No rows found in dataset {args.metadata_csv}")

    messages_for_run = min(args.messages_per_run, dataset_rows)
    output_dir = ensure_dir(args.output_dir)
    summary_csv = output_dir / "summary.csv"

    print(f"Loaded {len(experiments)} experiments.")
    print(f"Using {messages_for_run} frames per experiment.")
    print(f"Bootstrap servers: {bootstrap}")

    for exp in experiments:
        print(
            f"Running {exp.name}: producers={exp.producers}, consumers={exp.consumers}, "
            f"partitions={exp.partitions}, replicas={exp.replicas}"
        )
        metrics, run_dir = run_experiment(
            exp=exp,
            project_root=project_root,
            metadata_csv=args.metadata_csv,
            messages_for_run=messages_for_run,
            bootstrap=bootstrap,
            topic_prefix=args.topic_prefix,
            sleep_seconds=args.sleep_seconds,
            output_dir=output_dir,
            summary_csv=summary_csv,
            delete_topics_after_run=args.delete_topics_after_run,
        )
        print(
            f"Done {exp.name} -> throughput={metrics.throughput_mbps:.6f} Mbps, "
            f"max_latency={metrics.max_latency_seconds:.6f}s, run_dir={run_dir}"
        )

    print(f"All experiments completed. Summary: {summary_csv}")


if __name__ == "__main__":
    main()
