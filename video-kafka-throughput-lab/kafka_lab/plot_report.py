from __future__ import annotations

import argparse
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

from kafka_lab.common import ensure_dir


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Plot throughput and latency graphs.")
    parser.add_argument(
        "--summary-csv",
        type=Path,
        default=Path("results/summary.csv"),
        help="Summary CSV from run_experiments.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("results/plots"),
        help="Where to save graph images.",
    )
    return parser.parse_args()


def configuration_label(row: pd.Series) -> str:
    return (
        f"P{int(row['producers'])}-C{int(row['consumers'])}-"
        f"T{int(row['partitions'])}-R{int(row['replicas'])}"
    )


def main() -> None:
    args = parse_args()
    df = pd.read_csv(args.summary_csv)
    if df.empty:
        raise RuntimeError(f"Summary file is empty: {args.summary_csv}")

    df["config"] = df.apply(configuration_label, axis=1)
    out_dir = ensure_dir(args.output_dir)

    throughput_path = out_dir / "throughput_vs_config.png"
    latency_path = out_dir / "max_latency_vs_config.png"

    plt.figure(figsize=(14, 5))
    plt.plot(df["config"], df["throughput_mbps"], marker="o")
    plt.title("Throughput (Mbps) vs Configuration")
    plt.xlabel("Configuration")
    plt.ylabel("Throughput (Mbps)")
    plt.xticks(rotation=30, ha="right")
    plt.tight_layout()
    plt.savefig(throughput_path, dpi=150)
    plt.close()

    plt.figure(figsize=(14, 5))
    plt.plot(df["config"], df["max_latency_seconds"], marker="o", color="tab:red")
    plt.title("Max Latency (s) vs Configuration")
    plt.xlabel("Configuration")
    plt.ylabel("Max Latency (s)")
    plt.xticks(rotation=30, ha="right")
    plt.tight_layout()
    plt.savefig(latency_path, dpi=150)
    plt.close()

    print(f"Saved: {throughput_path}")
    print(f"Saved: {latency_path}")


if __name__ == "__main__":
    main()
