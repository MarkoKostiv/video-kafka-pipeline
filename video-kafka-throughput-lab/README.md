# Video Kafka Throughput Lab

Dummy distributed application that uses Kafka (Redpanda) to process video frames and measure system throughput/max latency for different producer/consumer/partition configurations.

## What is implemented

1. `prepare_dataset` microservice:
   - Reads a video file (target: around 30 minutes).
   - Extracts frames at configurable sampling FPS.
   - Saves JPEG frames and `frames.csv` metadata.

2. `generator_service` microservice:
   - Reads `frames.csv`.
   - Sends each frame to Kafka as a message (`image_base64` + metadata + send timestamp).

3. `consumer_service` microservice:
   - Consumes frame messages.
   - Sleeps for 1 second (imitated processing).
   - Logs frame numbers and timestamps to CSV.

4. `aggregate_report` microservice:
   - Merges consumer logs.
   - Computes:
     - Throughput in Mbps.
     - Max processing latency (`process_end - send_ts`).

5. `run_experiments` orchestrator:
   - Runs all required configurations from `experiments/required_experiments.yaml`.
   - Creates separate topics and logs/reports per run.

6. `plot_report`:
   - Builds graphs:
     - throughput vs configuration
     - max latency vs configuration

## Project structure

```text
video-kafka-throughput-lab/
  kafka_lab/
    prepare_dataset.py
    generator_service.py
    consumer_service.py
    aggregate_report.py
    run_experiments.py
    plot_report.py
    topic_admin.py
  experiments/required_experiments.yaml
  docker-compose.yml
```

## Setup

1. Install Python dependencies:

```bash
cd /Users/markokostiv/projects/ucu/kafka/video-kafka-throughput-lab
python3 -m venv .venv
source .venv/bin/activate
pip install -U pip
pip install -e .
```

2. Start Redpanda:

```bash
docker compose up -d
```

Kafka bootstrap server for scripts: `localhost:19092`.

## Dataset preparation (video)

```bash
python -m kafka_lab.prepare_dataset \
  --video-path /absolute/path/to/your_30min_video.mp4 \
  --output-dir data/video_dataset \
  --sample-fps 1.0
```

This produces:
- `data/video_dataset/frames/` (JPEG frames)
- `data/video_dataset/frames.csv`

## Run all required experiments

```bash
python -m kafka_lab.run_experiments \
  --metadata-csv data/video_dataset/frames.csv \
  --experiments-yaml experiments/required_experiments.yaml \
  --messages-per-run 200 \
  --sleep-seconds 1 \
  --output-dir results
```

Notes:
- `messages-per-run` is capped to available dataset rows.
- `replicas=1` is used in required configs (single-node Redpanda).  
  For replication factor >1, run a multi-node cluster.

## Generate graphs for the report

```bash
python -m kafka_lab.plot_report \
  --summary-csv results/summary.csv \
  --output-dir results/plots
```

Output:
- `results/plots/throughput_vs_config.png`
- `results/plots/max_latency_vs_config.png`

## Required experiment configurations

The file `experiments/required_experiments.yaml` includes:

1. `1 producer, 1 partition, 1 consumer`
2. `1 producer, 1 partition, 2 consumers`
3. `1 producer, 2 partitions, 2 consumers`
4. `1 producer, 5 partitions, 5 consumers`
5. `1 producer, 10 partitions, 1 consumer`
6. `1 producer, 10 partitions, 5 consumers`
7. `1 producer, 10 partitions, 10 consumers`
8. `2 producers, 10 partitions, 10 consumers`

## Report

Use `report/report.md` as the short report entry point.
