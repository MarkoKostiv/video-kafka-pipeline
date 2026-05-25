.PHONY: install up down prepare run plots

install:
	python3 -m venv .venv
	. .venv/bin/activate && pip install -U pip && pip install -e .

up:
	docker compose up -d

down:
	docker compose down

prepare:
	@echo "Usage: make prepare VIDEO=/absolute/path/to/video.mp4"
	python -m kafka_lab.prepare_dataset --video-path "$(VIDEO)" --output-dir data/video_dataset --sample-fps 1.0

run:
	python -m kafka_lab.run_experiments --metadata-csv data/video_dataset/frames.csv --output-dir results

plots:
	python -m kafka_lab.plot_report --summary-csv results/summary.csv --output-dir results/plots
