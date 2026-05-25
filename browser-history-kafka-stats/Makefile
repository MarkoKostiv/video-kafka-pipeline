.PHONY: test run logs clean

test:
	PYTHONPATH=. python -m unittest discover -s tests

run:
	docker compose up --build

logs:
	docker compose logs -f stats-stream generator

clean:
	docker compose down -v --remove-orphans
