.PHONY: up down seed seed-bulk seed-drip logs verify clean

up:
	docker compose up -d --build

down:
	docker compose down

# Full data volume, matches SPEC.md capacity notes. Override: make seed ROWS=500000
ROWS ?= 2000000
seed: seed-bulk

seed-bulk:
	docker compose --profile tools run --rm seeder bulk --rows=$(ROWS) --batch=5000

seed-drip:
	docker compose --profile tools run --rm seeder drip --rate=$(RATE) --duration=$(DURATION)

logs:
	docker compose logs -f

verify:
	bash verify.sh

clean:
	docker compose down -v
