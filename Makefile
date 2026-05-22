# ─────────────────────────────────────────────
# Kafka Streaming Pipeline — Makefile
# ─────────────────────────────────────────────

.PHONY: help up down restart logs status test clean

help:
	@echo "Available commands:"
	@echo "  make up       - Start all containers"
	@echo "  make down     - Stop all containers"
	@echo "  make restart  - Restart all containers"
	@echo "  make logs     - Show logs from all containers"
	@echo "  make status   - Show container status"
	@echo "  make test     - Run unit tests"
	@echo "  make clean    - Stop containers and remove volumes"

up:
	docker-compose up -d
	@echo "Pipeline started. Dashboard at http://localhost:3000"
	@echo "Kafka UI at http://localhost:8080"

down:
	docker-compose down

restart:
	docker-compose down
	docker-compose up -d

logs:
	docker-compose logs -f

status:
	docker-compose ps

test:
	docker-compose run --rm consumer python -m pytest tests/ -v

clean:
	docker-compose down -v
	@echo "All containers and volumes removed"