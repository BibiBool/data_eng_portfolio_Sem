COMPOSE_FILE := docker-compose.yaml

up:
	docker compose up airflow-init && docker compose up

down:
	docker compose down

clean: down
	docker compose down

.PHONY: up down clean