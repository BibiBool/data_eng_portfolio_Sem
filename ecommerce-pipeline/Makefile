.PHONY: docker-spin-up wait-for-scheduler perms up down restart sh

####################################################################################################################
# Environment Setup: Default to .env.dev if ENV_FILE is not specified
ENV_FILE ?= .env.dev

docker-spin-up:
	docker compose --env-file $(ENV_FILE) up airflow-init && docker compose --env-file $(ENV_FILE) up --build -d

wait-for-scheduler:
	@echo "Waiting for all Airflow services to be healthy..."
	until docker compose --env-file $(ENV_FILE) ps | grep -E 'commerce-pipeline-airflow-scheduler-1' | grep '(healthy)' | wc -l | grep -q 1; do \
		echo -n "."; sleep 5; \
	done
	@echo "All Airflow services are healthy!"

perms:
	sudo mkdir -p logs plugins dags tests migrations && sudo chmod -R u=rwx,g=rwx,o=rwx logs plugins dags tests migrations

up: perms docker-spin-up 

down:
	docker compose down --volumes --rmi all

restart: down up

sh:
	docker exec -T airflow-cli bash 

####################################################################################################################
# Testing, auto formatting, type checks, & Lint checks

isort:
	docker compose exec -T airflow-scheduler isort .

format:
	docker compose exec -T airflow-scheduler python3 -m black -S --line-length 79 .

lint: 
	docker compose exec -T airflow-scheduler flake8 /opt/airflow/dags

ci: isort lint