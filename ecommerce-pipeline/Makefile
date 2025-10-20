.PHONY: docker-spin-up wait-for-scheduler perms up down restatr sh

####################################################################################################################
# Environment Setup: Default to .env.dev if ENV_FILE is not specified
ENV_FILE ?= .env.dev

docker-spin-up:
	docker compose --env-file $(ENV_FILE) up airflow-init && docker compose --env-file $(ENV_FILE) up --build -d

wait-for scheduler:
	docker compose wait scheduler

perms:
	sudo mkdir -p logs plugins dags tests migrations && sudo chmod -R u=rwx,g=rwx,o=rwx logs plugins dags tests migrations

up: perms docker-spin-up 

down:
	docker compose down --volumes --rmi all

restart: down up

sh:
	docker exec -ti scheduler bash

####################################################################################################################
# Testing, auto formatting, type checks, & Lint checks

isort:
	docker exec scheduler isort .

lint: 
	docker exec scheduler flake8 /opt/airflow/dags

ci: isort lint