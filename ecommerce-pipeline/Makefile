.PHONY: docker-spin-up perms up down restatr sh

####################################################################################################################
# Setup containers to run Airflow

docker-spin-up:
	docker compose up airflow-init && docker compose up --build -d

perms:
	sudo mkdir -p logs plugins dags tests migrations && sudo chmod -R u=rwx,g=rwx,o=rwx logs plugins dags tests migrations

up: perms docker-spin-up 

down:
	docker compose down --volumes --rmi all

restart: down up

sh:
	docker exec -ti webserver bash
