COMPOSE_FILE := docker-compose.yaml

perms:
	sudo mkdir -p logs plugins dags raw_data clean_data data-db-volume && sudo chmod -R u=rwx,g=rwx,o=rwx logs plugins dags raw_data clean_data 

docker-spin-up:
	@echo "Initializing Airflow and spinning up Docker services..."
	docker compose up airflow-init && docker compose up  

up:	perms docker-spin-up

down:
	@echo "Bringing down Docker services..."
	docker compose down

clean: 
	@echo "Cleaning up Docker containers and networks..."
	docker compose -f $(COMPOSE_FILE) down --remove-orphans

.PHONY: up down clean docker-spin-up perms