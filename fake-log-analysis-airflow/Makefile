COMPOSE_FILE := docker-compose.yaml

perms:
	sudo mkdir -p logs plugins dags raw_data clean_data data-db-volume config && sudo chmod -R u=rwx,g=rwx,o=rwx logs plugins dags raw_data clean_data config

docker-spin-up:
	@echo "Initializing Airflow and spinning up Docker services..."
	docker compose up airflow-init && docker compose up  

up:	perms docker-spin-up

down:
	@echo "Bringing down Docker services..."
	docker compose down

clean: 
	@echo "Cleaning up Docker containers and networks..."
	docker compose down --remove-orphans

fresh-start:
	@echo "Cleaning up all Docker resources..."
	# Stop and remove containers, networks, and named volumes
	docker compose -f $(COMPOSE_FILE) down --volumes --rmi all
	# Prune any dangling volumes not removed by down --volumes
	docker volume prune -f
	# Prune any unused networks
	docker network prune -f
	@echo "Docker cleanup complete."

.PHONY: up down clean docker-spin-up perms clean-docker