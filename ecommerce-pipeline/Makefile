PROJECT_NAME = ecommerce-pipeline

# Named volumes from docker-compose.yml
VOLUMES = postgres_data metabase_data

.PHONY: help up down clean volumes logs

help: ## Show available make commands
	@echo "Available commands:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'

up: ## Start all containers
	docker-compose up -d

down: ## Stop and remove containers
	docker-compose down

volumes: ## Create named volumes manually
	@for v in $(VOLUMES); do \
		docker volume create $(PROJECT_NAME)_$$v; \
	done

clean: ## Remove containers, networks, and volumes
	docker-compose down -v --remove-orphans
	@for v in $(VOLUMES); do \
		docker volume rm -f $(PROJECT_NAME)_$$v || true; \
	done

logs: ## Tail logs
	docker-compose logs -f
