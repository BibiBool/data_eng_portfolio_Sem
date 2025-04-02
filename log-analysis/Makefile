COMPOSE_FILE := docker-compose.yml

up:
        docker-compose -f $(COMPOSE_FILE) up -d

down:
        docker-compose -f $(COMPOSE_FILE) down

clean: down
        docker-compose -f $(COMPOSE_FILE) images | awk 'NR>1 {print $$3}' | xargs docker rmi -f

.PHONY: up down clean