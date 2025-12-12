SHELL := /bin/bash

COMPOSE_FILE := compose-mcdonalds/docker-compose.yml
ENV_FILE := compose-mcdonalds/.env

COMPOSE := docker compose --env-file $(ENV_FILE) -f $(COMPOSE_FILE)

.PHONY: help up down restart build ps

help:
	@echo "Targets:"
	@echo "  make up          Arranca todos los servicios"
	@echo "  make down        Detiene y baja todos los servicios"
	@echo "  make restart     Re-arranca todos los servicios"
	@echo "  make build       Unicamente crea las imagenes"
	@echo "  make ps          Muestra los contenedores"

up:
	$(COMPOSE) up -d --build

down:
	$(COMPOSE) down -v

restart:
	$(COMPOSE) restart

build:
	$(COMPOSE) build

ps:
	$(COMPOSE) ps