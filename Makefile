SHELL := /bin/bash

PYTHON ?= python
COMPOSE_FILE := compose-mcdonalds/docker-compose.yml
ENV_FILE := compose-mcdonalds/.env

COMPOSE := docker compose --env-file $(ENV_FILE) -f $(COMPOSE_FILE)

.PHONY: help up down restart build ps lint-python test-python

help:
	@echo "Targets:"
	@echo "  make up          Arranca todos los servicios"
	@echo "  make down        Detiene y baja todos los servicios"
	@echo "  make restart     Re-arranca todos los servicios"
	@echo "  make build       Unicamente crea las imagenes"
	@echo "  make ps          Muestra los contenedores"
	@echo "  make lint-python Corre Ruff sobre el codigo Python"
	@echo "  make test-python Corre la suite de tests del modulo mcdonalds"

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

lint-python:
	$(COMPOSE) run --rm --no-deps --entrypoint sh dagster-user-code -lc "python -m pip install --quiet ruff && cd /opt/dagster/app/mcdonalds && ruff check src tests"

test-python:
	$(COMPOSE) run --rm --no-deps --entrypoint sh dagster-user-code -lc "cd /opt/dagster/app/mcdonalds && python -m unittest discover tests -p 'test_*.py'"
