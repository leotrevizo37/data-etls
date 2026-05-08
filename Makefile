SHELL := /bin/bash

PYTHON ?= python
COMPOSE_FILE := compose-data_etls/docker-compose.yml
ENV_FILE := compose-data_etls/.env

COMPOSE := docker compose --env-file $(ENV_FILE) -f $(COMPOSE_FILE)

.PHONY: help up down restart build ps lint-python test-python lint-mcdonalds lint-smartfit test-mcdonalds test-smartfit

help:
	@echo "Targets:"
	@echo "  make up          Arranca todos los servicios"
	@echo "  make down        Detiene y baja todos los servicios"
	@echo "  make restart     Re-arranca todos los servicios"
	@echo "  make build       Unicamente crea las imagenes"
	@echo "  make ps          Muestra los contenedores"
	@echo "  make lint-python Corre Ruff sobre los modulos Python"
	@echo "  make test-python Corre la suite de tests de los modulos Dagster"

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

lint-python: lint-mcdonalds lint-smartfit

lint-mcdonalds:
	$(COMPOSE) run --rm --no-deps --entrypoint sh dagster-user-code-mcd -lc "python -m pip install --quiet ruff && cd /opt/dagster/app/mcdonalds && ruff check src tests"

lint-smartfit:
	$(COMPOSE) run --rm --no-deps --entrypoint sh dagster-user-code-smft -lc "python -m pip install --quiet ruff && cd /opt/dagster/app/smartfit && ruff check src tests"

test-python: test-mcdonalds test-smartfit test-cdp

test-mcdonalds:
	$(COMPOSE) run --rm --no-deps --entrypoint sh dagster-user-code-mcd -lc "cd /opt/dagster/app/mcdonalds && python -m unittest discover tests -p 'test_*.py'"

test-smartfit:
	$(COMPOSE) run --rm --no-deps --entrypoint sh dagster-user-code-smft -lc "cd /opt/dagster/app/smartfit && python -m unittest discover tests -p 'test_*.py'"

test-cdp:
	$(COMPOSE) run --rm --no-deps --entrypoint sh dagster-user-code-mcd-cdp -lc "cd /opt/dagster/app/mcdonalds_cdp && python -m unittest discover tests -p 'test_*.py'"
