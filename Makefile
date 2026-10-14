SHELL := /bin/bash

PYTHON ?= python
COMPOSE_FILE := compose-data_etls/docker-compose.yml
ENV_FILE := compose-data_etls/.env
export COMPOSE_ENV_FILE := $(notdir $(ENV_FILE))

COMPOSE := docker compose --env-file $(ENV_FILE) -f $(COMPOSE_FILE)

.PHONY: help up down restart build ps lint-python test-python lint-emerson test-emerson

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

lint-python: lint-gabinete lint-valledelencino lint-carlsjr lint-emerson

lint-gabinete:
	$(COMPOSE) run --rm --no-deps --entrypoint sh dagster-user-code-ecosat -lc "python -m pip install --quiet ruff && cd /opt/dagster/app/ecosat && ruff check src tests"

lint-valledelencino:
	$(COMPOSE) run --rm --no-deps --entrypoint sh dagster-user-code-valledelencino -lc "python -m pip install --quiet ruff && cd /opt/dagster/app/valledelencino && ruff check src tests"

lint-carlsjr:
	$(COMPOSE) run --rm --no-deps --entrypoint sh dagster-user-code-carlsjr -lc "python -m pip install --quiet ruff && cd /opt/dagster/app/carlsjr && ruff check src tests"

lint-emerson:
	$(COMPOSE) run --rm --no-deps --entrypoint sh dagster-user-code-emerson -lc "python -m pip install --quiet ruff && cd /opt/dagster/app/emerson && ruff check src tests"

test-python: test-gabinete test-valledelencino test-carlsjr test-emerson

test-gabinete:
	$(COMPOSE) run --rm --no-deps --entrypoint sh dagster-user-code-ecosat -lc "cd /opt/dagster/app/ecosat && python -m unittest discover tests -p 'test_*.py'"

test-valledelencino:
	$(COMPOSE) run --rm --no-deps --entrypoint sh dagster-user-code-valledelencino -lc "cd /opt/dagster/app/valledelencino && python -m unittest discover tests -p 'test_*.py'"

test-carlsjr:
	$(COMPOSE) run --rm --no-deps --entrypoint sh dagster-user-code-carlsjr -lc "cd /opt/dagster/app/carlsjr && python -m unittest discover tests -p 'test_*.py'"

test-emerson:
	$(COMPOSE) run --rm --no-deps --entrypoint sh dagster-user-code-emerson -lc "cd /opt/dagster/app/emerson && python -m unittest discover tests -p 'test_*.py'"
