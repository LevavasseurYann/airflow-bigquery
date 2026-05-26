# ============================================================================
# Developer entry points. Run `make help` for the list.
#
# `make` ships with Linux/macOS and Git Bash. On Windows PowerShell without
# make, every target maps to a plain `docker compose ...` command — the README
# quick start shows the raw equivalents.
# ============================================================================
.DEFAULT_GOAL := help
COMPOSE := docker compose

.PHONY: help env build init up down restart stop logs ps shell \
        lint format typecheck test test-dags dbt-debug dbt-build dbt-docs \
        clean reset

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-14s\033[0m %s\n", $$1, $$2}'

env: ## Create .env from the template (no-op if it already exists)
	@test -f .env || cp .env.example .env
	@echo ".env is ready."

build: ## Build the custom Airflow image
	$(COMPOSE) build

init: env ## Bootstrap the metadata DB, admin user and pools
	$(COMPOSE) up airflow-init

up: ## Start the full Airflow cluster in the background
	$(COMPOSE) up -d

down: ## Stop the cluster and remove containers
	$(COMPOSE) down

restart: down up ## Restart the cluster

stop: ## Stop the cluster without removing containers
	$(COMPOSE) stop

logs: ## Tail the logs of every service
	$(COMPOSE) logs -f

ps: ## Show the status of every service
	$(COMPOSE) ps

shell: ## Open a bash shell inside a worker container
	$(COMPOSE) exec airflow-worker bash

lint: ## Lint Python with ruff
	ruff check src dags tests

format: ## Auto-format Python with ruff
	ruff format src dags tests
	ruff check --fix src dags tests

typecheck: ## Static type-check the hr_pipeline package
	mypy

test: ## Run the unit + DAG-integrity test suite
	pytest

test-dags: ## Run only the DAG-integrity tests
	pytest -m dags

dbt-debug: ## Verify the embedded dbt project connection
	$(COMPOSE) exec airflow-worker dbt debug \
		--project-dir /opt/airflow/include/dbt --profiles-dir /opt/airflow/include/dbt

dbt-build: ## Run the embedded dbt project end to end (seed + run + test)
	$(COMPOSE) exec airflow-worker dbt build \
		--project-dir /opt/airflow/include/dbt --profiles-dir /opt/airflow/include/dbt

dbt-docs: ## Generate the dbt documentation site artefacts
	$(COMPOSE) exec airflow-worker dbt docs generate \
		--project-dir /opt/airflow/include/dbt --profiles-dir /opt/airflow/include/dbt

clean: ## Remove local build artefacts (caches, dbt target, DuckDB file)
	rm -rf .pytest_cache .ruff_cache .mypy_cache **/__pycache__
	rm -rf include/dbt/target include/dbt/dbt_packages include/dbt/logs
	rm -f include/data/*.duckdb include/data/*.duckdb.wal

reset: ## Tear everything down INCLUDING volumes (full clean slate)
	$(COMPOSE) down --volumes --remove-orphans
	$(MAKE) clean
