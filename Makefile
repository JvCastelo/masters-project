# Shortcuts for the pipeline. Run `make` (or `make help`) to list the targets.
#
# Which config a target uses:
#   sonda, goes                      config/pipeline.json, or CONFIG=<file>
#   dataset, train, tune, predict,
#   plots, experiment                the experiment config (EXPERIMENT), or CONFIG=<file>
# CONFIG may be relative to this directory or absolute.

UV         ?= uv
PY          = $(UV) run python -m masters_project
EXPERIMENT ?= config/experiments/ourinhos-2023.json
BY         ?= year

ifneq ($(CONFIG),)
  $(if $(wildcard $(CONFIG)),,$(error CONFIG=$(CONFIG) does not exist))
endif
ML_CONFIG  = $(or $(CONFIG),$(EXPERIMENT))
ETL_ENV    = $(if $(CONFIG),MASTERS_PIPELINE_CONFIG=$(abspath $(CONFIG)))
ML_ENV     = MASTERS_PIPELINE_CONFIG=$(abspath $(ML_CONFIG))

.DEFAULT_GOAL := help
# The steps of a pipeline depend on each other: never run them side by side.
.NOTPARALLEL:
.PHONY: help install sonda goes etl dataset train tune predict plots experiment \
        show-config logs test cov lint format format-check check clean

help: ## List the targets
	@awk 'BEGIN {FS = ":.*## "} \
	  /^##@/ {printf "\n%s\n", substr($$0, 5)} \
	  /^[a-zA-Z_-]+:.*## / {printf "  %-14s %s\n", $$1, $$2}' $(MAKEFILE_LIST)
	@printf '\nVariables: CONFIG=<json>  EXPERIMENT=<json>  BY=year|month  STATION=<KEY>  GOES_DIR=<name>  PIPELINE=<log name>\n'

##@ Setup
install: ## Install the dependencies (uv sync)
	$(UV) sync

##@ Collection (ETL)
sonda: ## Download the SONDA data of every station (BY=year|month)
	@echo "-> etl.sonda   (config: $(or $(CONFIG),config/pipeline.json))"
	$(ETL_ENV) $(PY).etl.sonda --by $(BY)

goes: ## Collect the GOES data (long; the per-day cache makes it resumable)
	@case "$(CONFIG)" in *experiments/*) \
	  echo "Refusing to run the GOES ETL with an experiment config: its period covers years" >&2; \
	  echo "that were not collected for every station and would read a huge number of files." >&2; \
	  exit 1;; esac
	@echo "-> etl.goes   (config: $(or $(CONFIG),config/pipeline.json))"
	$(ETL_ENV) $(PY).etl.goes

etl: ## SONDA, then GOES (a SONDA period missing at the source only warns)
	@$(MAKE) --no-print-directory sonda || echo "WARNING: SONDA exited with an error (a period may be unpublished); continuing with GOES"
	@$(MAKE) --no-print-directory goes

##@ Model input and models
dataset: ## Build the model input (STATION=<KEY> for one station, GOES_DIR=<name> for another directory)
	@echo "-> etl.build_dataset   (config: $(ML_CONFIG))"
	$(ML_ENV) $(PY).etl.build_dataset $(if $(STATION),--station $(STATION)) $(if $(GOES_DIR),--goes-dir $(GOES_DIR))

train: ## Train one model per station on ml_config.train
	@echo "-> ml.train   (config: $(ML_CONFIG))"
	$(ML_ENV) $(PY).ml.train

tune: ## Search the hyperparameters of the selected model
	@echo "-> ml.tune   (config: $(ML_CONFIG))"
	$(ML_ENV) $(PY).ml.tune

predict: ## Evaluate the trained models and ensembles on ml_config.predict
	@echo "-> ml.predict   (config: $(ML_CONFIG))"
	$(ML_ENV) $(PY).ml.predict

plots: ## Draw the figures of the last predict
	@echo "-> ml.plots   (config: $(ML_CONFIG))"
	$(ML_ENV) $(PY).ml.plots

experiment: dataset train predict plots ## The whole experiment: dataset, train, predict, plots

##@ Inspection
show-config: ## Show the config each group of targets uses, and the periods in it
	@echo "sonda, goes                : $(or $(CONFIG),config/pipeline.json)"
	@echo "dataset, train, ... plots  : $(ML_CONFIG)"
	@$(ML_ENV) $(UV) run python -c "from masters_project.settings import settings as s; p = s.train_period; \
	  print(f'  ETL period     {s.etl.start_date}..{s.etl.end_date}'); \
	  print(f'  train period   {p.start_date}..{p.end_date}'); \
	  print('  predict        ' + (f'{s.ml.predict.target_station} {s.ml.predict.start_date}..{s.ml.predict.end_date}' if s.ml.predict else 'not configured'))"

logs: ## Follow the newest log of a pipeline: make logs PIPELINE=etl_goes
	@test -n "$(PIPELINE)" || { echo "usage: make logs PIPELINE=<name>  (etl_sonda, etl_goes, etl_build_dataset, ml_train, ml_tune, ml_predict, ml_plots)" >&2; exit 1; }
	tail -n 40 -f data/logs/$(PIPELINE)/latest.log

##@ Quality
test: ## Run the tests
	$(UV) run pytest

cov: ## Run the tests with coverage
	$(UV) run pytest --cov

lint: ## Check the code with ruff
	$(UV) run ruff check src tests notebooks

format: ## Format the code with ruff
	$(UV) run ruff format src tests notebooks

format-check: ## Check the formatting without changing files
	$(UV) run ruff format --check src tests notebooks

check: lint format-check cov ## Lint, formatting and tests with coverage (what CI would run)

clean: ## Remove Python and tool caches (never touches data/)
	find src tests notebooks -name __pycache__ -type d -prune -exec rm -rf {} +
	rm -rf .pytest_cache .ruff_cache .coverage
