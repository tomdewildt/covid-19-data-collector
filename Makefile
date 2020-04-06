.PHONY: init run/get_general_dataset run/get_municipality_dataset run/clean_general_dataset run/clean_municipality_dataset run/merge_general_dataset run/merge_municipality_dataset test lint
.DEFAULT_GOAL := help

NAMESPACE := tomdewildt
NAME := covid-19-data-data_collector

ENV := dev

export PYTHONPATH=src:test
export CONFIG=env/${ENV}/config.yaml

help: ## Show this help
	@echo "${NAMESPACE}/${NAME}"
	@echo
	@fgrep -h "##" $(MAKEFILE_LIST) | \
	fgrep -v fgrep | sed -e 's/## */##/' | column -t -s##

##

init: ## Initialize the environment
	for f in requirements/*.txt; do \
		pip install -r "$$f"; \
	done

##

run/get_general_dataset: ## Run the get general dataset task
	python src/collector/tasks/get_general_dataset --output_folder raw/general

run/get_municipality_dataset: ## Run the get municipality dataset task
	python src/collector/tasks/get_municipality_dataset --output_folder raw/municipality

run/clean_general_dataset: ## Run the clean general dataset task
	python src/collector/tasks/clean_general_dataset --input_folder raw/general --output_folder interim/general

run/clean_municipality_dataset: ## Run the clean municipality dataset task
	python src/collector/tasks/clean_municipality_dataset --input_folder raw/municipality --output_folder interim/municipality

run/merge_general_dataset: ## Run the merge general dataset task
	python src/collector/tasks/merge_general_dataset --name rivm-covid-19-general --input_folder interim/general --output_folder processed

run/merge_municipality_dataset: ## Run the merge municipality dataset task
	python src/collector/tasks/merge_municipality_dataset --name rivm-covid-19-municipality --input_folder interim/municipality --output_folder processed
	
test: ## Run tests
	pytest test

##

lint: ## Run lint
	pylint src test
