.PHONY: init test lint
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

notebook: ## Run the notebook server
	jupyter-notebook --notebook-dir notebooks

run/get_national_dataset: ## Run the get national dataset task
	python src/collector/tasks/get_national_dataset --output_folder raw/national

run/get_municipality_dataset: ## Run the get municipality dataset task
	python src/collector/tasks/get_municipality_dataset --output_folder raw/municipality

run/get_intensive_care_dataset: ## Run the get intensive care dataset task
	python src/collector/tasks/get_intensive_care_dataset --output_folder raw/intensive-care

run/clean_national_dataset: ## Run the clean national dataset task
	python src/collector/tasks/clean_national_dataset --input_folder raw/national --output_folder interim/national

run/clean_municipality_dataset: ## Run the clean municipality dataset task
	python src/collector/tasks/clean_municipality_dataset --input_folder raw/municipality --output_folder interim/municipality

run/clean_intensive_care_dataset: ## Run the clean intensive care dataset task
	python src/collector/tasks/clean_intensive_care_dataset --input_folder raw/intensive-care --output_folder interim/intensive-care

run/merge_national_dataset: ## Run the merge national dataset task
	python src/collector/tasks/merge_national_dataset --name rivm-covid-19-national --input_folder interim/national --output_folder processed

run/merge_municipality_dataset: ## Run the merge municipality dataset task
	python src/collector/tasks/merge_municipality_dataset --name rivm-covid-19-municipality --input_folder interim/municipality --output_folder processed
	
run/merge_intensive_care_dataset: ## Run the merge intensive care dataset task
	python src/collector/tasks/merge_intensive_care_dataset --name nice-covid-19-intensive-care --input_folder interim/intensive-care --output_folder processed

test: ## Run tests
	pytest test

##

lint: ## Run lint
	pylint src test
