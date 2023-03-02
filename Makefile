.PHONY: list docs

list:
	@LC_ALL=C $(MAKE) -pRrq -f $(lastword $(MAKEFILE_LIST)) : 2>/dev/null | awk -v RS= -F: '/^# File/,/^# Finished Make data base/ {if ($$1 !~ "^[#.]") {print $$1}}' | sort | grep -E -v -e '^[^[:alnum:]]' -e '^$@$$'

init:
	poetry install --with dev --with docs --with jupyter --sync
	pre-commit install

style:
	pre-commit run black -a

lint:
	pre-commit run ruff -a

mypy:
	dmypy run searvey

test:
	python -m pytest -vlx

cov:
	coverage erase
	python -m pytest --cov=searvey --cov-report term-missing -n auto --durations=10 --record-mode=none

docs:
	make -C docs html

deps:
	pre-commit run poetry-lock -a
	pre-commit run poetry-export -a
