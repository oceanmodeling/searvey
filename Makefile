.PHONY: list docs

list:
	@LC_ALL=C $(MAKE) -pRrq -f $(lastword $(MAKEFILE_LIST)) : 2>/dev/null | awk -v RS= -F: '/^# File/,/^# Finished Make data base/ {if ($$1 !~ "^[#.]") {print $$1}}' | sort | egrep -v -e '^[^[:alnum:]]' -e '^$@$$'

lint:
	prospector --absolute-paths --no-external-config --profile-path .prospector.yaml -w profile-validator searvey

mypy:
	dmypy run searvey

docs:
	make -C docs html

deps:
	deptry --ignore-notebooks ./
	poetry export --without-hashes -f requirements.txt -o requirements/requirements.txt
	poetry export --without-hashes -f requirements.txt --with dev -o requirements/requirements-dev.txt
