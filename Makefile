PY ?= python3
export PYTHONPATH := src

.PHONY: install install-spark demo generate run report summary uc-sql test lint spark-demo test-spark up down clean

install:
	$(PY) -m pip install -r requirements-dev.txt

install-spark:
	$(PY) -m pip install -r requirements-spark.txt pytest

## Reference engine end to end: synthetic landing zone → bronze → silver → gold → charts → summary
demo: clean generate run report summary uc-sql

generate:
	$(PY) -m lakehouse generate

run:
	$(PY) -m lakehouse run

report:
	$(PY) -m lakehouse report

summary:
	$(PY) -m lakehouse summary

uc-sql:
	$(PY) -m lakehouse uc-sql

test:
	$(PY) -m pytest -q

lint:
	$(PY) -m ruff check src tests

## Same landing files through PySpark + Delta Lake (needs requirements-spark.txt and Java 17)
spark-demo:
	rm -rf data/spark_lake
	$(PY) -m lakehouse spark

test-spark:
	$(PY) -m pytest -q -m spark tests/spark

up:
	docker compose up --build

down:
	docker compose down -v

clean:
	rm -rf data/landing data/lake
