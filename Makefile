clean_repo:
	rm -rf .venv day-summary *.checkpoint .pytest_cache .coverage

init: clean_repo
	pip install poetry
	poetry install
	pre-commit install

test:
	poetry run python -m pytest --cov=mercado_bitcoin tests/ -v

# CI/CD
ci-init:
	pip install poetry
	poetry install

ci-test:
	poetry run python -m pytest --cov=mercado_bitcoin tests/ -v
