DBT = cd predicthq_dbt && set -a && source ../ingestion/.env && set +a && ../dbt-venv/bin/dbt

dbt-run:
	$(DBT) run

dbt-test:
	$(DBT) test

dbt-all:
	$(DBT) run && ../dbt-venv/bin/dbt test

ingest:
	cd ingestion && set -a && source .env && set +a && ../.venv/bin/python ingest.py

load:
	set -a && source ingestion/.env && set +a && .venv/bin/python load_snowflake.py

airflow-up:
	set -a && source ingestion/.env && set +a && docker compose up -d

airflow-down:
	docker compose down

airflow-logs:
	docker compose logs -f airflow

airflow-password:
	docker compose logs airflow 2>/dev/null | grep "Password for user" || true

export:
	set -a && source ingestion/.env && set +a && .venv/bin/python export_snapshot.py
