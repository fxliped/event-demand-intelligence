import sys
from datetime import datetime, timedelta
from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


def _ingest_events(ds: str, **_) -> None:
    sys.path.insert(0, '/opt/airflow')
    from ingestion.ingest import retrieve_and_upload
    retrieve_and_upload(ds, ds, upload=True)


def _load_to_snowflake(**_) -> None:
    sys.path.insert(0, '/opt/airflow')
    from load_snowflake import _connect, setup, load_from_stage
    conn = _connect()
    cur = conn.cursor()
    try:
        setup(cur)
        load_from_stage(cur)
    finally:
        cur.close()
        conn.close()


with DAG(
    dag_id='predicthq_pipeline',
    default_args=default_args,
    description='PredictHQ events: ingest → S3 → Snowflake → dbt',
    schedule='0 3 * * *',
    start_date=datetime(2026, 1, 1),
    end_date=datetime(2026, 5, 28), # last date we want to run the DAG
    catchup=False,
) as dag:

    ingest_task = PythonOperator(
        task_id='ingest_events',
        python_callable=_ingest_events,
    )

    load_task = PythonOperator(
        task_id='load_to_snowflake',
        python_callable=_load_to_snowflake,
    )

    dbt_task = BashOperator(
        task_id='dbt_run',
        bash_command='cd /opt/airflow/predicthq_dbt && dbt run',
    )

    ingest_task >> load_task >> dbt_task
