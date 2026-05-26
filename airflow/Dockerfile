FROM apache/airflow:3.0.1-python3.11
USER airflow
RUN pip install --no-cache-dir \
    requests \
    boto3 \
    python-dotenv \
    snowflake-connector-python \
    dbt-snowflake
