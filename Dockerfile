FROM astrocrpublic.azurecr.io/runtime:3.0-6

RUN python -m venv dbt_venv && source dbt_venv/bin/activate && \
    pip install --no-cache-dir dbt-snowflake && deactivate

COPY dags/netflix_dbt /usr/local/airflow/dags/netflix_dbt
COPY dags /usr/local/airflow/dags/DB_Raw_Ingestion