import os
from datetime import datetime

from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from airflow.models.dag import DAG

from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles import SnowflakeUserPasswordProfileMapping
from pathlib import Path
from dotenv import load_dotenv
import sys

# --- Define the Python Ingestion Task ---
# The path to ingestion script. Airflow's dags folder is the root.
DB_INGESTION_SCRIPT_PATH = "/usr/local/airflow/dags/DB_Raw_Ingestion/DB_Raw_Ingestion.py"

# This function will be called by the PythonOperator.
# It handles importing and running script.
def run_db_raw_ingestion():
    # Import script. The path is relative to the DAG folder.
    project_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), 'DB_Raw_Ingestion'))
    if project_dir not in sys.path:
        sys.path.append(project_dir)
    try:
        from DB_Raw_Ingestion import DB_Raw_Ingestion
        # load dotenv
        load_dotenv()
        schema_dir = str(project_dir / "sql" /  "tables")
        data_dir = str(project_dir / "data" / "processed_files")

        db_raw_ingestion = DB_Raw_Ingestion(data_folder=data_dir, schema_folder=schema_dir)
        db_raw_ingestion.ingest_data(is_full_refresh= True)
        
    except ImportError as e:
        raise ImportError(f"Could not import IngestionManager from {project_dir}. Check your file paths. Error: {e}")
    except Exception as e:
        raise Exception(f"An error occurred during raw data ingestion: {e}")

# --- Define the Dbt Task Group ---
# Define the dbt project and profile configurations
profile_config = ProfileConfig(
    profile_name="default",
    target_name="dev",
    profile_mapping=SnowflakeUserPasswordProfileMapping(
        conn_id="snowflake_conn", 
        profile_args={"database": "NETFLIX_DATA", "schema": "NETFLIX_DATA.dev"},
    )
)

# Use DbtTaskGroup to integrate into a parent DAG
dbt_task_group = DbtTaskGroup(
    group_id="netflix_dbt_run",
    project_config=ProjectConfig("/usr/local/airflow/dags/netflix_dbt"),
    operator_args={"install_deps": True},
    profile_config=profile_config,
    execution_config=ExecutionConfig(dbt_executable_path=f"{os.environ['AIRFLOW_HOME']}/dbt_venv/bin/dbt"),
)

# --- Define the Parent DAG and Task Dependencies ---
with DAG(
    dag_id="full_netflix_pipeline",
    start_date=datetime(2023, 9, 10),
    schedule_interval="@daily",
    catchup=False,
    tags=["dbt", "ingestion", "pipeline"],
) as dag:

    # Task 1: Run the Python ingestion script
    run_ingestion_task = PythonOperator(
        task_id="run_db_raw_ingestion",
        python_callable=run_db_raw_ingestion,
    )

    # Task 2: Run the dbt project
    run_dbt_task = dbt_task_group

    # Set the dependency: The dbt task group runs after the ingestion task.
    run_ingestion_task >> run_dbt_task