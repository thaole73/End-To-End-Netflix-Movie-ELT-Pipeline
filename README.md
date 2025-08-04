# Project: NETFLIX Movie Data ELT Pipeline

### 1. Introduction

This project demonstrates the design and implementation of a robust ELT (Extract, Load, Transform) data pipeline. It ingests movie data from flat files, stages it in an AWS S3 bucket, and loads it into a Snowflake data warehouse. The raw data is then transformed into a clean, analytics-ready dimensional model using dbt (Data Build Tool). The entire workflow is orchestrated using Apache Airflow, ensuring reliability and scheduled execution.

### 2. Project Goal & Problem Statement

The primary goal was to create a reliable data pipeline to support business intelligence and analytical reporting on movie-related data (e.g., ratings, genres, tags). The key challenges addressed were:

* **Ingestion:** Reliably extracting raw data from flat files and landing it in a secure, accessible location.
* **Scalability:** The solution needed to handle increasing data volumes without manual intervention.
* **Transformation:** The raw data was not suitable for direct analytics. It required a structured, normalized model that was easy for business users to query.
* **Orchestration:** Ensuring that each step of the pipeline (ingestion, loading, transformation) was executed in the correct sequence and with clear dependency management.

### 3. Solution Architecture & Data Flow

The pipeline follows a standard ELT pattern, broken down into three main stages orchestrated by Airflow.

1.  **Extract & Load (EL):**
    * A custom **Python script (`DB_Raw_Ingestion.py`)** is the first task in the pipeline. It reads flat files containing movie data (e.g., `movies.csv`, `ratings.csv`, `tags.csv`) from a local directory.
    * The script uploads these files to an **AWS S3 bucket**, which acts as a cost-effective data lake for temporary staging. The files are uploaded with a timestamp prefix for versioning and uniqueness.
    * Next, the script leverages **Snowflake's external staging** feature. It executes a `COPY INTO` command to load the data from S3 directly into a `RAW` schema within the Snowflake data warehouse. This approach offloads data processing and minimizes network transfer, maximizing efficiency.
    * The ingestion script is designed to be **incremental**, processing only new or updated files based on a tracking log, but can also perform a full refresh.

2.  **Transform (T):**
    * Once the raw data is loaded into Snowflake, the next task is to transform it using **dbt (Data Build Tool)**.
    * The dbt project `netflix_dbt` contains modular SQL models to transform the raw data. The transformation process adheres to the **Kimball Methodology**, building a dimensional model.
    * **Staging Models:** Initial models clean, rename, and type-cast the raw data into staging tables.
    * **Dimensional Models:** These staging tables are then used to build conformed dimensions and fact tables.

3.  **Orchestration:**
    * **Apache Airflow** is the backbone of the entire pipeline.
    * A single DAG (`full_netflix_pipeline`) is defined to manage the end-to-end workflow.
    * The DAG is composed of two primary tasks:
        * `run_db_raw_ingestion`: A `PythonOperator` that executes the ingestion script.
        * `netflix_dbt_run`: A `DbtTaskGroup` (a feature of the `cosmos` framework) that runs the entire dbt project. This group automatically handles the dependencies between individual dbt models.
    * A clear dependency is set: `run_db_raw_ingestion >> netflix_dbt_run`. This ensures that the dbt transformation never starts until the raw data has been successfully ingested and loaded.

### 4. Key Technologies & Tools

* **Apache Airflow:** For scheduling, monitoring, and orchestrating complex workflows as Directed Acyclic Graphs (DAGs). It provides a reliable and visual interface to manage the pipeline.
* **Python:** Used for developing the custom data ingestion script.
* **AWS S3:** Serves as a staging area (data lake) for incoming data, providing a durable and scalable storage solution.
* **Snowflake:** The cloud-native data warehouse where all data loading and transformation takes place, chosen for its separation of storage and compute, elasticity, and SQL-based operations.
* **dbt (Data Build Tool):** Manages all data transformation logic. It allows for modular, version-controlled, and testable SQL, turning data transformation into a software engineering practice.