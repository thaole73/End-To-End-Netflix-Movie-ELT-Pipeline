#%%
import os
import pandas as pd
from dotenv import load_dotenv 
import logging
import datetime
from io import StringIO
import snowflake.connector
import boto3
import glob
import argparse
from pathlib import Path
import re 

#---Set up logging---
logging.basicConfig(level = logging.INFO, format = '%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DB_Raw_Ingestion:
    """
    Class to ingest raw data from S3 to Snowflake
    """
    def __init__(self, schema_folder: str = 'sql/tables', data_folder: str = 'data/processed_files', processed_files_log: str = 'processed_files.log'):
        """
        Initialize the class with configuration.
        """
        load_dotenv()

        #---Snowflake configuration---
        self.sf_account = os.getenv('SNOWFLAKE_ACCOUNT')
        self.sf_user = os.getenv('SNOWFLAKE_USER')
        self.sf_password = os.getenv('SNOWFLAKE_PASSWORD')
        self.sf_warehouse = os.getenv('SNOWFLAKE_WAREHOUSE')
        self.sf_database = os.getenv('SNOWFLAKE_DATABASE')
        self.sf_schema_raw = os.getenv('SNOWFLAKE_SCHEMA_RAW')
        self.sf_raw_table = os.getenv('SNOWFLAKE_RAW_TABLE')

        #---S3 configuration---
        self.s3_bucket = os.getenv('S3_BUCKET')
        self.s3_prefix = os.getenv('S3_PREFIX_RAW')
        self.snowflake_stage_name = os.getenv('SNOWFLAKE_STAGE_NAME')
        self.aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
        self.aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')
        self.aws_region = os.getenv('AWS_REGION')


        #---Local configuration---
        self.local_data_folder = Path(data_folder)
        self.sql_schema_folder = Path(schema_folder)
        self.processed_files_log = Path(processed_files_log)

        #---File to Table Mapping---
        self.file_to_table_map = {
            'genome-scores.csv': {
                'table_name': 'raw_genome_scores',
                'schema_file': 'raw_genome_scores.sql'},
            'genome-tags.csv': {
                'table_name': 'raw_genome_tags',
                'schema_file': 'raw_genome_tags.sql'},
            'links.csv': {
                'table_name': 'raw_links',
                'schema_file': 'raw_links.sql'},
            'movies.csv': {
                'table_name': 'raw_movies',
                'schema_file': 'raw_movies.sql'},
            'ratings.csv': {
                'table_name': 'raw_ratings',
                'schema_file': 'raw_ratings.sql'},
            'tags.csv': {
                'table_name': 'raw_tags',
                'schema_file': 'raw_tags.sql'}
        }

        #---Initialize S3 client---
        self.s3_client = boto3.client('s3', region_name='eu-central-1')

    def _validate_config(self):
        """
        Validates that all necessary configuration variables are set.
        """
        required_vars = {
            'SNOWFLAKE_ACCOUNT': self.sf_account,
            'SNOWFLAKE_USER': self.sf_user,
            'SNOWFLAKE_PASSWORD': self.sf_password,
            'SNOWFLAKE_WAREHOUSE': self.sf_warehouse,
            'SNOWFLAKE_DATABASE': self.sf_database,
            'SNOWFLAKE_SCHEMA_RAW': self.sf_schema_raw,
            'S3_BUCKET': self.s3_bucket,
            'S3_PREFIX': self.s3_prefix, 
            'SNOWFLAKE_STAGE_NAME': self.snowflake_stage_name,
            'AWS_ACCESS_KEY_ID': self.aws_access_key_id,
            'AWS_SECRET_ACCESS_KEY': self.aws_secret_access_key,
            'AWS_REGION': self.aws_region 
        }
        missing_vars = [name for name, value in required_vars.items() if not value]
        if missing_vars:
            raise ValueError(f"Missing required environment variables: {', '.join(missing_vars)}")
        logger.info("Configuration variables validated.")

    def _get_snowflake_connection(self):
        """
        Get a Snowflake connection.
        """
        try:
            conn = snowflake.connector.connect(
                user = self.sf_user,
                password = self.sf_password,
                account = self.sf_account,
                warehouse = self.sf_warehouse,
                database = self.sf_database,
                schema = self.sf_schema_raw,
                logging_timeout = 60,
                network_timeout = 300
            )
            return conn
        except snowflake.connector.errors.ProgrammingError as e:
            raise Exception(f"Snowflake connection error: {e}")

    def _upload_to_s3(self, data_buffer: StringIO, file_key: str):
        """
        Upload data from a StringIO buffer to S3
        """
        try:
            self.s3_client.put_object(
                Bucket = self.s3_bucket,
                Key = file_key,
                Body = data_buffer.getvalue()
            )
            logger.info(f"Successfully uploaded {file_key} to s3://{self.s3_bucket}")
        except Exception as e:
            logger.error(f"Failed to upload {file_key} to s3://{self.s3_bucket}")
            raise

    def _execute_sql_command(self, conn, sql_command: str, description: str = ''):
        """
        Executes a SQL command using the provided Snowflake connection.

        Args:
            conn: The active Snowflake connection object.
            sql_command (str): The SQL command string to execute.
            description (str): A description for logging purposes.
        """
        try:
            cursor = conn.cursor()
            cursor.execute(sql_command)
            results = cursor.fetchall()
            logger.info(f"Successfully executed SQL. Results: {results}")
        except Exception as e:
            logger.error(f"Failed to execute {description}: {sql_command[:100]}... Error: {e}", exc_info=True)
            raise
        finally:
            if 'cursor' in locals() and cursor:
                cursor.close()
    
    def _copy_into_snowflake(self, conn, s3_file_key: str, target_table: str):
        """
        Executes a COPY INTO command in Snowflake to load data from S3.
        Ensures PURGE = TRUE is include to delete the source file.

        Args:
            conn: The active Snowflake connection object.
            s3_file_key (str): The S3 key of the file to load.
            target_table (str): The Snowflake table to load data into.
            file_format_name (str): The name of the Snowflake file format to use (e.g., 'CSV_FILE_FORMAT').
        """
        # Construct the basic COPY INTO statement
        copy_sql = f"""
        COPY INTO {target_table}
        FROM '@{self.snowflake_stage_name}/{s3_file_key}'
        FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"')
        ON_ERROR = 'CONTINUE'
        PURGE = TRUE;
        """
        logger.info(f"Copying data from S3 '{s3_file_key}' into Snowflake table '{target_table}'.")
        self._execute_sql_command(conn, copy_sql, description=f"COPY INTO {target_table}")
    
    def _read_sql_file_content(self, file_path: Path) -> str:
        """
        Reads the entire content of a SQL file and returns it as a string.
        """
        if not file_path.exists():
            raise FileNotFoundError(f"SQL file not found: {file_path}")

        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                sql_content = f.read()
            return sql_content
        except Exception as e:
            raise IOError(f"Error reading SQL file '{file_path}': {e}")

    def _get_processed_files(self) -> set:
        """
        Get a list of processed files from the log file.
        """
        if not os.path.exists(self.processed_files_log):
            return set()
        with open(self.processed_files_log, 'r') as f:
            return set(f.read().splitlines())

    def _log_processed_file(self, file_path):
        """
        Log a processed file to the log file.
        """
        with open(self.processed_files_log, 'a') as f:
            f.write(f"{file_path}\n")
    
    def _get_base_filename(self, file_path):
        """
        Gets the base filename that should be used as a key in file_to_table_map.
        Handles date patterns like '_YYYYMMDD.csv'.

        Args:
            file_name_with_date_and_ext (str): The full filename (e.g., 'ratings_20240721.csv').

        Returns:
            str: The base filename for map lookup (e.g., 'ratings.csv').
        """
        match_date = re.search(r'_\d{8}', file_path)
        if match_date:
            base_name = file_path[:match_date.start()] + '.csv'
            return base_name
        else:
            return file_path
    
    
    def ingest_data(self, is_full_refresh: bool = False):
        """
        Main function to orchestrate the ingestion process.
        Handles full refresh and incremental loading from S3 to Snowflake.
        """
        logger.info(f"Starting ingestion process. Full refresh: {is_full_refresh}")
        conn = None
        processed_files = self._get_processed_files()

        # Retrieve all files in local folder
        all_local_csv_files = sorted(glob.glob(os.path.join(self.local_data_folder, '*.csv')))
        
        files_to_process_in_this_run = []

        try:
            conn = self._get_snowflake_connection()
            
            if is_full_refresh:
                logger.info("Full refresh is enabled. Processing all files.")
                files_to_process_in_this_run = all_local_csv_files
                if os.path.exists(self.processed_files_log):
                    os.remove(self.processed_files_log)
                open(self.processed_files_log, 'a').close() # create empty log file
                processed_files = set()
            else:
                logger.info("Incremental loading is enabled. Processing new files.")
                files_to_process_in_this_run = [f for f in all_local_csv_files if f not in processed_files]
                processed_files.update(files_to_process_in_this_run)
                logger.info(f"Processed files: {processed_files}")

            if not files_to_process_in_this_run:
                logger.info("No new files to process.")
                return
            
        # Process file
            for local_file_path in files_to_process_in_this_run:
                full_filename = os.path.basename(local_file_path)
                logger.info(f"Processing file: {full_filename}")

                # get the base filename
                base_csv_filename = self._get_base_filename(full_filename)
                logger.info(f"Base filename: {base_csv_filename}")
                mapping_info = self.file_to_table_map.get(base_csv_filename)
                if not mapping_info:
                    logger.warning(f"File {full_filename} not found in file_to_table_map. Skipping...")
                    continue

                raw_table_name = mapping_info['table_name']
                sql_script_name = mapping_info['schema_file']
                sql_script_path = os.path.join(self.sql_schema_folder, sql_script_name)

                if not os.path.exists(sql_script_path):
                    logger.error(f"SQL script {sql_script_path} not found. Skipping...")
                    continue
                
                # Upload file to S3
                timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
                s3_key = f"{self.s3_prefix}{timestamp}_{full_filename}"
                with open(local_file_path, 'r', encoding='utf-8') as f:
                    file_content_buffer = StringIO()
                    file_content_buffer.write(f.read())
                file_content_buffer.seek(0)
                self._upload_to_s3(file_content_buffer, s3_key)

                # Execute SQL to load raw data in Snowflake

                # Use schema in database
                use_schema_sql = f"USE SCHEMA {self.sf_schema_raw};"
                self._execute_sql_command(conn, use_schema_sql, description=f"Use schema {self.sf_schema_raw}")
                logger.info(f"Executed: {use_schema_sql}")
                
                # Read the SQL script content
                sql_content = self._read_sql_file_content(Path(sql_script_path))

                if is_full_refresh:
                    sql_command = re.sub(r'CREATE\s+TABLE', r'CREATE OR REPLACE TABLE', sql_content, 1, re.IGNORECASE)
                    logger.info(f"SQL command for full refresh: {sql_command}")
                else:
                    sql_command = re.sub(r'CREATE\s+TABLE', r'CREATE TABLE IF NOT EXISTS', sql_content, 1, re.IGNORECASE)
                    logger.info(f"SQL command for incremental loading: {sql_command}")

                # Create the table
                self._execute_sql_command(conn, sql_command, description=f"Table {raw_table_name} creation from SQL script {sql_script_name}")
                logger.info(f"Successfully executed SQL script {sql_script_name} for file {full_filename}")

                # Copy data from S3 to Snowflake
                self._copy_into_snowflake(conn, s3_key, raw_table_name)
                
                # Log processed file
                self._log_processed_file(local_file_path)
                logger.info(f"Successfully logged processed file: {local_file_path}")

        except Exception as e:
            logger.error(f"Raw data ingestion failed. Error: {e}")
            raise

        finally:
            if conn:
                conn.close()


if __name__ == "__main__":
    # load dotenv
    load_dotenv()
    # Define paths relative to the project root
    project_dir = Path(__file__).resolve().parent # project directory
    schema_dir = str(project_dir / "sql" /  "tables")
    data_dir = str(project_dir / "data" / "processed_files")

    db_raw_ingestion = DB_Raw_Ingestion(data_folder=data_dir, schema_folder=schema_dir)
    db_raw_ingestion.ingest_data(is_full_refresh= True)

    
#%%