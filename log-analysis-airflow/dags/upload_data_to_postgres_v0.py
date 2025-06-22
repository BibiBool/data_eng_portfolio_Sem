from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import pandas as pd
import numpy as np
import psycopg2
from psycopg2 import sql
from utilities.convert_column import convert_dataframe_column_types
import os


@dag(
    default_args={
        "owner": "sem",
        "start_date": datetime(2024, 4, 5)
    },
    schedule_interval="@once",
    catchup=False,
    tags=["archives-log-analysis"]
)
def upload_data_to_postgres_dag():
    """Upload data from the aws raw bucket to the analytics postgres table"""

    @task
    def download_aws_log():
        """Download the aws log file from the S3 bucket"""
        # **IMPLEMENT YOUR S3 DOWNLOAD LOGIC HERE**
        local_file_path = "/opt/airflow/raw_data/E4ZNTW6XOW5DN.2025-04-12-15.b3632fd3.parquet"
        print(f"Downloaded file to: {local_file_path}")
        return local_file_path
    
    @task
    def clean_and_convert_logs(file_path):
        print(f"Attempting to process file: {file_path}")
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File not found at path: {file_path}")
        print(f"File found at: {file_path}")

        logs_df = pd.read_parquet(file_path)
        print(f"DataFrame shape: {logs_df.shape}")

        staging_area = "/opt/airflow/raw_data/E4ZNTW6XOW5DN.2025-04-12-15.b3632fd3.csv"
        logs_df.columns = [col.lower() for col in logs_df.columns]
        logs_df = logs_df.replace("-", np.nan)
        clean_logs_df = convert_dataframe_column_types(logs_df)
        clean_logs_df.to_csv(staging_area, index=False)
        #print(f"Cleaned columns: {clean_logs_df.columns}")
        return staging_area
        
    @task    
    def upload_to_postgres(file_path):
        print(f"Attempting to process file: {file_path}")
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File not found at path: {file_path}")
        print(f"File found at: {file_path}")

        clean_logs_df = pd.read_csv(file_path)

        # Ensure you have registered your Postgres connection in Airflow with the ID 'postgres_default'
        pg_hook = PostgresHook(postgres_conn_id='postgres_default')
        table_name = "raw_cdn_logs"

        try:
            print(f"Initiating bulk upload to PostgreSQL table: {table_name}")
            clean_logs_df.to_sql(
                name=table_name,
                con=pg_hook.get_sqlalchemy_engine(),
                if_exists='append',
                index=False
            )
            print(f"{len(clean_logs_df)} rows successfully inserted into the {table_name} table.")

        except psycopg2.Error as e:
             # Catch broader exceptions for database interaction errors
            print(f"Error during database upload: {e}")
            raise # Re-raise the exception to mark the Airflow task as failed
        finally:
            print("database connection closed")

    # Define the task dependency and pass the output of download_aws_log
    downloaded_file_path = download_aws_log()
    cleaned_logs = clean_and_convert_logs(file_path=downloaded_file_path)
    upload_to_postgres(file_path=cleaned_logs) 

upload_data_to_postgres_dag()
             