from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import psycopg2
from psycopg2 import sql
from utilities.convert_column import convert_dataframe_column_types
import os


@dag(
    default_args={
        "owner": "sem",
        "start_date": datetime(2025, 6, 25)
    },
    schedule_interval="@daily",
    catchup=False,
    tags=["data_engineering", "junior_engineer_practice", "logs", "parquet"]
)
def upload_data_to_postgres_dag():
    """Upload data from the aws raw bucket to the analytics postgres table"""

    @task
    def download_aws_log(execution_date_str : str = "{{ logical_date }}"):
        """Download the aws log file from the S3 bucket"""
        # **IMPLEMENT YOUR S3 DOWNLOAD LOGIC HERE**
        local_directory= "/opt/airflow/raw_data"
        local_filename = f"website_logs_{execution_date_str}.parquet"
        file_path = os.path.join(local_directory, local_filename)
        print(f"Downloaded file to: {file_path}")
        return file_path
    
    @task
    def clean_and_convert_logs(file_path: str, execution_date_str : str = "{{ logical_date }}"):
        print(os.listdir("/opt/airflow/raw_data"))
        print(f"Attempting to process file: {file_path}")
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File not found at path: {file_path}")
        print(f"File found at: {file_path}")

        logs_df = pd.read_parquet(file_path)
        print(f"DataFrame shape: {logs_df.shape}")

        staging_area = f"/opt/airflow/raw_data/website_logs_staged_{execution_date_str}.csv"
        logs_df.columns = [col.lower() for col in logs_df.columns]
        logs_df = logs_df.replace("-", np.nan)
        clean_logs_df = convert_dataframe_column_types(logs_df)
        clean_logs_df.to_csv(staging_area, index=False)

        print(f"Cleaned columns: {clean_logs_df.columns}")
        return staging_area
        
    @task    
    def upload_to_postgres(staging_area):
        print(f"Attempting to process file: {staging_area}")
        if not os.path.exists(staging_area):
            raise FileNotFoundError(f"File not found at path: {staging_area}")
        print(f"File found at: {staging_area}")

        clean_logs_df = pd.read_csv(staging_area)

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
            print(f"Error during database upload: {e}")
            raise # Re-raise the exception to mark the Airflow task as failed


    # Define the task dependency and pass the output of download_aws_log
    downloaded_file_path = download_aws_log(execution_date_str = "{{ logical_date.strftime('%Y-%m-%d')}}")
    cleaned_logs = clean_and_convert_logs(file_path=downloaded_file_path, execution_date_str = "{{ logical_date.strftime('%Y-%m-%d')}}")
    upload_to_postgres(staging_area=cleaned_logs) 

upload_data_to_postgres_dag()
             