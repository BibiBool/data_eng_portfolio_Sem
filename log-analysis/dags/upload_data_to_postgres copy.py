from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import pandas as pd
import numpy as np
import psycopg2
from psycopg2 import sql
from lib.convert_column import convert_dataframe_column_types
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
def upload_data_to_postgres_dag_v2():
    """Upload data from the aws raw bucket to the analytics postgres table"""

    @task
    def download_aws_log():
        """Download the aws log file from the S3 bucket"""
        # **IMPLEMENT YOUR S3 DOWNLOAD LOGIC HERE**
        # This is a placeholder - replace with actual code to download
        # the file and return the local path.
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

        staging_area = "/opt/airflow/raw_data/E4ZNTW6XOW5DN.b3632fd3.csv"
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
        try:
            conn = psycopg2.connect(
                dbname=os.getenv("DATA_DB_DBNAME"),
                user=os.getenv("DATA_DB_USER"),
                password=os.getenv("DATA_DB_PASSWORD"),
                host=os.getenv("DATA_DB_HOST")
            )
            cursor= conn.cursor()

            for index, row in clean_logs_df.iterrows():
                columns = clean_logs_df.columns.tolist()
                placeholders = (', ').join(['%s'] * len(columns))
                insert_sql = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
                    sql.Identifier("archives_logs"),
                    sql.SQL(', ').join(map(sql.Identifier, columns)),
                    sql.SQL(placeholders)
                )
                try:
                    cursor.execute(insert_sql, row.tolist())
                except psycopg2.Error as e:
                    print(f"Error inserting row {index}: {e}")
                    conn.rollback() # Rollback the transaction if an error occurs

            conn.commit()
            print(f"{len(clean_logs_df)} rows successfully inserted into the Archives_logs table")

        except psycopg2.Error as e:
            print(f"Error connecting or interacting with the database: {e}")
            if conn:
                cursor.close()
                conn.close()
                print("database connection closed")
        finally:
            if conn:
                cursor.close()
                conn.close()
                print("database connection closed")

    # Define the task dependency and pass the output of download_aws_log
    downloaded_file_path = download_aws_log()
    cleaned_logs = clean_and_convert_logs(file_path=downloaded_file_path)
    upload_to_postgres(file_path=cleaned_logs) 

upload_data_to_postgres_dag_v2()
             