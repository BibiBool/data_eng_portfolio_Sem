from __future__ import annotations

from datetime import datetime, timedelta
import random
import uuid
import pandas as pd
import numpy as np 
import psycopg2 
from faker import Faker
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from utilities.convert_column import convert_dataframe_column_types

import os

@dag(
    dag_id="fixed_day_website_logs_generator",
    start_date=datetime(2025, 6, 25),
    schedule="@daily",
    catchup=False,
    tags=["data_engineering", "junior_engineer_practice", "logs", "parquet"],
    render_template_as_native_obj=True,
)
def website_fixed_day_logs_generator_dag():
    # This DAG generates a daily Parquet file containing simulated website logs.

    @task
    def generate_website_logs(num_entries: int = random.randrange(1001, 5000), execution_date_str : str = "{{ logical_date }}") -> str:
        """
        Generates a specified number of synthetic website log entries
        with realistic and varied data.
        """

        # Determine the date for log generation (yesterday's date relative to DAG run)
        log_date = execution_date_str

        fake = Faker()
        logs_data = []

        # To ensure at least 90% unique IPs
        unique_ips_to_generate = int(num_entries * 0.90)
        generated_ips = set()
        ip_pool = []

        # Generate unique IPs first
        print("Starting log generation process...")
        while len(generated_ips) < unique_ips_to_generate:
            ip = fake.ipv4_public()
            if ip not in generated_ips:
                generated_ips.add(ip)
                ip_pool.append(ip)

        # Add some duplicate IPs to fill the rest, ensuring non-consecutive
        # by shuffling or picking randomly from the unique set
        remaining_ips = num_entries - unique_ips_to_generate
        if remaining_ips > 0:
            for _ in range(remaining_ips):
                ip_pool.append(random.choice(list(generated_ips))) # Pick a random unique IP

        random.shuffle(ip_pool) # Shuffle to make sure IPs are non-consecutive

        # Pre-define some realistic values for certain columns
        http_methods = ["GET", "POST", "PUT", "DELETE", "HEAD", "OPTIONS"]
        sc_statuses = [200, 200, 200, 200, 404, 500, 301, 403] # More 200s
        x_edge_result_types = ["Hit", "Miss", "Error", "LimitExceeded"]
        ssl_protocols = ["TLSv1.2", "TLSv1.3", None]
        ssl_ciphers = ["ECDHE-RSA-AES128-GCM-SHA256", "AES256-GCM-SHA384", None]
        fle_statuses = ["", "Processed", "Failed"]
        x_edge_detailed_result_types = ["Hit", "Miss", "Error", "LambdaExecutionError", "BadGateway"]
        sc_content_types = ["text/html", "application/json", "image/jpeg", "text/css", "application/javascript"]

           # --- Start of new/modified code for cs_uri_stem ---
        data_engineering_uri_stems = [
        "/blog/what-is-data-engineering",
        "/blog/etl-vs-elt-difference",
        "/blog/apache-spark-beginners-guide",
        "/blog/data-lakes-vs-data-warehouses",
        "/blog/introduction-to-airflow-dags",
        "/blog/building-data-pipelines-python",
        "/blog/real-time-data-streaming-kafka",
        "/blog/data-governance-best-practices",
        "/blog/cloud-data-platforms-aws-azure-gcp",
        "/blog/data-modeling-techniques-data-engineers",
        "/blog/scalable-data-architecture",
        "/blog/monitoring-data-pipelines",
        "/blog/data-quality-checks-etl",
        "/blog/data-security-encryption",
        "/blog/data-versioning-lakehouse",
        "/blog/containerization-docker-kubernetes-data",
        "/blog/dataops-principles-practice",
        "/blog/machine-learning-ops-mldata",
        "/blog/graph-databases-data-engineering",
        "/blog/time-series-data-processing",
        "/blog/serverless-data-processing",
        "/blog/data-mesh-architecture",
        "/blog/streaming-etl-flink-spark-streaming",
        "/blog/data-catalog-importance",
        "/blog/metadata-management-data-lakes",
        "/blog/data-observability-tools",
        "/blog/data-lineage-tracking",
        "/blog/data-lake-security-best-practices",
        "/blog/modern-data-stack-components",
        "/blog/data-virtualization-use-cases",
        "/blog/data-marts-data-warehousing",
        "/blog/data-integration-strategies",
        "/blog/delta-lake-apache-iceberg",
        "/blog/data-pipeline-orchestration-tools",
        "/blog/cost-optimization-cloud-data",
        "/blog/data-governance-frameworks",
        "/blog/change-data-capture-cdc",
        "/blog/data-transformation-techniques",
        "/blog/choosing-right-database",
        "/blog/data-storage-solutions",
        "/blog/api-integration-data-pipelines",
        "/blog/data-quality-metrics",
        "/blog/building-api-gateways-data",
        "/blog/distributed-computing-data",
        "/blog/data-privacy-regulations-gdpr",
        "/blog/columnar-databases-analytics",
        "/blog/nosql-databases-data-engineering",
        "/blog/data-vault-modeling",
        "/blog/streaming-analytics-applications",
        "/blog/realtime-dashboards-data",
        "/about",
        "/blog",
        "/blog/search",
        "/blog/category",
    ]
    # --- End of new/modified code for cs_uri_stem ---

        print("Generating log entries...")
        for i in range(num_entries):
            # Generate time for yesterday
            log_time = fake.time(pattern="%H:%M:%S", end_datetime=None) # Time of day

            log_entry = {
                "date": log_date,
                "time": log_time,
                "x_edge_location": fake.bothify(text='???###-E#', letters='ABCDEF'), # e.g., ORD3-E1, DFW1-E2
                "sc_bytes": random.randint(100, 100000),
                "c_ip": ip_pool[i], # Assign pre-generated IP
                "cs_method": random.choice(http_methods),
                "cs_Host": "d8bnainxtot1t.cloudfront.net",
                "cs_uri_stem": random.choice(data_engineering_uri_stems),
                "sc_status": random.choice(sc_statuses),
                "cs_Referer": fake.uri() if random.random() > 0.1 else "-", # 90% chance of referrer
                "cs_User_Agent": fake.user_agent(),
                "cs_uri_query": fake.uri_page() if random.random() > 0.5 else "", # 50% chance of query
                "cs_Cookie": fake.md5() if random.random() > 0.3 else "-", # 70% chance of cookie
                "x_edge_result_type": random.choice(x_edge_result_types),
                "x_edge_request_id": str(uuid.uuid4()),
                "x_host_header": "archivesnationales-ht.com",
                "cs_protocol": "HTTPS" if random.random() > 0.1 else "HTTP", # Mostly HTTPS
                "cs_bytes": random.randint(50, 5000),
                "time_taken": round(random.uniform(0.001, 2.5), 3),
                "x_forwarded_for": fake.ipv4_public() if random.random() > 0.2 else "-", # 80% chance of x-forwarded-for
                "ssl_protocol": random.choice(ssl_protocols),
                "ssl_cipher": random.choice(ssl_ciphers),
                "x_edge_response_result_type": random.choice(x_edge_result_types), # Often similar to request result type
                "cs_protocol_version": "HTTP/1.1" if random.random() > 0.2 else "HTTP/2.0",
                "fle_status": random.choice(fle_statuses),
                "fle_encrypted_fields": fake.word() if random.random() > 0.8 else "-", # Less frequent
                "c_port": random.randint(1024, 65535),
                "time_to_first_byte": round(random.uniform(0.001, 1.0), 3),
                "x_edge_detailed_result_type": random.choice(x_edge_detailed_result_types),
                "sc_content_type": random.choice(sc_content_types),
                "sc_content_len": random.randint(0, 500000), # Can be 0 for redirects/no content
                "sc_range_start": random.randint(0, 1000) if random.random() > 0.7 else None,
                "sc_range_end": random.randint(1001, 2000) if random.random() > 0.7 else None,
            }
            logs_data.append(log_entry)

        df = pd.DataFrame(logs_data)
        print(f"DataFrame shape: {df.shape}")

        output_filepath = None # Initialize to None in case of error before assignment
        try:
            output_directory = "/opt/airflow/raw_data"
            output_filename = f"website_logs_{log_date}.parquet"
            output_filepath = os.path.join(output_directory, output_filename)

            # Ensure the output directory exists
            os.makedirs(output_directory, exist_ok=True) # <--- This is the key line

            df.to_parquet(output_filepath, index=False)
            print(f"Generated {num_entries} log entries and saved to {output_filepath}")

        except OSError as e:
            # Catch OS-related errors like permission denied, disk full, invalid path
            print(f"Error creating directory or writing Parquet file: {e}")
            raise # Re-raise the exception to make the Airflow task fail
        except Exception as e:
            # Catch any other unexpected errors
            print(f"An unexpected error occurred during file generation: {e}")
            raise # Re-raise the exception to make the Airflow task fail

        return output_filepath

    @task
    def clean_and_convert_logs(file_path: str, execution_date_str : str = "{{ data_interval_end }}"):
        print(os.listdir("/opt/airflow/raw_data"))
        print(f"Attempting to process file: {file_path}")
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File not found at path: {file_path}")
        print(f"File found at: {file_path}")

        logs_df = pd.read_parquet(file_path)
        print(f"DataFrame shape: {logs_df.shape}")
        print(f"Dataframe colums from parquet files: {logs_df.columns} ")

        staging_area = f"/opt/airflow/raw_data/website_logs_staged_{execution_date_str}.csv"
        logs_df.columns = [col.lower() for col in logs_df.columns]
        logs_df = logs_df.replace("-", np.nan)

        logs_df['timestamp'] = pd.to_datetime(logs_df['date'] + ' ' + logs_df['time'], errors='raise')
        logs_df= logs_df.drop(columns=['date', 'time'])

        logs_df = logs_df.drop_duplicates(subset=['timestamp', 'x_edge_request_id', 'c_ip'])

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
        table_name = "cdn_logs"

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
            raise

    upsert_incremental_visits = SQLExecuteQueryOperator(
        task_id="upsert_incremental_visits",
        conn_id="postgres_default",
        sql="sql/incremental_visits.sql",
        )
    
    # Task dependencies
    log_file_path = generate_website_logs(execution_date_str="{{ logical_date.strftime('%Y-%m-%d') }}")
    cleaned_logs = clean_and_convert_logs(file_path=log_file_path)
    upload_to_postgres(staging_area=cleaned_logs) >> upsert_incremental_visits

website_fixed_day_logs_generator_dag()