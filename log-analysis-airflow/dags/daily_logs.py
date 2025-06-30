from __future__ import annotations

from datetime import datetime, timedelta
import random
import uuid
import pandas as pd
from faker import Faker
from airflow.decorators import dag, task
import os

@dag(
    dag_id="website_logs_generator",
    start_date=datetime(2025, 6, 25),
    schedule="*/2 * * * *",
    catchup=False,
    tags=["data_engineering", "junior_engineer_practice", "logs", "parquet"],
    render_template_as_native_obj=True,
)
def website_logs_generator_dag():
    # This DAG generates a daily Parquet file containing simulated website logs.

    @task
    def generate_website_logs(num_entries: int = 100, execution_date_str : str = "{{ logical_date }}") -> str:
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
                "cs_Host": fake.domain_name(),
                "cs_uri_stem": fake.uri_path(),
                "sc_status": random.choice(sc_statuses),
                "cs_Referer": fake.uri() if random.random() > 0.1 else "-", # 90% chance of referrer
                "cs_User_Agent": fake.user_agent(),
                "cs_uri_query": fake.uri_page() if random.random() > 0.5 else "", # 50% chance of query
                "cs_Cookie": fake.md5() if random.random() > 0.3 else "-", # 70% chance of cookie
                "x_edge_result_type": random.choice(x_edge_result_types),
                "x_edge_request_id": str(uuid.uuid4()),
                "x_host_header": fake.domain_name(),
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
            # output_directory = "/home/sem/data-portfolio/log-analysis-airflow"
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
    def process_logs(filepath: str):
        """
        A placeholder task for further processing of the generated logs.
        Junior engineers can expand on this task.
        """
        print(f"Starting processing for log file: {filepath}")
        # Here, a junior engineer could add code to:
        # - Load the parquet file: df = pd.read_parquet(filepath)
        # - Perform data cleaning, transformation, or analysis
        # - Store the processed data in another format or database
        print("Log processing completed (placeholder).")


    # Task dependencies
    log_file_path = generate_website_logs(execution_date_str="{{ logical_date.strftimr('%Y-%m-%d') }}")
    process_logs(filepath=log_file_path)

website_logs_generator_dag()