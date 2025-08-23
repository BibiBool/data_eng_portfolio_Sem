from airflow.sdk import dag, task
import time
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import psycopg2
from utilities.convert_column import convert_dataframe_column_types
import tempfile
import os
import shutil
import json
import logging 
import re 

from airflow.providers.amazon.aws.hooks.s3 import S3Hook

# Basic logging
log = logging.getLogger(__name__)

# Buckets and paths
S3_BUCKET_NAME = "archives-nationales-logs"
S3_BASE_FIXED_PATH = "AWSLogs/713881793976/CloudFront/"
AIRFLOW_TEMP_S3_BUCKET ='archives-nationales-logs' # <<< CHANGE THIS to your temporary S3 bucket
AIRFLOW_TEMP_S3_PREFIX = 'AWSLogs/713881793976/airflow-tmp/cloudfront-log-processor/'

def task_failure_alert():
    pass

@dag(
    schedule='@daily',
    start_date=datetime(2025, 7, 25),
    catchup=False,
    tags=["s3", "cloudfront", "logs", "processing", "error-handling", "data-engineering"],
    default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'email_on_failure': False, # Using custom callback
        'email_on_retry': False,   # Using custom callback
        'retries': 3,
        'retry_delay': timedelta(minutes=5),
        'on_failure_callback': task_failure_alert,
    }
)

def s3_log_processing_pipeline_v4():
    """
    ### Cloudfrot Log Processor
    This DAG processes the logs frmo s3
    """
    @task()
    def _get_s3_daily_logs(bucket_name: str, 
                           base_fixed_path: str, 
                           temp_s3_bucket: str, 
                           temp_s3_prefix: str,
                           data_interval_start_str: str
                           ) -> str | None:
        """
        Retrieves s3 keys for specified date and save the list as a JSON file to 
        a temporary S3 location
        """
        s3_hook = S3Hook(aws_conn_id='aws_sem')

        # Convert data_interval_start_str to datetime object
        data_interval_start = datetime.fromisoformat(data_interval_start_str.replace('Z', '+00:00'))
        target_date_str = data_interval_start.strftime('%Y-%m-%d')

        # Regex to extact the date from the key
        date_pattern = re.compile(r'\.(\d{4}-\d{2}-\d{2})-\d{2}\..*\.parquet$')

        log.info(f"Listing S3 keys in bucket: {bucket_name} with base prefix: {base_fixed_path}")
        log.info(f"Targeting logs for date: {target_date_str}")

        try:
            all_found_keys = s3_hook.list_keys(bucket_name=bucket_name, prefix=base_fixed_path)
            if not all_found_keys:
                log.warning(f"No S3 keys found under prefix {base_fixed_path} in bucket {bucket_name}.")
                return None
        except Exception as e:
            log.error(f"Failed to list S3 keys for prefix {base_fixed_path} in bucket {bucket_name}: {e}")
            raise 

        # Filter keys for the target date
        target_date_keys = []
        for key in all_found_keys:
            match = date_pattern.search(key)
            if match:
                key_date = match.group(1)
                if key_date == target_date_str:
                    target_date_keys.append(key)
                else:
                    log.debug(f"Key did not match expected regex pattern, skipping: {key}")
    
        if not target_date_keys:
            log.warning(f"No S3 keys found for date {target_date_str} under prefix {base_fixed_path}")
        else:
            log.info(f"Found {len(target_date_keys)} keys for processing date {data_interval_start}:")

        intermediate_file_s3_key = f"{temp_s3_prefix}{target_date_str}/s3_keys_list_{target_date_str}.json"

        try:
            #Write the keys to a temporary json file
            with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.json') as temp_json_file:
                json.dump(target_date_keys, temp_json_file)
                local_json_path = temp_json_file.name
                log.info(f"Temporary local JSON file created at: {local_json_path}")

            # Uplaod json to S3
            s3_hook.load_file(filename=local_json_path, 
                              key=intermediate_file_s3_key, 
                              bucket_name=temp_s3_bucket, 
                              replace=True)
            log.info(f"Succesfully uploaed key list to s3://{temp_s3_bucket}{intermediate_file_s3_key}")

        except Exception as e:
            log.error(f"Failed to create or upload intermediate key list to S3: {e}")
        finally:
            #Clean up local temp file
            if os.path.exists(local_json_path):
                time.sleep(5)
                os.remove(local_json_path)
                log.info(f"Cleaned up local json file: {local_json_path}")

        return intermediate_file_s3_key

    @task()
    def _download_s3_files_from_list(intermediate_file_s3_key: str, source_bucket_name: str):
        """
        Dowmloads the list of json file from the intermediate json file and 
        iterates and process each file
        """

        if intermediate_file_s3_key is None:
            log.info("No keys were found in the previous step. Skipping processing.")
            return
        
        s3_hook = S3Hook(aws_conn_id='aws_sem')

        # Directory for downloaded files
        local_dir_path = f"/opt/airflow/raw_data/{intermediate_file_s3_key.split('/')[-1].replace('.json', '')}"
        os.makedirs(local_dir_path, exist_ok=True)

        # Get the filename from the s3 key
        local_json_filename = os.path.basename(intermediate_file_s3_key)
        # Construct the full local file path
        local_json_file_path = os.path.join(local_dir_path, local_json_filename)

        # STEP 1: Download the log files
        try:                
            s3_hook.download_file(
                key=intermediate_file_s3_key,
                bucket_name=AIRFLOW_TEMP_S3_BUCKET,
                local_path=local_dir_path,
                use_autogenerated_subdir=False,
                preserve_file_name=True
            )
            log.info(f"Downloaded key list from s3://{AIRFLOW_TEMP_S3_BUCKET}/{intermediate_file_s3_key} to {local_dir_path}")

            # Read the list of S3 keys from the downloaded json file
            with open(local_json_file_path, 'r') as f:
                    s3_keys_to_process = json.load(f)
                    log.info("Contents of the downloaded json file:")
                    log.info(s3_keys_to_process)

            if not s3_keys_to_process:
               log.warning(f"Intermediate JSON file was empty. No keys to process.")
               return
            
            log.info(f"Starting processing of {len(s3_keys_to_process)} log files.")
            for s3_key in s3_keys_to_process:
                #local_log_file_path = os.path.join(local_dir_path, os.path.basename(s3_key).)

                try:
                    s3_hook.download_file(
                        key=s3_key,
                        bucket_name=source_bucket_name,
                        local_path=local_dir_path,
                        use_autogenerated_subdir=False,
                        preserve_file_name=True
                    )
                    log.info(f"Downloaded {s3_key} to {local_dir_path}")

                except Exception as e:
                    log.error(f"Failed to process individual log file {s3_key}: {e}")
                    raise

            log.info(f"Finished processing of all log files from the list")

            # STEP 2: Process the data from the log files
            

        except Exception as e:
            log.error(f"An error occurred while handling the intermediate JSON file or overall processing: {e}")
            log.info("Erasing temporary folders...")
            
    
        return local_dir_path
    
    @task
    def clean_and_convert_logs(local_dir_path: str, execution_date_str : str = "{{ data_interval_end }}"):
        log.info(os.listdir("/opt/airflow/raw_data"))
        log.info(f"Attempting to process file: {local_dir_path}")
        if not os.path.exists(local_dir_path):
            raise FileNotFoundError(f"File not found at path: {local_dir_path}")
        log.info(f"File found at: {local_dir_path}")

        import glob 
        parquet_files = glob.glob(os.path.join(local_dir_path, '*.parquet'))

        if not parquet_files:
            log.warning(f"No parquet files found in directory: {local_dir_path}")
            return
        log.info(f"Found {len(parquet_files)} Parquet files to process")

        logs_df = pd.read_parquet(parquet_files)
        log.info(f"DataFrame shape: {logs_df.shape}")

        staging_area = f"/opt/airflow/raw_data/website_logs_staged_{execution_date_str}.csv"
        logs_df.columns = [col.lower() for col in logs_df.columns]
        logs_df = logs_df.replace("-", np.nan)
        clean_logs_df = convert_dataframe_column_types(logs_df)
        clean_logs_df.to_csv(staging_area, index=False)

        log.info(f"Cleaned columns: {clean_logs_df.columns}")
        return local_dir_path


    @task()
    def _clean_temp_folders(local_dir_path: str):
        """Clean all temporary folders."""

        try:
            if os.path.exists(local_dir_path):
                time.sleep(10)
                shutil.rmtree(local_dir_path)
                log.info(f"Cleaned up local intermediate JSON folder and files: {local_dir_path}")
        
        except Exception as e:
            log.error("An error occured while erasing temporary folders.")
            raise
   
    # --- Task Instantiation and Dependencies ---
    
    list_s3_daily_log = _get_s3_daily_logs(
        bucket_name=S3_BUCKET_NAME,
        base_fixed_path=S3_BASE_FIXED_PATH,
        temp_s3_bucket=AIRFLOW_TEMP_S3_BUCKET,
        temp_s3_prefix=AIRFLOW_TEMP_S3_PREFIX,
        data_interval_start_str="{{ data_interval_start }}",
    )

    donwlaod_logs = _download_s3_files_from_list(
        intermediate_file_s3_key=list_s3_daily_log, # Pass output from upstream task
        source_bucket_name=S3_BUCKET_NAME
    )

    process_logs = clean_and_convert_logs(local_dir_path=donwlaod_logs)

    clean_temp_folders = _clean_temp_folders(
        local_dir_path=process_logs)


    list_s3_daily_log >> donwlaod_logs >> process_logs >> clean_temp_folders

s3_log_processing_pipeline_v4()


      