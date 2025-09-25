# import statements
import datetime 
import tempfile
import os 
import re 
import logging

from airflow.providers.amazon.aws.hooks.s3 import S3Hook

log = logging.getLogger(__name__)

# Processing logic: listering and filtering logs
# create a hook to s3

def list_s3_daily_keys(bucket_name: str, 
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
