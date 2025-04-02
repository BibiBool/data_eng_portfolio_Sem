import os
import boto3
import json
import pandas as pd


def download_s3_logs (bucket_name: str, object_prefix: str, local_directory: str):
    """
    Downlaod the logs data from an s3 bucket based on given prefix

    Args:
        bucket_name (str): The name of the s3 bucket
        object_name: str: The name of the obkect in the s3 bucket
        fileame: str: The name of the file to save the data
         None
    """
    session = boto3.Session(profile_name='Developer-permission-set-713881793976')
    s3 = session.client('s3')

     # Ensure the local directory exists
    os.makedirs(local_directory, exist_ok=True)
    try:
        paginator = s3.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=bucket_name,
            Prefix=object_prefix)

        # print(json.dumps(response, indent=4, default=str))
        for page in pages:
            if 'Contents' in page:
                for obj in page['Contents']:
                    object_key = obj['Key']
                    filename = os.path.join(local_directory, os.path.basename(object_key))
                    s3.download_file(Bucket=bucket_name, Key=object_key, Filename=filename)
                    print(f"{object_key} downloaded to {filename}")
            else:
                print(f"No more objects found with prefix: {object_prefix} in this page")

    except Exception as e:
        print(f"An error occurred: {e}")

download_s3_logs(
    bucket_name='archives-nationales-logs',
    object_prefix='AWSLogs/713881793976/CloudFront/',
    local_directory='./log-analysis/CLoudFront/')
    

#logs_raw_df = pd.read_parquet('E4ZNTW6XOW5DN.2025-03-29-14.d610f678.parquet')
#print(logs_raw_df.head())