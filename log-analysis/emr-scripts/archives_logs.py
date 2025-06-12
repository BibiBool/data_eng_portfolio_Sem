import argparse
from pyspark.sql import SparkSession

def process_and_clean_logs(input_path, output_path):
    """
    Process and clean small log files in parquet format from the input path,
    clean them, writes bigger, cleaned parquet files 
    and save to the output path.
    """

    with SparkSession.builder.appName("DataCleaningAndCompaction").getOrCreate() as spark:
        # Extract Phase: Read parquet files from the input path
        print(f"Reading parquet file from {input_path}")
        try:
            raw_logs_df = spark.read.parquet(input_path)
            print(f"Succesfully read {raw_logs_df.count()} records.")
            raw_logs_df.printSchema()
            raw_logs_df.show(5)

        except Exception as e:
            print(f"Error reading parquet file: {e}")
            raise e

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process and clean log files.")
    parser.add_argument("--input_path", type=str, required=True, help="Path to the input parquet files.")
    parser.add_argument("--output_path", type=str, required=True, help="Path to save the cleaned parquet files.")

    args = parser.parse_args()

    process_and_clean_logs(args.input_path, args.output_path)
        