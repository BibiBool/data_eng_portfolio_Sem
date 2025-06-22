from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, concat_ws, to_timestamp, year, month, dayofmonth, hour
from pyspark.sql.types import TimestampType, LongType, DoubleType, IntegerType

def compact_raw_logs(input_path="/home/jupyter/raw_data/AWSLogs/713881793976/CloudFront/*", 
                     output_path="/home/jupyter/raw_consolidated_data/AWSLogs/713881793976/CloudFront/*"):

    spark = SparkSession.builder.appName("compact_raw_logs").master("local[*]").getOrCreate()

    try:
        raw_logs_df = spark.read.parquet(input_path)

        # Merge "date" and "time" into "timestamp"
        raw_consolidated_data_df = raw_logs_df.withColumn("timestamp", to_timestamp(concat_ws(" ", col("date"), col("time")), "yyyy-MM-dd HH:mm:ss"))

        # Correct colum types
        raw_consolidated_data_df = raw_consolidated_data_df.withColumn("sc_bytes", col("sc_bytes").cast(LongType())) \
                                                            .withColumn("cs_bytes", col("cs_bytes").cast(LongType())) \
                                                            .withColumn("cs_bytes", col("cs_bytes").cast(LongType())) \
                                                            .withColumn("cs_bytes", col("cs_bytes").cast(LongType())) \
                                                            .withColumn("cs_bytes", col("cs_bytes").cast(LongType())) \

        # Handle columns where empty values are represented as '-'
        logs_df_cleaned = logs_df_cleaned.withColumn("sc_range_start", expr("CASE WHEN sc_range_start = '-' THEN NULL ELSE sc_range_start END").cast(LongType())) \
                                         .withColumn("sc_range_end", expr("CASE WHEN sc_range_end = '_' THEN NULL ELSE sc_range_end END").cast(LongType()))

        # Handle potential nulls in the new timestamp column (from parsing errors)
        initial_row_count = logs_df_cleaned.count()
        logs_df_cleaned = logs_df_cleaned.na.drop(subset=["timestamp"])
        if logs_df_cleaned.count() < initial_row_count:
            print(f"Number of rows dropped: {initial_row_count - logs_df_cleaned}.")

        # Extract partitioning columns (year, month, day, hour) from the new 'timestamp' column
        logs_df_cleaned = logs_df_cleaned.withColumn("year", year(col("timestamp"))) \
                                         .withColumn("month", month(col("timestamp"))) \
                                         .withColumn("day", dayofmonth(col("timestamp"))) \
                                         .withColumn("hour", hour(col("timestamp")))

        # Drop 'date' and 'time'
        logs_df_cleaned = logs_df_cleaned.drop("date", "time")

        print("\nSchema after cleaning, type casting, and adding partitioning columns:")
        logs_df_cleaned.printSchema()

        print(f"\nWriting cleaned and repartitioned data to: {output_path}")

        # Save data partitionned by year, month, day, hour
        logs_df_cleaned.write.mode("overwrite").partitionBy("year", "month", "day", "hour").parquet("/home/jupyter/clean_data/AWSLogs/")


        print("Repartitioning complete. Data saved to:", output_path)
    except Exception as e:
        print(f"Ane error has occurred during log processing: {e}")
        raise 
    finally:
        #spark.stop()
        print("Sprak session has stopped.")

if __name__ == "__main__":
    compact_raw_logs()