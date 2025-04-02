import os
import dotenv
import psycopg2
import pandas as pd

# Load .env file
dotenv.load_dotenv()

# To do
#Connect to postgres databse
# Create table
# Fill table with data

def create_connection():
    """ Create a database connection to the postgresql database"""
    
    try:
        conn = psycopg2.connect(
            dbname=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            host=os.getenv("DB_HOST"),
            port=os.getenv("DB_PORT")
        )
        print("Connection to database successful")
    except Exception as e:
        print(e)
        print("Connection to database failed. Verify your credentials in the .env file")

create_connection()


def create_table():
    try:
        conn = create_connection()
        cursor = conn.cursor()

        # Create table if it doesn't exist
        cursor.execute("""
                       CREATE TABLE IF NOT EXISTS Archives-logs(
                           id SERIAL PRIMARY KEY,
                           date VARCHAR(255),
                           time VARCHAR(255),
                           x_edge_location VARCHAR(255),
                           sc_bytes INTEGER,
                           c_ip VARCHAR(255),
                           cs_method VARCHAR(255),
                           cs_Host VARCHAR(255),
                           cs_uri_stem TEXT,
                           sc_status INTEGER,
                           cs_Referer TEXT,
                           cs_User_Agent TEXT,
                           cs_uri_query TEXT,
                           cs_Cookie TEXT,
                           x_edge_result_type VARCHAR(255),
                           x_edge_request_id VARCHAR(255),
                           x_host_header VARCHAR(255),
                           cs_protocol VARCHAR(255),
                           cs_bytes INTEGER,
                           time_taken FLOAT,
                           x_forwarded_for VARCHAR(255),
                           ssl_protocol VARCHAR(255),
                           ssl_cipher VARCHAR(255),
                           x_edge_response_result_type VARCHAR(255),
                           cs_protocol_version VARCHAR(255),
                           fle_status VARCHAR(255),
                           fle_encrypted_fields VARCHAR(255),
                           c_port INTEGER,
                           time_to_first_byte FLOAT,
                           x_edge_detailed_result_type VARCHAR(255),
                           sc_content_type VARCHAR(255),
                           sc_content_len INTEGER,
                           sc_range_start INTEGER,
                           sc_range_end INTEGER
                       )
                       USING iceberg
                       PARTITIONNED BY (date)
                       """)
        conn.commit()

        print("Table 'Archives_logs' created successfully.")
    except psycopg2.Error as e:
        print(f"Error creating table: {e}")

    finally:
        if conn:
            cursor.close()
            conn.close()

def aggregate_data(path:str):
    """ Aggregate all Parquet files in the directory before uploading them to 
    the database, tracking the number of aggregated and remaining files"""
    try:
        parquet_files = [file for file in os.listdir(path) if file.endswith(".parquet")]
        total_files = len(parquet_files)
        aggregated_count = 0

        logs_df = pd.DataFrame()

        for file in parquet_files:
            print(f"Aggreating {file} ({aggregated_count} +1)/{total_files})")
                
            file_df = pd.read_parquet(os.path.join(path, file))
            logs_df = pd.concat([logs_df, file_df], ignore_index=True)
            aggregated_count += 1
            print(f"Aggregated ({aggregated_count} +1)/{total_files})")

        print(f"Aggregation complete. Total files aggregated: {aggregated_count}/{total_files}")
        print(logs_df.head())
        print(logs_df.info())
        return logs_df #return the aggregated dataframe.
    
    except Exception as e:
        print(f"Aggregation of parquet files failed: {e}")
        return None
    
def fill_table():
    """ Fill the table with data from the aggregated dataframe"""
    try:
        # Connect to the database
        conn = create_connection()
        
        # Create a cursor object
        cursor = conn.cursor()

        # Create table if it doesn't exist
        create_table()

        # Load the aggregated data into the database
        logs_df = aggregate_data("./log-analysis/CLoudfront/")
        logs_df.to_sql('logs', conn, if_exists='append', index=False)

        # Commit the changes and close the connection
        conn.commit()
        cursor.close()
        conn.close()

    except Exception as e:
        print(f"Filling table failed: {e}")
        return None
