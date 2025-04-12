from airflow.decorators import dag, task
from datetime import datetime
import psycopg2
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
def fill_table_v1():
    """Data from the aws raw bucket to populate the analytics postgres table"""
    # Define the tasks
    @task
    def check_database_connection_v1():
        """Check the connection to the database"""
        try:
            conn = psycopg2.connect(
                dbname=os.getenv("AIRFLOW_DATABASE_DBNAME"),
                user=os.getenv("AIRFLOW_DATABASE_USER"),
                password=os.getenv("AIRFLOW_DATABASE_PASSWORD"),
                host=os.getenv("AIRFLOW_DATABASE_HOST")
            )
            print("Connection to database successful")

            cursor = conn.cursor()
            cursor.execute("""
                SELECT datname
                FROM pg_database
                WHERE datistemplate = false; """)
            databases = cursor.fetchall()
            print("Databases:", databases)
            return True
        except Exception as e:
            print(f"Connection to database failed: {e}")
            print("Verify your credentials in the .env file and Docker Compose configuration.")
            raise 
        finally:
            if conn:
                conn.close()

    @task
    def create_table():
        conn = None
        cursor = None
        try:
            conn = psycopg2.connect(
                dbname=os.getenv("AIRFLOW_DATABASE_DBNAME"),
                user=os.getenv("AIRFLOW_DATABASE_USER"),
                password=os.getenv("AIRFLOW_DATABASE_PASSWORD"),
                host=os.getenv("AIRFLOW_DATABASE_HOST")
            )
            cursor = conn.cursor()
            # Create table if it doesn't exist
            cursor.execute("""
                           CREATE TABLE IF NOT EXISTS Archives_logs(
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
                               sc_range_end INTEGER)
                           """)
            conn.commit()

            print("Table 'Archives_logs' created successfully.")
        except psycopg2.Error as e:
            print(f"Error creating table: {e}")
            raise 
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()        

    check_db_task = check_database_connection_v1()
    create_table_task = create_table()

    check_db_task >> create_table_task

fill_table_v1()