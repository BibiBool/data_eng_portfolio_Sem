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
            conn.close()
            return databases
        except Exception as e:
            print(f"Connection to database failed: {e}")
            print("Verify your credentials in the .env file and Docker Compose configuration.")
            raise  # It's good practice to re-raise the exception for Airflow to track failure

    check_db_task = check_database_connection_v1()

fill_table_v1()