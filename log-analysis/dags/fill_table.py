from airflow.decorators import dag, task
from datetime import datetime 
import psycopg2
import os

@dag(
    default_args={
        "owner": "sem",
        "start_date": datetime(2024,4,5)
    },
        schedule_interval="@once",
        catchup=False, 
        tags=["archives-log-analysis"])

def fill_table():
    """Data from the aws raw bucket to populate the analytics postgres table"""
    # Define the tasks
    @task 
    def create_connection():
        """Create a connection to the database"""    
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

    create_connection_task = create_connection()

fill_table()