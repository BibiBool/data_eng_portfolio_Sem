from airflow.decorators import dag, task
from datetime import datetime
import psycopg2
import os

# This DAG creates a PostgreSQL table for storing logs from AWS S3 bucket
@dag(
    default_args={
        "owner": "sem",
        "start_date": datetime(2024, 4, 5)
    },
    schedule="@once",
    catchup=False,
    tags=["data_engineering", "junior_engineer_practice", "logs", "parquet"]
)
def dynamic_table_creator():
    # Data from the aws raw bucket to populate the analytics postgres table

    # First we defined the dictionary
    table_schemas = {
        "cdn_logs": [
            ("x_edge_request_id", "VARCHAR(255)"),
            ("timestamp", "TIMESTAMP"), 
            ("x_edge_location", "VARCHAR(255)"),
            ("sc_bytes", "INTEGER"),
            ("c_ip", "VARCHAR(255)"),
            ("cs_method", "VARCHAR(255)"),
            ("cs_Host", "VARCHAR(255)"),
            ("cs_uri_stem", "TEXT"),
            ("sc_status", "INTEGER"),
            ("cs_Referer", "TEXT"),
            ("cs_User_Agent", "TEXT"),
            ("cs_uri_query", "TEXT"),
            ("cs_Cookie", "TEXT"),
            ("x_edge_result_type", "VARCHAR(255)"),
            ("x_host_header", "VARCHAR(255)"),
            ("cs_protocol", "VARCHAR(255)"),
            ("cs_bytes", "INTEGER"),
            ("time_taken", "FLOAT"),
            ("x_forwarded_for", "VARCHAR(255)"),
            ("ssl_protocol", "VARCHAR(255)"),
            ("ssl_cipher", "VARCHAR(255)"),
            ("x_edge_response_result_type", "VARCHAR(255)"),
            ("cs_protocol_version", "VARCHAR(255)"),
            ("fle_status", "VARCHAR(255)"),
            ("fle_encrypted_fields", "VARCHAR(255)"),
            ("c_port", "INTEGER"),
            ("time_to_first_byte", "FLOAT"),
            ("x_edge_detailed_result_type", "VARCHAR(255)"),
            ("sc_content_type", "VARCHAR(255)"),
            ("sc_content_len", "INTEGER"),
            ("sc_range_start", "INTEGER"),
            ("sc_range_end", "INTEGER"),
            ("created_at", "TIMESTAMP", "default current_timestamp"),
            "PRIMARY KEY (x_edge_request_id, timestamp, c_ip)"
        ],
        "aggr_visits": [
            ("id", "SERIAL"),
            ("date", "date"),
            ("cs_uri_stem", "VARCHAR(255)"),
            ("visits", "INTEGER"),
            "PRIMARY KEY (date, cs_uri_stem, visits)"
        ]
    }

    @task
    def check_database_connection_v1():
        # Check the connection to the database
        try:
            conn = psycopg2.connect(
                dbname=os.getenv("DATA_DB_DBNAME"),
                user=os.getenv("DATA_DB_USER"),
                password=os.getenv("DATA_DB_PASSWORD"),
                host=os.getenv("DATA_DB_HOST")
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
            print("Check your credentials in the .env file and Docker Compose configuration.")
            raise 
        finally:
            if conn:
                conn.close()

    @task
    def create_dynamic_table(table_name: str, columns_and_constraints: list):
        conn = None
        cursor = None
        try:
            conn = psycopg2.connect(
                dbname=os.getenv("DATA_DB_DBNAME"),
                user=os.getenv("DATA_DB_USER"),
                password=os.getenv("DATA_DB_PASSWORD"),
                host=os.getenv("DATA_DB_HOST")
            )
            cursor = conn.cursor()

            column_definitions = []
            table_constraints = []
            for item in columns_and_constraints:
                if isinstance(item, tuple): # It's a column definition
                    if len(item) == 3:
                        column_definitions.append(f"{item[0]} {item[1]} {item[2]}")
                    else: # Assuming length 2 for (name, type)
                        column_definitions.append(f"{item[0]} {item[1]}")
                elif isinstance(item, str): # It's a table-level constraint string
                    table_constraints.append(item)

            all_definitions = column_definitions + table_constraints

            create_table_sql = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                {", ".join(all_definitions)}
            );"""

            # Create table if it doesn't exist
            cursor.execute(create_table_sql)
            conn.commit()
            print(f"Table {table_name} created successfully.")

            # Verify table creation and columns
            cursor.execute("""
                           SELECT tablename
                           FROM pg_catalog.pg_tables
                           WHERE schemaname!= 'pg_catalog' AND schemaname != 'information_schema';
                           """)
            tables = cursor.fetchall()
            print(f"\nTables in the database after creating {table_name}:")
            if tables:
                for table in tables:
                    print(f"- {table[0]}")
            else:
                print("No tables found (other tan system tables).")

            print(f"\nColumns of '{table_name}' :")
            cursor.execute(f"""
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_schema = 'public' 
                AND table_name = '{table_name}' 
                ORDER BY ordinal_position;
            """)
            columns = cursor.fetchall()
            if columns:
                for col in columns:
                    print(f"- {col[0]} ({col[1]})")
            else:
                print(f"No columns found for '{table_name}'. Check table name and schema.")
        
        except psycopg2.Error as e:
            print(f"Error creating table '{table_name}': {e}")
            raise 
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()        

    check_db_task = check_database_connection_v1()

    # Dinamycally create tasks for each table in table_schemas
    create_table_tasks = []

    for table_name, columns_and_constraints in table_schemas.items():
        task_id = f"Create_{table_name}_table"
        create_table_task = create_dynamic_table.override(task_id=task_id)(
            table_name=table_name, 
            columns_and_constraints=columns_and_constraints
        )
        create_table_tasks.append(create_table_task)

    check_db_task >> create_table_tasks

dynamic_table_creator()