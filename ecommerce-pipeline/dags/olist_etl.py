# Batch csv pipeline that runs daily
from datetime import datetime

import polars as pl
from airflow.sdk import dag, task


@dag(
    start_date=datetime(2025, 10, 19),
    schedule="@daily",
    default_args={"owner": "BibiBool", "retries": 3},
    tags=["olist", "ecommerce"],
)
def olist_etl():

    @task
    def fetch_products_category():
        products_category_df = pl.read_csv(
            "olist_data/olist_sellers_dataset.csv")
        return products_category_df

    @task
    def print_products_category(print_products_category_df):
        print(print_products_category_df.head())

    print_products_category(fetch_products_category())


olist_etl()
