# Airflow DAG Deployment Practice

## Description

This repository provides a simple setup for practicing the deployment of an Apache Airflow Directed Acyclic Graph (DAG). It utilizes Docker to orchestrate the necessary services: Airflow, PostgreSQL, and PGAdmin.

## Objective

The primary objectives of this project are to demonstrate and practice:

1. **Database and Table Provisioning:** Creating an Airflow pipeline that sets up a PostgreSQL database and a dedicated table for storing cleaned log data.
2. **ETL Pipeline for Log Data:** Developing a second Airflow pipeline that performs the following steps:
    * Ingests raw log data.
    * Cleans and transforms the raw log data.
    * Loads the cleaned log data into the PostgreSQL table created in the first objective.
