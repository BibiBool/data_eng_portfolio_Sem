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
    * Loads the data into an aggregated table

## Analysis questions

We will answer three questions from the Business:

1. What are the most popular content or pages on our site, and how has their popularity changed over time?
2. What is the distribution of client access by geographic location (approximated by edge location), and what are the peak access times for different regions? (to do)
3. How is our service performing in terms of response times and error rates, and are there specific request types or client behaviors contributing to performance issues? (to do)
