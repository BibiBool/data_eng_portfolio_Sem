# E-Commerce Data Pipeline - Requirements

## 1. Business Context

The goal of this project is to design and implement an **end-to-end data pipeline** for an e-commerce platform.

### Overview

- The platform tracks **customers, products, and orders**
- The business needs both:
  - **Batch analytics** (daily reports on sales, revenue, customers)
  - **Real-time insights** (live order stream, cancellations, top-selling products)

### Users

- **Business analysts**: Use Metabase dashboards for KPIs
- **Data engineers**: Extend and maintain the pipeline

## 2. Data Context

### Sources of Truth

- **Postgres**
  - Holds historical e-commerce data
  - Seeded from Olist dataset
- **Kafka**
  - Ingests real-time order events
  - Events produced by Faker-based Python generator

### Data Schema (MVP)

```sql
customers(
    customer_id,
    name,
    email,
    created_at
)

products(
    product_id,
    name,
    category,
    price
)

orders(
    order_id,
    customer_id,
    product_id,
    quantity,
    price,
    status,
    order_timestamp
)
```

### Schema Evolution

- Version control for schemas in `docs/erd.png`
- SQL migrations for schema changes

## 3. Pipeline Scope

### In Scope (MVP)

#### Batch Processing

- Daily ETL job (Airflow → Spark) to clean and transform order data
- Store curated tables in Postgres:
  - `fact_orders`
  - `dim_customers`
  - `dim_products`

#### Streaming Processing

- Kafka producer simulates new order events
- Spark Structured Streaming:
  - Consumes events
  - Processes data
  - Inserts into Postgres

#### Analytics & BI

Metabase dashboards for:

- Daily sales revenue
- Top 10 products by sales
- Orders per category
- Orders per hour (real-time view)

### Out of Scope (MVP)

- Machine Learning models (recommendations, demand forecasting)
- Complex alerting/monitoring (to be added later)
- Cloud deployment (MVP runs on local Docker Compose)

## 4. Non-Functional Requirements (NFRs)

### Scalability

- System should handle at least 1M rows of orders without breaking

### Reproducibility

- One-command pipeline setup:

```bash
docker-compose up
```

### Reliability

- Failed Airflow tasks must retry at least 3 times
- Tasks should be idempotent

### Observability

- Logs for all services visible in container logs:
  - Kafka
  - Spark
  - Airflow

### Security

- Secrets managed in `.env` files, not in code:
  - Postgres passwords
  - JWTs
  - API keys

## 5. Project Success Criteria

### End-to-End Pipeline

- Historical batch load from Postgres
- Streaming ingestion via Kafka
- Spark jobs producing curated datasets
- Airflow orchestrating batch DAGs
- Metabase dashboards showing insights

### Tools & Infrastructure

- Technologies:
  - Postgres
  - Kafka
  - Spark
  - Airflow
  - Metabase
  - Docker Compose

- Deployment:
  - All services containerized for portability
  - Local-first deployment (no cloud for MVP)
