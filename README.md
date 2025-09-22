Overview

This repository shows a minimal end-to-end project which:

Stores raw CSVs in GCS (e.g., gs://retail_raw/sales/)

Loads staging tables into BigQuery (analytics.stg_sales)

Creates a star schema (dim_customers, dim_products, fact_sales) where fact_sales is partitioned by date and clustered by product_id

Uses an Apache Beam Python pipeline on Dataflow to clean/append CSVs into analytics.fact_sales

Orchestrates job with Composer (Airflow DAG) that launches Dataflow and runs DDL/DML refresh tasks

Builds Looker Studio report for visualizations and sets BigQuery cost controls (partitioning and budget alerts)