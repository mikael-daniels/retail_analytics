from datetime import datetime
from airflow import models
from airflow.providers.google.cloud.operators.dataflow import DataflowTemplatedJobStartOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

PROJECT_ID = "deep-mile-293709"
REGION = "us-east1"
BUCKET = "gs://retail_raw_csv"

# Path to the Dataflow template
DATAFLOW_TEMPLATE_PATH = f"gs://dataflow-templates-us-east1/latest/GCS_CSV_to_BigQuery"

# BigQuery DDL statements to recreate tables
BQ_REFRESH_SQL = """
CREATE OR REPLACE TABLE `deep-mile-293709.analytics.dim_customers` AS
SELECT DISTINCT customer_id, customer_name, region
FROM `deep-mile-293709.analytics.customers`;

CREATE OR REPLACE TABLE `deep-mile-293709.analytics.dim_products` AS
SELECT DISTINCT product_id, product_name, category
FROM `deep-mile-293709.analytics.products`;

CREATE OR REPLACE TABLE `deep-mile-293709.analytics.fact_sales` (
  sale_id STRING,
  sale_date DATE,
  amount FLOAT64,
  customer_id STRING,
  product_id STRING
)
PARTITION BY sale_date
CLUSTER BY product_id;

INSERT INTO `deep-mile-293709.analytics.fact_sales`
SELECT
  sale_id,
  DATE(date) AS sale_date,
  CAST(amount AS FLOAT64),
  customer_id,
  product_id
FROM `deep-mile-293709.analytics.sales`;
"""

with models.DAG(
    dag_id="composer_dag",
    schedule_interval=None,  # Run on demand
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["composer", "dataflow", "bigquery"],
) as dag:

    # Task 1: Launch Dataflow job (classic template)
    start_dataflow = DataflowTemplatedJobStartOperator(
        task_id="start_dataflow_job",
        project_id=PROJECT_ID,
        location=REGION,
        job_name="sales-pipeline-mp--1758625662-14680898288852279898",
        template=DATAFLOW_TEMPLATE_PATH,
        parameters={
            "inputFilePattern": f"{BUCKET}/sales (1).csv",
            "outputTable": f"{PROJECT_ID}:analytics.sales",
            "bigQueryLoadingTemporaryDirectory": f"{BUCKET}/temp",
            "schemaJSONPath": f"{BUCKET}/sales json schema.json",  # Path to your schema JSON in GCS
            "badRecordsOutputTable": f"{PROJECT_ID}:analytics.errors",
            "delimiter": ",",      # CSV delimiter
            "csvFormat": "Default",
            "containsHeaders": "true"
        },
        environment={
        "serviceAccountEmail": "my-account@deep-mile-293709.iam.gserviceaccount.com"
        }
    )

    # Task 2: Run BigQuery DDL/DML to refresh tables
    refresh_bq_tables = BigQueryInsertJobOperator(
        task_id="refresh_bq_tables",
        configuration={
            "query": {
                "query": BQ_REFRESH_SQL,
                "useLegacySql": False,
                "multiStatementTransaction": True,  # allows running multiple CREATEs in one job
            }
        },
        location="US",
    )

    # Set dependencies: run BigQuery refresh after Dataflow job succeeds
    start_dataflow >> refresh_bq_tables