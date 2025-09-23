CREATE TABLE
  OR REPLACE TABLE analytics.dim_customers (
    customer_id STRING,
    customer_name STRING,
    region STRING
  );


INSERT INTO
  analytics.dim_customers
SELECT
  customer_id,
  customer_name,
  region
FROM
  analytics.customers;