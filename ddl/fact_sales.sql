CREATE
OR REPLACE TABLE analytics.fact_sales (
  sale_id STRING,
  sale_date DATE,
  amount FLOAT64,
  customer_id STRING,
  product_id STRING
) PARTITION BY sale_date CLUSTER BY product_id;


INSERT INTO
  analytics.fact_sales
SELECT
  sale_id,
  DATE(date) AS sale_date,
  CAST(amount AS FLOAT64),
  customer_id,
  product_id
FROM
  analytics.sales;