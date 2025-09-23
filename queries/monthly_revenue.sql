select
  FORMAT_DATE('%Y-%m', DATE(sale_date)) as sale_month,
  sum(amount) as total_amount
from
  analytics.fact_sales
group by
  1
order by
  1;