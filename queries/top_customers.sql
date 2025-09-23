select
  c.customer_name,
  sum(s.amount) as total_amount
from
  analytics.fact_sales s
  left join analytics.dim_customers c on s.customer_id = c.customer_id
group by
  c.customer_name
order by
  2 DESC
LIMIT
  10;