WITH fact_sales__source AS (
  SELECT customer_key
    , order_date
  FROM {{ref('fact_sales_order_line')}}
)

, fact_sales__first_order AS (
  SELECT 
    DATE_TRUNC(MIN(order_date), MONTH) AS first_order_month
    , customer_key
  FROM fact_sales__source
  GROUP BY 2
  ORDER BY 1
)

, fact_sales__transaction AS (
  SELECT DISTINCT
    DATE_TRUNC(order_date, MONTH) AS order_month
    , customer_key
  FROM fact_sales__source
  ORDER BY 2
)

, fact_cohort__cohort_size  as (
  SELECT 
    first_order_month AS cohort_month
    , count(customer_key) AS cohort_size
  FROM fact_sales__first_order
  GROUP BY 1
  ORDER BY 1
)

, fact_cohort__retention AS (
  SELECT  
    fact_sales__first_order.first_order_month AS cohort_month
    -- , fact_sales.order_month
    , DATE_DIFF(fact_sales.order_month, fact_sales__first_order.first_order_month, MONTH)  AS period
    , COUNT(customer_key) AS active_user
  FROM fact_sales__transaction  AS fact_sales
  LEFT JOIN fact_sales__first_order USING (customer_key)
  GROUP BY 1,2
  order by 1,2
)

SELECT
  cohort_month
  , fact_cohort__retention.period
  , fact_cohort__cohort_size.cohort_size
  , fact_cohort__retention.active_user
  , fact_cohort__retention.active_user*100 / fact_cohort__cohort_size.cohort_size as percentage
FROM fact_cohort__retention
LEFT JOIN fact_cohort__cohort_size USING (cohort_month)
ORDER BY 1,2