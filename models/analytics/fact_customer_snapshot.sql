WITH fact_customer_snapshot__source AS (
  SELECT 
    customer_key
    , DATE_TRUNC(order_date, MONTH) AS order_date
    , SUM(gross_amount) AS sales_amount
  FROM data-modeling-370410.wide_world_importers_dwh.fact_sales_order_line
  GROUP BY 1, 2
)
 
, dim_year_month AS (
  SELECT DISTINCT year_month
  FROM data-modeling-370410.wide_world_importers_dwh.dim_date
)

, fact_customer_snapshot__densed AS (
  SELECT DISTINCT customer_key
  , year_month
  FROM fact_customer_snapshot__source
  CROSS JOIN dim_year_month
)

, fact_customer_snapshot__calculation  AS (
  SELECT 
    fact_customer_snapshot__source.*
    , SUM(fact_customer_snapshot__source.sales_amount) OVER (PARTITION BY fact_customer_snapshot__source.customer_key 
    ORDER BY fact_customer_snapshot__source.order_date) AS life_time_sales_amount
  FROM fact_customer_snapshot__source
  LEFT JOIN fact_customer_snapshot__densed 
    ON fact_customer_snapshot__densed.customer_key = fact_customer_snapshot__source.customer_key
    AND fact_customer_snapshot__source.order_date = fact_customer_snapshot__densed.year_month
)

, fact_customer_snapshot__percentile AS (
  SELECT *
  , PERCENT_RANK() OVER (PARTITION BY order_date ORDER BY sales_amount) AS sales_amount_percentile_rank
  , PERCENT_RANK() OVER (PARTITION BY order_date ORDER BY life_time_sales_amount) AS life_time_sales_amount_percentile_rank  

  FROM fact_customer_snapshot__calculation
)

, fact_customer_snapshot__segmentation AS (
  SELECT *
    , CASE 
        WHEN sales_amount_percentile_rank <0.5 THEN 'Low'
        WHEN sales_amount_percentile_rank BETWEEN 0.5 AND 0.8 THEN 'Medium'
        WHEN sales_amount_percentile_rank >0.8 THEN 'High'
      ELSE 'Undefined'
      END AS sales_amount_segment
    , CASE 
        WHEN life_time_sales_amount_percentile_rank <0.5 THEN 'Low'
        WHEN life_time_sales_amount_percentile_rank BETWEEN 0.5 AND 0.8 THEN 'Medium'
        WHEN life_time_sales_amount_percentile_rank >0.8 THEN 'High'
      ELSE 'Undefined'
      END AS life_time_sales_amount_segment

  FROM fact_customer_snapshot__percentile
)

SELECT
  customer_key
  , order_date
  , sales_amount 
  , life_time_sales_amount
  , sales_amount_percentile_rank
  , life_time_sales_amount_percentile_rank
  , sales_amount_segment
  , life_time_sales_amount_segment
FROM fact_customer_snapshot__segmentation 
