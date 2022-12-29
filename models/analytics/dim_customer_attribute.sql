WITH dim_customer_attribute__gross_amount AS (
  SELECT 
    customer_key
    , SUM(gross_amount) AS lifetime_sales_amount
    , COUNT(sales_order_key) AS life_time_frequency
    , SUM (CASE WHEN
          order_date BETWEEN '2016-05-01' AND '2016-05-31'
          THEN gross_amount
          END
          ) AS last_month_sales_amount

  FROM {{ref('fact_sales_order_line')}}
  GROUP BY 1
)
, dim_customer_attribute__percentile AS (
  SELECT 
    *
    , PERCENT_RANK() OVER (ORDER BY lifetime_sales_amount) AS sales_amount_percent_rank
    , PERCENT_RANK() OVER (ORDER BY life_time_frequency) AS frequency_percent_rank

  FROM dim_customer_attribute__gross_amount
)
  SELECT 
    *
    , CASE 
        WHEN sales_amount_percent_rank <0.5 THEN 'Low'
        WHEN sales_amount_percent_rank BETWEEN 0.5 AND 0.8 THEN 'Medium'
        WHEN sales_amount_percent_rank >0.8 THEN 'High'
      ELSE 'Undefined'
      END AS lifetime_monetary_segment
    , CASE 
        WHEN frequency_percent_rank <0.5 THEN 'Low'
        WHEN frequency_percent_rank BETWEEN 0.5 AND 0.8 THEN 'Medium'
        WHEN frequency_percent_rank >0.8 THEN 'High'
      ELSE 'Undefined'
      END AS frequency_segment

  FROM dim_customer_attribute__percentile

                                 