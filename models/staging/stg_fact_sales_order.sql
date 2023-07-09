WITH fact_sales_order__source AS (
    SELECT *
    FROM `vit-lam-data.wide_world_importers.sales__orders`
)

, fact_sales_order__rename_column AS (
    SELECT 
      order_id AS sales_order_key
      , customer_id AS customer_key
      , picked_by_person_id AS picked_by_person_key
      , salesperson_person_id AS salesperson_person_key
      , backorder_order_id AS backorder_order_key
      , order_date
      , expected_delivery_date
      , is_undersupply_backordered
    FROM fact_sales_order__source
)

, fact_sales_order__cast_type AS (
    SELECT 
      CAST(sales_order_key AS INTEGER) AS sales_order_key
      , CAST (customer_key AS INTEGER) AS customer_key
      , CAST (picked_by_person_key AS INTEGER) AS picked_by_person_key
      , CAST (salesperson_person_key AS INTEGER) AS salesperson_person_key
      , CAST (backorder_order_key AS INTEGER) AS backorder_order_key
      , CAST (order_date AS date) as order_date
      , CAST (expected_delivery_date AS date) as expected_delivery_date
      , CAST (is_undersupply_backordered AS BOOLEAN) AS is_undersupply_backordered_boolean

    FROM fact_sales_order__rename_column
)

, fact_sales_order__convert_boolean AS (
    SELECT *
      , CASE 
        WHEN is_undersupply_backordered_boolean IS TRUE THEN 'Undersupply Backordered'
        WHEN is_undersupply_backordered_boolean IS FALSE THEN 'Not Undersupply Backordered'
      ELSE 'Undefined'
      END AS is_undersupply_backordered
    
    FROM fact_sales_order__cast_type
)

SELECT 
  sales_order_key
  , customer_key
  , COALESCE(picked_by_person_key,0) AS picked_by_person_key
  , COALESCE(salesperson_person_key,0) AS salesperson_person_key
  , COALESCE(backorder_order_key,0) AS backorder_order_key
  , order_date
  , expected_delivery_date
  , is_undersupply_backordered
FROM fact_sales_order__convert_boolean