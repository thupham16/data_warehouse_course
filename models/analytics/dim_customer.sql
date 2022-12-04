WITH dim_customer_source AS (
    SELECT *
    FROM `vit-lam-data.wide_world_importers.sales__customers`
),
  
  dim_customer_rename AS (
    SELECT  
      customer_id AS customer_key,
      customer_name ,
      customer_category_id AS customer_category_key,
      buying_group_id AS buying_group_key
    FROM dim_customer_source
 ),
  
  dim_customer_cast_type AS (
    SELECT 
      CAST (customer_key AS INTEGER) AS customer_key,
      CAST (customer_name AS STRING) AS customer_name,
      CAST (customer_category_key AS INTEGER) AS customer_category_key,
      CAST (buying_group_key AS INTEGER) AS buying_group_key
    FROM dim_customer_rename
  )

SELECT 
  dim_customer.customer_key,
  dim_customer.customer_name,
  dim_customer.customer_category_key,
  dim_customer_category.customer_category_name,
  dim_customer.buying_group_key,
  dim_buying_group.buying_group_name
FROM dim_customer_cast_type AS dim_customer 
LEFT JOIN {{ ref ('stg_dim_customer_category') }} AS dim_customer_category 
ON dim_customer_category.customer_category_key = dim_customer.customer_category_key

LEFT JOIN {{ ref ('stg_dim_sales_buying_group') }} AS dim_buying_group
ON dim_buying_group.buying_group_key = dim_customer.buying_group_key

