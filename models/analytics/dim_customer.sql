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
  customer_key,
  customer_name,
  customer_category_key,
  buying_group_key
FROM dim_customer_cast_type
