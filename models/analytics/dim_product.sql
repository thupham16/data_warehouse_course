WITH dim_product__souce AS (
  SELECT *
  FROM `vit-lam-data.wide_world_importers.warehouse__stock_items`
),
  dim_product__renamecolumn AS (
  SELECT 
    stock_item_id AS product_key,
    stock_item_name AS product_name,
    brand AS brand_name
  FROM dim_product__souce
  )

SELECT 
  CAST(product_key AS INTEGER) as product_key,
  CAST(product_name AS STRING) as product_name,
  CAST (brand_name AS STRING) as brand_name
FROM dim_product__renamecolumn

