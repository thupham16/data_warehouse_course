WITH dim_external_stock_item__source AS (
  SELECT *
  FROM `vit-lam-data.wide_world_importers.external__stock_item`
)

, dim_external_stock_item__rename_column AS (
  SELECT
    stock_item_id AS product_key
    , category_id AS category_key

  FROM dim_external_stock_item__source
)

, dim_external_stock_item__cast_type AS (
  SELECT 
    CAST(product_key AS INTEGER) AS product_key
    , CAST(category_key AS INTEGER) AS category_key

  FROM dim_external_stock_item__rename_column
)

SELECT 
  product_key
  , category_key

FROM dim_external_stock_item__cast_type
