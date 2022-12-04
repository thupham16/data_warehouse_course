WITH dim_product__souce AS (
  SELECT *
  FROM `vit-lam-data.wide_world_importers.warehouse__stock_items`
),
  dim_product__renamecolumn AS (
  SELECT 
    stock_item_id AS product_key,
    stock_item_name AS product_name,
    brand AS brand_name,
    supplier_id AS supplier_key
  FROM dim_product__souce
  ),
  dim_product__cast_type AS (
  SELECT 
    CAST(product_key AS INTEGER) as product_key,
    CAST(product_name AS STRING) as product_name,
    CAST (brand_name AS STRING) as brand_name,
    CAST (supplier_key AS INTEGER) AS supplier_key
  FROM dim_product__renamecolumn
  )

SELECT 
  dim_product.product_key,
  dim_product.product_name,
  dim_product.brand_name,
  dim_product.supplier_key,
  dim_supplier.supplier_name
FROM dim_product__cast_type AS dim_product
LEFT JOIN {{ ref ('dim_supplier')}} AS dim_supplier
ON dim_product.supplier_key = dim_supplier.supplier_key
