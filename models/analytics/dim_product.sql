WITH dim_product__souce AS (
  SELECT *
  FROM `vit-lam-data.wide_world_importers.warehouse__stock_items`
),
  dim_product__renamecolumn AS (
  SELECT 
    stock_item_id AS product_key,
    stock_item_name AS product_name,
    brand AS brand_name,
    supplier_id AS supplier_key,
    is_chiller_stock
  FROM dim_product__souce
  ),
  dim_product__cast_type AS (
  SELECT 
    CAST(product_key AS INTEGER) as product_key,
    CAST(product_name AS STRING) as product_name,
    CAST (brand_name AS STRING) as brand_name,
    CAST (supplier_key AS INTEGER) AS supplier_key,
    CAST (is_chiller_stock AS BOOLEAN) AS is_chiller_stock
  FROM dim_product__renamecolumn
  ),

  dim_product__convert_boolean AS (
  SELECT *,
    CASE 
      WHEN is_chiller_stock IS TRUE THEN 'Chiller Stock'
      WHEN is_chiller_stock IS FALSE THEN 'NOT Chiller Stock'
    ELSE 'Undefined'
    END AS chiller_stock

  FROM dim_product__cast_type
  )


SELECT 
  dim_product.product_key,
  dim_product.product_name,
  dim_product.brand_name,
  dim_product.supplier_key,
  dim_supplier.supplier_name,
  dim_product.chiller_stock
FROM dim_product__convert_boolean AS dim_product
LEFT JOIN {{ ref ('dim_supplier') }} AS dim_supplier
ON dim_product.supplier_key = dim_supplier.supplier_key
