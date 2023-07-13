WITH dim_product__souce AS (
  SELECT *
  FROM `vit-lam-data.wide_world_importers.warehouse__stock_items`
),
  dim_product__renamecolumn AS (
  SELECT 
    stock_item_id AS product_key
    , stock_item_name AS product_name
    , is_chiller_stock
    , lead_time_days
    , quantity_per_outer
    , brand AS brand_name
    , supplier_id AS supplier_key
    , color_id AS color_key
    , unit_package_id AS unit_package_type_key
    , outer_package_id AS outer_package_type_key

  FROM dim_product__souce
  ),
  dim_product__cast_type AS (
  SELECT 
    CAST(product_key AS INTEGER) as product_key
    , CAST(product_name AS STRING) as product_name
    , CAST (is_chiller_stock AS BOOLEAN) AS is_chiller_stock_boolean
    , CAST(lead_time_days AS INTEGER) AS lead_time_days
    , CAST(quantity_per_outer AS INTEGER) AS quantity_per_outer
    , CAST (brand_name AS STRING) as brand_name
    , CAST (supplier_key AS INTEGER) AS supplier_key
    , CAST (color_key AS INTEGER) AS color_key
    , CAST (unit_package_type_key AS INTEGER) AS unit_package_type_key
    , CAST (outer_package_type_key AS INTEGER) AS outer_package_type_key

  FROM dim_product__renamecolumn
  ),

  dim_product__convert_boolean AS (
  SELECT *
    , CASE 
      WHEN is_chiller_stock_boolean IS TRUE THEN 'Chiller Stock'
      WHEN is_chiller_stock_boolean IS FALSE THEN 'NOT Chiller Stock'
    ELSE 'Undefined'
    END AS is_chiller_stock

  FROM dim_product__cast_type
  )

, dim_product__join_1 AS (
    SELECT 
      dim_product.product_key
      , dim_product.product_name
      , dim_product.is_chiller_stock
      , dim_product.lead_time_days
      , dim_product.quantity_per_outer
      , COALESCE(dim_product.brand_name, 'Undefined') AS brand_name
      , dim_product.supplier_key
      , COALESCE(dim_supplier.supplier_name,'Undefined') AS supplier_name
      , COALESCE(dim_product.color_key, 0) AS color_key
      , COALESCE(dim_color.color_name,'Undefined') AS color_name
      , dim_product.unit_package_type_key
      , COALESCE(dim_unit_package_type.package_type_name,'Undefined') AS unit_package_type_name
      , dim_product.outer_package_type_key
      , COALESCE(dim_outer_package_type.package_type_name,'Undefined') AS outer_package_type_name
      , dim_external_stock_item.category_key
      
    FROM dim_product__convert_boolean AS dim_product

    LEFT JOIN {{ref ('dim_supplier') }} AS dim_supplier
      ON dim_product.supplier_key = dim_supplier.supplier_key

    LEFT JOIN {{ref('stg_dim_color')}} AS dim_color
      ON dim_product.color_key = dim_color.color_key

    LEFT JOIN {{ref ('stg_dim_package_type')}} AS dim_unit_package_type
      ON dim_product.unit_package_type_key = dim_unit_package_type.package_type_key

    LEFT JOIN {{ref ('stg_dim_package_type')}} AS dim_outer_package_type
      ON dim_product.outer_package_type_key = dim_outer_package_type.package_type_key

    LEFT JOIN {{ref('stg_dim_external_stock_item')}} AS dim_external_stock_item
      USING (product_key)
)

SELECT 
  product_key
  , product_name
  , is_chiller_stock
  , lead_time_days
  , quantity_per_outer
  , brand_name
  , supplier_key
  , supplier_name
  , color_key
  , color_name
  , unit_package_type_key
  , unit_package_type_name
  , outer_package_type_key
  , outer_package_type_name
  , category_key
  , COALESCE(dim_external_category.category_name,'Undefined') AS category_name
  , COALESCE(dim_external_category.parent_category_key, 0) AS parent_category_key
  , COALESCE(dim_external_category.category_level, 0) AS category_level

FROM dim_product__join_1
LEFT JOIN {{ref('stg_dim_external_category')}} AS dim_external_category USING (category_key)
