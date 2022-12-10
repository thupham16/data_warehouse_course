WITH dim_supplier_category__source AS (
  SELECT *
  FROM vit-lam-data.wide_world_importers.purchasing__supplier_categories
),

  dim_supplier_category__rename AS (
  SELECT
    supplier_category_id AS supplier_category_key,
    supplier_category_name
  FROM dim_supplier_category__source
),

  dim__supplier_categoryy__cast_type AS (
  SELECT
    CAST (supplier_category_key AS INTEGER) AS supplier_category_key,
    CAST (supplier_category_name AS STRING) AS supplier_category_name
  FROM dim_supplier_category__rename
  )

SELECT 
  supplier_category_key,
  supplier_category_name

FROM dim__supplier_categoryy__cast_type