WITH dim_external_category__source AS (
  SELECT *
  FROM `vit-lam-data.wide_world_importers.external__categories`
)
, dim_external_category__rename AS (
  SELECT 
    category_id AS category_key
    , category_name
    , parent_category_id AS parent_category_key
    , category_level
  FROM dim_external_category__source
 )

, dim_external_category__cast_type AS (
  SELECT 
    CAST(category_key AS INTEGER) AS category_key
    , CAST(category_name AS STRING) AS category_name
    , CAST(parent_category_key AS INTEGER) AS parent_category_key
    , CAST(category_level AS INTEGER) AS category_level
    
  FROM dim_external_category__rename
 )

, dim_external_category__add_undefined_record AS (
   SELECT
    *
   FROM dim_external_category__cast_type

   UNION ALL

   SELECT
   0 AS category_key
   , 'Undefined' AS category_name
   , 0 AS parent_category_key
   , 0 AS category_level
 )

SELECT 
  category_key
  , category_name
  , parent_category_key
  , category_level
FROM dim_external_category__add_undefined_record