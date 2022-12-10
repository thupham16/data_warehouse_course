WITH dim_is_undersupply_backordered AS (
  SELECT 
   'Not Undersupply Backordered' AS is_undersupply_backordered
UNION ALL
  SELECT
   'Undersupply Backordered'AS is_undersupply_backordered
)

SELECT  
  CONCAT(dim_is_undersupply_backordered.is_undersupply_backordered,
  ',',
  dim_package_type.package_type_key,
  ',',
  dim_package_type.package_type_name
  ) AS sales_order_line_indicator_key,
  is_undersupply_backordered,
  dim_package_type.package_type_key,
  dim_package_type.package_type_name
FROM dim_is_undersupply_backordered
CROSS JOIN data-modeling-370410.wide_world_importers_dwh_staging.stg_dim_package_type AS dim_package_type

