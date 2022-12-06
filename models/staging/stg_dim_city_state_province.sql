WITH dim_city__source AS (
  SELECT *
  FROM `vit-lam-data.wide_world_importers.application__cities`
),
  dim_city__rename AS (
  SELECT 
    city_id AS city_key,
    city_name,
    state_province_id AS state_province_key
  FROM dim_city__source
),

  dim_city__cast_type AS (
  SELECT 
    CAST(city_key AS INTEGER) AS city_key,
    CAST (city_name AS STRING) AS city_name,
    CAST(state_province_key AS INTEGER) AS state_province_key

  FROM dim_city__rename
)
SELECT 
  city_key,
  city_name,
  state_province_key,
  dim_state_province.state_province_name 
FROM dim_city__cast_type
JOIN `vit-lam-data.wide_world_importers.application__state_provinces` AS dim_state_province
ON dim_city__cast_type.state_province_key = dim_state_province.state_province_id

 

