WITH dim_category AS (
  SELECT *
  FROM data-modeling-370410.wide_world_importers_dwh_staging.stg_dim_external_category
)

, dim_category_map_bridge__depth_0 AS (
  SELECT 
    category_key AS parent_category_key
    , category_key AS child_category_key
    , 0 AS depth_from_parent
  FROM dim_category
)

, dim_category_map_bridge__depth_1 AS (
  SELECT parent_category_key 
    , category_key AS child_category_key
    , 1 AS depth_from_parent
  FROM dim_category
  WHERE parent_category_key <> 0
)

, dim_category_role_playing__parent AS (
      SELECT parent_category_key AS grand_parent_category_key
        , category_key AS parent_category_key
      FROM dim_category
)

, dim_category_map_bridge__depth_2 AS (
    SELECT 
      -- parent_category_key
      -- , category_key AS child_category_key
      -- , grand_parent_category_key
      grand_parent_category_key AS parent_category_key
      , category_key AS child_category_key
      , 2 AS depth_from_parent
    FROM dim_category
    LEFT JOIN dim_category_role_playing__parent
    USING (parent_category_key)
    WHERE grand_parent_category_key <> 0
)

, dim_category_role_playing__grand_parent AS (
      SELECT parent_category_key AS grand_grand_parent_category_key
        , category_key AS grand_parent_category_key
      FROM dim_category
)

, dim_category_map_bridge__depth_3 AS (
    SELECT 
      -- category_key AS child_category_key
      -- , parent_category_key
      -- , dim_category_role_playing__parent.grand_parent_category_key
      -- , grand_grand_parent_category_key
      grand_grand_parent_category_key AS parent_category_key
      , category_key AS child_category_key
      , 3 as depth_from_parent

    FROM dim_category
    LEFT JOIN dim_category_role_playing__parent
    USING (parent_category_key)
    LEFT JOIN dim_category_role_playing__grand_parent
    ON dim_category_role_playing__grand_parent.grand_parent_category_key = dim_category_role_playing__parent.grand_parent_category_key
    WHERE grand_grand_parent_category_key <> 0
)

, dim_category_map_bridge__union_level AS (
  SELECT *
  FROM dim_category_map_bridge__depth_0

  UNION ALL

  SELECT *
  FROM dim_category_map_bridge__depth_1

  UNION ALL

  SELECT *
  FROM dim_category_map_bridge__depth_2

  UNION ALL

  SELECT *
  FROM dim_category_map_bridge__depth_3
)

SELECT
  dim_category_map_bridge.parent_category_key
  , dim_parent_category.category_name AS parent_category_name
  , dim_category_map_bridge.child_category_key 
  , dim_category.category_name AS child_category_name
  , depth_from_parent

FROM dim_category_map_bridge__union_level AS dim_category_map_bridge
LEFT JOIN {{ref('stg_dim_external_category')}} AS dim_category
    ON dim_category_map_bridge.child_category_key = dim_category.category_key

LEFT JOIN {{ref('stg_dim_external_category')}} AS dim_parent_category
    ON dim_category_map_bridge.parent_category_key = dim_parent_category.category_key
