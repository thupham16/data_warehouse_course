
WITH app_people__source AS (
  SELECT  *
  FROM `vit-lam-data.wide_world_importers.application__people`
),

  app_people__rename AS (
  SELECT 
    person_id AS person_key,
    full_name
  FROM app_people__source
  ),

  app_people__concast_type AS (
  SELECT 
    CAST (person_key AS INTEGER) AS person_key,
    CAST (full_name AS STRING) AS full_name
  FROM app_people__rename
  )

SELECT 
  person_key,
  full_name 
FROM app_people__concast_type
