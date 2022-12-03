WITH fact_sales_order_line_source AS (
  SELECT *
  FROM `vit-lam-data.wide_world_importers.sales__order_lines`

),

  fact_sales_rename_column AS (
  SELECT
    order_line_id AS sales_order_line_key,
    stock_item_id AS product_key,
    quantity,
    unit_price     
  FROM fact_sales_order_line_source
),

 fact_sales_cast_type AS (
  SELECT
    CAST(sales_order_line_key AS INTEGER) AS sales_order_line_key,
    CAST (quantity AS INTEGER) AS quantity,
    CAST (unit_price AS NUMERIC) AS unit_price,
    CAST(product_key AS INTEGER) AS product_key
  FROM fact_sales_rename_column
)

SELECT 
  sales_order_line_key,
  quantity,
  unit_price,
  product_key,
  quantity * unit_price AS gross_amount
FROM fact_sales_cast_type

  
