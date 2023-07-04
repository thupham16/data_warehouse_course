WITH dim_customer__source AS (
    SELECT *
    FROM `vit-lam-data.wide_world_importers.sales__customers`
),
  
  dim_customer__rename AS (
    SELECT  
      customer_id AS customer_key,
      customer_name ,
      is_statement_sent AS is_statement_sent_boolean,
      is_on_credit_hold AS is_on_credit_hold_boolean,
      payment_days,
      standard_discount_percentage,
      credit_limit,
      customer_category_id AS customer_category_key,
      buying_group_id AS buying_group_key,
      delivery_method_id AS delivery_method_key,
      delivery_city_id AS delivery_city_key,
      account_opened_date
    FROM dim_customer__source
 ),
  
  dim_customer__cast_type AS (
    SELECT 
      CAST (customer_key AS INTEGER) AS customer_key,
      CAST (customer_name AS STRING) AS customer_name,
      CAST (is_statement_sent_boolean AS BOOLEAN) AS is_statement_sent_boolean,
      CAST (is_on_credit_hold_boolean AS BOOLEAN) AS is_on_credit_hold_boolean,
      CAST (payment_days AS INTEGER) AS payment_days,
      CAST (standard_discount_percentage AS INTEGER) AS standard_discount_percentage,
      CAST (credit_limit AS INTEGER) AS credit_limit,
      CAST (customer_category_key AS INTEGER) AS customer_category_key,
      CAST (buying_group_key AS INTEGER) AS buying_group_key,
      CAST (delivery_method_key AS INTEGER) AS delivery_method_key,
      CAST(delivery_city_key AS INTEGER) AS delivery_city_key,
      CAST(account_opened_date AS date) AS account_opened_date
    FROM dim_customer__rename
  ),

  dim_customer__convert_boolean AS (
    SELECT *,
      CASE 
        WHEN is_on_credit_hold_boolean IS TRUE THEN 'On Credit Hold'
        WHEN is_on_credit_hold_boolean IS FALSE THEN 'Not On Credit Hold'
        ELSE 'Undefined'
        END AS is_on_credit_hold,
      CASE 
        WHEN is_statement_sent_boolean IS TRUE THEN 'Statement Sent'
        WHEN is_statement_sent_boolean IS FALSE THEN 'Not Statement Sent'
        ELSE 'Undefined'
        END AS is_statement_sent

    FROM dim_customer__cast_type
  )

SELECT 
  dim_customer.customer_key,
  dim_customer.customer_name,
  dim_customer.is_statement_sent,
  dim_customer.is_on_credit_hold,
  dim_customer.payment_days,
  dim_customer.standard_discount_percentage,
  dim_customer.credit_limit,
  dim_customer.customer_category_key,
  COALESCE(dim_customer_category.customer_category_name,'Undefined') AS customer_category_name,
  dim_customer.buying_group_key,
  COALESCE(dim_buying_group.buying_group_name,'Undefined') AS buying_group_name,
  dim_customer.delivery_method_key,
  COALESCE(dim_delivery_method.delivery_method_name,'Undefined') AS delivery_method_name,
  dim_customer.delivery_city_key,
  COALESCE(dim_city.city_name,'Undefined') AS delivery_city_name,
  dim_city.state_province_key,
  COALESCE(dim_city.state_province_name,'Undefined') AS delivery_state_name,
  dim_customer.account_opened_date

FROM dim_customer__convert_boolean AS dim_customer 
LEFT JOIN {{ref ('stg_dim_customer_category')}} AS dim_customer_category 
ON dim_customer_category.customer_category_key = dim_customer.customer_category_key

LEFT JOIN {{ref ('stg_dim_sales_buying_group')}} AS dim_buying_group
ON dim_buying_group.buying_group_key = dim_customer.buying_group_key

LEFT JOIN {{ref('stg_dim_delivery_method')}} AS dim_delivery_method
ON dim_delivery_method.delivery_method_key = dim_customer.delivery_method_key

LEFT JOIN {{ref ('stg_dim_city')}} AS dim_city
ON dim_city.city_key = dim_customer.delivery_city_key
