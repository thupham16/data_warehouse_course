SELECT 
  person_key AS salesperson_key,
  full_name AS salesperson_full_name,
  search_name AS salesperson_search_name

FROM {{ref ('dim_person')}}
WHERE is_salesperson = 'Salesperson' 
