{{ config(materialized='table') }}
SELECT tb.id FROM {{ source('service_1', 'table_b') }} tb
JOIN {{ source('service_1', 'table_c') }} tc ON tb.id = tc.id
