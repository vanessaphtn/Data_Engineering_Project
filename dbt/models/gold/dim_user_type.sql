{{ config(materialized='table') }}

WITH distinct_user_types AS (
    SELECT DISTINCT
        user_type
    FROM {{ ref('silver_trips') }}
),

numbered AS (
    SELECT
        row_number() OVER (ORDER BY user_type) AS user_type_id,  -- surrogate INT key
        user_type
    FROM distinct_user_types
)

SELECT
    user_type_id,
    user_type AS user_type_name
FROM numbered
