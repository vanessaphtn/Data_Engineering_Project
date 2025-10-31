{{ config(materialized='table') }}

WITH distinct_bike_types AS (
    SELECT DISTINCT
        bike_type
    FROM {{ ref('silver_trips') }}
),

numbered AS (
    SELECT
        row_number() OVER (ORDER BY bike_type) AS bike_type_id,  -- surrogate INT key
        bike_type
    FROM distinct_bike_types
)

SELECT
    bike_type_id,
    bike_type AS bike_type_name
FROM numbered
