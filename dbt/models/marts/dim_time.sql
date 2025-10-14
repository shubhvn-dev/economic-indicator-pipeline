-- Time dimension for date-based analysis

{{ config(
    materialized='table'
) }}

WITH date_spine AS (
    SELECT DISTINCT
        observation_date
    FROM {{ ref('stg_fred_observations') }}
)

SELECT
    observation_date AS date_key,
    observation_date,
    EXTRACT(YEAR FROM observation_date) AS year,
    EXTRACT(QUARTER FROM observation_date) AS quarter,
    EXTRACT(MONTH FROM observation_date) AS month,
    EXTRACT(DAY FROM observation_date) AS day,
    EXTRACT(DOW FROM observation_date) AS day_of_week,
    EXTRACT(DOY FROM observation_date) AS day_of_year,
    DATE_TRUNC('month', observation_date) AS month_start,
    DATE_TRUNC('quarter', observation_date) AS quarter_start,
    DATE_TRUNC('year', observation_date) AS year_start,
    CASE WHEN EXTRACT(DOW FROM observation_date) IN (0, 6) THEN true ELSE false END AS is_weekend
FROM date_spine
ORDER BY observation_date