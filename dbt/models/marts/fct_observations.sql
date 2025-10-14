-- Fact table for economic observations with metrics

{{ config(
    materialized='table'
) }}

WITH observations AS (
    SELECT
        series_id,
        observation_date,
        value,
        data_as_of_date,
        ingested_at
    FROM {{ ref('stg_fred_observations') }}
),

lagged_values AS (
    SELECT
        series_id,
        observation_date,
        value,
        LAG(value, 1) OVER (PARTITION BY series_id ORDER BY observation_date) AS prev_value,
        LAG(value, 12) OVER (PARTITION BY series_id ORDER BY observation_date) AS value_12m_ago,
        data_as_of_date,
        ingested_at
    FROM observations
)

SELECT
    series_id AS indicator_id,
    observation_date AS date_key,
    value AS observation_value,
    prev_value AS previous_value,
    value_12m_ago AS value_year_ago,
    CASE 
        WHEN prev_value IS NOT NULL AND prev_value != 0 
        THEN ((value - prev_value) / prev_value) * 100
    END AS period_change_pct,
    CASE 
        WHEN value_12m_ago IS NOT NULL AND value_12m_ago != 0
        THEN ((value - value_12m_ago) / value_12m_ago) * 100
    END AS yoy_change_pct,
    data_as_of_date,
    ingested_at,
    CURRENT_TIMESTAMP AS created_at
FROM lagged_values