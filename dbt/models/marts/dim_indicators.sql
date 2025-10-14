-- Dimension table for economic indicators

{{ config(
    materialized='table'
) }}

SELECT DISTINCT
    series_id AS indicator_id,
    series_name AS indicator_name,
    CASE 
        WHEN series_id = 'GDP' THEN 'Output'
        WHEN series_id = 'UNRATE' THEN 'Labor Market'
        WHEN series_id = 'CPIAUCSL' THEN 'Inflation'
        WHEN series_id = 'DFF' THEN 'Monetary Policy'
        WHEN series_id = 'UMCSENT' THEN 'Sentiment'
    END AS category,
    CASE 
        WHEN series_id IN ('GDP', 'CPIAUCSL') THEN 'Quarterly/Monthly'
        WHEN series_id IN ('DFF') THEN 'Daily'
        WHEN series_id IN ('UNRATE', 'UMCSENT') THEN 'Monthly'
    END AS frequency,
    CURRENT_TIMESTAMP AS created_at
FROM {{ ref('stg_fred_observations') }}