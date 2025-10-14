-- Stage raw FRED observations from Silver layer
-- Reads directly from Parquet files created by Spark

{{ config(
    materialized='view'
) }}

WITH source_data AS (
    SELECT
        series_id,
        series_name,
        observation_date,
        value,
        value_raw,
        is_missing,
        is_valid,
        realtime_start,
        realtime_end,
        ingestion_timestamp
    FROM read_parquet('/opt/airflow/data/silver/fred/**/*.parquet', hive_partitioning=1)
    WHERE is_valid = true
)

SELECT
    series_id,
    series_name,
    observation_date,
    value,
    realtime_start AS data_as_of_date,
    CAST(ingestion_timestamp AS TIMESTAMP) AS ingested_at,
    CURRENT_TIMESTAMP AS transformed_at
FROM source_data
ORDER BY series_id, observation_date