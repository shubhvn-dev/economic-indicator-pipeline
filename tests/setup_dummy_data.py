import duckdb
import pandas as pd
from pathlib import Path

# Create directories
Path("data/gold").mkdir(parents=True, exist_ok=True)
Path("data/silver/fred").mkdir(parents=True, exist_ok=True)

# Create database
conn = duckdb.connect("data/gold/economic_indicators.duckdb")

# Create dummy tables
conn.execute(
    """CREATE TABLE dim_indicators (
        indicator_id VARCHAR, 
        indicator_name VARCHAR, 
        category VARCHAR, 
        frequency VARCHAR, 
        created_at TIMESTAMP
    )"""
)

conn.execute(
    """CREATE TABLE dim_time (
        date_key DATE, 
        observation_date DATE, 
        year INTEGER, 
        quarter INTEGER, 
        month INTEGER, 
        day INTEGER, 
        day_of_week INTEGER, 
        day_of_year INTEGER, 
        month_start DATE, 
        quarter_start DATE, 
        year_start DATE, 
        is_weekend BOOLEAN
    )"""
)

conn.execute(
    """CREATE TABLE fct_observations (
        indicator_id VARCHAR, 
        date_key DATE, 
        observation_value DOUBLE, 
        previous_value DOUBLE, 
        value_year_ago DOUBLE, 
        period_change_pct DOUBLE, 
        yoy_change_pct DOUBLE, 
        data_as_of_date VARCHAR, 
        ingested_at VARCHAR, 
        created_at TIMESTAMP
    )"""
)

conn.execute(
    """CREATE VIEW stg_fred_observations AS 
    SELECT 
        indicator_id as series_id, 
        indicator_name as series_name, 
        date_key as observation_date, 
        observation_value as value, 
        data_as_of_date, 
        ingested_at, 
        created_at as transformed_at 
    FROM fct_observations"""
)

# Create dummy parquet
df = pd.DataFrame({
    "series_id": ["GDP"],
    "series_name": ["GDP"],
    "observation_date": ["2024-01-01"],
    "value": [1.0],
    "is_valid": [True],
})
df.to_parquet("data/silver/fred/test.parquet")

conn.close()
print("Dummy data created successfully")