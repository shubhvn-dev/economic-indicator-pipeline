import pytest
import duckdb
from pathlib import Path
import pandas as pd

def test_bronze_layer_exists():
    """Test that Bronze layer has data files"""
    bronze_path = Path('data/bronze/fred')
    if bronze_path.exists():
        parquet_files = list(bronze_path.glob('*.parquet'))
        assert len(parquet_files) > 0, "No parquet files in Bronze layer"

def test_silver_layer_structure():
    """Test Silver layer partitioning"""
    silver_path = Path('data/silver/fred')
    if silver_path.exists():
        # Check for partitioned directories
        partitions = [d for d in silver_path.iterdir() if d.is_dir()]
        assert len(partitions) > 0, "No partitions in Silver layer"

def test_gold_layer_tables():
    """Test Gold layer DuckDB tables exist"""
    db_path = 'data/gold/economic_indicators.duckdb'
    if Path(db_path).exists():
        conn = duckdb.connect(db_path, read_only=True)
        
        tables = conn.execute("SHOW TABLES").fetchdf()
        expected_tables = ['dim_indicators', 'dim_time', 'fct_observations']
        
        for table in expected_tables:
            assert table in tables['name'].values, \
                f"Table {table} not found in Gold layer"
        
        conn.close()

def test_data_quality_gold_layer():
    """Test data quality in Gold layer"""
    db_path = 'data/gold/economic_indicators.duckdb'
    if Path(db_path).exists():
        conn = duckdb.connect(db_path, read_only=True)
        
        # Test no nulls in primary keys
        result = conn.execute("""
            SELECT COUNT(*) as null_count 
            FROM fct_observations 
            WHERE indicator_id IS NULL OR date_key IS NULL
        """).fetchone()
        assert result[0] == 0, "Found nulls in fact table primary keys"
        
        # Test referential integrity
        result = conn.execute("""
            SELECT COUNT(*) as orphan_count
            FROM fct_observations f
            LEFT JOIN dim_indicators d ON f.indicator_id = d.indicator_id
            WHERE d.indicator_id IS NULL
        """).fetchone()
        assert result[0] == 0, "Found orphaned records in fact table"
        
        conn.close()