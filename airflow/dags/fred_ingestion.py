from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import pandas as pd
import json
import os
from pathlib import Path

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# FRED API configuration
FRED_API_KEY = os.getenv('FRED_API_KEY')
FRED_BASE_URL = 'https://api.stlouisfed.org/fred'

# Key economic indicators we want to track
INDICATORS = {
    'GDP': 'Gross Domestic Product',
    'UNRATE': 'Unemployment Rate',
    'CPIAUCSL': 'Consumer Price Index',
    'DFF': 'Federal Funds Rate',
    'UMCSENT': 'Consumer Sentiment Index'
}

def fetch_fred_series(series_id, series_name, **context):
    """
    Fetch a single economic time series from FRED API
    """
    execution_date = context['ds']  # Date string in YYYY-MM-DD format
    
    print(f"Fetching {series_name} ({series_id}) for date: {execution_date}")
    
    # API endpoint for series observations
    url = f"{FRED_BASE_URL}/series/observations"
    
    params = {
        'series_id': series_id,
        'api_key': FRED_API_KEY,
        'file_type': 'json',
        'observation_start': '2020-01-01',  # Get data from 2020 onwards
    }
    
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()
        
        if 'observations' not in data:
            print(f"No observations found for {series_id}")
            return
        
        # Convert to DataFrame
        df = pd.DataFrame(data['observations'])
        
        # Add metadata
        df['series_id'] = series_id
        df['series_name'] = series_name
        df['ingestion_timestamp'] = datetime.now().isoformat()
        
        # Save to Bronze layer (raw data)
        bronze_path = Path('/opt/airflow/data/bronze/fred')
        bronze_path.mkdir(parents=True, exist_ok=True)
        
        # Partition by date and series
        output_file = bronze_path / f"{series_id}_{execution_date}.parquet"
        df.to_parquet(output_file, engine='pyarrow', index=False)
        
        print(f"Saved {len(df)} observations to {output_file}")
        print(f"Date range: {df['date'].min()} to {df['date'].max()}")
        
        return str(output_file)
        
    except requests.exceptions.RequestException as e:
        print(f"Error fetching {series_id}: {e}")
        raise
    except Exception as e:
        print(f"Error processing {series_id}: {e}")
        raise

def validate_data(**context):
    """
    Basic data quality checks on ingested data
    """
    execution_date = context['ds']
    bronze_path = Path('/opt/airflow/data/bronze/fred')
    
    print(f"Validating data for {execution_date}")
    
    files_found = 0
    total_records = 0
    
    for series_id in INDICATORS.keys():
        file_path = bronze_path / f"{series_id}_{execution_date}.parquet"
        
        if file_path.exists():
            df = pd.read_parquet(file_path)
            files_found += 1
            total_records += len(df)
            print(f"Validated {series_id}: {len(df)} records")
        else:
            print(f"Warning: {series_id} file not found")
    
    print(f"Validation summary: {files_found}/{len(INDICATORS)} series ingested, {total_records} total records")
    
    if files_found == 0:
        raise ValueError("No data files were created")
    
    return {"files": files_found, "records": total_records}

# Create the DAG
with DAG(
    'fred_ingestion',
    default_args=default_args,
    description='Ingest economic indicators from FRED API',
    schedule_interval='@daily',
    catchup=False,  # Don't run for past dates on first deploy
    tags=['ingestion', 'fred', 'bronze'],
) as dag:
    
    # Create a task for each economic indicator
    fetch_tasks = []
    
    for series_id, series_name in INDICATORS.items():
        task = PythonOperator(
            task_id=f'fetch_{series_id}',
            python_callable=fetch_fred_series,
            op_kwargs={
                'series_id': series_id,
                'series_name': series_name,
            },
            provide_context=True,
        )
        fetch_tasks.append(task)
    
    # Data validation task (runs after all fetches complete)
    validate_task = PythonOperator(
        task_id='validate_data',
        python_callable=validate_data,
        provide_context=True,
    )
    
    # Set task dependencies: all fetches must complete before validation
    fetch_tasks >> validate_task