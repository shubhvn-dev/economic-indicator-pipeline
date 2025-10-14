Economic Indicators Pipeline
============================

A production-quality data engineering pipeline for analyzing global economic indicators, demonstrating modern data engineering practices and tools.

-------------------------------------------------------------------------------
Overview
-------------------------------------------------------------------------------

This project implements a complete data pipeline following the Medallion Architecture (Bronze/Silver/Gold) to ingest, process, and analyze economic data from the Federal Reserve Economic Data (FRED) API.

-------------------------------------------------------------------------------
Architecture
-------------------------------------------------------------------------------
FRED API
   ↓
[Airflow Orchestration]
   ↓
Bronze Layer (Raw Parquet files)
   ↓
[Spark Processing]
   ↓
Silver Layer (Cleaned, validated data)
   ↓
[dbt Transformations]
   ↓
Gold Layer (Star schema in DuckDB)
   ↓
[Streamlit Dashboard]

-------------------------------------------------------------------------------
Tech Stack
-------------------------------------------------------------------------------
Orchestration: Apache Airflow (LocalExecutor)
Processing: Apache Spark (PySpark in local mode)
Storage:
  - Data Lake: Parquet files on local filesystem
  - Data Warehouse: DuckDB
Transformations: dbt-core with DuckDB adapter
Visualization: Streamlit with Plotly
Containerization: Docker Compose
Version Control: Git

-------------------------------------------------------------------------------
Data Sources
-------------------------------------------------------------------------------
FRED API – Federal Reserve Economic Data
  - GDP (Gross Domestic Product)
  - UNRATE (Unemployment Rate)
  - CPIAUCSL (Consumer Price Index)
  - DFF (Federal Funds Rate)
  - UMCSENT (Consumer Sentiment Index)

-------------------------------------------------------------------------------
Features
-------------------------------------------------------------------------------
Data Engineering
  - Medallion architecture (Bronze/Silver/Gold)
  - Incremental data processing
  - Data quality validation at each layer
  - Partitioned data storage for performance
  - Slowly Changing Dimensions (SCD) handling

Analytics
  - Year-over-year and period-over-period growth
  - Correlation analysis across indicators
  - Time-series aggregations
  - Star schema data modeling

Dashboard
  - Interactive time-series visualization
  - Multi-indicator comparison
  - Correlation heatmap
  - Data export functionality
  - Real-time metrics

-------------------------------------------------------------------------------
Project Structure
-------------------------------------------------------------------------------
economic-indicators-pipeline/
├── airflow/
│   ├── dags/
│   │   ├── fred_ingestion.py
│   │   └── master_pipeline.py
│   ├── Dockerfile
│   └── requirements.txt
├── spark/
│   └── jobs/
│       └── bronze_to_silver.py
├── dbt/
│   ├── models/
│   │   ├── staging/
│   │   └── marts/
│   ├── dbt_project.yml
│   └── profiles.yml
├── streamlit/
│   └── dashboard.py
├── data/
│   ├── bronze/
│   ├── silver/
│   └── gold/
├── docker-compose.yml
├── .env
└── README.md

-------------------------------------------------------------------------------
Setup Instructions
-------------------------------------------------------------------------------

Prerequisites
  - Docker Desktop
  - Docker Compose
  - Git
  - FRED API Key (get one from https://fred.stlouisfed.org/docs/api/api_key.html)

Installation
  1. Clone the repository:
       git clone <your-repo-url>
       cd economic-indicators-pipeline

  2. Create a .env file:
       FRED_API_KEY=your_api_key_here
       AIRFLOW_UID=50000

  3. Start services:
       docker-compose up -d

  4. Access after initialization:
       Airflow UI: http://localhost:8081 (user: airflow / pass: airflow)
       Streamlit Dashboard: http://localhost:8501

-------------------------------------------------------------------------------
Running the Pipeline
-------------------------------------------------------------------------------

Manual Execution
  - Open Airflow UI → Unpause 'master_pipeline' → Trigger DAG → Monitor Graph View

Scheduled Execution
  - Runs daily at midnight when unpaused

-------------------------------------------------------------------------------
Accessing the Dashboard
-------------------------------------------------------------------------------

Ensure pipeline has run at least once, then start Streamlit manually:
  docker exec -d economic-indicators-pipeline-airflow-scheduler-1     streamlit run /opt/airflow/streamlit/dashboard.py     --server.port 8501 --server.address 0.0.0.0

Then open: http://localhost:8501

-------------------------------------------------------------------------------
Data Models
-------------------------------------------------------------------------------

Bronze Layer
  - Raw JSON/CSV converted to Parquet
  - Partitioned by source/date
  - All original fields retained

Silver Layer
  - Cleaned and validated data
  - Standardized schema and quality flags
  - Partitioned by series_id

Gold Layer (Star Schema)
  Dimensions:
    - dim_indicators (indicator metadata)
    - dim_time (date dimension)
  Facts:
    - fct_observations (economic metrics with YoY, PoP, derived analytics)

-------------------------------------------------------------------------------
Testing
-------------------------------------------------------------------------------

Run dbt tests:
  docker exec economic-indicators-pipeline-airflow-scheduler-1     bash -c "cd /opt/airflow/dbt && dbt test --profiles-dir ."

Tests ensure:
  - Not-null, unique, and referential integrity
  - Data completeness

-------------------------------------------------------------------------------
Development
-------------------------------------------------------------------------------

Adding New Indicators
  - Add new series_id in airflow/dags/fred_ingestion.py
  - Run ingestion DAG
  - dbt automatically processes it

Editing Transformations
  - Modify dbt models under dbt/models/
  - Run: dbt run --profiles-dir /opt/airflow/dbt
  - Test: dbt test --profiles-dir /opt/airflow/dbt

-------------------------------------------------------------------------------
Monitoring
-------------------------------------------------------------------------------

Airflow: DAG runs, logs, history
dbt: Data quality test results
Streamlit: Metrics and visualizations

-------------------------------------------------------------------------------
Troubleshooting
-------------------------------------------------------------------------------

Containers not starting:
  docker-compose down
  docker-compose up -d

DAG not visible:
  Wait 30s or check logs:
    docker-compose logs airflow-scheduler

Dashboard not loading:
  docker exec economic-indicators-pipeline-airflow-scheduler-1 pkill -f streamlit
  docker exec -d economic-indicators-pipeline-airflow-scheduler-1     streamlit run /opt/airflow/streamlit/dashboard.py     --server.port 8501 --server.address 0.0.0.0

-------------------------------------------------------------------------------
Future Enhancements
-------------------------------------------------------------------------------

  - Add World Bank and OECD data
  - Implement ML forecasting models
  - Anomaly detection and alerting
  - Cloud deployment (AWS/GCP)
  - CI/CD integration
  - Incremental optimization

-------------------------------------------------------------------------------
License
-------------------------------------------------------------------------------

MIT License

-------------------------------------------------------------------------------
Author
-------------------------------------------------------------------------------

Shubhan Kadam - Data Engineering Portfolio Project
Built with: Python, Apache Airflow, Apache Spark, dbt, DuckDB, Streamlit, Docker
