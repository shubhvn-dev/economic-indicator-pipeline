# Architecture Overview

## System Architecture
┌────────────────────────────────────────────────────────────────┐
│                        Data Sources                            │
│                                                                │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐                      │
│  │ FRED API │  │ World    │  │  OECD    │  (Future sources)    │
│  │          │  │ Bank API │  │   API    │                      │
│  └────┬─────┘  └────┬─────┘  └────┬─────┘                      │
└───────┼─────────────┼─────────────┼────────────────────────────┘
        │             │             │
        └─────────────┴─────────────┘
                      │
┌─────────────▼────────────────────────────────┐
│   Apache Airflow (Orchestration Layer)       │
│                                              │
│  ┌──────────────┐      ┌─────────────────┐   │
│  │ fred_        │      │ master_         │   │
│  │ ingestion    │◄─────┤ pipeline        │   │
│  └──────┬───────┘      └─────────────────┘   │
└─────────┼────────────────────────────────────┘
          │
┌─────────▼─────────────────────────────────────┐
│         Bronze Layer (Raw Data)               │
│                                               │
│  ┌──────────────────────────────────────────┐ │
│  │  Parquet Files (partitioned by date)     │ │
│  │  - fred/GDP_2025-10-13.parquet           │ │
│  │  - fred/UNRATE_2025-10-13.parquet        │ │
│  │  Raw JSON → Parquet conversion           │ │
│  └──────────────────────────────────────────┘ │
└─────────┬─────────────────────────────────────┘
          │
┌─────────▼─────────────────────────────────────┐
│   Apache Spark (Processing Layer)             │
│                                               │
│  ┌──────────────────────────────────────────┐ │
│  │ bronze_to_silver.py                      │ │
│  │ - Data validation                        │ │
│  │ - Type conversion                        │ │
│  │ - Missing value handling                 │ │
│  │ - Quality checks                         │ │
│  └──────────────────────────────────────────┘ │
└─────────┬─────────────────────────────────────┘
│
┌─────────▼─────────────────────────────────────┐
│        Silver Layer (Cleaned Data)            │
│                                               │
│  ┌──────────────────────────────────────────┐ │
│  │  Parquet Files (partitioned by series)   │ │
│  │  - series_id=GDP/.parquet                │ │
│  │  - series_id=UNRATE/.parquet             │ │
│  │  Validated, typed, standardized          │ │
│  └──────────────────────────────────────────┘ │
└─────────┬─────────────────────────────────────┘
          │
┌─────────▼─────────────────────────────────────┐
│         dbt (Transformation Layer)            │
│                                               │
│  ┌──────────────────────────────────────────┐ │
│  │ Staging Models                           │ │
│  │  └─ stg_fred_observations                │ │
│  │                                          │ │
│  │ Mart Models                              │ │
│  │  ├─ dim_indicators (dimension)           │ │
│  │  ├─ dim_time (dimension)                 │ │
│  │  └─ fct_observations (fact)              │ │
│  │                                          │ │
│  │ Data Tests (13 tests)                    │ │
│  └──────────────────────────────────────────┘ │
└─────────┬─────────────────────────────────────┘
          │
┌─────────▼─────────────────────────────────────┐
│         Gold Layer (Analytics-Ready)          │
│                                               │
│  ┌──────────────────────────────────────────┐ │
│  │  DuckDB Database                         │ │
│  │                                          │ │
│  │  Star Schema:                            │ │
│  │  ┌─────────────────┐                     │ │
│  │  │ dim_indicators  │◄───┐                │ │
│  │  └─────────────────┘    │                │ │
│  │  ┌─────────────────┐    │                │ │
│  │  │ dim_time        │◄───┤                │ │
│  │  └─────────────────┘    │                │ │
│  │  ┌─────────────────┐    │                │ │
│  │  │fct_observations │────┘                │ │
│  │  └─────────────────┘                     │ │
│  └──────────────────────────────────────────┘ │
└─────────┬─────────────────────────────────────┘
          │
┌─────────▼─────────────────────────────────────┐
│      Streamlit (Visualization Layer)          │
│                                               │
│  ┌──────────────────────────────────────────┐ │
│  │ Interactive Dashboard                    │ │
│  │ - Time series charts                     │ │
│  │ - YoY analysis                           │ │
│  │ - Correlation matrix                     │ │
│  │ - Data export                            │ │
│  └──────────────────────────────────────────┘ │
└───────────────────────────────────────────────┘

## Data Flow

1. **Ingestion (Bronze)**
   - Airflow triggers FRED API calls
   - Raw JSON responses saved as Parquet
   - Partitioned by date and source
   - Metadata added (ingestion timestamp)

2. **Processing (Silver)**
   - Spark reads all Bronze files
   - Validates data types and ranges
   - Handles missing values
   - Adds quality flags
   - Partitions by series_id

3. **Transformation (Gold)**
   - dbt reads Silver layer
   - Creates staging views
   - Builds dimension tables
   - Constructs fact table with metrics
   - Runs 13 data quality tests

4. **Visualization**
   - Streamlit connects to DuckDB
   - Queries Gold layer tables
   - Renders interactive charts
   - Enables data exploration

## Technology Decisions

### Why Medallion Architecture?
- Clear separation of concerns
- Progressive data quality improvement
- Easy to debug and maintain
- Industry standard pattern

### Why DuckDB?
- Embedded database (no separate server)
- Excellent for analytics workloads
- Fast aggregations
- Small footprint

### Why Local Spark?
- Demonstrates distributed processing concepts
- No cluster management overhead
- Sufficient for project data volume
- Easy to scale to cluster mode

### Why dbt?
- SQL-based transformations
- Built-in testing framework
- Excellent documentation
- Version control friendly

## Scalability Considerations

### Current Limitations
- Single machine processing
- Local file storage
- No distributed computing

### Cloud Migration Path
Local Storage    →  AWS S3 / GCS
Local DuckDB     →  Snowflake / BigQuery / Redshift
Docker Compose   →  Kubernetes / ECS
Local Airflow    →  AWS MWAA / Cloud Composer
Local Spark      →  EMR / Dataproc

## Monitoring & Observability

- **Pipeline Health**: Airflow UI task status
- **Data Quality**: dbt test results
- **Performance**: Airflow task duration logs
- **Data Freshness**: Ingestion timestamps

## Security

- API keys in environment variables
- Read-only database connections for dashboard
- Container isolation
- No exposed credentials in code