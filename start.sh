#!/bin/bash

echo "Starting Economic Indicators Pipeline..."

# Check if .env exists
if [ ! -f .env ]; then
    echo "Error: .env file not found!"
    echo "Please create .env file with your FRED_API_KEY"
    exit 1
fi

# Start services
docker-compose up -d

echo "Waiting for services to start..."
sleep 30

# Start Streamlit
docker exec -d economic-indicators-pipeline-airflow-scheduler-1 \
    streamlit run /opt/airflow/streamlit/dashboard.py \
    --server.port 8501 --server.address 0.0.0.0

echo ""
echo "Pipeline started successfully!"
echo ""
echo "Access points:"
echo "  - Airflow UI: http://localhost:8081 (user: airflow, pass: airflow)"
echo "  - Streamlit Dashboard: http://localhost:8501"
echo ""
echo "To trigger the pipeline:"
echo "  1. Go to Airflow UI"
echo "  2. Unpause 'master_pipeline'"
echo "  3. Click trigger button"