from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta
import subprocess

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def run_spark_job():
    """
    Execute Spark job to process Bronze to Silver layer
    """
    result = subprocess.run(
        ["python", "/opt/airflow/spark/jobs/bronze_to_silver.py"], capture_output=True, text=True
    )
    print(result.stdout)
    if result.returncode != 0:
        print(result.stderr)
        raise Exception(f"Spark job failed with return code {result.returncode}")


def run_dbt_models():
    """
    Execute dbt to build Gold layer models
    """
    result = subprocess.run(
        ["dbt", "run", "--profiles-dir", "/opt/airflow/dbt", "--project-dir", "/opt/airflow/dbt"],
        capture_output=True,
        text=True,
    )
    print(result.stdout)
    if result.returncode != 0:
        print(result.stderr)
        raise Exception(f"dbt run failed with return code {result.returncode}")


def run_dbt_tests():
    """
    Execute dbt data quality tests
    """
    result = subprocess.run(
        ["dbt", "test", "--profiles-dir", "/opt/airflow/dbt", "--project-dir", "/opt/airflow/dbt"],
        capture_output=True,
        text=True,
    )
    print(result.stdout)
    if result.returncode != 0:
        print(result.stderr)
        raise Exception(f"dbt test failed with return code {result.returncode}")


with DAG(
    "master_pipeline",
    default_args=default_args,
    description="Master pipeline: FRED ingestion -> Spark processing -> dbt transformation",
    schedule_interval="@daily",
    catchup=False,
    tags=["master", "pipeline", "orchestration"],
) as dag:
    # Trigger FRED ingestion DAG
    trigger_fred = TriggerDagRunOperator(
        task_id="trigger_fred_ingestion",
        trigger_dag_id="fred_ingestion",
        wait_for_completion=True,
        poke_interval=30,
    )

    # Run Spark processing
    spark_processing = PythonOperator(
        task_id="spark_bronze_to_silver",
        python_callable=run_spark_job,
    )

    # Run dbt models
    dbt_run = PythonOperator(
        task_id="dbt_build_gold_layer",
        python_callable=run_dbt_models,
    )

    # Run dbt tests
    dbt_test = PythonOperator(
        task_id="dbt_run_tests",
        python_callable=run_dbt_tests,
    )

    # Set dependencies
    trigger_fred >> spark_processing >> dbt_run >> dbt_test
