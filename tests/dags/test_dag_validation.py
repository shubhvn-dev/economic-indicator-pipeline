import os

import pytest
from airflow.models import DagBag


def get_dags():
    """Load all DAGs from the dags folder"""
    dag_bag = DagBag(dag_folder="airflow/dags", include_examples=False)
    return dag_bag


def test_no_import_errors():
    """Test that all DAGs can be imported without errors"""
    dag_bag = get_dags()
    assert len(dag_bag.import_errors) == 0, f"DAG import errors: {dag_bag.import_errors}"


def test_fred_ingestion_dag_structure():
    """Test fred_ingestion DAG structure"""
    dag_bag = get_dags()
    assert "fred_ingestion" in dag_bag.dags

    dag = dag_bag.get_dag("fred_ingestion")
    assert dag is not None
    assert len(dag.tasks) == 6  # 5 fetch tasks + 1 validate task


def test_master_pipeline_dag_structure():
    """Test master_pipeline DAG structure"""
    dag_bag = get_dags()
    assert "master_pipeline" in dag_bag.dags

    dag = dag_bag.get_dag("master_pipeline")
    assert dag is not None
    assert len(dag.tasks) == 4  # trigger + spark + dbt run + dbt test


def test_dag_tags():
    """Test that DAGs have proper tags"""
    dag_bag = get_dags()

    fred_dag = dag_bag.get_dag("fred_ingestion")
    assert "ingestion" in fred_dag.tags
    assert "fred" in fred_dag.tags

    master_dag = dag_bag.get_dag("master_pipeline")
    assert "master" in master_dag.tags
    assert "orchestration" in master_dag.tags


def test_dag_schedules():
    """Test DAG scheduling"""
    dag_bag = get_dags()

    fred_dag = dag_bag.get_dag("fred_ingestion")
    assert fred_dag.schedule_interval == "@daily"

    master_dag = dag_bag.get_dag("master_pipeline")
    assert master_dag.schedule_interval == "@daily"


def test_dag_retries():
    """Test retry configuration"""
    dag_bag = get_dags()

    for dag_id in dag_bag.dag_ids:
        dag = dag_bag.get_dag(dag_id)
        for task in dag.tasks:
            assert task.retries >= 1, f"Task {task.task_id} should have retries configured"
