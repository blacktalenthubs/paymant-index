#!/usr/bin/env python3
"""
This Airflow DAG deploys the Data Quality Pipeline.
It checks for the quality flag in 'data/quality/_SUCCESS'. If the flag exists, quality validation is skipped.
Otherwise, it executes the quality validation process.
Branching is handled via a BranchPythonOperator, with subsequent join and complete tasks.
"""

from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from dags.data_quality import validate_data  # Import the function from data_quality.py

def check_quality_success_flag():
    success_flag = 'data/quality/_SUCCESS'
    if __import__("os").path.exists(success_flag):
        return "skip_quality"
    else:
        return "validate_data"

default_args = {'start_date': datetime(2025, 1, 1)}

with DAG(
    dag_id="data_quality_pipeline",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False
) as dag:

    init_task = EmptyOperator(task_id="init")

    check_flag = BranchPythonOperator(
        task_id="check_quality_success_flag",
        python_callable=check_quality_success_flag
    )

    quality = PythonOperator(
        task_id="validate_data_code",
        python_callable=validate_data
    )

    skip_quality = EmptyOperator(task_id="skip_quality")

    join_task = EmptyOperator(task_id="join_tasks", trigger_rule="none_failed_min_one_success")
    complete_task = EmptyOperator(task_id="complete")

    init_task >> check_flag
    check_flag >> quality >> join_task
    check_flag >> skip_quality >> join_task
    join_task >> complete_task

quality_dag = dag
