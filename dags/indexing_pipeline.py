#!/usr/bin/env python3
"""
This Airflow DAG deploys the Data Indexing Pipeline.
It first checks for the existence of 'data/indexed/_SUCCESS'. If found, the DAG skips processing.
Otherwise, it executes the indexing function.
Branching is handled via a BranchPythonOperator with subsequent join and finish tasks.
"""

import os
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator


from dags.indexing import index_data


def check_indexing_success_flag():
    success_flag = 'data/indexed/_SUCCESS'
    if os.path.exists(success_flag):
        return "skip_indexing"
    else:
        return "index_data"

default_args = {'start_date': datetime(2025, 1, 1)}
with DAG(dag_id='indexing_pipeline', schedule_interval='@daily', default_args=default_args, catchup=False) as dag:
    begin_task = EmptyOperator(task_id='begin')

    check_flag = BranchPythonOperator(
        task_id='check_indexing_success_flag',
        python_callable=check_indexing_success_flag
    )

    index = PythonOperator(
        task_id='index_data',
        python_callable=index_data
    )

    skip_indexing = EmptyOperator(task_id='skip_indexing')

    join_task = EmptyOperator(task_id='join_tasks', trigger_rule="none_failed_min_one_success")
    finish_task = EmptyOperator(task_id='finish')

    begin_task >> check_flag
    check_flag >> index >> join_task
    check_flag >> skip_indexing >> join_task
    join_task >> finish_task

indexing_dag = dag
