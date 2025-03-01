#!/usr/bin/env python3
"""
This Airflow DAG deploys the Data Ingestion Pipeline using PythonOperators.
It calls the generate_ingestion_data function from the upstreams_curation module,
and then writes a success flag.
"""

from datetime import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

# Import your ingestion function (adjust the import path if needed)
from dags.upstreams_curation import generate_ingestion_data


def write_success_flag():
    flag_path = '/opt/airflow/data/ingestion/_SUCCESS'
    import os
    os.makedirs('/opt/airflow/data/ingestion', exist_ok=True)
    with open(flag_path, 'w') as f:
        f.write("SUCCESS")
    print(f"Success flag written to {flag_path}")


default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 1, 1)
}

with DAG(
        dag_id="ingestion_pipeline",
        default_args=default_args,
       schedule_interval="*/2 * * * *",
        catchup=False
) as dag:
    start = EmptyOperator(task_id="start")

    data_ingestion = PythonOperator(
        task_id="data_ingestion",
        python_callable=generate_ingestion_data
    )

    flag_writer = PythonOperator(
        task_id="write_success_flag",
        python_callable=write_success_flag
    )

    done = EmptyOperator(task_id="done")

    start >> data_ingestion >> flag_writer >> done

data_ingestion_dag = dag


