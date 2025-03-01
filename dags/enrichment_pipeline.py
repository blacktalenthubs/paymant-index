#!/usr/bin/env python3
"""
This Airflow DAG deploys the Data Enrichment Pipeline using a PythonOperator.
It calls the enrich_data function from the enrichment_app module.
"""

from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

from dags.enrichment import enrich_data

# Import the enrichment function; adjust the import path if needed.

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 1, 1)
}

with DAG(
    dag_id="enrichment_pipeline",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False
) as dag:

    start = EmptyOperator(task_id="start")

    data_enrichment = PythonOperator(
        task_id="data_enrichment",
        python_callable=enrich_data
    )

    done = EmptyOperator(task_id="done")

    start >> data_enrichment >> done

enrichment_dag = dag
