# Audit DAG, invokes other DAG
import os
import yaml
import pandas as pd
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.utils.edgemodifier import Label 
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

# Dynamically get the DAG folder path
DAGS_FOLDER = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(DAGS_FOLDER, "config/job_audit.yaml")

# Load YAML configuration
with open(CONFIG_PATH, "r") as file:
    config = yaml.safe_load(file)

# Define paths
START_SQL = config["start_sql"]
END_SQL = config["end_sql"]

# Default arguments
default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "catchup": False
}

# Define the DAG
with DAG(
    dag_id="dag_job_audit",
    default_args=default_args,
    schedule=None,  # Trigger manually
    start_date=days_ago(1),
    catchup=False,
    tags=['etl_pipeline','audit']
) as dag:

    # Dummy tasks
    start_pipeline = DummyOperator(
        task_id='start_pipeline'
    )

    end_pipeline = DummyOperator(
        task_id='end_pipeline'
    )

    # Start Job Audit
    start_job = SQLExecuteQueryOperator(
        task_id="start_job",
        conn_id=config["sqlite_conn_id"],
        sql=START_SQL,
    )

    # Trigger employee_base dag
    trig_dag_employee_base = TriggerDagRunOperator(
        task_id="trig_dag_employee_base",
        trigger_dag_id="dag_employee_base",
        
    )

    # End Job Audit
    end_job = SQLExecuteQueryOperator(
        task_id="end_job",
        conn_id=config["sqlite_conn_id"],
        sql=END_SQL,
    )


    # Modify dependencies
    start_pipeline >> Label('Create new Job entry') >> start_job >> Label('Trigger ETL Dag') >> trig_dag_employee_base >> Label('Update Job run details') >> end_job >> end_pipeline

