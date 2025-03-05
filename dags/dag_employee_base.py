# DAG to load data from CSV file to employee_base final table in Sqlite DB
import os
import yaml
import pandas as pd
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator 
from airflow.operators.python import BranchPythonOperator 
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.sqlite.hooks.sqlite import SqliteHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

# Dynamically get the DAG folder path
DAGS_FOLDER = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(DAGS_FOLDER, "config/employee_base_config.yaml")

# Load YAML configuration
with open(CONFIG_PATH, "r") as file:
    config = yaml.safe_load(file)

# Define paths
CSV_PATH = config["csv_path"]
TRUNCATE_SQL = config["truncate_sql"]
INSERT_DATA_SQL = config["insert_data_sql"]
LOAD_SQL = config["load_sql"]

# Default arguments
default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "catchup": False
}

# Function to load CSV data into SQLite table
def load_csv_to_sqlite():
    # Read the CSV file into a Pandas DataFrame
    df = pd.read_csv(CSV_PATH)
    
    # Establish connection to SQLite
    sqlite_hook = SqliteHook(sqlite_conn_id=config["sqlite_conn_id"])
    
    # Insert data into employee_detail table
    df.to_sql(config["stg_tbl"], sqlite_hook.get_sqlalchemy_engine(), if_exists="append", index=False)


# Function to check loaded table count
def check_records_in_table():
    sqlite_hook = SqliteHook(sqlite_conn_id=config["sqlite_conn_id"])
    result = sqlite_hook.get_first(config["count_query"])
    print(result) #logging
    if result and result[0] > 0:
        return "load_core_from_stg"
    else:
        return "end_pipeline"

# Define the DAG
with DAG(
    dag_id="dag_employee_base",
    default_args=default_args,
    schedule=None,  # Trigger manually
    start_date=days_ago(1),
    catchup=False,
    tags=['etl_pipeline']
) as dag:

    # Dummy tasks
    start_pipeline = DummyOperator(
        task_id='start_pipeline'
    )

    end_pipeline = DummyOperator(
        task_id='end_pipeline'
    )

    # Load CSV data into SQLite table
    load_csv_to_sqlite = PythonOperator(
        task_id="load_csv_to_sqlite",
        python_callable=load_csv_to_sqlite
    )

    # Run final JOIN query to insert data into employee_base table
    insert_data = SQLExecuteQueryOperator(
        task_id="insert_data",
        conn_id=config["sqlite_conn_id"],
        sql=INSERT_DATA_SQL,
    )

    # Truncate the SQLite table before loading CSV data
    truncate_tbl = SQLExecuteQueryOperator(
        task_id="truncate_tbl",
        conn_id=config["sqlite_conn_id"],
        sql=TRUNCATE_SQL,
    )

    # Load from stg to core table employee_detail
    load_core_from_stg = SQLExecuteQueryOperator(
        task_id="load_core_from_stg",
        conn_id=config["sqlite_conn_id"],
        sql=LOAD_SQL,
    )

    # Check the count of table after loading CSV data, if empty, then skip inserting
    check_stg_count = BranchPythonOperator(
    task_id="check_stg_count",
    python_callable=check_records_in_table
    )

    # Modify dependencies
    start_pipeline >> truncate_tbl
    truncate_tbl >> load_csv_to_sqlite
    load_csv_to_sqlite >> check_stg_count
    check_stg_count >> [load_core_from_stg, end_pipeline]
    load_core_from_stg >> insert_data >> end_pipeline