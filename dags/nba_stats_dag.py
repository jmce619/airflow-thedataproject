from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook

# import your ETL function
from scripts.nba_stats import fetch_and_load

def get_redshift_url(conn_id="redshift_default"):
    conn = BaseHook.get_connection(conn_id)
    return conn.get_uri()

def run_etl():
    os.environ["REDSHIFT_URL"] = get_redshift_url()
    fetch_and_load()

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="nba_stats_to_redsht",
    default_args=default_args,
    description="Fetch NBA stats daily and load into Redshift",
    schedule_interval="0 2 * * *",      # every day at 02:00 UTC
    start_date=datetime(2025, 5, 24),
    catchup=False,
    tags=["nba", "redshift"],
) as dag:

    etl_task = PythonOperator(
        task_id="fetch_and_load_nba_stats",
        python_callable=run_etl,
    )
