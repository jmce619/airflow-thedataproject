from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from sqlalchemy import create_engine

# Import your ETL function
from scripts.nba_stats import fetch_and_load

def run_etl():
    # Pull connection info from Airflow Connections (Conn Id: 'redshift_default')
    conn = BaseHook.get_connection("redshift_default")
    engine = create_engine(conn.get_uri())  
    fetch_and_load(engine)

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="nba_stats_to_redshift",
    default_args=default_args,
    description="Fetch NBA stats daily and load into Redshift Serverless",
    schedule_interval="0 2 * * *",     # Daily at 02:00 UTC
    start_date=datetime(2025, 5, 24),
    catchup=False,
    tags=["nba", "redshift"],
) as dag:

    etl_task = PythonOperator(
        task_id="fetch_and_load_nba_stats",
        python_callable=run_etl,
    )
