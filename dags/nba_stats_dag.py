from datetime import datetime, timedelta
import os

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from sqlalchemy import create_engine

# import your script
from packages.nba_stats import main as fetch_and_load

# If you used an Airflow connection:
def get_redshift_url(conn_id="redshift_default"):
    conn = BaseHook.get_connection(conn_id)
    # SQLAlchemy URL (e.g. "postgresql+psycopg2://user:pw@host:port/db")
    return conn.get_uri()

# pass the URL via an env var before calling main()
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
    dag_id="nba_stats_to_redshift",
    default_args=default_args,
    description="Fetch NBA stats daily and load into Redshift",
    schedule_interval="0 2 * * *",      # every day at 02:00 UTC
    start_date=datetime(2025, 5, 24),    # pick your start date
    catchup=False,
    tags=["nba", "redshift"],
) as dag:

    etl_task = PythonOperator(
        task_id="fetch_and_load_nba_stats",
        python_callable=run_etl,
    )
