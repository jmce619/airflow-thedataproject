from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from sqlalchemy import create_engine, text
import pandas as pd

# Import your ETL function and helper
from scripts.nba_stats import fetch_and_load, fetch_player_stats

def normalize_uri(raw_uri: str) -> str:
    if raw_uri.startswith("postgres://"):
        return "postgresql+psycopg2://" + raw_uri[len("postgres://"):]
    if raw_uri.startswith("postgresql://"):
        return raw_uri.replace("postgresql://", "postgresql+psycopg2://", 1)
    return raw_uri

def run_etl():
    conn = BaseHook.get_connection("redshift_default")
    uri  = normalize_uri(conn.get_uri())
    engine = create_engine(uri)
    fetch_and_load(engine)

def qc_fetch_profile():
    """QC test: can we fetch a single player profile?"""
    data = fetch_player_stats("LeBron James")
    if not data:
        raise ValueError("QC FAILED: fetch_player_stats returned None")
    # Log a sample field
    print(f"QC OK: fetched profile for {data['DISPLAY_FIRST_LAST']} on {data['TEAM_ABBREVIATION']}")

def qc_redshift_conn():
    """QC test: can we connect and run SELECT 1?"""
    conn = BaseHook.get_connection("redshift_default")
    uri  = normalize_uri(conn.get_uri())
    engine = create_engine(uri)
    with engine.connect() as cn:
        result = cn.execute(text("SELECT 1"))
        if result.scalar() != 1:
            raise ValueError("QC FAILED: SELECT 1 did not return 1")
    print("QC OK: Redshift connection & SELECT 1 succeeded")

def qc_redshift_read():
    """QC test: can we read from nba_player_stats?"""
    conn = BaseHook.get_connection("redshift_default")
    uri  = normalize_uri(conn.get_uri())
    engine = create_engine(uri)
    with engine.connect() as cn:
        try:
            result = cn.execute(text("SELECT COUNT(*) FROM nba_player_stats"))
            count = result.scalar()
        except Exception as e:
            raise RuntimeError(f"QC FAILED: reading nba_player_stats error: {e}")
    print(f"QC OK: nba_player_stats row count = {count}")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="nba_stats_to_redshift",
    default_args=default_args,
    description="Fetch NBA stats daily and load into Redshift Serverless with QC steps",
    schedule_interval="0 2 * * *",
    start_date=datetime(2025, 5, 24),
    catchup=False,
    tags=["nba", "redshift"],
) as dag:

    # 1) Smoke-test external Internet access
    test_internet = BashOperator(
        task_id="test_internet",
        bash_command=(
            "curl -IL https://stats.nba.com -m 10 "
            "&& echo 'Internet OK' "
            "|| (echo 'Internet check failed' && exit 1)"
        )
    )

    # 2) QC: fetch a single player profile
    qc_profile = PythonOperator(
        task_id="qc_fetch_profile",
        python_callable=qc_fetch_profile,
    )

    # 3) QC: test Redshift connectivity
    qc_conn = PythonOperator(
        task_id="qc_redshift_conn",
        python_callable=qc_redshift_conn,
    )

    # 4) QC: test reading from the target table
    qc_read = PythonOperator(
        task_id="qc_redshift_read",
        python_callable=qc_redshift_read,
    )

    # 5) The actual ETL
    etl = PythonOperator(
        task_id="fetch_and_load_nba_stats",
        python_callable=run_etl,
    )

    # DAG dependency graph
    test_internet >> qc_profile >> qc_conn >> qc_read >> etl
