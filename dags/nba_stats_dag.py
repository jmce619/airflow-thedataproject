# dags/nba_stats_dag.py
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from sqlalchemy import create_engine, text
import requests

# your existing import
from scripts.nba_stats import fetch_and_load, fetch_player_stats

# common headers for NBA API
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
                  " AppleWebKit/537.36 (KHTML, like Gecko)"
                  " Chrome/114.0.0.0 Safari/537.36"
}

def basic_http_check():
    """QC #1: can we do a simple GET to stats.nba.com?"""
    resp = requests.get("https://stats.nba.com", timeout=5)
    if resp.status_code >= 400:
        raise RuntimeError(f"HTTP {resp.status_code} from stats.nba.com")
    print("QC OK: basic HTTP GET succeeded")

def nba_api_endpoint_check():
    """QC #2: can we hit the actual NBA stats endpoint?"""
    url = ("https://stats.nba.com/stats/commonplayerinfo"
           "?PlayerID=2544")  # LeBron James ID
    resp = requests.get(url, headers=HEADERS, timeout=10)
    if resp.status_code != 200:
        raise RuntimeError(f"NBA-API returned {resp.status_code}")
    print("QC OK: NBA API endpoint returned 200")

def qc_fetch_profile():
    """QC #3: can your wrapper fetch_player_stats still pull a dict?"""
    data = fetch_player_stats("LeBron James")
    if not data:
        raise ValueError("QC FAILED: fetch_player_stats returned None")
    print(f"QC OK: fetch_player_stats fetched {data['DISPLAY_FIRST_LAST']}")

def qc_redshift_conn():
    """QC #4: can we connect & SELECT 1 from Redshift?"""
    conn = BaseHook.get_connection("redshift_default")
    raw_uri = conn.get_uri()
    # normalize postgres:// → postgresql+psycopg2://
    if raw_uri.startswith("postgres://"):
        uri = "postgresql+psycopg2://" + raw_uri[len("postgres://"):]
    elif raw_uri.startswith("postgresql://"):
        uri = raw_uri.replace("postgresql://",
                              "postgresql+psycopg2://", 1)
    else:
        uri = raw_uri

    engine = create_engine(uri)
    with engine.connect() as cn:
        res = cn.execute(text("SELECT 1"))
        if res.scalar() != 1:
            raise RuntimeError("QC FAILED: SELECT 1 did not return 1")
    print("QC OK: Redshift SELECT 1 succeeded")

def run_etl():
    conn = BaseHook.get_connection("redshift_default")
    raw_uri = conn.get_uri()
    if raw_uri.startswith("postgres://"):
        uri = "postgresql+psycopg2://" + raw_uri[len("postgres://"):]
    elif raw_uri.startswith("postgresql://"):
        uri = raw_uri.replace("postgresql://",
                              "postgresql+psycopg2://", 1)
    else:
        uri = raw_uri

    engine = create_engine(uri)
    fetch_and_load(engine)

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 0,            # QC steps will surface failures immediately
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="nba_stats_to_redshift_qc",
    default_args=default_args,
    description="QC pipeline to isolate connectivity issues",
    schedule_interval=None,
    start_date=datetime(2025, 5, 24),
    catchup=False,
) as dag:

    # 0) Smoke-test via curl (still useful)
    test_internet = BashOperator(
        task_id="test_internet",
        bash_command=(
            "curl -IL https://stats.nba.com -m 10 "
            "&& echo 'Internet OK' "
            "|| (echo 'Internet check failed' && exit 1)"
        ),
    )

    http_check = PythonOperator(
        task_id="basic_http_check",
        python_callable=basic_http_check,
    )

    nba_api_check = PythonOperator(
        task_id="nba_api_endpoint_check",
        python_callable=nba_api_endpoint_check,
    )

    qc_profile = PythonOperator(
        task_id="qc_fetch_profile",
        python_callable=qc_fetch_profile,
    )

    qc_conn = PythonOperator(
        task_id="qc_redshift_conn",
        python_callable=qc_redshift_conn,
    )

    etl = PythonOperator(
        task_id="fetch_and_load_nba_stats",
        python_callable=run_etl,
    )

    (
        test_internet
        >> http_check
        >> nba_api_check
        >> qc_profile
        >> qc_conn
        >> etl
    )
