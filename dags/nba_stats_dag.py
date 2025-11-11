# dags/nba_stats_to_redshift_qc.py
from __future__ import annotations

import requests
from datetime import timedelta

import pendulum
from sqlalchemy import create_engine, text

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook

from scripts.nba_stats import fetch_and_load, fetch_player_stats

# Reuse the same headers for QC
API_HEADERS = {
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.5",
    "Connection": "keep-alive",
    "Host": "stats.nba.com",
    "Origin": "https://stats.nba.com",
    "Referer": "https://stats.nba.com/",
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/114.0.0.0 Safari/537.36"
    ),
    "x-nba-stats-origin": "stats",
    "x-nba-stats-token": "true",
}

def normalize_uri(raw_uri: str) -> str:
    if raw_uri.startswith("postgres://"):
        return "postgresql+psycopg2://" + raw_uri[len("postgres://"):]
    if raw_uri.startswith("postgresql://"):
        return raw_uri.replace("postgresql://", "postgresql+psycopg2://", 1)
    return raw_uri

def basic_http_check():
    resp = requests.get("https://stats.nba.com", timeout=10, headers={"User-Agent": API_HEADERS["User-Agent"]})
    if resp.status_code >= 400:
        raise RuntimeError(f"HTTP {resp.status_code} from stats.nba.com")
    print("QC OK: basic HTTP GET succeeded")

def nba_api_endpoint_check():
    url = "https://stats.nba.com/stats/commonplayerinfo?PlayerID=2544"
    resp = requests.get(url, headers=API_HEADERS, timeout=20)
    if resp.status_code != 200:
        raise RuntimeError(f"NBA-API returned {resp.status_code}")
    print("QC OK: NBA API endpoint returned 200")

def qc_fetch_profile():
    data = fetch_player_stats("LeBron James")
    if not data:
        raise ValueError("QC FAILED: fetch_player_stats returned None")
    print(f"QC OK: fetch_player_stats fetched {data['DISPLAY_FIRST_LAST']}")

def qc_redshift_conn():
    conn = BaseHook.get_connection("redshift_default")
    uri = normalize_uri(conn.get_uri())
    engine = create_engine(uri)
    with engine.connect() as cn:
        if cn.execute(text("SELECT 1")).scalar() != 1:
            raise RuntimeError("QC FAILED: SELECT 1 did not return 1")
    print("QC OK: Redshift SELECT 1 succeeded")

def run_etl():
    conn = BaseHook.get_connection("redshift_default")
    uri = normalize_uri(conn.get_uri())
    engine = create_engine(uri)
    fetch_and_load(engine)

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="nba_stats_to_redshift_qc",
    description="NBA→Redshift with QC steps",
    # Airflow 3.x: use `schedule` (not schedule_interval)
    schedule=None,
    start_date=pendulum.datetime(2025, 5, 24, tz="UTC"),
    catchup=False,
    default_args=default_args,
    tags=["nba", "redshift", "qc"],
) as dag:

    test_internet = BashOperator(
        task_id="test_internet",
        bash_command=(
            "curl -IL https://stats.nba.com -m 10 "
            "&& echo 'Internet OK' "
            "|| (echo 'Internet failed' && exit 1)"
        ),
    )

    http_check = PythonOperator(
        task_id="basic_http_check",
        python_callable=basic_http_check,
    )

    api_check = PythonOperator(
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

    test_internet >> http_check >> api_check >> qc_profile >> qc_conn >> etl
