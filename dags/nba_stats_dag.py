from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from sqlalchemy import create_engine

# Import your ETL function
from scripts.nba_stats import fetch_and_load

def run_etl():
    # 1) Get the connection URI from Airflow
    conn = BaseHook.get_connection("redshift_default")
    raw_uri = conn.get_uri()  
    # e.g. "postgres://admin:pw@host:5439/dev"
    
    # 2) Normalize it to a SQLAlchemy‐compatible form
    if raw_uri.startswith("postgres://"):
        uri = "postgresql+psycopg2://" + raw_uri[len("postgres://"):]
    elif raw_uri.startswith("postgresql://"):
        uri = raw_uri.replace("postgresql://", "postgresql+psycopg2://", 1)
    else:
        uri = raw_uri
    
    # 3) Build the engine and run your ETL
    engine = create_engine(uri)
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

    # 1) Smoke-test external Internet access
    test_internet = BashOperator(
        task_id="test_internet",
        bash_command=(
            "curl -IL https://stats.nba.com -m 10 "
            "&& echo 'Internet OK' "
            "|| (echo 'Internet check failed' && exit 1)"
        )
    )

    # 2) Run the ETL
    etl_task = PythonOperator(
        task_id="fetch_and_load_nba_stats",
        python_callable=run_etl,
    )

    # Wire them together
    test_internet >> etl_task
