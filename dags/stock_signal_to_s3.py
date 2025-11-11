# dags/stock_signal_to_s3.py
from __future__ import annotations

import os
import json
import time
import math
import logging
from io import StringIO
from datetime import datetime, timedelta

import requests
import pandas as pd
import numpy as np
import boto3

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

# ============================
# Config / Constants
# ============================

# Prefer Airflow Variables; fallback to env for local dev
ALPHA_VANTAGE_KEY = Variable.get("ALPHA_VANTAGE_KEY", default_var=os.environ.get("ALPHA_VANTAGE_KEY"))
S3_BUCKET = Variable.get("SIGNAL_BUCKET", default_var=os.environ.get("SIGNAL_BUCKET", "data-app-stg"))
S3_PREFIX = Variable.get("SIGNAL_PREFIX", default_var=os.environ.get("SIGNAL_PREFIX", "stock-json/"))

# Set your rate-plan-appropriate batch size (AlphaVantage premium ~75/min; free is much lower)
BATCH_SIZE = int(Variable.get("ALPHA_VANTAGE_BATCH_SIZE", default_var="75"))

# Limit universe if you want (e.g., only symbols >= 'P')
SYMBOL_CUTOFF = Variable.get("SYMBOL_CUTOFF", default_var="P")  # e.g. "P" means P..Z
ONLY_US_ACTIVE = True  # the listing has other exchanges too—filter to NYSE/Nasdaq/AMEX if desired

LISTING_URL = f"https://www.alphavantage.co/query?function=LISTING_STATUS&apikey={ALPHA_VANTAGE_KEY}"
BASE_URL = "https://www.alphavantage.co/query"

log = logging.getLogger(__name__)
s3 = boto3.client("s3")


# ============================
# Indicator Calculations
# ============================

def calc_zscore_signal(df: pd.DataFrame, window=20, threshold=1.0) -> pd.Series:
    df = df.copy()
    df["mean"] = df["close"].rolling(window).mean()
    df["std"] = df["close"].rolling(window).std()
    df["zscore"] = (df["close"] - df["mean"]) / df["std"]
    sig = pd.Series(0, index=df.index)
    sig[df["zscore"] > threshold] = -1
    sig[df["zscore"] < -threshold] = 1
    return sig


def calc_sma_signal(df: pd.DataFrame, fast=20, slow=50) -> pd.Series:
    df = df.copy()
    df["sma_fast"] = df["close"].rolling(fast).mean()
    df["sma_slow"] = df["close"].rolling(slow).mean()
    cross = df["sma_fast"] - df["sma_slow"]
    sig = pd.Series(0, index=df.index)
    sig[cross > 0] = 1
    sig[cross < 0] = -1
    # Report only *change* in regime as signal spikes (+1/-1) like your script
    sig = sig.diff().fillna(0).astype(int)
    return sig


def calc_stoch_signal(df: pd.DataFrame, k_period=14, d_period=3, overbought=80, oversold=20) -> pd.Series:
    df = df.copy()
    low_min = df["low"].rolling(window=k_period).min()
    high_max = df["high"].rolling(window=k_period).max()
    # Avoid division by zero
    denom = (high_max - low_min).replace(0, np.nan)
    k = 100 * (df["close"] - low_min) / denom
    d = k.rolling(window=d_period).mean()

    sig = pd.Series(0, index=df.index)
    # Buy: %K crosses above %D while oversold -> simplified version from your script
    buy = (k < oversold) & (k > d.shift(1))
    # Sell: %K crosses below %D while overbought
    sell = (k > overbought) & (k < d.shift(1))
    sig[buy] = 1
    sig[sell] = -1
    return sig.fillna(0).astype(int)


# ============================
# Data Fetchers
# ============================

def fetch_listing() -> list[str]:
    log.info("Downloading ticker listing...")
    r = requests.get(LISTING_URL, timeout=60)
    r.raise_for_status()
    df = pd.read_csv(StringIO(r.text))

    if ONLY_US_ACTIVE:
        # Keep only Active US listings on common US exchanges
        us_exchanges = {"NYSE", "Nasdaq", "NYSE MKT", "NYSE Arca", "BATS", "AMEX"}
        df = df[(df["status"] == "Active") & (df["exchange"].isin(us_exchanges))]
    else:
        df = df[df["status"] == "Active"]

    tickers = df["symbol"].dropna().astype(str).tolist()
    log.info("Found %d active tickers.", len(tickers))
    return tickers


def fetch_daily(ticker: str) -> pd.DataFrame | None:
    params = {
        "function": "TIME_SERIES_DAILY_ADJUSTED",
        "symbol": ticker,
        "outputsize": "full",
        "apikey": ALPHA_VANTAGE_KEY,
    }
    r = requests.get(BASE_URL, params=params, timeout=90)
    # Alpha Vantage often returns HTTP 200 with an error JSON—check content
    try:
        data = r.json()
    except Exception:
        log.warning("Non-JSON response for %s", ticker)
        return None

    if "Note" in data or "Error Message" in data or "Information" in data:
        log.warning("AlphaVantage throttle/error for %s: %s", ticker, data)
        return None

    ts = data.get("Time Series (Daily)", {})
    if not ts:
        return None

    df = pd.DataFrame(ts).T
    df.index = pd.to_datetime(df.index, errors="coerce")
    df = df.sort_index()
    df = df.rename(
        columns={
            "1. open": "open",
            "2. high": "high",
            "3. low": "low",
            "4. close": "close",
            "5. adjusted close": "adj_close",
            "6. volume": "volume",
        }
    )
    for col in ["open", "high", "low", "close", "adj_close", "volume"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # Keep approx one year of trading days
    df = df.tail(252).dropna(subset=["close"])
    if df.empty:
        return None
    return df


# ============================
# S3 Upload
# ============================

def upload_json_to_s3(symbol: str, records: list[dict]) -> None:
    key = f"{S3_PREFIX}{symbol}.json"
    body = json.dumps(records, separators=(",", ":"), ensure_ascii=False)
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=key,
        Body=body.encode("utf-8"),
        ContentType="application/json",
        CacheControl="max-age=300",
    )
    log.info("Uploaded %s (%d bytes)", key, len(body))


# ============================
# Main task callable
# ============================

def run_pipeline(**context):
    # 1) Universe
    tickers = fetch_listing()

    # 2) Optional alphabet cutoff (e.g., symbols >= 'P')
    cutoff = (SYMBOL_CUTOFF or "").strip()
    if cutoff:
        tickers = [t for t in tickers if isinstance(t, str) and t and t[0].upper() >= cutoff.upper()]

    if not tickers:
        log.warning("No tickers to process after filtering.")
        return

    log.info("Processing %d tickers; batch_size=%d; cutoff=%s", len(tickers), BATCH_SIZE, cutoff or "None")

    # 3) Loop in batches to respect rate limits
    for i in range(0, len(tickers), BATCH_SIZE):
        batch = tickers[i : i + BATCH_SIZE]
        log.info("Batch %d: symbols %d..%d", (i // BATCH_SIZE) + 1, i + 1, i + len(batch))

        for symbol in batch:
            try:
                df = fetch_daily(symbol)
                if df is None or df.empty:
                    log.info("Skipping %s (no data).", symbol)
                    continue

                z = calc_zscore_signal(df)
                s = calc_sma_signal(df)
                k = calc_stoch_signal(df)

                out = pd.DataFrame(
                    {
                        "date": df.index.strftime("%Y-%m-%d"),
                        "close": df["close"].round(6),
                        "zscore_signal": z.astype(int),
                        "sma_signal": s.astype(int),
                        "stoch_signal": k.astype(int),
                    }
                )

                # Convert to list[dict] for compact JSON
                records = out.to_dict(orient="records")
                upload_json_to_s3(symbol, records)
                log.info("%s processed.", symbol)

            except Exception as e:
                log.exception("Error for %s: %s", symbol, e)

        # 4) Throttle between batches (Alpha Vantage limit)
        if i + BATCH_SIZE < len(tickers):
            log.info("Sleeping 61s to respect Alpha Vantage rate limit...")
            time.sleep(61)


# ============================
# DAG Definition
# ============================

default_args = {
    "owner": "data-platform",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="stock_signal_to_s3",
    description="Fetch prices from Alpha Vantage, compute signals, and upload JSON to S3 for the app",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule_interval="0 23 * * 1-5",  # Weekdays @ 23:00 UTC (adjust to your needs)
    catchup=False,
    max_active_runs=1,  # keep it simple; avoid overlapping runs
    tags=["stocks", "signals", "alpha-vantage", "s3"],
) as dag:

    run = PythonOperator(
        task_id="compute_and_upload_signals",
        python_callable=run_pipeline,
        provide_context=True,
    )

    run
