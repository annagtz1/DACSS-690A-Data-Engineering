#!/usr/bin/env python3
"""
Minimal Prefect ETL for final project (meant to be run locally).
- Reads a CSV (local path or s3://)
- Enriches with exchangerate.host
- Writes results locally under pipeline_outputs/ (S3 optional)
"""
from pathlib import Path
import io
import json
from typing import Optional, Dict, Any
import pandas as pd
import requests
import boto3
from prefect import flow, task, get_run_logger

OUTPUT_DIR = Path("pipeline_outputs")
OUTPUT_DIR.mkdir(exist_ok=True)
RATES_CACHE_FILE = OUTPUT_DIR / "rates_cache.json"
DEFAULT_ORDERS_CSV = r"C:\Users\analy\iCloudDrive\Desktop\DACSS Materials and Job Hunt\DACSS 690A Data Engineering\archive (4)\olist_orders_dataset.csv"

def load_rates_cache() -> Dict[str, Optional[float]]:
    if RATES_CACHE_FILE.exists():
        try:
            return json.loads(RATES_CACHE_FILE.read_text())
        except Exception:
            return {}
    return {}

def save_rates_cache(cache: Dict[str, Optional[float]]):
    try:
        RATES_CACHE_FILE.write_text(json.dumps(cache))
    except Exception:
        pass

@task(retries=1, retry_delay_seconds=5)
def extract_csv(path: str) -> pd.DataFrame:
    logger = get_run_logger()
    logger.info(f"Reading CSV: {path}")
    df = pd.read_csv(path)
    logger.info(f"Loaded rows={len(df)} cols={len(df.columns)}")
    return df

@task(retries=2, retry_delay_seconds=5)
def fetch_rate_for_date(date_str: str) -> Optional[float]:
    logger = get_run_logger()
    try:
        resp = requests.get(f"https://api.exchangerate.host/{date_str}", params={"base":"BRL","symbols":"USD"}, timeout=10)
        resp.raise_for_status()
        rate = resp.json().get("rates", {}).get("USD")
        logger.info(f"Fetched rate for {date_str}: {rate}")
        return float(rate) if rate is not None else None
    except Exception:
        logger.exception("Failed to fetch rate")
        return None

@task
def enrich_orders(df: pd.DataFrame) -> pd.DataFrame:
    logger = get_run_logger()
    if "order_purchase_timestamp" not in df.columns:
        logger.warning("No order_purchase_timestamp column; skipping enrichment")
        df["brl_to_usd_rate"] = None
        df["payment_usd"] = None
        return df
    df = df.copy()
    df["order_purchase_timestamp"] = pd.to_datetime(df["order_purchase_timestamp"], errors="coerce")
    df["date_only"] = df["order_purchase_timestamp"].dt.date.astype(str)
    cache = load_rates_cache()
    dates = df["date_only"].dropna().unique().tolist()
    for d in dates:
        if d not in cache:
            # call the task function directly; Prefect will run tasks when the flow executes
            cache[d] = fetch_rate_for_date(d)
    # Coerce any Prefect task-returned objects to simple floats where possible
    for k, v in list(cache.items()):
        if isinstance(v, float) or v is None:
            continue
        try:
            cache[k] = float(v)
        except Exception:
            cache[k] = None
    save_rates_cache(cache)
    df["brl_to_usd_rate"] = df["date_only"].map(cache)
    amt_col = "price" if "price" in df.columns else "payment_value" if "payment_value" in df.columns else None
    if amt_col:
        df["payment_usd"] = df[amt_col] * df["brl_to_usd_rate"]
    else:
        df["payment_usd"] = None
    df = df.drop(columns=["date_only"])
    missing = int(df["brl_to_usd_rate"].isna().sum()) if "brl_to_usd_rate" in df.columns else 0
    if missing > 0:
        logger.warning(f"{missing} rows missing conversion rate")
    return df

@task(retries=2, retry_delay_seconds=5)
def upload_df_to_s3(df: pd.DataFrame, bucket: str, key: str) -> str:
    logger = get_run_logger()
    logger.info(f"Uploading to s3://{bucket}/{key}")
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)
    s3 = boto3.client("s3")
    s3.put_object(Bucket=bucket, Key=key, Body=csv_buffer.getvalue().encode("utf-8"))
    url = f"s3://{bucket}/{key}"
    logger.info(f"Uploaded to {url}")
    return url

@flow(name="olist_csv_api_pipeline")
def data_processing_flow(csv_orders: str = DEFAULT_ORDERS_CSV, s3_bucket: str = "") -> dict:
    logger = get_run_logger()
    logger.info("Starting minimal ETL")
    orders = extract_csv(csv_orders)
    enriched = enrich_orders(orders)
    try:
        enriched["order_purchase_timestamp"] = pd.to_datetime(enriched["order_purchase_timestamp"], errors="coerce")
        enriched["month"] = enriched["order_purchase_timestamp"].dt.to_period("M")
        monthly = enriched.groupby("month")["payment_usd"].sum().reset_index().rename(columns={"payment_usd":"monthly_sales_usd"})
    except Exception:
        monthly = pd.DataFrame()
    result = {"monthly": None, "enriched": None}
    if s3_bucket:
        result["monthly"] = upload_df_to_s3(monthly, s3_bucket, "processed/monthly_sales_usd.csv")
        result["enriched"] = upload_df_to_s3(enriched, s3_bucket, "processed/enriched_orders.csv")
    else:
        monthly.to_csv(OUTPUT_DIR/"monthly_sales_usd.csv", index=False)
        enriched.to_csv(OUTPUT_DIR/"enriched_orders.csv", index=False)
        result["monthly"] = str(OUTPUT_DIR/"monthly_sales_usd.csv")
        result["enriched"] = str(OUTPUT_DIR/"enriched_orders.csv")
    logger.info("ETL done")
    return result

if __name__ == "__main__":
    data_processing_flow()
