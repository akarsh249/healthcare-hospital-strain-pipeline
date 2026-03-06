import json
from datetime import datetime, timezone
import requests

from src.utils.s3_client import get_s3_client

DATASET_JSON_URL = "https://data.cdc.gov/resource/mpgq-jmmr.json"

BUCKET = "lake"
PREFIX = "bronze/hrd"

def ensure_bucket(s3):
    existing = [b["Name"] for b in s3.list_buckets().get("Buckets", [])]
    if BUCKET not in existing:
        s3.create_bucket(Bucket=BUCKET)

def fetch_latest(limit: int = 5000):
    params = {"$limit": limit}
    r = requests.get(DATASET_JSON_URL, params=params, timeout=60)
    r.raise_for_status()
    return r.json()

def write_jsonl_to_s3(rows, s3_key: str):
    s3 = get_s3_client()
    ensure_bucket(s3)
    body = "\n".join(json.dumps(row) for row in rows).encode("utf-8")
    s3.put_object(Bucket=BUCKET, Key=s3_key, Body=body, ContentType="application/json")

def run_ingestion():
    started = datetime.now(timezone.utc)
    rows = fetch_latest()
    ts = started.strftime("%Y%m%dT%H%M%SZ")
    s3_key = f"{PREFIX}/ingest_ts={ts}/data.jsonl"
    write_jsonl_to_s3(rows, s3_key)
    return {"records": len(rows), "s3_key": s3_key, "ingested_at": ts}
