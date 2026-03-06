import json
from datetime import datetime, timezone
import pandas as pd
import psycopg2

from src.utils.s3_client import get_s3_client

BUCKET = "lake"

def read_jsonl_from_s3(s3_key: str) -> pd.DataFrame:
    s3 = get_s3_client()
    obj = s3.get_object(Bucket=BUCKET, Key=s3_key)
    text = obj["Body"].read().decode("utf-8")
    rows = [json.loads(line) for line in text.splitlines() if line.strip()]
    return pd.DataFrame(rows)

def _to_float(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")

def compute_strain_index(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()

    candidate_pairs = [
        ("icu_beds_used", "icu_beds_total"),
        ("total_icu_beds_used_7d_avg", "total_icu_beds_7d_avg"),
        ("inpatient_beds_used", "inpatient_beds_total"),
        ("total_inpatient_beds_used_7d_avg", "total_inpatient_beds_7d_avg"),
    ]

    def utilization(used_col, total_col):
        if used_col in out.columns and total_col in out.columns:
            used = _to_float(out[used_col])
            total = _to_float(out[total_col])
            return (used / total.replace({0: pd.NA})) * 100.0
        return pd.Series([pd.NA] * len(out))

    icu_util = pd.Series([pd.NA] * len(out))
    bed_util = pd.Series([pd.NA] * len(out))

    for u, t in candidate_pairs:
        util = utilization(u, t)
        if util.notna().any():
            if "icu" in u:
                icu_util = util
            else:
                bed_util = util

    out["icu_util_pct"] = icu_util
    out["bed_util_pct"] = bed_util

    score = (0.6 * out["icu_util_pct"].fillna(0) + 0.4 * out["bed_util_pct"].fillna(0)).clip(0, 100)
    out["strain_score_0_100"] = score

    out["pipeline_loaded_at_utc"] = datetime.now(timezone.utc).isoformat()
    return out

def load_to_postgres(df: pd.DataFrame, table: str):
    conn = psycopg2.connect(
        host="postgres",
        dbname="warehouse",
        user="postgres",
        password="postgres",
        port=5432,
    )
    conn.autocommit = True
    cur = conn.cursor()

    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS gold.{table} (
          id BIGSERIAL PRIMARY KEY,
          pipeline_loaded_at_utc TEXT,
          strain_score_0_100 DOUBLE PRECISION,
          icu_util_pct DOUBLE PRECISION,
          bed_util_pct DOUBLE PRECISION,
          payload JSONB
        );
    """)

    for _, row in df.iterrows():
        payload = row.to_dict()
        cur.execute(
            f"""
            INSERT INTO gold.{table} (pipeline_loaded_at_utc, strain_score_0_100, icu_util_pct, bed_util_pct, payload)
            VALUES (%s, %s, %s, %s, %s::jsonb)
            """,
            (
                row.get("pipeline_loaded_at_utc"),
                float(row.get("strain_score_0_100", 0)),
                None if pd.isna(row.get("icu_util_pct")) else float(row.get("icu_util_pct")),
                None if pd.isna(row.get("bed_util_pct")) else float(row.get("bed_util_pct")),
                json.dumps(payload),
            ),
        )

    cur.close()
    conn.close()

def run_bronze_to_gold(s3_key: str):
    df = read_jsonl_from_s3(s3_key)
    gold = compute_strain_index(df)
    load_to_postgres(gold, table="hrd_strain_scored")
    return {"rows_loaded": len(gold)}
