from __future__ import annotations

import io
import json
import logging
from datetime import datetime, timedelta, timezone

import pandas as pd
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook

# -----------------------------
# Connections / Config
# -----------------------------
S3_CONN_ID = "minio_s3"
PG_CONN_ID = "p2_postgres"
BUCKET = "lake"

CDC_URL = "https://healthdata.gov/resource/g62h-syeh.json?$limit=5000"
log = logging.getLogger("airflow.task")


# -----------------------------
# Helpers
# -----------------------------
def _safe_to_num(s) -> pd.Series:
    if s is None:
        return pd.Series(dtype="float64")
    if not isinstance(s, pd.Series):
        s = pd.Series(s)
    return pd.to_numeric(s, errors="coerce")


def fetch_real_hospital_ops(**context):
    r = requests.get(CDC_URL, timeout=60)
    r.raise_for_status()
    rows = r.json()

    # Keep fields we actually see in your logs (plus safe fallbacks)
    keep = [
        "collection_week",
        "state",
        "hospital_name",
        "hospital_pk",
        "inpatient_bed_utilization",
        "adult_icu_bed_utilization",
        "total_beds_7_day_avg",
        "inpatient_beds_7_day_avg",
        "inpatient_bed_utilization_numerator",
        "inpatient_bed_utilization_denominator",
        "adult_icu_bed_utilization_numerator",
        "adult_icu_bed_utilization_denominator",
    ]

    cleaned = [{k: row.get(k) for k in keep} for row in rows]
    context["ti"].xcom_push(key="rows", value=cleaned)


def land_to_minio(**context):
    rows = context["ti"].xcom_pull(task_ids="fetch_real_ops", key="rows")
    if not rows:
        raise ValueError("No rows fetched.")

    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    key = f"bronze/cdc_hospital_ops/dt={ts[:8]}/cdc_ops_{ts}.jsonl"

    buf = io.StringIO()
    for r in rows:
        buf.write(json.dumps(r) + "\n")

    s3 = S3Hook(aws_conn_id=S3_CONN_ID)
    s3.load_bytes(
        bytes_data=buf.getvalue().encode("utf-8"),
        key=key,
        bucket_name=BUCKET,
        replace=True,
    )

    context["ti"].xcom_push(key="latest_bronze_key", value=key)


def score_and_alert(**context):
    s3 = S3Hook(aws_conn_id=S3_CONN_ID)

    # Prefer the bronze key from this same DAG run
    latest_key = context["ti"].xcom_pull(task_ids="land_to_minio", key="latest_bronze_key")
    if not latest_key:
        keys = s3.list_keys(bucket_name=BUCKET, prefix="bronze/cdc_hospital_ops/") or []
        if not keys:
            raise ValueError("No bronze files found in lake/bronze/cdc_hospital_ops/")
        latest_key = sorted(keys)[-1]

    obj = s3.get_key(key=latest_key, bucket_name=BUCKET)
    lines = obj.get()["Body"].read().decode("utf-8").splitlines()
    rows = [json.loads(x) for x in lines if x.strip()]
    df = pd.DataFrame(rows)

    # ---- Log column presence and NON-NULL counts (debug) ----
    log.info("Columns in dataset: %s", list(df.columns))
    check_cols = [
        "inpatient_bed_utilization",
        "adult_icu_bed_utilization",
        "inpatient_bed_utilization_numerator",
        "inpatient_bed_utilization_denominator",
        "adult_icu_bed_utilization_numerator",
        "adult_icu_bed_utilization_denominator",
        "total_beds_7_day_avg",
        "inpatient_beds_7_day_avg",
    ]
    for c in check_cols:
        if c in df.columns:
            log.info("NON-NULL count for %s = %s / %s", c, df[c].notna().sum(), len(df))
        else:
            log.info("Column missing: %s", c)

    # ---- Convert ICU fields to numeric ----
    if "adult_icu_bed_utilization" in df.columns:
        df["adult_icu_bed_utilization"] = _safe_to_num(df["adult_icu_bed_utilization"])
    for col in ["adult_icu_bed_utilization_numerator", "adult_icu_bed_utilization_denominator"]:
        if col in df.columns:
            df[col] = _safe_to_num(df[col])

    # -----------------------------
    # OPTION A (recommended for your dataset):
    # Score based ONLY on ICU utilization since inpatient metrics are 100% null.
    # -----------------------------
    icu_rate = df["adult_icu_bed_utilization"] if "adult_icu_bed_utilization" in df.columns else pd.Series([pd.NA] * len(df))

    num = df["adult_icu_bed_utilization_numerator"] if "adult_icu_bed_utilization_numerator" in df.columns else pd.Series([pd.NA] * len(df))
    den = df["adult_icu_bed_utilization_denominator"] if "adult_icu_bed_utilization_denominator" in df.columns else pd.Series([pd.NA] * len(df))

    icu_calc = num / den
    icu_calc = pd.to_numeric(icu_calc, errors="coerce").replace([float("inf"), float("-inf")], pd.NA)
    icu_calc = icu_calc.where((icu_calc.isna()) | ((icu_calc >= 0) & (icu_calc <= 1)), pd.NA)

    df["icu_util_final"] = icu_rate.fillna(icu_calc)
    df["icu_util_final"] = pd.to_numeric(df["icu_util_final"], errors="coerce")
    df["icu_util_final"] = df["icu_util_final"].where((df["icu_util_final"].isna()) | ((df["icu_util_final"] >= 0) & (df["icu_util_final"] <= 1)), pd.NA)

    # Strain score (0-100) = ICU utilization * 100
    df["strain_score"] = (df["icu_util_final"] * 100).round(1)

    def level(x):
        if pd.isna(x):
            return "UNKNOWN"
        if x >= 90:
            return "CRITICAL"
        if x >= 80:
            return "HIGH"
        if x >= 65:
            return "WATCH"
        return "NORMAL"

    df["alert_level"] = df["strain_score"].apply(level)

    run_ts = datetime.now(timezone.utc).isoformat()
    df["run_ts_utc"] = run_ts
    df["source_key"] = latest_key

    # -----------------------------
    # Load to Postgres (gold)
    # -----------------------------
    pg = PostgresHook(postgres_conn_id=PG_CONN_ID)
    engine = pg.get_sqlalchemy_engine()

    with engine.begin() as conn:
        conn.exec_driver_sql("CREATE SCHEMA IF NOT EXISTS gold;")
        conn.exec_driver_sql(
            """
            CREATE TABLE IF NOT EXISTS gold.hospital_strain_alerts (
              run_ts_utc TEXT,
              collection_week TEXT,
              state TEXT,
              hospital_name TEXT,
              hospital_pk TEXT,
              adult_icu_bed_utilization DOUBLE PRECISION,
              adult_icu_bed_utilization_numerator DOUBLE PRECISION,
              adult_icu_bed_utilization_denominator DOUBLE PRECISION,
              icu_util_final DOUBLE PRECISION,
              strain_score DOUBLE PRECISION,
              alert_level TEXT,
              source_key TEXT
            );
            """
        )

    out = df[
        [
            "run_ts_utc",
            "collection_week",
            "state",
            "hospital_name",
            "hospital_pk",
            "adult_icu_bed_utilization",
            "adult_icu_bed_utilization_numerator",
            "adult_icu_bed_utilization_denominator",
            "icu_util_final",
            "strain_score",
            "alert_level",
            "source_key",
        ]
    ].copy()

    out.to_sql("hospital_strain_alerts", engine, schema="gold", if_exists="append", index=False)

    context["ti"].xcom_push(key="latest_source_key", value=latest_key)
    context["ti"].xcom_push(key="run_ts_utc", value=run_ts)


def data_quality_check(**context):
    latest_key = context["ti"].xcom_pull(task_ids="score_and_alert", key="latest_source_key")
    if not latest_key:
        raise ValueError("DQ check couldn't find latest_source_key from score_and_alert")

    pg = PostgresHook(postgres_conn_id=PG_CONN_ID)

    total = pg.get_first(
        "SELECT COUNT(*) FROM gold.hospital_strain_alerts WHERE source_key = %s;",
        parameters=(latest_key,),
    )[0]

    unknown = pg.get_first(
        "SELECT COUNT(*) FROM gold.hospital_strain_alerts WHERE source_key = %s AND alert_level = 'UNKNOWN';",
        parameters=(latest_key,),
    )[0]

    null_scores = pg.get_first(
        "SELECT COUNT(*) FROM gold.hospital_strain_alerts WHERE source_key = %s AND strain_score IS NULL;",
        parameters=(latest_key,),
    )[0]

    if total == 0:
        raise ValueError(f"DQ FAIL: 0 rows inserted for source_key={latest_key}")

    unknown_rate = unknown / total
    null_rate = null_scores / total

    log.info("===== DATA QUALITY CHECK =====")
    log.info("source_key: %s", latest_key)
    log.info("rows_total: %s", total)
    log.info("unknown_rows: %s (rate=%.2f)", unknown, unknown_rate)
    log.info("null_strain_score_rows: %s (rate=%.2f)", null_scores, null_rate)

    # Now that we score from ICU, these should NOT be 1.00 anymore.
    if unknown_rate > 0.40:
        raise ValueError(f"DQ FAIL: UNKNOWN rate too high: {unknown_rate:.2f} (> 0.40)")
    if null_rate > 0.20:
        raise ValueError(f"DQ FAIL: strain_score NULL rate too high: {null_rate:.2f} (> 0.20)")

    log.info("DQ PASS ✅")
    log.info("===== END DATA QUALITY CHECK =====")


with DAG(
    dag_id="hospital_ops_stream_plus_batch",
    start_date=datetime(2026, 1, 1),
    schedule="*/5 * * * *",
    catchup=False,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=1)},
    tags=["project2", "healthcare", "minio", "postgres"],
) as dag:
    t1 = PythonOperator(task_id="fetch_real_ops", python_callable=fetch_real_hospital_ops)
    t2 = PythonOperator(task_id="land_to_minio", python_callable=land_to_minio)
    t3 = PythonOperator(task_id="score_and_alert", python_callable=score_and_alert)
    t4 = PythonOperator(task_id="data_quality_check", python_callable=data_quality_check)

    t1 >> t2 >> t3 >> t4