# dags/facility_api_to_snowflake.py
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

import requests
from airflow import DAG
from airflow.models import Variable, Param
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

DAG_ID = "facility_api_to_snowflake"

# Map facility -> API base / auth / db key
FACILITIES = {
    "kisumu": {"base_url": "https://api.company.com", "db": "kisumu_db"},
    "aar": {"base_url": "https://api.company.com", "db": "aar_db"},
}

S3_CONN_ID = "aws_default"
SNOWFLAKE_CONN_ID = "snowflake_default"

S3_BUCKET = "your-raw-bucket"
S3_PREFIX = "raw/facilities"  # will write: raw/facilities/facility_id/dt=YYYY-MM-DD/...

RAW_TABLE = "RAW.FACILITY_EVENTS_RAW"  # (facility_id, ingested_at, payload)
STAGE = "@RAW.FACILITY_RAW_STAGE"      # external stage pointing to S3_PREFIX
FILE_FORMAT = "RAW.JSON_FF"            # JSON file format object


def _get_watermark_key(facility: str) -> str:
    return f"wm__{DAG_ID}__{facility}"


def extract_to_s3(**context):
    facility = context["params"]["facility"]
    cfg = FACILITIES[facility]

    wm_key = _get_watermark_key(facility)
    last_run = Variable.get(wm_key, default_var="1970-01-01T00:00:00Z")

    # --- API call (adjust to your API) ---
    url = f"{cfg['base_url']}/events"
    headers = {"Authorization": "Bearer YOUR_TOKEN"}  # or pull from Airflow Connection/Variable
    params = {"database": cfg["db"], "updated_since": last_run, "limit": 500}

    all_rows = []
    while True:
        r = requests.get(url, headers=headers, params=params, timeout=60)
        r.raise_for_status()
        payload = r.json()

        rows = payload.get("data", payload)  # adapt if shape differs
        all_rows.extend(rows)

        next_page = payload.get("next_page")  # adapt (could be cursor/offset)
        if not next_page:
            break
        params["page"] = next_page

    ingested_at = datetime.now(timezone.utc).isoformat()
    dt = ingested_at[:10]

    # Write one JSONL file per run (simple)
    key = f"{S3_PREFIX}/facility_id={facility}/dt={dt}/{context['run_id']}.jsonl"
    body = "\n".join(json.dumps(row, separators=(",", ":")) for row in all_rows) + ("\n" if all_rows else "")

    s3 = S3Hook(aws_conn_id=S3_CONN_ID)
    s3.load_string(body, key=key, bucket_name=S3_BUCKET, replace=True)

    # Move watermark forward ONLY if extract succeeded
    Variable.set(wm_key, ingested_at)

    # pass S3 path to downstream
    context["ti"].xcom_push(key="s3_key", value=key)
    context["ti"].xcom_push(key="ingested_at", value=ingested_at)


def copy_into_snowflake(**context):
    facility = context["params"]["facility"]
    s3_key = context["ti"].xcom_pull(key="s3_key")
    ingested_at = context["ti"].xcom_pull(key="ingested_at")

    # COPY only the new file
    copy_sql = f"""
    COPY INTO {RAW_TABLE} (facility_id, ingested_at, payload)
    FROM (
      SELECT
        '{facility}'::STRING AS facility_id,
        '{ingested_at}'::TIMESTAMP_TZ AS ingested_at,
        PARSE_JSON($1) AS payload
      FROM {STAGE}
    )
    FILES = ('{s3_key}')
    FILE_FORMAT = (FORMAT_NAME = {FILE_FORMAT})
    ON_ERROR = 'ABORT_STATEMENT';
    """

    SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID).run(copy_sql)


def merge_clean(**context):
    facility = context["params"]["facility"]

    # Example: MERGE into a clean table using payload:id as the natural key
    merge_sql = f"""
    MERGE INTO ANALYTICS.FACILITY_EVENTS t
    USING (
      SELECT
        facility_id,
        payload:id::STRING             AS event_id,
        payload:event_time::TIMESTAMP  AS event_time,
        payload:type::STRING           AS event_type,
        payload                         AS payload,
        ingested_at
      FROM {RAW_TABLE}
      WHERE facility_id = '{facility}'
      QUALIFY ROW_NUMBER() OVER (
        PARTITION BY facility_id, payload:id::STRING
        ORDER BY ingested_at DESC
      ) = 1
    ) s
    ON t.facility_id = s.facility_id AND t.event_id = s.event_id
    WHEN MATCHED THEN UPDATE SET
      event_time = s.event_time,
      event_type = s.event_type,
      payload    = s.payload,
      ingested_at= s.ingested_at
    WHEN NOT MATCHED THEN INSERT
      (facility_id, event_id, event_time, event_type, payload, ingested_at)
    VALUES
      (s.facility_id, s.event_id, s.event_time, s.event_type, s.payload, s.ingested_at);
    """

    SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID).run(merge_sql)


with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2025, 1, 1),
    schedule=None,  # trigger manually (choose facility)
    catchup=False,
    default_args={"retries": 3, "retry_delay": timedelta(minutes=2)},
    params={
        "facility": Param("kisumu", enum=list(FACILITIES.keys())),
    },
    tags=["facility", "api", "snowflake"],
) as dag:

    t1 = PythonOperator(task_id="extract_to_s3", python_callable=extract_to_s3)
    t2 = PythonOperator(task_id="copy_into_snowflake_raw", python_callable=copy_into_snowflake)
    t3 = PythonOperator(task_id="merge_clean_table", python_callable=merge_clean)

    t1 >> t2 >> t3