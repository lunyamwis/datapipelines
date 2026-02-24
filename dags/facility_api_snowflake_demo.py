# dags/facility_api_to_snowflake_demo.py
from __future__ import annotations

from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Param
from airflow.operators.empty import EmptyOperator

DAG_ID = "facility_api_to_snowflake_DEMO"

# Facilities available in demo dropdown
FACILITIES = ["kisumu", "aar"]

with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=1),
    },
    params={
        "facility": Param("kisumu", enum=FACILITIES)
    },
    tags=["demo", "facility", "data-pipeline"],
) as dag:

    # -----------------------------
    # EXTRACT LAYER (API → S3)
    # -----------------------------

    start = EmptyOperator(task_id="start_pipeline")

    extract_api = EmptyOperator(
        task_id="extract_api_events"
    )

    write_to_s3 = EmptyOperator(
        task_id="write_raw_to_s3"
    )

    # -----------------------------
    # LOAD LAYER (S3 → Snowflake RAW)
    # -----------------------------

    stage_to_snowflake = EmptyOperator(
        task_id="copy_into_snowflake_raw"
    )

    # -----------------------------
    # TRANSFORM LAYER (RAW → CLEAN)
    # -----------------------------

    merge_clean_table = EmptyOperator(
        task_id="merge_into_clean_table"
    )

    # -----------------------------
    # END
    # -----------------------------

    end = EmptyOperator(task_id="pipeline_complete")

    # Pipeline Flow
    start >> extract_api >> write_to_s3 >> stage_to_snowflake >> merge_clean_table >> end