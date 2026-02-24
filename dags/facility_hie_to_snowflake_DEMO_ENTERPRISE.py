# dags/facility_hie_to_snowflake_demo_enterprise_anonymized.py
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Param
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.utils.trigger_rule import TriggerRule


DAG_ID = "facility_hie_to_snowflake_DEMO_ENTERPRISE_ANON"

FACILITIES = ["kisumu", "aar", "embu_demo"]
DATA_DOMAINS = ["encounters", "labs", "pharmacy", "billing", "patients"]
HIE_STANDARDS = ["HL7v2", "FHIR_R4", "ICD10", "LOINC", "SNOMED_CT", "CPT"]

DEFAULT_ARGS = {
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "owner": "lunyamwi-data-platform",
}


# -----------------------------
# DEMO ROUTING (NO REAL I/O)
# -----------------------------
def choose_ingestion_mode(**context):
    run_mode = context["params"]["run_mode"]
    backfill = context["params"]["backfill"]
    return "mode__backfill" if (run_mode == "backfill" or backfill) else "mode__incremental"


def choose_payload_format(**context):
    fmt = context["params"]["payload_format"]
    return "fmt__hl7v2" if fmt == "HL7v2" else "fmt__fhir_r4"


def choose_phi_handling(**context):
    """
    - include_phi=False -> anonymized analytics lane (tokenize/mask)
    - include_phi=True  -> encrypted PHI vault lane
    """
    include_phi = context["params"]["include_phi"]
    return "phi__encrypted_vault" if include_phi else "phi__tokenize_mask"


def choose_qa_mode(**context):
    return "qa__strict" if context["params"]["strict_qa"] else "qa__standard"


def choose_release_path(**context):
    """
    - publish_analytics=True -> publish anonymized marts
    - else -> stop after clean layer (demo)
    """
    return "release__publish_analytics" if context["params"]["publish_analytics"] else "release__hold"


with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    default_args=DEFAULT_ARGS,
    params={
        "facility": Param("kisumu", enum=FACILITIES),
        "run_mode": Param("incremental", enum=["incremental", "backfill"]),
        "backfill": Param(False, type="boolean"),
        "payload_format": Param("FHIR_R4", enum=["FHIR_R4", "HL7v2"]),
        "include_phi": Param(False, type="boolean"),
        "strict_qa": Param(True, type="boolean"),
        "publish_analytics": Param(True, type="boolean"),
        "domains": Param(DATA_DOMAINS, type="array"),
        "standards": Param(HIE_STANDARDS, type="array"),
        # anonymization posture (demo knobs)
        "min_cell_size": Param(10, type="integer"),
        "age_bands": Param(True, type="boolean"),
        "geo_generalize": Param(True, type="boolean"),
    },
    tags=["demo", "hie", "privacy", "anonymization", "hl7", "fhir", "snowflake"],
    description="Enterprise demo HIE ingestion with standards gates, PHI controls, anonymization lane, and governance release controls.",
) as dag:
    # =============================
    # CONTROL / GOVERNANCE
    # =============================
    start = EmptyOperator(task_id="ctl__start")
    change_window_open = EmptyOperator(task_id="ctl__change_window_open")
    acquire_lock = EmptyOperator(task_id="ctl__acquire_facility_lock")

    gov_manifest = EmptyOperator(task_id="gov__create_run_manifest")
    gov_lineage = EmptyOperator(task_id="gov__register_openlineage")
    gov_audit_start = EmptyOperator(task_id="gov__emit_audit_start")

    # Branches
    branch_mode = BranchPythonOperator(task_id="branch__ingestion_mode", python_callable=choose_ingestion_mode)
    mode_incremental = EmptyOperator(task_id="mode__incremental")
    mode_backfill = EmptyOperator(task_id="mode__backfill")
    join_mode = EmptyOperator(task_id="join__mode", trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)

    branch_format = BranchPythonOperator(task_id="branch__payload_format", python_callable=choose_payload_format)
    fmt_hl7 = EmptyOperator(task_id="fmt__hl7v2")
    fmt_fhir = EmptyOperator(task_id="fmt__fhir_r4")
    join_format = EmptyOperator(task_id="join__format", trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)

    branch_phi = BranchPythonOperator(task_id="branch__phi_handling", python_callable=choose_phi_handling)
    phi_tokenize = EmptyOperator(task_id="phi__tokenize_mask")
    phi_vault = EmptyOperator(task_id="phi__encrypted_vault")
    join_phi = EmptyOperator(task_id="join__phi", trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)

    branch_qa = BranchPythonOperator(task_id="branch__qa_mode", python_callable=choose_qa_mode)
    qa_standard = EmptyOperator(task_id="qa__standard")
    qa_strict = EmptyOperator(task_id="qa__strict")
    join_qa_mode = EmptyOperator(task_id="join__qa_mode", trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)

    # =============================
    # SOURCE / INGESTION (DEMO)
    # =============================
    src_connectivity = EmptyOperator(task_id="src__connectivity_check")
    src_contract = EmptyOperator(task_id="src__api_contract_check")
    src_rate_limit = EmptyOperator(task_id="src__rate_limit_guard")

    fanout_domains = EmptyOperator(task_id="ingest__fanout_domains")
    ingest_patients = EmptyOperator(task_id="ingest__patients")
    ingest_encounters = EmptyOperator(task_id="ingest__encounters")
    ingest_labs = EmptyOperator(task_id="ingest__labs")
    ingest_pharmacy = EmptyOperator(task_id="ingest__pharmacy")
    ingest_billing = EmptyOperator(task_id="ingest__billing")

    raw_land = EmptyOperator(task_id="raw__land_to_object_storage")
    raw_hash = EmptyOperator(task_id="raw__compute_content_hash")
    raw_dedupe = EmptyOperator(task_id="raw__dedupe")

    # =============================
    # STANDARDS / CONFORMANCE (DEMO)
    # =============================
    parse_canonical = EmptyOperator(task_id="std__parse_and_canonicalize")

    validate_icd10 = EmptyOperator(task_id="std__validate_icd10")
    validate_loinc = EmptyOperator(task_id="std__validate_loinc")
    validate_snomed = EmptyOperator(task_id="std__validate_snomed")
    validate_cpt = EmptyOperator(task_id="std__validate_cpt")

    validate_fhir_profiles = EmptyOperator(task_id="std__fhir_profile_conformance_gate")
    validate_hl7_segments = EmptyOperator(task_id="std__hl7v2_segment_structure_gate")

    # Security attestations (demo)
    tls_attestation = EmptyOperator(task_id="sec__tls_in_transit_attestation")
    encryption_at_rest = EmptyOperator(task_id="sec__encrypt_at_rest_attestation")
    rbac_row_policies = EmptyOperator(task_id="sec__rbac_row_level_policy_attestation")

    # =============================
    # ✅ PRIVACY / ANONYMIZATION LANE (DEMO)
    # =============================
    phi_field_inventory = EmptyOperator(task_id="privacy__phi_field_inventory_and_classification")
    direct_identifier_strip = EmptyOperator(task_id="privacy__remove_direct_identifiers")

    # Tokenization / hashing for join keys (consistent but non-reversible in analytics)
    tokenize_patient_id = EmptyOperator(task_id="privacy__tokenize_patient_identifiers")
    hash_device_ids = EmptyOperator(task_id="privacy__hash_device_and_account_ids")

    # Generalization / aggregation to reduce re-identification risk
    generalize_age = EmptyOperator(task_id="privacy__generalize_dob_to_age_bands")
    generalize_geo = EmptyOperator(task_id="privacy__generalize_geo_to_ward_subcounty")
    redact_free_text = EmptyOperator(task_id="privacy__redact_free_text_notes")

    # Privacy QA gates
    min_cell_suppression = EmptyOperator(task_id="privacy__min_cell_size_suppression_gate")
    reid_risk_check = EmptyOperator(task_id="privacy__reidentification_risk_check")
    privacy_release_approval = EmptyOperator(task_id="privacy__dataset_release_approval_gate")

    join_privacy = EmptyOperator(task_id="join__privacy", trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)

    # =============================
    # QA / DATA QUALITY (DEMO)
    # =============================
    qa_schema_drift = EmptyOperator(task_id="qa__schema_drift_check")
    qa_nulls = EmptyOperator(task_id="qa__null_thresholds_check")
    qa_volume = EmptyOperator(task_id="qa__volume_anomaly_detection")
    qa_sla = EmptyOperator(task_id="qa__timeliness_sla_gate")

    qa_strict_refint = EmptyOperator(task_id="qa_strict__referential_integrity")
    qa_strict_recon = EmptyOperator(task_id="qa_strict__financial_reconciliation_controls")
    qa_strict_rules = EmptyOperator(task_id="qa_strict__clinical_rule_engine_validation")

    join_quality = EmptyOperator(task_id="join__quality", trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)

    # =============================
    # WAREHOUSE LAYERS (DEMO)
    # =============================
    sf_stage = EmptyOperator(task_id="sf__stage_files")
    sf_copy_raw = EmptyOperator(task_id="sf__copy_into_bronze_raw")
    sf_merge_clean = EmptyOperator(task_id="sf__merge_into_silver_clean")

    # ✅ Publish anonymized marts (Gold)
    branch_release = BranchPythonOperator(task_id="branch__release_path", python_callable=choose_release_path)
    release_publish = EmptyOperator(task_id="release__publish_analytics")
    release_hold = EmptyOperator(task_id="release__hold")

    build_gold_anon = EmptyOperator(task_id="sf__build_gold_anonymized_marts")
    publish_semantic_layer = EmptyOperator(task_id="sf__publish_semantic_layer_views")

    # =============================
    # OBS / CLOSEOUT (DEMO)
    # =============================
    obs_metrics = EmptyOperator(task_id="obs__publish_metrics")
    obs_lineage = EmptyOperator(task_id="obs__publish_lineage")
    ops_notify = EmptyOperator(task_id="ops__notify_oncall_demo")

    gov_audit_end = EmptyOperator(task_id="gov__emit_audit_end", trigger_rule=TriggerRule.ALL_DONE)
    release_lock = EmptyOperator(task_id="ctl__release_facility_lock", trigger_rule=TriggerRule.ALL_DONE)
    change_window_close = EmptyOperator(task_id="ctl__change_window_close", trigger_rule=TriggerRule.ALL_DONE)
    end = EmptyOperator(task_id="ctl__end", trigger_rule=TriggerRule.ALL_DONE)

    # =============================
    # FLOW
    # =============================
    start >> change_window_open >> acquire_lock >> gov_manifest >> gov_lineage >> gov_audit_start

    gov_audit_start >> branch_mode
    branch_mode >> mode_incremental >> join_mode
    branch_mode >> mode_backfill >> join_mode

    join_mode >> branch_format
    branch_format >> fmt_fhir >> join_format
    branch_format >> fmt_hl7 >> join_format

    join_format >> branch_phi
    branch_phi >> phi_tokenize >> join_phi
    branch_phi >> phi_vault >> join_phi

    join_phi >> branch_qa
    branch_qa >> qa_standard >> join_qa_mode
    branch_qa >> qa_strict >> join_qa_mode

    join_qa_mode >> src_connectivity >> src_contract >> src_rate_limit

    src_rate_limit >> fanout_domains
    fanout_domains >> [ingest_patients, ingest_encounters, ingest_labs, ingest_pharmacy, ingest_billing]

    [ingest_patients, ingest_encounters, ingest_labs, ingest_pharmacy, ingest_billing] >> raw_land >> raw_hash >> raw_dedupe

    raw_dedupe >> parse_canonical
    parse_canonical >> [validate_icd10, validate_loinc, validate_snomed, validate_cpt]

    # Format gates (both shown, only one is active per run)
    fmt_fhir >> validate_fhir_profiles
    fmt_hl7 >> validate_hl7_segments

    [validate_fhir_profiles, validate_hl7_segments] >> tls_attestation >> encryption_at_rest >> rbac_row_policies

    # ✅ Privacy lane only “matters” for analytics path; vault path is shown too
    rbac_row_policies >> phi_field_inventory

    # Tokenize/mask branch (analytics-friendly)
    phi_tokenize >> phi_field_inventory >> direct_identifier_strip >> tokenize_patient_id >> hash_device_ids
    hash_device_ids >> generalize_age >> generalize_geo >> redact_free_text
    redact_free_text >> min_cell_suppression >> reid_risk_check >> privacy_release_approval >> join_privacy

    # Encrypted vault branch (still goes through inventory but not de-identification)
    phi_vault >> phi_field_inventory >> join_privacy

    # Quality gates
    [validate_icd10, validate_loinc, validate_snomed, validate_cpt, join_privacy] >> qa_schema_drift
    qa_schema_drift >> qa_nulls >> qa_volume >> qa_sla

    qa_standard >> join_quality
    qa_sla >> join_quality
    qa_strict >> qa_strict_refint >> qa_strict_recon >> qa_strict_rules >> join_quality

    # Warehouse layers
    join_quality >> sf_stage >> sf_copy_raw >> sf_merge_clean

    # Release controls
    sf_merge_clean >> branch_release
    branch_release >> release_publish
    branch_release >> release_hold

    release_publish >> build_gold_anon >> publish_semantic_layer
    release_hold >> obs_metrics  # stop early but still publish run metrics

    publish_semantic_layer >> obs_metrics >> obs_lineage >> ops_notify

    # Closeout
    ops_notify >> gov_audit_end >> release_lock >> change_window_close >> end