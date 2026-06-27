"""
============================================================================
pipeline_dag.py — Telecom Billing DWH Airflow DAG
============================================================================
DAG name:  telecom_billing_dwh_pipeline
Schedule:  every 30 minutes (simulates Huawei CBS SFTP polling at Telecom)
SLA:       15 minutes end-to-end

Task chain:
  t1_sense_landing    → FileSensor: waits for new CSV in data/landing/stream/
  t2_ingest_bronze    → BashOperator: runs ingestion/ingest_bronze.py
  t3_quality_checks   → BashOperator: runs quality/quality_checks.py
  t4_transform_silver → BashOperator: runs spark/transform_silver.py
  t5_build_gold       → BashOperator: runs spark/build_gold.py
  t6_build_mart       → BashOperator: runs mart/financial_mart.py
  t7_notify           → PythonOperator: logs pipeline completion summary

Maps to (the telecom operator production):
  In production, this DAG runs on the operator's Airflow cluster and orchestrates
  the exact same pipeline against HDFS/YARN:
    - FileSensor watches SFTP drop zone for Huawei CBS files
    - BashOperators submit spark-submit jobs to YARN
    - The notify task pushes metrics to the Revenue Assurance Grafana board
    - SLA misses page the on-call data engineer via PagerDuty
============================================================================
"""

from datetime import datetime, timedelta
import logging

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor


# ============================================================================
# PROJECT ROOT
# ============================================================================
# In production: HDFS base path (e.g. hdfs://namenode:8020/data/telecom/billing)
# Locally:       absolute path to the project working directory.
# Change this to match your local clone path before running.
PROJECT_ROOT = "/opt/airflow/dags/../../../"   # adjust to your project root

# Equivalent of: data/landing/stream/ under PROJECT_ROOT
STREAM_DIR = f"{PROJECT_ROOT}/data/landing/stream"


# ============================================================================
# DEFAULT ARGUMENTS
# ============================================================================
# Maps to: the operator's Airflow default_args in the centralized DAG factory.
# email_on_failure=True is configured here; actual SMTP details go in
# Airflow's connections (Admin > Connections > smtp_default).

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email": ["de-team@telecom.com"],       # Revenue Assurance on-call alias
    "email_on_failure": True,               # page on any task failure
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,      # 2m, 4m on successive retries
    "max_retry_delay": timedelta(minutes=10),
    "execution_timeout": timedelta(minutes=14),  # hard kill before 15-min SLA
}


# ============================================================================
# SLA MISS CALLBACK
# ============================================================================

def sla_miss_callback(dag, task_list, blocking_task_list, slas, blocking_tis):
    """
    Called by Airflow when the pipeline SLA of 15 minutes is breached.
    Maps to: the operator's PagerDuty webhook triggered by SLA breach events.
    In production, replace the log statement with a webhook call or
    Slack alert to the Revenue Assurance team channel.
    """
    logging.warning(
        "SLA BREACH: DAG '%s' exceeded 15-minute SLA. "
        "Blocking tasks: %s. "
        "Action: investigate Spark job or FileSensor timeout.",
        dag.dag_id,
        [t.task_id for t in blocking_tis],
    )


# ============================================================================
# NOTIFY TASK
# ============================================================================

def log_pipeline_summary(**context):
    """
    PythonOperator payload — logs a completion summary to Airflow task logs.

    Reads dag_run.conf for optional run metadata (custom date range, triggered
    by whom). In production, this function would also:
      - Push ARPU / reconciliation metrics to Grafana via pushgateway
      - Write a run record to the pipeline audit table in the DWH
      - Post a success message to the #data-alerts Slack channel

    dag_run.conf example (for manual trigger with custom range):
      {"start_date": "2025-04-01", "end_date": "2025-06-30", "triggered_by": "analyst-1"}
    """
    run_conf = context.get("dag_run").conf or {}
    run_id   = context.get("run_id", "n/a")
    exec_dt  = context.get("execution_date")

    start_date   = run_conf.get("start_date", "default (last 30min)")
    end_date     = run_conf.get("end_date",   "default (last 30min)")
    triggered_by = run_conf.get("triggered_by", "schedule")

    logging.info("=" * 60)
    logging.info("Telecom Billing DWH Pipeline — COMPLETED")
    logging.info("=" * 60)
    logging.info("  Run ID:        %s", run_id)
    logging.info("  Execution dt:  %s", exec_dt)
    logging.info("  Triggered by:  %s", triggered_by)
    logging.info("  Date range:    %s → %s", start_date, end_date)
    logging.info("-" * 60)
    logging.info("  Layers written:")
    logging.info("    [OK] Bronze   — raw Parquet ingested from Huawei CBS SFTP")
    logging.info("    [OK] Quality  — DQ gate passed; quarantine written")
    logging.info("    [OK] Silver   — standardized, deduplicated, enriched")
    logging.info("    [OK] Gold     — star schema (fact_billing + 5 dims)")
    logging.info("    [OK] Mart     — 5 KPI tables (ARPU, MOU, Revenue, Recon, VIP)")
    logging.info("=" * 60)
    logging.info("  Next steps: check data/mart/kpi_summary.csv for KPI values.")
    logging.info("  SLA budget: 15 minutes. Check Airflow SLA view for breaches.")
    logging.info("=" * 60)


# ============================================================================
# DAG DEFINITION
# ============================================================================

with DAG(
    dag_id="telecom_billing_dwh_pipeline",
    description="End-to-end Telecom Billing DWH pipeline for the telecom operator "
                "(Bronze → Silver → Gold → Mart)",
    default_args=default_args,
    schedule_interval="*/30 * * * *",          # every 30 minutes
    start_date=datetime(2025, 4, 1),
    catchup=False,                             # don't backfill missed runs
    max_active_runs=1,                         # prevent overlapping runs
    tags=["telecom", "billing", "dwh"],
    sla_miss_callback=sla_miss_callback,
    doc_md="""
## Telecom Billing DWH Pipeline

End-to-end Airflow DAG orchestrating the Telecom Billing DWH pipeline.

### Architecture
Huawei CBS → SFTP → Landing Zone → **Bronze** → DQ Gate → **Silver** → **Gold** → **Mart**

### Schedule
Every **30 minutes** to match the Huawei CBS micro-batch SFTP push cycle.

### SLA
**15 minutes** — breach triggers SLA callback (PagerDuty in production).

### Manual Trigger
```json
{"start_date": "2025-04-01", "end_date": "2025-06-30", "triggered_by": "your-name"}
```

### Tags
`telecom` · `billing` · `dwh`
    """,
) as dag:

    # ------------------------------------------------------------------ #
    # T1 — FileSensor: wait for new CSV from Huawei CBS SFTP drop         #
    # ------------------------------------------------------------------ #
    # Maps to: the operator's SFTP landing zone on HDFS monitored by Airflow.
    # The sensor polls every 30 seconds and times out after 20 minutes,
    # preventing the DAG from blocking when no new data arrives.
    t1_sense_landing = FileSensor(
        task_id="t1_sense_landing",
        filepath=STREAM_DIR,
        fs_conn_id="fs_default",             # Airflow file system connection
        poke_interval=30,                    # check every 30 seconds
        timeout=60 * 20,                     # timeout after 20 minutes
        mode="reschedule",                   # release worker slot while waiting
        soft_fail=True,                      # skip gracefully if no file arrives
        sla=timedelta(minutes=3),            # SLA for sensor portion
        doc_md="Watches `data/landing/stream/` for new CSV files from Huawei CBS.",
    )

    # ------------------------------------------------------------------ #
    # T2 — Ingest Bronze: CSV → Parquet with metadata columns             #
    # ------------------------------------------------------------------ #
    # Maps to: Spark job reading CBS SFTP files into HDFS bronze zone.
    # --source stream targets only the micro-batch streaming files.
    t2_ingest_bronze = BashOperator(
        task_id="t2_ingest_bronze",
        bash_command=(
            f"cd {PROJECT_ROOT} && "
            "python 'src/phase_2_ingestion/ingest_bronze.py' --source stream"
        ),
        sla=timedelta(minutes=2),
        doc_md="Reads CSVs from `data/landing/stream/` and writes Snappy Parquet "
               "to `data/bronze/`. Adds `_ingested_at`, `_source_file`, `_batch_id`.",
    )

    # ------------------------------------------------------------------ #
    # T3 — Quality Checks: 5-category DQ gate                            #
    # ------------------------------------------------------------------ #
    # Maps to: Great Expectations / custom DQ job between bronze and silver.
    # Quarantined rows go to data/quarantine/; quality_report.csv to logs/.
    t3_quality_checks = BashOperator(
        task_id="t3_quality_checks",
        bash_command=(
            f"cd {PROJECT_ROOT} && "
            "python 'src/phase_3_quality/quality_checks.py'"
        ),
        sla=timedelta(minutes=3),
        doc_md="Runs completeness, validity, uniqueness, consistency, and "
               "timeliness checks. Quarantines bad rows; writes `logs/quality_report.csv`.",
    )

    # ------------------------------------------------------------------ #
    # T4 — Silver Transform: PySpark standardization + enrichment         #
    # ------------------------------------------------------------------ #
    # Maps to: spark-submit job on YARN (--master yarn --deploy-mode cluster).
    # Performs: quarantine exclusion, type casting, dedup, customer enrichment,
    # per-customer metrics. Partitioned by billing_month.
    t4_transform_silver = BashOperator(
        task_id="t4_transform_silver",
        bash_command=(
            f"cd {PROJECT_ROOT} && "
            "python 'src/phase_4_silver/transform_silver.py'"
        ),
        sla=timedelta(minutes=5),
        doc_md="PySpark job: dedup, type-cast, enrich billing with customer dim. "
               "Writes `data/silver/` partitioned by `billing_month`.",
    )

    # ------------------------------------------------------------------ #
    # T5 — Build Gold: Star schema (fact_billing + 5 dims)                #
    # ------------------------------------------------------------------ #
    # Maps to: spark-submit on YARN building the Gold Hive tables.
    # Includes CDR reconciliation: each billing event checked for a
    # matching Huawei MSC CDR on (customer, service, date).
    t5_build_gold = BashOperator(
        task_id="t5_build_gold",
        bash_command=(
            f"cd {PROJECT_ROOT} && "
            "python 'src/phase_5_gold/build_gold.py'"
        ),
        sla=timedelta(minutes=8),
        doc_md="PySpark job: builds star schema. "
               "Dimensions: dim_customer, dim_service, dim_date, dim_region, dim_plan. "
               "Fact: fact_billing with CDR reconciliation flag.",
    )

    # ------------------------------------------------------------------ #
    # T6 — Build Mart: 5 KPI tables via DuckDB SQL                       #
    # ------------------------------------------------------------------ #
    # Maps to: Presto/Trino queries on Hive Gold tables.
    # DuckDB reads Gold Parquet directly — no Spark overhead for SQL.
    t6_build_mart = BashOperator(
        task_id="t6_build_mart",
        bash_command=(
            f"cd {PROJECT_ROOT} && "
            "python 'src/phase_6_mart/financial_mart.py'"
        ),
        sla=timedelta(minutes=12),
        doc_md="DuckDB job: computes ARPU, MOU, Revenue by Plan, "
               "Reconciliation Rate, and Top-10 customers. "
               "Writes `data/mart/*.parquet` and `data/mart/kpi_summary.csv`.",
    )

    # ------------------------------------------------------------------ #
    # T7 — Notify: log completion summary                                 #
    # ------------------------------------------------------------------ #
    # Maps to: Grafana push + Slack notification in production.
    # Reads dag_run.conf for custom date range when triggered manually.
    t7_notify = PythonOperator(
        task_id="t7_notify",
        python_callable=log_pipeline_summary,
        provide_context=True,
        sla=timedelta(minutes=13),           # must finish before 15-min SLA
        doc_md="Logs pipeline completion summary. In production: pushes KPI "
               "metrics to Grafana pushgateway and posts to #data-alerts Slack.",
    )

    # ------------------------------------------------------------------ #
    # TASK DEPENDENCIES                                                    #
    # ------------------------------------------------------------------ #
    # Linear chain — each step must succeed before the next starts.
    # t3 is the DQ gate: if quality fails (score < threshold), the DAG
    # halts and the quarantine files are available for investigation.
    (
        t1_sense_landing
        >> t2_ingest_bronze
        >> t3_quality_checks
        >> t4_transform_silver
        >> t5_build_gold
        >> t6_build_mart
        >> t7_notify
    )
