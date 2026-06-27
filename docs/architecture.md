# Architecture Document — Telecom Billing DWH (the telecom operator)

**Project:** Telecom Billing Data Warehouse  
**Author:** Data Engineering Team  
**Date:** 2026-06-26  
**Version:** 1.0

---

## 1. Project Overview

### Business Problem

the telecom operator processes hundreds of thousands of billing events daily through its **Huawei Convergent Billing System (CBS)** and generates Call Detail Records (CDRs) from its **Huawei Mobile Switching Centers (MSC)**. These two systems produce separate data streams that must be:

1. **Reconciled** — every billed event should have a supporting network CDR; mismatches indicate revenue leakage or rating errors (a critical Revenue Assurance risk).
2. **Aggregated** — Finance needs ARPU, MOU, and revenue trends by region, plan, and month for investor reporting and budget planning.
3. **Governed** — raw data arriving from CBS is dirty (10% anomaly rate); a quality gate must quarantine bad rows before they corrupt downstream analytics.

This project builds a **Medallion Data Warehouse** that solves all three problems with a fully automated, Airflow-orchestrated pipeline.

---

## 2. Full Architecture Diagram (ASCII)

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                          the telecom operator — SOURCE SYSTEMS                        │
│                                                                             │
│  ┌───────────────────────┐          ┌──────────────────────────┐            │
│  │  Huawei CBS           │          │  Huawei MSC / MGW        │            │
│  │  (Convergent Billing) │          │  (Mobile Switching)      │            │
│  │  - Rated events       │          │  - Call Detail Records   │            │
│  │  - Subscriber master  │          │  - Duration, rated amt   │            │
│  │  CSV over SFTP        │          │  CSV over SFTP           │            │
│  └───────────┬───────────┘          └────────────┬─────────────┘            │
└──────────────┼───────────────────────────────────┼──────────────────────────┘
               │ every 30 min                       │ every 30 min
               ▼                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                          LANDING ZONE                                       │
│  data/landing/batch/          data/landing/stream/                          │
│  (initial full loads)         (micro-batch SFTP drops)                      │
│                                         ▲                                   │
│                                         │  FileSensor (Airflow t1)          │
└─────────────────────────────────────────┼───────────────────────────────────┘
                                          │
                                          ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                    BRONZE LAYER   [t2_ingest_bronze]                        │
│  data/bronze/customers/         Append-only · All types as string           │
│  data/bronze/billing_events/    Adds: _ingested_at, _source_file, _batch_id │
│  data/bronze/cdrs/              Snappy Parquet · Partitioned by event_date  │
└─────────────────────────────────────────┬───────────────────────────────────┘
                                          │
                                          ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                    DQ GATE   [t3_quality_checks]                            │
│  5 rule categories:                                                         │
│    1. Completeness  — null/empty checks on required columns                 │
│    2. Validity      — types, ranges, formats (msisdn, amounts)              │
│    3. Uniqueness    — duplicate primary key detection                       │
│    4. Consistency   — cross-dataset referential integrity                   │
│    5. Timeliness    — stale (>180d) and future-dated records                │
│                                                                             │
│  Output: data/quarantine/{dataset}_quarantined.parquet                      │
│          logs/quality_report.csv                                            │
└──────────────┬────────────────────────────────────────┬────────────────────┘
               │ clean rows                              │ bad rows
               ▼                                         ▼
┌──────────────────────────────┐            ┌───────────────────────────────┐
│   SILVER LAYER               │            │   data/quarantine/            │
│   [t4_transform_silver]      │            │   customers_quarantined.parquet│
│                              │            │   billing_quarantined.parquet  │
│   PySpark (local[*])         │            │   cdrs_quarantined.parquet     │
│   - Anti-join quarantine     │            │   (Revenue Assurance review)  │
│   - Cast & standardize types │            └───────────────────────────────┘
│   - Deduplicate on PK        │
│   - Enrich billing + customer│
│   - Per-customer metrics     │
│                              │
│   data/silver/               │
│     billing_enriched/        │
│     cdrs_cleaned/            │
│     customer_metrics/        │
│     customers_cleaned/       │
└──────────────┬───────────────┘
               │
               ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                    GOLD LAYER   [t5_build_gold]                             │
│                    Star Schema (PySpark local[*])                           │
│                                                                             │
│   DIMENSIONS                          FACT                                  │
│  ┌──────────────┐                    ┌──────────────────────────────────┐   │
│  │ dim_customer │◄──────────────────►│ fact_billing                     │   │
│  │ dim_service  │◄──────────────────►│   fact_id (PK)                   │   │
│  │ dim_date     │◄──────────────────►│   customer_id (FK)               │   │
│  │ dim_region   │◄──────────────────►│   service_id (FK)                │   │
│  │ dim_plan     │◄──────────────────►│   date_id (FK)                   │   │
│  └──────────────┘                    │   region_id (FK)                 │   │
│                                      │   plan_id (FK)                   │   │
│   CDR Reconciliation:                │   billed_amount                  │   │
│   billing JOIN cdrs on               │   duration_sec / data_mb         │   │
│   (customer, service, date)          │   is_reconciled  ← RA key metric │   │
│   → is_reconciled flag               │   billing_month (partition)      │   │
│                                      └──────────────────────────────────┘   │
└──────────────┬──────────────────────────────────────────────────────────────┘
               │
               ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                    MART LAYER   [t6_build_mart]                             │
│                    KPI Aggregations (DuckDB SQL)                            │
│                                                                             │
│   kpi_arpu_by_month_region       → CFO dashboard, investor reporting        │
│   kpi_mou_by_service             → Network Planning capacity forecast       │
│   kpi_revenue_by_plan_governorate→ Marketing plan performance               │
│   kpi_reconciliation_rate        → Revenue Assurance audit                  │
│   kpi_top_customers_by_month     → Enterprise Sales VIP tracking            │
│   mart/kpi_summary.csv           → Daily KPI digest (morning email)         │
└──────────────┬──────────────────────────────────────────────────────────────┘
               │
               ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                    ORCHESTRATION   [Airflow DAG]                            │
│                    telecom_billing_dwh_pipeline                             │
│                                                                             │
│   Schedule: */30 * * * *   (every 30 minutes)                              │
│   SLA:      15 minutes end-to-end                                           │
│                                                                             │
│   t1_sense_landing → t2_ingest_bronze → t3_quality_checks →                │
│   t4_transform_silver → t5_build_gold → t6_build_mart → t7_notify          │
└─────────────────────────────────────────────────────────────────────────────┘
               │
               ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                    MONITORING   [pipeline_logger.py]                        │
│                                                                             │
│   Reads: logs/quality_report.csv, logs/bronze_ingestion_log.csv            │
│   Output: monitoring/health_report.json + terminal dashboard                │
│   Status: HEALTHY (>90%) / WARNING (75-90%) / CRITICAL (<75% or stale)    │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 3. Tool Mapping: Local Stack vs. Telecom Production Stack

| Layer | This Project (Local) | Telecom Production Equivalent |
|---|---|---|
| Data source | Python faker/emitter generates CSVs | Huawei CBS SFTP push to HDFS landing zone |
| Landing zone | `data/landing/` (local filesystem) | HDFS `/landing/` zone on Hadoop cluster |
| Bronze ingestion | `pandas` + `pyarrow` | Apache Spark on YARN cluster |
| Quality gate | `pandas` custom rules | Great Expectations + custom Spark DQ job |
| Silver transform | PySpark `local[*]` | `spark-submit --master yarn --deploy-mode cluster` |
| Gold star schema | PySpark `local[*]` | `spark-submit --master yarn` + Hive metastore |
| Mart / KPI queries | DuckDB (in-memory) | Presto / Trino against Hive external tables |
| Storage format | Snappy Parquet on local disk | Snappy Parquet on HDFS |
| Orchestration | Airflow DAG (local scheduler) | Airflow on Kubernetes (Telecom analytics cluster) |
| File sensor | `FileSensor` on local path | `FileSensor` on HDFS + SFTP sensor |
| Monitoring | `pipeline_logger.py` terminal | Grafana + Prometheus pushgateway + PagerDuty |
| BI consumption | Parquet files (DuckDB queries) | Tableau / Power BI over Presto JDBC |
| Quarantine review | Local Parquet inspection | Hive quarantine table reviewed in Tableau |

**Key insight:** The PySpark code is **zero-change portable** to the cluster. Only the SparkSession master URL changes from `local[*]` to `yarn`. DuckDB SQL queries are directly portable to Presto/Trino.

---

## 4. Data Flow — Layer by Layer

### Layer 0: Data Generation (simulation only)
`generation/generate_data.py` creates 3,000 customers, 50,000 billing events, and 20,000 CDRs with 10% dirty data injected (nulls, negative amounts, invalid MSISDNs, duplicates). `generation/emitter.py` simulates the Huawei CBS SFTP cycle by writing a new CSV to `data/landing/stream/` every 30 seconds.

### Layer 1: Landing Zone
Raw CSV files arrive from two paths:
- `data/landing/batch/` — full initial loads (one-time)
- `data/landing/stream/` — micro-batch files from the emitter (ongoing)

The Airflow `FileSensor` (t1) watches `data/landing/stream/` and triggers the pipeline when a new file appears.

### Layer 2: Bronze
`ingestion/ingest_bronze.py` reads every CSV and writes it as Snappy Parquet to `data/bronze/{table}/`. Three metadata columns are added for lineage: `_ingested_at` (UTC timestamp), `_source_file` (original filename), `_batch_id` (UUID grouping rows from the same run). No type casting, no deduplication — bronze is a faithful copy of the source.

### Layer 3: Quality Gate
`quality/quality_checks.py` runs 5 categories of rules across all three bronze tables. Failed rows are written to `data/quarantine/` as Parquet with a `_dq_flags` column listing which rules each row violated. A machine-readable quality report is written to `logs/quality_report.csv`. Rows that fail any check are excluded from silver via anti-join.

### Layer 4: Silver
`spark/transform_silver.py` reads bronze excluding quarantined rows, standardizes all columns (types, casing, date parsing), deduplicates on primary keys (latest `_ingested_at` wins), enriches billing events with customer dimension attributes, and computes per-customer billing metrics. Output is partitioned by `billing_month` or `call_month` for query efficiency.

### Layer 5: Gold
`spark/build_gold.py` builds a complete star schema from silver tables. The critical transformation is **CDR reconciliation**: for each billing event, the pipeline checks whether a Huawei MSC CDR exists for the same (customer_id, service_type, event_date). The resulting `is_reconciled` flag is the core Revenue Assurance metric. Five surrogate-key dimensions are built and joined to produce the `fact_billing` table.

### Layer 6: Mart
`mart/financial_mart.py` opens a DuckDB session, registers all Gold Parquet tables as views, and computes 5 KPIs using analytical SQL. Results are written as Parquet files to `data/mart/` and summarized in `mart/kpi_summary.csv`. This is the consumption layer for BI tools.

---

## 5. Design Decisions

### 5.1 Medallion Architecture (Bronze / Silver / Gold / Mart)
**Why:** Separation of concerns — each layer has a single responsibility. Bronze is the immutable audit log. Silver is the clean analytical dataset. Gold is optimized for BI queries. Mart is pre-aggregated for dashboards. This structure allows partial reprocessing: if a DQ rule changes, only Silver and downstream layers need to be re-run; Bronze is untouched.

### 5.2 Star Schema in Gold (not wide table)
**Why:** A wide denormalized table would duplicate dimension attributes across millions of fact rows (wasteful storage, slow scans). A star schema separates slowly-changing attributes (customer name, plan tier) from frequently-accessed facts (billed amounts). Presto/Trino and DuckDB both excel at star-schema joins on Parquet — the broadcast join for small dimensions is nearly free.

### 5.3 DuckDB for Mart (not Spark)
**Why:** The mart KPI queries are aggregations over ~45,000 rows of Gold data — well within DuckDB's sweet spot. Spinning up a Spark context for this volume would add 30+ seconds of JVM startup overhead with no benefit. DuckDB reads Parquet directly and runs the same ANSI SQL as Presto, making it a perfect lightweight substitute for local development. In production the same SQL runs on Presto/Trino with no changes.

### 5.4 Append-Only Bronze
**Why:** The bronze layer must be a tamper-proof audit log of all data received from Huawei CBS and MSC. If a DQ rule is later found to be incorrect and rows were wrongly quarantined, we need the ability to re-derive silver from the original raw data. Deleting or overwriting bronze would destroy that capability.

### 5.5 Anti-Join Quarantine Pattern (not DELETE)
**Why:** The silver layer excludes bad rows by performing a left anti-join against the quarantine table rather than deleting them from bronze. This keeps bronze immutable and allows the quarantine population to change over time (as DQ rules are tuned) without touching the raw data store.

### 5.6 CDR Reconciliation at the Fact Level
**Why:** Revenue Assurance best practice is to flag reconciliation at the finest grain — individual billing events — rather than computing aggregate match rates and discarding the detail. Storing `is_reconciled` at the event level allows the RA team to query unreconciled events by customer, service, region, or time period to identify root causes.

### 5.7 15-Minute SLA on a 30-Minute Schedule
**Why:** The Airflow DAG runs every 30 minutes, matching the Huawei CBS SFTP batch cycle. The pipeline must complete in under 15 minutes to leave a 15-minute buffer before the next batch arrives. This prevents batches from queuing and ensures the mart KPIs are updated within one batch window of data arrival.

---

## 6. How to Run the Full Pipeline

### Prerequisites
```bash
pip install pandas pyarrow pyspark duckdb pyyaml apache-airflow
```

### Step-by-Step

```bash
# 1. Generate synthetic data (3,000 customers, 50,000 events, 20,000 CDRs)
python generation/generate_data.py

# 2. Start the micro-batch emitter (optional, runs in background)
python generation/emitter.py &

# 3. Ingest landing CSVs into bronze Parquet
python ingestion/ingest_bronze.py

# 4. Run data quality checks; quarantine bad rows
python quality/quality_checks.py

# 5. PySpark: standardize + enrich → silver layer
python spark/transform_silver.py

# 6. PySpark: build star schema → gold layer
python spark/build_gold.py

# 7. DuckDB: compute KPIs → mart layer
python mart/financial_mart.py

# 8. Check pipeline health dashboard
python monitoring/pipeline_logger.py
```

### Via Airflow (production mode)
```bash
# Start Airflow scheduler and webserver
airflow db init
airflow scheduler &
airflow webserver --port 8080 &

# Trigger manually with a custom date range
airflow dags trigger telecom_billing_dwh_pipeline \
  --conf '{"start_date": "2025-04-01", "end_date": "2025-06-30", "triggered_by": "you"}'

# Or wait for the scheduled run (every 30 minutes)
```

### Check the KPI summary
```bash
# Quick review of all 5 KPIs
cat mart/kpi_summary.csv

# Full health dashboard
python monitoring/pipeline_logger.py
```
