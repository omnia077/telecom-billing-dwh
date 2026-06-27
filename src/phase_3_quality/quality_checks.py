"""
============================================================================
quality_checks.py — Data Quality Validation Engine
============================================================================
Responsibility:
    Read Bronze-layer Parquet files and run five categories of quality
    rules against customers, billing_events, and CDRs. Bad rows are
    quarantined; a quality report is written for audit and monitoring.

    Quality rule categories:
      1. Completeness  — null / empty checks on required columns
      2. Validity      — type checks, range checks, format validation
      3. Uniqueness    — duplicate detection on primary keys
      4. Consistency   — cross-dataset logical checks
      5. Timeliness    — flag stale (>180 days) or future-dated records

Maps to (the telecom operator production):
    In production, Telecom runs DQ validation as an Airflow task between
    bronze and silver layers. Failed rows are routed to a quarantine
    zone on HDFS. DQ scores are pushed to a Grafana dashboard for the
    Revenue Assurance team to monitor CBS/MSC feed health.

    This module replicates that gate using pandas for lightweight
    local execution (no Spark overhead for validation logic).

Output:
    - data/quarantine/{dataset}_quarantined.parquet  (bad rows)
    - logs/quality_report.csv                        (rule-level results)
    - Console: per-dataset quality scores

Usage:
    python quality/quality_checks.py
============================================================================
"""

import os
import uuid
import yaml
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, timezone
from pathlib import Path
from collections import defaultdict


# ============================================================================
# CONFIGURATION
# ============================================================================

CONFIG_PATH = Path(__file__).resolve().parent.parent.parent / "config" / "pipeline_config.yaml"
with open(CONFIG_PATH, "r") as f:
    CONFIG = yaml.safe_load(f)

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
BRONZE_DIR = PROJECT_ROOT / CONFIG["paths"]["bronze"]
QUARANTINE_DIR = PROJECT_ROOT / CONFIG["paths"]["quarantine"]
LOGS_DIR = PROJECT_ROOT / "logs"

# Telecom domain constants for validation rules
VALID_OPERATOR_PREFIXES = tuple(CONFIG["telecom"]["operator_prefixes"])
VALID_COMPETITOR_PREFIXES = tuple(CONFIG["telecom"]["competitor_prefixes"])
ALL_VALID_PREFIXES = VALID_OPERATOR_PREFIXES + VALID_COMPETITOR_PREFIXES
VALID_PLAN_TYPES = set(CONFIG["telecom"]["plan_types"])
VALID_SERVICE_TYPES = set(CONFIG["telecom"]["service_types"])

# Timeliness thresholds
# Maps to: the operator's SLA — events older than 180 days are flagged for review
STALENESS_DAYS = 180
REFERENCE_DATE = datetime(2025, 6, 30)  # aligns with generated data end date

# Valid charge types — mirrors Huawei CBS output codes
VALID_CHARGE_TYPES = {
    "voice_charge", "sms_charge", "data_charge", "vas_charge",
    "roaming_charge", "subscription_fee", "bundle_purchase",
    "balance_topup", "promotional_credit", "penalty_fee"
}

# DQ run metadata
DQ_RUN_ID = str(uuid.uuid4())
DQ_RUN_TIMESTAMP = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


# ============================================================================
# DATA LOADERS
# ============================================================================

def load_bronze_table(table_name):
    """
    Load all Parquet files from a bronze subdirectory into one DataFrame.
    Maps to: spark.read.parquet(f"/data/bronze/{table_name}")
    """
    table_dir = BRONZE_DIR / table_name
    parquet_files = list(table_dir.glob("*.parquet"))

    if not parquet_files:
        print(f"  [WARN] No Parquet files found in {table_dir}")
        return pd.DataFrame()

    dfs = [pd.read_parquet(f) for f in parquet_files]
    df = pd.concat(dfs, ignore_index=True)
    print(f"  Loaded {table_name}: {len(df):,} rows from {len(parquet_files)} file(s)")
    return df


# ============================================================================
# QUALITY RULE ENGINE
# ============================================================================

class QualityReport:
    """
    Collects quality check results across all datasets and rules.
    Maps to: the operator's DQ metrics pushed to Grafana for Revenue Assurance.
    """

    def __init__(self):
        self.entries = []

    def add(self, dataset, category, rule_name, total_rows, failed_rows, failed_ids):
        """Record one quality rule result."""
        self.entries.append({
            "dq_run_id": DQ_RUN_ID,
            "dq_run_timestamp": DQ_RUN_TIMESTAMP,
            "dataset": dataset,
            "category": category,
            "rule_name": rule_name,
            "total_rows": total_rows,
            "failed_rows": failed_rows,
            "pass_rate": round((1 - failed_rows / max(total_rows, 1)) * 100, 2),
            "failed_row_ids": failed_ids,
        })

    def to_dataframe(self):
        """Convert report entries to a DataFrame for CSV export."""
        df = pd.DataFrame(self.entries)
        if "failed_row_ids" in df.columns:
            df = df.drop(columns=["failed_row_ids"])
        return df

    def get_quarantine_ids(self, dataset):
        """Get all unique row IDs that failed any check for a dataset."""
        all_ids = set()
        for entry in self.entries:
            if entry["dataset"] == dataset and entry["failed_row_ids"]:
                all_ids.update(entry["failed_row_ids"])
        return all_ids


# ============================================================================
# CATEGORY 1: COMPLETENESS CHECKS
# ============================================================================
# Detect NULL / empty values in columns that must always be populated.
# Maps to: the operator's mandatory field rules from the Huawei CBS interface spec.

def check_completeness(df, dataset, id_col, required_cols, report):
    """
    Check that required columns have non-null, non-empty values.
    Every telecom record must have its primary key and core attributes.
    """
    print(f"\n  [COMPLETENESS] {dataset}")

    for col in required_cols:
        if col not in df.columns:
            report.add(dataset, "completeness", f"missing_column_{col}",
                       len(df), len(df), list(df[id_col]))
            print(f"    FAIL: column '{col}' does not exist ({len(df)} rows affected)")
            continue

        # Check for null, NaN, empty string, and whitespace-only values
        mask = df[col].isna() | (df[col].astype(str).str.strip() == "")
        failed = df.loc[mask]
        failed_ids = list(failed[id_col])

        report.add(dataset, "completeness", f"null_check_{col}",
                   len(df), len(failed), failed_ids)

        if len(failed) > 0:
            print(f"    FAIL: {col} has {len(failed)} null/empty values "
                  f"({len(failed)/len(df)*100:.1f}%)")
        else:
            print(f"    PASS: {col}")


# ============================================================================
# CATEGORY 2: VALIDITY CHECKS
# ============================================================================
# Verify data types, value ranges, and format constraints.
# Maps to: Huawei CBS rated-event validation rules.

def check_validity_customers(df, report):
    """
    Validity rules for customer records:
      - msisdn must start with a valid operator prefix and be 11 digits
      - balance must be numeric and non-negative for prepaid/hybrid
      - date_of_birth must be a valid date, not in the future
      - plan_type must be in the allowed set
    """
    dataset = "customers"
    id_col = "customer_id"
    print(f"\n  [VALIDITY] {dataset}")

    # Rule: msisdn format — must be 11 digits starting with valid prefix
    # Maps to: national numbering plan
    valid_msisdn = df["msisdn"].astype(str).str.match(r"^09\d{9}$")
    known_prefix = df["msisdn"].astype(str).str[:4].isin(
        list(VALID_OPERATOR_PREFIXES) + list(VALID_COMPETITOR_PREFIXES)
    )
    msisdn_invalid = ~(valid_msisdn & known_prefix) & (df["msisdn"].astype(str).str.strip() != "")
    failed = df.loc[msisdn_invalid]
    report.add(dataset, "validity", "msisdn_format", len(df), len(failed),
               list(failed[id_col]))
    print(f"    msisdn_format: {len(failed)} invalid "
          f"({len(failed)/len(df)*100:.1f}%)")

    # Rule: balance must be non-negative for prepaid and hybrid plans
    # Postpaid accounts can carry negative balances (credit usage)
    df_prepaid = df[df["plan_type"].isin(["prepaid", "hybrid"])].copy()
    df_prepaid["balance_num"] = pd.to_numeric(df_prepaid["balance"], errors="coerce")
    neg_balance = df_prepaid[df_prepaid["balance_num"] < 0]
    report.add(dataset, "validity", "negative_prepaid_balance", len(df_prepaid),
               len(neg_balance), list(neg_balance[id_col]))
    print(f"    negative_prepaid_balance: {len(neg_balance)} invalid "
          f"({len(neg_balance)/max(len(df_prepaid),1)*100:.1f}% of prepaid/hybrid)")

    # Rule: plan_type must be in valid set
    invalid_plan = ~df["plan_type"].isin(VALID_PLAN_TYPES)
    failed = df.loc[invalid_plan]
    report.add(dataset, "validity", "plan_type_valid", len(df), len(failed),
               list(failed[id_col]))
    print(f"    plan_type_valid: {len(failed)} invalid")

    # Rule: registration_date must not be in the future
    df["reg_date_parsed"] = pd.to_datetime(df["registration_date"], errors="coerce")
    future_reg = df[df["reg_date_parsed"] > REFERENCE_DATE]
    report.add(dataset, "validity", "future_registration_date", len(df),
               len(future_reg), list(future_reg[id_col]))
    print(f"    future_registration_date: {len(future_reg)} invalid "
          f"({len(future_reg)/len(df)*100:.1f}%)")


def check_validity_billing(df, report):
    """
    Validity rules for billing events:
      - amount must be numeric and non-negative (credits are separate charge_types)
      - charge_type must be in the valid CBS code set
      - event_timestamp must be a valid datetime
      - service_type must be in the valid set
    """
    dataset = "billing_events"
    id_col = "event_id"
    print(f"\n  [VALIDITY] {dataset}")

    # Rule: amount must be non-negative
    # Maps to: Huawei CBS rated amounts are always >= 0; reversals use status='reversed'
    df["amount_num"] = pd.to_numeric(df["amount"], errors="coerce")
    neg_amount = df[df["amount_num"] < 0]
    report.add(dataset, "validity", "negative_amount", len(df), len(neg_amount),
               list(neg_amount[id_col]))
    print(f"    negative_amount: {len(neg_amount)} invalid "
          f"({len(neg_amount)/len(df)*100:.1f}%)")

    # Rule: charge_type must be a known CBS code
    invalid_charge = ~df["charge_type"].isin(VALID_CHARGE_TYPES)
    failed = df.loc[invalid_charge]
    report.add(dataset, "validity", "charge_type_valid", len(df), len(failed),
               list(failed[id_col]))
    print(f"    charge_type_valid: {len(failed)} invalid "
          f"({len(failed)/len(df)*100:.1f}%)")

    # Rule: event_timestamp must parse as a valid datetime
    parsed = pd.to_datetime(df["event_timestamp"], errors="coerce")
    unparseable = df[parsed.isna()]
    report.add(dataset, "validity", "event_timestamp_parseable", len(df),
               len(unparseable), list(unparseable[id_col]))
    print(f"    event_timestamp_parseable: {len(unparseable)} invalid")

    # Rule: service_type must be in valid set
    invalid_svc = ~df["service_type"].isin(VALID_SERVICE_TYPES)
    failed = df.loc[invalid_svc]
    report.add(dataset, "validity", "service_type_valid", len(df), len(failed),
               list(failed[id_col]))
    print(f"    service_type_valid: {len(failed)} invalid")


def check_validity_cdrs(df, report):
    """
    Validity rules for CDRs:
      - duration_seconds must be non-negative
      - duration_seconds must be <= 86400 (24 hours max per call)
      - rated_amount must be non-negative
      - calling_msisdn must be a valid format
      - completed calls must have duration > 0
    """
    dataset = "cdrs"
    id_col = "cdr_id"
    print(f"\n  [VALIDITY] {dataset}")

    df["duration_num"] = pd.to_numeric(df["duration_seconds"], errors="coerce")
    df["rated_num"] = pd.to_numeric(df["rated_amount"], errors="coerce")

    # Rule: duration_seconds must be non-negative
    neg_dur = df[df["duration_num"] < 0]
    report.add(dataset, "validity", "negative_duration", len(df), len(neg_dur),
               list(neg_dur[id_col]))
    print(f"    negative_duration: {len(neg_dur)} invalid "
          f"({len(neg_dur)/len(df)*100:.1f}%)")

    # Rule: duration_seconds must be <= 86400 (24 hours)
    # Maps to: Huawei MSC auto-disconnects calls at the 24-hour mark
    extreme_dur = df[df["duration_num"] > 86400]
    report.add(dataset, "validity", "extreme_duration_gt_24h", len(df),
               len(extreme_dur), list(extreme_dur[id_col]))
    print(f"    extreme_duration_gt_24h: {len(extreme_dur)} invalid "
          f"({len(extreme_dur)/len(df)*100:.1f}%)")

    # Rule: rated_amount must be non-negative
    neg_rate = df[df["rated_num"] < 0]
    report.add(dataset, "validity", "negative_rated_amount", len(df), len(neg_rate),
               list(neg_rate[id_col]))
    print(f"    negative_rated_amount: {len(neg_rate)} invalid "
          f"({len(neg_rate)/len(df)*100:.1f}%)")

    # Rule: calling_msisdn must match local phone format
    valid_calling = df["calling_msisdn"].astype(str).str.match(r"^09\d{9}$")
    invalid_calling = ~valid_calling
    failed = df.loc[invalid_calling]
    report.add(dataset, "validity", "calling_msisdn_format", len(df), len(failed),
               list(failed[id_col]))
    print(f"    calling_msisdn_format: {len(failed)} invalid "
          f"({len(failed)/len(df)*100:.1f}%)")

    # Rule: completed calls must have duration > 0
    # A completed call with 0 duration is a logical impossibility
    zero_completed = df[(df["status"] == "completed") & (df["duration_num"] == 0)]
    report.add(dataset, "validity", "zero_duration_completed_call", len(df),
               len(zero_completed), list(zero_completed[id_col]))
    print(f"    zero_duration_completed_call: {len(zero_completed)} invalid "
          f"({len(zero_completed)/len(df)*100:.1f}%)")


# ============================================================================
# CATEGORY 3: UNIQUENESS CHECKS
# ============================================================================
# Detect duplicate primary keys. In production, CBS retry storms can
# produce duplicate rated events. MSC failovers can duplicate CDRs.

def check_uniqueness(df, dataset, id_col, report):
    """
    Check that the primary key column has no duplicates.
    Maps to: the operator's dedup logic in the bronze-to-silver pipeline.
    """
    print(f"\n  [UNIQUENESS] {dataset}")

    duplicates = df[df.duplicated(subset=[id_col], keep=False)]
    num_dup_rows = len(duplicates)
    num_dup_keys = duplicates[id_col].nunique()

    # Report the duplicate rows (all copies, not just the extras)
    report.add(dataset, "uniqueness", f"duplicate_{id_col}", len(df),
               num_dup_rows, list(duplicates[id_col]))

    if num_dup_rows > 0:
        print(f"    FAIL: {num_dup_rows} duplicate rows across "
              f"{num_dup_keys} repeated {id_col} values "
              f"({num_dup_rows/len(df)*100:.1f}%)")
    else:
        print(f"    PASS: no duplicate {id_col} values")


# ============================================================================
# CATEGORY 4: CONSISTENCY CHECKS
# ============================================================================
# Cross-dataset logical validation. Ensures referential integrity and
# business rule coherence between related tables.
# Maps to: the operator's Revenue Assurance reconciliation checks.

def check_consistency(customers_df, billing_df, cdrs_df, report):
    """
    Cross-dataset consistency rules:
      - Billing events must reference existing customer_ids (referential integrity)
      - CDR calling_msisdn should exist in the customer table (on-net validation)
      - CDR service_type should be consistent with billing service_type for the
        same subscriber within the same day
    """
    print(f"\n  [CONSISTENCY] cross-dataset")

    # --- Rule 1: Billing customer_id must exist in customers table ---
    # Maps to: FK constraint between CBS rated events and CRM subscriber master
    valid_customer_ids = set(customers_df["customer_id"])
    billing_non_empty = billing_df[billing_df["customer_id"].astype(str).str.strip() != ""]
    orphan_billing = billing_non_empty[
        ~billing_non_empty["customer_id"].isin(valid_customer_ids)
    ]
    report.add("billing_events", "consistency", "orphan_customer_id",
               len(billing_df), len(orphan_billing),
               list(orphan_billing["event_id"]))
    print(f"    orphan_billing_customer_id: {len(orphan_billing)} events reference "
          f"non-existent customers ({len(orphan_billing)/len(billing_df)*100:.1f}%)")

    # --- Rule 2: On-net CDR calling_msisdn should exist in customers ---
    # Only check on-net calls — off-net/international callers are not our subscribers
    valid_msisdns = set(customers_df["msisdn"].astype(str))
    onnet_cdrs = cdrs_df[cdrs_df["call_type"] == "on_net"].copy()
    orphan_cdrs = onnet_cdrs[~onnet_cdrs["calling_msisdn"].isin(valid_msisdns)]
    report.add("cdrs", "consistency", "onnet_caller_not_in_customers",
               len(onnet_cdrs), len(orphan_cdrs),
               list(orphan_cdrs["cdr_id"]))
    print(f"    onnet_caller_not_in_customers: {len(orphan_cdrs)} on-net CDRs "
          f"have unrecognized calling_msisdn "
          f"({len(orphan_cdrs)/max(len(onnet_cdrs),1)*100:.1f}% of on-net)")

    # --- Rule 3: CDR service_type vs billing service_type consistency ---
    # For subscribers with both CDRs and billing events on the same day,
    # the service types should align. Mismatches indicate a rating error.
    # Join CDRs to customers to get customer_id, then check billing
    cdr_with_cust = cdrs_df.merge(
        customers_df[["customer_id", "msisdn"]],
        left_on="calling_msisdn",
        right_on="msisdn",
        how="inner"
    )

    if len(cdr_with_cust) > 0:
        # Build daily service sets from billing: what services each customer was billed for
        billing_daily_svc = (
            billing_df
            .groupby(["customer_id", "event_date"])["service_type"]
            .apply(set)
            .reset_index()
            .rename(columns={"service_type": "billed_services"})
        )

        cdr_check = cdr_with_cust.merge(
            billing_daily_svc,
            left_on=["customer_id", "call_date"],
            right_on=["customer_id", "event_date"],
            how="inner"
        )

        if len(cdr_check) > 0:
            mismatched = cdr_check[
                cdr_check.apply(
                    lambda r: r["service_type"] not in r["billed_services"], axis=1
                )
            ]
            report.add("cdrs", "consistency", "service_type_billing_mismatch",
                       len(cdr_check), len(mismatched),
                       list(mismatched["cdr_id"]))
            print(f"    service_type_billing_mismatch: {len(mismatched)} CDRs have "
                  f"service_type not matching same-day billing "
                  f"({len(mismatched)/max(len(cdr_check),1)*100:.1f}%)")
        else:
            report.add("cdrs", "consistency", "service_type_billing_mismatch",
                       0, 0, [])
            print(f"    service_type_billing_mismatch: no overlapping records to check")
    else:
        report.add("cdrs", "consistency", "service_type_billing_mismatch",
                   0, 0, [])
        print(f"    service_type_billing_mismatch: no CDR-customer join matches")


# ============================================================================
# CATEGORY 5: TIMELINESS CHECKS
# ============================================================================
# Flag records that are too old (stale) or in the future (premature).
# Maps to: the operator's SLA monitoring — late CBS file drops trigger alerts.

def check_timeliness(df, dataset, id_col, date_col, report, is_dimension=False):
    """
    Timeliness rules:
      - Fact tables: flag records older than 180 days (stale feed) or future-dated
      - Dimension tables: only check for future-dated records (old registration
        dates are expected — a customer who signed up in 2020 is not stale data)

    Maps to: the operator's SLA monitoring distinguishes between fact feeds
    (CBS/MSC events that should arrive within hours) and dimension
    feeds (CRM snapshots that contain historical records).
    """
    print(f"\n  [TIMELINESS] {dataset}")

    df["_date_parsed"] = pd.to_datetime(df[date_col], errors="coerce")

    # Rule: stale records — only for fact/transactional tables
    # Dimension tables like customers have historical dates by design
    if not is_dimension:
        staleness_cutoff = REFERENCE_DATE - timedelta(days=STALENESS_DAYS)
        stale = df[df["_date_parsed"] < staleness_cutoff]
        report.add(dataset, "timeliness", f"stale_record_gt_{STALENESS_DAYS}d",
                   len(df), len(stale), list(stale[id_col]))
        print(f"    stale (>{STALENESS_DAYS} days): {len(stale)} records "
              f"({len(stale)/len(df)*100:.1f}%)")
    else:
        print(f"    stale check: skipped (dimension table)")

    # Rule: future-dated records — applies to all tables
    future = df[df["_date_parsed"] > REFERENCE_DATE]
    report.add(dataset, "timeliness", f"future_dated_record",
               len(df), len(future), list(future[id_col]))
    print(f"    future-dated: {len(future)} records "
          f"({len(future)/len(df)*100:.1f}%)")


# ============================================================================
# QUARANTINE WRITER
# ============================================================================

def quarantine_bad_rows(df, dataset, id_col, quarantine_ids, report):
    """
    Extract rows that failed any quality check and write to quarantine.
    Maps to: the operator's HDFS quarantine zone at /data/quarantine/{dataset}/

    Quarantined rows include a _dq_flags column listing which rules they
    violated, enabling the Revenue Assurance team to triage by failure type.
    """
    if not quarantine_ids:
        print(f"  [{dataset}] No rows to quarantine")
        return pd.DataFrame()

    quarantined = df[df[id_col].isin(quarantine_ids)].copy()

    # Drop temporary columns added by quality check functions
    # (pandas datetime64[ns] columns cause Spark 4 read errors)
    temp_cols = [c for c in quarantined.columns
                 if c.endswith("_parsed") or c.endswith("_num")]
    quarantined = quarantined.drop(columns=temp_cols, errors="ignore")

    # Build a flag column showing which rules each row violated
    flag_map = defaultdict(list)
    for entry in report.entries:
        if entry["dataset"] == dataset:
            for rid in entry["failed_row_ids"]:
                flag_map[rid].append(entry["rule_name"])

    quarantined["_dq_flags"] = quarantined[id_col].map(
        lambda x: "|".join(sorted(set(flag_map.get(x, []))))
    )
    quarantined["_dq_run_id"] = DQ_RUN_ID
    quarantined["_quarantined_at"] = DQ_RUN_TIMESTAMP

    # Write quarantine Parquet
    os.makedirs(QUARANTINE_DIR, exist_ok=True)
    output_path = QUARANTINE_DIR / f"{dataset}_quarantined.parquet"
    quarantined.to_parquet(output_path, engine="pyarrow", compression="snappy",
                           index=False)

    size_kb = os.path.getsize(output_path) / 1024
    print(f"  [{dataset}] Quarantined {len(quarantined):,} rows "
          f"({len(quarantined)/len(df)*100:.1f}%) -> {output_path.name} "
          f"({size_kb:.1f} KB)")

    return quarantined


# ============================================================================
# QUALITY SCORE CALCULATOR
# ============================================================================

def calculate_quality_scores(report, dataset_sizes):
    """
    Calculate overall and per-category quality scores.
    Score = percentage of rows that passed ALL checks in that category.
    Maps to: the operator's DQ KPIs tracked in the Revenue Assurance dashboard.
    """
    print("\n" + "=" * 70)
    print("QUALITY SCORE SUMMARY")
    print("=" * 70)

    overall_total = 0
    overall_passed = 0

    for dataset, total_rows in dataset_sizes.items():
        quarantine_ids = report.get_quarantine_ids(dataset)
        clean_rows = total_rows - len(quarantine_ids)
        score = (clean_rows / max(total_rows, 1)) * 100

        overall_total += total_rows
        overall_passed += clean_rows

        print(f"\n  {dataset}:")
        print(f"    Total rows:       {total_rows:>10,}")
        print(f"    Clean rows:       {clean_rows:>10,}")
        print(f"    Quarantined:      {len(quarantine_ids):>10,}")
        print(f"    Quality score:    {score:>9.1f}%")

        # Per-category breakdown
        categories = set(e["category"] for e in report.entries
                        if e["dataset"] == dataset)
        for cat in sorted(categories):
            cat_entries = [e for e in report.entries
                          if e["dataset"] == dataset and e["category"] == cat]
            if cat_entries:
                avg_pass = np.mean([e["pass_rate"] for e in cat_entries])
                print(f"      {cat:<20s} avg pass rate: {avg_pass:.1f}%")

    overall_score = (overall_passed / max(overall_total, 1)) * 100
    print(f"\n  {'='*50}")
    print(f"  OVERALL QUALITY SCORE: {overall_score:.1f}%")
    print(f"    Total rows:     {overall_total:>10,}")
    print(f"    Clean rows:     {overall_passed:>10,}")
    print(f"    Quarantined:    {overall_total - overall_passed:>10,}")
    print(f"  {'='*50}")

    return overall_score


# ============================================================================
# MAIN EXECUTION
# ============================================================================

def main():
    """
    Entry point — run all quality checks, quarantine bad rows, write report.

    Pipeline flow:
      1. Load bronze Parquet tables
      2. Run 5 categories of checks per dataset
      3. Collect all failed row IDs
      4. Quarantine failed rows to data/quarantine/
      5. Write quality_report.csv to logs/
      6. Print quality scores
    """
    print("=" * 70)
    print("Telecom Billing DWH -- Data Quality Validation Engine")
    print("=" * 70)
    print(f"  DQ Run ID:    {DQ_RUN_ID[:8]}...")
    print(f"  Timestamp:    {DQ_RUN_TIMESTAMP}")
    print(f"  Bronze dir:   {BRONZE_DIR}")
    print(f"  Quarantine:   {QUARANTINE_DIR}")
    print("-" * 70)

    # --- Step 1: Load Bronze Data ---
    print("\n[Loading Bronze Tables]")
    customers_df = load_bronze_table("customers")
    billing_df = load_bronze_table("billing_events")
    cdrs_df = load_bronze_table("cdrs")

    if customers_df.empty or billing_df.empty or cdrs_df.empty:
        print("\n[ERROR] One or more bronze tables are empty. Run generate_data.py "
              "and ingest_bronze.py first.")
        return

    report = QualityReport()

    # --- Step 2: Run Quality Checks ---
    print("\n" + "=" * 70)
    print("RUNNING QUALITY CHECKS")
    print("=" * 70)

    # ----- CUSTOMERS -----
    print("\n" + "-" * 40)
    print("Dataset: CUSTOMERS")
    print("-" * 40)

    check_completeness(
        customers_df, "customers", "customer_id",
        required_cols=["customer_id", "msisdn", "national_id", "first_name",
                       "last_name", "plan_type", "status"],
        report=report
    )
    check_validity_customers(customers_df, report)
    check_uniqueness(customers_df, "customers", "customer_id", report)
    check_timeliness(customers_df, "customers", "customer_id",
                     "registration_date", report, is_dimension=True)

    # ----- BILLING EVENTS -----
    print("\n" + "-" * 40)
    print("Dataset: BILLING EVENTS")
    print("-" * 40)

    check_completeness(
        billing_df, "billing_events", "event_id",
        required_cols=["event_id", "customer_id", "event_timestamp",
                       "charge_type", "amount"],
        report=report
    )
    check_validity_billing(billing_df, report)
    check_uniqueness(billing_df, "billing_events", "event_id", report)
    check_timeliness(billing_df, "billing_events", "event_id",
                     "event_date", report)

    # ----- CDRs -----
    print("\n" + "-" * 40)
    print("Dataset: CDRs")
    print("-" * 40)

    check_completeness(
        cdrs_df, "cdrs", "cdr_id",
        required_cols=["cdr_id", "calling_msisdn", "called_msisdn",
                       "call_start_time", "duration_seconds"],
        report=report
    )
    check_validity_cdrs(cdrs_df, report)
    check_uniqueness(cdrs_df, "cdrs", "cdr_id", report)
    check_timeliness(cdrs_df, "cdrs", "cdr_id", "call_date", report)

    # ----- CROSS-DATASET CONSISTENCY -----
    print("\n" + "-" * 40)
    print("Dataset: CROSS-DATASET CONSISTENCY")
    print("-" * 40)

    check_consistency(customers_df, billing_df, cdrs_df, report)

    # --- Step 3: Quarantine Bad Rows ---
    print("\n" + "=" * 70)
    print("QUARANTINE")
    print("=" * 70)

    dataset_sizes = {
        "customers": len(customers_df),
        "billing_events": len(billing_df),
        "cdrs": len(cdrs_df),
    }

    quarantine_bad_rows(customers_df, "customers", "customer_id",
                        report.get_quarantine_ids("customers"), report)
    quarantine_bad_rows(billing_df, "billing_events", "event_id",
                        report.get_quarantine_ids("billing_events"), report)
    quarantine_bad_rows(cdrs_df, "cdrs", "cdr_id",
                        report.get_quarantine_ids("cdrs"), report)

    # --- Step 4: Write Quality Report ---
    os.makedirs(LOGS_DIR, exist_ok=True)
    report_path = LOGS_DIR / "quality_report.csv"
    report_df = report.to_dataframe()
    report_df.to_csv(report_path, index=False, encoding="utf-8")
    print(f"\n  Quality report written to: {report_path}")
    print(f"  Total rules evaluated: {len(report_df)}")

    # --- Step 5: Quality Scores ---
    overall_score = calculate_quality_scores(report, dataset_sizes)

    print("\n" + "=" * 70)
    print("Data Quality Gate complete.")
    if overall_score >= 90:
        print(f"  RESULT: PASS (score {overall_score:.1f}% >= 90% threshold)")
    else:
        print(f"  RESULT: WARN (score {overall_score:.1f}% < 90% threshold)")
    print("=" * 70)


if __name__ == "__main__":
    main()
