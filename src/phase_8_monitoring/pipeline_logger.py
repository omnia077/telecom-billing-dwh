"""
============================================================================
pipeline_logger.py — Pipeline Health Monitor
============================================================================
Responsibility:
    Read logs/bronze_ingestion_log.csv and logs/quality_report.csv,
    compute pipeline health metrics, print a terminal dashboard, and
    write monitoring/health_report.json.

Health status rules:
    HEALTHY   → quality pass rate > 90%
    WARNING   → quality pass rate 75–90%
    CRITICAL  → quality pass rate < 75%  OR  no runs in the last 2 hours

Maps to (the telecom operator production):
    In production, this runs as a cron job every 15 minutes and pushes
    metrics to the Revenue Assurance Grafana dashboard. The status field
    drives alerting: CRITICAL pages the on-call data engineer via PagerDuty.

Usage:
    python monitoring/pipeline_logger.py
============================================================================
"""

import json
import os
import sys
import pandas as pd
from datetime import datetime, timedelta, timezone
from pathlib import Path


# ============================================================================
# PATHS
# ============================================================================

PROJECT_ROOT    = Path(__file__).resolve().parent.parent.parent
LOGS_DIR        = PROJECT_ROOT / "logs"
MONITORING_DIR  = PROJECT_ROOT / "monitoring"
BRONZE_LOG_PATH = LOGS_DIR / "bronze_ingestion_log.csv"
QUALITY_LOG_PATH = LOGS_DIR / "quality_report.csv"
HEALTH_REPORT   = MONITORING_DIR / "health_report.json"

# SLA thresholds — mirror the Airflow DAG SLA and DQ gate thresholds
HEALTHY_THRESHOLD  = 90.0   # pass rate > 90% → HEALTHY
WARNING_THRESHOLD  = 75.0   # pass rate 75–90% → WARNING; below → CRITICAL
STALE_HOURS        = 2      # no successful run in 2h → CRITICAL regardless of score


# ============================================================================
# DATA LOADERS
# ============================================================================

def load_quality_report():
    """
    Load logs/quality_report.csv.
    Each row is one quality rule result from quality_checks.py.
    Returns an empty DataFrame with correct columns if the file is missing.
    """
    if not QUALITY_LOG_PATH.exists():
        return pd.DataFrame(columns=[
            "dq_run_id", "dq_run_timestamp", "dataset", "category",
            "rule_name", "total_rows", "failed_rows", "pass_rate",
        ])
    df = pd.read_csv(QUALITY_LOG_PATH)
    df["dq_run_timestamp"] = pd.to_datetime(df["dq_run_timestamp"], errors="coerce")
    return df


def load_bronze_log():
    """
    Load logs/bronze_ingestion_log.csv if it exists.
    Falls back gracefully — the bronze script writes console output but
    does not always write a structured log file (added here as extension).
    Returns None if file does not exist.
    """
    if not BRONZE_LOG_PATH.exists():
        return None
    df = pd.read_csv(BRONZE_LOG_PATH)
    if "ingested_at" in df.columns:
        df["ingested_at"] = pd.to_datetime(df["ingested_at"], errors="coerce")
    return df


# ============================================================================
# METRICS COMPUTATION
# ============================================================================

def compute_metrics(quality_df, bronze_df):
    """
    Derive health metrics from quality report and (optionally) bronze log.

    Returns a dict with:
      total_rows_ingested_today   — rows seen in DQ report today
      quality_pass_rate_pct       — weighted average pass rate across all rules
      quarantine_rate_pct         — inverse of pass rate (failed rows %)
      last_successful_run         — ISO timestamp of most recent DQ run
      minutes_since_last_run      — staleness indicator
      pipeline_status             — HEALTHY / WARNING / CRITICAL
      status_reason               — human-readable explanation
    """
    now_utc = datetime.now(timezone.utc)
    metrics = {}

    # ---- Last successful run ----
    if quality_df.empty or quality_df["dq_run_timestamp"].isna().all():
        last_run_ts = None
        minutes_since = None
    else:
        last_run_ts = quality_df["dq_run_timestamp"].dropna().max()
        # Make timezone-aware for comparison
        if last_run_ts.tzinfo is None:
            last_run_ts = last_run_ts.replace(tzinfo=timezone.utc)
        delta = now_utc - last_run_ts
        minutes_since = delta.total_seconds() / 60

    metrics["last_successful_run"] = (
        last_run_ts.strftime("%Y-%m-%d %H:%M:%S UTC") if last_run_ts else "NEVER"
    )
    metrics["minutes_since_last_run"] = (
        round(minutes_since, 1) if minutes_since is not None else None
    )

    # ---- Rows ingested today ----
    today_str = now_utc.strftime("%Y-%m-%d")
    if not quality_df.empty:
        today_runs = quality_df[
            quality_df["dq_run_timestamp"].dt.strftime("%Y-%m-%d") == today_str
        ]
        # Each rule reports total_rows for its dataset; use max per dataset to avoid
        # double-counting (multiple rules run against the same row set)
        total_today = 0
        for dataset in today_runs["dataset"].unique():
            ds_rows = today_runs[today_runs["dataset"] == dataset]["total_rows"].max()
            if pd.notna(ds_rows):
                total_today += int(ds_rows)
        metrics["total_rows_ingested_today"] = total_today
    else:
        metrics["total_rows_ingested_today"] = 0

    # Override with bronze log if available and has today's data
    if bronze_df is not None and "rows_ingested" in bronze_df.columns:
        if "ingested_at" in bronze_df.columns:
            today_bronze = bronze_df[
                bronze_df["ingested_at"].dt.strftime("%Y-%m-%d") == today_str
            ]
            if not today_bronze.empty:
                metrics["total_rows_ingested_today"] = int(
                    today_bronze["rows_ingested"].sum()
                )

    # ---- Quality pass rate ----
    if quality_df.empty:
        pass_rate = 0.0
    else:
        # Weight each rule by total_rows so large datasets don't get swamped
        # by small-table rules: pass_rate = Σ(pass_rate_i * total_rows_i) / Σ(total_rows_i)
        quality_df_valid = quality_df[quality_df["total_rows"] > 0]
        if quality_df_valid.empty:
            pass_rate = 0.0
        else:
            weighted_sum = (
                quality_df_valid["pass_rate"] * quality_df_valid["total_rows"]
            ).sum()
            total_weight = quality_df_valid["total_rows"].sum()
            pass_rate = round(weighted_sum / total_weight, 2)

    quarantine_rate = round(100.0 - pass_rate, 2)
    metrics["quality_pass_rate_pct"] = pass_rate
    metrics["quarantine_rate_pct"]   = quarantine_rate

    # ---- Determine pipeline status ----
    is_stale = (minutes_since is None) or (minutes_since > STALE_HOURS * 60)

    if is_stale:
        status = "CRITICAL"
        reason = (
            "No DQ run found in the last 2 hours - pipeline may be stalled. "
            "Check Airflow for failed tasks."
        )
    elif pass_rate < WARNING_THRESHOLD:
        status = "CRITICAL"
        reason = (
            f"Quality pass rate {pass_rate:.1f}% is below the CRITICAL threshold "
            f"of {WARNING_THRESHOLD:.0f}%. High quarantine volume detected."
        )
    elif pass_rate < HEALTHY_THRESHOLD:
        status = "WARNING"
        reason = (
            f"Quality pass rate {pass_rate:.1f}% is between WARNING threshold "
            f"({WARNING_THRESHOLD:.0f}%) and HEALTHY threshold ({HEALTHY_THRESHOLD:.0f}%). "
            "Review quality_report.csv for failing rules."
        )
    else:
        status = "HEALTHY"
        reason = (
            f"Quality pass rate {pass_rate:.1f}% exceeds HEALTHY threshold "
            f"({HEALTHY_THRESHOLD:.0f}%). Pipeline operating normally."
        )

    metrics["pipeline_status"] = status
    metrics["status_reason"]   = reason
    metrics["computed_at"]     = now_utc.strftime("%Y-%m-%d %H:%M:%S UTC")

    # ---- Per-dataset breakdown ----
    if not quality_df.empty:
        breakdown = {}
        for dataset in quality_df["dataset"].unique():
            ds_df = quality_df[quality_df["dataset"] == dataset]
            ds_valid = ds_df[ds_df["total_rows"] > 0]
            if ds_valid.empty:
                ds_rate = 0.0
            else:
                ds_weighted = (ds_valid["pass_rate"] * ds_valid["total_rows"]).sum()
                ds_total    = ds_valid["total_rows"].sum()
                ds_rate = round(ds_weighted / ds_total, 2)
            breakdown[dataset] = {
                "pass_rate_pct": ds_rate,
                "rules_evaluated": int(len(ds_df)),
                "failed_rules": int((ds_df["failed_rows"] > 0).sum()),
            }
        metrics["dataset_breakdown"] = breakdown
    else:
        metrics["dataset_breakdown"] = {}

    return metrics


# ============================================================================
# TERMINAL DASHBOARD
# ============================================================================

STATUS_ICONS = {
    "HEALTHY":  "[HEALTHY]",
    "WARNING":  "[WARNING]",
    "CRITICAL": "[CRITICAL]",
}

STATUS_SEPARATORS = {
    "HEALTHY":  "=" * 66,
    "WARNING":  "*" * 66,
    "CRITICAL": "!" * 66,
}


def print_dashboard(metrics):
    """
    Print a clean pipeline health dashboard to the terminal.

    Layout:
      ┌─ header with status ─────────────────────────────┐
      │  core metrics                                     │
      │  dataset breakdown                                │
      │  status reason                                    │
      └───────────────────────────────────────────────────┘
    """
    status = metrics["pipeline_status"]
    sep    = STATUS_SEPARATORS[status]
    icon   = STATUS_ICONS[status]

    print()
    print(sep)
    print(f"  TELECOM BILLING DWH -- PIPELINE HEALTH MONITOR")
    print(f"  the telecom operator | Revenue Assurance | {metrics['computed_at']}")
    print(sep)

    # ---- Core Metrics ----
    print()
    print(f"  {'STATUS':<30s}  {icon}")
    print()
    print(f"  {'Rows ingested today':<30s}  {metrics['total_rows_ingested_today']:>12,}")
    print(f"  {'Quality pass rate':<30s}  {metrics['quality_pass_rate_pct']:>11.1f}%")
    print(f"  {'Quarantine rate':<30s}  {metrics['quarantine_rate_pct']:>11.1f}%")
    print(f"  {'Last successful run':<30s}  {metrics['last_successful_run']:>20s}")

    if metrics["minutes_since_last_run"] is not None:
        mins = metrics["minutes_since_last_run"]
        staleness = f"{mins:.1f} min ago"
        if mins > 120:
            staleness += "  <<< STALE"
        print(f"  {'Time since last run':<30s}  {staleness:>20s}")
    else:
        print(f"  {'Time since last run':<30s}  {'N/A — no run found':>20s}")

    # ---- Dataset Breakdown ----
    if metrics.get("dataset_breakdown"):
        print()
        print(f"  {'-' * 62}")
        print(f"  {'Dataset':<22s}  {'Pass Rate':>10s}  {'Rules':>6s}  {'Failing':>8s}")
        print(f"  {'-' * 62}")
        for dataset, info in sorted(metrics["dataset_breakdown"].items()):
            rate   = info["pass_rate_pct"]
            rules  = info["rules_evaluated"]
            failed = info["failed_rules"]
            flag   = " <<<" if rate < HEALTHY_THRESHOLD else ""
            print(f"  {dataset:<22s}  {rate:>9.1f}%  {rules:>6d}  {failed:>8d}{flag}")

    # ---- Status Reason ----
    print()
    print(f"  {'-' * 62}")
    print(f"  Reason: {metrics['status_reason']}")

    # ---- Thresholds Legend ----
    print()
    print(f"  Thresholds:  HEALTHY > {HEALTHY_THRESHOLD:.0f}%  |  "
          f"WARNING {WARNING_THRESHOLD:.0f}-{HEALTHY_THRESHOLD:.0f}%  |  "
          f"CRITICAL < {WARNING_THRESHOLD:.0f}% or stale > {STALE_HOURS}h")
    print()
    print(sep)
    print()


# ============================================================================
# HEALTH REPORT WRITER
# ============================================================================

def write_health_report(metrics):
    """
    Write health metrics to monitoring/health_report.json.
    Maps to: Prometheus metrics endpoint scraped by Grafana in production.
    """
    os.makedirs(MONITORING_DIR, exist_ok=True)
    with open(HEALTH_REPORT, "w", encoding="utf-8") as f:
        json.dump(metrics, f, indent=2, default=str)
    print(f"  Health report written to: {HEALTH_REPORT}")


# ============================================================================
# MAIN
# ============================================================================

def main():
    """
    Entry point — load logs, compute metrics, print dashboard, write JSON.
    """
    print("Loading pipeline logs...")

    quality_df = load_quality_report()
    bronze_df  = load_bronze_log()

    if quality_df.empty:
        print(
            "\n  [WARN] logs/quality_report.csv not found or empty.\n"
            "  Run the full pipeline first:\n"
            "    1. python generation/generate_data.py\n"
            "    2. python ingestion/ingest_bronze.py\n"
            "    3. python quality/quality_checks.py\n"
        )

    metrics = compute_metrics(quality_df, bronze_df)

    print_dashboard(metrics)
    write_health_report(metrics)


if __name__ == "__main__":
    main()
