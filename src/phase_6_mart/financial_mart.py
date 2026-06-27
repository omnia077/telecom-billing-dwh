"""
============================================================================
financial_mart.py — Financial KPI Mart (DuckDB)
============================================================================
Responsibility:
    Read Gold-layer Parquet star schema into DuckDB and compute five
    financial KPIs that the operator's Revenue Assurance and Finance
    teams consume daily. Results are written as Parquet files for
    downstream BI consumption and a CSV summary for quick review.

    KPIs computed:
      1. ARPU by month and region
      2. MOU (Minutes of Use) by service type and month
      3. Revenue by plan and governorate
      4. Billing reconciliation rate by month
      5. Top 10 revenue customers per month

Maps to (the telecom operator production):
    In production, KPI marts are built using Presto/Trino queries
    against Hive tables on the operator's Hadoop cluster. The results feed
    Tableau dashboards consumed by:
      - CFO office         → revenue, ARPU trends
      - Revenue Assurance  → reconciliation rates, leakage detection
      - Network Planning   → MOU by region for capacity planning
      - Marketing          → customer segmentation and plan performance

    DuckDB here simulates Presto's analytical SQL engine. The queries
    are directly portable — DuckDB and Presto share ANSI SQL semantics
    and both read Parquet natively.

Output:
    - data/mart/kpi_arpu_by_month_region.parquet
    - data/mart/kpi_mou_by_service.parquet
    - data/mart/kpi_revenue_by_plan_governorate.parquet
    - data/mart/kpi_reconciliation_rate.parquet
    - data/mart/kpi_top_customers_by_month.parquet
    - mart/kpi_summary.csv

Usage:
    python mart/financial_mart.py
============================================================================
"""

import os
import yaml
import duckdb
import pandas as pd
from pathlib import Path
from datetime import datetime, timezone


# ============================================================================
# CONFIGURATION
# ============================================================================

CONFIG_PATH = Path(__file__).resolve().parent.parent.parent / "config" / "pipeline_config.yaml"
with open(CONFIG_PATH, "r") as f:
    CONFIG = yaml.safe_load(f)

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
GOLD_DIR = PROJECT_ROOT / CONFIG["paths"]["gold"]
MART_DIR = PROJECT_ROOT / "data" / "mart"
SUMMARY_PATH = PROJECT_ROOT / "data" / "mart" / "kpi_summary.csv"

# KPI thresholds — maps to the operator's SLA and business targets
RECONCILIATION_WARN_THRESHOLD = 0.90  # flag months below 90%


# ============================================================================
# DUCKDB SESSION
# ============================================================================

def create_duckdb_session():
    """
    Create an in-memory DuckDB connection and register Gold Parquet tables.

    Maps to: Presto/Trino session on the operator's analytics cluster.
    DuckDB reads Parquet directly — no data loading step required,
    same as Presto reading from Hive external tables on HDFS.
    """
    con = duckdb.connect(database=":memory:")

    gold_tables = {
        "fact_billing": GOLD_DIR / "fact_billing",
        "dim_customer": GOLD_DIR / "dim_customer",
        "dim_service":  GOLD_DIR / "dim_service",
        "dim_date":     GOLD_DIR / "dim_date",
        "dim_region":   GOLD_DIR / "dim_region",
        "dim_plan":     GOLD_DIR / "dim_plan",
    }

    for table_name, table_path in gold_tables.items():
        parquet_glob = str(table_path / "**" / "*.parquet")
        con.execute(f"""
            CREATE VIEW {table_name} AS
            SELECT * FROM read_parquet('{parquet_glob}', hive_partitioning=true)
        """)
        count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        print(f"  {table_name}: {count:,} rows")

    return con


# ============================================================================
# KPI 1: ARPU BY MONTH AND REGION
# ============================================================================

def compute_arpu(con):
    """
    Average Revenue Per User (ARPU) by billing month and region.

    Formula: SUM(billed_amount) / COUNT(DISTINCT customer_id)
             grouped by billing_month and region

    Maps to: the operator's primary financial KPI — reported monthly to the
    CFO and included in investor presentations. ARPU is tracked by
    region to identify geographic performance variations and guide
    network investment decisions.
    """
    query = """
        SELECT
            f.billing_month,
            r.region_name                                        AS region,
            r.governorate,
            COUNT(DISTINCT f.customer_id)                        AS active_subscribers,
            ROUND(SUM(f.billed_amount), 2)                      AS total_revenue,
            ROUND(SUM(f.billed_amount) /
                  NULLIF(COUNT(DISTINCT f.customer_id), 0), 2)   AS arpu
        FROM fact_billing f
        LEFT JOIN dim_region r ON f.region_id = r.region_id
        WHERE f.status = 'rated'
        GROUP BY f.billing_month, r.region_name, r.governorate
        ORDER BY f.billing_month, arpu DESC
    """
    df = con.execute(query).fetchdf()
    print(f"\n  KPI 1 — ARPU by Month & Region: {len(df)} rows")

    if not df.empty:
        overall_arpu = df["total_revenue"].sum() / max(df["active_subscribers"].sum(), 1)
        print(f"    Overall ARPU: {overall_arpu:,.2f} LC")
        print(f"    Month range: {df['billing_month'].min()} to {df['billing_month'].max()}")

    return df


# ============================================================================
# KPI 2: MOU (MINUTES OF USE) BY SERVICE TYPE
# ============================================================================

def compute_mou(con):
    """
    Minutes of Use (MOU) by service type and billing month.

    Formula: SUM(duration_sec) / 60  grouped by service_type, billing_month

    Maps to: the operator's network utilization KPI. MOU drives capacity
    planning — the Network Planning team uses MOU trends to forecast
    BSC/RNC expansion needs. High MOU growth in a region triggers
    Huawei equipment procurement requests.
    """
    query = """
        SELECT
            f.billing_month,
            s.service_type,
            s.service_category,
            ROUND(SUM(f.duration_sec) / 60.0, 2)   AS total_mou,
            COUNT(*)                                 AS event_count,
            ROUND(AVG(f.duration_sec) / 60.0, 2)    AS avg_mou_per_event
        FROM fact_billing f
        LEFT JOIN dim_service s ON f.service_id = s.service_id
        WHERE f.duration_sec > 0
        GROUP BY f.billing_month, s.service_type, s.service_category
        ORDER BY f.billing_month, total_mou DESC
    """
    df = con.execute(query).fetchdf()
    print(f"\n  KPI 2 — MOU by Service Type: {len(df)} rows")

    if not df.empty:
        total_mou = df["total_mou"].sum()
        print(f"    Total MOU: {total_mou:,.0f} minutes")

    return df


# ============================================================================
# KPI 3: REVENUE BY PLAN AND GOVERNORATE
# ============================================================================

def compute_revenue_by_plan(con):
    """
    Revenue breakdown by tariff plan and governorate.

    Formula: SUM(billed_amount) grouped by plan_name, governorate

    Maps to: the operator's product performance report. The Marketing team
    uses this to evaluate plan adoption and revenue contribution
    across geographic segments. Underperforming plans in specific
    governorates may trigger localized promotions.
    """
    query = """
        SELECT
            p.plan_name,
            p.plan_type,
            p.plan_tier,
            r.governorate,
            r.zone,
            COUNT(*)                            AS billing_events,
            COUNT(DISTINCT f.customer_id)       AS subscribers,
            ROUND(SUM(f.billed_amount), 2)      AS total_revenue,
            ROUND(AVG(f.billed_amount), 2)      AS avg_event_amount,
            ROUND(SUM(f.billed_amount) /
                  NULLIF(COUNT(DISTINCT f.customer_id), 0), 2) AS revenue_per_subscriber
        FROM fact_billing f
        LEFT JOIN dim_plan p    ON f.plan_id = p.plan_id
        LEFT JOIN dim_region r  ON f.region_id = r.region_id
        WHERE f.status = 'rated'
        GROUP BY p.plan_name, p.plan_type, p.plan_tier, r.governorate, r.zone
        ORDER BY total_revenue DESC
    """
    df = con.execute(query).fetchdf()
    print(f"\n  KPI 3 — Revenue by Plan & Governorate: {len(df)} rows")

    if not df.empty:
        total_rev = df["total_revenue"].sum()
        print(f"    Total rated revenue: {total_rev:,.2f} LC")
        top = df.head(3)
        for _, row in top.iterrows():
            print(f"    Top: {row['plan_name']} / {row['governorate']} — "
                  f"{row['total_revenue']:,.2f} LC")

    return df


# ============================================================================
# KPI 4: BILLING RECONCILIATION RATE
# ============================================================================

def compute_reconciliation(con):
    """
    Billing reconciliation rate by month.

    Formula: COUNT(is_reconciled=1) / COUNT(*) per billing_month

    Maps to: the operator's Revenue Assurance reconciliation dashboard.
    This is the most critical RA metric — it measures the percentage
    of billing events that have a corresponding CDR from the Huawei MSC.

    Unreconciled events indicate potential:
      - Revenue leakage (charges without network evidence)
      - CBS rating errors (duplicate charges)
      - MSC CDR drops (network events not captured)

    Any month below 90% triggers a WARNING and investigation.
    """
    query = """
        SELECT
            billing_month,
            COUNT(*)                                         AS total_events,
            SUM(CASE WHEN is_reconciled = 1 THEN 1 ELSE 0 END) AS reconciled_events,
            SUM(CASE WHEN is_reconciled = 0 THEN 1 ELSE 0 END) AS unreconciled_events,
            ROUND(
                SUM(CASE WHEN is_reconciled = 1 THEN 1 ELSE 0 END) * 100.0
                / NULLIF(COUNT(*), 0), 2
            )                                                AS reconciliation_pct,
            ROUND(
                SUM(CASE WHEN is_reconciled = 0 THEN billed_amount ELSE 0 END), 2
            )                                                AS unreconciled_revenue
        FROM fact_billing
        GROUP BY billing_month
        ORDER BY billing_month
    """
    df = con.execute(query).fetchdf()
    print(f"\n  KPI 4 — Reconciliation Rate: {len(df)} rows")

    if not df.empty:
        for _, row in df.iterrows():
            pct = row["reconciliation_pct"]
            status = "OK" if pct >= RECONCILIATION_WARN_THRESHOLD * 100 else "WARNING"
            flag = " <<<" if status == "WARNING" else ""
            print(f"    {row['billing_month']}: {pct:.1f}% "
                  f"({row['reconciled_events']:,}/{row['total_events']:,}) "
                  f"[{status}]{flag}")

        df["status"] = df["reconciliation_pct"].apply(
            lambda x: "OK" if x >= RECONCILIATION_WARN_THRESHOLD * 100 else "WARNING"
        )

    return df


# ============================================================================
# KPI 5: TOP 10 REVENUE CUSTOMERS PER MONTH
# ============================================================================

def compute_top_customers(con):
    """
    Top 10 highest-revenue customers per billing month.

    Formula: SUM(billed_amount) per customer, ranked by month

    Maps to: the operator's VIP customer tracking. The enterprise sales team
    monitors high-value subscribers for retention risk. Any top-10
    customer showing declining revenue triggers a proactive retention
    outreach from the Key Account Manager.
    """
    query = """
        WITH ranked AS (
            SELECT
                f.billing_month,
                f.customer_id,
                c.msisdn,
                c.full_name,
                c.segment,
                c.plan                                       AS plan_type,
                ROUND(SUM(f.billed_amount), 2)               AS monthly_revenue,
                COUNT(*)                                      AS event_count,
                ROW_NUMBER() OVER (
                    PARTITION BY f.billing_month
                    ORDER BY SUM(f.billed_amount) DESC
                )                                             AS revenue_rank
            FROM fact_billing f
            LEFT JOIN dim_customer c ON f.customer_id = c.customer_id
            WHERE f.status = 'rated'
            GROUP BY f.billing_month, f.customer_id,
                     c.msisdn, c.full_name, c.segment, c.plan
        )
        SELECT *
        FROM ranked
        WHERE revenue_rank <= 10
        ORDER BY billing_month, revenue_rank
    """
    df = con.execute(query).fetchdf()
    print(f"\n  KPI 5 — Top 10 Customers by Month: {len(df)} rows")

    if not df.empty:
        months = df["billing_month"].unique()
        for month in sorted(months)[:2]:
            month_df = df[df["billing_month"] == month].head(3)
            print(f"    {month} top 3:")
            for _, row in month_df.iterrows():
                print(f"      #{int(row['revenue_rank'])} {row['full_name']} "
                      f"({row['msisdn']}) — {row['monthly_revenue']:,.2f} LC")

    return df


# ============================================================================
# KPI SUMMARY
# ============================================================================

def build_kpi_summary(arpu_df, mou_df, revenue_df, recon_df, top_cust_df):
    """
    Build a one-row-per-KPI summary table for quick review.

    Maps to: the operator's daily KPI digest email sent to the senior
    leadership team every morning at 07:00 Khartoum time.
    """
    run_ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    summary = []

    # KPI 1: ARPU
    if not arpu_df.empty:
        overall_arpu = (
            arpu_df["total_revenue"].sum()
            / max(arpu_df["active_subscribers"].sum(), 1)
        )
        summary.append({
            "kpi_name": "ARPU",
            "description": "Average Revenue Per User (overall)",
            "value": round(overall_arpu, 2),
            "unit": "LC",
            "status": "OK" if overall_arpu > 0 else "WARNING",
            "computed_at": run_ts,
        })

    # KPI 2: MOU
    if not mou_df.empty:
        total_mou = mou_df["total_mou"].sum()
        summary.append({
            "kpi_name": "MOU",
            "description": "Total Minutes of Use (all services)",
            "value": round(total_mou, 0),
            "unit": "minutes",
            "status": "OK",
            "computed_at": run_ts,
        })

    # KPI 3: Revenue
    if not revenue_df.empty:
        total_rev = revenue_df["total_revenue"].sum()
        summary.append({
            "kpi_name": "Total Revenue",
            "description": "Total rated revenue across all plans",
            "value": round(total_rev, 2),
            "unit": "LC",
            "status": "OK",
            "computed_at": run_ts,
        })

    # KPI 4: Reconciliation
    if not recon_df.empty:
        avg_recon = recon_df["reconciliation_pct"].mean()
        warn_months = len(recon_df[recon_df["reconciliation_pct"]
                                   < RECONCILIATION_WARN_THRESHOLD * 100])
        status = "WARNING" if warn_months > 0 else "OK"
        summary.append({
            "kpi_name": "Reconciliation Rate",
            "description": f"Avg billing-CDR match rate ({warn_months} months below 90%)",
            "value": round(avg_recon, 2),
            "unit": "%",
            "status": status,
            "computed_at": run_ts,
        })

    # KPI 5: Top Customers
    if not top_cust_df.empty:
        top_rev = top_cust_df[top_cust_df["revenue_rank"] == 1]["monthly_revenue"].mean()
        summary.append({
            "kpi_name": "Top Customer Revenue",
            "description": "Avg monthly revenue of #1 customer per month",
            "value": round(top_rev, 2),
            "unit": "LC",
            "status": "OK",
            "computed_at": run_ts,
        })

    return pd.DataFrame(summary)


# ============================================================================
# PARQUET WRITER
# ============================================================================

def write_mart_table(df, table_name):
    """
    Write a KPI DataFrame as Parquet to data/mart/.
    Maps to: Hive external table on HDFS at /data/mart/{table_name}.
    """
    os.makedirs(MART_DIR, exist_ok=True)
    output_path = MART_DIR / f"{table_name}.parquet"
    df.to_parquet(output_path, engine="pyarrow", compression="snappy", index=False)
    size_kb = os.path.getsize(output_path) / 1024
    print(f"    -> {table_name}.parquet ({len(df)} rows, {size_kb:.1f} KB)")


# ============================================================================
# MAIN EXECUTION
# ============================================================================

def main():
    """
    Entry point — compute all financial KPIs from the Gold star schema.

    Pipeline steps:
      1. Create DuckDB session and register Gold tables
      2. Compute 5 KPIs using analytical SQL
      3. Write each KPI as Parquet to data/mart/
      4. Build and write KPI summary to mart/kpi_summary.csv
    """
    print("=" * 70)
    print("Telecom Billing DWH -- Financial KPI Mart (DuckDB)")
    print("=" * 70)
    print(f"  Gold source:  {GOLD_DIR}")
    print(f"  Mart output:  {MART_DIR}")
    print(f"  Summary:      {SUMMARY_PATH}")
    print("-" * 70)

    # --- Step 1: Create DuckDB Session ---
    print("\n[1/4] Connecting to DuckDB and registering Gold tables...")
    con = create_duckdb_session()

    # --- Step 2: Compute KPIs ---
    print("\n[2/4] Computing KPIs...")

    arpu_df = compute_arpu(con)
    mou_df = compute_mou(con)
    revenue_df = compute_revenue_by_plan(con)
    recon_df = compute_reconciliation(con)
    top_cust_df = compute_top_customers(con)

    # --- Step 3: Write KPI Parquet Files ---
    print("\n[3/4] Writing KPI Parquet files to data/mart/...")
    write_mart_table(arpu_df, "kpi_arpu_by_month_region")
    write_mart_table(mou_df, "kpi_mou_by_service")
    write_mart_table(revenue_df, "kpi_revenue_by_plan_governorate")
    write_mart_table(recon_df, "kpi_reconciliation_rate")
    write_mart_table(top_cust_df, "kpi_top_customers_by_month")

    # --- Step 4: KPI Summary ---
    print("\n[4/4] Building KPI summary...")
    summary_df = build_kpi_summary(arpu_df, mou_df, revenue_df,
                                   recon_df, top_cust_df)

    os.makedirs(os.path.dirname(SUMMARY_PATH), exist_ok=True)
    summary_df.to_csv(SUMMARY_PATH, index=False, encoding="utf-8")
    print(f"    -> kpi_summary.csv ({len(summary_df)} KPIs)")

    # --- Final Summary ---
    print("\n" + "=" * 70)
    print("KPI SUMMARY")
    print("=" * 70)
    for _, row in summary_df.iterrows():
        status_flag = " <<<" if row["status"] == "WARNING" else ""
        print(f"  {row['kpi_name']:<25s} {row['value']:>15,.2f} {row['unit']:<10s} "
              f"[{row['status']}]{status_flag}")
    print("=" * 70)

    con.close()
    print("\n  DuckDB session closed.")
    print("  Financial KPI mart complete!")


if __name__ == "__main__":
    main()
