"""
============================================================================
build_gold.py — Gold Layer Star Schema Builder (PySpark)
============================================================================
Responsibility:
    Read Silver-layer Parquet tables and build a dimensional star schema
    in the Gold layer. This is the analytics-ready layer consumed by
    BI tools (Tableau, Power BI) and the financial KPI mart.

    Star schema components:
      Dimensions:
        - dim_customer   — subscriber profiles with segmentation
        - dim_service    — service type taxonomy
        - dim_date       — calendar dimension (2-year range)
        - dim_region     — geographic hierarchy
        - dim_plan       — tariff plan attributes and expected ARPU
      Facts:
        - fact_billing   — billing transactions with FK links to all dims,
                           CDR-reconciled for revenue assurance

Maps to (the telecom operator production):
    In production, this PySpark job runs on the operator's Hadoop/YARN cluster:
      - spark-submit --master yarn --deploy-mode cluster build_gold.py
      - Reads from HDFS /data/silver/
      - Writes to HDFS /data/gold/ (Hive external tables)
      - Downstream: Presto/Trino queries from Tableau dashboards
      - Scheduled as an Airflow task downstream of the Silver transform

    Surrogate keys use monotonically_increasing_id() locally.
    In production, Telecom uses Hive SEQUENCE or a key management service.

Output:
    - data/gold/dim_customer/
    - data/gold/dim_service/
    - data/gold/dim_date/
    - data/gold/dim_region/
    - data/gold/dim_plan/
    - data/gold/fact_billing/   (partitioned by billing_month)

Usage:
    python spark/build_gold.py
============================================================================
"""

import os
import sys
import yaml
from pathlib import Path
from datetime import date, timedelta
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    IntegerType, DateType, BooleanType
)


# ============================================================================
# CONFIGURATION
# ============================================================================

CONFIG_PATH = Path(__file__).resolve().parent.parent.parent / "config" / "pipeline_config.yaml"
with open(CONFIG_PATH, "r") as f:
    CONFIG = yaml.safe_load(f)

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
SILVER_DIR = str(PROJECT_ROOT / CONFIG["paths"]["silver"])
GOLD_DIR = str(PROJECT_ROOT / CONFIG["paths"]["gold"])


# ============================================================================
# DOMAIN REFERENCE DATA
# ============================================================================
# Maps to: the operator's master data management (MDM) system.
# In production, these mappings live in Hive reference tables maintained
# by the business intelligence team. Here we inline them for portability.

# City → (Governorate, Zone)
# Zone logic: Khartoum State cities → Central, all others → Regional
REGION_LOOKUP = [
    ("Khartoum",    "Khartoum",       "Central"),
    ("Omdurman",    "Khartoum",       "Central"),
    ("Bahri",       "Khartoum",       "Central"),
    ("Wad Madani",  "Gezira",         "Central"),
    ("Sennar",      "Sennar",         "Central"),
    ("Port the country",  "Red Sea",        "Eastern"),
    ("Kassala",     "Kassala",        "Eastern"),
    ("Gedaref",     "Gedaref",        "Eastern"),
    ("Atbara",      "River Nile",     "Northern"),
    ("Dongola",     "Northern",       "Northern"),
    ("El Obeid",    "North Kordofan", "Western"),
    ("Nyala",       "South Darfur",   "Western"),
    ("El Fasher",   "North Darfur",   "Western"),
    ("Kadugli",     "South Kordofan", "Western"),
    ("Ed Damazin",  "Blue Nile",      "Southern"),
]

# Service type → business category
# Maps to: the operator's service catalogue in Huawei CBS
SERVICE_LOOKUP = [
    ("voice",   "communication"),
    ("sms",     "messaging"),
    ("data",    "internet"),
    ("vas",     "value_added_services"),
    ("roaming", "roaming"),
]

# Plan type → (plan_name, plan_tier, expected_arpu_sdg)
# Maps to: the operator's commercial tariff catalogue
# Expected ARPU based on the operator's published investor metrics (approximate)
PLAN_LOOKUP = [
    ("prepaid",  "Operator Prepaid",  "basic",    150.00),
    ("postpaid", "Operator Postpaid", "premium",  500.00),
    ("hybrid",   "Operator Flex",     "standard", 300.00),
]


# ============================================================================
# SPARK SESSION
# ============================================================================

def create_spark_session():
    """
    Create a SparkSession for local Gold layer build.
    Maps to: the operator's cluster SparkSession with YARN master.
    """
    spark = (
        SparkSession.builder
        .master("local[*]")
        .appName("TelecomBillingDWH_GoldBuild")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.driver.memory", "2g")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


# ============================================================================
# DIMENSION BUILDERS
# ============================================================================

def build_dim_customer(spark, customers_df, metrics_df):
    """
    Build the customer dimension — SCD Type 1 (overwrite with latest).

    Enrichment:
      - full_name from first_name + last_name
      - segment derived from total billing amount:
          high_value  (> 2000 LC), mid_value (> 500 LC), low_value
      - governorate mapped from city/state via REGION_LOOKUP
      - is_active flag from subscriber status

    Maps to: the operator's Customer 360 dimension in the enterprise data warehouse.
    SCD Type 1 means we always overwrite with the latest snapshot — no
    history tracking at the dimension level (history is in fact tables).
    """
    region_schema = StructType([
        StructField("_region_name", StringType()),
        StructField("_governorate", StringType()),
        StructField("_zone", StringType()),
    ])
    region_ref = spark.createDataFrame(REGION_LOOKUP, region_schema)

    dim = customers_df.join(
        metrics_df.select("customer_id", "total_billed_amount"),
        on="customer_id",
        how="left"
    )

    dim = dim.join(
        F.broadcast(region_ref),
        dim["state"] == region_ref["_region_name"],
        "left"
    )

    dim = (
        dim
        .withColumn("full_name",
                    F.concat_ws(" ", F.initcap("first_name"), F.initcap("last_name")))
        .withColumn("segment",
                    F.when(F.col("total_billed_amount") > 2000, "high_value")
                     .when(F.col("total_billed_amount") > 500, "mid_value")
                     .otherwise("low_value"))
        .withColumn("is_active",
                    F.when(F.col("status") == "active", True).otherwise(False))
        .select(
            "customer_id",
            "msisdn",
            "full_name",
            "segment",
            F.col("plan_type").alias("plan"),
            F.col("state").alias("region"),
            F.coalesce(F.col("_governorate"), F.lit("Unknown")).alias("governorate"),
            F.col("registration_date").alias("activation_date"),
            "is_active",
        )
    )

    print(f"  dim_customer: {dim.count():,} rows")
    return dim


def build_dim_service(spark):
    """
    Build the service dimension from the Telecom service catalogue.

    Category mapping follows the operator's internal service taxonomy:
      voice   → communication
      sms     → messaging
      data    → internet
      vas     → value_added_services
      roaming → roaming

    Maps to: Huawei CBS service catalogue table (SERVICE_OFFERING).
    Surrogate key: monotonically_increasing_id() starting from 1.
    """
    schema = StructType([
        StructField("service_type", StringType()),
        StructField("service_category", StringType()),
    ])
    dim = spark.createDataFrame(SERVICE_LOOKUP, schema).coalesce(1)
    dim = dim.withColumn("service_id",
                         (F.monotonically_increasing_id() + 1).cast(IntegerType()))
    dim = dim.select("service_id", "service_type", "service_category")

    print(f"  dim_service: {dim.count()} rows")
    return dim


def build_dim_date(spark):
    """
    Build a calendar dimension covering 2024-01-01 to 2025-12-31.

    Attributes:
      date_id (YYYYMMDD int), full_date, day, month, month_name,
      quarter, year, is_weekend, is_month_end

    Maps to: the operator's shared calendar dimension in the enterprise DWH.
    Every fact table joins to dim_date via date_id for time-based
    analysis, drill-down, and partition pruning.
    """
    start = date(2024, 1, 1)
    end = date(2025, 12, 31)
    num_days = (end - start).days + 1

    dim = spark.range(0, num_days).select(
        F.date_add(F.lit(str(start)), F.col("id").cast("int")).alias("full_date")
    )

    dim = (
        dim
        .withColumn("date_id",
                    F.date_format("full_date", "yyyyMMdd").cast(IntegerType()))
        .withColumn("day", F.dayofmonth("full_date"))
        .withColumn("month", F.month("full_date"))
        .withColumn("month_name", F.date_format("full_date", "MMMM"))
        .withColumn("quarter", F.quarter("full_date"))
        .withColumn("year", F.year("full_date"))
        .withColumn("is_weekend",
                    F.when(F.dayofweek("full_date").isin(1, 7), True)
                     .otherwise(False))
        .withColumn("is_month_end",
                    F.when(
                        F.col("full_date") == F.last_day("full_date"), True
                    ).otherwise(False))
        .select("date_id", "full_date", "day", "month", "month_name",
                "quarter", "year", "is_weekend", "is_month_end")
    )

    print(f"  dim_date: {dim.count()} rows ({start} to {end})")
    return dim


def build_dim_region(spark):
    """
    Build the region dimension from the operator's geographic hierarchy.

    Hierarchy: region_name (city) → governorate (state) → zone
    Zone logic: Khartoum State cities → Central, all others → Regional

    Maps to: the operator's network planning regions. The Central/Regional split
    reflects the operator's organizational structure — Khartoum has a dedicated
    operations team while other regions share a field operations unit.
    """
    schema = StructType([
        StructField("region_name", StringType()),
        StructField("governorate", StringType()),
        StructField("zone", StringType()),
    ])
    dim = spark.createDataFrame(REGION_LOOKUP, schema).coalesce(1)
    dim = dim.withColumn("region_id",
                         (F.monotonically_increasing_id() + 1).cast(IntegerType()))
    dim = dim.select("region_id", "region_name", "governorate", "zone")

    print(f"  dim_region: {dim.count()} rows")
    return dim


def build_dim_plan(spark):
    """
    Build the plan dimension from the operator's tariff catalogue.

    Attributes:
      plan_id, plan_name, plan_type, plan_tier, expected_arpu

    Expected ARPU (Average Revenue Per User) values are approximate
    monthly values in LC based on the operator's published investor reports.

    Maps to: the operator's commercial product catalogue maintained by
    the Marketing & Products team. In production, this would be
    sourced from the CRM system.
    """
    schema = StructType([
        StructField("plan_type", StringType()),
        StructField("plan_name", StringType()),
        StructField("plan_tier", StringType()),
        StructField("expected_arpu", DoubleType()),
    ])
    dim = spark.createDataFrame(PLAN_LOOKUP, schema).coalesce(1)
    dim = dim.withColumn("plan_id",
                         (F.monotonically_increasing_id() + 1).cast(IntegerType()))
    dim = dim.select("plan_id", "plan_name", "plan_type", "plan_tier", "expected_arpu")

    print(f"  dim_plan: {dim.count()} rows")
    return dim


# ============================================================================
# FACT BUILDER
# ============================================================================

def build_fact_billing(spark, billing_df, cdrs_df, customers_df,
                       dim_service, dim_region, dim_plan):
    """
    Build the billing fact table with FK links to all dimensions.

    Key transformation: CDR reconciliation
      For each billing event, check whether a supporting CDR exists
      for the same (customer_id, service_type, event_date). If yes,
      is_reconciled = 1. Unreconciled billing events are a revenue
      leakage signal — they indicate charges with no network evidence.

    Metrics derived from CDR join:
      - duration_sec:  total CDR duration for voice billing events
      - data_mb:       estimated data usage (amount / rate) for data events
      - sms_count:     CDR count for SMS billing events

    Maps to: the operator's Revenue Assurance reconciliation process. In production,
    the RA team runs daily reconciliation between Huawei CBS rated events
    and MSC CDRs. Mismatches trigger investigations and are tracked on
    the RA Grafana dashboard.
    """

    # --- Step 0: Filter to enriched-only billing events ---
    # Gold layer requires fully-joined data — exclude billing events
    # that couldn't be matched to a customer (quarantined subscribers).
    billing_df = billing_df.filter(F.col("_enrichment_status") == "matched")
    print(f"    Enriched billing events: {billing_df.count():,}")

    # --- Step 1: Prepare CDR reconciliation data ---
    # Map CDR calling_msisdn → customer_id via the customer dimension
    cdr_with_cust = cdrs_df.join(
        customers_df.select(
            F.col("customer_id").alias("_cdr_cust_id"),
            F.col("msisdn").alias("_cdr_msisdn"),
        ),
        cdrs_df["calling_msisdn"] == F.col("_cdr_msisdn"),
        "inner"
    ).drop("_cdr_msisdn")

    # Aggregate CDRs by (customer, service, date) for reconciliation
    cdr_recon = (
        cdr_with_cust
        .groupBy(
            F.col("_cdr_cust_id"),
            F.col("service_type").alias("_cdr_svc"),
            F.col("call_date").alias("_cdr_date"),
        )
        .agg(
            F.sum("duration_seconds").alias("_cdr_duration"),
            F.count("*").alias("_cdr_count"),
        )
    )

    # --- Step 2: Left-join billing events to CDR aggregates ---
    fact = billing_df.join(
        cdr_recon,
        (billing_df["customer_id"] == cdr_recon["_cdr_cust_id"]) &
        (billing_df["service_type"] == cdr_recon["_cdr_svc"]) &
        (billing_df["event_date"] == cdr_recon["_cdr_date"]),
        "left"
    )

    # --- Step 3: Derive reconciliation flag and usage metrics ---
    fact = (
        fact
        .withColumn("is_reconciled",
                    F.when(F.col("_cdr_count").isNotNull(), 1).otherwise(0))
        .withColumn("duration_sec",
                    F.when(F.col("service_type") == "voice",
                           F.coalesce(F.col("_cdr_duration"), F.lit(0)))
                     .otherwise(F.lit(0)))
        .withColumn("sms_count",
                    F.when(F.col("service_type") == "sms",
                           F.coalesce(F.col("_cdr_count"), F.lit(0)))
                     .otherwise(F.lit(0)))
        .withColumn("data_mb",
                    F.when(F.col("service_type") == "data",
                           F.round(F.col("amount") / 0.05, 2))
                     .otherwise(F.lit(0.0)))
    )

    # --- Step 4: Join to dimension tables for surrogate FK keys ---
    # Service FK
    fact = fact.join(
        F.broadcast(dim_service.select(
            F.col("service_id"),
            F.col("service_type").alias("_dim_svc_type"),
        )),
        fact["service_type"] == F.col("_dim_svc_type"),
        "left"
    ).drop("_dim_svc_type")

    # Region FK — join on the customer's state (cust_state from enriched billing)
    fact = fact.join(
        F.broadcast(dim_region.select(
            F.col("region_id"),
            F.col("region_name").alias("_dim_region_name"),
        )),
        fact["cust_state"] == F.col("_dim_region_name"),
        "left"
    ).drop("_dim_region_name")

    # Plan FK — join on the billing event's plan_type
    fact = fact.join(
        F.broadcast(dim_plan.select(
            F.col("plan_id"),
            F.col("plan_type").alias("_dim_plan_type"),
        )),
        fact["plan_type"] == F.col("_dim_plan_type"),
        "left"
    ).drop("_dim_plan_type")

    # Date FK — derive from event_date as YYYYMMDD integer
    fact = fact.withColumn(
        "date_id",
        F.date_format("event_date", "yyyyMMdd").cast(IntegerType())
    )

    # --- Step 5: Add surrogate fact_id and billing_cycle ---
    fact = (
        fact
        .withColumn("fact_id", F.monotonically_increasing_id())
        .withColumn("billing_cycle", F.month("event_date"))
        .withColumn("billed_amount", F.col("amount"))
    )

    # --- Step 6: Select final star-schema columns ---
    fact = fact.select(
        "fact_id",
        "customer_id",
        "service_id",
        "date_id",
        "region_id",
        "plan_id",
        "billed_amount",
        "duration_sec",
        "data_mb",
        "sms_count",
        "billing_month",
        "billing_cycle",
        "currency",
        F.col("status"),
        "is_reconciled",
    )

    recon_rate = (
        fact.filter(F.col("is_reconciled") == 1).count()
        / max(fact.count(), 1)
        * 100
    )
    print(f"  fact_billing: {fact.count():,} rows")
    print(f"    Reconciliation rate: {recon_rate:.1f}%")

    return fact


# ============================================================================
# GOLD WRITER
# ============================================================================

def write_gold_table(df, table_name, partition_col=None):
    """
    Write a DataFrame to the Gold layer as Parquet.
    Maps to: Hive external table on HDFS at /data/gold/{table_name}.
    """
    output_path = os.path.join(GOLD_DIR, table_name)

    writer = df.write.mode("overwrite").option("compression", "snappy")

    if partition_col:
        writer = writer.partitionBy(partition_col)

    writer.parquet(output_path)

    file_count = len([f for f in Path(output_path).rglob("*.parquet")])
    row_count = df.count()
    print(f"    -> {table_name}: {row_count:,} rows, {file_count} file(s)")

    if partition_col:
        partitions = df.select(partition_col).distinct().count()
        print(f"       Partitioned by {partition_col}: {partitions} partitions")


# ============================================================================
# MAIN EXECUTION
# ============================================================================

def main():
    """
    Entry point — build the full Gold star schema from Silver layer.

    Pipeline steps:
      1. Create SparkSession
      2. Load Silver tables
      3. Build 5 dimension tables
      4. Build fact_billing with CDR reconciliation
      5. Write all tables to data/gold/ as Parquet
    """
    print("=" * 70)
    print("Telecom Billing DWH -- Gold Layer Star Schema (PySpark)")
    print("=" * 70)
    print(f"  Silver:  {SILVER_DIR}")
    print(f"  Gold:    {GOLD_DIR}")
    print("-" * 70)

    # --- Step 1: Spark Session ---
    print("\n[1/5] Creating SparkSession...")
    spark = create_spark_session()
    print(f"  Spark {spark.version} | Master: {spark.sparkContext.master}")

    try:
        # --- Step 2: Load Silver Tables ---
        print("\n[2/5] Loading Silver tables...")
        billing_enriched = spark.read.parquet(
            os.path.join(SILVER_DIR, "billing_enriched")
        )
        cdrs_cleaned = spark.read.parquet(
            os.path.join(SILVER_DIR, "cdrs_cleaned")
        )
        customers_cleaned = spark.read.parquet(
            os.path.join(SILVER_DIR, "customers_cleaned")
        )
        customer_metrics = spark.read.parquet(
            os.path.join(SILVER_DIR, "customer_metrics")
        )
        print(f"  billing_enriched: {billing_enriched.count():,} rows")
        print(f"  cdrs_cleaned:     {cdrs_cleaned.count():,} rows")
        print(f"  customers_cleaned:{customers_cleaned.count():,} rows")
        print(f"  customer_metrics: {customer_metrics.count():,} rows")

        # --- Step 3: Build Dimensions ---
        print("\n[3/5] Building dimension tables...")

        dim_customer = build_dim_customer(spark, customers_cleaned, customer_metrics)
        dim_service = build_dim_service(spark)
        dim_date = build_dim_date(spark)
        dim_region = build_dim_region(spark)
        dim_plan = build_dim_plan(spark)

        # --- Step 4: Build Fact Table ---
        print("\n[4/5] Building fact_billing (with CDR reconciliation)...")
        fact_billing = build_fact_billing(
            spark, billing_enriched, cdrs_cleaned, customers_cleaned,
            dim_service, dim_region, dim_plan
        )

        # --- Step 5: Write Gold Layer ---
        print("\n[5/5] Writing Gold Parquet tables...")
        write_gold_table(dim_customer, "dim_customer")
        write_gold_table(dim_service, "dim_service")
        write_gold_table(dim_date, "dim_date")
        write_gold_table(dim_region, "dim_region")
        write_gold_table(dim_plan, "dim_plan")
        write_gold_table(fact_billing, "fact_billing", partition_col="billing_month")

        # --- Summary ---
        print("\n" + "=" * 70)
        print("Gold star schema build complete!")
        print(f"  Dimensions:")
        print(f"    dim_customer:  {dim_customer.count():,} rows")
        print(f"    dim_service:   {dim_service.count()} rows")
        print(f"    dim_date:      {dim_date.count()} rows")
        print(f"    dim_region:    {dim_region.count()} rows")
        print(f"    dim_plan:      {dim_plan.count()} rows")
        print(f"  Facts:")
        print(f"    fact_billing:  {fact_billing.count():,} rows")
        print(f"  Output: {GOLD_DIR}")
        print("=" * 70)

    finally:
        spark.stop()
        print("\n  SparkSession stopped.")


if __name__ == "__main__":
    main()
