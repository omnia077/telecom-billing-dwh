"""
============================================================================
transform_silver.py — PySpark Bronze-to-Silver Transformation
============================================================================
Responsibility:
    Read clean rows from Bronze (excluding quarantined records), apply
    standardization and enrichment transforms, and write the Silver
    layer as partitioned Parquet.

    Transformations applied:
      1. Quarantine exclusion — anti-join against quarantined row IDs
      2. Column standardization — lowercase strings, cast types, fix dates
      3. Deduplication — keep latest ingested copy of duplicate keys
      4. Customer enrichment — join billing events with customer dimensions
      5. Per-customer metrics — total_billed, total_events, avg_amount,
         dominant_service
      6. Partition by billing_month for query efficiency

Maps to (the telecom operator production):
    In production, this is a PySpark job on the operator's Hadoop/YARN cluster:
      - spark-submit --master yarn --deploy-mode cluster transform_silver.py
      - Reads from HDFS /data/bronze/
      - Writes to HDFS /data/silver/ partitioned by billing_month
      - Scheduled as an Airflow task downstream of the DQ gate

    We use PySpark locally in standalone mode. The code is portable
    to a cluster deployment with zero changes — only the SparkSession
    master URL needs to change (local[*] -> yarn).

Output:
    - data/silver/billing_enriched/    (billing + customer joined, partitioned)
    - data/silver/cdrs_cleaned/        (standardized CDRs, partitioned)
    - data/silver/customer_metrics/    (per-customer aggregates)

Usage:
    python spark/transform_silver.py
============================================================================
"""

import os
import sys
import yaml
from pathlib import Path
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    IntegerType, TimestampType, DateType
)


# ============================================================================
# CONFIGURATION
# ============================================================================

CONFIG_PATH = Path(__file__).resolve().parent.parent.parent / "config" / "pipeline_config.yaml"
with open(CONFIG_PATH, "r") as f:
    CONFIG = yaml.safe_load(f)

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
BRONZE_DIR = str(PROJECT_ROOT / CONFIG["paths"]["bronze"])
SILVER_DIR = str(PROJECT_ROOT / CONFIG["paths"]["silver"])
QUARANTINE_DIR = str(PROJECT_ROOT / CONFIG["paths"]["quarantine"])


# ============================================================================
# SPARK SESSION
# ============================================================================

def create_spark_session():
    """
    Create a SparkSession for local execution.
    Maps to: the operator's cluster SparkSession with YARN master and Hive metastore.

    In production, this would be:
      SparkSession.builder
          .master("yarn")
          .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
          .config("hive.metastore.uris", "thrift://metastore:9083")
          .enableHiveSupport()
          .getOrCreate()
    """
    spark = (
        SparkSession.builder
        .master("local[*]")
        .appName("TelecomBillingDWH_SilverTransform")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.driver.memory", "2g")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


# ============================================================================
# STEP 1: LOAD BRONZE DATA WITH QUARANTINE EXCLUSION
# ============================================================================

def load_bronze_excluding_quarantine(spark, table_name, id_col):
    """
    Load a bronze table and remove any rows present in the quarantine.
    This implements the DQ gate — only clean data passes to Silver.

    Maps to: the operator's Spark pipeline reads bronze, then does an anti-join
    against the quarantine table in Hive to exclude flagged records.
    """
    bronze_path = os.path.join(BRONZE_DIR, table_name)
    quarantine_path = os.path.join(QUARANTINE_DIR, f"{table_name}_quarantined.parquet")

    # Read bronze table
    bronze_df = spark.read.parquet(bronze_path)
    initial_count = bronze_df.count()

    # Anti-join against quarantine if quarantine file exists
    if os.path.exists(quarantine_path):
        quarantine_df = spark.read.parquet(quarantine_path).select(id_col).distinct()
        clean_df = bronze_df.join(quarantine_df, on=id_col, how="left_anti")
        excluded = initial_count - clean_df.count()
        print(f"  {table_name}: {initial_count:,} bronze rows "
              f"- {excluded:,} quarantined = {clean_df.count():,} clean")
    else:
        clean_df = bronze_df
        print(f"  {table_name}: {initial_count:,} bronze rows "
              f"(no quarantine file found)")

    return clean_df


# ============================================================================
# STEP 2: STANDARDIZE COLUMNS
# ============================================================================

def standardize_customers(df):
    """
    Standardize the customers dimension table:
      - Lowercase string columns for consistency
      - Cast balance to double
      - Parse dates to proper date type
      - Trim whitespace from all string columns

    Maps to: the operator's customer dimension conforming step before loading
    into the silver layer Hive tables.
    """
    df = (
        df
        # Trim whitespace from all string columns
        .withColumn("first_name", F.trim(F.lower(F.col("first_name"))))
        .withColumn("last_name", F.trim(F.lower(F.col("last_name"))))
        .withColumn("gender", F.trim(F.upper(F.col("gender"))))
        .withColumn("plan_type", F.trim(F.lower(F.col("plan_type"))))
        .withColumn("status", F.trim(F.lower(F.col("status"))))
        .withColumn("state", F.trim(F.initcap(F.col("state"))))

        # Cast types
        .withColumn("balance", F.col("balance").cast(DoubleType()))
        .withColumn("date_of_birth", F.to_date(F.col("date_of_birth"), "yyyy-MM-dd"))
        .withColumn("registration_date", F.to_date(F.col("registration_date"), "yyyy-MM-dd"))
    )
    return df


def standardize_billing(df):
    """
    Standardize the billing events fact table:
      - Cast amount to double for accurate arithmetic
      - Parse event_timestamp to proper timestamp type
      - Derive billing_month partition column (YYYY-MM)
      - Lowercase categorical columns

    Maps to: the operator's billing fact table conforming in the Spark ETL.
    The billing_month column enables partition pruning in Presto/DuckDB
    queries — critical for ARPU and revenue reports.
    """
    df = (
        df
        # Cast types
        .withColumn("amount", F.col("amount").cast(DoubleType()))
        .withColumn("event_timestamp",
                    F.to_timestamp(F.col("event_timestamp"), "yyyy-MM-dd HH:mm:ss"))
        .withColumn("event_date", F.to_date(F.col("event_date"), "yyyy-MM-dd"))

        # Lowercase categoricals
        .withColumn("charge_type", F.trim(F.lower(F.col("charge_type"))))
        .withColumn("service_type", F.trim(F.lower(F.col("service_type"))))
        .withColumn("channel", F.trim(F.lower(F.col("channel"))))
        .withColumn("plan_type", F.trim(F.lower(F.col("plan_type"))))
        .withColumn("status", F.trim(F.lower(F.col("status"))))
        .withColumn("currency", F.trim(F.upper(F.col("currency"))))

        # Derive billing_month partition column
        .withColumn("billing_month",
                    F.date_format(F.col("event_date"), "yyyy-MM"))
    )
    return df


def standardize_cdrs(df):
    """
    Standardize the CDR fact table:
      - Cast duration_seconds to integer, rated_amount to double
      - Parse call_start_time to proper timestamp type
      - Derive call_month partition column (YYYY-MM)
      - Lowercase categoricals

    Maps to: the operator's CDR fact table conforming step. The Huawei MSC
    outputs all fields as strings; Spark casts them to proper types
    for efficient storage and query performance.
    """
    df = (
        df
        # Cast types
        .withColumn("duration_seconds", F.col("duration_seconds").cast(IntegerType()))
        .withColumn("rated_amount", F.col("rated_amount").cast(DoubleType()))
        .withColumn("call_start_time",
                    F.to_timestamp(F.col("call_start_time"), "yyyy-MM-dd HH:mm:ss"))
        .withColumn("call_date", F.to_date(F.col("call_date"), "yyyy-MM-dd"))

        # Lowercase categoricals
        .withColumn("call_type", F.trim(F.lower(F.col("call_type"))))
        .withColumn("service_type", F.trim(F.lower(F.col("service_type"))))
        .withColumn("status", F.trim(F.lower(F.col("status"))))

        # Derive call_month partition column
        .withColumn("call_month",
                    F.date_format(F.col("call_date"), "yyyy-MM"))
    )
    return df


# ============================================================================
# STEP 3: DEDUPLICATION
# ============================================================================

def deduplicate(df, id_col):
    """
    Remove duplicate records, keeping the most recently ingested copy.
    Uses _ingested_at as the tiebreaker — latest ingestion wins.

    Maps to: the operator's dedup logic in production Spark jobs. Huawei CBS
    retry storms can produce duplicate rated events; MSC failovers
    can duplicate CDRs. Bronze stores all copies; Silver keeps only
    the latest version of each record.
    """
    window = Window.partitionBy(id_col).orderBy(F.col("_ingested_at").desc())
    deduped = (
        df
        .withColumn("_row_num", F.row_number().over(window))
        .filter(F.col("_row_num") == 1)
        .drop("_row_num")
    )
    removed = df.count() - deduped.count()
    if removed > 0:
        print(f"    Dedup: removed {removed} duplicate rows")
    return deduped


# ============================================================================
# STEP 4: ENRICH BILLING WITH CUSTOMER DATA
# ============================================================================

def enrich_billing_with_customers(billing_df, customers_df):
    """
    Join billing events with customer dimensions to create an enriched
    billing fact table. This enables downstream queries like:
      - Revenue by state (customer.state)
      - Revenue by plan_type (customer.plan_type)
      - Churn correlation with billing patterns

    Maps to: the operator's denormalized billing fact table in the silver layer.
    In production, this join runs on the YARN cluster using a broadcast
    join (customers is small enough to broadcast).

    Join key: customer_id
    Join type: LEFT — billing events with no matching customer are kept
    but flagged as enrichment failures for investigation.
    """
    # Select customer dimension columns for enrichment
    # Prefix with 'cust_' to avoid column name collisions
    customer_dims = customers_df.select(
        F.col("customer_id"),
        F.col("msisdn").alias("cust_msisdn"),
        F.col("first_name").alias("cust_first_name"),
        F.col("last_name").alias("cust_last_name"),
        F.col("gender").alias("cust_gender"),
        F.col("plan_type").alias("cust_plan_type"),
        F.col("status").alias("cust_status"),
        F.col("state").alias("cust_state"),
        F.col("registration_date").alias("cust_registration_date"),
    )

    enriched = billing_df.join(
        F.broadcast(customer_dims),
        on="customer_id",
        how="left"
    )

    # Flag rows where customer enrichment failed (no matching customer)
    enriched = enriched.withColumn(
        "_enrichment_status",
        F.when(F.col("cust_msisdn").isNull(), "unmatched").otherwise("matched")
    )

    matched = enriched.filter(F.col("_enrichment_status") == "matched").count()
    total = enriched.count()
    print(f"    Enrichment: {matched:,}/{total:,} billing events matched "
          f"to customers ({matched/max(total,1)*100:.1f}%)")

    return enriched


# ============================================================================
# STEP 5: COMPUTE PER-CUSTOMER METRICS
# ============================================================================

def compute_customer_metrics(enriched_billing_df):
    """
    Aggregate billing data to produce per-customer metrics:
      - total_billed_amount:  sum of all charges in LC
      - total_events:         count of billing events
      - avg_event_amount:     mean charge per event
      - dominant_service:     most frequently used service type

    Maps to: the operator's customer 360 profile in the silver layer.
    These metrics feed into Gold-layer KPIs like ARPU, MOU, and
    churn prediction models.

    The dominant_service calculation uses a window function to find
    the service_type with the highest count per customer — this is
    the PySpark equivalent of a mode() aggregation.
    """
    # Base aggregations
    base_metrics = (
        enriched_billing_df
        .filter(F.col("status") == "rated")
        .groupBy("customer_id")
        .agg(
            F.sum("amount").alias("total_billed_amount"),
            F.count("*").alias("total_events"),
            F.avg("amount").alias("avg_event_amount"),
            F.min("event_date").alias("first_event_date"),
            F.max("event_date").alias("last_event_date"),
            F.countDistinct("service_type").alias("distinct_services"),
        )
    )

    # Round monetary values to 2 decimal places
    base_metrics = (
        base_metrics
        .withColumn("total_billed_amount", F.round("total_billed_amount", 2))
        .withColumn("avg_event_amount", F.round("avg_event_amount", 2))
    )

    # Dominant service — find the most frequent service_type per customer
    # Uses window function: rank service types by count within each customer
    svc_counts = (
        enriched_billing_df
        .filter(F.col("status") == "rated")
        .groupBy("customer_id", "service_type")
        .agg(F.count("*").alias("svc_count"))
    )

    svc_window = Window.partitionBy("customer_id").orderBy(F.col("svc_count").desc())
    dominant_svc = (
        svc_counts
        .withColumn("rank", F.row_number().over(svc_window))
        .filter(F.col("rank") == 1)
        .select("customer_id", F.col("service_type").alias("dominant_service"))
    )

    # Join metrics with dominant service
    customer_metrics = base_metrics.join(dominant_svc, on="customer_id", how="left")

    # Add customer dimension attributes for convenience
    cust_attrs = (
        enriched_billing_df
        .select("customer_id", "cust_msisdn", "cust_plan_type",
                "cust_status", "cust_state")
        .dropDuplicates(["customer_id"])
    )

    customer_metrics = customer_metrics.join(cust_attrs, on="customer_id", how="left")

    return customer_metrics


# ============================================================================
# SILVER WRITERS
# ============================================================================

def write_silver_table(df, table_name, partition_col=None):
    """
    Write a DataFrame to the Silver layer as Parquet.
    Maps to: df.write.parquet("/data/silver/{table}", mode="overwrite",
             compression="snappy", partitionBy=partition_col)
    on the operator's HDFS cluster.
    """
    output_path = os.path.join(SILVER_DIR, table_name)

    writer = df.write.mode("overwrite").option("compression", "snappy")

    if partition_col:
        writer = writer.partitionBy(partition_col)

    writer.parquet(output_path)

    # Count output files for reporting
    file_count = len([f for f in Path(output_path).rglob("*.parquet")])
    row_count = df.count()
    print(f"  {table_name}: {row_count:,} rows, {file_count} file(s)")

    if partition_col:
        partitions = df.select(partition_col).distinct().count()
        print(f"    Partitioned by {partition_col}: {partitions} partitions")


# ============================================================================
# MAIN EXECUTION
# ============================================================================

def main():
    """
    Entry point — run the full bronze-to-silver transformation pipeline.

    Pipeline steps:
      1. Create SparkSession
      2. Load bronze data (excluding quarantined rows)
      3. Standardize column formats and types
      4. Deduplicate on primary keys
      5. Enrich billing events with customer dimensions
      6. Compute per-customer metrics
      7. Write Silver Parquet (partitioned by month)
    """
    print("=" * 70)
    print("Telecom Billing DWH -- Silver Layer Transformation (PySpark)")
    print("=" * 70)
    print(f"  Bronze:      {BRONZE_DIR}")
    print(f"  Silver:      {SILVER_DIR}")
    print(f"  Quarantine:  {QUARANTINE_DIR}")
    print("-" * 70)

    # --- Step 1: Create Spark Session ---
    print("\n[1/7] Creating SparkSession...")
    spark = create_spark_session()
    print(f"  Spark version: {spark.version}")
    print(f"  Master: {spark.sparkContext.master}")

    try:
        # --- Step 2: Load Bronze (exclude quarantine) ---
        print("\n[2/7] Loading bronze data (excluding quarantined rows)...")
        customers_raw = load_bronze_excluding_quarantine(
            spark, "customers", "customer_id"
        )
        billing_raw = load_bronze_excluding_quarantine(
            spark, "billing_events", "event_id"
        )
        cdrs_raw = load_bronze_excluding_quarantine(
            spark, "cdrs", "cdr_id"
        )

        # --- Step 3: Standardize ---
        print("\n[3/7] Standardizing columns...")
        customers_std = standardize_customers(customers_raw)
        print(f"  customers: standardized {customers_std.count():,} rows")

        billing_std = standardize_billing(billing_raw)
        print(f"  billing_events: standardized {billing_std.count():,} rows")

        cdrs_std = standardize_cdrs(cdrs_raw)
        print(f"  cdrs: standardized {cdrs_std.count():,} rows")

        # --- Step 4: Deduplicate ---
        print("\n[4/7] Deduplicating on primary keys...")
        customers_dedup = deduplicate(customers_std, "customer_id")
        billing_dedup = deduplicate(billing_std, "event_id")
        cdrs_dedup = deduplicate(cdrs_std, "cdr_id")

        # --- Step 5: Enrich billing with customer data ---
        print("\n[5/7] Enriching billing events with customer dimensions...")
        billing_enriched = enrich_billing_with_customers(
            billing_dedup, customers_dedup
        )

        # --- Step 6: Compute per-customer metrics ---
        print("\n[6/7] Computing per-customer metrics...")
        customer_metrics = compute_customer_metrics(billing_enriched)
        metrics_count = customer_metrics.count()
        print(f"  Computed metrics for {metrics_count:,} customers")

        # Show a sample of the metrics
        print("\n  Sample customer metrics:")
        customer_metrics.select(
            "customer_id", "total_billed_amount", "total_events",
            "avg_event_amount", "dominant_service"
        ).show(5, truncate=False)

        # --- Step 7: Write Silver Layer ---
        print("\n[7/7] Writing Silver Parquet tables...")
        write_silver_table(billing_enriched, "billing_enriched",
                          partition_col="billing_month")
        write_silver_table(cdrs_dedup, "cdrs_cleaned",
                          partition_col="call_month")
        write_silver_table(customer_metrics, "customer_metrics")
        write_silver_table(customers_dedup, "customers_cleaned")

        # --- Summary ---
        print("\n" + "=" * 70)
        print("Silver transformation complete!")
        print(f"  billing_enriched:  {billing_enriched.count():,} rows (partitioned by billing_month)")
        print(f"  cdrs_cleaned:      {cdrs_dedup.count():,} rows (partitioned by call_month)")
        print(f"  customer_metrics:  {metrics_count:,} rows")
        print(f"  customers_cleaned: {customers_dedup.count():,} rows")
        print(f"  Output: {SILVER_DIR}")
        print("=" * 70)

    finally:
        spark.stop()
        print("\n  SparkSession stopped.")


if __name__ == "__main__":
    main()
