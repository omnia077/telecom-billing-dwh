"""
============================================================================
ingest_bronze.py — Landing CSV → Bronze Parquet Ingestion
============================================================================
Responsibility:
    Read raw CSV files from data/landing/ (batch and stream) and write
    them as Parquet files to data/bronze/. This is a pure format conversion
    with NO data transformations — the bronze layer is an append-only,
    immutable copy of raw data.

    Adds three metadata columns to every row:
      - _ingested_at:  UTC timestamp when the row entered bronze
      - _source_file:  original CSV filename for lineage tracking
      - _batch_id:     UUID identifying this ingestion run

Maps to (the telecom operator production):
    In production, this would be a PySpark job running on the operator's
    Hadoop/YARN cluster:
      - Reads from HDFS landing zone (files dropped by Huawei CBS SFTP)
      - Writes Snappy-compressed Parquet to HDFS bronze zone
      - Partitioned by event_date / call_date for query efficiency
      - Triggered by Airflow FileSensor detecting new files

    Here we use pandas + pyarrow locally. The logic is identical —
    only the execution engine differs (pandas vs. Spark DataFrame).

Design decisions:
    - Append-only: never overwrites existing bronze Parquet files
    - No dedup: duplicates pass through — cleaned in Silver layer
    - No type casting: all columns preserved as-is from CSV
    - Source tracking: _source_file enables lineage audits
    - Batch tracking: _batch_id groups rows ingested together

Usage:
    python ingestion/ingest_bronze.py                    # ingest all
    python ingestion/ingest_bronze.py --source batch     # batch only
    python ingestion/ingest_bronze.py --source stream    # stream only
============================================================================
"""

import argparse
import os
import uuid
import yaml
import pandas as pd
from datetime import datetime, timezone
from pathlib import Path


# ============================================================================
# CONFIGURATION
# ============================================================================

CONFIG_PATH = Path(__file__).resolve().parent.parent.parent / "config" / "pipeline_config.yaml"
with open(CONFIG_PATH, "r") as f:
    CONFIG = yaml.safe_load(f)

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent

# Layer paths
LANDING_BATCH = PROJECT_ROOT / CONFIG["paths"]["landing_batch"]
LANDING_STREAM = PROJECT_ROOT / CONFIG["paths"]["landing_stream"]
BRONZE_DIR = PROJECT_ROOT / CONFIG["paths"]["bronze"]

# Bronze settings
COMPRESSION = CONFIG["bronze"]["compression"]          # snappy
PARTITION_COLS = CONFIG["bronze"]["partition_cols"]     # per-table partition keys

# File-to-table mapping — determines which bronze subdirectory each file goes to
# Maps to: the operator's ingestion routing rules that map CBS file patterns to HDFS paths
FILE_TABLE_MAP = {
    "customers": "customers",
    "billing_events": "billing_events",
    "cdrs": "cdrs",
}


# ============================================================================
# SCHEMA DEFINITIONS
# ============================================================================

# Expected columns per dataset — used for validation, not enforcement.
# Bronze layer accepts all columns; this is for logging mismatches.
# Maps to: the operator's schema registry (Confluent Schema Registry / Hive Metastore)
EXPECTED_SCHEMAS = {
    "customers": [
        "customer_id", "msisdn", "national_id", "first_name", "last_name",
        "gender", "date_of_birth", "registration_date", "plan_type",
        "status", "state", "balance"
    ],
    "billing_events": [
        "event_id", "customer_id", "event_timestamp", "event_date",
        "charge_type", "service_type", "amount", "currency",
        "channel", "plan_type", "status"
    ],
    "cdrs": [
        "cdr_id", "calling_msisdn", "called_msisdn", "call_start_time",
        "call_date", "duration_seconds", "call_type", "service_type",
        "cell_tower_id", "rated_amount", "status"
    ],
}


# ============================================================================
# CORE INGESTION LOGIC
# ============================================================================

def detect_table_name(filename):
    """
    Determine which bronze table a CSV file belongs to based on filename.
    Maps to: the operator's file routing rules — CBS files are prefixed with
    the source system and record type (e.g., CBS_RATED_EVENT_20250601.csv).
    """
    fname_lower = filename.lower()
    for pattern, table in FILE_TABLE_MAP.items():
        if pattern in fname_lower:
            return table
    return None


def add_metadata_columns(df, source_file, batch_id):
    """
    Add audit/lineage metadata columns to every row.
    These columns are critical for:
      - _ingested_at:  debugging late-arriving data, SLA tracking
      - _source_file:  lineage audits, reprocessing specific files
      - _batch_id:     correlating rows ingested in the same run
    """
    df["_ingested_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    df["_source_file"] = source_file
    df["_batch_id"] = batch_id
    return df


def validate_schema(df, table_name, source_file):
    """
    Log schema drift between the CSV and expected columns.
    Does NOT block ingestion — bronze accepts everything.
    Maps to: the operator's schema evolution monitoring in production.
    """
    if table_name not in EXPECTED_SCHEMAS:
        print(f"    [WARN] No expected schema for table '{table_name}'")
        return

    expected = set(EXPECTED_SCHEMAS[table_name])
    actual = set(df.columns) - {"_ingested_at", "_source_file", "_batch_id"}

    missing = expected - actual
    extra = actual - expected

    if missing:
        print(f"    [SCHEMA DRIFT] {source_file}: missing columns {missing}")
    if extra:
        print(f"    [SCHEMA DRIFT] {source_file}: unexpected columns {extra}")


def ingest_csv_to_parquet(csv_path, batch_id):
    """
    Ingest a single CSV file into the bronze layer as Parquet.

    Steps:
      1. Read CSV with pandas (all columns as strings to preserve raw data)
      2. Detect target bronze table from filename
      3. Validate schema (log-only, non-blocking)
      4. Add metadata columns (_ingested_at, _source_file, _batch_id)
      5. Write Parquet with Snappy compression
      6. Return row count for summary reporting

    Maps to: A single Spark job reading one CBS SFTP file into HDFS.
    """
    filename = os.path.basename(csv_path)
    table_name = detect_table_name(filename)

    if table_name is None:
        print(f"    [SKIP] Cannot determine table for: {filename}")
        return 0

    # Read CSV — dtype=str preserves raw data exactly as received
    # In production Spark: spark.read.csv(path, header=True, inferSchema=False)
    df = pd.read_csv(csv_path, dtype=str)

    if df.empty:
        print(f"    [SKIP] Empty file: {filename}")
        return 0

    # Schema validation (log-only)
    validate_schema(df, table_name, filename)

    # Add metadata columns for lineage
    df = add_metadata_columns(df, filename, batch_id)

    # Determine output path — append-only pattern using batch_id in filename
    # Maps to: HDFS path like /data/bronze/billing_events/batch_id=<uuid>/part-00000.parquet
    output_dir = BRONZE_DIR / table_name
    os.makedirs(output_dir, exist_ok=True)

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    output_file = output_dir / f"{table_name}_{timestamp}_{batch_id[:8]}.parquet"

    # Write Parquet — Snappy compression matches the operator's Spark defaults
    # In production: df.write.parquet(path, mode="append", compression="snappy")
    df.to_parquet(output_file, engine="pyarrow", compression=COMPRESSION, index=False)

    size_kb = os.path.getsize(output_file) / 1024
    print(f"    [OK] {filename} -> {table_name}/ "
          f"({len(df)} rows, {size_kb:.1f} KB)")

    return len(df)


def ingest_directory(landing_dir, source_label):
    """
    Ingest all CSV files from a landing directory into bronze.

    Each call generates a unique batch_id so all files ingested
    together can be correlated in downstream audits.

    Maps to: Airflow task that processes all new files in a landing zone.
    """
    batch_id = str(uuid.uuid4())
    csv_files = sorted(Path(landing_dir).glob("*.csv"))

    if not csv_files:
        print(f"  [{source_label}] No CSV files found in {landing_dir}")
        return 0, 0

    print(f"  [{source_label}] Found {len(csv_files)} CSV files "
          f"(batch_id: {batch_id[:8]}...)")

    total_rows = 0
    files_processed = 0

    for csv_file in csv_files:
        rows = ingest_csv_to_parquet(str(csv_file), batch_id)
        if rows > 0:
            total_rows += rows
            files_processed += 1

    return files_processed, total_rows


def move_processed_files(landing_dir, archive=False):
    """
    After successful ingestion, either delete or archive source CSVs.

    Maps to: the operator's post-ingestion file management:
      - Production: files are moved to an archive directory on HDFS
      - Here: we delete them to keep the landing zone clean for the
        next emitter cycle. Set archive=True to move instead.
    """
    csv_files = list(Path(landing_dir).glob("*.csv"))
    for f in csv_files:
        if archive:
            archive_dir = Path(landing_dir) / "_processed"
            os.makedirs(archive_dir, exist_ok=True)
            f.rename(archive_dir / f.name)
        else:
            f.unlink()

    if csv_files:
        action = "archived" if archive else "cleaned up"
        print(f"    [{action}] {len(csv_files)} source files")


# ============================================================================
# MAIN EXECUTION
# ============================================================================

def main():
    """
    Entry point — ingest CSVs from landing zone(s) into bronze Parquet.

    Supports two modes:
      --source batch   : ingest from data/landing/batch/ (initial loads)
      --source stream  : ingest from data/landing/stream/ (emitter output)
      --source all     : ingest from both (default)
    """
    parser = argparse.ArgumentParser(
        description="Landing CSV → Bronze Parquet Ingestion"
    )
    parser.add_argument(
        "--source", choices=["batch", "stream", "all"], default="all",
        help="Which landing zone to ingest from (default: all)"
    )
    parser.add_argument(
        "--archive", action="store_true",
        help="Archive processed CSVs instead of deleting them"
    )
    parser.add_argument(
        "--no-cleanup", action="store_true",
        help="Leave source CSVs in place after ingestion"
    )
    args = parser.parse_args()

    print("=" * 70)
    print("Telecom Billing DWH — Bronze Ingestion Layer")
    print("=" * 70)
    print(f"  Source:      {args.source}")
    print(f"  Bronze dir:  {BRONZE_DIR}")
    print(f"  Compression: {COMPRESSION}")
    print(f"  Cleanup:     {'archive' if args.archive else 'delete' if not args.no_cleanup else 'none'}")
    print("-" * 70)

    total_files = 0
    total_rows = 0

    # --- Ingest Batch Landing ---
    if args.source in ("batch", "all"):
        print("\n[Batch Ingestion]")
        files, rows = ingest_directory(LANDING_BATCH, "batch")
        total_files += files
        total_rows += rows
        if not args.no_cleanup and files > 0:
            move_processed_files(LANDING_BATCH, archive=args.archive)

    # --- Ingest Stream Landing ---
    if args.source in ("stream", "all"):
        print("\n[Stream Ingestion]")
        files, rows = ingest_directory(LANDING_STREAM, "stream")
        total_files += files
        total_rows += rows
        if not args.no_cleanup and files > 0:
            move_processed_files(LANDING_STREAM, archive=args.archive)

    # --- Summary ---
    print("\n" + "=" * 70)
    print("Bronze ingestion complete!")
    print(f"  Files processed: {total_files}")
    print(f"  Total rows:      {total_rows:,}")
    print(f"  Output:          {BRONZE_DIR}")
    print("=" * 70)


if __name__ == "__main__":
    main()
