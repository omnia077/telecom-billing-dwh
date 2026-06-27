"""
============================================================================
emitter.py — Micro-Batch CSV Streamer (Simulated Real-Time)
============================================================================
Responsibility:
    Simulate a near-real-time data feed by writing small CSV files
    (micro-batches) to data/landing/stream/ every 30 seconds.
    Each file contains ~100 rows of billing events or CDRs.

Maps to (the telecom operator production):
    In production, Huawei CBS pushes rated event files to an SFTP
    landing zone every few minutes. Airflow sensors detect new files
    and trigger the ingestion pipeline. This emitter replicates that
    pattern locally so we can demonstrate streaming-like behavior
    without requiring Kafka or a real SFTP server.

    Flow:  emitter.py → data/landing/stream/ → Airflow FileSensor → pipeline

Usage:
    python generation/emitter.py              # run indefinitely
    python generation/emitter.py --batches 5  # emit exactly 5 batches then stop

Press Ctrl+C to stop graceful shutdown.
============================================================================
"""

import argparse
import csv
import os
import random
import signal
import sys
import time
import uuid
import yaml
from datetime import datetime, timedelta
from pathlib import Path


# ============================================================================
# CONFIGURATION
# ============================================================================

CONFIG_PATH = Path(__file__).resolve().parent.parent.parent / "config" / "pipeline_config.yaml"
with open(CONFIG_PATH, "r") as f:
    CONFIG = yaml.safe_load(f)

BATCH_SIZE = CONFIG["emitter"]["batch_size"]              # 100 rows per batch
INTERVAL = CONFIG["emitter"]["interval_seconds"]          # 30 seconds
STREAM_DIR = Path(__file__).resolve().parent.parent.parent / CONFIG["paths"]["landing_stream"]

# Telecom domain constants
CURRENCY = CONFIG["telecom"]["currency"]
OPERATOR_PREFIXES = CONFIG["telecom"]["operator_prefixes"]
COMPETITOR_PREFIXES = CONFIG["telecom"]["competitor_prefixes"]
PLAN_TYPES = CONFIG["telecom"]["plan_types"]
SERVICE_TYPES = CONFIG["telecom"]["service_types"]
DIRTY_PCT = CONFIG["generation"]["dirty_data_pct"]

# Graceful shutdown flag
RUNNING = True


def signal_handler(sig, frame):
    """Handle Ctrl+C for graceful shutdown."""
    global RUNNING
    print("\n[emitter] Shutdown signal received. Finishing current batch...")
    RUNNING = False


signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)


# ============================================================================
# MICRO-BATCH GENERATORS
# ============================================================================

# Charge types for billing events — same as Huawei CBS output codes
CHARGE_TYPES = [
    "voice_charge", "sms_charge", "data_charge", "vas_charge",
    "roaming_charge", "subscription_fee", "bundle_purchase",
    "balance_topup", "promotional_credit", "penalty_fee"
]


def generate_phone_number(prefix=None):
    """Generate a the telecom operator format mobile number."""
    if prefix is None:
        prefix = random.choice(OPERATOR_PREFIXES)
    suffix = "".join([str(random.randint(0, 9)) for _ in range(7)])
    return f"{prefix}{suffix}"


def emit_billing_events(batch_size):
    """
    Generate a micro-batch of billing events.
    ~10% of rows are injected with dirty data to test the quality layer.
    """
    events = []
    now = datetime.utcnow()

    for _ in range(batch_size):
        is_dirty = random.random() < DIRTY_PCT
        event_dt = now - timedelta(seconds=random.randint(0, 300))

        event = {
            "event_id": str(uuid.uuid4()),
            "customer_id": str(uuid.uuid4()),
            "event_timestamp": event_dt.strftime("%Y-%m-%d %H:%M:%S"),
            "event_date": event_dt.strftime("%Y-%m-%d"),
            "charge_type": random.choice(CHARGE_TYPES),
            "service_type": random.choice(SERVICE_TYPES),
            "amount": round(random.uniform(0.50, 500.00), 2),
            "currency": CURRENCY,
            "channel": random.choice([
                "ussd", "app", "ivr", "auto_renew", "agent", "system"
            ]),
            "plan_type": random.choice(PLAN_TYPES),
            "status": random.choices(
                ["rated", "pending", "failed", "reversed"],
                weights=[0.80, 0.10, 0.05, 0.05],
                k=1
            )[0],
        }

        # --- DIRTY DATA INJECTION ---
        if is_dirty:
            corruption = random.choice([
                "negative_amount", "null_customer", "future_date"
            ])
            if corruption == "negative_amount":
                event["amount"] = round(random.uniform(-500, -0.50), 2)
            elif corruption == "null_customer":
                event["customer_id"] = ""
            elif corruption == "future_date":
                future_dt = now + timedelta(days=random.randint(30, 365))
                event["event_timestamp"] = future_dt.strftime("%Y-%m-%d %H:%M:%S")
                event["event_date"] = future_dt.strftime("%Y-%m-%d")

        events.append(event)
    return events


def emit_cdrs(batch_size):
    """
    Generate a micro-batch of CDRs.
    Simulates Huawei MSC CDR file drops with ~10% dirty records.
    """
    cdrs = []
    now = datetime.utcnow()
    tower_prefixes = ["KRT", "OMD", "PSD", "KSL", "WAD", "NYL", "ATB"]

    for _ in range(batch_size):
        is_dirty = random.random() < DIRTY_PCT
        call_dt = now - timedelta(seconds=random.randint(0, 300))
        duration = random.randint(5, 3600)

        call_type = random.choices(
            ["on_net", "off_net", "international", "roaming"],
            weights=[0.50, 0.30, 0.10, 0.10],
            k=1
        )[0]

        rate_per_sec = {
            "on_net": 0.015, "off_net": 0.025,
            "international": 0.10, "roaming": 0.08
        }

        if call_type == "off_net":
            called = generate_phone_number(prefix=random.choice(COMPETITOR_PREFIXES))
        elif call_type == "international":
            called = f"+{random.randint(1,99)}{random.randint(100000000, 999999999)}"
        else:
            called = generate_phone_number()

        cdr = {
            "cdr_id": str(uuid.uuid4()),
            "calling_msisdn": generate_phone_number(),
            "called_msisdn": called,
            "call_start_time": call_dt.strftime("%Y-%m-%d %H:%M:%S"),
            "call_date": call_dt.strftime("%Y-%m-%d"),
            "duration_seconds": duration,
            "call_type": call_type,
            "service_type": random.choices(
                ["voice", "sms", "data"],
                weights=[0.60, 0.25, 0.15],
                k=1
            )[0],
            "cell_tower_id": f"{random.choice(tower_prefixes)}-{random.randint(1000, 9999)}",
            "rated_amount": round(duration * rate_per_sec[call_type], 2),
            "status": random.choices(
                ["completed", "dropped", "failed", "busy"],
                weights=[0.85, 0.05, 0.05, 0.05],
                k=1
            )[0],
        }

        # --- DIRTY DATA INJECTION ---
        if is_dirty:
            corruption = random.choice([
                "negative_duration", "null_called", "future_date"
            ])
            if corruption == "negative_duration":
                cdr["duration_seconds"] = random.randint(-3600, -1)
                cdr["rated_amount"] = round(
                    cdr["duration_seconds"] * rate_per_sec[call_type], 2
                )
            elif corruption == "null_called":
                cdr["called_msisdn"] = ""
            elif corruption == "future_date":
                future_dt = now + timedelta(days=random.randint(30, 365))
                cdr["call_start_time"] = future_dt.strftime("%Y-%m-%d %H:%M:%S")
                cdr["call_date"] = future_dt.strftime("%Y-%m-%d")

        cdrs.append(cdr)
    return cdrs


# ============================================================================
# FILE WRITERS
# ============================================================================

BILLING_FIELDS = [
    "event_id", "customer_id", "event_timestamp", "event_date",
    "charge_type", "service_type", "amount", "currency",
    "channel", "plan_type", "status"
]

CDR_FIELDS = [
    "cdr_id", "calling_msisdn", "called_msisdn", "call_start_time",
    "call_date", "duration_seconds", "call_type", "service_type",
    "cell_tower_id", "rated_amount", "status"
]


def write_batch(records, fieldnames, data_type, batch_num):
    """
    Write a micro-batch CSV to the landing/stream/ directory.

    Filename convention: {data_type}_{timestamp}_{batch_num}.csv
    Maps to: Huawei CBS/MSC file naming conventions on SFTP.
    """
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    filename = f"{data_type}_{timestamp}_batch{batch_num:04d}.csv"
    filepath = STREAM_DIR / filename

    os.makedirs(STREAM_DIR, exist_ok=True)
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(records)

    size_kb = os.path.getsize(filepath) / 1024
    return filename, size_kb


# ============================================================================
# MAIN LOOP
# ============================================================================

def main():
    """
    Main emitter loop — continuously writes micro-batch CSVs.

    Each iteration:
      1. Randomly selects billing_events or cdrs
      2. Generates BATCH_SIZE rows with ~10% dirty data
      3. Writes CSV to data/landing/stream/
      4. Sleeps for INTERVAL seconds
      5. Repeats until --batches limit or Ctrl+C
    """
    parser = argparse.ArgumentParser(
        description="Telecom Billing Micro-Batch Emitter"
    )
    parser.add_argument(
        "--batches", type=int, default=0,
        help="Number of batches to emit (0 = run indefinitely)"
    )
    parser.add_argument(
        "--interval", type=int, default=INTERVAL,
        help=f"Seconds between batches (default: {INTERVAL})"
    )
    parser.add_argument(
        "--batch-size", type=int, default=BATCH_SIZE,
        help=f"Rows per batch (default: {BATCH_SIZE})"
    )
    args = parser.parse_args()

    interval = args.interval
    batch_size = args.batch_size
    max_batches = args.batches

    print("=" * 70)
    print("Telecom Billing DWH — Micro-Batch Emitter")
    print("=" * 70)
    print(f"  Output:     {STREAM_DIR}")
    print(f"  Interval:   {interval}s between batches")
    print(f"  Batch size: {batch_size} rows")
    print(f"  Max:        {'unlimited' if max_batches == 0 else max_batches} batches")
    print(f"  Dirty data: {DIRTY_PCT*100:.0f}%")
    print("-" * 70)
    print("Press Ctrl+C to stop.\n")

    batch_num = 0

    while RUNNING:
        batch_num += 1

        # Alternate between billing events and CDRs
        if random.random() < 0.6:
            data_type = "billing_events"
            records = emit_billing_events(batch_size)
            fields = BILLING_FIELDS
        else:
            data_type = "cdrs"
            records = emit_cdrs(batch_size)
            fields = CDR_FIELDS

        filename, size_kb = write_batch(records, fields, data_type, batch_num)

        timestamp = datetime.utcnow().strftime("%H:%M:%S")
        print(f"  [{timestamp}] Batch #{batch_num:04d} | {data_type:<16} | "
              f"{len(records)} rows | {size_kb:.1f} KB | {filename}")

        # Check if we've hit the batch limit
        if max_batches > 0 and batch_num >= max_batches:
            print(f"\n[emitter] Reached batch limit ({max_batches}). Stopping.")
            break

        # Sleep in 1-second increments so Ctrl+C is responsive
        for _ in range(interval):
            if not RUNNING:
                break
            time.sleep(1)

    print(f"\n[emitter] Done. Emitted {batch_num} batches to {STREAM_DIR}")


if __name__ == "__main__":
    main()
