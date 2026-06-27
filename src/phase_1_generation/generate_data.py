"""
============================================================================
generate_data.py — Synthetic Telecom Billing Data Generator
============================================================================
Responsibility:
    Produce realistic synthetic datasets for a telecom billing DWH:
      - customers.csv       (3,000 subscriber profiles)
      - billing_events.csv  (50,000 monthly billing transactions)
      - cdrs.csv            (20,000 call detail records)

    Injects ~10% dirty data across all datasets so the downstream
    quality layer has realistic anomalies to detect and quarantine.

Maps to (the telecom operator production):
    In production, these feeds arrive from:
      - Huawei CBS (Converged Billing System) → billing events
      - Huawei MSC/MGW (Mobile Switching Center) → CDRs
      - CRM / Subscriber Management → customer master
    This generator simulates those upstream systems for local development.

Dirty data injected (~10% of rows):
    - NULL / missing values in required fields
    - Negative amounts (impossible charges)
    - Future dates (billing events dated ahead of today)
    - Invalid phone numbers (wrong prefix, wrong length)
    - Duplicate records (same transaction_id)
    - Out-of-range values (call durations > 24h, negative durations)
    - Inconsistent foreign keys (billing events referencing non-existent customers)

Usage:
    python generation/generate_data.py
============================================================================
"""

import csv
import os
import random
import string
import uuid
import yaml
from datetime import datetime, timedelta
from pathlib import Path


# ============================================================================
# CONFIGURATION
# ============================================================================

# Load pipeline config for domain-specific values
CONFIG_PATH = Path(__file__).resolve().parent.parent.parent / "config" / "pipeline_config.yaml"
with open(CONFIG_PATH, "r") as f:
    CONFIG = yaml.safe_load(f)

# Generation parameters from config
NUM_CUSTOMERS = CONFIG["generation"]["num_customers"]            # 3,000
NUM_BILLING_EVENTS = CONFIG["generation"]["num_billing_events"]  # 50,000
NUM_CDRS = CONFIG["generation"]["num_cdrs"]                      # 20,000
DIRTY_PCT = CONFIG["generation"]["dirty_data_pct"]               # 0.10

# Telecom domain constants from config
CURRENCY = CONFIG["telecom"]["currency"]                         # LC
COUNTRY_CODE = CONFIG["telecom"]["country_code"]                 # +249
OPERATOR_PREFIXES = CONFIG["telecom"]["operator_prefixes"]       # Telecom prefixes
COMPETITOR_PREFIXES = CONFIG["telecom"]["competitor_prefixes"]    # MTN, competitor B
PLAN_TYPES = CONFIG["telecom"]["plan_types"]
SERVICE_TYPES = CONFIG["telecom"]["service_types"]

# Output directory
OUTPUT_DIR = Path(__file__).resolve().parent.parent.parent / CONFIG["paths"]["landing_batch"]

# Date range for generated events (simulate 3 months of data)
END_DATE = datetime(2025, 6, 30)
START_DATE = END_DATE - timedelta(days=90)

# Reproducibility
random.seed(42)


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def generate_phone_number(prefix=None):
    """
    Generate a realistic local mobile number.
    Format: 09XX-XXX-XXXX (10 digits total).
    Maps to: the telecom operator numbering plan under NTCA allocation.
    """
    if prefix is None:
        prefix = random.choice(OPERATOR_PREFIXES)
    suffix = "".join([str(random.randint(0, 9)) for _ in range(7)])
    return f"{prefix}{suffix}"


def generate_national_id():
    """
    Generate a placeholder national ID.
    Format: 11-digit numeric string.
    Maps to: national identification number.
    """
    return "".join([str(random.randint(0, 9)) for _ in range(11)])


def random_datetime(start, end):
    """Return a random datetime between start and end."""
    delta = end - start
    random_seconds = random.randint(0, int(delta.total_seconds()))
    return start + timedelta(seconds=random_seconds)


def inject_dirty_flag():
    """Return True for ~DIRTY_PCT of calls — marks a row for corruption."""
    return random.random() < DIRTY_PCT


# ============================================================================
# CUSTOMER GENERATION
# ============================================================================

# Realistic local names for data generation
FIRST_NAMES_MALE = [
    "Ahmed", "Mohammed", "Khalid", "Omar", "Ibrahim", "Yousif", "Hassan",
    "Ali", "Mustafa", "Abdalla", "Osman", "Babiker", "Salah", "Omer",
    "Abdelrahman", "Mahdi", "Tariq", "Jamal", "Kamal", "Nasr"
]
FIRST_NAMES_FEMALE = [
    "Fatima", "Amira", "Huda", "Mariam", "Telecomab", "Rania", "Sara",
    "Amal", "Nadia", "Samira", "Leila", "Mona", "Hala", "Dina",
    "Widad", "Nahla", "Sawsan", "Abeer", "Intisar", "Somaya"
]
LAST_NAMES = [
    "Adam", "Mohamed", "Abdullah", "Hassan", "Hussein", "Ali",
    "Ibrahim", "Osman", "Idris", "Salih", "Musa", "Issa",
    "Yousif", "Hamid", "Bashir", "Fadl", "Nour", "Bakri",
    "Mahjoub", "Suleiman"
]
STATES = [
    "Khartoum", "Omdurman", "Bahri", "Port the country", "Kassala",
    "El Obeid", "Nyala", "El Fasher", "Wad Madani", "Atbara",
    "Dongola", "Sennar", "Gedaref", "Kadugli", "Ed Damazin"
]


def generate_customers():
    """
    Generate 3,000 synthetic customer records.

    Schema mirrors a telecom CRM subscriber table:
      - customer_id:     UUID primary key
      - msisdn:          mobile number (the telecom operator format)
      - national_id:     civil registry ID
      - first_name:      subscriber first name
      - last_name:       subscriber last name
      - gender:          M/F
      - date_of_birth:   YYYY-MM-DD
      - registration_date: when the SIM was activated
      - plan_type:       prepaid / postpaid / hybrid
      - status:          active / suspended / churned
      - state:           region (geographic region)
      - balance:         current account balance in LC

    Dirty data injections (~10%):
      - Missing msisdn (NULL phone number)
      - Invalid phone prefix (e.g., 0999 instead of 0912)
      - Future registration dates
      - Negative balances on prepaid accounts
      - Missing national_id
    """
    customers = []
    dirty_count = 0

    for i in range(NUM_CUSTOMERS):
        is_dirty = inject_dirty_flag()
        gender = random.choice(["M", "F"])

        first_name = random.choice(
            FIRST_NAMES_MALE if gender == "M" else FIRST_NAMES_FEMALE
        )
        last_name = random.choice(LAST_NAMES)

        # Base clean record
        customer = {
            "customer_id": str(uuid.uuid4()),
            "msisdn": generate_phone_number(),
            "national_id": generate_national_id(),
            "first_name": first_name,
            "last_name": last_name,
            "gender": gender,
            "date_of_birth": (
                datetime(1960, 1, 1)
                + timedelta(days=random.randint(0, 365 * 45))
            ).strftime("%Y-%m-%d"),
            "registration_date": random_datetime(
                datetime(2020, 1, 1), END_DATE
            ).strftime("%Y-%m-%d"),
            "plan_type": random.choice(PLAN_TYPES),
            "status": random.choices(
                ["active", "suspended", "churned"],
                weights=[0.75, 0.15, 0.10],
                k=1
            )[0],
            "state": random.choice(STATES),
            "balance": round(random.uniform(0, 5000), 2),
        }

        # --- DIRTY DATA INJECTION ---
        if is_dirty:
            dirty_count += 1
            corruption = random.choice([
                "null_msisdn",
                "invalid_prefix",
                "future_registration",
                "negative_balance",
                "null_national_id",
            ])

            if corruption == "null_msisdn":
                customer["msisdn"] = ""
            elif corruption == "invalid_prefix":
                customer["msisdn"] = generate_phone_number(prefix="0999")
            elif corruption == "future_registration":
                future = END_DATE + timedelta(days=random.randint(30, 365))
                customer["registration_date"] = future.strftime("%Y-%m-%d")
            elif corruption == "negative_balance":
                customer["balance"] = round(random.uniform(-5000, -1), 2)
            elif corruption == "null_national_id":
                customer["national_id"] = ""

        customers.append(customer)

    print(f"  [customers] Generated {len(customers)} records "
          f"({dirty_count} dirty, {dirty_count/len(customers)*100:.1f}%)")
    return customers


# ============================================================================
# BILLING EVENTS GENERATION
# ============================================================================

CHARGE_TYPES = [
    "voice_charge", "sms_charge", "data_charge", "vas_charge",
    "roaming_charge", "subscription_fee", "bundle_purchase",
    "balance_topup", "promotional_credit", "penalty_fee"
]


def generate_billing_events(customer_ids):
    """
    Generate 50,000 billing event records.

    Schema mirrors the Huawei CBS rated-event output:
      - event_id:        UUID primary key
      - customer_id:     FK to customers table
      - event_timestamp: when the charge was applied
      - event_date:      date partition key (YYYY-MM-DD)
      - charge_type:     type of billing event
      - service_type:    voice / sms / data / vas / roaming
      - amount:          charge amount in LC
      - currency:        always LC (local currency)
      - channel:         how the charge originated
      - plan_type:       subscriber's plan at time of charge
      - status:          rated / pending / failed / reversed

    Dirty data injections (~10%):
      - Negative amounts (impossible charges)
      - Future event dates
      - NULL customer_id (orphan billing events)
      - Duplicate event_ids (simulates CBS retry storms)
      - Invalid charge types (typos / unknown codes)
    """
    events = []
    dirty_count = 0
    used_event_ids = []

    for i in range(NUM_BILLING_EVENTS):
        is_dirty = inject_dirty_flag()
        event_dt = random_datetime(START_DATE, END_DATE)
        event_id = str(uuid.uuid4())

        event = {
            "event_id": event_id,
            "customer_id": random.choice(customer_ids),
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
            dirty_count += 1
            corruption = random.choice([
                "negative_amount",
                "future_date",
                "null_customer",
                "duplicate_id",
                "invalid_charge_type",
            ])

            if corruption == "negative_amount":
                event["amount"] = round(random.uniform(-500, -0.50), 2)
            elif corruption == "future_date":
                future_dt = END_DATE + timedelta(days=random.randint(30, 365))
                event["event_timestamp"] = future_dt.strftime("%Y-%m-%d %H:%M:%S")
                event["event_date"] = future_dt.strftime("%Y-%m-%d")
            elif corruption == "null_customer":
                event["customer_id"] = ""
            elif corruption == "duplicate_id":
                if used_event_ids:
                    event["event_id"] = random.choice(used_event_ids)
            elif corruption == "invalid_charge_type":
                event["charge_type"] = random.choice([
                    "UNKNOWN_CODE", "voice_charg", "dat_charge", "##ERROR##"
                ])

        used_event_ids.append(event["event_id"])
        events.append(event)

    print(f"  [billing_events] Generated {len(events)} records "
          f"({dirty_count} dirty, {dirty_count/len(events)*100:.1f}%)")
    return events


# ============================================================================
# CDR GENERATION
# ============================================================================

def generate_cdrs(customer_msisdns):
    """
    Generate 20,000 Call Detail Records.

    Schema mirrors Huawei MSC/MGW CDR output:
      - cdr_id:            UUID primary key
      - calling_msisdn:    A-party phone number (Telecom subscriber)
      - called_msisdn:     B-party phone number (on-net or off-net)
      - call_start_time:   when the call was initiated
      - call_date:         date partition key (YYYY-MM-DD)
      - duration_seconds:  call length in seconds
      - call_type:         on_net / off_net / international / roaming
      - service_type:      voice / sms / data
      - cell_tower_id:     originating cell tower identifier
      - rated_amount:      charge applied for this CDR in LC
      - status:            completed / dropped / failed / busy

    Dirty data injections (~10%):
      - Negative call durations
      - Extremely long calls (> 86400 seconds = 24 hours)
      - Invalid calling_msisdn (wrong format)
      - NULL called_msisdn
      - Future call dates
      - Zero-duration completed calls (logical inconsistency)
    """
    cdrs = []
    dirty_count = 0

    # Cell tower IDs — simulates the operator's tower infrastructure
    tower_prefixes = ["KRT", "OMD", "PSD", "KSL", "WAD", "NYL", "ATB"]

    for i in range(NUM_CDRS):
        is_dirty = inject_dirty_flag()
        call_dt = random_datetime(START_DATE, END_DATE)
        duration = random.randint(5, 3600)  # 5 sec to 1 hour

        # Determine call type and B-party number
        call_type = random.choices(
            ["on_net", "off_net", "international", "roaming"],
            weights=[0.50, 0.30, 0.10, 0.10],
            k=1
        )[0]

        if call_type == "on_net":
            called = random.choice(customer_msisdns) if customer_msisdns else generate_phone_number()
        elif call_type == "off_net":
            called = generate_phone_number(prefix=random.choice(COMPETITOR_PREFIXES))
        elif call_type == "international":
            called = f"+{random.randint(1,99)}{random.randint(100000000, 999999999)}"
        else:  # roaming
            called = generate_phone_number()

        # Rate calculation — simplified version of Huawei CBS rating engine
        rate_per_sec = {
            "on_net": 0.015, "off_net": 0.025,
            "international": 0.10, "roaming": 0.08
        }

        cdr = {
            "cdr_id": str(uuid.uuid4()),
            "calling_msisdn": random.choice(customer_msisdns) if customer_msisdns else generate_phone_number(),
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
            dirty_count += 1
            corruption = random.choice([
                "negative_duration",
                "extreme_duration",
                "invalid_calling",
                "null_called",
                "future_date",
                "zero_duration_completed",
            ])

            if corruption == "negative_duration":
                cdr["duration_seconds"] = random.randint(-3600, -1)
                cdr["rated_amount"] = round(
                    cdr["duration_seconds"] * rate_per_sec[call_type], 2
                )
            elif corruption == "extreme_duration":
                cdr["duration_seconds"] = random.randint(86401, 200000)
                cdr["rated_amount"] = round(
                    cdr["duration_seconds"] * rate_per_sec[call_type], 2
                )
            elif corruption == "invalid_calling":
                cdr["calling_msisdn"] = "".join(
                    random.choices(string.ascii_letters, k=8)
                )
            elif corruption == "null_called":
                cdr["called_msisdn"] = ""
            elif corruption == "future_date":
                future_dt = END_DATE + timedelta(days=random.randint(30, 365))
                cdr["call_start_time"] = future_dt.strftime("%Y-%m-%d %H:%M:%S")
                cdr["call_date"] = future_dt.strftime("%Y-%m-%d")
            elif corruption == "zero_duration_completed":
                cdr["duration_seconds"] = 0
                cdr["rated_amount"] = 0.00
                cdr["status"] = "completed"

        cdrs.append(cdr)

    print(f"  [cdrs] Generated {len(cdrs)} records "
          f"({dirty_count} dirty, {dirty_count/len(cdrs)*100:.1f}%)")
    return cdrs


# ============================================================================
# CSV WRITERS
# ============================================================================

def write_csv(filepath, records, fieldnames):
    """Write a list of dicts to CSV with explicit field ordering."""
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(records)
    size_mb = os.path.getsize(filepath) / (1024 * 1024)
    print(f"  [write] {filepath} ({size_mb:.2f} MB)")


# ============================================================================
# MAIN EXECUTION
# ============================================================================

def main():
    """
    Entry point — generate all three datasets and write to landing/batch/.
    This simulates a full initial load from the operator's upstream systems.
    """
    print("=" * 70)
    print("Telecom Billing DWH — Synthetic Data Generator")
    print("=" * 70)
    print(f"Target: {NUM_CUSTOMERS} customers, {NUM_BILLING_EVENTS} billing events, "
          f"{NUM_CDRS} CDRs")
    print(f"Dirty data injection rate: {DIRTY_PCT*100:.0f}%")
    print(f"Output directory: {OUTPUT_DIR}")
    print("-" * 70)

    # --- Step 1: Generate Customers ---
    print("\n[1/3] Generating customer records...")
    customers = generate_customers()
    customer_ids = [c["customer_id"] for c in customers]
    customer_msisdns = [c["msisdn"] for c in customers if c["msisdn"]]

    # --- Step 2: Generate Billing Events ---
    print("\n[2/3] Generating billing events...")
    billing_events = generate_billing_events(customer_ids)

    # --- Step 3: Generate CDRs ---
    print("\n[3/3] Generating CDRs...")
    cdrs = generate_cdrs(customer_msisdns)

    # --- Step 4: Write to CSV ---
    print("\n[Writing CSVs to landing/batch/]")

    write_csv(
        OUTPUT_DIR / "customers.csv",
        customers,
        fieldnames=[
            "customer_id", "msisdn", "national_id", "first_name", "last_name",
            "gender", "date_of_birth", "registration_date", "plan_type",
            "status", "state", "balance"
        ]
    )

    write_csv(
        OUTPUT_DIR / "billing_events.csv",
        billing_events,
        fieldnames=[
            "event_id", "customer_id", "event_timestamp", "event_date",
            "charge_type", "service_type", "amount", "currency",
            "channel", "plan_type", "status"
        ]
    )

    write_csv(
        OUTPUT_DIR / "cdrs.csv",
        cdrs,
        fieldnames=[
            "cdr_id", "calling_msisdn", "called_msisdn", "call_start_time",
            "call_date", "duration_seconds", "call_type", "service_type",
            "cell_tower_id", "rated_amount", "status"
        ]
    )

    # --- Summary ---
    print("\n" + "=" * 70)
    print("Generation complete!")
    print(f"  Customers:      {len(customers):>10,}")
    print(f"  Billing Events: {len(billing_events):>10,}")
    print(f"  CDRs:           {len(cdrs):>10,}")
    print("=" * 70)


if __name__ == "__main__":
    main()
