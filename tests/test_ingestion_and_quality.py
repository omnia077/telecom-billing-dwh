"""
Tests — Phase 2 (Bronze Ingestion) + Phase 3 (Data Quality)

Phase 2 covers:
  - detect_table_name     : filename → bronze table routing
  - add_metadata_columns  : lineage columns added with correct structure
  - validate_schema       : schema drift detection (log-only, non-blocking)

Phase 3 covers:
  - QualityReport         : add, get_quarantine_ids, to_dataframe
  - check_completeness    : null/empty detection on required columns
  - check_uniqueness      : duplicate primary key detection
  - check_validity_customers : msisdn format, balance, plan type, reg date
  - check_validity_billing   : negative amount, charge type, timestamp
  - check_validity_cdrs      : duration, rated amount, completed call logic
  - check_timeliness         : stale records, future-dated, dimension skip
"""

import sys
import pandas as pd
import pytest
from datetime import datetime, timedelta, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(PROJECT_ROOT / "src/phase_2_ingestion"))
sys.path.insert(0, str(PROJECT_ROOT / "src/phase_3_quality"))

from ingest_bronze import (
    detect_table_name,
    add_metadata_columns,
    validate_schema,
    EXPECTED_SCHEMAS,
)
from quality_checks import (
    QualityReport,
    check_completeness,
    check_uniqueness,
    check_validity_customers,
    check_validity_billing,
    check_validity_cdrs,
    check_timeliness,
    VALID_PLAN_TYPES,
    VALID_CHARGE_TYPES,
    REFERENCE_DATE,
)


# ============================================================================
# PHASE 2 — BRONZE INGESTION
# ============================================================================

class TestDetectTableName:

    def test_customers_file(self):
        assert detect_table_name("customers_20250601.csv") == "customers"

    def test_billing_events_file(self):
        assert detect_table_name("billing_events_stream_001.csv") == "billing_events"

    def test_cdrs_file(self):
        assert detect_table_name("cdrs_20250601_batch.csv") == "cdrs"

    def test_case_insensitive(self):
        assert detect_table_name("BILLING_EVENTS_001.CSV") == "billing_events"

    def test_unknown_file_returns_none(self):
        assert detect_table_name("unknown_feed_001.csv") is None

    def test_empty_filename_returns_none(self):
        assert detect_table_name("") is None

    def test_partial_match_billing(self):
        assert detect_table_name("billing_events_rated_20250601.csv") == "billing_events"


class TestAddMetadataColumns:

    def _sample_df(self):
        return pd.DataFrame({"event_id": ["evt_001", "evt_002"], "amount": ["10.0", "20.0"]})

    def test_columns_added(self):
        df = add_metadata_columns(self._sample_df(), "test_file.csv", "batch-uuid-1")
        assert "_ingested_at" in df.columns
        assert "_source_file" in df.columns
        assert "_batch_id" in df.columns

    def test_source_file_value(self):
        df = add_metadata_columns(self._sample_df(), "billing_events_001.csv", "abc")
        assert (df["_source_file"] == "billing_events_001.csv").all()

    def test_batch_id_value(self):
        df = add_metadata_columns(self._sample_df(), "file.csv", "test-batch-123")
        assert (df["_batch_id"] == "test-batch-123").all()

    def test_ingested_at_is_utc_string(self):
        df = add_metadata_columns(self._sample_df(), "file.csv", "batch-1")
        ts = pd.to_datetime(df["_ingested_at"].iloc[0])
        assert ts is not None

    def test_original_columns_preserved(self):
        df = add_metadata_columns(self._sample_df(), "f.csv", "b")
        assert "event_id" in df.columns
        assert "amount" in df.columns

    def test_row_count_unchanged(self):
        original = self._sample_df()
        result = add_metadata_columns(original.copy(), "f.csv", "b")
        assert len(result) == len(original)


class TestValidateSchema:

    def _customers_df(self):
        return pd.DataFrame({col: ["val"] for col in EXPECTED_SCHEMAS["customers"]})

    def test_no_output_on_matching_schema(self, capsys):
        validate_schema(self._customers_df(), "customers", "customers_001.csv")
        assert "SCHEMA DRIFT" not in capsys.readouterr().out

    def test_missing_column_logged(self, capsys):
        df = self._customers_df().drop(columns=["msisdn"])
        validate_schema(df, "customers", "customers_001.csv")
        captured = capsys.readouterr().out
        assert "SCHEMA DRIFT" in captured and "msisdn" in captured

    def test_extra_column_logged(self, capsys):
        df = self._customers_df()
        df["unexpected_column"] = "extra"
        validate_schema(df, "customers", "customers_001.csv")
        captured = capsys.readouterr().out
        assert "SCHEMA DRIFT" in captured and "unexpected_column" in captured

    def test_metadata_columns_not_flagged_as_extra(self, capsys):
        df = self._customers_df()
        df["_ingested_at"] = "2025-06-01"
        validate_schema(df, "customers", "customers_001.csv")
        assert "_ingested_at" not in capsys.readouterr().out

    def test_unknown_table_warns(self, capsys):
        validate_schema(pd.DataFrame({"col": [1]}), "unknown_table", "f.csv")
        assert "WARN" in capsys.readouterr().out

    def test_non_blocking_on_missing_column(self):
        try:
            validate_schema(pd.DataFrame({"only_col": ["x"]}), "customers", "bad.csv")
        except Exception as e:
            pytest.fail(f"validate_schema raised unexpectedly: {e}")


# ============================================================================
# PHASE 3 — DATA QUALITY
# ============================================================================

class TestQualityReport:

    def test_add_and_length(self):
        r = QualityReport()
        r.add("customers", "completeness", "null_check_id", 100, 5, ["c1", "c2"])
        assert len(r.entries) == 1

    def test_pass_rate_calculation(self):
        r = QualityReport()
        r.add("billing_events", "validity", "negative_amount", 200, 20, [])
        assert r.entries[0]["pass_rate"] == pytest.approx(90.0)

    def test_pass_rate_zero_rows(self):
        r = QualityReport()
        r.add("cdrs", "completeness", "null_check", 0, 0, [])
        assert r.entries[0]["pass_rate"] == 100.0

    def test_get_quarantine_ids_single_rule(self):
        r = QualityReport()
        r.add("customers", "completeness", "null_check_id", 10, 3, ["c1", "c2", "c3"])
        assert r.get_quarantine_ids("customers") == {"c1", "c2", "c3"}

    def test_get_quarantine_ids_union_across_rules(self):
        r = QualityReport()
        r.add("customers", "completeness", "rule_a", 10, 2, ["c1", "c2"])
        r.add("customers", "validity",    "rule_b", 10, 2, ["c2", "c3"])
        assert r.get_quarantine_ids("customers") == {"c1", "c2", "c3"}

    def test_get_quarantine_ids_wrong_dataset(self):
        r = QualityReport()
        r.add("customers", "completeness", "rule_a", 10, 2, ["c1"])
        assert r.get_quarantine_ids("billing_events") == set()

    def test_to_dataframe_drops_failed_row_ids(self):
        r = QualityReport()
        r.add("customers", "completeness", "rule_a", 10, 1, ["c1"])
        assert "failed_row_ids" not in r.to_dataframe().columns

    def test_to_dataframe_columns(self):
        r = QualityReport()
        r.add("cdrs", "uniqueness", "duplicate_cdr_id", 50, 2, ["x"])
        df = r.to_dataframe()
        for col in ["dataset", "category", "rule_name", "total_rows", "failed_rows", "pass_rate"]:
            assert col in df.columns


class TestCheckCompleteness:

    def test_detects_null(self):
        df = pd.DataFrame({"event_id": ["e1","e2","e3"], "customer_id": ["c1",None,"c3"], "amount": ["10","20",""]})
        r = QualityReport()
        check_completeness(df, "billing_events", "event_id", required_cols=["customer_id"], report=r)
        assert "e2" in r.get_quarantine_ids("billing_events")

    def test_detects_empty_string(self):
        df = pd.DataFrame({"event_id": ["e1","e2","e3"], "customer_id": ["c1",None,"c3"], "amount": ["10","20",""]})
        r = QualityReport()
        check_completeness(df, "billing_events", "event_id", required_cols=["amount"], report=r)
        assert "e3" in r.get_quarantine_ids("billing_events")

    def test_clean_column_passes(self):
        df = pd.DataFrame({"event_id": ["e1","e2"], "amount": ["10","20"]})
        r = QualityReport()
        check_completeness(df, "billing_events", "event_id", required_cols=["amount"], report=r)
        assert r.entries[0]["failed_rows"] == 0

    def test_missing_column_flags_all_rows(self):
        df = pd.DataFrame({"event_id": ["e1","e2"]})
        r = QualityReport()
        check_completeness(df, "billing_events", "event_id", required_cols=["nonexistent_col"], report=r)
        assert r.entries[0]["failed_rows"] == 2


class TestCheckUniqueness:

    def test_no_duplicates_passes(self):
        df = pd.DataFrame({"event_id": ["e1","e2","e3"], "val": [1,2,3]})
        r = QualityReport()
        check_uniqueness(df, "billing_events", "event_id", r)
        assert r.entries[0]["failed_rows"] == 0

    def test_duplicate_detected(self):
        df = pd.DataFrame({"event_id": ["e1","e1","e2"], "val": [1,1,2]})
        r = QualityReport()
        check_uniqueness(df, "billing_events", "event_id", r)
        assert r.entries[0]["failed_rows"] == 2

    def test_all_duplicates(self):
        df = pd.DataFrame({"cdr_id": ["c1","c1","c1"]})
        r = QualityReport()
        check_uniqueness(df, "cdrs", "cdr_id", r)
        assert r.entries[0]["failed_rows"] == 3


class TestCheckValidityCustomers:

    def _row(self, **kw):
        base = {"customer_id":"cust_001","msisdn":"09123456789","balance":"100.0",
                "plan_type":"prepaid","registration_date":"2023-01-01"}
        return pd.DataFrame([{**base, **kw}])

    def test_valid_msisdn_passes(self):
        r = QualityReport()
        check_validity_customers(self._row(), r)
        entry = next(e for e in r.entries if e["rule_name"] == "msisdn_format")
        assert entry["failed_rows"] == 0

    def test_invalid_msisdn_flagged(self):
        r = QualityReport()
        check_validity_customers(self._row(msisdn="0012345678"), r)
        entry = next(e for e in r.entries if e["rule_name"] == "msisdn_format")
        assert entry["failed_rows"] == 1

    def test_short_msisdn_flagged(self):
        r = QualityReport()
        check_validity_customers(self._row(msisdn="09123"), r)
        entry = next(e for e in r.entries if e["rule_name"] == "msisdn_format")
        assert entry["failed_rows"] == 1

    def test_negative_prepaid_balance_flagged(self):
        r = QualityReport()
        check_validity_customers(self._row(balance="-50.0", plan_type="prepaid"), r)
        entry = next(e for e in r.entries if e["rule_name"] == "negative_prepaid_balance")
        assert entry["failed_rows"] == 1

    def test_negative_postpaid_balance_allowed(self):
        r = QualityReport()
        check_validity_customers(self._row(balance="-50.0", plan_type="postpaid"), r)
        entry = next(e for e in r.entries if e["rule_name"] == "negative_prepaid_balance")
        assert entry["failed_rows"] == 0

    def test_invalid_plan_type_flagged(self):
        r = QualityReport()
        check_validity_customers(self._row(plan_type="enterprise"), r)
        entry = next(e for e in r.entries if e["rule_name"] == "plan_type_valid")
        assert entry["failed_rows"] == 1

    def test_future_registration_date_flagged(self):
        future = (REFERENCE_DATE + timedelta(days=30)).strftime("%Y-%m-%d")
        r = QualityReport()
        check_validity_customers(self._row(registration_date=future), r)
        entry = next(e for e in r.entries if e["rule_name"] == "future_registration_date")
        assert entry["failed_rows"] == 1

    def test_past_registration_date_passes(self):
        r = QualityReport()
        check_validity_customers(self._row(registration_date="2020-06-01"), r)
        entry = next(e for e in r.entries if e["rule_name"] == "future_registration_date")
        assert entry["failed_rows"] == 0


class TestCheckValidityBilling:

    def _row(self, **kw):
        base = {"event_id":"evt_001","amount":"15.0","charge_type":"voice_charge",
                "event_timestamp":"2025-06-01 10:00:00","service_type":"voice"}
        return pd.DataFrame([{**base, **kw}])

    def test_valid_event_passes_all(self):
        r = QualityReport()
        check_validity_billing(self._row(), r)
        for e in r.entries:
            assert e["failed_rows"] == 0, f"Rule {e['rule_name']} failed unexpectedly"

    def test_negative_amount_flagged(self):
        r = QualityReport()
        check_validity_billing(self._row(amount="-5.0"), r)
        entry = next(e for e in r.entries if e["rule_name"] == "negative_amount")
        assert entry["failed_rows"] == 1

    def test_invalid_charge_type_flagged(self):
        r = QualityReport()
        check_validity_billing(self._row(charge_type="mystery_charge"), r)
        entry = next(e for e in r.entries if e["rule_name"] == "charge_type_valid")
        assert entry["failed_rows"] == 1

    def test_unparseable_timestamp_flagged(self):
        r = QualityReport()
        check_validity_billing(self._row(event_timestamp="not-a-date"), r)
        entry = next(e for e in r.entries if e["rule_name"] == "event_timestamp_parseable")
        assert entry["failed_rows"] == 1

    def test_invalid_service_type_flagged(self):
        r = QualityReport()
        check_validity_billing(self._row(service_type="carrier_pigeon"), r)
        entry = next(e for e in r.entries if e["rule_name"] == "service_type_valid")
        assert entry["failed_rows"] == 1

    def test_zero_amount_passes(self):
        r = QualityReport()
        check_validity_billing(self._row(amount="0.0"), r)
        entry = next(e for e in r.entries if e["rule_name"] == "negative_amount")
        assert entry["failed_rows"] == 0


class TestCheckValidityCdrs:

    def _row(self, **kw):
        base = {"cdr_id":"cdr_001","duration_seconds":"180","rated_amount":"12.5",
                "calling_msisdn":"09123456789","status":"completed"}
        return pd.DataFrame([{**base, **kw}])

    def test_valid_cdr_passes_all(self):
        r = QualityReport()
        check_validity_cdrs(self._row(), r)
        for e in r.entries:
            assert e["failed_rows"] == 0, f"Rule {e['rule_name']} failed unexpectedly"

    def test_negative_duration_flagged(self):
        r = QualityReport()
        check_validity_cdrs(self._row(duration_seconds="-10"), r)
        entry = next(e for e in r.entries if e["rule_name"] == "negative_duration")
        assert entry["failed_rows"] == 1

    def test_extreme_duration_flagged(self):
        r = QualityReport()
        check_validity_cdrs(self._row(duration_seconds="90000"), r)
        entry = next(e for e in r.entries if e["rule_name"] == "extreme_duration_gt_24h")
        assert entry["failed_rows"] == 1

    def test_exactly_24h_duration_passes(self):
        r = QualityReport()
        check_validity_cdrs(self._row(duration_seconds="86400"), r)
        entry = next(e for e in r.entries if e["rule_name"] == "extreme_duration_gt_24h")
        assert entry["failed_rows"] == 0

    def test_negative_rated_amount_flagged(self):
        r = QualityReport()
        check_validity_cdrs(self._row(rated_amount="-1.0"), r)
        entry = next(e for e in r.entries if e["rule_name"] == "negative_rated_amount")
        assert entry["failed_rows"] == 1

    def test_zero_duration_completed_call_flagged(self):
        r = QualityReport()
        check_validity_cdrs(self._row(duration_seconds="0", status="completed"), r)
        entry = next(e for e in r.entries if e["rule_name"] == "zero_duration_completed_call")
        assert entry["failed_rows"] == 1

    def test_zero_duration_failed_call_allowed(self):
        r = QualityReport()
        check_validity_cdrs(self._row(duration_seconds="0", status="failed"), r)
        entry = next(e for e in r.entries if e["rule_name"] == "zero_duration_completed_call")
        assert entry["failed_rows"] == 0


class TestCheckTimeliness:

    def _df(self, date_str):
        return pd.DataFrame({"event_id": ["e1"], "event_date": [date_str]})

    def test_recent_date_passes(self):
        recent = (REFERENCE_DATE - timedelta(days=10)).strftime("%Y-%m-%d")
        r = QualityReport()
        check_timeliness(self._df(recent), "billing_events", "event_id", "event_date", r)
        stale = next(e for e in r.entries if "stale" in e["rule_name"])
        assert stale["failed_rows"] == 0

    def test_stale_date_flagged(self):
        stale_date = (REFERENCE_DATE - timedelta(days=200)).strftime("%Y-%m-%d")
        r = QualityReport()
        check_timeliness(self._df(stale_date), "billing_events", "event_id", "event_date", r)
        stale = next(e for e in r.entries if "stale" in e["rule_name"])
        assert stale["failed_rows"] == 1

    def test_future_date_flagged(self):
        future = (REFERENCE_DATE + timedelta(days=30)).strftime("%Y-%m-%d")
        r = QualityReport()
        check_timeliness(self._df(future), "billing_events", "event_id", "event_date", r)
        future_e = next(e for e in r.entries if "future" in e["rule_name"])
        assert future_e["failed_rows"] == 1

    def test_dimension_table_skips_stale_check(self):
        r = QualityReport()
        check_timeliness(self._df("2010-01-01"), "customers", "event_id", "event_date", r,
                         is_dimension=True)
        assert not any("stale" in e["rule_name"] for e in r.entries)
