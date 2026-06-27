"""
Tests — Phase 6 (KPI Mart) + Phase 8 (Monitoring)

Phase 6 covers:
  - build_kpi_summary : output structure, row count, KPI values, status logic
  - Reconciliation WARNING when rate < 90%
  - Empty-input handling

Phase 8 covers:
  - compute_metrics : HEALTHY / WARNING / CRITICAL status thresholds
  - Pass rate, quarantine rate, weighted average across datasets
  - Staleness detection (no run in last 2 hours → CRITICAL)
  - Per-dataset breakdown, rows ingested today
"""

import sys
import pandas as pd
import pytest
from datetime import datetime, timedelta, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(PROJECT_ROOT / "src/phase_6_mart"))
sys.path.insert(0, str(PROJECT_ROOT / "src/phase_8_monitoring"))

from financial_mart import build_kpi_summary
from pipeline_logger import (
    compute_metrics,
    HEALTHY_THRESHOLD,
    WARNING_THRESHOLD,
    STALE_HOURS,
)


# ============================================================================
# PHASE 6 — KPI MART
# ============================================================================

def _arpu_df():
    return pd.DataFrame([
        {"billing_month":"2025-04","region":"City A","governorate":"Gov A",
         "active_subscribers":300,"total_revenue":90000.0,"arpu":300.0},
        {"billing_month":"2025-05","region":"City B","governorate":"Gov B",
         "active_subscribers":200,"total_revenue":60000.0,"arpu":300.0},
    ])

def _mou_df():
    return pd.DataFrame([
        {"billing_month":"2025-04","service_type":"voice","service_category":"communication",
         "total_mou":50000.0,"event_count":8000,"avg_mou_per_event":6.25},
    ])

def _revenue_df():
    return pd.DataFrame([
        {"plan_name":"Operator Prepaid","plan_type":"prepaid","plan_tier":"basic",
         "governorate":"Gov A","zone":"Central","billing_events":20000,
         "subscribers":1000,"total_revenue":150000.0,
         "avg_event_amount":7.5,"revenue_per_subscriber":150.0},
    ])

def _recon_ok():
    return pd.DataFrame([
        {"billing_month":"2025-04","total_events":15000,"reconciled_events":14000,
         "unreconciled_events":1000,"reconciliation_pct":93.33,"unreconciled_revenue":5000.0},
        {"billing_month":"2025-05","total_events":16000,"reconciled_events":15500,
         "unreconciled_events":500,"reconciliation_pct":96.875,"unreconciled_revenue":2000.0},
    ])

def _recon_warn():
    return pd.DataFrame([
        {"billing_month":"2025-04","total_events":10000,"reconciled_events":8000,
         "unreconciled_events":2000,"reconciliation_pct":80.0,"unreconciled_revenue":10000.0},
    ])

def _top_df():
    return pd.DataFrame([
        {"billing_month":"2025-04","customer_id":"cust_001","msisdn":"0912111111",
         "full_name":"Customer A","segment":"high_value","plan_type":"postpaid",
         "monthly_revenue":4500.0,"event_count":210,"revenue_rank":1},
        {"billing_month":"2025-05","customer_id":"cust_002","msisdn":"0916222222",
         "full_name":"Customer B","segment":"high_value","plan_type":"postpaid",
         "monthly_revenue":3900.0,"event_count":195,"revenue_rank":1},
    ])


class TestKpiSummaryStructure:

    def _summary(self):
        return build_kpi_summary(_arpu_df(), _mou_df(), _revenue_df(), _recon_ok(), _top_df())

    def test_returns_dataframe(self):
        assert isinstance(self._summary(), pd.DataFrame)

    def test_has_five_rows(self):
        assert len(self._summary()) == 5

    def test_required_columns_present(self):
        for col in ["kpi_name","description","value","unit","status","computed_at"]:
            assert col in self._summary().columns

    def test_kpi_names_all_present(self):
        names = set(self._summary()["kpi_name"])
        for expected in ["ARPU","MOU","Total Revenue","Reconciliation Rate","Top Customer Revenue"]:
            assert expected in names

    def test_all_statuses_valid(self):
        for s in self._summary()["status"]:
            assert s in ("OK","WARNING")

    def test_computed_at_is_string(self):
        for v in self._summary()["computed_at"]:
            assert isinstance(v, str)


class TestKpiSummaryValues:

    def _s(self):
        return build_kpi_summary(_arpu_df(), _mou_df(), _revenue_df(), _recon_ok(), _top_df())

    def test_arpu_positive_with_lc_unit(self):
        row = self._s()[self._s()["kpi_name"] == "ARPU"].iloc[0]
        assert row["value"] > 0 and row["unit"] == "LC"

    def test_mou_positive_minutes(self):
        row = self._s()[self._s()["kpi_name"] == "MOU"].iloc[0]
        assert row["value"] > 0 and row["unit"] == "minutes"

    def test_total_revenue_matches_input(self):
        row = self._s()[self._s()["kpi_name"] == "Total Revenue"].iloc[0]
        assert row["value"] == pytest.approx(150000.0, rel=0.01)

    def test_reconciliation_rate_value(self):
        row = self._s()[self._s()["kpi_name"] == "Reconciliation Rate"].iloc[0]
        assert row["value"] == pytest.approx(95.1, abs=0.2)

    def test_top_customer_revenue_value(self):
        row = self._s()[self._s()["kpi_name"] == "Top Customer Revenue"].iloc[0]
        assert row["value"] == pytest.approx(4200.0, rel=0.01)


class TestReconciliationStatus:

    def test_ok_when_all_months_above_90(self):
        s = build_kpi_summary(_arpu_df(), _mou_df(), _revenue_df(), _recon_ok(), _top_df())
        row = s[s["kpi_name"] == "Reconciliation Rate"].iloc[0]
        assert row["status"] == "OK"

    def test_warning_when_any_month_below_90(self):
        s = build_kpi_summary(_arpu_df(), _mou_df(), _revenue_df(), _recon_warn(), _top_df())
        row = s[s["kpi_name"] == "Reconciliation Rate"].iloc[0]
        assert row["status"] == "WARNING"

    def test_warning_description_mentions_month_count(self):
        s = build_kpi_summary(_arpu_df(), _mou_df(), _revenue_df(), _recon_warn(), _top_df())
        row = s[s["kpi_name"] == "Reconciliation Rate"].iloc[0]
        assert "1" in row["description"]


class TestEmptyInputs:

    def test_empty_arpu_reduces_row_count(self):
        s = build_kpi_summary(pd.DataFrame(), _mou_df(), _revenue_df(), _recon_ok(), _top_df())
        assert "ARPU" not in s["kpi_name"].values and len(s) == 4

    def test_empty_recon_reduces_row_count(self):
        s = build_kpi_summary(_arpu_df(), _mou_df(), _revenue_df(), pd.DataFrame(), _top_df())
        assert "Reconciliation Rate" not in s["kpi_name"].values

    def test_all_empty_returns_empty_dataframe(self):
        s = build_kpi_summary(pd.DataFrame(), pd.DataFrame(), pd.DataFrame(),
                              pd.DataFrame(), pd.DataFrame())
        assert len(s) == 0


# ============================================================================
# PHASE 8 — MONITORING
# ============================================================================

def _quality_df(pass_rate, total_rows=1000, dataset="billing_events", minutes_ago=30):
    failed_rows = int(total_rows * (1 - pass_rate / 100))
    ts = datetime.now(timezone.utc) - timedelta(minutes=minutes_ago)
    return pd.DataFrame([{
        "dq_run_id":"test-run","dq_run_timestamp":ts,"dataset":dataset,
        "category":"validity","rule_name":"test_rule",
        "total_rows":total_rows,"failed_rows":failed_rows,"pass_rate":pass_rate,
    }])


class TestPipelineStatus:

    def test_healthy_above_threshold(self):
        assert compute_metrics(_quality_df(95.0), None)["pipeline_status"] == "HEALTHY"

    def test_healthy_exactly_at_threshold(self):
        assert compute_metrics(_quality_df(HEALTHY_THRESHOLD), None)["pipeline_status"] == "HEALTHY"

    def test_warning_between_thresholds(self):
        mid = (WARNING_THRESHOLD + HEALTHY_THRESHOLD) / 2
        assert compute_metrics(_quality_df(mid), None)["pipeline_status"] == "WARNING"

    def test_critical_below_warning_threshold(self):
        assert compute_metrics(_quality_df(WARNING_THRESHOLD - 1), None)["pipeline_status"] == "CRITICAL"

    def test_critical_when_no_runs(self):
        assert compute_metrics(pd.DataFrame(), None)["pipeline_status"] == "CRITICAL"

    def test_critical_when_stale(self):
        stale = _quality_df(99.0, minutes_ago=(STALE_HOURS * 60) + 10)
        assert compute_metrics(stale, None)["pipeline_status"] == "CRITICAL"

    def test_healthy_recent_high_pass_rate(self):
        assert compute_metrics(_quality_df(98.0, minutes_ago=5), None)["pipeline_status"] == "HEALTHY"


class TestPassAndQuarantineRates:

    def test_pass_rate_matches_input(self):
        m = compute_metrics(_quality_df(92.5), None)
        assert m["quality_pass_rate_pct"] == pytest.approx(92.5, abs=0.1)

    def test_quarantine_rate_is_complement(self):
        m = compute_metrics(_quality_df(88.0), None)
        assert m["quality_pass_rate_pct"] + m["quarantine_rate_pct"] == pytest.approx(100.0, abs=0.01)

    def test_zero_rows_does_not_crash(self):
        m = compute_metrics(_quality_df(100.0, total_rows=0), None)
        assert "pipeline_status" in m

    def test_weighted_average_across_datasets(self):
        ts = datetime.now(timezone.utc) - timedelta(minutes=10)
        df = pd.DataFrame([
            {"dq_run_id":"r","dq_run_timestamp":ts,"dataset":"A","category":"c",
             "rule_name":"r1","total_rows":100,"failed_rows":20,"pass_rate":80.0},
            {"dq_run_id":"r","dq_run_timestamp":ts,"dataset":"B","category":"c",
             "rule_name":"r1","total_rows":900,"failed_rows":0,"pass_rate":100.0},
        ])
        m = compute_metrics(df, None)
        assert m["quality_pass_rate_pct"] == pytest.approx(98.0, abs=0.1)


class TestStaleness:

    def test_minutes_since_last_run_computed(self):
        m = compute_metrics(_quality_df(95.0, minutes_ago=45), None)
        assert m["minutes_since_last_run"] == pytest.approx(45.0, abs=1.0)

    def test_last_successful_run_contains_utc(self):
        m = compute_metrics(_quality_df(95.0), None)
        assert "UTC" in m["last_successful_run"]

    def test_never_run_shows_none_for_minutes(self):
        assert compute_metrics(pd.DataFrame(), None)["minutes_since_last_run"] is None

    def test_never_run_shows_never_string(self):
        assert compute_metrics(pd.DataFrame(), None)["last_successful_run"] == "NEVER"


class TestDatasetBreakdown:

    def _multi_df(self):
        ts = datetime.now(timezone.utc) - timedelta(minutes=5)
        return pd.DataFrame([
            {"dq_run_id":"r","dq_run_timestamp":ts,"dataset":ds,"category":"c",
             "rule_name":"r","total_rows":100,"failed_rows":5,"pass_rate":95.0}
            for ds in ["customers","billing_events","cdrs"]
        ])

    def test_breakdown_contains_all_datasets(self):
        m = compute_metrics(self._multi_df(), None)
        for ds in ["customers","billing_events","cdrs"]:
            assert ds in m["dataset_breakdown"]

    def test_breakdown_pass_rate_correct(self):
        ts = datetime.now(timezone.utc) - timedelta(minutes=5)
        df = pd.DataFrame([{"dq_run_id":"r","dq_run_timestamp":ts,"dataset":"cdrs",
                            "category":"c","rule_name":"r","total_rows":200,
                            "failed_rows":10,"pass_rate":95.0}])
        m = compute_metrics(df, None)
        assert m["dataset_breakdown"]["cdrs"]["pass_rate_pct"] == pytest.approx(95.0, abs=0.1)

    def test_breakdown_failed_rules_count(self):
        ts = datetime.now(timezone.utc) - timedelta(minutes=5)
        df = pd.DataFrame([
            {"dq_run_id":"r","dq_run_timestamp":ts,"dataset":"billing_events",
             "category":"c","rule_name":"rule_a","total_rows":100,"failed_rows":5,"pass_rate":95.0},
            {"dq_run_id":"r","dq_run_timestamp":ts,"dataset":"billing_events",
             "category":"c","rule_name":"rule_b","total_rows":100,"failed_rows":0,"pass_rate":100.0},
        ])
        m = compute_metrics(df, None)
        bd = m["dataset_breakdown"]["billing_events"]
        assert bd["rules_evaluated"] == 2 and bd["failed_rules"] == 1


class TestRowsIngestedToday:

    def test_rows_from_quality_report(self):
        m = compute_metrics(_quality_df(100.0, total_rows=5000), None)
        assert m["total_rows_ingested_today"] == 5000

    def test_zero_for_empty_report(self):
        assert compute_metrics(pd.DataFrame(), None)["total_rows_ingested_today"] == 0

    def test_multi_dataset_rows_summed(self):
        ts = datetime.now(timezone.utc) - timedelta(minutes=5)
        df = pd.DataFrame([
            {"dq_run_id":"r","dq_run_timestamp":ts,"dataset":"billing_events",
             "category":"c","rule_name":"r1","total_rows":5000,"failed_rows":0,"pass_rate":100.0},
            {"dq_run_id":"r","dq_run_timestamp":ts,"dataset":"cdrs",
             "category":"c","rule_name":"r1","total_rows":2000,"failed_rows":0,"pass_rate":100.0},
        ])
        assert compute_metrics(df, None)["total_rows_ingested_today"] == 7000
