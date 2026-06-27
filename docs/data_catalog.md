# Data Catalog — Telecom Billing DWH (the telecom operator)

**Project:** Telecom Billing Data Warehouse  
**Owner:** Data Engineering Team  
**Domain:** Telecom Billing & Revenue Assurance  
**Updated:** 2026-06-26  
**Currency:** LC (local currency)

---

## Table of Contents

1. [Bronze Layer](#bronze-layer)
   - [customers](#customers-bronze)
   - [billing_events](#billing_events-bronze)
   - [cdr_records](#cdr_records-bronze)
2. [Silver Layer](#silver-layer)
   - [billing_enriched](#billing_enriched-silver)
   - [cdrs_cleaned](#cdrs_cleaned-silver)
   - [customer_metrics](#customer_metrics-silver)
   - [customers_cleaned](#customers_cleaned-silver)
3. [Gold Layer — Dimensions](#gold-layer--dimensions)
   - [dim_customer](#dim_customer)
   - [dim_service](#dim_service)
   - [dim_date](#dim_date)
   - [dim_region](#dim_region)
   - [dim_plan](#dim_plan)
4. [Gold Layer — Facts](#gold-layer--facts)
   - [fact_billing](#fact_billing)
5. [Mart Layer — KPI Tables](#mart-layer--kpi-tables)
   - [kpi_arpu_by_month_region](#kpi_arpu_by_month_region)
   - [kpi_mou_by_service](#kpi_mou_by_service)
   - [kpi_revenue_by_plan_governorate](#kpi_revenue_by_plan_governorate)
   - [kpi_reconciliation_rate](#kpi_reconciliation_rate)
   - [kpi_top_customers_by_month](#kpi_top_customers_by_month)

---

## Bronze Layer

The bronze layer stores raw, append-only copies of data from the Huawei CBS SFTP drop zone. No transformations are applied — all columns are ingested as strings. Three metadata columns are added for lineage tracking.

**Design:** Append-only · No deduplication · All types as string · Snappy Parquet

---

### customers (Bronze)

| Attribute | Value |
|---|---|
| **Layer** | Bronze |
| **Path** | `data/bronze/customers/` |
| **Source** | Huawei CBS subscriber master (CRM snapshot) |
| **Format** | Parquet (Snappy) |
| **Approx. rows** | 3,000 |
| **Partition key** | None (small dimension table) |
| **Update frequency** | Daily CRM snapshot |
| **Owner** | Data Engineering |

**Business description:** Raw subscriber master records from the operator's CRM system as exported by the Huawei CBS subscriber management module. Contains one row per active or churned subscriber. This is the source of truth for customer identity, plan assignment, and geographic segmentation.

**Columns:**

| Column | Type | Description | Example |
|---|---|---|---|
| customer_id | string | Unique subscriber identifier (UUID) | `cust_00001` |
| msisdn | string | Mobile number (11 digits, local format) | `0912345678` |
| national_id | string | national ID number | `1234567890` |
| first_name | string | Subscriber first name | `Ahmed` |
| last_name | string | Subscriber last name | `Hassan` |
| gender | string | M / F | `M` |
| date_of_birth | string | Birth date (YYYY-MM-DD) | `1990-04-15` |
| registration_date | string | SIM activation date (YYYY-MM-DD) | `2022-01-10` |
| plan_type | string | prepaid / postpaid / hybrid | `prepaid` |
| status | string | active / suspended / churned | `active` |
| state | string | region / city | `Khartoum` |
| balance | string | Account balance in LC (prepaid only) | `450.75` |
| _ingested_at | string | UTC ingestion timestamp | `2025-06-01 08:00:00` |
| _source_file | string | Original CSV filename | `customers_20250601.csv` |
| _batch_id | string | UUID for this ingestion run | `a1b2c3d4` |

**Quality rules applied:** null checks on customer_id, msisdn, national_id, first_name, last_name, plan_type, status; msisdn format validation (11-digit, valid prefix); non-negative balance for prepaid/hybrid; registration_date not in future.

---

### billing_events (Bronze)

| Attribute | Value |
|---|---|
| **Layer** | Bronze |
| **Path** | `data/bronze/billing_events/` |
| **Source** | Huawei CBS rated events (SFTP micro-batch) |
| **Format** | Parquet (Snappy) |
| **Approx. rows** | 50,000 |
| **Partition key** | `event_date` |
| **Update frequency** | Every 30 minutes (CBS batch cycle) |
| **Owner** | Data Engineering |

**Business description:** Raw rated billing events produced by the Huawei Convergent Billing System (CBS). Each row represents one charge applied to a subscriber for a telecom service (voice, SMS, data, VAS, roaming). This is the primary revenue source of truth for the operator's Finance and Revenue Assurance teams.

**Columns:**

| Column | Type | Description | Example |
|---|---|---|---|
| event_id | string | Unique billing event identifier | `evt_00001` |
| customer_id | string | FK to customers.customer_id | `cust_00001` |
| event_timestamp | string | Event datetime (YYYY-MM-DD HH:MM:SS) | `2025-06-01 14:32:00` |
| event_date | string | Event date (YYYY-MM-DD), partition key | `2025-06-01` |
| charge_type | string | CBS charge code | `voice_charge` |
| service_type | string | voice / sms / data / vas / roaming | `voice` |
| amount | string | Charged amount in LC | `12.50` |
| currency | string | Always LC for domestic | `LC` |
| channel | string | ussd / app / ivr / web / agent | `ussd` |
| plan_type | string | Subscriber plan at time of event | `prepaid` |
| status | string | rated / pending / reversed / failed | `rated` |
| _ingested_at | string | UTC ingestion timestamp | `2025-06-01 14:35:00` |
| _source_file | string | Original CSV filename | `billing_stream_001.csv` |
| _batch_id | string | UUID for this ingestion run | `b2c3d4e5` |

**Quality rules applied:** null checks on event_id, customer_id, event_timestamp, charge_type, amount; non-negative amount; charge_type in valid CBS code set; event_timestamp parseable; service_type in valid set; duplicate event_id detection; timeliness check (stale > 180 days, future-dated); orphan customer_id check.

---

### cdr_records (Bronze)

| Attribute | Value |
|---|---|
| **Layer** | Bronze |
| **Path** | `data/bronze/cdrs/` |
| **Source** | Huawei MSC call detail records |
| **Format** | Parquet (Snappy) |
| **Approx. rows** | 20,000 |
| **Partition key** | `call_date` |
| **Update frequency** | Every 30 minutes (MSC batch) |
| **Owner** | Data Engineering |

**Business description:** Raw Call Detail Records (CDRs) from the operator's Huawei MSC (Mobile Switching Center). Each row represents one network event (call, SMS, data session). CDRs are the network-layer evidence used by the Revenue Assurance team to validate billing events — every billed event should have a corresponding CDR.

**Columns:**

| Column | Type | Description | Example |
|---|---|---|---|
| cdr_id | string | Unique CDR identifier | `cdr_00001` |
| calling_msisdn | string | Originating phone number | `0912345678` |
| called_msisdn | string | Destination phone number | `0922876543` |
| call_start_time | string | Call start datetime (YYYY-MM-DD HH:MM:SS) | `2025-06-01 14:31:45` |
| call_date | string | Call date (YYYY-MM-DD), partition key | `2025-06-01` |
| duration_seconds | string | Call duration in seconds | `180` |
| call_type | string | on_net / off_net / international | `on_net` |
| service_type | string | voice / sms / data / vas / roaming | `voice` |
| cell_tower_id | string | Serving cell tower ID | `KH-001` |
| rated_amount | string | Amount rated by MSC (LC) | `12.50` |
| status | string | completed / failed / busy | `completed` |
| _ingested_at | string | UTC ingestion timestamp | `2025-06-01 14:35:10` |
| _source_file | string | Original CSV filename | `cdr_stream_001.csv` |
| _batch_id | string | UUID for this ingestion run | `c3d4e5f6` |

**Quality rules applied:** null checks on cdr_id, calling_msisdn, called_msisdn, call_start_time, duration_seconds; non-negative duration; duration ≤ 86400s (Huawei MSC 24h limit); non-negative rated_amount; calling_msisdn format validation; completed calls must have duration > 0; duplicate cdr_id detection; timeliness checks.

---

## Silver Layer

The silver layer stores cleaned, standardized, deduplicated, and enriched data. All columns are properly typed. Billing events are enriched with customer dimension attributes. Bad rows identified by the DQ gate are excluded via anti-join against the quarantine table.

**Design:** Overwrite on reprocess · Quarantine-excluded · Snappy Parquet · Partitioned by month

---

### billing_enriched (Silver)

| Attribute | Value |
|---|---|
| **Layer** | Silver |
| **Path** | `data/silver/billing_enriched/` |
| **Source** | bronze/billing_events + bronze/customers (anti-joined quarantine) |
| **Format** | Parquet (Snappy) |
| **Approx. rows** | ~45,000 (after DQ exclusions) |
| **Partition key** | `billing_month` (YYYY-MM) |
| **Update frequency** | Every 30 minutes (Airflow DAG) |
| **Owner** | Data Engineering |

**Business description:** Cleaned and enriched billing events. Each billing event is joined to the customer dimension to add subscriber attributes (region, plan, status). `billing_month` partition enables efficient ARPU and revenue queries. This is the primary input to the Gold layer star schema.

**Key added/transformed columns:**

| Column | Type | Description | Example |
|---|---|---|---|
| amount | double | Amount cast from string to double | `12.50` |
| event_timestamp | timestamp | Parsed timestamp | `2025-06-01 14:32:00` |
| billing_month | string | Derived partition key (YYYY-MM) | `2025-06` |
| cust_msisdn | string | Customer MSISDN (from join) | `0912345678` |
| cust_first_name | string | Customer first name (lowercased) | `ahmed` |
| cust_last_name | string | Customer last name (lowercased) | `hassan` |
| cust_gender | string | Customer gender (uppercased) | `M` |
| cust_plan_type | string | Plan type from customer record | `prepaid` |
| cust_status | string | Customer status | `active` |
| cust_state | string | Customer region/city | `Khartoum` |
| _enrichment_status | string | matched / unmatched | `matched` |

---

### cdrs_cleaned (Silver)

| Attribute | Value |
|---|---|
| **Layer** | Silver |
| **Path** | `data/silver/cdrs_cleaned/` |
| **Source** | bronze/cdrs (anti-joined quarantine) |
| **Format** | Parquet (Snappy) |
| **Approx. rows** | ~18,000 (after DQ exclusions) |
| **Partition key** | `call_month` (YYYY-MM) |
| **Update frequency** | Every 30 minutes |
| **Owner** | Data Engineering |

**Business description:** Standardized CDR records with proper types and derived partition column. Used for CDR reconciliation in the Gold layer fact_billing build — each billing event is checked for a matching CDR on (customer, service, date).

**Key added/transformed columns:**

| Column | Type | Description | Example |
|---|---|---|---|
| duration_seconds | integer | Cast from string | `180` |
| rated_amount | double | Cast from string | `12.50` |
| call_start_time | timestamp | Parsed timestamp | `2025-06-01 14:31:45` |
| call_date | date | Parsed date | `2025-06-01` |
| call_month | string | Derived partition key (YYYY-MM) | `2025-06` |

---

### customer_metrics (Silver)

| Attribute | Value |
|---|---|
| **Layer** | Silver |
| **Path** | `data/silver/customer_metrics/` |
| **Source** | billing_enriched (rated events only) |
| **Format** | Parquet (Snappy) |
| **Approx. rows** | ~3,000 (one row per customer) |
| **Partition key** | None |
| **Update frequency** | Every 30 minutes |
| **Owner** | Data Engineering |

**Business description:** Per-customer billing aggregates. One row per subscriber summarizing their complete billing history. Used in the Gold layer to derive customer segments (high_value / mid_value / low_value) for the dim_customer dimension.

**Columns:**

| Column | Type | Description | Example |
|---|---|---|---|
| customer_id | string | Subscriber identifier | `cust_00001` |
| total_billed_amount | double | Sum of all rated charges (LC) | `1,250.00` |
| total_events | long | Count of billing events | `85` |
| avg_event_amount | double | Mean charge per event (LC) | `14.71` |
| first_event_date | date | Earliest billing event date | `2025-04-01` |
| last_event_date | date | Most recent billing event date | `2025-06-30` |
| distinct_services | long | Count of distinct service types used | `3` |
| dominant_service | string | Most frequent service type | `voice` |
| cust_msisdn | string | Subscriber MSISDN | `0912345678` |
| cust_plan_type | string | Current plan type | `prepaid` |
| cust_status | string | Subscriber status | `active` |
| cust_state | string | Subscriber region | `Khartoum` |

---

### customers_cleaned (Silver)

| Attribute | Value |
|---|---|
| **Layer** | Silver |
| **Path** | `data/silver/customers_cleaned/` |
| **Source** | bronze/customers (anti-joined quarantine) |
| **Format** | Parquet (Snappy) |
| **Approx. rows** | ~2,700 (after DQ exclusions) |
| **Partition key** | None |
| **Update frequency** | Daily |
| **Owner** | Data Engineering |

**Business description:** Standardized customer dimension with proper types, lowercase strings, and parsed dates. Input to the Gold layer dim_customer build.

---

## Gold Layer — Dimensions

The Gold layer implements a **star schema** optimized for BI tool queries. Dimensions use surrogate integer keys. The fact table joins to all five dimensions for slice-and-dice analysis.

---

### dim_customer

| Attribute | Value |
|---|---|
| **Layer** | Gold |
| **Path** | `data/gold/dim_customer/` |
| **Source** | silver/customers_cleaned + silver/customer_metrics |
| **Format** | Parquet (Snappy) |
| **Approx. rows** | ~2,700 |
| **SCD type** | Type 1 (overwrite with latest snapshot) |
| **Partition key** | None |
| **Update frequency** | Every 30 minutes |
| **Owner** | Data Engineering |

**Business description:** Conformed customer dimension enriched with billing-derived segment classification. Used by BI tools for subscriber demographic analysis, churn correlation, and VIP identification.

**Columns:**

| Column | Type | Description | Example |
|---|---|---|---|
| customer_id | string | Natural key (matches bronze) | `cust_00001` |
| msisdn | string | Mobile number | `0912345678` |
| full_name | string | Concatenated first + last name | `Ahmed Hassan` |
| segment | string | high_value / mid_value / low_value (by total billing) | `mid_value` |
| plan | string | prepaid / postpaid / hybrid | `prepaid` |
| region | string | city | `Khartoum` |
| governorate | string | governorate | `Khartoum` |
| activation_date | date | SIM activation date | `2022-01-10` |
| is_active | boolean | True if status = active | `true` |

**Segmentation rule:** `high_value` > 2,000 LC total billed · `mid_value` > 500 LC · `low_value` ≤ 500 LC

---

### dim_service

| Attribute | Value |
|---|---|
| **Layer** | Gold |
| **Path** | `data/gold/dim_service/` |
| **Source** | Telecom CBS service catalogue (inline reference) |
| **Format** | Parquet (Snappy) |
| **Approx. rows** | 5 |
| **Partition key** | None |
| **Update frequency** | Manual (product catalogue changes) |
| **Owner** | Data Engineering / Marketing |

**Business description:** Service taxonomy mapping Huawei CBS service codes to business categories. Used for MOU and revenue analysis by service type.

**Columns:**

| Column | Type | Description | Example |
|---|---|---|---|
| service_id | integer | Surrogate key | `1` |
| service_type | string | voice / sms / data / vas / roaming | `voice` |
| service_category | string | communication / messaging / internet / value_added_services / roaming | `communication` |

---

### dim_date

| Attribute | Value |
|---|---|
| **Layer** | Gold |
| **Path** | `data/gold/dim_date/` |
| **Source** | Generated (2024-01-01 to 2025-12-31) |
| **Format** | Parquet (Snappy) |
| **Approx. rows** | 731 |
| **Partition key** | None |
| **Update frequency** | Annual (extend date range) |
| **Owner** | Data Engineering |

**Business description:** Standard calendar dimension for time-based drill-down in BI tools. Covers the full data range of the DWH.

**Columns:**

| Column | Type | Description | Example |
|---|---|---|---|
| date_id | integer | Surrogate key (YYYYMMDD) | `20250601` |
| full_date | date | Calendar date | `2025-06-01` |
| day | integer | Day of month | `1` |
| month | integer | Month number | `6` |
| month_name | string | Full month name | `June` |
| quarter | integer | Fiscal quarter (1–4) | `2` |
| year | integer | Calendar year | `2025` |
| is_weekend | boolean | True if Saturday or Sunday | `false` |
| is_month_end | boolean | True if last day of month | `false` |

---

### dim_region

| Attribute | Value |
|---|---|
| **Layer** | Gold |
| **Path** | `data/gold/dim_region/` |
| **Source** | the telecom operator geographic hierarchy (inline MDM reference) |
| **Format** | Parquet (Snappy) |
| **Approx. rows** | 15 |
| **Partition key** | None |
| **Update frequency** | Manual (territory changes) |
| **Owner** | Data Engineering / Network Planning |

**Business description:** geographic hierarchy aligned with the operator's operational regions. Used for geographic revenue and MOU analysis, and for network capacity planning.

**Columns:**

| Column | Type | Description | Example |
|---|---|---|---|
| region_id | integer | Surrogate key | `1` |
| region_name | string | City / district name | `Khartoum` |
| governorate | string | governorate | `Khartoum` |
| zone | string | Telecom operational zone: Central / Eastern / Northern / Western / Southern | `Central` |

---

### dim_plan

| Attribute | Value |
|---|---|
| **Layer** | Gold |
| **Path** | `data/gold/dim_plan/` |
| **Source** | the telecom operator commercial tariff catalogue |
| **Format** | Parquet (Snappy) |
| **Approx. rows** | 3 |
| **Partition key** | None |
| **Update frequency** | Manual (tariff catalogue changes) |
| **Owner** | Data Engineering / Marketing |

**Business description:** Tariff plan dimension mapping plan types to commercial names and expected ARPU targets. Used for plan performance and revenue per subscriber analysis.

**Columns:**

| Column | Type | Description | Example |
|---|---|---|---|
| plan_id | integer | Surrogate key | `1` |
| plan_name | string | Commercial plan name | `Operator Prepaid` |
| plan_type | string | prepaid / postpaid / hybrid | `prepaid` |
| plan_tier | string | basic / standard / premium | `basic` |
| expected_arpu | double | Target monthly ARPU in LC | `150.00` |

---

## Gold Layer — Facts

---

### fact_billing

| Attribute | Value |
|---|---|
| **Layer** | Gold |
| **Path** | `data/gold/fact_billing/` |
| **Source** | silver/billing_enriched + silver/cdrs_cleaned + all Gold dims |
| **Format** | Parquet (Snappy) |
| **Approx. rows** | ~45,000 |
| **Partition key** | `billing_month` (YYYY-MM) |
| **Update frequency** | Every 30 minutes |
| **Owner** | Data Engineering |

**Business description:** Central billing fact table in the star schema. Each row is one billing event with foreign keys to all five dimensions and CDR-derived metrics. The `is_reconciled` flag is the core Revenue Assurance metric — it indicates whether a CBS billing event has a matching Huawei MSC CDR on the same (customer, service, date). Unreconciled events signal potential revenue leakage or rating errors.

**Columns:**

| Column | Type | Description | Example |
|---|---|---|---|
| fact_id | long | Surrogate fact key | `12345` |
| customer_id | string | FK → dim_customer.customer_id | `cust_00001` |
| service_id | integer | FK → dim_service.service_id | `1` |
| date_id | integer | FK → dim_date.date_id (YYYYMMDD) | `20250601` |
| region_id | integer | FK → dim_region.region_id | `1` |
| plan_id | integer | FK → dim_plan.plan_id | `1` |
| billed_amount | double | Charge amount in LC | `12.50` |
| duration_sec | integer | Total CDR duration for voice events (seconds) | `180` |
| data_mb | double | Estimated data MB for data events | `250.00` |
| sms_count | integer | CDR count for SMS events | `0` |
| billing_month | string | Partition key (YYYY-MM) | `2025-06` |
| billing_cycle | integer | Month number (1–12) | `6` |
| currency | string | Always LC | `LC` |
| status | string | rated / pending / reversed / failed | `rated` |
| is_reconciled | integer | 1 = CDR match found · 0 = unreconciled | `1` |

**Data quality rules applied:** CDR reconciliation join on (customer_id, service_type, event_date); referential integrity to all five dimensions; only status=matched enriched billing events included.

---

## Mart Layer — KPI Tables

The mart layer contains pre-aggregated financial KPIs computed by DuckDB from the Gold star schema. These tables are consumed directly by BI tools (Tableau, Power BI) and the daily KPI digest email.

---

### kpi_arpu_by_month_region

| Attribute | Value |
|---|---|
| **Layer** | Mart |
| **Path** | `data/mart/kpi_arpu_by_month_region.parquet` |
| **Consumers** | CFO dashboard, investor reporting, regional performance review |
| **Approx. rows** | ~45 (15 regions × 3 months) |
| **Update frequency** | Every 30 minutes |

**Business description:** Average Revenue Per User (ARPU) grouped by billing month and region. Primary financial KPI for the telecom operator. Formula: `SUM(billed_amount) / COUNT(DISTINCT customer_id)`.

| Column | Type | Description | Example |
|---|---|---|---|
| billing_month | string | YYYY-MM | `2025-06` |
| region | string | City name | `Khartoum` |
| governorate | string | governorate | `Khartoum` |
| active_subscribers | long | Distinct billed customers | `450` |
| total_revenue | double | Sum of rated charges (LC) | `125,000.00` |
| arpu | double | Average revenue per user (LC) | `277.78` |

---

### kpi_mou_by_service

| Attribute | Value |
|---|---|
| **Layer** | Mart |
| **Path** | `data/mart/kpi_mou_by_service.parquet` |
| **Consumers** | Network Planning, capacity forecasting |
| **Approx. rows** | ~9 (3 service types × 3 months) |
| **Update frequency** | Every 30 minutes |

**Business description:** Minutes of Use (MOU) by service type and billing month. Formula: `SUM(duration_sec) / 60`. Used by Network Planning to forecast BSC/RNC expansion needs.

| Column | Type | Description | Example |
|---|---|---|---|
| billing_month | string | YYYY-MM | `2025-06` |
| service_type | string | voice / sms / data / etc. | `voice` |
| service_category | string | Business category | `communication` |
| total_mou | double | Total minutes of use | `82,500.00` |
| event_count | long | Number of events with duration > 0 | `12,400` |
| avg_mou_per_event | double | Average minutes per event | `6.65` |

---

### kpi_revenue_by_plan_governorate

| Attribute | Value |
|---|---|
| **Layer** | Mart |
| **Path** | `data/mart/kpi_revenue_by_plan_governorate.parquet` |
| **Consumers** | Marketing, product performance |
| **Approx. rows** | ~45 (3 plans × 15 governorates) |
| **Update frequency** | Every 30 minutes |

**Business description:** Revenue breakdown by tariff plan and governorate. Used by Marketing to evaluate plan adoption across geographic segments and identify underperforming plan-region combinations.

| Column | Type | Description | Example |
|---|---|---|---|
| plan_name | string | Commercial plan name | `Operator Prepaid` |
| plan_type | string | prepaid / postpaid / hybrid | `prepaid` |
| plan_tier | string | basic / standard / premium | `basic` |
| governorate | string | governorate | `Khartoum` |
| zone | string | Operational zone | `Central` |
| billing_events | long | Total billing events | `18,000` |
| subscribers | long | Distinct subscribers | `1,200` |
| total_revenue | double | Sum of rated charges (LC) | `200,000.00` |
| avg_event_amount | double | Mean charge per event (LC) | `11.11` |
| revenue_per_subscriber | double | Revenue per subscriber (LC) | `166.67` |

---

### kpi_reconciliation_rate

| Attribute | Value |
|---|---|
| **Layer** | Mart |
| **Path** | `data/mart/kpi_reconciliation_rate.parquet` |
| **Consumers** | Revenue Assurance, Finance audit |
| **Approx. rows** | 3 (one per billing month) |
| **Update frequency** | Every 30 minutes |

**Business description:** Billing-CDR reconciliation rate by month. Core Revenue Assurance metric measuring the percentage of CBS billing events that have a matching Huawei MSC CDR. Months below 90% trigger a WARNING status and Revenue Assurance investigation.

| Column | Type | Description | Example |
|---|---|---|---|
| billing_month | string | YYYY-MM | `2025-06` |
| total_events | long | All billing events in the month | `15,000` |
| reconciled_events | long | Events with a matching CDR | `13,800` |
| unreconciled_events | long | Events without a CDR | `1,200` |
| reconciliation_pct | double | Reconciliation rate (%) | `92.00` |
| unreconciled_revenue | double | Revenue from unreconciled events (LC) | `18,750.00` |
| status | string | OK (≥ 90%) or WARNING (< 90%) | `OK` |

---

### kpi_top_customers_by_month

| Attribute | Value |
|---|---|
| **Layer** | Mart |
| **Path** | `data/mart/kpi_top_customers_by_month.parquet` |
| **Consumers** | Enterprise Sales, VIP retention team |
| **Approx. rows** | 30 (top 10 per month × 3 months) |
| **Update frequency** | Every 30 minutes |

**Business description:** Top 10 highest-revenue subscribers per billing month. Used by the Enterprise Sales team to monitor VIP customers for retention risk. Declining revenue rank in a top-10 subscriber triggers proactive outreach from the Key Account Manager.

| Column | Type | Description | Example |
|---|---|---|---|
| billing_month | string | YYYY-MM | `2025-06` |
| customer_id | string | Subscriber identifier | `cust_02847` |
| msisdn | string | Mobile number | `0918765432` |
| full_name | string | Subscriber full name | `Fatima Ali` |
| segment | string | Customer segment | `high_value` |
| plan_type | string | Plan type | `postpaid` |
| monthly_revenue | double | Total revenue this month (LC) | `3,850.00` |
| event_count | long | Billing events this month | `210` |
| revenue_rank | long | Rank within the billing month (1 = highest) | `1` |
