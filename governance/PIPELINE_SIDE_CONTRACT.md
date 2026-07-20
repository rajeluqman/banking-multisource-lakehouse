# PIPELINE_SIDE_CONTRACT — producer-published control-plane interface

> **Status:** Accepted (2026-07-19). Authority: ADR-011 (Airflow external orchestration) +
> Addendum #2 (`databricks.yml` ground-truth correction). Referenced by
> `journey/07_PIPELINE_SPEC.md` (Orchestration). This is the file journey/07 calls
> `03_PIPELINE_SIDE_CONTRACT.md`; it now lives here (producer-owned), not in
> `../control_plane_lab/` — see ADR-011 Addendum #2, Correction 6.

## What this document is

This repo (`banking-multisource-lakehouse`) is the **compute / data-product side**. It exposes a
**coarse, stable contract** that the **external orchestration repo**
(`banking-multisource-lakehouse-airflow-dag`) consumes. Airflow **triggers and polls; it never
computes the medallion** (ADR-011 D11.4 anti-pattern #1). D-10 holds: **no Airflow code lives in
this repo.**

The contract is deliberately **coarse**. It publishes the trigger handle, the extraction
entrypoints, and the Landing hand-off — and **deliberately withholds** the Databricks job's 22
internal task-keys and their `depends_on` graph, because publishing them would let the orchestrator
re-own a dependency graph `databricks.yml` already owns (the two-owner-drift anti-pattern ADR-011
D11.4 #3 / ADR-008 D8.1 exists to prevent).

---

## 1. The DAB job trigger handle (processing tier)

| Field | Value | Source of truth |
|---|---|---|
| Databricks job **name** | `banking-lakehouse-berka-salesforce-bronze-silver` | `databricks.yml:38` |
| Bundle resource key | `banking_bronze_silver` | `databricks.yml:37` |
| Bundle name | `banking-lakehouse` | `databricks.yml:19` |
| Trigger mechanism | `DatabricksRunNowOperator` (Airflow `apache-airflow-providers-databricks`) — **one call, whole job** | ADR-011 Add #2 Correction 1 |
| Internal shape | ONE job, **22 tasks**, first task `promotion_gate_salesforce` (Landing→Bronze); terminal `compaction` + `mart_pipeline_health` | `databricks.yml` |
| Schedule/trigger block | **NONE** in `databricks.yml` — cadence is owned by the external orchestrator (D-10, ADR-008 D8.6) | `databricks.yml:12-13` |

The orchestrator resolves the numeric `job_id` **by this job name** (or consumes the DAB-deployed
job_id) and calls `run_now`. It does **not** address individual task-keys. Per-task run status is
observed in the **Databricks Jobs UI**, the data plane's own observability surface.

## 2. The extraction entrypoints (ingestion tier — Airflow extraction executor)

The 5 source→Landing extractions have **no Databricks task-key** (the DAB job starts at
Landing→Bronze). They run on an **Airflow-managed extraction executor** — the standard Airflow
ingestion pattern, a named scoped exception to "Airflow never computes" (ADR-011 Add #2 Correction 2:
bounded I/O, not medallion Spark; two sources are local Docker containers a Databricks cluster cannot
reach). Each is a uniform `def main() -> int` module (0 = success), runnable as:

| Source | Entrypoint (`python -m ...`) | Cadence | Landing tables (`SOURCE_TABLES` in `pipeline/promote/promotion_gate.py`) |
|---|---|---|---|
| Postgres (Home Credit) | `pipeline.extract.postgres_extract` | `batch` (@daily) | application, bureau, bureau_balance, previous_application, pos_cash_balance, credit_card_balance, installments_payments |
| MS SQL (PaySim) | `pipeline.extract.mssql_extract` | `batch` (@daily) | paysim_transactions |
| Salesforce (Berka/CRM) | `pipeline.extract.salesforce_extract` | `bulk_api_poll` (short interval) | contact, account, accountcontactrelation, transaction, district, case |
| Teradata (UCI Bank Mktg) | `pipeline.extract.teradata_extract` | `cdc_poll` (short interval) | bank_marketing (cdc) + bank_marketing (batch, R-40 initial snapshot) |
| Open Bank Project (REST) | `pipeline.extract.obp_client` | `batch` (@daily) | accounts, transactions |

## 3. The Landing hand-off contract (what extracts write / `promotion_gate_salesforce` reads)

Path layout resolved by `pipeline/common/lake_paths.py`:

```
lake_root = s3://${S3_BUCKET}/${S3_PREFIX:-banking}      (AWS creds present)
          | ./data                                        (local-disk fallback, same layout)

Landing (batch): <lake_root>/landing/<source>/<table>/dt=<YYYY-MM-DD>/...
Landing (cdc)  : <lake_root>/landing/<source>/<table>_cdc/dt=<YYYY-MM-DD>/...
```

- Partitioning is by `dt=<logical date>` — **the ingest task's `data_interval_start` maps to `dt`**
  (idempotency, ADR-011 D11.5 — never `now()`). Mechanism: the ingestion executor sets
  `DATA_INTERVAL_START`/`DATA_INTERVAL_END` (ISO-8601) in the extraction task's environment for
  every run, including backfills; `pipeline/common/run_interval.py`'s `logical_date()` reads
  `DATA_INTERVAL_START`'s date for `dt=`, falling back to wall-clock `today()` only when neither
  var is set (local dev-loop / direct-CLI runs with no Airflow above them — not a production
  path). The two time-watermarked extractors (Postgres/MSSQL via `jdbc_batch_common.py`,
  Salesforce) additionally bound their pull predicate to `[start - overlap, end)` in this mode
  and skip lake-watermark read/write entirely, so a backfill is a pure function of the logical
  date. The Teradata CDC path (`cdc_common.py`) keeps pulling via its monotonic `_cdc_log.seq`
  offset regardless — only its `dt=` partition key follows the logical date, since a seq-keyed
  change log has no natural time window to bound a backfill against.
- `promotion_gate_salesforce` reads each source's `dt=*` Landing partitions not yet `_promoted` and
  appends to Bronze Delta. It loops over `SOURCE_TABLES` generically — new tables under an existing
  source are picked up automatically, no gate change.
- Watermark/offset state lives in the lake at `<lake_root>/_control/...`
  (the `control_path()` helper in `pipeline/common/lake_paths.py`), not in code.

## 4. Airflow Dataset URIs (ingestion → processing coupling)

Each `dag_ingest_<source>` emits an Airflow **Dataset** for its Landing prefix; `dag_pipeline` is
**data-aware scheduled** on all 5 (idiomatic modern Airflow; `ExternalTaskSensor` is the fallback).
Recommended Dataset URIs (external repo pins these):

```
s3://${S3_BUCKET}/${S3_PREFIX}/landing/postgres/
s3://${S3_BUCKET}/${S3_PREFIX}/landing/mssql/
s3://${S3_BUCKET}/${S3_PREFIX}/landing/salesforce/
s3://${S3_BUCKET}/${S3_PREFIX}/landing/teradata/
s3://${S3_BUCKET}/${S3_PREFIX}/landing/obp/
```

## 5. Explicitly NOT part of the contract

- The 22 internal Databricks task-keys and their `depends_on` graph — owned solely by
  `databricks.yml`. The orchestrator MUST NOT re-declare them (Option-B drift, rejected in ADR-011
  Add #2 Correction 1).
- Any `schedule:`/`trigger:` inside this repo — cadence belongs to the external orchestrator (D-10).
- The dbt serving models — `dbt build` is a separate terminal Airflow task against Snowflake (the
  retired analytics marts as views: BQ-01..08 + BQ-11, ADR-010); the orchestrator triggers it after
  the DAB job reaches terminal success.

## 6. Change policy

This contract is **producer-owned**. Changes to the job name, the extraction entrypoints, or the
Landing layout are **breaking** and MUST be announced here first; the external repo pins/vendors this
file and CI-drift-checks against it. Adding a Landing table under an existing source is
**non-breaking** (the gate picks it up generically).
