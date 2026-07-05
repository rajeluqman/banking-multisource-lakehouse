# 07 — Pipeline Spec

## Layers and compute
| Layer | Storage | Compute/engine | Why this engine |
|---|---|---|---|
| Landing | S3 (`s3://<bucket>/banking/landing/`), transient short TTL (7 days); local-disk fallback (`data/landing/`) if no AWS credentials | Databricks portable PySpark (writes) | ADR-002/ADR-003 |
| Bronze | S3 (`s3://<bucket>/banking/bronze/`), permanent Delta, append-only | Databricks portable PySpark + Delta | ADR-002 |
| Silver | S3 (`s3://<bucket>/banking/silver/`), Delta, MERGE-upserted | Databricks portable PySpark + Delta | ADR-002 |
| Gold/marts | S3 (`s3://<bucket>/banking/gold/`), Delta | Databricks portable PySpark + Delta, Unity Catalog governance | ADR-002, ADR-005 |
| Serving | Snowflake external tables over Gold S3 (or DuckDB $0 fallback) + Power BI (Fasa E) | Snowflake / DuckDB | ADR-002 |

Dev-loop note (D-14): all of the above run identically against a small deterministic sample set on
local Spark for iteration; the canonical full-scale run happens on the disposable Databricks trial
with Unity Catalog attached, evidence harvested into `journey/08_SERVING_AND_EVIDENCE.md`.

## Orchestration
Local Makefile targets (`make seed`, `make landing`, `make promote`, `make silver`, `make gold`)
for the dev loop — no private Airflow inside this repo (D-10). The repo exposes the control-plane
orchestration contract (`../control_plane_lab/03_PIPELINE_SIDE_CONTRACT.md` in the planning
workspace) so `airflow_dag_running_pipeline` can later drive this pipeline as pipeline #6;
implementing that adoption is out of THIS repo's scope.
- Schedule/trigger: manual/on-demand in v1 (batch-first, ADR-004) — no cron/scheduler wired by
  default; `drip_feed.py` runs as a standalone interval loop to simulate live source traffic
  between manual pipeline runs.
- Retry policy: extractors retry-with-backoff on transient failure (DB connection drop, OBP
  429/5xx); a fully failed extractor run leaves Landing partial and the promotion gate quarantines
  it — the next run is safe to just re-run (idempotent per watermark + per date partition).

## Idempotency & rerun semantics
- Identity key: `customer_id` via `dim_customer_xwalk` for cross-source dedup at Gold; per-source
  native PK (`SK_ID_CURR`, generated PaySim `txn_id`, Berka `client_id`/`account_id`, OBP
  `account_id`) for Bronze/Silver skip-existing and MERGE keys (`journey/04_DATA_MODEL.md`
  identity section).
- Partial-failure rerun: safe to just re-run at every layer. Landing extraction re-runs the same
  watermark window (idempotent — a re-pulled row is deduped by PK+`updated_at` at the Silver
  MERGE, not at Landing). Bronze promotion re-checks the same manifest; an already-promoted
  partition is a no-op, not a duplicate append. Silver/Gold rebuilds are full MERGE/overwrite from
  Bronze, so they can always be safely re-run from scratch.
- Backfill: re-run any historical `dt=` partition through the same promotion gate; Bronze append
  is idempotent per partition (checksum-keyed), so a backfill re-run does not duplicate data.

## Failure handling
Slack failure alert on any fasa's hard failure (promotion-gate quarantine, DQ hard-fail, Gold
build failure) — same pattern as CIL's `_notify_slack_failure` (`01_OPUS_DECISIONS.md` D-16 §9,
`06_SECURITY_MODEL.md` §9: solo-owner, one Slack channel, one responder). Implementation lives in
pipeline/common/alerts.py (built alongside the Fasa B/C/D transforms it wraps — cite the actual
file:line in `BUILD_REPORT.md` once implemented, not "we have alerting" as an unverified claim).
