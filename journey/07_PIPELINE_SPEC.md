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

**Source-side compute (ADR-006):** Postgres and MS SQL Server run as Docker containers wherever
the pipeline is executed (owner's dedicated Codespace for actual runs, not the planning session).
Salesforce (a Developer/trial org) and Teradata (Vantage Express / Teradata Cloud free tier)
are the owner's own provisioned instances — never spun up or connected to from the docs-authoring session; connection
details arrive via `.env` only when the owner is ready to run the extractor against them.

## Salesforce / Teradata prerequisites (owner action, before Fasa B can run live)
1. Provision a Salesforce Developer/trial org — create an External Client App with **Client
   Credentials Flow** enabled (Setup → Flow Enablement; NOT Username-Password/ROPC — this org's
   External Client App model doesn't expose that flow at all, live-confirmed BUILD_REPORT.md §11)
   and a **Run As** user set; note the Consumer Key/Secret + the org's My Domain host (`SALESFORCE_
   LOGIN_URL`). Salesforce is a public SaaS endpoint — no network-exposure step; R-39 does not apply
   to source #4 (ADR-006 Add #2).
2. Provision Teradata (Vantage Express or Teradata Cloud free tier) — network-exposure check (R-39).
3. `pip install simple-salesforce` (or a Bulk API 2.0 client) + the Teradata Python driver in the
   environment that will run seed/salesforce/load_berka.py / seed/teradata/load_bank_marketing.py.
4. Fill the Salesforce (Connected App) / Teradata block in `.env` (never commit real values — see
   `journey/09_SECURITY_AND_ACCESS.md` §1).
Until these are done, all Salesforce/Teradata code in this repo is written but UNVERIFIED against a
live instance — tagged as such in `BUILD_REPORT.md`, not silently claimed as tested.

## Orchestration (ADR-007 — decoupled, config-driven)
27 independently-runnable pipeline components (5 extraction + 1 promotion gate + 5 Silver
domain pipelines + 6 Gold dims/facts + 9 Gold marts + 1 orchestrator), sequenced by
pipeline/orchestrate.py reading pipeline/orchestrate_config.yml — same "one config,
scripts read from it" philosophy as `gates/framework.yml`, not a hardcoded DAG in Python.
No private Airflow inside this repo (D-10). The repo exposes the control-plane orchestration
contract (`../control_plane_lab/03_PIPELINE_SIDE_CONTRACT.md` in the planning workspace) so
`airflow_dag_running_pipeline` can later drive this pipeline as pipeline #6; implementing
that adoption is out of THIS repo's scope — `orchestrate.py` is the local dev-loop
sequencer, not a competing scheduler.
- Cadence per source (config-driven, not uniform): `batch` (Postgres/MSSQL — scheduled,
  e.g. nightly) vs `cdc_poll` (Teradata — short interval, e.g. every 5 min) and `bulk_api_poll` (Salesforce —
  Bulk API 2.0 `SystemModstamp` incremental, short interval; ADR-006 Add #2).
  `drip_feed.py` runs as a standalone interval loop simulating live source traffic between
  orchestrated runs.
- Retry policy: extractors retry-with-backoff on transient failure (DB connection drop, OBP
  429/5xx); a fully failed extractor run leaves Landing partial and the promotion gate quarantines
  it — the next run is safe to just re-run (idempotent per watermark + per date partition).
- Run-status: the orchestrator writes a (stage, status, timestamp, error) row per stage into
  the same control-plane store `pipeline/common/watermark.py` uses — `mart_pipeline_health`
  (BQ-10) reads this alongside its row-count reconciliation, so BQ-10 reflects orchestration
  health, not just data counts.

## Databricks execution path (PROVEN 2026-07-16 — ADR-002 Addendum #6)
`orchestrate.py` above is the local dev-loop sequencer. On Databricks the SAME entrypoints run
via a **git-sourced Job** — now a real, proven path, not aspirational:
- **Deployment (git-native, superseding Add #5's ad-hoc command-execution code-shipping):** push
  to the public GitHub remote → Databricks clones it into a Workspace Repo (`w.repos.create(...,
  provider="gitHub")`) → a Job (`w.jobs.create`) with `GitSource(git_provider=GIT_HUB,
  git_branch=...)` runs one `spark_python_task` (`source=Source.GIT`,
  `python_file="pipeline/.../<entrypoint>.py"`) per stage, `depends_on`-chained. Runs on the
  `SINGLE_USER` cluster (ADR-002 Add #5, unchanged).
- **Run-triggering (owner override 2026-07-16):** the agent MAY call `run_now` on the git-sourced
  Job when the owner explicitly prompts "run" (the prompt is the authorization); it does not
  auto-trigger unprompted. `run_now` ships zero code (Databricks pulls from git), so it is NOT the
  BANNED pattern. A future external orchestrator (Airflow, D-10) owns scheduled triggering. Ad-hoc
  command-execution code-shipping (tar / per-file base64 of `pipeline/`) remains BANNED —
  harness-blocked, not just discouraged (ADR-002 Add #6).
- **Proven run:** `pipeline/promote/promotion_gate.py` → Bronze then `pipeline/silver/silver_crm.py`
  → Silver, `RunResultState.SUCCESS`, both layers independently boto3-verified in S3.
- **Entrypoint contract (durable Databricks platform fact):** every `__main__` guard must
  `raise SystemExit(rc)` ONLY when `rc != 0`. Databricks' git-sourced `spark_python_task` runner
  treats ANY raised `SystemExit` — including `SystemExit(0)` (success) — as a task failure and
  cascades it to `depends_on` children (ADR-002 Add #6). Being retrofitted proactively across all
  entrypoints by `@senior-data-engineer` (ADR-002 Add #6 retrofit ruling).

## CI/CD and Infrastructure-as-Code (ADR-008)
The imperative `w.jobs.create` script that stood up the git-sourced Job above is NOT infra-as-code;
ADR-008 graduates it to a declarative bundle + a gated CI/CD pipeline. Design-of-record:
- **IaC = Databricks Asset Bundles (DAB).** A `databricks.yml` bundle at repo root is the sole
  declarative source of truth for the Job (tasks, `depends_on`, `git_source`, cluster-id as a
  bundle variable). `databricks bundle deploy` reconciles the workspace; the one-off SDK script is
  retired (single owner = no drift). Bundle keeps `source: GIT`, so it uploads only the Job spec —
  Databricks still pulls `pipeline/**` from the public git remote itself (the agent ships zero
  code; the ADR-002 Add #6 BANNED code-shipping shape is not re-introduced).
- **CI (extend `.github/workflows/ci.yml`):** the four governance gates PLUS a new unit-test job
  (`python -m unittest discover -s tests`). $0, no secrets, on PR + push:main.
- **CD (new `.github/workflows/cd.yml`):** `workflow_dispatch` ONLY (never `push`, never
  `schedule` — a cron here would be an in-repo scheduler, which D-10 forbids and Airflow owns).
  Runs against a GitHub Environment `databricks` (owner-approval-gated, holds `DATABRICKS_HOST` +
  `DATABRICKS_TOKEN`). Steps: `bundle validate` → `bundle deploy` → (only on a `deploy-and-run`
  input) `bundle run`.
- **Cost gate (finops):** `deploy` is free control-plane (no cluster); `bundle run` is the only
  metered path and is reachable only via an explicit manual `deploy-and-run` dispatch behind the
  Environment approval. No commit auto-runs the cluster. Credit ceiling deferred to `@finops`.
- **D-10 / Airflow:** the DAB Job carries NO `schedule`/`trigger` block — it is the control-plane
  contract surface the external Airflow (`../control_plane_lab/03_PIPELINE_SIDE_CONTRACT.md`) will
  drive as pipeline #6. CD is a deploy tool, not a scheduler.
- **Owner-action (one, blocking CD only):** create the `databricks` GitHub Environment and add the
  two secrets to it. No token ever lives in the repo (`secrets_scan.py` stays green); until this is
  done CI is fully live and CD is inert-but-valid.

## Historical-data strategies (ADR-007 D7.4)
1. **Initial load + incremental backfill** — `pipeline/extract/jdbc_batch_common.py`'s
   watermark-or-full-pull branch, made explicit via a `--full-backfill` flag on
   `postgres_extract.py`/`mssql_extract.py` for a deliberate historical re-pull.
2. **Partition pruning** — `pipeline/gold/fact_txn.py`/`fact_card_fraud.py` write
   `.partitionBy("txn_year", "txn_month")` (Landing already partitions by `dt=`, ADR-003;
   this extends the same principle to Gold facts so Snowflake/Power BI query pruning works
   end-to-end).
3. **Hot/cold hybrid** — Teradata's dual-role cold-tier (`ADR-006` Addendum #1): a native
   Teradata SQL view (pipeline/gold/cold_tier/teradata_cold_view.sql, aggregated grain
   only — never row-level, to avoid bypassing D-07 masking / D-04 MDM resolution) serves
   pre-CDC-cutover history directly; Power BI's composite model UNIONs it with Gold-sourced
   hot data. Compute for the cold path runs entirely on Teradata, never pulled through
   Databricks.

## Idempotency & rerun semantics
- Identity key: `customer_id` via `dim_customer_xwalk` for cross-source dedup at Gold; per-source
  native PK (`SK_ID_CURR`, generated PaySim `txn_id`, Berka `client_id`/`account_id`, OBP
  `account_id`) for Bronze/Silver skip-existing and MERGE keys (`journey/04_DATA_MODEL.md`
  identity section). Teradata CDC extraction keys off `_cdc_log.seq` (a monotonic offset, stored in
  the lake like the batch watermark); Salesforce (source #4) keys off the `SystemModstamp`
  high-watermark via Bulk API 2.0 (ADR-006 Add #2) — the same watermarked-incremental shape as the
  Postgres/MSSQL extractors.
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
