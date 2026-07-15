# ADR-002 — Ratified stack: Databricks (transform) + S3 (truth) + Snowflake (serving)

**Status:** Accepted
**Date:** 2026-07-05
**Owners:** owner (ratified), architect (sign-off)
**Context refs:** `01_OPUS_DECISIONS.md` D-01 (body + Addenda #1–#3) in the planning lab —
this ADR is the in-repo record of that ruling; do not re-litigate here, cite the lab doc for the
full alternatives-considered discussion.

## Context
Three stack axes needed locking before any code: compute engine, storage/truth location, serving
layer. Two earlier addenda (local-first, then Fabric-then-Snowflake) were superseded same-day by
the owner after weighing the resume claim ("Databricks · Snowflake · AWS") against the CIL
Fabric-trial-wall lesson.

## Decision
- **Compute**: ALL transform (Landing→Bronze→Silver→Gold) runs on **Databricks** — portable
  PySpark + Delta, governed by **Unity Catalog** over S3 external locations.
- **Storage**: **S3** (`s3://<bucket>/banking/`) is the sole source of truth. Both Databricks and
  Snowflake read it in place; neither owns it. Local-disk fallback (same layout) when no AWS
  credentials are available for the dev loop.
- **Serving**: **Snowflake** external tables over the Gold S3 prefix + a Power BI page (Fasa E,
  optional). **DuckDB is the $0 fallback** if no live Snowflake account exists when Fasa E runs.
- **Fabric: fully OUT** of this repo (build and serving) — not on the resume; the owner's separate
  `home-credit-fabric-migration` project uses the active Fabric trial instead.
- **Portability is mandatory, not aspirational**: no DLT, no notebook-only magic, no heavy
  `dbutils` on the critical path (enforced by `gates/boundary_contract.py` banning `import dlt`
  repo-wide). A UC-catalog-vs-path-based config switch keeps the same PySpark runnable locally
  after the disposable Databricks trial is deleted.

## Alternatives considered (and rejected — with reason)
| Alternative | Why rejected |
|---|---|
| Local-first (PySpark local mode + DuckDB, no cloud) — original D-01 body | Superseded once the owner weighed the resume's named stack (Databricks/Snowflake/AWS) against local-only; kept as the dev-loop path, not the canonical stack |
| Build ON the owner's active Fabric trial | 57-day trial clock would gate the WHOLE project timeline, and a portfolio needing a paid capacity to demo is a dead portfolio post-expiry |
| AWS Glue instead of Databricks | Serverless/simpler, but Unity Catalog's unified lineage/access/discovery is exactly the governance story this repo sells (multi-source MDM); Glue's Catalog+Lake Formation is more fragmented |
| Managed (Databricks-internal) storage instead of S3-external | Simpler day-to-day, but kills the multi-engine story — external engines (Snowflake) can't read managed tables without unload/Delta-Share |
| Databricks SQL as the only serving layer (no Snowflake) | Functionally sufficient (would cost nothing to drop) — Snowflake is kept deliberately to demonstrate the "Databricks-for-eng + Snowflake-for-serving" enterprise split as a second resume-relevant skill, not because Databricks can't serve |

## Consequences
- Locks in a two-cloud-service operating cost surface (Databricks trial + optional Snowflake
  trial) — both are explicitly disposable/timeboxed by design (D-01 Add #3), not a standing cost.
- Requires portable-PySpark discipline enforced by a gate (`gates/boundary_contract.py`
  `banned_imports.dlt`), not just a written rule — code, not vigilance.
- Does NOT decide: the exact Databricks cluster sizing, or whether Snowflake vs DuckDB is used for
  Fasa E — that's a runtime call made when Fasa E actually starts, contingent on account
  availability (see `journey/07_PIPELINE_SPEC.md`).

## Addendum log
- **2026-07-06 (Addendum #1):** owner override adds two more real (non-Databricks-transform)
  systems to the source estate — **SAP HANA Cloud** (BTP Free Tier) and **Teradata** — as the
  hosts for two of the five source systems. This does NOT change the transform/storage/serving
  decision above (Databricks still does all Landing→Bronze→Silver→Gold transform; S3 stays sole
  truth; Snowflake stays the serving veneer) — it only adds two more source systems upstream of
  Landing. Full rationale: `ADR-006-real-sap-hana-teradata-cdc-showcase.md`.

- **2026-07-14 (Addendum #2) — Databricks host: AWS → Azure. Owner override, forced by a
  provisioning blocker; ratified. Consequence: Unity Catalog does NOT govern the S3 path under
  this pairing (read-only limitation), so the Decision's "governed by Unity Catalog over S3
  external locations" (line 18) is amended below.**
  The transform engine is unchanged — **still Databricks, still portable PySpark + Delta, still
  writing to the same S3 `s3://<bucket>/banking/` as sole truth.** Only the cloud the Databricks
  workspace is *hosted in* moves from AWS to Azure. Live-verified this session (write+read+delete
  round-trip from an Azure Databricks cluster into the AWS S3 bucket; see `BUILD_REPORT.md` §12).
  - **Why the move (blocker, not preference).** The AWS-hosted Databricks path was attempted first
    (it is the same-cloud ideal — see the amended consequence below). Two AWS routes both dead-ended
    on the owner's account: (a) the instant/managed free trial provisions **only serverless SQL
    warehouses**, which cannot run this repo's `pipeline/*.py` PySpark/Delta transforms (SQL-only
    compute executes SQL, not arbitrary Python/Spark jobs); (b) the "connect your own AWS account"
    trial and the AWS Marketplace subscription both failed with *"Accounts with the free plan are
    not eligible to purchase paid offers"* — an AWS account-maturity gate (needs a verified
    payment method with purchase history), unrelated to Databricks or this project, and not
    resolvable in-session. Azure Databricks (Premium tier, required for Unity Catalog) provisioned
    cleanly into an isolated Resource Group, with a UC metastore auto-attached and a running
    single-node cluster (20-min auto-termination — the disposable-trial discipline of D-01 Add #3,
    now realized as "delete the Azure Resource Group" rather than "delete the AWS workspace").
  - **Amended consequence — the "Unity Catalog governs S3" claim is now PARTIAL, and this is a
    named gap, not a silent one.** On an **Azure-hosted** Databricks workspace, Unity Catalog can
    register an AWS S3 bucket as an external location **read-only** — this is a hard,
    Microsoft-documented platform limitation ("Support for S3 in Azure Databricks is read-only",
    learn.microsoft.com/azure/databricks .../s3-external-location-manual, verified 2026-07-14), NOT
    a configuration we can toggle or a trial restriction. It applies at any price tier. Same-cloud
    (AWS Databricks + AWS S3) would give full UC-governed read+write; the cross-cloud Azure→S3
    pairing does not. Because the medallion **writes** at every layer (Landing→Bronze→Silver→Gold),
    a read-only governed credential cannot carry the pipeline. **Resolution adopted:** S3 read+write
    uses **cluster-level Spark/boto3 credentials** (`AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY`
    as cluster env vars — the same key pair already in `.env` for the local dev loop), which is
    exactly the "path-based, UC-catalog-vs-path config switch" portability escape hatch the
    Decision (line 26–29) already mandates. **What this costs:** the Gold-layer "Unity Catalog
    governed" property named in `CLAUDE.md`'s stack table and leaned on in the "why not AWS Glue"
    rejection (unified lineage/access over the lake) does **not** hold for the S3 data path under
    this host — UC governs its own default (Azure-backed) catalog storage, but the project's S3
    Gold tables are path-based Delta, outside UC lineage/RBAC. Reading *can* still be UC-governed
    later (e.g. for BI consumers) via a read-only external location; writing cannot. The
    multi-engine "S3 as neutral truth, Databricks + Snowflake both read it in place" story
    (Decision line 19) is **fully preserved** — that never depended on UC governing the write path.
  - **What is NOT changed:** S3 remains sole source of truth; Snowflake remains the serving veneer
    (Snowflake's own AWS-region external tables over Gold S3 are unaffected — Snowflake reads S3
    natively, no UC involved); portable-PySpark discipline (no DLT, gate-enforced) is unchanged and
    now doubly load-bearing since path-based access is the actual runtime path; the disposable/
    timeboxed cost posture is unchanged (Azure Databricks bills pay-as-you-go through the Azure
    subscription, contained in one deletable Resource Group; the AWS SQL-only trial is abandoned,
    no migration needed — Databricks accounts do not share state across clouds).
  - **What this addendum does NOT decide (routed, not assumed):** whether to *also* stand up a
    read-only UC external location over Gold S3 for governed BI reads is deferred to Fasa E when/if
    Snowflake-vs-Databricks-SQL serving is chosen (Decision "does NOT decide" clause, line 45); the
    `USE_UNITY_CATALOG` switch in `.env` stays `false` (path-based) for the build/dev loop.
