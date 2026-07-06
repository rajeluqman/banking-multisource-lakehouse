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
