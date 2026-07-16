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

- **2026-07-15 (Addendum #3) — Live-attempted the first real canonical-run write, found the S3
  gap is DEEPER than Addendum #2 predicted: not read-only, but blocked outright on this cluster's
  access mode. Named, not silently worked around; cluster terminated, no data written.**
  Live-tested (`banking-lakehouse-cluster`, Azure, `USER_ISOLATION`/UC-governed, single-node):
  attempted `df.write.format("delta").save("s3://banking-lakehouse-pipeline/banking/gold/
  dim_fx_rate")` via `databricks-sdk` command execution.
  - **Finding #1 (expected, now confirmed live):** first attempt hit
    `UnsupportedOperationException: Writing to Delta table on AWS from non-AWS is unsafe` —
    Databricks' own cross-cloud Delta transaction-log safety guard
    (`spark.databricks.delta.logStore.crossCloud.fatal`). Owner-authorized disabling it (safe for
    this single-writer, no-concurrency pipeline, per Databricks' own documented escape hatch) —
    cluster edited (`spark_conf` + `kind=CLASSIC_PREVIEW`/`is_single_node=True` required together,
    a v2 clusters-API shape quirk not in the SDK docs) and restarted. Guard confirmed off on
    re-check.
  - **Finding #2 (NOT predicted by Addendum #2 — a real, deeper gap):** with the guard off, the
    write still failed: `CloudAccessDeniedException ... 403 Forbidden ...
    credentials-provider: AnonymousAWSCredentials, credential-header: no-credential-header`.
    Addendum #2 predicted UC would make the S3 path **read-only**; live evidence shows UC's
    governed filesystem (`CredentialScopeFileSystem`/`LokiS3FS`) on a `USER_ISOLATION` cluster
    intercepts the S3 path entirely and attempts **anonymous** access — it does not fall through
    to the cluster-level `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY` env vars Addendum #2 already
    has configured on this exact cluster. No UC `STORAGE CREDENTIAL`/`EXTERNAL LOCATION` is
    registered for this bucket, so UC has nothing to vend and falls back to anonymous rather than
    ignoring governance and using the plain env-var credentials.
  - **Not attempted further, and why**: the mechanical next step (force raw S3A auth via
    `spark.conf.set("fs.s3a.access.key", ...)` inline in the remote command) would embed the raw
    AWS secret literally in a Databricks command-execution payload, which persists in that
    cluster's command history — a real credential-exposure anti-pattern, not just a permission
    formality. The actually-correct fix is registering a Unity Catalog `STORAGE CREDENTIAL` +
    `EXTERNAL LOCATION` for `s3://banking-lakehouse-pipeline/` (an admin/console action) or
    referencing a Databricks Secret Scope instead of a literal value — both out of a notebook
    command's reach, deferred rather than hacked around.
  - **Resolution: cluster terminated, zero data persisted to S3 this session** (disposable-trial
    discipline, D-01 Add #3 — stop the cost clock rather than keep trial-and-erroring on a running
    cluster). `s3://banking-lakehouse-pipeline/banking/` remains empty; "real S3 writes never
    verified" (`CLAUDE.md` "Known blocker") is still true, now with a precise, live-diagnosed
    reason instead of an assumed one.
  - **What this unlocks for the next attempt**: register a UC `STORAGE CREDENTIAL` (IAM role or
    the existing access key pair, whichever the owner's AWS account supports) + `EXTERNAL
    LOCATION` for the bucket via the Databricks account console (owner-only, cannot be done from
    a notebook/API with current permissions), OR switch the cluster's `data_security_mode` off UC
    governance (`SINGLE_USER`/`NONE`) if governed access isn't actually needed for this dev-loop
    trial — a scope/security tradeoff for the owner to rule on, not a silent code fix.

- **2026-07-15 (Addendum #4) — Owner did the console work Addendum #3 named; the UC
  read-write path is now DEFINITIVELY closed, confirmed via the UI itself, not inferred from
  a doc. Decision: proceed on `SINGLE_USER` cluster mode (Plan B), owner-ruled.**
  Owner-side work this pass (all console/IAM, correctly out of this session's own reach per
  `CLAUDE.md`): created IAM role `arn:aws:iam::579880301047:role/databricks-uc-role-
  banking-lakehouse` with a trust policy naming Databricks' Azure-specific UC role
  (`arn:aws:iam::414351767826:role/unity-catalog-prod-UCAzureMainRole-1AJ6UQSSB8F0Q`) +
  External ID condition, and a permissions policy granting `s3:GetObject`/`PutObject`/
  `DeleteObject`/`ListBucket`/`GetBucketLocation` scoped to the bucket — i.e. **the AWS-side
  IAM configuration is fully correct and grants real read+write**. Registered a matching UC
  Storage Credential (`databricks-uc-role-banking-lakehouse`) and External Location
  (`databricks-uc-s3-banking-lakehouse-external-location`) in the Databricks account console.
  - **First surprise**: the created Storage Credential showed `Limit to read-only use:
    Enabled`, immutable after creation (no edit toggle). Assumed fixable by creating a second
    credential with the box left unchecked at creation time.
  - **Definitive finding**: creating a *new* Storage Credential, the `Credential Type`
    dropdown offers exactly two options — `AWS IAM Role (Read-only)` and `Azure Managed
    Identity` (for ADLS). **There is no read-write AWS IAM Role option anywhere in this UI.**
    This is not a checkbox left on, not a misconfiguration, not something the IAM policy
    controls — this Azure-hosted Databricks account structurally cannot vend a read-write AWS
    S3 credential via Unity Catalog, full stop. This *is* the Microsoft-documented limitation
    Addendum #2 cited, now confirmed by direct UI inspection rather than by a doc URL alone.
  - **Kept, not wasted**: the read-only Storage Credential + External Location remain useful
    for future READ paths (e.g. Snowflake/Fasa E serving reading Gold from S3, or verification
    queries) — this dead-end is scoped to the WRITE path specifically.
  - **Decision (owner ruling, pros/cons discussed explicitly before deciding)**: proceed with
    `SINGLE_USER` cluster access mode for the pipeline-writing cluster, bypassing UC governance
    for that cluster's S3 writes, using the cluster's existing `AWS_ACCESS_KEY_ID`/
    `AWS_SECRET_ACCESS_KEY` env vars directly. **Named consequence, not silently accepted**: a
    table written this way is not automatically a UC-registered catalog object, so
    `journey/09_SECURITY_AND_ACCESS.md` §3's RBAC `GRANT`/`REVOKE` role matrix (R-31) does not
    apply to it automatically — closing that gap requires a follow-up step (registering the S3
    path as a UC external table via `pipeline/gold/grants/`'s existing DDL pattern) that is
    still pending, not yet done as of this addendum.
  - **Status at end of this pass**: cluster terminated again (compute-cost discipline), `SINGLE_
    USER` mode not yet applied, S3 bucket still empty — decision made, execution deferred to
    the next session. See `PROJECT_STATUS.md` "▶ RESUME HERE" for the exact resume point.

- **2026-07-16 (Addendum #5) — Plan B EXECUTED and PROVEN. Real S3 writes now work end-to-end;
  `CLAUDE.md` "Known blocker" (real S3 writes never verified) is RESOLVED. Two genuinely new
  live findings recorded precisely. Owner-authorized, `@staff-data-engineer` signed off (twice
  this session).**
  Executed Plan B on cluster `banking-lakehouse-cluster` (`0715-022729-6j0g8jhn`): created
  Databricks secret scope `banking-lakehouse-s3`, loaded the AWS key pair by env-var reference
  (`{{secrets/banking-lakehouse-s3/...}}` templating — **no literal secret ever entered a command
  payload / command history**, closing the anti-pattern Add #3 flagged), edited the cluster to
  `data_security_mode=SINGLE_USER` with `spark_env_vars` referencing those secrets.
  - **New finding #1 (NOT predicted by Add #3/#4): `SINGLE_USER` mode does NOT bypass a
    registered read-only UC External Location.** First write to `/banking/gold/dim_fx_rate`
    failed with a NEW error (not Add #3's anonymous-access):
    `UnauthorizedAccessException: PERMISSION_DENIED: User cannot write to a read-only external
    location databricks-uc-s3-banking-lakehouse-external-location`. UC's `ResolveWithCredential`
    intercepts any path under a registered External Location BEFORE the cluster's env-var S3A
    creds are consulted, even on `SINGLE_USER`. UC bypass is total only when NO External Location
    is registered over the path — not merely because the cluster is `SINGLE_USER`. Confirmed via
    API that the ext-loc `databricks-uc-s3-banking-lakehouse-external-location` covered
    `s3://banking-lakehouse-pipeline/banking` (`read_only=True`).
  - **New finding #2 (the structural contradiction on this host):** we hold ONLY a read-only S3
    Storage Credential (read-write is impossible per Add #4). Writing Gold via `SINGLE_USER` S3A
    requires NO External Location over the path; registering a UC external table for R-31
    read-governance requires an External Location over the path. **Mutually exclusive at the same
    prefix.** Resolved per `@staff-data-engineer` ruling **Option (a)**: this is the Add #2
    canonical resolution (Gold = path-based Delta, outside UC lineage/RBAC), not a new decision.
  - **Proof-of-mechanism (before touching `/banking`):** the identical write to
    `s3://banking-lakehouse-pipeline/_writetest/dim_fx_rate` (OUTSIDE any ext-loc) SUCCEEDED —
    Databricks read-back 4 rows + null sentinel preserved; independent `boto3` confirmed 7 S3
    objects (`_delta_log` + 4 parquet, 17,716 bytes). The write mechanism itself was sound; only
    the ext-loc blocked the governed path.
  - **Action taken (owner explicitly confirmed the specific destructive delete; reversible):**
    dropped External Location `databricks-uc-s3-banking-lakehouse-external-location`. This is UC
    **metadata only — zero S3 objects deleted**; the IAM role `databricks-uc-role-banking-
    lakehouse` and the Storage Credential of the same name were KEPT, so a read-only ext-loc is
    re-creatable in minutes for a future Fasa-E read path (Snowflake serving does not need it — it
    reads S3 via its own storage integration, `journey/09_SECURITY_AND_ACCESS.md` §1 / R-32). This SUPERSEDES Add #4's
    "kept, not wasted" framing (lines 175–177) for the WRITE path.
  - **Result — REAL canonical write PROVEN:** with the ext-loc gone, writing `dim_fx_rate` to the
    real `s3://banking-lakehouse-pipeline/banking/gold/dim_fx_rate` SUCCEEDED. Databricks
    read-back: 4 rows, 1 NULL sentinel (`unitless`) preserved, all four currencies correct.
    Independent `boto3`: 8 S3 objects, `_delta_log` + parquet present. `_writetest` proof artifact
    then cleaned up (0 objects remaining); cluster terminated (D-01 Add #3 cost discipline).
  - **R-31 status (named, not silently dropped — `@staff-data-engineer` ruling Q4):** R-31's
    actual bar (`journey/09_SECURITY_AND_ACCESS.md` §3: raw Landing/Bronze hold unmasked PII, no analyst/serving role may
    read them) is honored as **documented-and-path-based**: raw layers are never registered as UC
    objects and hold no analyst-reachable credential; the write creds live only on the
    `SINGLE_USER` cluster's secret scope. The full live-UC-`GRANT`/`REVOKE` demonstration is
    achievable only same-cloud (AWS Databricks + AWS S3) and is DEFERRED, named here per doctrine.
    A one-shot `CREATE EXTERNAL TABLE` over frozen Gold Delta (recreate read-only ext-loc → grant
    → screenshot for `journey/08_SERVING_AND_EVIDENCE.md`) remains available as a transient snapshot AFTER a real
    canonical run exists — not now (only the `dim_fx_rate` seed table writes this session).
  - **Corrected stale fact:** `CLAUDE.md` "Known blocker" also claimed no Kaggle API credentials
    exist. Live-tested this pass — `.env` now carries working `KAGGLE_USERNAME`/`KAGGLE_KEY`,
    `kaggle datasets list` authenticates (exit 0). Real-data download is now unblocked; a full
    multi-source canonical ingest remains a separate scoped effort (needs `@finops`/`@scope-
    guardian`), not started this session.
