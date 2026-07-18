# Plan — dbt-on-Snowflake analytics layer for the marts (Fasa E serving)

**Status:** ADR-000 steps 1-3 complete — Propose + Clarify + **`@staff-data-engineer` step-3
ruling done (2026-07-17): APPROVED to proceed, conditional on the 4 corrections now reflected
below** (owner chose "build dbt / retire the PySpark marts", not add-alongside). **Awaiting
`@scope-guardian` fresh scope sign-off** (step 3 continues — scope veto is a separate authority
from the technical ruling). No code written. No ADR-002 amendment committed yet — corrected draft
appended below for `@scope-guardian` + owner review.
**Governing ADR(s):** ADR-002 (locked stack — this proposal would amend its "Snowflake never
writes" clause, pending ruling). Cites journey/02_BUSINESS_QUESTIONS.md (all 10 BQs, serving-
mechanism change, not a new BQ) and journey/08_SERVING_AND_EVIDENCE.md (Fasa E, currently
optional/not started).
**Intake initiated:** 2026-07-17, owner request, following ADR-000 §Decision steps 1-2.

---

## Step 1 — Propose

Add **dbt** (`dbt-snowflake` adapter) as a new analytics-engineering layer, scoped **exclusively**
to the analytics-marts layer of Fasa E's serving veneer — never Silver, never masking (D-07),
never the MDM crosswalk (`dim_customer_xwalk`). dbt **retires and re-authors the 8 analytics
marts** (`mart_customer_360`, `mart_fraud_daily`, `mart_fraud_followup`, `mart_loan_funnel`,
`mart_risk_segment`, `mart_cross_sell`, `mart_dormancy`, `mart_daily_flows` — the BQ-01..08
answers) as versioned, tested, documented SQL models, materializing as **Snowflake VIEWS by
default** (derived tables only where a view is provably too slow, justified per-model). **`mart_pipeline_health`
(BQ-10) is DELIBERATELY EXCLUDED** — it is pipeline operational/reconciliation metadata
(journey/04:35), coupled to Spark run state, not a BQ analytics aggregation; it stays Spark-native
and is named-as-out, not silently absent (`@staff-data-engineer` step-3 ruling Q3). So of
`pipeline/gold/*.py`'s 16 Gold tables, exactly 8 (the analytics marts) move to dbt; the 7 facts/dims
plus `mart_pipeline_health` stay Spark-built. dbt sources read from the already-existing Gold
fact/dim tables (`fact_txn`, `fact_card_fraud`, `fact_loan_application`, `dim_customer`,
`dim_customer_xwalk`, `dim_date`, `dim_fx_rate`) via **read-only external tables over S3 — NO
physical copy into Snowflake** (no Snowpipe/`COPY INTO`; S3 stays sole physical truth). No
duplicate transformation of identity resolution, currency normalization, or masking logic — those
stay single-path in Spark; the 8 marts get exactly ONE authoring home (dbt), eliminating the
duplicate-transformation-path anti-pattern.

**Serves**: all 10 locked BQs (journey/02) — this is a serving-MECHANISM change (Fasa E,
explicitly optional, not yet started per `journey/08_SERVING_AND_EVIDENCE.md`), not a new
business question, so it does not expand the locked BQ-01..10 scope. It does add dbt — a
distinct, heavily-demanded skill not otherwise demonstrated in this repo's PySpark-only
transformation story — alongside the already-shown Databricks/Snowflake/AWS stack (per
`@staff-data-engineer`'s prior market-demand ruling, 2026-07-17 session).

---

## Step 2 — Clarify (unresolved questions, per ADR-000 §Decision item 2)

- Q1: Does dbt materializing tables inside Snowflake require reversing ADR-002's "Snowflake never
  writes" clause (`journey/09_SECURITY_AND_ACCESS.md` line 23: "S3 read-only key (serving) |
  Snowflake external tables | ... never write"), or is there a design where dbt's writes stay
  confined to a clearly-labeled derived/non-authoritative schema that doesn't contradict "S3 is
  sole source of truth"? → A: **RESOLVED (`@staff-data-engineer`, 2026-07-17)** — needs the
  amendment; no design has dbt materialize in Snowflake without touching "never writes." BUT the
  amendment is kept as NARROW as possible: dbt marts are **VIEWS** over the external tables by
  default (a view is DDL, not a second copy of truth — honors the serving = view doctrine,
  journey/04:47); a derived TABLE is allowed only where a view is provably too slow, justified
  per-model.
- Q2: Does dbt read from S3 via the EXISTING external-table pattern (nothing physically copied
  into Snowflake), or does Silver/Gold get physically copied into Snowflake-native storage (via
  Snowpipe/`COPY INTO`) first? → A: **RESOLVED** — **external tables, NO physical copy.**
  Snowpipe/`COPY INTO` would create a second physical truth and violate ADR-002's "S3 sole truth"
  (lines 20, 97). dbt reads facts/dims via the existing read-only external tables; marts are views
  on top.
- Q3: Does adding dbt **retire** the existing `pipeline/gold/mart_*.py` PySpark builders, or do
  both coexist? → A: **RESOLVED — RETIRE, mandatory. Coexistence is a VETO** (the
  duplicate-transformation-path anti-pattern — same aggregation authored twice, guaranteed drift).
  Marts get exactly one authoring home: dbt. **BUT 8 marts, not 9** — `mart_pipeline_health`
  (BQ-10) is pipeline operational/reconciliation metadata (journey/04:35), tightly coupled to
  Spark run state, NOT a BQ analytics aggregation → it stays Spark-native and is explicitly OUT of
  dbt's scope. dbt owns the 8 analytics marts (BQ-01..08).
- Q4: Which artifact is the "source of truth" for BQ evidence? → A: **RESOLVED** — after
  retirement each mart exists in exactly ONE place, so no disagreement is possible: BQ-01..08
  evidence → Snowflake dbt models; facts/dims + BQ-09/BQ-10 → S3/Spark. journey/08_SERVING_AND_EVIDENCE.md must be
  updated to record this per-BQ split — a **condition of approval**, not a follow-up.
- Q5: Cost — does `@finops-agent` need a warehouse-size + auto-suspend policy before any dbt run?
  → A: **CONFIRMED sequencing** — route warehouse-size + auto-suspend to `@finops` AFTER this
  ruling and AFTER `@scope-guardian`, before any dbt run.
- Q6: Scope classification — "Fasa E done differently" or needs fresh `@scope-guardian` sign-off?
  → A: **RESOLVED (staff-DE input; `@scope-guardian`'s call)** — MORE than "Fasa E done
  differently." It introduces a new tool/language, retires 8 governed builders, and amends a
  locked ADR → warrants `@scope-guardian`'s **fresh** sign-off, NOT auto-cover under prior Fasa E
  approval. Routing now.
- Q7: Two-stack cost vs portfolio value — formal judgment? → A: **RESOLVED — YES, worth it,
  BECAUSE of the retire ruling.** Coexistence would make it NO (redundant, drift). Retirement
  collapses the marts to one home, eliminating the "two stacks for the same logic" cost the
  question feared — what remains is one metered warehouse + one dbt CI path, bounded. Placed this
  way (Spark-EL/MDM + dbt view-based analytics-engineering over external tables) it demonstrates
  the CORRECT modern pattern, not redundancy. dbt is a distinct, heavily-demanded skill not
  otherwise shown here. Approved on the merits.

---

## Step 3 — Ruling

**`@staff-data-engineer` (technical/model-fit ruling, Opus, 2026-07-17): NEEDS the ADR-002
amendment — APPROVED to proceed to `@scope-guardian`, conditional on 4 corrections (all now
applied above and below). Not "fits as-is"; not rejected.** Reasoning: the proposal as originally
written contained the exact anti-pattern it claimed to avoid (coexistence, physical-copy
ambiguity, table-materialization); the 4 corrections fix it. Full answers to Q1-Q7 recorded in
Step 2 above. The owner then chose to proceed with the retire-and-replace (Option A), not
add-alongside.

**Four corrections imposed by the ruling (all reflected in this plan):**
1. dbt marts are **VIEWS by default** (serving = view doctrine, journey/04:47), tables only where
   a view is provably too slow, per-model justified — NOT unconditional "native tables."
2. **External tables, NO physical copy** — no Snowpipe/`COPY INTO` (S3 stays sole physical truth).
3. **RETIRE the 8 analytics-mart PySpark builders (coexistence = VETO); `mart_pipeline_health`
   (BQ-10) stays Spark-native, excluded** — 8 marts move to dbt, not 9.
4. journey/08_SERVING_AND_EVIDENCE.md evidence paths updated to the per-BQ split (BQ-01..08 → dbt/Snowflake; BQ-09/10 +
   facts/dims → S3/Spark) — a **condition of approval**, built in Phase 2 below, not deferred.

**`@scope-guardian` (scope ruling, 2026-07-17): DEFERRED — not approved.** Full reasoning recorded
in `governance/BACKLOG.md`'s "Deferred" table (skill-showcase motive not a capability gap; retires
8 working just-fixed builders for zero net new capability; reopens ADR-002's locked
"Snowflake never writes" clause absent a functional deficiency; disproportionate for an optional
Fasa E layer).

**Owner override (2026-07-17, same day): PROCEED anyway.** Per `@scope-guardian`'s own stated
mandate, only the owner may authorize an override, recorded as a new dated entry not a silent
edit — see `governance/BACKLOG.md`'s "Superseded deferrals" table. Owner weighed the deferral and
judged dbt's portfolio skill-demonstration value (distinct, heavily-demanded, not otherwise shown
in this PySpark-only repo) to outweigh the rework cost. `@staff-data-engineer`'s conditional
technical ruling (views-only, external-tables-only/no physical copy, `mart_pipeline_health`/BQ-10
excluded) stands as the binding design for the build.

**`@finops` cost sign-off (2026-07-18): APPROVED, with guardrails.** Warehouse: **X-Small**, no
larger justified (single dev, no concurrency, largest scan `fact_txn` ~6.36M rows is trivial for
XS; views cost nothing at DDL time, only at query time). **Auto-suspend: 60s, auto-resume: ON**
(bursty ad-hoc dev-loop work, not kept-warm). Required guardrails before any `dbt build` runs:
(1) a **resource monitor** with a low single-digit monthly credit quota, notify at 75%,
**suspend-and-require-owner-action at 100%** (the real backstop given finite trial credits and
this project's prior Databricks cost incidents); (2) `STATEMENT_TIMEOUT_IN_SECONDS=300` as a
runaway-query guard; (3) **single-cluster warehouse, no multi-cluster** (no concurrency need);
(4) **re-review trigger**: this approval covers views-only — if any of the 8 models later needs
to become a derived TABLE (per staff-DE's per-model exception), that specific model returns to
finops before materializing, not silently absorbed under this sign-off. Roster note: Snowflake
compute is now a real metered cost for this project (alongside Databricks/Kaggle) — `@finops`
stays on the roster.

---

## ADR-002 Addendum (corrected per staff-DE ruling — NOT committed; appended to ADR-002 only after `@scope-guardian` sign-off, before any code, per ADR-000 step 4)

> **Addendum #N — dbt-on-Snowflake for the analytics marts layer only.** Narrowly amends
> "Snowflake never writes": a dedicated non-authoritative schema (`ANALYTICS.DBT_MARTS`) may hold
> **dbt-managed views** over the read-only external tables on `s3://.../banking/gold/`; **derived
> tables only where a view is provably too slow, justified per-model.** The external-table schema
> stays read-only and is dbt's ONLY source. **No physical ingest** (no Snowpipe/`COPY INTO`) — S3
> remains sole physical truth for Silver, facts, and dimensions. dbt becomes the **sole** authoring
> path for the 8 analytics marts (BQ-01..08); the corresponding `pipeline/gold/mart_*.py` builders
> are **retired** from the orchestration/DoD (code retained in git history for reversibility, S3
> mart tables dropped only AFTER dbt marts are verified). **`mart_pipeline_health` (BQ-10) is
> explicitly OUT — stays Spark-native** (pipeline run metadata/reconciliation, not analytics).
> journey/08_SERVING_AND_EVIDENCE.md evidence paths updated to the per-artifact split.

---

## Phases (pending `@scope-guardian` sign-off — not started)

| Phase | Output | Validation gate | Status |
|---|---|---|---|
| 1 | `@scope-guardian` scope sign-off; `@finops` warehouse-size + auto-suspend policy | Plan Step 3 `@scope-guardian` line filled; finops policy recorded | **done** (2026-07-18) |
| 1b | **ADR-005 Addendum #4 (2026-07-18) — 5 new Gold objects so dbt reads Gold-only, never Silver.** Discovered mid-build: 6/8 marts read Silver directly (security-boundary conflict). `@staff-data-engineer` vetoed exposing Silver, ruled Option B (promote to Gold); `@scope-guardian` approved the volume as debt-paydown (no fresh ADR-000). 5 new Spark builders + `sources.yml` grows 7→12 external tables (all still Gold). | ADR-005 Add #4 committed + `journey/04_DATA_MODEL.md` grains added BEFORE builders; 5 new Gold tables deployed + verified vs S3; 5 new external tables created + row-count verified | **in progress** (2026-07-18) |
| 2 | ADR-002 addendum committed; `dbt-snowflake` project scaffolded (`dbt_project.yml`, `profiles.yml`, `sources.yml` over the **12** external tables); 8 dbt mart models (views) authored + `dbt test` (not_null/unique on keys) + `dbt docs`; journey/08 per-BQ evidence split recorded | `dbt build` green; each dbt mart's output reconciles to its retiring PySpark mart's current S3 numbers BEFORE the PySpark builder is removed | not started |
| 3 | Retire the 8 `pipeline/gold/mart_*.py` builders from orchestration (`databricks.yml`/`orchestrate_config.yml`) + DoD; drop the 8 S3 mart tables ONLY after Phase-2 reconciliation passes | 4 gates green; `mart_pipeline_health` still Spark-built and reconciling; one Databricks run confirms the 8 removed tasks are gone cleanly | not started |

## Rollback
Corrected per the retire ruling (the original "additive, DROP SCHEMA only" claim was wrong under
retire-and-replace): rollback = git-revert the builder removals + the ADR-002 addendum + the
`databricks.yml`/orchestrate changes, then ONE Spark rebuild to repopulate the 8 S3 mart tables.
Reversibility stays HIGH (all builder code retained in git history throughout; facts/dims and the
S3 truth layer untouched), but it is NOT the trivial single `DROP SCHEMA` originally claimed. The
Phase-3 physical S3-mart drop is deliberately sequenced AFTER Phase-2 dbt verification so
reversibility stays cheap during cutover (revert before Phase 3 = nothing to rebuild).
