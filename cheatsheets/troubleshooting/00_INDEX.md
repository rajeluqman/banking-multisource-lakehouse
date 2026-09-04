# Troubleshooting Library — banking-multisource-lakehouse (INDEX)

> Failure-path twin of the optimization library. One card per failure mode, per phase. Symptom
> presented FAR from root → trace backward (observability-first). Content is English.

> ## 🚧 STATUS: 4 real cards (2026-09-04) — still one file, still grows on real incidents only
> **This INDEX file *is* the entire troubleshooting artifact for now.** It is NOT split into
> per-phase files yet — that would be premature sprawl. Under the split rule below, the busiest
> phase (transformation) holds 2 real cards, short of the ≥3–4 threshold.
> - **Gate:** a real incident card is authored only AFTER a real incident is hit during the build.
> - **Owner:** @senior-data-engineer (build/diagnosis); @staff-data-engineer is Incident
>   Commander under the TWO-STRIKE rule (ADR-009) for any stage failing twice.
> - **Authoring rule:** every ✅ HARDENED card cites a real `file:line` from the actual fix.
>   **No fabricated incidents, no invented citations.** A 🟡 APPLICABLE seed is a real, undrilled
>   pattern — NOT a claim that an incident happened.
> - **Split rule (lazy):** promote cards into their own `0N_<phase>.md` file only when that one
>   phase earns **≥3–4 real cards** — split on volume, never preemptively on taxonomy.

## Binding reality note (read first)
This project **does** run Databricks portable PySpark + Delta (unlike a DuckDB-only stack). So
generic Spark advice applies directly — but two things are specific to THIS platform:

| Generic DE troubleshooting | This project's reality |
|----------------------------|------------------------|
| "Check the Spark UI / stage timeline" | Databricks Spark UI + Delta history (`DESCRIBE HISTORY`); local Spark repro needs `JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64` |
| "Executor OOM / shuffle spill" | `spark.sql.shuffle.partitions` sizing; skew on the MDM join key |
| "Run succeeded, data looks off" | **Never trust run SUCCESS — verify at the ARTIFACT level** (read the Delta output). ADR-009. |
| "No shared key across tables" | the MDM crosswalk (`dim_customer_xwalk`, D-04) — the identity layer is the #1 suspect for count anomalies |
| Same stage fails twice | **STOP paid execution — TWO-STRIKE rule (ADR-009).** @staff-data-engineer as Incident Commander before any further cluster run. |

## Card format (copy this)
```
### <ID> — <symptom, far from root>
- **Phase:** triage | ingestion | extraction | transformation | load | validation | orchestration | cicd | postmortem
- **Status:** ✅ HARDENED (fix cited) | 🟡 APPLICABLE (real, undrilled)
- **Provenance:** discovered | injected   <- MANDATORY, see below
- **Symptom (business/observability):** what a stakeholder/monitor sees first.
- **Backward trace:** observable → … → root.
- **Root cause:** the actual defect.
- **Fix / guard:** `path/to/file:LN` (✅ only).
- **Junior mistake:** the wrong first move.
```

### Provenance — mandatory on every card
`discovered` = the incident arose on its own during the build. `injected` = it was deliberately
induced as a training drill. The diagnosis, the fix and the `file:line` are **equally real either
way**; only the ORIGIN differs — and the claim language differs with it.

`DE-Mastery/Interview-Prep/integrity-rules.md` Rule 2 is binding: an `injected` card may be
claimed as *"a training drill I built to practise identifying anti-patterns"* and **never** as
*"a production incident I handled."* Without this field a structured card format is persuasive
enough to launder a drill into a war story, which is precisely what the authoring rule above
exists to prevent. Mirrors the `provenance` key on each `learning/INCIDENTS.jsonl` record.

**Every card below is `discovered`. No `injected` card exists — the BREAK/fault-injection
programme is deferred (see `BANKING-FRAMEWORK-RECON.md` §9.1).**

## Phase map (planned files — none split out yet; see split rule above)
Cards are authored inline in this single doc until a phase earns its own file (split rule above).
A ⬜ phase has had no real incident yet — it is not a backlog.

| File | Phase | Status | Cards | Example failure modes for this project |
|------|-------|--------|-------|----------------------------------------|
| `01_triage.md` | Triage | ⬜ gated | 0 | "Customer-360 mart empty" / "fraud BQ returns nothing" — where to look first |
| `03_ingestion.md` | Source→Landing | ⬜ gated | 0 | watermark not advancing, CDC-poll `_cdc_log` gap, 0-byte export, connection creds |
| `05_transformation.md` | Silver/Gold | 🟢 inline | 3 | TS-ORDER-01, TS-MONEY-01, TS-MONEY-02 (+ TS-XWALK-01 seed, 🟡 undrilled) |
| — | Load | 🟢 inline | 1 | TS-UPSERT-01 — no planned file; add one if this phase reaches the split threshold |
| `06_validation.md` | DQ | 🟢 inline | 1 | TS-GRAIN-01 |
| `07_orchestration.md` | Orchestration | ⬜ gated | 0 | skip-existing not firing, re-run non-idempotent, partial batch |
| `09_postmortem.md` | Postmortem | ⬜ gated | 0 | two-strike incident write-up (ADR-009) |

## Seed card
### TS-XWALK-01 — "Customer-360 shows more customers than the bank actually has"
- **Phase:** transformation
- **Status:** 🟡 APPLICABLE (real pattern for this project, undrilled)
- **Symptom:** the Customer-360 mart reports a customer count higher than any single source, and
  some "customers" have activity in only one system.
- **Backward trace:** inflated count → Gold facts FK to distinct `customer_id`s that are really
  the SAME human → `dim_customer_xwalk` failed to resolve that identity across sources → the four
  sources share no common key, so a fuzzy/deterministic match rule missed a link.
- **Root cause:** identity resolution gap in the MDM crosswalk — one human became N customers.
- **Fix / guard:** strengthen the resolution rule in `seed/build_xwalk.py`; re-derive the xwalk at
  real full scale (this actually happened once — see the BQ-09 rebuild in git history).
- **Junior mistake:** trusting the raw per-source customer IDs as if they were global keys.

---

## Real cards — all `provenance: discovered`

> Authored 2026-09-04 from incidents that actually occurred. None was injected, so the authoring
> gate at the top of this file is satisfied. Ledger: `learning/INCIDENTS.jsonl` INC-0001…0005.
> Each card separates **Observed** (supported by repo evidence), **Root cause (diagnosis)** (what
> the code proves) and **Lesson (interpretation)** — an interpretation is never stated as an
> observation.

### TS-GRAIN-01 — "The customer-grain fact has more rows than it has customers, and every test is green"
- **Phase:** validation
- **Status:** ✅ HARDENED
- **Provenance:** `discovered` — found by an independent re-audit on 2026-07-18 that re-read the code and re-queried live data rather than trusting the build session's own narrative. Ledger `INC-0001`.
- **Symptom (business/observability):** `fact_repayment_behavior` declared the grain *one row per `customer_id`* (`journey/04_DATA_MODEL.md:35`), the dbt `unique` test on that key passed, and the mart built cleanly. Nothing reported a problem.
- **Observed:** 47,180 rows carried a **NULL** `customer_id` (`PROJECT_STATUS.md:10-11`, `:90`). The dbt `unique` test passed anyway.
- **Backward trace:** grain declared → `unique` green → row count ≠ distinct customer count → keys are NULL, not duplicated → NULLs are unmatched `home_credit` crosswalk rows (R-29 late-arriving dimension, `PROJECT_STATUS.md:90`).
- **Root cause (diagnosis):** SQL `UNIQUE` semantics treat NULLs as mutually distinct, so dbt's generic `unique` test **excludes NULL keys from the assertion by construction**. The test was not broken and did not "miss" the rows — it is structurally incapable of seeing this defect. A grain is expressed by `unique` **and** `not_null` together; only one was in place.
- **Fix / guard:** commit `9e5eb19` — grain enforced explicitly rather than inferred from `unique`; the three poisoned Silver prefixes deleted from S3 (its own card, TS-UPSERT-01). Guard asserted in `pipeline/gold/fact_repayment_behavior.py` per `journey/06_DQ_PLAN.md` (exact guard line not re-verified this session). Re-verified against S3 and live Snowflake: 287,530 rows / 0 NULL `customer_id` / 0 duplicates (`PROJECT_STATUS.md:21-22`).
- **Junior mistake:** reading a green `unique` test as proof of key integrity, and concluding the grain holds.
- **Lesson (interpretation):** **a passing uniqueness test does not necessarily prove key integrity when NULL semantics are involved.** Pair `unique` with `not_null` on every declared grain key — they test different things, and only their conjunction states "one row per X".
- **Soundbite:** *"Our customer-grain fact had 47,180 NULL keys and the uniqueness test still passed — SQL treats NULLs as distinct, so `unique` can't see them. A grain needs `unique` AND `not_null`."*

### TS-UPSERT-01 — "The bug was fixed and redeployed, and the bad rows were still there"
- **Phase:** load
- **Status:** 🟡 APPLICABLE — remediated operationally, **not** fixed in code. Ledger `INC-0002` deliberately stands at `diagnosed`.
- **Provenance:** `discovered` — surfaced while remediating TS-GRAIN-01.
- **Symptom (business/observability):** the corrected transform was deployed and re-run, yet previously-written bad rows persisted in Silver and kept flowing to Gold.
- **Observed:** a real S3 deletion of **3 poisoned Silver prefixes** was required, *"because `merge_upsert` never deletes — the pre-fix orphan rows would have stayed forever otherwise"* (`PROJECT_STATUS.md:18-20`), noted there as *"the same lesson as the BQ-10 'delete and recreate' precedent"*.
- **Backward trace:** bad rows survive a green re-run → the re-run wrote only currently-produced keys → prior rows were never targeted → the writer has no delete branch.
- **Root cause (diagnosis):** `pipeline/silver/common.py::merge_upsert` issues `whenMatchedUpdateAll()` + `whenNotMatchedInsertAll()` and nothing else. That is deliberate (D-06 soft-delete semantics; hard-delete replay deferred to Fasa C per R-25) — but it means **a row written by a defective run is never retracted by a corrected run**. Re-running heals rows whose keys are still produced; it cannot heal rows whose keys are not.
- **Fix / guard:** none in code. Remediated by manual S3 deletion of the affected prefixes. The no-delete property is unchanged and intentional, so this card stays 🟡 rather than ✅.
- **Junior mistake:** assuming "deploy the fix and re-run" is a recovery strategy. It is a *forward* correction; it is not retraction.
- **Lesson (interpretation):** an upsert-only writer makes bad rows permanent. Any pipeline whose recovery story is "re-run it" needs an explicit retraction path — delete-and-recreate, or tombstones — decided **before** the incident, or the deletion gets done by hand under pressure.
- **Soundbite:** *"Our Silver writer is upsert-only by design, so a corrected re-run couldn't retract what a defective run wrote. We deleted three prefixes by hand. Recovery needs a retraction path, not just a fix."*

### TS-ORDER-01 — "The same input produced a different row on a re-run, and nothing failed"
- **Phase:** transformation
- **Status:** ✅ HARDENED
- **Provenance:** `discovered` — found by code inspection during the framework reconciliation, 2026-09-04. Ledger `INC-0003`.
- **Symptom (business/observability):** none available at the time. This defect has no symptom until two runs over identical input are compared — which is exactly why it survived review.
- **Observed (pre-fix code):** three independent "one row per key" selections whose ordering was not a total order — `pipeline/silver/common.py:54-55` fell back to `order_cols = [lit(1)]`, a constant on which every row ties; `latest_state_from_cdc_log` used `orderBy(seq.desc()).groupBy("pk_value").agg(first(...))`; `pipeline/gold/common.py:40` ordered `latest_balance_per_account` by `date.desc()` alone, on day-granular Berka data where an account routinely has several transactions per day.
- **Backward trace:** irreproducible artifact → same code, same input, different survivor → `row_number()`/`first()` resolving a tie → ordering columns not unique per key.
- **Root cause (diagnosis):** ties were resolved by executor/partition order, which is not part of the query semantics. In the CDC case specifically, `groupBy` shuffles and the preceding `orderBy` does not survive it — Spark documents `first()` as non-deterministic for exactly this reason. The previous docstring called the `lit(1)` fallback *"an arbitrary but stable per-run tie-break — still correct"*; stable within one execution's plan is not stable across executions.
- **Fix / guard:** `pipeline/common/ordering.py::latest_row_per_key` — ordering always closes with a content-derived tie-break, so the survivor is identical across runs, partition counts and cluster sizes. All three call sites converted. Regression tests `tests/test_cdc_ordering_determinism.py` (10 tests, including 10-repetition and 1/3/7-partition stability).
- **Junior mistake:** checking the row count, seeing it correct, and concluding the dedup is correct.
- **Lesson (interpretation):** *"stable per run"* is not *"deterministic across runs"*. A dedup can return the right **number** of rows and the wrong **rows**, indefinitely, with no error — and every re-run, backfill and reconciliation argument silently depends on it not doing that.
- **Soundbite:** *"Three of our dedups ordered by a non-unique column, so which row survived was decided by partitioning. Right row count, potentially different data each run. I closed the ordering with a content hash so the survivor is reproducible."*

### TS-MONEY-01 — "The same report totalled to a slightly different number the second time"
- **Phase:** transformation
- **Status:** 🟠 PARTIAL — **correction, 2026-09-04 (P3 pre-run inspection).** This card and
  `INC-0004` both said the money path was converted to fixed point "end to end". That was wrong:
  the fix covered the Gold conversion only. Silver was still casting to `double` upstream of it,
  and the Snowflake serving layer still re-declares the monetary columns `DOUBLE` downstream of
  it. See `TS-MONEY-02`, which carries the trace. Kept here rather than rewritten, because the
  overclaim is itself the lesson.
- **Provenance:** `discovered` — found by code inspection during the framework reconciliation, 2026-09-04. Ledger `INC-0004`.
- **Symptom (business/observability):** an aggregate that does not reproduce exactly between two runs over identical input. Differences sit in the low digits, so the report still looks right.
- **Observed (pre-fix code):** `pipeline/gold/common.py:27` computed `col(amount_col).cast("double") * col("rate_to_myr")`, and `pipeline/gold/dim_fx_rate.py:30` declared `rate_to_myr` as `DoubleType()`. Both sides of the currency conversion were floating point, feeding `fact_txn`, `fact_card_fraud` and `fact_account_balance`.
- **Backward trace:** irreproducible total → `SUM` over a double column → float addition order differs → Spark chooses that order by partitioning.
- **Root cause (diagnosis):** IEEE-754 addition is not associative — `(1e16 + 1) - 1e16 = 0.0` while `1e16 - 1e16 + 1 = 1.0`. Spark decides the grouping by partition layout, which is not part of the query semantics, so a float monetary `SUM` is order-dependent. It never raises, so it presents as irreproducibility rather than failure, and it quietly undermines the per-run source→Bronze→Silver→Gold reconciliation that `pipeline/gold/mart_pipeline_health.py` (R-30, BQ-10) publishes.
- **Fix / guard:** `pipeline/common/money.py` — `MONEY = DECIMAL(18,2)`, `FX_RATE = DECIMAL(18,6)`; `to_myr` and `dim_fx_rate` converted to fixed point end to end, with `assert_no_float_money()` available as a boundary guard. Precision and scale are **documented assumptions** — no `journey/` or ADR document specifies them. Regression tests `tests/test_money_precision.py` (11 tests, exact decimal equality, never a tolerance).
- **Junior mistake:** treating the discrepancy as a rounding artifact and adding a tolerance to the comparison — which hides exactly the drift the comparison exists to detect.
- **Lesson (interpretation):** money must be fixed-point. For a ledger, a total that changes between two runs over identical input is an audit finding, not a rounding preference.
- **Soundbite:** *"Our FX conversion was double × double, so MYR totals depended on partition order. Small, silent, never an error — but a banking total that won't reproduce is an audit problem. I moved the monetary path to DECIMAL and asserted exact equality."*


### TS-MONEY-02 — "We fixed the float-money bug, and the totals are still floats"
- **Phase:** transformation
- **Status:** 🟠 PARTIAL — Silver half fixed, serving half open (owner decision).
- **Provenance:** `discovered` — found by tracing the money path during P3 pre-run inspection, 2026-09-04. Ledger `INC-0005`.
- **Symptom (business/observability):** none. Every gate green, every test passing, `INC-0004` closed as `fixed`. The reported MYR figures still do not reproduce exactly, and nothing anywhere says so.
- **Observed (pre-fix code):** the fix landed only in the middle of the path. **Upstream:** `pipeline/silver/silver_core_banking.py:53` (`col("details.value.amount").cast("double")`) and `pipeline/silver/silver_crm.py:150-151` (`amount__c`, `balance__c`) — even though `journey/05_STTM.md:60` had declared `sil_trans.amount`/`balance` **decimal** all along. **Downstream:** `pipeline/serving/snowflake_setup.sql` re-declares `amount`/`amount_myr` (:103, :107, :123, :125), `rate_to_myr` (:195) and `current_balance`/`current_balance_myr` (:272-273) as `DOUBLE`, and every dbt mart aggregates over those — `sum(amount_myr)`, `sum(current_balance_myr)`, `approx_percentile(current_balance_myr, 0.5)`.
- **Backward trace:** MYR total still irreproducible → the `SUM` runs in Snowflake, not Spark → the Snowflake external table casts the DECIMAL Parquet column back to `DOUBLE` → and the value that reached Gold had already been through a `double` at Silver.
- **Root cause (diagnosis):** the fix was scoped to the file where the symptom was noticed (`to_myr`) instead of to the path the value travels. Gold's own `.cast(MONEY)` then *hid* the upstream half: rounding a double back to 2dp recovers ordinary amounts, so the output looked correct and degrades only at large magnitudes. No gate compared the Silver column types against the STTM that governs them.
- **Fix / guard:** Silver now uses `pipeline/common/money.py::to_money`, casting from the source representation rather than via `double` (`silver_core_banking.py:57`, `silver_crm.py:158-159`). `pipeline/gold/common.py::to_myr` now calls `assert_no_float_money()` on its input, so the Silver→Gold boundary fails closed instead of rounding quietly. Two **static** regression guards in `tests/test_money_precision.py::TestMoneyPathHasNoFloatReentry` — they need no Spark, no JDK and no lake, because the defect they catch is an *edit*, and an edit lands long before anyone can afford a cloud run. Both were mutation-verified: reintroduce either defect and they fail. **The serving half is deliberately not fixed** — changing those column types is a consumer-facing contract change (Power BI reads them) and belongs to the owner, not to a cleanup pass.
- **Junior mistake:** reporting a fix as complete because the tests you wrote for it pass. The tests covered the conversion helper; nothing covered the two ends of the path.
- **Lesson (interpretation):** a fix belongs to a data path, not to the file where the symptom surfaced. Trace the value from source to consumer and check both ends — a downstream cast that *rounds* a bad value is not a fix, it is camouflage. Corollary: "hardened" should require evidence at the path's edges, not at its middle.
- **Soundbite:** *"I'd closed a float-money bug as fixed. Tracing the path before the real run, I found the money was still double on both sides of my fix — cast to double at Silver, and cast back to double by the Snowflake external table the dashboards actually read. Gold's decimal cast in the middle was rounding it back to something that looked right. I fixed the upstream half, made the boundary assert instead of round, and left the consumer-facing half as an explicit owner decision rather than changing a published contract quietly."*

---

See `../README.md` for the honesty contract. Interview drills source their answer from a card's
fields, never from recollection (`learning/EXECUTIVE_STORYTELLING_TEMPLATE.md`).
