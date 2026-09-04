# NEXT SESSION PROMPT — P3: REAL END-TO-END HEALTHY RUN + BASELINE RECONCILIATION

> Paste this whole file as the opening prompt of a new session.
> It carries everything the previous session established, so no reconnaissance needs repeating.
> Written 2026-09-04. Repo: `banking-multisource-lakehouse`.

---

# PART 0 — CARRIED-FORWARD CONTEXT (read before acting; do not re-derive)

## 0.1 Where the state lives

| What | Where |
|---|---|
| Framework reconciliation, fault→repo mapping, governance conflicts | `BANKING-FRAMEWORK-RECON.md` (repo root) |
| Current checkpoint | `PROJECT_STATUS.md` → "▶ RESUME HERE", top entry dated 2026-09-04 |
| Incident ledger | `learning/INCIDENTS.jsonl` — INC-0001…0004 |
| Incident cards | `cheatsheets/troubleshooting/00_INDEX.md` — TS-GRAIN-01, TS-UPSERT-01, TS-ORDER-01, TS-MONEY-01 |
| Interview claims | `interview/CLAIMS.md` — CLAIM-001…004, **all `DROP`** pending teach-back |
| Determinism register | `<vault>/projects/de-fault-lab/03_healthy-baseline.md` §4 and §4.4 |
| Vault handoff | `<vault>/state/handoff-fault-lab.md`, `progress-fault-lab.md` |

Vault = `D:\Obsidian\Data Engineering Learn`. Persistent memory for this work is indexed at
`C:\Users\Maman\.claude\projects\d--Obsidian-Data-Engineering-Learn\memory\MEMORY.md`.

## 0.2 What was done in P1 + P2 (do not redo)

**P1 — four incidents carded, all `provenance: discovered`.** None injected.

| ID | Status | What |
|---|---|---|
| INC-0001 | `fixed` | 47,180 NULL `customer_id` rows in `fact_repayment_behavior`; dbt's `unique` test passed because SQL treats NULLs as distinct |
| INC-0002 | `diagnosed` | `merge_upsert` has no delete branch, so a corrected re-run cannot retract bad rows; remediated by manual S3 deletion, **never fixed in code** — that is why it is not `fixed` |
| INC-0003 | `fixed` | Three "one row per key" selections were not total orders |
| INC-0004 | `fixed` | FX path was `double × double` |

**P2 — code changed:**

- **New:** `pipeline/common/ordering.py` (`latest_row_per_key`, content-derived tie-break), `pipeline/common/money.py` (`MONEY = DECIMAL(18,2)`, `FX_RATE = DECIMAL(18,6)`, `assert_no_float_money`)
- **Modified:** `pipeline/silver/common.py` (`merge_upsert` + `latest_state_from_cdc_log`), `pipeline/gold/common.py` (`to_myr` + `latest_balance_per_account`), `pipeline/gold/dim_fx_rate.py` (schema → `FX_RATE`, parse via `Decimal(str(...))`)
- **Tests:** `tests/test_cdc_ordering_determinism.py` (10), `tests/test_money_precision.py` (11), `tests/_spark_support.py`

## 0.3 TWO KNOWN-INCOMPLETE ITEMS — these are the real P3 Step 4 work

Found after P2 was reported. **Neither is fixed. Both must be handled before any cloud run.**

**(a) The money path is fixed-point at Gold but NOT at Silver.** Still `double`:

- `pipeline/silver/silver_core_banking.py:53` — `col("details.value.amount").cast("double")` (OBP)
- `pipeline/silver/silver_crm.py:150` — `amount__c` (Salesforce)
- `pipeline/silver/silver_crm.py:151` — `balance__c` (Salesforce)

The representation error is baked in before Gold converts. Gold's cast to `DECIMAL(18,2)`
*recovers* normal amounts by rounding to 2dp, but that is recovery, not correctness, and it
degrades at large magnitudes. Use the existing `pipeline/common/money.py::MONEY` — **never define
a second money type.**

**(b) Two Gold facts will FAIL on the first real run.**

- `pipeline/gold/fact_txn.py:85` and `pipeline/gold/fact_card_fraud.py:42` use `.mode("append")`. Existing S3 tables carry `amount_myr` as `double`; the fix makes it `decimal(18,2)`. Delta enforces schema on append and `double → decimal` is **not** a permitted evolution, so these raise rather than write. **Append cannot retype a column — these two tables must be rebuilt (delete-and-recreate).**
- `pipeline/gold/fact_account_balance.py:25` uses `.mode("overwrite")`, which replaces data but keeps the schema by default — it needs `.option("overwriteSchema", "true")`.

This is INC-0002's own lesson landing on the fix: an append-only writer cannot retract or retype
what it wrote, so recovery is delete-and-recreate. **Use the repo's existing rebuild/cleanup
mechanism. If none exists, STOP and report what is missing — do not improvise destructive S3
commands.**

**Open question:** does `journey/05_STTM.md` pin a monetary precision that should override the
documented `DECIMAL(18,2)` / `DECIMAL(18,6)` assumptions in `pipeline/common/money.py`? Those are
assumptions, flagged as such in the module, because no journey/ADR document specified one. Check
before the run.

## 0.4 Environment facts

Real execution path (confirmed by the owner 2026-09-04):

```
GitHub Codespace → RDBMS Docker (PostgreSQL, MySQL, MSSQL) → Teradata Vantage Trial
→ Salesforce CRM Bulk API → Open Bank Project REST API → real cloud storage → Silver → Gold/marts
```

- Storage **S3** (ADR-002), local-disk `./data` fallback in `pipeline/common/lake_paths.py` when no AWS creds. Compute **Databricks portable PySpark + Delta**; `import dlt` banned repo-wide. Serving Snowflake external tables / DuckDB. Orchestration in sibling repo (ADR-011).
- **SAP HANA Cloud is ADR-006 source #3 but was NOT listed as currently runnable.** Availability unconfirmed — do not assume required, do not assume gone. Verify.
- Local dev box: Python 3.13.4 / PySpark 4.0.0 / Java 17. Repo pins `pyspark==3.5.3`. On Windows, `spark.createDataFrame(python_list, ...)` crashes the worker (PySpark 4 + CPython 3.13) — **use `spark.sql("SELECT * FROM VALUES ...")` fixtures**, already handled in `tests/_spark_support.py`.

## 0.5 Known-good test / gate baseline — use to separate pre-existing from introduced

`python -m unittest discover -s tests` → **44 tests, OK, 11 skipped**, ~17s, stable across runs.

- The **11 skips are pre-existing** (`simple_salesforce` not installed).
- Gates green: `journey_completeness`, `boundary_contract`, `secrets_scan`, `incident_hygiene`, `claim_ledger`.
- `doc_reference_contract` fails with **11 pre-existing violations**, all in `journey/07_PIPELINE_SPEC.md`, `ADR-004`, `ADR-008`, `ADR-011`, `governance/PIPELINE_SIDE_CONTRACT.md`, referencing undeclared sibling repos. Count was 11 before and after P1/P2. **Do not fix them** — the stable count is the control proving a change introduced nothing new.

## 0.6 Rules that bind this work

- `pipeline/gold/` and `pipeline/silver/` are **governed paths** (`gates/framework.yml`). Editing triggers `CLAUDE.md`'s STOP-GATE: open the governing doc first (`journey/04_DATA_MODEL.md` for Gold; `journey/09_SECURITY_AND_ACCESS.md` + `journey/06_DQ_PLAN.md` for Silver), then run the gates after.
- **TWO-STRIKE (ADR-009):** if the same stage fails twice, or a fix leaves the symptom, STOP paid execution and escalate. Never trust run SUCCESS — verify at the artifact level.
- **Provenance is mandatory** on every incident card and ledger entry: `discovered | injected`. `DE-Mastery/Interview-Prep/integrity-rules.md` Rule 2 is binding — an `injected` item is *"a training drill I built"*, **never** *"a production incident I handled."* P3 injects nothing, so nothing new should be `injected`.
- **Claims do not self-promote.** `CLAIM-001…004` stay `DROP` until a teach-back happens, per `gates/claim_ledger.py`.
- Everything written to disk is **English** (`<vault>/_rules/heart.md`).

## 0.7 Expect the numbers to move

Converting FX to fixed point changes `amount_myr` / `current_balance_myr` in `fact_txn`,
`fact_card_fraud`, `fact_account_balance` and every mart downstream. **That is the fix working.**
`PROJECT_STATUS.md` quotes MYR figures computed under the old float path — treat them as
`PRE-FIX / HISTORICAL EVIDENCE`, not as targets to reproduce.

---

# PART 1 — THE TASK

We are moving from P2 correctness fixes into **P3: real end-to-end healthy pipeline verification**.
Verify the pipeline is correct and reproducible after the P2 determinism and money-precision fixes.

## HARD SCOPE BOUNDARY

Do ONLY P3. **DO NOT:** implement BREAK · create `faultlab/` · implement the 34-fault injection
framework · create vertical slices · create compound-fault experiments · provision Dataproc ·
create a new ADR · override the Master Roadmap · invent incidents · turn drills into `discovered`
incidents · refactor unrelated pipeline code.

If you discover another issue, classify and document it. Fix it only if it directly blocks the
P3 healthy run.

---

## STEP 1 — PRE-RUN INSPECTION

Inspect the current implementation and determine the exact execution/rebuild path.

**Money path** — trace `source amount → Silver amount/balance → Gold amount_myr /
current_balance_myr → downstream marts` and confirm end-to-end fixed point: Silver monetary fields
use the shared `MONEY` type; FX rates use `FX_RATE`; Gold never casts money back to `DOUBLE`;
`to_myr()` stays fixed-point; no hidden `DOUBLE` remains. **Search all affected files, not only
the three in §0.3(a)** — those are the ones already known, not necessarily all of them.

**Affected Gold tables** — for `fact_txn`, `fact_card_fraud`, `fact_account_balance` determine:
existing Delta schema · current write mode · whether schema replacement is required · whether a
rebuild is required · the project's existing safe rebuild/cleanup mechanism · whether downstream
marts must also be rebuilt.

Do not delete or overwrite cloud data merely because it is convenient. If no safe rebuild
mechanism exists, **STOP and report what is missing** rather than improvising.

## STEP 2 — VERIFY SOURCE / INFRASTRUCTURE PREREQUISITES

Determine the intended bring-up order from the repo's existing run instructions (`Makefile`,
`README.md`, `docker-compose.yml`, `PROJECT_STATUS.md`): Codespace → RDBMS Docker → Teradata
Vantage → Salesforce Bulk API → OBP REST API → cloud creds/config.

Do not invent credentials or configuration. Verify which sources are actually available. If one is
unavailable: do not fake its output, do not substitute synthetic data, record it as unavailable,
and determine whether a legitimate healthy baseline is still possible without it. Explicitly
resolve whether **SAP HANA** is in the current execution path — do not assume it is required just
because it appears in ADR-006.

## STEP 3 — ESTABLISH PRE-FIX EVIDENCE

Before rebuilding any existing cloud Delta table, preserve the current documented values: row
counts · monetary aggregates · Gold totals · schema/type info · downstream mart totals · baseline
identifiers. Label them `PRE-FIX / HISTORICAL EVIDENCE`. Do **not** claim they are correct — they
are for comparison only.

## STEP 4 — APPLY ONLY THE REQUIRED P2 REPAIR

See §0.3. Minimum corrections only:

**A. Silver monetary types** → shared `MONEY`. No second money-type definition.
**B. Gold schema migration** → minimum safe mechanism so the three facts carry the corrected
DECIMAL schema. For append-only tables use the project's rebuild procedure rather than blindly
changing append behaviour. For overwrite tables use schema replacement only where appropriate.
**C. Regression guard** → extend the existing assertions (`assert_no_float_money`) so a future
change cannot silently reintroduce `DOUBLE` into the Silver→Gold monetary path.

Run the unit tests before the cloud run.

## STEP 5 — RUN THE REAL PIPELINE

Order: RDBMS Docker → Teradata → Salesforce → OBP → Landing → Bronze → Silver → Gold → Marts.
Use the repo's existing orchestration (`pipeline/orchestrate.py`, `orchestrate_config.yml`,
`Makefile`). Do not introduce a new orchestrator. **Inject nothing — this is a healthy run.**

Record: run timestamp · source availability · source row counts · Landing / Bronze / Silver
(promoted + quarantined) / Gold counts · key and monetary aggregates · schemas · duration ·
failures and retries · gate results.

## STEP 6 — MONEY RECONCILIATION

The most important verification. For every affected monetary output:
`old documented value → new DECIMAL value → difference → explanation`.

Classify each difference as (1) an expected consequence of removing floating-point representation,
or (2) an unexpected pipeline/data change. **Do not say "the numbers changed, so the fix worked."
Prove why they changed.** Reconcile source → Silver → Gold where possible and confirm no monetary
loss or rounding occurred beyond the documented scale.

## STEP 7 — DETERMINISM VERIFICATION

Run the healthy pipeline twice. Compare Run A vs Run B on row counts · PK uniqueness · selected
row-level results · monetary aggregates · Gold aggregates · schema fingerprints · mart metrics.

Required: `RUN A == RUN B`. If they differ, **STOP**, do not proceed, diagnose and report.

## STEP 8 — UPDATE HEALTHY BASELINE

Only after the real run and the reproducibility check pass. Update
`<vault>/projects/de-fault-lab/03_healthy-baseline.md` with the verified post-fix baseline,
keeping `PRE-FIX HISTORICAL EVIDENCE` and `POST-FIX VERIFIED BASELINE` clearly separate — do not
destroy the comparison. Update `PROJECT_STATUS.md` and the vault handoff/progress files. **Do not
promote interview claims automatically** — they move only through the existing claim/teach-back
gates.

## STEP 9 — TESTS AND GOVERNANCE GATES

Run `python -m unittest discover -s tests` and the applicable gates. Record total / passed /
skipped / failed, and preserve the distinction between **pre-existing** (see §0.5) and
**introduced by P3**. Do not fix unrelated pre-existing violations to make the run green.

## STEP 10 — FINAL P3 DECISION

Return exactly one:

**P3 PASS** — only if the real healthy pipeline completed, affected Gold schemas are correct, the
money path is fixed end-to-end, historical values were reconciled, the new baseline is documented,
Run A == Run B, no new unexplained correctness issue exists, and tests/gates are recorded.

**P3 BLOCKED** — state `BLOCKER` / `WHY IT BLOCKS P3` / `EVIDENCE` / `WHAT MUST HAPPEN NEXT`.

Either way: do not continue into BREAK / Fault Lab.

---

# FINAL REPORT FORMAT

## A. Execution Environment
Codespace · RDBMS Docker · Teradata · Salesforce · OBP · Cloud resources · SAP HANA · any
unavailable source.

## B. P2 Corrections Verified
| Area | Status | Evidence |
|---|---|---|
| Silver money | | |
| Gold money | | |
| Decimal FX | | |
| Deterministic ordering | | |
| Regression tests | | |

## C. Real Pipeline Run
| Layer | Status | Key evidence |
|---|---|---|
| Source | | |
| Landing | | |
| Bronze | | |
| Silver | | |
| Gold | | |
| Marts | | |

## D. Money Reconciliation
| Metric | Pre-fix | Post-fix | Difference | Explanation |
|---|---:|---:|---:|---|

## E. Determinism
`Run A:` / `Run B:` / `Equal:`

## F. Tests / Gates
| Check | Result | Notes (pre-existing vs introduced) |
|---|---|---|
| unittest | | baseline: 44 tests, OK, 11 pre-existing skips |
| journey_completeness | | |
| boundary_contract | | |
| secrets_scan | | |
| incident_hygiene | | |
| claim_ledger | | |
| doc_reference_contract | | baseline: 11 pre-existing violations |

## G. Evidence Updated
Every file changed, and why.

## H. Scope Verification
Confirm explicitly: BREAK not implemented · Fault Lab not implemented · 34-fault injection not
implemented · no Dataproc provisioning · no compound experiments · no roadmap override · no
invented incidents.

## I. Final Decision
`P3 = PASS / BLOCKED`
If PASS, the single exact next action. If BLOCKED, the single exact blocker-removal action.

**Hard stop after this report.**
