# BANKING-FRAMEWORK-RECON

> **Phase 0 — read-only reconnaissance.** No code changed, no faults injected, no framework built.
> This is the single planning artifact. After approval another agent can implement from this
> without repeating the recon.
> Date: 2026-09-04 · Scope: `banking-multisource-lakehouse` + the learning vault at
> `D:\Obsidian\Data Engineering Learn`

---

## 1. Executive summary

### What already exists — more than expected

The lifecycle you want to build is **already about 80% present**, split across two repositories:

| Stage | Where it lives | State |
|---|---|---|
| **LEARN** (fundamentals) | Vault — `sql/`, `python/`, `_rules/heart.md`, cikgu skill | Built, active, Tier 1 in progress |
| **LEARN** (project) | Banking — `learning/CURRICULUM.md` M0–M10 | Built, not started |
| **DESIGN** | Banking — `journey/` 9 docs + `governance/ADR/` 12 ADRs | Built, mature |
| **BUILD** | Banking — `pipeline/` 79 files, `dbt/`, `/de-build` | Built, largely working |
| **VALIDATE** | Banking — `gates/`, `promotion_gate.py`, `dq_currency_gate.py`, CI | Built, mature |
| **OPERATE** | Banking — `orchestrate.py`, sibling Airflow repo, `mart_pipeline_health.py` | Built |
| **BREAK** | — | **ABSENT. This is the only real gap.** |
| **DETECT** | Banking — DQ plan `journey/06_DQ_PLAN.md`, promotion gate | Built |
| **DIAGNOSE** | Banking — `de-diagnosis` skill, TWO-STRIKE (ADR-009), `incident-commander` | Built, **never exercised** |
| **RECOVER** | Banking — ADR-009 artifact-level verification | Built, **never exercised** |
| **PROVE** | Banking — `de-evidence`, `interview/CLAIMS.md`, `gates/claim_ledger.py` | Built, **ledger empty** |
| **DOCUMENT** | Banking — `cheatsheets/troubleshooting/`, `BUILD_REPORT.md` | **Stub by design** |

### The single most important finding

Three artifacts are empty, and they are empty **for the same reason**:

- `cheatsheets/troubleshooting/00_INDEX.md` — *"STATUS: STUB — grows on real incidents only… a real incident card is authored only AFTER a real incident is hit during the build."*
- `learning/INCIDENTS.jsonl` — **does not exist on disk** (verified), though `gates/_incident_ledger.py:29` defines its full lifecycle `open → diagnosed → fixed → taught → hardened`.
- `interview/CLAIMS.md` — still carries `<!-- FRAMEWORK_TEMPLATE: UNFILLED -->` at line 1.

The repo has a complete **incident-consumption** machinery — a ledger with five states, a card
format, a diagnosis skill, an incident commander, a claim ledger that demands `file:line`
evidence — and **no incident-supply mechanism**. It waits for failures to happen by accident.

**That is precisely what the DE Fault Lab is: a controlled incident generator.** So the correct
framing is not *"add a fault lab to banking."* It is:

> **The fault lab is the missing supply side for machinery this repo already built and
> deliberately starved.**

### What the new blueprint actually adds

Very little that is new *conceptually* — but two things that matter:

1. **A method for producing incidents on purpose**, with an ordering (`Origin → Detect → Resolve → Prove`), a compound-fault design, and a per-fault injection recipe.
2. **Two live correctness defects it already found in this repo** (§4.6) — which is the strongest possible evidence that the method works on this codebase.

### What must be removed or refused

Nothing in the existing framework should be removed. The GCP-specific half of the fault lab
(`ADR-001` Dataproc/GCS, Datastream, BigQuery bytes-billed, BigQuery policy tags) **does not
transfer** and must not be forced onto an S3/Delta/Databricks stack (§5.4).

### Three governance conflicts that must be resolved before implementation

These are not technical problems. They are decisions only you can make, and each has a written
rule currently pointing the other way. Detail in §9.1.

1. **Sequencing** — the roadmap's locked order puts banking at Tier 4 (capstone, last); you are in Tier 1.
2. **Prior rejection** — banking explicitly *rejected* building a sabotage gym as over-scope, with sign-off.
3. **Authoring honesty** — two separate rules forbid authoring a troubleshooting card from anything but a spontaneous incident.

---

## 2. Repository map

### 2.1 Learning framework — the vault (`D:\Obsidian\Data Engineering Learn`)

**This is the finalized learning method.** It is not a draft.

| Component | Path | What it does |
|---|---|---|
| Session router | `vault/00_INDEX.md` | Per-track read/write ownership; one track per session |
| Operating rules | `vault/_rules/heart.md` | Language policy, session ritual, quiz rules, **Definition of Done** |
| Note shape | `vault/_rules/topic-template.md` | Fixed 7-section shape per topic |
| Pedagogy | `.claude/skills/cikgu/SKILL.md` + 4 references | Evolution ladder, Plan-in-Comments-Then-Fill, hint style, C-P-I-D-I-R drill |
| Roadmap | `vault/01_MASTER-ROADMAP.md` | 4 tiers, **locked sequencing** (line 205) |
| Gym doctrine | `vault/shared/gym-and-troubleshooting-doctrine.md` | **Dormant until Tier 3** — the break/troubleshoot method |
| Integrity | `vault/DE-Mastery/Interview-Prep/integrity-rules.md` | 6 rules governing what may be claimed |
| SRE playbook | `vault/DE-Mastery/SRE-Playbook/M1–M6` | Toil, SLO/circuit-breaker, postmortem, burn-rate, idempotency, zero-trust |
| Fault lab | `vault/projects/de-fault-lab/` | 34 problems, 19 gates, ripple map, runbook, tested `reference/` |

**Definition of Done — `_rules/heart.md`, three gates, all required:**

1. **Teach-back** — explained in own words, no notes open.
2. **Quiz ≥ 80%** evidenced by a pasted receipt: `RECEIPT m1-dql-core · 8/10 (80%) · … · chk 4f2a`. The `chk` binds score to answer pattern — **tamper-evident, not tamper-proof**, and heart.md says so explicitly.
3. **≥ 3 coding exercises.**

The 70–79% band is deliberately "not failing, not done."

**The vault already names this repo.** `heart.md` → *"Reference project: rajeluqman/banking-multisource-lakehouse (medallion architecture)"*, and its DE-mapping section already maps SQL concepts onto lakehouse layers (dedup→Silver, MERGE→Gold, watermark→incremental). **The LEARN→BUILD link is declared; it is just not built out.**

### 2.2 Build framework — banking

- `journey/01…09` — the 9 mandatory docs: sources, business questions, DRD, data model, STTM, DQ plan, pipeline spec, serving/evidence, security.
- `governance/ADR/ADR-000…011` + `ADR-TEMPLATE.md`, `BOUNDARY_CONTRACT.md`, `PIPELINE_SIDE_CONTRACT.md`, `BACKLOG.md`, `plans/`.
- `.claude/` — 9 agents, 9 commands (`/de-build`, `/de-source`, `/de-audit`, `/de-fix`, `/de-rca`, `/de-claim`, `/de-teach`, `/de-explore`, `/de-maintain`), 5 skills, `hooks/governance_guard.py`.
- `gates/` — `framework.yml` (single config) + 6 gate scripts + `_incident_ledger.py`.
- Enforcement is **three-way**: CLAUDE.md prompt (soft) → `governance_guard.py` hook (blocks edits to governed paths) → CI (`.github/workflows/ci.yml` blocks the PR).

### 2.3 Break framework — split, and mostly dormant

| Artifact | Location | State |
|---|---|---|
| Gym doctrine (method) | Vault `shared/gym-and-troubleshooting-doctrine.md` | **Dormant** until Tier 3 |
| Troubleshooting library | Banking `cheatsheets/troubleshooting/00_INDEX.md` | **Stub**, 0 cards, gated on real incidents |
| Optimization library | Banking `cheatsheets/optimization/00_INDEX.md` | **Stub**, 0 cards, gated on real findings |
| Incident ledger | Banking `gates/_incident_ledger.py` | Code exists; `learning/INCIDENTS.jsonl` **absent** |
| Fault catalogue | Vault `projects/de-fault-lab/` | 34 problems, complete, **GCP-targeted** |
| Determinism primitives | Vault `projects/de-fault-lab/reference/` | 12 tests passing, engine-neutral |

### 2.4 Banking implementation

`Sources (5) → Landing → Bronze → Silver → Gold → Serving`

- **Sources:** Postgres/Docker (Home Credit), MSSQL/Docker (PaySim), **SAP HANA Cloud** (Berka), **Teradata** (UCI Bank Marketing), **Open Bank Project** REST API. Salesforce Bulk API added by ADR-006 Add #2.
- **Storage:** S3 `banking/{landing,bronze,silver,gold}/` with **local-disk `./data` fallback** when no AWS creds (`pipeline/common/lake_paths.py:20-26`). This is the free dev loop.
- **Compute:** Databricks portable PySpark + Delta. `gates/boundary_contract.py` bans `import dlt` repo-wide — transforms must survive the trial workspace being deleted.
- **Serving:** Snowflake external tables over Gold S3, or DuckDB $0 fallback.
- **Orchestration:** external, in sibling repo `banking-multisource-lakehouse-airflow-dag` (ADR-011); Airflow triggers-and-polls, does not compute the medallion.
- **Ingestion:** batch watermark for PG/MSSQL (ADR-004); **CDC-poll via trigger + `_cdc_log` change-table** for SAP HANA/Teradata (ADR-006) — deliberately *not* SLT/SDI/QueryGrid.

---

## 3. Old vs New build blueprint

| Area | Old (banking `journey/` + ADRs) | New (fault lab) | Better | Reason |
|---|---|---|---|---|
| Discovery | `journey/01_DATASET_AND_SOURCES.md` | Source-system comparison table | **Old** | Grounded in 5 real systems with real quirks, not a generic matrix |
| Requirements | `02_BUSINESS_QUESTIONS.md` — scope frozen at BQ-01…10 | — | **Old** | New has no requirements concept at all |
| Source profiling | `03_DATA_REQUIREMENTS.md` + R-id risk register | `SRC-01…07` root causes | **Merge** | R-ids are project-specific; SRC-ids are a reusable taxonomy. Map one to the other |
| STTM | `05_STTM.md` | — | **Old** | New has no STTM |
| Architecture | ADR-003 four-layer medallion | 5-layer contracts | **Merge** | Same layers. New adds *explicit contracts per layer* ("Landing never repairs"), which ADR-003 implies but does not state as a table |
| Data contracts | Schema-hash compare in `promotion_gate.py` | Registered column-set contract + classification | **Merge** | Old detects drift; new classifies it (additive-known / additive-unknown / breaking) |
| CDC | ADR-004 + ADR-006 CDC-poll | `BRZ-03` ordering + sequence guard | **New** | Old dedups on `(pk, op, seq)`; new adds the *total-order* requirement the old is missing (§4.6) |
| Bronze | ADR-003 append-only, D-05 verbatim | `BRZ-01…07` | **Old** for policy, **New** for the small-file/maintenance failure modes |
| Silver | D-07 masking, `merge_upsert` | `SLV-01…07` | **New** — old has a live determinism defect the new catalogue names |
| Gold/MDM | ADR-005 star schema + xwalk, Clean-ERD Doctrine | `SLV-01` crosswalk stability | **Merge** | Old builds it; new adds churn/over-merge assertions old lacks |
| Data quality | `06_DQ_PLAN.md` — ~20 gates, R-id traced | DLQ + rescued-data | **Old** | Genuinely stronger and already implemented. New adds only the rescued-data column |
| Testing | 3 test files | 12 determinism tests | **New** — this is the acknowledged gap (SPEC D-10) |
| Determinism | absent | Register of 12 items, 2 fixed | **New** — decisively |
| Failure handling | ADR-009 TWO-STRIKE + quarantine | Injection + gates | **Merge** — old handles, new *causes* |
| Observability | `mart_pipeline_health.py` (BQ-10) | Freshness SLO | **Merge** — old has row-count reconciliation; new adds freshness |
| CI/CD | 3 workflows + hook + gates | — | **Old** |
| Security | ADR-001 mandatory, D-07 Silver masking, `09_SECURITY_AND_ACCESS.md` | `GLD-07` policy tags | **Old** — new's is BigQuery-specific |
| Governance | ADR-000 intake, `@scope-guardian` veto | — | **Old** |
| Cost | `@finops-agent`, disposable trial | `GLD-01` BigQuery guardrails | **Old** — new's is GCP-specific |
| Documentation | 9 journey docs, BUILD_REPORT, PROJECT_STATUS | Cards | **Old** |
| Operational readiness | Airflow, orchestrate_config | Runbook phases | **Merge** |
| Evidence / proof | Claim Ledger + 4-part DoD | Baseline + assertion matrix | **Merge** — old defines what a claim needs; new defines what a *number* needs |

**Verdict: the old blueprint is stronger almost everywhere.** The new one wins on exactly three
things — **determinism**, **testing**, and **deliberate failure generation**. Those three are
also precisely what the old one lacks. That is a clean complement, not a competition.

---

## 4. Fault lab → banking mapping

Verdicts: `DEFENDED` · `PARTIAL` · `ABSENT` · `N/A` · `NEEDS ADAPTATION`

### 4.1 Sources

| ID | Fault | Verdict | Evidence / adaptation |
|---|---|---|---|
| SRC-01 | Breaking DDL | **PARTIAL** | Detection defended: schema-hash compare, *"controlled `mergeSchema` only after explicit review, never silent"* (`06_DQ_PLAN.md`, `promotion_gate.py:12-13`). Expand-Contract drill absent. Injectable — you own the Docker seeds |
| SRC-02 | Late / out-of-order | **PARTIAL** | `common/watermark.py`, `run_interval.py`; R-29 late-arriving dimension → unknown member `-1`. Ordering totality missing (see SRC-02→BRZ-03) |
| SRC-03 | Dirty data / type drift | **DEFENDED** | ~20 DQ gates in `06_DQ_PLAN.md`; null-rate thresholds, FK orphan quarantine, encoding checks. Rescued-data column absent → minor PARTIAL |
| SRC-04 | Identity fragmentation | **DEFENDED** | This is the project's defining constraint. `seed/build_xwalk.py`, `pipeline/gold/dim_customer_xwalk.py`, ADR-005, curriculum M4 |
| SRC-05 | Volume spike / hot key | **ABSENT** | Named as applicable in `cheatsheets/optimization/00_INDEX.md` (*"skew on the MDM join key"*), never drilled. **Closes integrity-rules Rule 6** |
| SRC-06 | CDC prerequisites | **NEEDS ADAPTATION** | No WAL/replication slot — CDC is trigger + `_cdc_log`. Slot-retention fault is **N/A**. But R-40 (*"seed-time bulk load never reaches `_cdc_log`"*) is the *same class* and is already documented — adapt to that |
| SRC-07 | Hard deletes | **PARTIAL** | CDC-poll carries `op`; Salesforce `SystemModstamp >` cannot see deletes. `merge_upsert` **never deletes** — confirmed by a real incident (§4.6) |

### 4.2 Landing

| ID | Fault | Verdict | Evidence |
|---|---|---|---|
| LND-01 | API rate limits / partial extract | **DEFENDED** | R-22 pagination reconciled vs API-reported totals — `promotion_gate.py` check 3 |
| LND-02 | Batch-level partial publish | **DEFENDED** | `_SUCCESS` marker + manifest checksum — `promotion_gate.py` checks 1–2. **Note:** S3, not GCS; the object-atomicity finding still holds, batch-level is still the real exposure |
| LND-03 | Non-idempotent re-run | **DEFENDED** | Checksum-keyed manifest dedup — `promotion_gate.py` check 6. **Real instance already survived:** the sibling repo's extractor keyed `dt=` off `now()` until PR #15 (curriculum M10 DIY) |
| LND-04 | Bookmark past failed write | **PARTIAL** | `watermark.py` exists; no window-completeness audit found. Highest-value ABSENT-ish gap at this layer |
| LND-05 | CDC gap / log retention | **NEEDS ADAPTATION** | No WAL retention. Equivalent = `_cdc_log` purge/retention policy. Same failure shape, different mechanism |
| LND-06 | Byte fidelity | **PARTIAL** | R-17 encoding/diacritics check on Berka fields. No canonical-vs-relaxed export concern (no Mongo) |

### 4.3 Bronze

| ID | Fault | Verdict | Evidence |
|---|---|---|---|
| BRZ-01 | Small files | **DEFENDED** | ADR-010, `pipeline/gold/compaction.py` |
| BRZ-02 | mergeSchema absorbs badly | **PARTIAL** | Policy is right (*never silent*); enforcement-in-code unverified |
| BRZ-03 | CDC ordering | **ABSENT — LIVE DEFECT** | `pipeline/silver/common.py:55` `order_cols = [lit(1)]` (§4.6) |
| BRZ-04 | Duplicate delivery | **DEFENDED** | `merge_upsert` + anti-join on `(source, table, pk_value, op, seq)` |
| BRZ-05 | Deletes / tombstone | **PARTIAL — REAL INCIDENT** | `merge_upsert` never deletes; PROJECT_STATUS records a required manual S3 deletion of 3 poisoned Silver prefixes |
| BRZ-06 | Maintenance debt | **PARTIAL** | ADR-010 compaction; VACUUM/retention policy not confirmed |
| BRZ-07 | Partition strategy | **N/A at scale** | Same conclusion as the vault: do not partition at this volume |

### 4.4 Silver

| ID | Fault | Verdict | Evidence |
|---|---|---|---|
| SLV-01 | Crosswalk instability | **PARTIAL** | Crosswalk built; no stability / over-merge / churn assertion found |
| SLV-02 | DLQ / quarantine | **DEFENDED** | `_quarantine_<table>_orphans`, *"count + report, never silently drop"* |
| SLV-03 | Shuffle / spill | **ABSENT** | Same gap as SRC-05. Integrity Rule 6 |
| SLV-04 | Fan-out join | **PARTIAL — ALREADY BIT YOU** | PROJECT_STATUS: declared grain didn't hold, **47,180 NULL-key rows**, and *dbt's `unique` test silently excludes NULLs so it passed anyway*. Fixed for that table; no systematic guard |
| SLV-05 | SCD2 | **PARTIAL** | Clean-ERD doctrine requires *"one explicit SCD strategy per table"*; overlap/multi-current assertions not found |
| SLV-06 | Determinism | **ABSENT — LIVE DEFECT** | §4.6. Load-bearing for everything else |
| SLV-07 | Conformance | **PARTIAL** | `dq_currency_gate.py` defends currency-*tag* presence (R-14). The FX *arithmetic* is float (§4.6) |

### 4.5 Gold

| ID | Fault | Verdict | Evidence / adaptation |
|---|---|---|---|
| GLD-01 | Cost blowout | **NEEDS ADAPTATION** | No BigQuery. Equivalent = Databricks DBU + Snowflake credits; `@finops-agent` + disposable-trial discipline already partly covers it. `require_partition_filter` / `maximum_bytes_billed` → **N/A** |
| GLD-02 | Stale data served | **ABSENT** | No freshness SLO found. Real gap, cheap to add |
| GLD-03 | Full recompute | **PARTIAL** | dbt marts; incremental strategy per model unverified |
| GLD-04 | Metric inconsistency | **DEFENDED** | STTM defines metrics once; Clean-ERD doctrine + `@staff-data-engineer` veto |
| GLD-05 | Serving contract break | **PARTIAL** | *"serving = view never a duplicated table"* limits blast radius; no deprecation/versioning window |
| GLD-06 | Restatement | **PARTIAL** | Real precedent exists (BQ-10 "delete and recreate"); no restatement log |
| GLD-07 | PII exposure | **DEFENDED** | ADR-001 security mandatory, D-07 masking at Silver, `09_SECURITY_AND_ACCESS.md`. BigQuery policy tags → **N/A** |

### 4.6 The two live defects — evidence

Found by direct read, not inference. These are the strongest argument that the method works here.

**Defect A — non-total CDC ordering (`BRZ-03`, determinism register #3)**

`pipeline/silver/common.py:49-57`:
```python
order_cols = []
if "updated_at" in df.columns:  order_cols.append(col("updated_at").desc())
if "created_at" in df.columns:  order_cols.append(col("created_at").desc())
if not order_cols:
    order_cols = [lit(1)]                     # ← every row ties
window = Window.partitionBy(*pk_columns).orderBy(*order_cols)
df = df.withColumn("_rn", row_number().over(window)).filter(col("_rn") == 1)
```
The docstring (`:41-43`) calls the fallback *"an arbitrary but **stable per-run** tie-break — still
correct."* Stable within one execution's plan is **not** stable across executions: the survivor
can change with partition count, task scheduling or speculative execution. Even the non-fallback
path is not a total order — two rows can share `updated_at`.

Same class at `pipeline/gold/common.py:42` and `dim_customer.py:59`.

**Defect B — money in floating point (`SLV-07`, determinism register #5)**

- `pipeline/gold/common.py:27` — `col(amount_col).cast("double") * col("rate_to_myr")` — **FX conversion in float**
- `pipeline/gold/dim_fx_rate.py:30` — `rate_to_myr` is `DoubleType()`
- `pipeline/silver/silver_core_banking.py:53`, `silver_crm.py:150-151` — amounts/balances cast to `double`

IEEE-754 addition is not associative, so `SUM` over these is partition-order dependent. For a
banking ledger, an aggregate that changes between two runs of identical input is an audit
finding. Fixes are already written and tested in the vault
(`projects/de-fault-lab/reference/`, 12 tests passing) and are engine-neutral — a port, not a build.

### 4.7 Tally

| Verdict | Count |
|---|---|
| DEFENDED | 11 |
| PARTIAL | 14 |
| ABSENT | 4 (`SRC-05`, `BRZ-03`, `SLV-03`, `SLV-06`, `GLD-02` — 5 counting GLD-02) |
| NEEDS ADAPTATION | 4 |
| N/A | ~4 sub-items (BigQuery-specific) |

**The repo already defends or partly defends 25 of 34.** The fault lab's value here is not
coverage — it is *exercising* what exists and closing the determinism/skew hole.

---

## 5. Proposed unified framework

### 5.1 Shape

```
GENERIC DE FRAMEWORK  (vault — reusable, engine-neutral)
        ↓
BANKING ADAPTER / PROFILE  (fault-id → R-id → ADR mapping)
        ↓
banking-multisource-lakehouse  (implementation)
```

The generic layer must never learn banking's vocabulary; the adapter is where `SRC-06`
becomes "trigger + `_cdc_log`" and `GLD-01` becomes "DBU + Snowflake credits."

### 5.2 The lifecycle, stage by stage

| Stage | Purpose | Inputs | Activities | Artifacts | Gate | Exit criterion | Skill developed |
|---|---|---|---|---|---|---|---|
| **LEARN** | Understand before touching | Vault track files; curriculum module | cikgu WHY-before-HOW; DIY ticket | Notes, quiz+receipt, `LEARNING_LOG` | heart.md 3-gate DoD | Teach-back + quiz ≥80% + 3 exercises | Concept fluency |
| **DESIGN** | Decide before building | Business questions, source profile | Journey docs, ADR | `journey/*`, `ADR-*` | `journey_completeness.py` | All 9 docs non-template | Architectural judgement |
| **BUILD** | Construct correctly | STTM, data model | `/de-build` | `pipeline/**`, `dbt/**` | `boundary_contract.py`, hook | Runs end-to-end on real data | Implementation |
| **VALIDATE** | Prove it is right *now* | Built pipeline | DQ suite, unit tests, CI | Test results, DQ report | CI green | All DQ gates pass | Testing |
| **OPERATE** | Run it repeatedly | Validated pipeline | Airflow schedule, health mart | Run history, `mart_pipeline_health` | Row-count reconciliation | 3 clean consecutive runs = **baseline** | Operations |
| **BREAK** | Violate an assumption **on purpose** | Baseline + fault card | Injection recipe, flag on | Injection log, drill record | Fault reproduces on demand | Fault reproduced **and** observed pre-gate | Failure imagination |
| **DETECT** | Prove something is wrong | Broken pipeline | Read signals before touching | Detection evidence | Gate fires at predicted layer | Detected at the *predicted* layer | Observability |
| **DIAGNOSE** | Find the root, not the symptom | Detection signal | Backward trace; **hypothesis log**; TWO-STRIKE | RCA, `INCIDENTS.jsonl` entry | Evidence-gate: `command + output` | Root cause cited `file:line` | Diagnosis |
| **SOLVE** | Restore correctness | Root cause | Fix at the layer that *owns* it | Code change + guard | Fix at correct layer, not nearest | Guard added where the contract lives | Layer ownership |
| **RECOVER** | Restore the data | Fix | Reprocess affected partitions | Recovery log | **Idempotent** — re-run is a no-op | Baseline reproduced | Recovery |
| **PROVE** | Show it is correct again | Recovered state | Assertion vs baseline | Assertion result | Proves *data*, never job success | Golden figures match | Evidence discipline |
| **DOCUMENT** | Make it reusable | The whole drill | Troubleshooting card; claim | Card + `CLAIMS.md` entry | `claim_ledger.py` | Card cites real `file:line`; **provenance tagged** | Communication |

### 5.3 Why BUILD / BREAK / SOLVE / PROVE must stay separate

They answer four different questions and fail independently:

- **BUILD** — *"Can I construct this correctly?"*
- **BREAK** — *"What happens when reality violates my assumption?"*
- **SOLVE** — *"Can I find the root cause and restore correctness?"*
- **PROVE** — *"What evidence shows the system is correct?"*

Collapsing them into "testing" is the failure this repo already experienced: dbt's `unique`
test passed while 47,180 rows carried a NULL key. That was a BUILD-stage test that could not
answer the PROVE-stage question.

### 5.4 What does not transfer from the fault lab

| Fault-lab item | Status here | Reason |
|---|---|---|
| `ADR-001` Dataproc + GCS | **Superseded** by ADR-002 ratified stack | Wrong cloud. Its *reasoning* survives: hand-written MERGE over a managed CDC abstraction, so the failure stays injectable |
| Datastream | **N/A** | CDC here is trigger + `_cdc_log` |
| Replication slot / WAL retention | **N/A** | No logical replication. Adapt to `_cdc_log` purge policy |
| BigQuery `require_partition_filter`, `maximum_bytes_billed` | **N/A** | Adapt to DBU/credit guardrails |
| BigQuery policy tags | **N/A** | D-07 Silver masking already covers PII |
| GCP cost guardrails phase | **N/A** | Local-disk fallback makes the dev loop free |

**Do not manufacture equivalence.** `N/A` is an acceptable, honest verdict.

---

## 6. Banking-specific framework

The existing `learning/CURRICULUM.md` M0–M10 is the right spine. **Do not replace it — extend it.**

Proposed: keep M0–M10 exactly as they are, and add a **break/solve tier M11–M15** that only
opens after M0–M10 close. Each new module reuses an existing artifact rather than inventing one.

| Module | Teaches | Fault IDs | Reuses | DIY |
|---|---|---|---|---|
| **M11** | Determinism — why a rerun must reproduce | `SLV-06`, `BRZ-03` | `silver/common.py:55`, vault `reference/` | Fix `lit(1)`; write the tie-detection assertion |
| **M12** | Money & precision | `SLV-07` | `gold/common.py:27`, `dim_fx_rate.py:30` | Convert FX path to `DECIMAL`; prove exact equality |
| **M13** | Break & detect — first injection | `SRC-01`, `SRC-03` | `promotion_gate.py` schema-hash | Inject DDL into Docker Postgres; observe *without* the gate first |
| **M14** | Diagnose & recover | `BRZ-05`, `SLV-04` | ADR-009, `de-diagnosis`, real 47,180-row incident | Hypothesis log → backward trace → idempotent recovery |
| **M15** | Skew & cost | `SRC-05`, `SLV-03`, `GLD-01` | `cheatsheets/optimization` | Measure before salting. **Closes integrity Rule 6** |

Each module closes with: an `INCIDENTS.jsonl` entry reaching `hardened`, one troubleshooting
card citing a real `file:line`, and one `CLAIMS.md` entry — tagged with drill provenance (§9.1c).

**Why this order:** M11 first because every later assertion depends on reruns being meaningful;
M12 second because it is the same class and already broken; M13 is the first *deliberate* break;
M14 exercises machinery that exists but has never run; M15 closes the one gap the integrity
rules already name as genuine.

---

## 7. Learning progression

| Level | Question they can answer | Vault stage | Banking stage |
|---|---|---|---|
| **Beginner** | "What does this code do?" | SQL M0–M5, Python Ch1–6 | — |
| **Junior** | "How do I build this?" | SQL M6–M9, Python Ch7–12 | M0–M3 (layers, MERGE, masking) |
| **Mid** | "Why does this design exist?" | Tier 2 Kimball, Tier 3 DE core | M4–M8 (MDM, star schema, DQ, serving) |
| **Senior** | "What assumption can fail, and how would I detect it?" | Gym doctrine (Tier 3) | M9–M10, then **M11–M14** |
| **Architecture** | "Where should the defence live, and how do I prove the fix?" | Fault lab ripple map | **M15** + full drill cycle |

The senior→architecture jump is exactly the ripple map's `Origin → Detect → Resolve → Prove`,
and it is what the current framework cannot teach because nothing ever breaks.

---

## 8. Gap analysis

### 8.1 Missing
- **Incident supply.** No mechanism generates failures. Everything downstream starves.
- **`learning/INCIDENTS.jsonl`** — ledger code exists, file does not.
- **Determinism discipline** — no register, no tests, two live defects.
- **Freshness SLO** (`GLD-02`).
- **Test coverage** — 3 test files for 79 pipeline modules (already known: SPEC D-10).
- **Skew/shuffle practice** — named by integrity Rule 6 as a genuine gap.

### 8.2 Duplicated — **three competing Definitions of Done**

| Source | Gates |
|---|---|
| Vault `_rules/heart.md` | Teach-back + quiz ≥80% w/ receipt + 3 exercises |
| Banking `learning/CURRICULUM.md` | Score from 100, −5/hint, <60 forces docs break |
| Banking `de-evidence` skill | Runs · Proves · Documents · Explains |

They are not contradictory but they are unreconciled. **Recommendation:** treat heart.md's
3-gate DoD as authoritative for *learning a concept*, and de-evidence's 4-part DoD as
authoritative for *closing a project module*; CURRICULUM's score becomes an input to gate 1,
not a fourth standard. Write this down once, in one place.

### 8.3 Conflicting — see §9.1. Three, all governance, none technical.

### 8.4 Outdated assumptions
- Banking CLAUDE.md scope says *"no streaming/CDC (Fasa C)"* while ADR-006 implements CDC-poll for two sources. Reconcile the wording.
- Fault-lab `ADR-001` names Dataproc/GCS — superseded here by ADR-002.
- Vault gym doctrine §3 says *"no live credentials to fence off by default"*; banking now has **working AWS/Databricks creds** (ADR-002 Add #5). The 3-layer safety mechanism it calls optional is **mandatory** here.

### 8.5 Needing redesign
- Fault-lab `L1-landing.md` is GCS/Datastream-shaped; needs an S3/promotion-gate rewrite.
- `SRC-06`/`LND-05` need re-basing from WAL onto `_cdc_log`.

---

## 9. Decisions required before implementation

### 9.1 The three conflicts

**(a) Sequencing.** `01_MASTER-ROADMAP.md:205` — *"Sequencing decision (locked)"*: finish Tier 1
(SQL M9 + Python Ch12, quizzes ≥70%), then Tier 2, then Tier 3, then Tier 4. Line 169 states
banking is the **Tier 4 capstone, last** — and explicitly corrects a previous "banking = start"
mislabel. Current position: **SQL M1, Python Ch4.** The gym doctrine is *dormant until Tier 3*.

Implementing the fault lab in banking now jumps three tiers. That is your call, but it is a
documented locked decision and reversing it silently would be the exact failure the roadmap's
correction note exists to prevent.

*Middle path:* Phase B (the two determinism fixes) is **bug-fixing, not curriculum** — it does
not touch sequencing. Everything from M13 onward does.

**(b) Prior rejection.** `gym-and-troubleshooting-doctrine.md:§1` — *"Two of the user's repos
(`banking-multisource-lakehouse`, `creative_intelligence_lab`) deliberately **rejected** building
a gym like this… and both explicitly said so in their sign-offs."* Banking's own CLAUDE.md
STOP-GATE rule 3: *"if a conflict is real, it needs an ADR addendum, not a silent workaround."*

→ Reversing this requires **ADR-012** (or an ADR-010 addendum) in this repo, through
`ADR-000-feature-intake-protocol.md`, with `@scope-guardian` sign-off. Your `ADR-012` instinct
was right — this repo has ADR-000…011.

**(c) Authoring honesty — the one that matters most.**

Two independent rules say the same thing:
- Vault doctrine §6: *"A card is written only after a real incident happens… Never author a card from an imagined scenario."*
- Banking troubleshooting stub: *"a real incident card is authored only AFTER a real incident is hit during the build… No fabricated incidents, no invented citations."*

A deliberately injected fault is **not** spontaneous. Left unaddressed, the fault lab would
launder training drills into war stories — precisely what these rules exist to stop.

**But `integrity-rules.md` Rule 2 already solves this**, from the pharma gym precedent:

> **SAY:** "Training drills I built to practise identifying anti-patterns"
> **DON'T SAY:** "Production incidents I handled"

→ **Resolution:** add a mandatory `provenance:` field to the card format and the incident
ledger, with values `discovered` (arose spontaneously) or `injected` (drill). The diagnosis,
the fix and the `file:line` are equally real in both cases — only the *origin* differs, and
Rule 2 already prescribes the claim language for `injected`. `claim_ledger.py` should refuse
`DEFEND` on an `injected` claim whose text implies a production incident.

This preserves both rules instead of overriding either, and it is cheap.

### 9.2 Two real instances already qualify as `discovered`

Neither needs injection — they already happened, and under the authoring rule they can be
carded **today**, before any of the conflicts above are resolved:

1. The 47,180 NULL-key grain violation that dbt's `unique` test passed (`SLV-04`).
2. `merge_upsert` never deleting, forcing manual S3 deletion of 3 poisoned Silver prefixes (`BRZ-05`).

---

## 10. Proposed structure and roadmap

### 10.1 Proposed file/folder structure — **proposal only, do not create**

```
banking-multisource-lakehouse/
├── governance/ADR/
│   └── ADR-012-fault-injection-drill-programme.md   # NEW — reverses the §9.1b rejection
├── faultlab/                                        # NEW — the only new top-level dir
│   ├── README.md                                    # adapter: fault-id ↔ R-id ↔ ADR
│   ├── PROFILE.md                                   # banking adapter (S3/Delta/_cdc_log)
│   ├── baseline/BASELINE.md                         # golden figures, frozen
│   ├── faults/FAULT-<ID>.md                         # one card per applicable fault
│   └── inject/                                      # flag-gated, default OFF
├── pipeline/common/
│   ├── ordering.py                                  # NEW — total-order CDC collapse
│   └── money.py                                     # NEW — DECIMAL types + guard
├── tests/
│   ├── test_cdc_ordering_determinism.py             # NEW — port of vault reference/
│   └── test_money_precision.py                      # NEW
├── learning/
│   ├── INCIDENTS.jsonl                              # NEW — the ledger file
│   └── CURRICULUM.md                                # EXTEND — add M11–M15
├── cheatsheets/troubleshooting/00_INDEX.md          # EXTEND — add provenance: field
├── interview/CLAIMS.md                              # FILL — 2 discovered incidents first
└── gates/framework.yml                              # EXTEND — register new gates
```

Vault side: `projects/de-fault-lab/` stays the **generic catalogue**; add `BANKING-PROFILE.md`
as the adapter. No duplication of cards.

### 10.2 Implementation roadmap

| Phase | Objective | Files | Depends on | Acceptance | Risk | Rollback |
|---|---|---|---|---|---|---|
| **P0** | Approve this doc; decide §9.1 (a)(b)(c) | — | — | Three decisions recorded | None | — |
| **P1** | Card the two **discovered** incidents | `cheatsheets/troubleshooting/`, `learning/INCIDENTS.jsonl`, `interview/CLAIMS.md` | P0(c) only | 2 cards w/ real `file:line`; ledger reaches `hardened`; 2 `CLAIMS` entries | **Very low** — documentation of things that already happened | Delete 3 files |
| **P2** | Fix the two determinism defects | `pipeline/silver/common.py`, `pipeline/gold/common.py`, `dim_fx_rate.py`, `silver_core_banking.py`, `silver_crm.py`, `tests/` | P1 | 12 ported tests pass; 3 existing tests pass; `make gates` green; **FX numbers change — expect and verify** | **Medium** — touches live money math | Revert commit; numbers are reproducible |
| **P3** | Baseline capture | `faultlab/baseline/` | P2 | 3 consecutive local runs reproduce; golden figures frozen | Low | Delete dir |
| **P4** | ADR-012 + adapter profile | `governance/ADR/`, `faultlab/PROFILE.md` | P0(a)(b) | ADR through ADR-000 intake; `@scope-guardian` sign-off; gates green | Low | Withdraw ADR |
| **P5** | Injection harness, flag-gated | `faultlab/inject/`, `gates/framework.yml` | P4 | Clean path **byte-identical** with flag unset | **High** — could contaminate the showcase | Feature branch; flag default off |
| **P6** | M11–M15 curriculum | `learning/CURRICULUM.md` | P5 | Each module closes DoD; cards tagged `provenance: injected` | Low | Revert section |
| **P7** | Remaining faults by verdict priority | `faultlab/faults/` | P6 | ABSENT first, then PARTIAL | Medium | Per-fault |

**Recommended start: P1 → P2 only.** P1 needs just decision (c) and is nearly risk-free. P2 is a
port of code already written and tested, fixes real bugs in your money path, and belongs on
`master` whether or not the wider programme proceeds. Both stop cleanly if you decide the
sequencing conflict (a) should hold.

---

## Rule compliance

- **Evidence before opinion** — every verdict cites a path, and where possible a line number. Absences were verified by search (`INCIDENTS.jsonl`, ADR files, test count), not assumed.
- **No duplication** — REUSE (journey, ADRs, gates, DQ plan, curriculum, claim ledger), EXTEND (curriculum M11–M15, troubleshooting card format, framework.yml), RECONCILE (three DoDs, CDC scope wording), NEW only for `faultlab/`, `ordering.py`, `money.py`, `INCIDENTS.jsonl`.
- **No implementation performed** — this file is the only artifact created.
- **Generic vs specific separated** — vault holds the reusable catalogue; `faultlab/PROFILE.md` is the banking adapter.
- **Existing strengths preserved** — the old blueprint wins in 15 of 23 compared areas and is retained wherever it wins.

**Awaiting approval. Nothing else will be modified until §9.1 (a), (b) and (c) are decided.**
