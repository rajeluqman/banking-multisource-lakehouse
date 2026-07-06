# 08 — Serving & Evidence

> Status at Fasa 0 (bootstrap): this doc is the CONTRACT for what evidence Fasa D/E must capture.
> The actual query outputs are pasted in as each fasa completes — this is not yet possible before
> Gold exists. Updated incrementally; do not backfill fake output here.

## What is actually served, to whom, how
- **Dev loop**: local Spark/subset queries against local-disk Delta tables, run ad-hoc from
  `pipeline/gold/` scripts or a notebook — read/write mechanism, not a UI.
- **Canonical**: Databricks SQL / notebook queries against Unity-Catalog-governed Gold tables on
  the disposable trial, screenshotted (D-01 Add #3 evidence-first rule).
- **Serving veneer (Fasa E, optional)**: Snowflake external tables over the Gold S3 prefix + one
  Power BI page covering BQ-01 + BQ-02, or a DuckDB query if no live Snowflake account.

## Per-BQ evidence (Fasa D code shipped 2026-07-06 — output pending a live run)
| BQ | Mart | Query location | Output captured | Status |
|---|---|---|---|---|
| BQ-01 | mart_customer_360 | pipeline/gold/mart_customer_360.py | none yet | built, UNVERIFIED (no live Spark/Bronze run this session) |
| BQ-02 | mart_fraud_daily | pipeline/gold/mart_fraud_daily.py | none yet | built, UNVERIFIED |
| BQ-03 | mart_fraud_followup | pipeline/gold/mart_fraud_followup.py | none yet | built, UNVERIFIED — uses a documented proxy (no real ticketing source, journey/03) |
| BQ-04 | mart_loan_funnel | pipeline/gold/mart_loan_funnel.py | none yet | built, UNVERIFIED — DAYS_DECISION column name (unverified) against real schema |
| BQ-05 | mart_risk_segment | pipeline/gold/mart_risk_segment.py | none yet | built, UNVERIFIED |
| BQ-06 | mart_cross_sell | pipeline/gold/mart_cross_sell.py | none yet | built, UNVERIFIED |
| BQ-07 | mart_dormancy | pipeline/gold/mart_dormancy.py | none yet | built, UNVERIFIED |
| BQ-08 | mart_daily_flows | pipeline/gold/mart_daily_flows.py | none yet | built, UNVERIFIED |
| BQ-09 | fact_txn x dim_customer | pipeline/gold/fact_txn.py + pipeline/gold/dim_customer.py | none yet | built, UNVERIFIED (query itself not yet written — join the two on customer_id) |
| BQ-10 | mart_pipeline_health | pipeline/gold/mart_pipeline_health.py | none yet | built, UNVERIFIED — mandatory mart present |

"Built, UNVERIFIED" means: py_compile-clean, reviewed, matches the STTM/data-model contract,
but never executed against real Bronze/Silver data (no Spark, no live DB connections, no cloud
in this planning session — owner instruction). Running each mart + capturing real output is the
next action once the owner's dedicated Codespace has live Postgres/MSSQL/SAP HANA/Teradata/OBP
data flowing through Fasa A-C.

## Proven vs claimed (the Volve lesson)
| Claim (in README/resume/docs) | Evidence (file:line or command output) | Status |
|---|---|---|
| "Four bootstrap gates green" (Fasa 0) | pasted gate output in `BUILD_REPORT.md` | pending — filled at Fasa 0 gate checkpoint |
| "10/10 BQs answerable" | this table, filled per-BQ as Fasa D ships | pending |
| "Landing→Bronze isolation proven" | Fasa B gate proof (kill-and-rerun, partial-arrival quarantine) | pending Fasa B |
| "PII masked before Gold" | grep/query proof, no unmasked account/card/birth_number in Gold | pending Fasa C/D |

No claim is written into a README/resume bullet for this project until its row here is `proven`,
not `unverified` — per the anti-shortcut reconcile-before-done rule.

## Resume-claim reconciliation
Deferred to `BUILD_REPORT.md` (final self-audit) once Fasa D/E are complete — mirrors the
pipeline_retrofit `INTERVIEW_GUIDE.md` pattern (resume bullet → repo evidence → flagged if
unsupported). Not duplicated here to avoid two documents drifting out of sync; this doc holds the
per-BQ query evidence, `BUILD_REPORT.md` holds the full resume-claim reconciliation.
