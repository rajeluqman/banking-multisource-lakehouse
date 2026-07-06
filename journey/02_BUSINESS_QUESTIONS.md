# 02 — Business Questions (BRD-lite)

> Content source: `04_BUSINESS_QUESTIONS.md` in the planning lab. These 10 questions ARE the
> entire scope of Gold — any mart not answering one of them is scope creep (`@scope-guardian`
> hard veto, `governance/BACKLOG.md`).

## North-star question
"Across four disjoint banking systems that were never designed to share a key, can we produce one
trustworthy customer view — and prove, per run, that the numbers behind it reconcile?" Nobody in
this simulated bank could answer a cross-system customer question before this pipeline; each
source system only sees its own slice.

## Question → Metric → Definition of Done
| # | Business question | Metric(s) | Definition of done | Priority |
|---|---|---|---|---|
| BQ-01 | Customer 360 — how many products does each customer hold, and total relationship value? | product count by type (deposit/card/loan/term-deposit); total balance + outstanding loan value in reporting currency | `mart_customer_360` exists; one runnable query returns product mix + relationship value per customer | P0 |
| BQ-02 | Fraud trend — fraud txn count & value this month vs last, by type/channel | fraud txn count, fraud value, MoM delta, breakdown by `transaction_type` | `mart_fraud_daily` exists; query returns current vs prior month comparison | P0 |
| BQ-03 | Fraud follow-up SLA — % of fraud-hit customers with a CRM ticket within 48h | % within-SLA, count breached | `mart_fraud_followup` exists; query returns SLA % | P1 |
| BQ-04 | Loan funnel — applications/month, approval rate, time-to-decision | applications count, approval rate, avg days app→decision | `mart_loan_funnel` exists; query returns monthly funnel | P0 |
| BQ-05 | Default risk by segment — default rate by income band/employment; which ACTIVE customers are high-risk now | default rate by segment (job/education-enriched, ADR-006 D6.4), current high-risk customer list, cross-checked against Bank Marketing's independent `default` flag | `mart_risk_segment` exists; query returns segment default rates + risk list | P1 |
| BQ-06 | Cross-sell targets — healthy-deposit, active, no card/loan customers | count + list of qualifying customers, ranked by prior campaign responsiveness (`poutcome`/`y`, ADR-006 D6.4) | `mart_cross_sell` exists; query returns target list | P1 |
| BQ-07 | Dormancy — customers with no txn in X days this month | dormant count, dormant customer profile | `mart_dormancy` exists; query returns this month's dormant cohort | P1 |
| BQ-08 | Liquidity view — total deposits, daily net flow | total deposits, daily in/out, net flow trend | `mart_daily_flows` exists; query returns daily trend | P1 |
| BQ-09 | Spending behaviour — txn type/value distribution by segment/month | txn count/value by type × segment × month | query against `fact_txn` × `dim_customer` returns the distribution | P2 |
| BQ-10 | Can we trust the numbers? — source freshness, DQ failures, source→Gold reconciliation | last-refresh per source, DQ fail count yesterday, row-count reconciliation delta | `mart_pipeline_health` exists (mandatory, non-optional); query returns freshness+reconciliation | P0 |

Segment definitions (income band, "dormant X days," "healthy" deposit balance) are set once in
`journey/03_DATA_REQUIREMENTS.md` — not reinvented per mart.

## Explicitly out of scope
Named REJECTED items (`01_OPUS_DECISIONS.md` REJECTED list, binding — not a "maybe later"):
1. AI creative-search / semantic search over any of this data.
2. RAG script/report generation.
3. A live creative-ops or banking-ops dashboard (evidence = queries + captured output in
   journey/08; a Power BI page in Fasa E is a serving veneer, not a dashboard product).
4. ML model training (a fraud-detection or default-prediction model) — `isFraud` and the
   default-risk fields are labels this pipeline SERVES, never trains against.
5. Real-time streaming/CDC (Fasa C territory, not v1 — D-02).
6. Any mart beyond BQ-01…10. A new mart idea mid-build goes through
   `governance/ADR/ADR-000-feature-intake-protocol.md`, not straight into `pipeline/gold/`.
7. Terraform/IaC (D-13) — this repo's cloud footprint is 2–3 resources, hand-managed.
8. Fabric/OneLake anywhere in this repo (D-01 Addendum #2) — separate project.

## Stakeholder / audience
Simulated stakeholder roles, one per BQ group (management/RMs, fraud ops, loan/sales, risk,
marketing, retention, treasury, "every stakeholder" for BQ-10) — see the RBAC matrix in
`journey/09_SECURITY_AND_ACCESS.md` §3 for which UC role each maps to. In this portfolio context
the actual consumer is: (a) the owner, running the evidence queries in journey/08, and (b) an
interviewer evaluating the pipeline's design — so "late or wrong" here means the mart doesn't run,
the reconciliation in BQ-10 doesn't hold, or a resume claim in journey/08 can't be backed by a
`file:line`/command-output row.
