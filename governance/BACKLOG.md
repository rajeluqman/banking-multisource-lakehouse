# Backlog — rejected / deferred ledger

> Every item here came through the CIL planning lab's cabinet convene (`01_OPUS_DECISIONS.md`
> REJECTED list) as either REJECTED or DEFERRED before this repo's first commit — ported here so
> the same idea can't be silently re-proposed mid-build without anyone remembering it was decided.

## Rejected (will not be built here)
| Item | Date | Reason | Ruled by |
|---|---|---|---|
| AI creative-search / semantic search engine | 2026-07-05 | Wrong project — no vector DB/RAG/semantic anything in scope | owner |
| RAG script/report generator | 2026-07-05 | Same as above | owner |
| Live dashboard product (beyond a Fasa E Power BI page) | 2026-07-05 | Evidence = queries + captured output (journey/08_SERVING_AND_EVIDENCE.md), not a dashboard build | owner |
| ML model training (fraud/default prediction) | 2026-07-05 | This is a data-engineering portfolio repo; `isFraud`/`TARGET` are labels to serve, never predict | owner |
| Real-time streaming / Kafka / CDC in v1 | 2026-07-05 | Batch-first ruling (ADR-004); CDC is a later, separate fasa | owner |
| Terraform / IaC | 2026-07-05 | ~3 cloud resources total — ceremony, not warranted; resume doesn't claim IaC | owner |
| Microsoft Fabric / OneLake (anywhere in this repo) | 2026-07-05 | Not on the resume; owner's active Fabric trial serves a separate project (`home-credit-fabric-migration`) | owner |
| SAP BTP trial / ABAP Docker (real SAP instance) | 2026-07-05 | 90-day trial wall, 16–32GB RAM; a file-export simulation is cheaper AND more realistic for legacy SAP integration | owner |
| Wise sandbox (alternate core-banking API) | 2026-07-05 | Auth ceremony buys nothing over Open Bank Project | owner |
| `ga4_obfuscated_sample_ecommerce` dataset | 2026-07-05 | Static (Nov 2020–Jan 2021) despite being pitched as "live" — factually wrong claim in the original brainstorm | owner |
| Separate `security/` folder (threat_model.md, incident_response.md, etc. as standalone files) | 2026-07-05 | Considered and rejected twice (kit ADR-001 rej-alt #2, CIL ADR-014); content lives in `journey/09_SECURITY_AND_ACCESS.md` | owner |
| Type 2 SCD on `dim_customer` | 2026-07-05 | No business need in the locked BQ list; named as deliberately out per the clean-model doctrine | architect (ADR-005) |

## Superseded rejections (owner override — original row kept for audit trail, not deleted)
| Item | Original date | Original reason | Override date | Override reason | Ruled by |
|---|---|---|---|---|---|
| SAP BTP trial / ABAP Docker (real SAP instance) | 2026-07-05 | 90-day trial wall, 16–32GB RAM | 2026-07-06 | Owner's operating model makes trial-wall a non-issue (projects run ~3-4 times, new trial account per future project — same reasoning already accepted for the Databricks trial, ADR-002). Heavy-RAM concern doesn't apply either: this override uses **SAP HANA Cloud (BTP Free Tier)** only — a managed database, not full ABAP/Netweaver. Full design: `governance/ADR/ADR-006-real-sap-hana-teradata-cdc-showcase.md` | owner |
| Source #4 host: SAP HANA Cloud (per ADR-006, 2026-07-06) | 2026-07-06 | Chosen as the CRM/CDC-showcase host for Berka (trigger + `_cdc_log` pattern, D6.3) | 2026-07-14 | Moved source #4 to **Salesforce** (CRM role unchanged; Berka stays the seeded data + golden-record keystone, ADR-005 L26). Trigger+`_cdc_log` CDC is physically unavailable on OLTP SaaS — replaced for source #4 by Bulk API 2.0 + `SystemModstamp` incremental; the CDC-showcase skill is preserved by Teradata (source #5), which keeps trigger-CDC. Full design: `governance/ADR/ADR-006-real-sap-hana-teradata-cdc-showcase.md` Addendum #2 | owner |

## Deferred (not now, revisit if X happens)
| Item | Date | Condition to revisit | Ruled by |
|---|---|---|---|
| BNM OpenAPI live FX enrich (beyond the static seed table) | 2026-07-05 | Revisit only if a stakeholder needs live-rate accuracy beyond the static FX seed table (D-12) — never a build dependency | owner |
| Fabric as a Fasa-E serving option | 2026-07-05 | Superseded by Snowflake serving (ADR-002); revisit only if Snowflake/DuckDB both become unavailable | owner |
| Address-change velocity flag (new append-only fact_address_change event mart, sourced from Salesforce Contact address-change history) | 2026-07-14 | Flagged by @staff-data-engineer per ADR-006 Addendum #2 (~L218-219) as a new capability the Salesforce move *tempts* but does not authorize. No BQ in the locked 10 (`journey/02_BUSINESS_QUESTIONS.md`) covers address-change-velocity-as-risk-signal; this is a new mart/grain, which is explicitly out of scope item #6 ("Any mart beyond BQ-01…10... goes through ADR-000, not straight into pipeline/gold/"). Revisit only if a future scope round explicitly adds an 11th+ BQ for identity/behavioral-risk signals — the existence of Salesforce address-history data does not by itself unlock this. | scope-guardian |
| Complaint-pattern detection (new fact_complaint mart from Salesforce Case, cross-joined to txn/fraud) | 2026-07-14 | Flagged by @staff-data-engineer per ADR-006 Addendum #2 (~L220-221). No complaint entity exists in the locked model; this implies a new grain + a new mart = an 11th BQ, out of scope item #6. Also risks drifting into "pattern detection" as an analytics/ML capability rather than a straight aggregation — out of scope item #4 (ML model training) bans learned-model pattern detection; a rule-based version would still need its own BQ and DoD before it could be considered. Revisit only if a future scope round explicitly adds a complaint/case-analytics BQ AND scopes "detect pattern" as deterministic rules, not a trained model. | scope-guardian |
