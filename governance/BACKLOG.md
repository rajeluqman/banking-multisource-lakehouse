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

## Deferred (not now, revisit if X happens)
| Item | Date | Condition to revisit | Ruled by |
|---|---|---|---|
| BNM OpenAPI live FX enrich (beyond the static seed table) | 2026-07-05 | Revisit only if a stakeholder needs live-rate accuracy beyond the static FX seed table (D-12) — never a build dependency | owner |
| Fabric as a Fasa-E serving option | 2026-07-05 | Superseded by Snowflake serving (ADR-002); revisit only if Snowflake/DuckDB both become unavailable | owner |
