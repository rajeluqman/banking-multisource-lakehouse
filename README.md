# banking-multisource-lakehouse

Multi-source banking data-platform simulation: **5 heterogeneous "production" systems** with no
shared customer key, ingested through a 4-layer medallion (Landing → Bronze → Silver → Gold) on
Databricks + Delta + Unity Catalog, S3 as sole source of truth, served read-only through Snowflake.

| # | Source system | Type | Ingestion pattern | Extractor |
|---|---|---|---|---|
| 1 | PostgreSQL — "Sales / Loan Dept" | RDBMS (Docker) | Watermark-incremental batch | `pipeline/extract/postgres_extract.py` |
| 2 | MS SQL Server 2022 — "Credit Card + Fraud" | RDBMS (Docker) | Watermark-incremental batch | `pipeline/extract/mssql_extract.py` |
| 3 | Teradata — "Marketing / Campaign" | Enterprise warehouse | Trigger + `_cdc_log` change-table CDC | `pipeline/extract/teradata_extract.py` |
| 4 | Salesforce — "Internal CRM" | SaaS CRM | Bulk API 2.0, `SystemModstamp` high-watermark | `pipeline/extract/salesforce_extract.py` |
| 5 | Open Bank Project sandbox — "Core Banking API" | REST API (nested JSON) | Paginated on-demand pull | `pipeline/extract/obp_client.py` |

BNM OpenAPI is an *optional* enrichment source only — never a build dependency (D-12), and not
counted among the five. Full inventory, licence and PII constraints: `journey/01_DATASET_AND_SOURCES.md`.

Start here:
- `CLAUDE.md` — governance, stop-gate, anti-shortcut protocol, stack, known blockers.
- `PROJECT_STATUS.md` — resume-safe checkpoint ("▶ RESUME HERE").
- `journey/` — the 9 mandatory design docs (dataset/sources → business questions → data
  requirements → data model → STTM → DQ plan → pipeline spec → serving/evidence →
  security/access).
- `governance/ADR/` — the locked architectural decisions, numbered.
- `BUILD_REPORT.md` — self-audit against every named risk (R-01…R-35) and business question
  (BQ-01…BQ-10), written once the build reaches its checkpoint.

## Bootstrap gates
```
python gates/journey_completeness.py
python gates/boundary_contract.py
python gates/doc_reference_contract.py
python gates/secrets_scan.py
```
All four must be green before any commit lands (`.github/workflows/ci.yml` enforces this on
every PR/push to main).
