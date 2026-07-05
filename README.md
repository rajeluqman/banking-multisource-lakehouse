# banking-multisource-lakehouse

Multi-source banking data-platform simulation: 4 heterogeneous "production" systems (PostgreSQL,
MS SQL Server, a legacy file-drop, and the Open Bank Project sandbox API) with no shared customer
key, ingested through a 4-layer medallion (Landing → Bronze → Silver → Gold) on Databricks +
Delta + Unity Catalog, S3 as sole source of truth, served read-only through Snowflake.

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
