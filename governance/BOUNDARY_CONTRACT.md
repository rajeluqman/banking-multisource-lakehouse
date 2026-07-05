# Boundary Contract — banking-multisource-lakehouse

> This is the human-readable doc; the enforced version is `gates/boundary_contract.py` reading
> `gates/framework.yml` → `boundary:`. Keep these in sync — if you change one, change the other.

## Locked stack (what IS allowed, and where)
| Layer | Storage | Compute/engine | Sanctioned surface |
|---|---|---|---|
| Landing/Bronze/Silver/Gold | S3 `s3://<bucket>/banking/` (local-disk fallback for dev) | Databricks portable PySpark + Delta | `pipeline/`, `seed/` |
| Serving (Fasa E, optional) | Gold S3 prefix, read-only | Snowflake external tables (or DuckDB $0 fallback) | Fasa E serving scripts only |
| Governance | Unity Catalog over the S3 external locations | — | RBAC grants (`journey/09_SECURITY_AND_ACCESS.md` §3), lineage, audit |

Full alternatives-considered discussion: `governance/ADR/ADR-002-ratified-stack.md`.

## Rejected tech (what is explicitly NOT allowed, and why)
| Rejected | Reason | ADR reference |
|---|---|---|
| Microsoft Fabric / OneLake (anywhere — build or serving) | Not on the resume; the owner's active Fabric trial serves a separate project | ADR-002 |
| Delta Live Tables (`import dlt`) | Databricks-notebook lock-in; portable PySpark must survive the disposable trial being deleted | ADR-002 |
| Kafka / Confluent Kafka / streaming CDC in v1 | Batch-first ruling; CDC is a later fasa | ADR-004 |
| Terraform / IaC | ~3 cloud resources total — ceremony, not warranted; resume doesn't claim IaC | `01_OPUS_DECISIONS.md` D-13 (CIL planning lab) |
| ML training libs (sklearn/xgboost/tensorflow/torch) | Data-engineering portfolio repo — `isFraud`/`TARGET` are labels to serve, never predict | `journey/02_BUSINESS_QUESTIONS.md` "Explicitly out of scope" |
| Vector DB / RAG (langchain/chromadb/pinecone) | Wrong project | `journey/02_BUSINESS_QUESTIONS.md` "Explicitly out of scope" |
| Content cleansing/masking inside the Landing→Bronze promotion gate | Would launder "raw"; Bronze must stay verbatim | ADR-003 |
| A separate `security/` folder | Considered and rejected twice (kit ADR-001 rej-alt #2, CIL ADR-014); content lives in `journey/09_SECURITY_AND_ACCESS.md` instead | `01_OPUS_DECISIONS.md` D-16 |

## Ingestion allowlist
Only these ingestion mechanisms are sanctioned: psycopg2/sqlalchemy watermark extractor
(PostgreSQL), pyodbc/sqlalchemy watermark extractor (MS SQL Server), simulated SFTP file-drop
pickup (SAP-sim), Open Bank Project sandbox REST API client, BNM OpenAPI (optional FX enrich
only). Anything else (Fivetran/Airbyte/a new connector) requires an ADR before adoption.

## Enforcement
`gates/boundary_contract.py` checks:
- No banned import (`dlt`, `kafka`, `confluent_kafka`, `sklearn`, `xgboost`, `tensorflow`,
  `torch`, `langchain`, `chromadb`, `pinecone`) appears anywhere in the repo — no sanctioned
  overrides exist for any of these (every ban is a hard, repo-wide rejection).
- No dbt/adapter profile file exists (this repo has none — Databricks connection config lives in
  env vars / secret scopes, not a `profiles.yml`).
