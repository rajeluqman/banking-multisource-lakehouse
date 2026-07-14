# 09 — Security & Access

> Filled RICHLY per D-16 (`01_OPUS_DECISIONS.md`) — banking is a real-PII domain (birth_number,
> account/card numbers, financial transactions), unlike CIL's ad-video domain. Content ported
> from `06_SECURITY_MODEL.md` in the planning lab. This is NOT a separate `security/` folder —
> that structure was considered and rejected twice (kit ADR-001 rejected-alt #2, CIL ADR-014);
> the substance lives here, in the one mandatory doc that already exists.

## Why this doc is mandatory (not tiered)
This project simulates on-call/production-representative banking data handling — a leaked
credential or an RBAC misconfiguration here is exactly the incident class a real bank's security
team drills against. Modeled on CIL's ADR-014 precedent: one real least-privilege role set, named
explicitly, not a speculative security *program*.

## 1. Secrets management
| Secret | Used by | Stored in | Never |
|---|---|---|---|
| Postgres / MS SQL creds | watermark extractors | Databricks secret scope (canonical run) / `.env` (gitignored, dev loop) | in code or config committed to git |
| Salesforce OAuth (Connected App consumer key/secret + refresh token) | `salesforce_extract.py`, seed loader (ADR-006 Add #2) | Databricks secret scope / `.env` (gitignored) | in code, chat, or committed config — owner supplies via `.env` only |
| Teradata connection (host/creds) | `teradata_extract.py`, seed loader (ADR-006) | Databricks secret scope / `.env` (gitignored) | same as above |
| OBP OAuth client id/secret + token | OBP API client | Databricks secret scope | in the landing path or logs |
| S3 access key (transform) | Databricks ↔ S3 | Unity Catalog storage credential / instance profile | as a long-lived key hardcoded in code |
| S3 read-only key (serving) | Snowflake external tables | Snowflake storage integration | scoped beyond `banking/gold/`, never write |
| `BANK_PAT` (git push) | git credential helper | Codespaces secret ($env only) | on disk / in the remote URL |

Runtime origin, stated plainly: secrets come from the Databricks secret scope at transform time
and from Codespaces/`.env` in the dev loop — never a literal in a script. Enforcement:
`gates/secrets_scan.py` + `gates/framework.yml` → `secrets_scan.extra_patterns` (Databricks PAT
shape `dapi[0-9a-f]{32}`, OBP DirectLogin header shape, OBP OAuth secret assignment shape).

## 2. Data classification
| Data | Where | Classification | Handling |
|---|---|---|---|
| `birth_number` (Berka) | landing/bronze `salesforce_crm` (Contact `birth_number__c`) | **sensitive** (national-ID-shaped, encodes DOB+gender) | decode → `birth_date`+`gender` at Silver (R-12); raw value dropped after decode, never persisted |
| account number / `account_id` | postgres, mssql, OBP, Berka | **sensitive** (financial account) | mask to last-4 at Silver (D-07) |
| card number | mssql `cards` | **sensitive** (PCI-shaped) | mask to last-4 at Silver; never in Gold raw |
| balances, `amount`, txn value | all sources | **confidential** (financial) | aggregate-only in most Gold marts; row-level access restricted |
| name, address, district | Berka CRM | **confidential** (PII) | restricted; not exposed in marketing-facing marts (`mart_cross_sell` carries customer_id + segment, not name/address) |
| `isFraud` label | mssql | **confidential** (risk) | fraud-ops role only |
| `job`/`marital`/`education` (Teradata Bank Marketing) | landing/bronze/silver `sil_campaign_response` | **confidential** (demographic) | restricted at Gold to risk/marketing-facing marts only (BQ-05/06), not exposed row-level elsewhere |
| `credit_in_default` (Teradata Bank Marketing) | landing/bronze/silver `sil_campaign_response` | **confidential** (risk) | risk role only, same as `isFraud` — treated as a risk signal, not public |
| currency codes, product types, dates | all | internal | unrestricted internally |

Every non-public column gets a row — "sensitive" is flagged even where no regulation currently
bites, so a future reader knows it was considered (this table itself is the audit trail).

## 3. RBAC role matrix (role × layer × permission) — REAL GRANTs, Unity Catalog
| Role | Layer / objects | Permission | Used by |
|---|---|---|---|
| `pipeline_svc` | Landing, Bronze, Silver, Gold | READ + WRITE | extractors/transforms (service identity, not a human) |
| `data_engineer` | Silver, Gold (+ Bronze READ) | READ; WRITE Silver/Gold | development / debugging |
| `analyst_marketing` | **Gold only** (masked marts — `mart_cross_sell`, `mart_customer_360`, BQ-09 query) | READ | BQ-01/06/09 — NEVER Landing/Bronze |
| `fraud_ops` | Gold fraud marts + `isFraud` (`mart_fraud_daily`, `mart_fraud_followup`) | READ | BQ-02/03 |
| `risk` | Gold risk marts (`mart_risk_segment`) | READ | BQ-05 |
| `serving_ro` (Snowflake) | Gold external tables only (`s3://<bucket>/banking/gold/`) | READ | Snowflake/Power BI veneer (Fasa E) |
| `landing_admin` | Landing, Bronze (raw PII) | READ | break-glass only, audited |

**Load-bearing rule (R-31): raw layers (Landing/Bronze) hold unmasked PII — no analyst/serving
role may read them.** This is enforced as real Unity Catalog `GRANT`/`REVOKE` statements at Fasa D
build time (pipeline/gold/grants/ — the DDL that instantiates this table), not prose. The exact
`GRANT` statements + a screenshot of the UC permissions UI are captured as Fasa D/canonical-run
evidence in `journey/08_SERVING_AND_EVIDENCE.md`.

## 4. Service identities
- `pipeline_svc` — one scoped identity per pipeline concern; least-privilege (an extractor can
  write its own Landing prefix + read its own watermark state, nothing more).
- `serving_ro` — read-only, scoped to `s3://<bucket>/banking/gold/` only (R-32).
- **Dedicated-not-reused** (CIL ADR-014 rule, carried over): the git-push identity (`BANK_PAT`) ≠
  the ingestion identity (`pipeline_svc`) ≠ the serving identity (`serving_ro`). No shared admin
  credential exists anywhere in this repo's design.

## 5. Audit / log enablement
Platform-native, no bespoke audit pipeline (R-33): Unity Catalog **query history** + **table/
column lineage**; S3 server access logs on the `banking/` prefix; OBP API call logs. Answers "who
read/deleted what, when" without a custom audit store. Default platform retention is sufficient at
portfolio scale — no longer-retention requirement is named, so none is built.

## 6. PII handling
Enters via: Berka CRM (`birth_number`, name, address), account/card numbers across all four
sources. Path: lands raw in Landing→Bronze (restricted access, R-27) → masked/decoded exactly once
at Silver (D-07, R-12) → Gold and serving see only masked/aggregated forms. PII never reaches
`analyst_marketing`/`serving_ro` unmasked. Leaves the system only through Gold marts, already
masked — there is no export path that bypasses Silver's masking step.

## 7. Compliance flags
| Regime | Applies? | Note |
|---|---|---|
| GDPR | Partial (synthetic data, modeled as if EU subjects for realism) | right-to-erasure is *tractable*: `dim_customer_xwalk` (D-04) resolves one customer across all 4 sources, so a delete request is executable end-to-end — documented as a capability (R-34), not built as a live feature in v1 |
| PDPA (Malaysia) | Modeled | same erasure path; banking-sector data-protection framing for the local market |
| PCI-DSS | Shape only | card numbers masked to last-4 (D-07); no real cardholder data (fully synthetic PaySim) |
| BNM / banking-secrecy | Framing | account data treated as confidential; access restricted per the §3 RBAC matrix |

Synthetic data means nothing legally binds here — modeling the controls is the interview point,
stated plainly rather than oversold.

## 8. Threat model (as a section, not a folder — D-16)
| Threat | Vector | Mitigation |
|---|---|---|
| Credential leak | secret in code/config/log | `gates/secrets_scan.py` gate; Databricks secret scopes; `BANK_PAT` env-only (§1) |
| PII exposure to wrong role | analyst reads raw Bronze | UC RBAC: raw layers deny analyst/serving roles (§3) |
| Over-privileged pipeline | shared admin identity | dedicated least-privilege service identities, none shared (§4) |
| Poisoned data | implausible values injected (negative balances, future dates) | DQ range/reject gates — security-as-DQ (`journey/06_DQ_PLAN.md`) |
| Untraceable change | no audit trail | UC query history + lineage (§5) |
| Serving-key abuse | write/escape from the read-only veneer | `serving_ro` read-only, scoped to `gold/` only (§4) |

## 9. Incident contacts
Solo-owner project: the owner is the single responder. Failure alerts (including a security-shaped
one — e.g. `gates/secrets_scan.py` catching a real leaked credential pre-commit) route to the
pipeline's Slack failure channel (`journey/07_PIPELINE_SPEC.md` "Failure handling" — same pattern
as CIL's `_notify_slack_failure`). One person, one channel, stated plainly.
