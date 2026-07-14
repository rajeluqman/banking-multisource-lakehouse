# 05 — Source-to-Target Mapping (STTM)

> Full column-level mapping is written into each `pipeline/silver/*.py` / `pipeline/gold/*.py`
> file's header comment as it's built (Fasa C/D) — restating every column here would drift the
> instant a source schema is confirmed against the real Kaggle CSV. This doc fixes the
> **contract**: target table, its sources, and the transform RULES that don't belong in a single
> file's docstring because they're project-wide or cross-cutting (birth_number decode, step→ts,
> currency, PII masking). Per-column detail lives with the code that implements it.

## Silver targets (Bronze → Silver, content-quality gate — ADR-003)

### Target: `silver.dim_customer_xwalk`
| Target column | Type | Source | Source column | Transform rule | Nullable? |
|---|---|---|---|---|---|
| customer_id | string | seed/build_xwalk.py | generated | bank-wide surrogate, generated once at seed (D-04) | No |
| sk_id_curr | string | Postgres `application` | SK_ID_CURR | passthrough | Yes (not every customer has a loan) |
| name_orig | string | MSSQL PaySim | nameOrig | passthrough | Yes |
| berka_client_id | string | Salesforce Contact `berka_client_id__c` (ADR-006 Add #2) | berka_client_id__c | passthrough | Yes |
| obp_account_id | string | OBP `/accounts` | account_id | passthrough | Yes |
| source_priority_rank | int | derived | — | CRM=1, core=2, loans=3, cards=4 (ADR-005 survivorship order) | No |

### Target: `silver.sil_client` (Berka CRM via Salesforce Contact, ADR-006 Add #2 — golden-record source of DOB/gender)
> Source object is now Salesforce **Contact** (Berka seeded in, ADR-006 Add #2): `client_id` = Contact
> `berka_client_id__c`, `birth_number` = Contact `birth_number__c`. Decode/masking rules below are
> unchanged (journey/09 L34 still applies).
| Target column | Type | Source | Source column | Transform rule | Nullable? |
|---|---|---|---|---|---|
| client_id | string | Berka `client` | client_id | passthrough (PK) | No |
| birth_date | date | Berka `client` | birth_number | **decode rule (R-12)**: YYMMDD where MM has +50 added if the client is female; parse both halves, subtract 50 from month when month > 50 to recover real month | No |
| gender | string | Berka `client` | birth_number | 'F' if original MM > 50 else 'M' — derived in the SAME decode step as birth_date, unit-tested with known fixtures (see tests/test_birth_number_decode.py) | No |
| birth_number_raw | — | Berka `client` | birth_number | **DROPPED after decode** (D-07) — never stored in Silver/Gold | N/A |
| district_id | string | Berka `client` | district_id | passthrough, FK to `sil_district` | Yes (R-03 orphan check) |

### Target: `silver.sil_crm_case` (Salesforce Case — CRM ticket, ADR-006 Add #2, BQ-03 enrichment)
| Target column | Type | Source | Source column | Transform rule | Nullable? |
|---|---|---|---|---|---|
| case_id | string | Salesforce `Case` | Id | passthrough (PK) | No |
| customer_id | string | Salesforce `Case` | ContactId — Contact `berka_client_id__c` — xwalk | resolved to bank-wide customer_id via the xwalk `berka_client_id` link (deterministic — Berka has a real master, no synthetic assignment) | No |
| opened_at | timestamp | Salesforce `Case` | CreatedDate | passthrough (UTC) — the real CRM-ticket timestamp BQ-03 previously lacked (journey/03 L8) | No |
| case_type | string | Salesforce `Case` | Type | passthrough — filter to fraud-follow-up cases for BQ-03 | Yes |

### Target: `silver.sil_campaign_response` (Teradata — UCI Bank Marketing, ADR-006)
| Target column | Type | Source | Source column | Transform rule | Nullable? |
|---|---|---|---|---|---|
| customer_id | string | seed/teradata/load_bank_marketing.py | assigned (R-38) | deterministic sampled linkage to an existing `dim_customer_xwalk` customer_id — this is NOT a native key from the source, it's assigned at seed time | No |
| job / marital / education | string | Teradata `bank_marketing` | job / marital / education | passthrough; feeds BQ-05 segment enrichment (ADR-006 D6.4) | Per null-rate expectation |
| credit_in_default | boolean | Teradata `bank_marketing` | default | passthrough — second, independent default signal alongside Home Credit's `TARGET` (BQ-05); NEVER merged/reconciled automatically, surfaced as a disagreement if the two differ | No |
| avg_yearly_balance | decimal | Teradata `bank_marketing` | balance | passthrough; currency assumed EUR (source is a Portuguese bank study) — tagged with a currency code at seed like every other monetary column (D-12) | No |
| prior_campaign_outcome | string | Teradata `bank_marketing` | poutcome | passthrough — feeds BQ-06 cross-sell ranking (ADR-006 D6.4) | Yes (source uses "unknown" for no-prior-contact, kept as an explicit category, not nulled) |
| subscribed_term_deposit | boolean | Teradata `bank_marketing` | y | passthrough — one more product flag for BQ-01's product-mix count | No |

### Target: `silver.sil_card_txn` (PaySim)
| Target column | Type | Source | Source column | Transform rule | Nullable? |
|---|---|---|---|---|---|
| txn_id | string | MSSQL PaySim | generated at seed (D-03.1, every table gets a PK) | passthrough | No |
| txn_ts | timestamp | MSSQL PaySim | step | `base_date + step * INTERVAL 1 HOUR`, rebased so max(txn_ts) = seed day (R-06, D-03.2/3) | No |
| txn_type | string | MSSQL PaySim | type | passthrough | No |
| amount | decimal | MSSQL PaySim | amount | passthrough; currency = "unitless" tagged as a documented source quirk (R-14) | No |
| name_orig / name_dest | string | MSSQL PaySim | nameOrig/nameDest | passthrough; joined to `customer_id` via `dim_customer_xwalk` | Yes for merchant dest (R-09) |
| is_fraud | boolean | MSSQL PaySim | isFraud | passthrough — **this is the Gold KPI label** (R-08) | No |
| is_flagged_fraud | boolean | MSSQL PaySim | isFlaggedFraud | passthrough — **rule-performance analysis only, NEVER used as the fraud KPI** (R-08) | No |
| account_number_masked | string | MSSQL PaySim | nameOrig/nameDest (where account-shaped) | mask to last 4 chars (D-07) | Yes |

### Target: `silver.sil_application` (Home Credit)
| Target column | Type | Source | Source column | Transform rule | Nullable? |
|---|---|---|---|---|---|
| sk_id_curr | string | Postgres `application` | SK_ID_CURR | passthrough (PK) | No |
| target_default | boolean | Postgres `application` | TARGET | passthrough — default label | Yes (test set rows may lack it — DQ-gate expectation, not silently coerced) |
| income_total | decimal | Postgres `application` | AMT_INCOME_TOTAL | passthrough; feeds income-band segment (journey/03) | Per null-rate expectation (R-04) |
| income_type | string | Postgres `application` | NAME_INCOME_TYPE | passthrough | Per null-rate expectation |
| kept_ext_columns | — | Postgres `application` | ~122 `EXT_SOURCE_*`-style anonymized columns | Bronze keeps ALL verbatim (R-02); Silver prunes to the STTM-selected set above — any column not listed here is dropped at Silver, not silently carried forward | — |

## Transform conventions used project-wide
- **Naming**: `snake_case` everywhere; Silver tables prefixed `sil_`, Gold dims `dim_`, facts
  `fact_`, marts `mart_` (matches `gates/framework.yml` `model_globs`/doc-reference C1 check).
- **Null handling**: explicit `NULL`, never a sentinel value (no `-1`/`"UNKNOWN"` string standing
  in for missing data) — EXCEPT the documented unknown-member surrogate key `-1` for a genuinely
  late-arriving dimension row (R-29, journey/04), which is a join-key sentinel, not a data-value
  sentinel.
- **Timezone/date normalization**: all timestamps normalized to UTC at Silver; `dt=YYYY-MM-DD`
  partitioning uses the UTC date.
- **Currency (D-12)**: every monetary column carries a currency code from seed. Gold normalizes to
  one reporting currency (MYR) via a static FX seed table; BNM OpenAPI is an optional live enrich,
  never a build dependency.
- **PII masking (D-07, enforced at Silver, never at Bronze)**: account/card numbers → last-4 only;
  `birth_number` → decoded to `birth_date`+`gender`, raw value dropped, never persisted past the
  decode step.

## Drift check
Once `pipeline/silver/*.py` and `pipeline/gold/*.py` files exist (Fasa C/D), every `sil_`/`dim_`/
`fact_`/`mart_` token referenced across `journey/` and `governance/` must resolve to a real file —
enforced automatically by `gates/doc_reference_contract.py` (C1 check) the moment the first such
file is created. Pre-build (now): no model files exist yet, so this check is dormant by
construction (`known` objects set is empty) — expected and not a false pass, per that gate's
own C1 logic.
