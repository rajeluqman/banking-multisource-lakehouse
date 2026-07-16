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
| obp_account_id | string | OBP `/accounts` | account_id | RESERVED — not populated in v1 (ADR-005 Add #2: OBP is Silver-terminal, not a conformed-dimension member) | Yes |
| source_priority_rank | int | derived | — | CRM(Berka)=1, loans(Home Credit)=3, cards(PaySim)=4 — rank **2 is a reserved gap** (the withdrawn OBP/"core" tier, ADR-005 Add #2; left un-renumbered so the seeded ranks stay stable). Only relative order matters (`dim_customer.py` `Window.orderBy(source_priority_rank asc)`). OBP never enters the golden record | No |

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
> **Correction (this build session): `client_id`, not `customer_id`.** `customer_id` only
> exists in `gold.dim_customer_xwalk` — a Silver table resolving it directly would invert
> the medallion's Silver->Gold dependency direction (ADR-003). `sil_crm_case` keeps
> `client_id` (native Berka key) at Silver, same as every other CRM Silver table;
> `pipeline/gold/mart_fraud_followup.py` does the `client_id` -> `customer_id` xwalk join
> itself, exactly as it already did against `sil_client`.
| Target column | Type | Source | Source column | Transform rule | Nullable? |
|---|---|---|---|---|---|
| case_id | string | Salesforce `Case` | Id | passthrough (PK) | No |
| client_id | string | Salesforce `Case` | ContactId — resolved via Contact `berka_client_id__c` | Silver-layer join (this table's own transform) recovers the native Berka `client_id` from Salesforce's ContactId lookup — Gold resolves `client_id` -> bank-wide `customer_id` via the xwalk, deterministic (Berka has a real master, no synthetic assignment) | No |
| opened_at | timestamp | Salesforce `Case` | CreatedDate | passthrough (UTC) — the real CRM-ticket timestamp BQ-03 previously lacked (journey/03 L8); seed-time synthetic (Berka has no native ticket table, seed/salesforce/load_berka.py `_generate_cases`), NOT causally linked to any real fraud event (Berka/PaySim seeded independently) | No |
| case_type | string | Salesforce `Case` | Type | passthrough — filter to `"Fraud Follow-up"` for BQ-03 | Yes |

### Target: `silver.sil_account` / `sil_disp` / `sil_trans` / `sil_district` (Salesforce Account/AccountContactRelation/Transaction__c/District__c, ADR-006 Add #2)
> `card`/`loan` (Berka's own tables, distinct from PaySim/Home Credit) are NOT loaded into
> Salesforce and have no Silver target in this build (build-scope note, seed/salesforce/
> load_berka.py docstring) — neither is read by any Gold builder.
| Target column | Type | Source | Source column | Transform rule | Nullable? |
|---|---|---|---|---|---|
| sil_account.account_id | string | Salesforce `Account` | berka_account_id__c | passthrough (PK) | No |
| sil_account.district_id | string | Salesforce `Account` | berka_district_id__c | passthrough, FK to `sil_district` | Yes (R-03 orphan check) |
| sil_disp.disp_id | string | Salesforce `AccountContactRelation` | berka_disp_id__c | passthrough (PK) | No |
| sil_disp.client_id / account_id | string | Salesforce `AccountContactRelation` | ContactId / AccountId | resolved from Salesforce record Ids back to native Berka keys via a Silver-layer join against `bronze.salesforce.contact`/`account` (native N:N bridge kept as a bridge table, not a CTE) | No |
| sil_trans.trans_id | string | Salesforce `Transaction__c` | berka_trans_id__c | passthrough (PK) | No |
| sil_trans.account_id | string | Salesforce `Transaction__c` | berka_account_id__c | passthrough (plain text FK, NOT masked — this is the join key `fact_txn.py`/`mart_cross_sell.py`/`mart_daily_flows.py` use to bridge to `sil_disp`) | No |
| sil_trans.amount / sil_trans.balance | decimal | Salesforce `Transaction__c` | amount__c / balance__c | passthrough | No |
| sil_trans.currency | string | derived at Silver | — | `lit("CZK")` (D-12/R-14, this session) — tagged HERE rather than at the Salesforce seed object: a new custom field on the live org is an owner-only Setup UI action, out of reach for a Silver/Gold-layer session; CZK is a single invariant constant for 100% of Berka rows (a Czech bank), same as PaySim/Teradata's own seed-time tags are single constants applied uniformly — a Silver-layer tag is equivalent in correctness, just applied one layer later. Previously this was a `lit("CZK")` scattered inside `pipeline/gold/fact_txn.py`'s Gold builder, invisible to the R-14 gate | No |
| sil_trans.partner_account | string | Salesforce `Transaction__c` | partner_account__c | masked to last-4 (D-07) — this is Berka's own `account` column, a real counterparty bank account number, unlike the `account_id` surrogate join key | Yes |
| sil_district.district_id | string | Salesforce `District__c` | berka_district_id__c | passthrough (PK) | No |
| sil_district.name / region | string | Salesforce `District__c` | district_name__c / region__c | passthrough — narrowed field set (not all ~13 Berka district demographic columns; nothing downstream reads them beyond the `district_id` orphan-check) | Yes |

### Target: `silver.sil_campaign_response` (Teradata — UCI Bank Marketing, ADR-006)
| Target column | Type | Source | Source column | Transform rule | Nullable? |
|---|---|---|---|---|---|
| customer_id | string | seed/teradata/load_bank_marketing.py | assigned (R-38) | deterministic sampled linkage to an existing `dim_customer_xwalk` customer_id — this is NOT a native key from the source, it's assigned at seed time | No |
| job / marital / education | string | Teradata `bank_marketing` | job / marital / education | passthrough; feeds BQ-05 segment enrichment (ADR-006 D6.4) | Per null-rate expectation |
| credit_in_default | boolean | Teradata `bank_marketing` | default | passthrough — second, independent default signal alongside Home Credit's `TARGET` (BQ-05); NEVER merged/reconciled automatically, surfaced as a disagreement if the two differ | No |
| avg_yearly_balance | decimal | Teradata `bank_marketing` | balance | passthrough; currency assumed EUR (source is a Portuguese bank study) — tagged at seed (`seed/teradata/load_bank_marketing.py`), threaded through Silver's `_VALUE_COLUMNS` (D-12/R-14 fix, this session — was tagged at seed and landed in Bronze but silently dropped at Silver until now, live-caught, never actually read/converted by any Gold builder so no downstream numbers were wrong) | No |
| currency | string | Teradata `bank_marketing` | currency (seed-tagged "EUR", `seed/teradata/load_bank_marketing.py`) | passthrough | No |
| prior_campaign_outcome | string | Teradata `bank_marketing` | poutcome | passthrough — feeds BQ-06 cross-sell ranking (ADR-006 D6.4) | Yes (source uses "unknown" for no-prior-contact, kept as an explicit category, not nulled) |
| subscribed_term_deposit | boolean | Teradata `bank_marketing` | y | passthrough — one more product flag for BQ-01's product-mix count | No |

### Target: `silver.sil_card_txn` (PaySim)
| Target column | Type | Source | Source column | Transform rule | Nullable? |
|---|---|---|---|---|---|
| txn_id | string | MSSQL PaySim | generated at seed (D-03.1, every table gets a PK) | passthrough | No |
| txn_ts | timestamp | MSSQL PaySim | step | `base_date + step * INTERVAL 1 HOUR`, rebased so max(txn_ts) = seed day (R-06, D-03.2/3) | No |
| txn_type | string | MSSQL PaySim | type | passthrough | No |
| amount | decimal | MSSQL PaySim | amount | passthrough | No |
| currency | string | MSSQL PaySim | currency (seed-tagged "MYR", `seed/mssql/load_paysim.py`) | passthrough — **doc correction, this session**: this row previously said currency = "unitless"; the actual seed code has always tagged `MYR` (the reporting-currency baseline, D-12), confirmed live against `seed/mssql/load_paysim.py` and Bronze's real schema — territory wins over the stale map entry (CLAUDE.md anti-shortcut rule #5) | No |
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
| currency | string | derived at Silver | — | `lit("unitless")` (D-12/R-14 exception, @staff-data-engineer sign-off this session) — anonymized Kaggle competition data, real-world currency unknown; tagged with `dim_fx_rate`'s non-convertible sentinel so D-12's "every monetary column carries a currency code" holds literally. Never fed through `to_myr` (`pipeline/gold/common.py`) — only percentile-banded within its own source (`mart_risk_segment.py`), never summed/converted | No |
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
  never a build dependency. **Built this session** (was previously documented but never
  implemented — a real, live correctness bug, `mart_daily_flows.py`/`mart_customer_360.py` were
  silently summing CZK+MYR together; BUILD_REPORT.md §16): `seed/artifacts/fx_rates.csv` +
  `pipeline/gold/dim_fx_rate.py` (grain: one row per `currency_code`, ADR-005 addendum #1) is the
  static seed table; `to_myr` (`pipeline/gold/common.py`) is the ONE join/conversion point (ADR-005 —
  no second resolution path), applied once at the fact layer (`fact_txn.amount_myr`,
  `fact_card_fraud.amount_myr`) and once in the shared `latest_balance_per_account` helper
  (`current_balance_myr`) — native `amount`/`currency`/`current_balance` columns are kept, never
  overwritten, for lineage. `pipeline/gold/dq_currency_gate.py` is the R-14 completeness gate
  (journey/06_DQ_PLAN.md), checking 6 monetary columns across all 5 sources. `AMT_INCOME_TOTAL`
  (Home Credit) is the one documented exception — tagged `unitless`, never converted.
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
