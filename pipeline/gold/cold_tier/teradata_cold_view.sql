-- pipeline/gold/cold_tier/teradata_cold_view.sql
--
-- Native Teradata cold-tier view (ADR-006 Addendum #1 — Teradata dual-role;
-- ADR-007 D7.4 Strategy 3 — hot/cold hybrid historical-data strategy).
--
-- Rows dated BEFORE the CDC cutover are never pulled into Landing/Bronze/Silver/Gold at
-- all — this view serves them directly off Teradata, queried by Power BI's composite model
-- via DirectQuery and UNIONed at report time with the Gold-sourced hot data (post-cutover
-- rows, which DO flow through the medallion via pipeline/extract/teradata_extract.py's CDC
-- poll + pipeline/silver/silver_marketing.py). Compute for this cold path runs entirely on
-- Teradata; nothing here is pulled through Databricks.
--
-- HARD RULE, not a style choice: AGGREGATE GRAIN ONLY. No `customer_id`, no row-level PII,
-- no column that could re-identify a person. Row-level cold data would bypass D-07 masking
-- and D-04 MDM resolution (no customer_id link without going through
-- dim_customer_xwalk) — aggregating away individual rows is exactly what makes skipping the
-- medallion safe here. A row-level DirectQuery to Teradata would be a real governance
-- violation (R-27-shaped) and is explicitly rejected — do not add customer_id or any other
-- row-identifying column to this view, no matter how convenient it would be for debugging.
--
-- CUTOVER-DATE PARAMETER: {{CDC_CUTOVER_DATE}} below is a PER-DEPLOYMENT value, not a
-- constant — it is the actual date seed/common/cdc_ddl.py's setup_cdc() installed the
-- `bank_marketing_cdc` triggers in THIS deployment's Teradata instance (recorded by whoever
-- ran seed/teradata/load_bank_marketing.py). It is intentionally NOT computed dynamically —
-- bank_marketing has no native per-row event timestamp of its own (the UCI source's
-- `day`/`month` columns describe the last-contact day of a marketing campaign with no year,
-- not a row-landing date, so they cannot substitute for a real cutover boundary); the only
-- real per-row timestamp is the seed-time `created_at`/`updated_at` (D-03.1). A
-- dynamically-derived cutover could silently mis-classify hot vs cold rows on every seed
-- re-run, which is worse than requiring an explicit literal value here before deploying.
-- Replace {{CDC_CUTOVER_DATE}} with a literal DATE (e.g. DATE '2026-07-06') before running
-- this DDL against a live instance.

REPLACE VIEW bank_marketing_pre_cutover_agg AS
SELECT
    job,
    education,
    poutcome                                     AS prior_campaign_outcome,
    EXTRACT(YEAR FROM created_at)                AS contact_year,
    EXTRACT(WEEK FROM created_at)                AS contact_week,
    COUNT(*)                                     AS contact_count,
    AVG(CAST(balance AS DECIMAL(12,2)))          AS avg_yearly_balance,
    SUM(CASE WHEN y = 'yes' THEN 1 ELSE 0 END)   AS subscribed_count
FROM bank_marketing
WHERE created_at < DATE '{{CDC_CUTOVER_DATE}}'
GROUP BY
    job,
    education,
    poutcome,
    EXTRACT(YEAR FROM created_at),
    EXTRACT(WEEK FROM created_at);
