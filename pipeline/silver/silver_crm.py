#!/usr/bin/env python3
"""Bronze -> Silver, Berka/Salesforce ("Internal CRM") domain (ADR-007 D7.1 — split out
of the former build_silver.py so a birth_number-decode spike or other CRM-domain failure
never blocks the other 4 domains). Source is now Salesforce Bronze (ADR-006 Add #2 —
Bulk API 2.0 + `SystemModstamp` watermark, NOT the old SAP HANA CDC-log shape), covering
`client` (Contact, birth_number decode R-12), `account` (Account), `disp`
(AccountContactRelation — resolves native client_id/account_id back out of Salesforce's
own record Ids, R-12/ADR-006 Add #2), `trans` (Transaction__c), `district` (District__c),
and the new `sil_crm_case` (Case, BQ-03 enrichment).

`card`/`loan` are NOT built here — ADR-006 Add #2's build-scope note: neither Berka table
is loaded into Salesforce (no Gold builder reads them), a disclosed narrowing from the
original 7-table Berka set, not a silent drop.

Deliberate STTM correction (surfaced, not silent): journey/05_STTM.md's `sil_crm_case` row
names its resolved-identity column `customer_id`, but every other CRM Silver table (and
the existing `mart_fraud_followup.py` precedent) keeps `client_id` (the native Berka key)
at Silver and resolves to bank-wide `customer_id` at Gold via `dim_customer_xwalk` — Silver
depending on a Gold artifact would invert the medallion's dependency direction (ADR-003).
`sil_crm_case` keeps `client_id` here for the same reason; `mart_fraud_followup.py` does
the xwalk join, exactly as it already does for `sil_client`.

Not executed against live Bronze data this session (no Spark/cloud connection here, per
owner instruction) — written and py_compile-checked; live-run verification is pending the
dedicated Codespace (BUILD_REPORT.md).
"""

from __future__ import annotations

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from pyspark.sql.types import StringType

from pipeline.common.lake_paths import layer_path
from pipeline.silver.birth_number_decode import InvalidBirthNumber, decode_birth_number
from pipeline.silver.common import mask_last4, merge_upsert


def build_sil_client(spark: SparkSession) -> None:
    """Salesforce `Contact` -> `sil_client` — decode birth_number -> birth_date + gender
    (R-12), DROP the raw value after decode (D-07). Rows that fail to decode go to a
    quarantine table, counted and reported (same discipline as R-03 orphans), never
    silently dropped or coerced."""
    raw = (
        spark.read.format("delta").load(layer_path("bronze", "salesforce", "contact"))
        .select(
            col("berka_client_id__c").alias("client_id"),
            col("birth_number__c").alias("birth_number"),
            col("berka_district_id__c").alias("district_id"),
        )
    )

    def _decode_row(birth_number: str):
        try:
            birth_date, gender = decode_birth_number(birth_number)
            return str(birth_date), gender, None
        except InvalidBirthNumber as e:
            return None, None, str(e)

    from pyspark.sql.functions import udf
    from pyspark.sql.types import ArrayType

    decode_udf = udf(_decode_row, ArrayType(StringType()))
    decoded = raw.withColumn("_decoded", decode_udf(col("birth_number")))
    decoded = decoded.withColumn("birth_date", col("_decoded")[0]) \
                      .withColumn("gender", col("_decoded")[1]) \
                      .withColumn("_decode_error", col("_decoded")[2]) \
                      .drop("_decoded", "birth_number")  # D-07 — raw dropped after decode

    clean = decoded.filter(col("_decode_error").isNull()).drop("_decode_error")
    quarantined = decoded.filter(col("_decode_error").isNotNull())

    merge_upsert(spark, clean, "silver", "client", "client_id")
    if quarantined.count() > 0:
        quarantined.write.format("delta").mode("append").save(layer_path("silver", "_quarantine_client_birth_number"))
        print(f"WARNING: {quarantined.count()} client rows quarantined for unparseable birth_number (R-12)")


def build_sil_account(spark: SparkSession) -> None:
    """Salesforce `Account` -> `sil_account`. Passthrough, naming only (STTM)."""
    raw = (
        spark.read.format("delta").load(layer_path("bronze", "salesforce", "account"))
        .select(
            col("berka_account_id__c").alias("account_id"),
            col("berka_district_id__c").alias("district_id"),
            col("berka_frequency__c").alias("frequency"),
            col("berka_account_open_date__c").alias("date"),
        )
    )
    merge_upsert(spark, raw, "silver", "account", "account_id")


def build_sil_disp(spark: SparkSession) -> None:
    """Salesforce `AccountContactRelation` -> `sil_disp`. The native N:N bridge (kept as a
    bridge table, not flattened into a CTE) — but its `AccountId`/`ContactId` are
    Salesforce record Ids, not Berka's native `account_id`/`client_id`; this transform
    resolves them back via `Contact`/`Account` Bronze (a Silver-layer join, same "resolve
    identity at Silver" discipline ADR-006 Add #2 already applies to `client_id`)."""
    disp = (
        spark.read.format("delta").load(layer_path("bronze", "salesforce", "accountcontactrelation"))
        .select(
            col("berka_disp_id__c").alias("disp_id"),
            col("ContactId").alias("_contact_sf_id"),
            col("AccountId").alias("_account_sf_id"),
            col("berka_disp_type__c").alias("type"),
        )
    )
    contact = (
        spark.read.format("delta").load(layer_path("bronze", "salesforce", "contact"))
        .select(col("Id").alias("_contact_sf_id"), col("berka_client_id__c").alias("client_id"))
    )
    account = (
        spark.read.format("delta").load(layer_path("bronze", "salesforce", "account"))
        .select(col("Id").alias("_account_sf_id"), col("berka_account_id__c").alias("account_id"))
    )

    resolved = (
        disp.join(contact, "_contact_sf_id", "left")
        .join(account, "_account_sf_id", "left")
        .drop("_contact_sf_id", "_account_sf_id")
    )
    merge_upsert(spark, resolved, "silver", "disp", "disp_id")


def build_sil_trans(spark: SparkSession) -> None:
    """Salesforce `Transaction__c` -> `sil_trans`. `partner_account` (Berka's own `account`
    column — a real counterparty bank account number, unlike the `account_id` surrogate
    join key) is masked to last-4 (D-07); `account_id` itself is NOT masked here — it is
    the join key `fact_txn.py`/`mart_cross_sell.py`/`mart_daily_flows.py` use to bridge to
    `sil_disp`, and masking it would silently break every one of those joins.

    `currency="CZK"` (D-12/R-14): tagged HERE at Silver rather than literally at the
    Salesforce seed object — adding a new custom field to the live org is an owner-only
    Setup UI action (established precedent, PROJECT_STATUS.md history), out of this build
    session's reach. CZK is a single invariant constant for 100% of Berka rows (a Czech
    bank), same as PaySim/Teradata's own seed-time tags are single constants applied
    uniformly (`seed/mssql/load_paysim.py`, `seed/teradata/load_bank_marketing.py`) — a
    Silver-layer tag is equivalent in correctness, just applied one layer later. Previously
    this was a `lit("CZK")` scattered inside `pipeline/gold/fact_txn.py`'s Gold builder,
    invisible to the R-14 Silver->Gold completeness gate — moved here so the gate can check
    it like every other source's currency column."""
    raw = (
        spark.read.format("delta").load(layer_path("bronze", "salesforce", "transaction"))
        .select(
            col("berka_trans_id__c").alias("trans_id"),
            col("berka_account_id__c").alias("account_id"),
            col("trans_date__c").alias("date"),
            col("trans_type__c").alias("type"),
            col("operation__c").alias("operation"),
            col("amount__c").cast("double").alias("amount"),
            col("balance__c").cast("double").alias("balance"),
            col("k_symbol__c").alias("k_symbol"),
            col("bank__c").alias("bank"),
            col("partner_account__c").alias("partner_account"),
            lit("CZK").alias("currency"),
        )
    )
    raw = mask_last4(raw, "partner_account")
    merge_upsert(spark, raw, "silver", "trans", "trans_id")


def build_sil_district(spark: SparkSession) -> None:
    """Salesforce `District__c` -> `sil_district`. Narrowed field set (name + region only —
    ADR-006 Add #2 build-scope note): this table exists to satisfy `client.district_id`'s
    R-03 orphan-check, not for the full ~13-column Berka demographic detail, which no Gold
    builder reads."""
    raw = (
        spark.read.format("delta").load(layer_path("bronze", "salesforce", "district"))
        .select(
            col("berka_district_id__c").alias("district_id"),
            col("district_name__c").alias("name"),
            col("region__c").alias("region"),
        )
    )
    merge_upsert(spark, raw, "silver", "district", "district_id")


def build_sil_crm_case(spark: SparkSession) -> None:
    """Salesforce `Case` -> `sil_crm_case` (BQ-03 enrichment, ADR-006 Add #2 — replaces the
    synthetic `client.updated_at` proxy `mart_fraud_followup.py` used before). Keeps
    `client_id` (native Berka key) at Silver, same layering as `sil_client` — see module
    docstring for why this deviates from the STTM's literal `customer_id` column name."""
    case = (
        spark.read.format("delta").load(layer_path("bronze", "salesforce", "case"))
        .select(
            col("Id").alias("case_id"),
            col("ContactId").alias("_contact_sf_id"),
            col("CreatedDate").alias("opened_at"),
            col("Type").alias("case_type"),
        )
    )
    contact = (
        spark.read.format("delta").load(layer_path("bronze", "salesforce", "contact"))
        .select(col("Id").alias("_contact_sf_id"), col("berka_client_id__c").alias("client_id"))
    )
    resolved = case.join(contact, "_contact_sf_id", "left").drop("_contact_sf_id")
    merge_upsert(spark, resolved, "silver", "crm_case", "case_id")


def main() -> int:
    from pipeline.common.spark_session import get_spark

    spark = get_spark("silver_crm")
    build_sil_client(spark)
    build_sil_account(spark)
    build_sil_disp(spark)
    build_sil_trans(spark)
    build_sil_district(spark)
    build_sil_crm_case(spark)
    print("silver_crm complete: 6 tables.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
