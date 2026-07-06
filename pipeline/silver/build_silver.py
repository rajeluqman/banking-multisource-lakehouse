#!/usr/bin/env python3
"""Bronze -> Silver, CONTENT QUALITY gate (ADR-003 — never transport integrity, that's
promotion_gate.py). One builder function per Silver table; a generic `build_simple_table`
covers passthrough tables, dedicated functions handle the tables with real transform rules
(birth_number decode, column pruning, PII masking, orphan quarantine, fraud-label handling).

Not executed against live Bronze data this session (no Spark/cloud connection here, per
owner instruction) — written and py_compile-checked; live-run verification is pending the
dedicated Codespace (BUILD_REPORT.md).
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, first
from pyspark.sql.types import StringType

from pipeline.common.lake_paths import layer_path
from pipeline.silver.birth_number_decode import InvalidBirthNumber, decode_birth_number
from pipeline.silver.common import mask_last4, merge_upsert, orphan_quarantine

# ---- generic passthrough builder ----

def build_simple_table(
    spark: SparkSession, source: str, bronze_table: str, silver_table: str, pk_column: str,
    mask_columns: list[str] | None = None,
) -> DataFrame:
    """Read the latest Bronze state (batch tables: one row per PK already; CDC tables:
    replay op-log to latest-state per PK first — see `_latest_state_from_cdc_log`), apply
    any PII masking, MERGE into Silver."""
    bronze_path = layer_path("bronze", source, bronze_table)
    df = spark.read.format("delta").load(bronze_path)
    for column in mask_columns or []:
        if column in df.columns:
            df = mask_last4(df, column)
    merge_upsert(spark, df, "silver", silver_table, pk_column)
    return df


def _latest_state_from_cdc_log(spark: SparkSession, source: str, cdc_bronze_table: str, pk_column: str) -> DataFrame:
    """CDC Bronze holds a raw op-log (I/U/D events, ADR-006 D6.3) — Silver needs the LATEST
    state per pk_value, with 'D' ops excluded (soft-delete semantics, D-06; hard-delete
    replay is a Fasa C-later CDC concern per R-25, unchanged by ADR-006)."""
    events = spark.read.format("delta").load(layer_path("bronze", source, cdc_bronze_table))
    latest = (
        events.orderBy(col("seq").desc())
        .groupBy("pk_value")
        .agg(first("op").alias("latest_op"), first("changed_at").alias("latest_changed_at"))
    )
    return latest.filter(col("latest_op") != "D").withColumnRenamed("pk_value", pk_column)


# ---- dedicated builders (real transform rules) ----

def build_sil_client(spark: SparkSession) -> None:
    """Berka `client` (via SAP HANA CDC) — decode birth_number -> birth_date + gender
    (R-12), DROP the raw value after decode (D-07). Rows that fail to decode go to a
    quarantine table, counted and reported (same discipline as R-03 orphans), never
    silently dropped or coerced."""
    latest = _latest_state_from_cdc_log(spark, "sap_hana", "client_cdc", "client_id")
    raw = spark.read.format("delta").load(layer_path("bronze", "sap_hana", "client_cdc"))
    full = latest.join(raw.select("pk_value", "birth_number", "district_id"), latest.client_id == raw.pk_value)

    def _decode_row(birth_number: str):
        try:
            birth_date, gender = decode_birth_number(birth_number)
            return str(birth_date), gender, None
        except InvalidBirthNumber as e:
            return None, None, str(e)

    from pyspark.sql.functions import udf
    from pyspark.sql.types import ArrayType

    decode_udf = udf(_decode_row, ArrayType(StringType()))
    decoded = full.withColumn("_decoded", decode_udf(col("birth_number")))
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


def build_sil_application(spark: SparkSession) -> None:
    """Home Credit `application` — Bronze keeps ALL ~122 anonymized columns verbatim (R-02);
    Silver prunes to the STTM-selected set (journey/05_STTM.md), everything else dropped
    here, not silently carried forward."""
    KEEP_COLUMNS = [
        "SK_ID_CURR", "TARGET", "AMT_INCOME_TOTAL", "NAME_INCOME_TYPE", "ORGANIZATION_TYPE",
        "created_at", "updated_at", "is_deleted",
    ]
    df = spark.read.format("delta").load(layer_path("bronze", "postgres", "application"))
    pruned = df.select(*[c for c in KEEP_COLUMNS if c in df.columns])
    merge_upsert(spark, pruned, "silver", "application", "SK_ID_CURR")


def build_sil_bureau(spark: SparkSession) -> None:
    """R-03 — orphan `bureau` rows (no matching `application.SK_ID_CURR`) go to quarantine,
    counted and reported, never silently dropped."""
    bureau = spark.read.format("delta").load(layer_path("bronze", "postgres", "bureau"))
    application = spark.read.format("delta").load(layer_path("silver", "application"))
    clean, orphans = orphan_quarantine(bureau, "SK_ID_CURR", application, "SK_ID_CURR")
    merge_upsert(spark, clean, "silver", "bureau", "SK_ID_BUREAU")
    if orphans.count() > 0:
        orphans.write.format("delta").mode("append").save(layer_path("silver", "_quarantine_bureau_orphans"))
        print(f"WARNING: {orphans.count()} bureau rows quarantined as orphan FKs (R-03)")


def build_sil_card_txn(spark: SparkSession) -> None:
    """PaySim -> `sil_card_txn`. `isFraud` is the Gold KPI label; `isFlaggedFraud` is kept
    for rule-performance analysis ONLY (R-08) — this builder does not conflate them, and no
    downstream Gold model may read isFlaggedFraud as the fraud KPI (architect veto,
    journey/06_DQ_PLAN.md)."""
    df = spark.read.format("delta").load(layer_path("bronze", "mssql", "paysim_transactions"))
    df = df.withColumnRenamed("isFraud", "is_fraud").withColumnRenamed("isFlaggedFraud", "is_flagged_fraud")
    df = mask_last4(df, "nameOrig").withColumnRenamed("nameOrig", "name_orig_masked")
    merge_upsert(spark, df, "silver", "card_txn", "txn_id")


def build_sil_campaign_response(spark: SparkSession) -> None:
    """Teradata Bank Marketing (CDC) -> `sil_campaign_response`. No native key of its own
    (R-38) — `customer_id` was assigned at seed time, carried through Bronze verbatim."""
    latest = _latest_state_from_cdc_log(spark, "teradata", "bank_marketing_cdc", "customer_id")
    raw = spark.read.format("delta").load(layer_path("bronze", "teradata", "bank_marketing_cdc"))
    full = latest.join(
        raw.select("pk_value", "job", "marital", "education", "default", "balance", "poutcome", "y"),
        latest.customer_id == raw.pk_value,
    ).drop("pk_value")
    df = full.withColumnRenamed("default", "credit_in_default") \
             .withColumnRenamed("balance", "avg_yearly_balance") \
             .withColumnRenamed("poutcome", "prior_campaign_outcome") \
             .withColumnRenamed("y", "subscribed_term_deposit")
    merge_upsert(spark, df, "silver", "campaign_response", "customer_id")


# ---- straightforward passthrough tables (STTM: naming + masking only) ----

SIMPLE_TABLES = [
    # (source, bronze_table, silver_table, pk_column, mask_columns)
    ("sap_hana", "account_cdc", "account", "account_id", ["account_id"]),
    ("sap_hana", "disp_cdc", "disp", "disp_id", []),
    ("sap_hana", "card_cdc", "card", "card_id", ["card_id"]),
    ("sap_hana", "loan_cdc", "loan", "loan_id", []),
    ("sap_hana", "trans_cdc", "trans", "trans_id", ["account_id"]),
    ("sap_hana", "district_cdc", "district", "district_id", []),
    ("postgres", "previous_application", "previous_application", "SK_ID_PREV", []),
    ("obp", "accounts", "obp_accounts", "account_id", ["account_id"]),
    ("obp", "transactions", "obp_transactions", "transaction_id", []),
]


def main() -> int:
    from pipeline.common.spark_session import get_spark

    spark = get_spark("build_silver")
    build_sil_client(spark)
    build_sil_application(spark)
    build_sil_bureau(spark)
    build_sil_card_txn(spark)
    build_sil_campaign_response(spark)
    for source, bronze_table, silver_table, pk_column, mask_columns in SIMPLE_TABLES:
        build_simple_table(spark, source, bronze_table, silver_table, pk_column, mask_columns)
    print(f"Silver build complete: {6 + len(SIMPLE_TABLES)} tables.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
