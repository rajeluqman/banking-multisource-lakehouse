#!/usr/bin/env python3
"""Bronze -> Silver, Berka/SAP HANA Cloud ("Internal CRM") domain (ADR-007 D7.1 — split out
of the former build_silver.py so a birth_number-decode spike or other CRM-domain failure
never blocks the other 4 domains). Covers `client` (birth_number decode, R-12) and the 6
passthrough tables (account, disp, card, loan, trans, district).

Not executed against live Bronze data this session (no Spark/cloud connection here, per
owner instruction) — written and py_compile-checked; live-run verification is pending the
dedicated Codespace (BUILD_REPORT.md).
"""

from __future__ import annotations

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StringType

from pipeline.common.lake_paths import layer_path
from pipeline.silver.birth_number_decode import InvalidBirthNumber, decode_birth_number
from pipeline.silver.common import build_simple_table, latest_state_from_cdc_log, merge_upsert


def build_sil_client(spark: SparkSession) -> None:
    """Berka `client` (via SAP HANA CDC) — decode birth_number -> birth_date + gender
    (R-12), DROP the raw value after decode (D-07). Rows that fail to decode go to a
    quarantine table, counted and reported (same discipline as R-03 orphans), never
    silently dropped or coerced."""
    latest = latest_state_from_cdc_log(spark, "sap_hana", "client_cdc", "client_id")
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


SIMPLE_TABLES = [
    # (source, bronze_table, silver_table, pk_column, mask_columns)
    ("sap_hana", "account_cdc", "account", "account_id", ["account_id"]),
    ("sap_hana", "disp_cdc", "disp", "disp_id", []),
    ("sap_hana", "card_cdc", "card", "card_id", ["card_id"]),
    ("sap_hana", "loan_cdc", "loan", "loan_id", []),
    ("sap_hana", "trans_cdc", "trans", "trans_id", ["account_id"]),
    ("sap_hana", "district_cdc", "district", "district_id", []),
]


def main() -> int:
    from pipeline.common.spark_session import get_spark

    spark = get_spark("silver_crm")
    build_sil_client(spark)
    for source, bronze_table, silver_table, pk_column, mask_columns in SIMPLE_TABLES:
        build_simple_table(spark, source, bronze_table, silver_table, pk_column, mask_columns)
    print(f"silver_crm complete: {1 + len(SIMPLE_TABLES)} tables.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
