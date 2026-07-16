#!/usr/bin/env python3
"""Simulate live source traffic across all seeded sources (D-03.5).

Every interval: a few random rows per source get INSERT'd or UPDATE'd, always touching
`updated_at`; soft-deletes flip `is_deleted` rather than physically removing a row (D-06 —
hard deletes are a Fasa C CDC concern, R-25). For Teradata, INSERT/UPDATE/DELETE naturally
fire the CDC triggers (ADR-006 D6.3) automatically — no special-casing needed. Salesforce
(ADR-006 Add #2) has no trigger/CDC-log surface at all — its drip is a plain REST API field
edit on a random sample of Contacts, which bumps `SystemModstamp` automatically (Salesforce
system-managed field, not settable directly) so the next `salesforce_extract.py` Bulk API
pull picks the touched rows up via its `SystemModstamp` watermark, same effect as the
DB-trigger sources reaching their `_cdc_log`.

Not run by this session (owner instruction — no live connections here). Meant to run
continuously in whichever environment executes the pipeline, once all 4 sources are seeded.

Run:  python drip_feed.py --interval-seconds 60 --rows-per-tick 5
"""

from __future__ import annotations

import argparse
import datetime as dt
import os
import time

from sqlalchemy import create_engine, text


def _postgres_engine():
    user, pwd = os.environ["POSTGRES_USER"], os.environ["POSTGRES_PASSWORD"]
    host = os.environ.get("POSTGRES_HOST", "localhost")
    port = os.environ.get("POSTGRES_PORT", "5432")
    db = os.environ.get("POSTGRES_DB", "banking_sales")
    return create_engine(f"postgresql+psycopg2://{user}:{pwd}@{host}:{port}/{db}") # secrets-scan:allow — built from env vars, not a literal secret


def _mssql_engine():
    user = os.environ.get("MSSQL_USER", "sa")
    pwd = os.environ["MSSQL_PASSWORD"]
    host = os.environ.get("MSSQL_HOST", "localhost")
    port = os.environ.get("MSSQL_PORT", "1433")
    db = os.environ.get("MSSQL_DB", "banking_cards")
    return create_engine(
        f"mssql+pyodbc://{user}:{pwd}@{host}:{port}/{db}?driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=yes" # secrets-scan:allow — built from env vars, not a literal secret
    )


def _teradata_connection():
    import teradatasql
    return teradatasql.connect(
        host=os.environ["TERADATA_HOST"], user=os.environ["TERADATA_USER"],
        password=os.environ["TERADATA_PASSWORD"],
    )


def _touch_random_rows_sqlalchemy(engine, table: str, pk_column: str, n: int, soft_delete_every: int) -> int:
    now = dt.datetime.now(dt.timezone.utc)
    with engine.begin() as conn:
        pks = conn.execute(
            text(f'SELECT "{pk_column}" FROM "{table}" ORDER BY random() LIMIT :n'), {"n": n}
        ).scalars().all()
        for i, pk in enumerate(pks):
            is_delete = soft_delete_every and (i % soft_delete_every == 0)
            conn.execute(
                text(f'UPDATE "{table}" SET updated_at = :ts, is_deleted = :del WHERE "{pk_column}" = :pk'),
                {"ts": now, "del": is_delete, "pk": pk},
            )
        return len(pks)


def _touch_random_rows_dbapi(connection, table: str, pk_column: str, n: int, soft_delete_every: int) -> int:
    now = dt.datetime.now(dt.timezone.utc)
    cur = connection.cursor()
    cur.execute(f'SELECT "{pk_column}" FROM "{table}"')
    all_pks = [row[0] for row in cur.fetchall()]
    if not all_pks:
        return 0
    import random
    picked = random.sample(all_pks, min(n, len(all_pks)))
    for i, pk in enumerate(picked):
        is_delete = soft_delete_every and (i % soft_delete_every == 0)
        cur.execute(
            f'UPDATE "{table}" SET updated_at = ?, is_deleted = ? WHERE "{pk_column}" = ?',
            (now.isoformat(), str(is_delete), pk),
        )  # fires the AFTER UPDATE CDC trigger automatically (ADR-006 D6.3)
    connection.commit()
    return len(picked)


def _touch_random_rows_salesforce(sf, sf_object: str, touch_field: str, n: int) -> int:
    """Salesforce has no trigger/CDC-log surface (ADR-006 Add #2) and `SystemModstamp` is
    system-managed (not directly settable), so "touching" a row here means a real REST
    field UPDATE with the field's OWN current value re-written — a genuine no-op value
    change, same shape as a DB `UPDATE ... SET updated_at = now()` touch, that still bumps
    `SystemModstamp` so the next `salesforce_extract.py` Bulk API pull picks it up. No
    soft-delete simulation for Salesforce — Contact/Account have no `is_deleted`-shaped
    field seeded onto them (D-06's soft-delete convention doesn't extend to this source)."""
    result = sf.query(f"SELECT Id, {touch_field} FROM {sf_object} LIMIT {n * 5}")
    records = result["records"]
    if not records:
        return 0
    import random
    picked = random.sample(records, min(n, len(records)))
    sf_type = getattr(sf, sf_object)
    for rec in picked:
        sf_type.update(rec["Id"], {touch_field: rec[touch_field]})
    return len(picked)


# One representative table per source is drip-fed — same INSERT/UPDATE/soft-delete shape
# applies to every other seeded table, this is not special-cased beyond the demo table.
DRIP_TARGETS = {
    "postgres": ("application", "SK_ID_CURR"),
    "mssql": ("paysim_transactions", "txn_id"),
    "teradata": ("bank_marketing", "customer_id"),
}
SALESFORCE_DRIP_TARGET = ("Contact", "berka_client_id__c")  # (sf_object, touch_field)


def _tick(rows_per_tick: int, soft_delete_every: int) -> None:
    now = dt.datetime.now(dt.timezone.utc)
    print(f"[{now.isoformat()}] drip-feed tick starting ({rows_per_tick} rows/source)")

    table, pk = DRIP_TARGETS["postgres"]
    n = _touch_random_rows_sqlalchemy(_postgres_engine(), table, pk, rows_per_tick, soft_delete_every)
    print(f"  postgres.{table}: {n} rows touched")

    table, pk = DRIP_TARGETS["mssql"]
    n = _touch_random_rows_sqlalchemy(_mssql_engine(), table, pk, rows_per_tick, soft_delete_every)
    print(f"  mssql.{table}: {n} rows touched")

    from pipeline.extract.salesforce_auth import get_salesforce_client
    sf_object, touch_field = SALESFORCE_DRIP_TARGET
    n = _touch_random_rows_salesforce(get_salesforce_client(), sf_object, touch_field, rows_per_tick)
    print(f"  salesforce.{sf_object}: {n} rows touched (SystemModstamp bumped, ADR-006 Add #2)")

    table, pk = DRIP_TARGETS["teradata"]
    n = _touch_random_rows_dbapi(_teradata_connection(), table, pk, rows_per_tick, soft_delete_every)
    print(f"  teradata.{table}: {n} rows touched (CDC trigger fired per row, ADR-006)")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--interval-seconds", type=int,
                     default=int(os.environ.get("DRIP_FEED_INTERVAL_SECONDS", 60)))
    ap.add_argument("--rows-per-tick", type=int,
                     default=int(os.environ.get("DRIP_FEED_ROWS_PER_TICK", 5)))
    ap.add_argument("--soft-delete-every", type=int, default=10,
                     help="every Nth touched row is a soft-delete instead of a plain update (D-06)")
    ap.add_argument("--once", action="store_true", help="run a single tick and exit (for testing)")
    args = ap.parse_args()

    _tick(args.rows_per_tick, args.soft_delete_every)
    if args.once:
        return 0
    while True:
        time.sleep(args.interval_seconds)
        _tick(args.rows_per_tick, args.soft_delete_every)


if __name__ == "__main__":
    raise SystemExit(main())
