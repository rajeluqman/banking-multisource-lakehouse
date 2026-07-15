#!/usr/bin/env python3
"""Seed Salesforce ("Internal CRM") from the Berka CSVs (ADR-006 Add #2 — replaces
`seed/sap_hana/load_berka.py`).

Object mapping (journey/01_DATASET_AND_SOURCES.md, journey/05_STTM.md):
  district -> District__c (NEW custom object), client -> Contact, account -> Account,
  disp -> AccountContactRelation (native N:N — bridge, not a CTE) PLUS each client's
  primary Contact.AccountId (see "Live-verified Salesforce requirement" below), trans ->
  Transaction__c (NEW custom object). `card`/`loan` are NOT loaded (build-scope decision,
  ADR-006 Add #2 — neither is read by any Gold builder, so this is a disclosed narrowing of
  the original 7-table Berka set, not a silent drop). CRM tickets -> Case are SEED-TIME
  SYNTHETIC (Berka has no native ticketing table at all) — a small seeded-random subset of
  Contacts get one Case each, per the same class of deliberate seed-time invention already
  accepted for Teradata's customer linkage (R-38).

**Salesforce org setup required before a live run (owner action, beyond the
birth_number__c/berka_client_id__c fields already created on Contact per BUILD_REPORT.md
§11)** — this is new scope this session surfaced, not silently worked around:
  1. Two NEW custom objects: `Transaction__c`, `District__c` (API-accessible, no special
     licensing needed on a Developer/trial org).
  2. NEW custom fields: `Contact.berka_district_id__c` (Text); `Account.berka_account_id__c`
     (Text, external ID), `Account.berka_district_id__c` (Text), `Account.berka_frequency__c`
     (Text), `Account.berka_account_open_date__c` (Date); `AccountContactRelation.
     berka_disp_id__c` (Text, external ID), `AccountContactRelation.berka_disp_type__c`
     (Text — NOT the native `Roles` picklist, whose default values don't include Berka's
     OWNER/DISPONENT and would fail insert); `Transaction__c.*` (berka_trans_id__c external
     ID + one field per Berka `trans` column, all Text/Number/Date); `District__c.*`
     (berka_district_id__c external ID + district_name__c + region__c — a narrowed field set,
     not all ~13 Berka district demographic columns, since nothing downstream reads them
     beyond the district_id orphan-check, R-03).
  3. Case.Type picklist needs 3 new values added: "Fraud Follow-up", "Card Dispute",
     "General Inquiry" (STTM `case_type`).
  4. "Set Audit Fields upon Record Creation" org permission enabled, so this script's
     synthetic `CreatedDate` on Case actually sticks (otherwise Salesforce silently
     overwrites it with the insert timestamp — every Case would land "today", collapsing
     BQ-03's SLA window to zero, not a subtle bug worth leaving undocumented).

**Two more constraints found via a real live run (not in any doc beforehand — TERRITORY,
not MAP), both now handled by this script rather than needing further owner action:**
  5. **Developer Edition data storage is tiny (live-confirmed: 5MB DataStorageMB cap).**
     Independently sampling each table at dev-loop scale (the old `--sample 5000`) blew
     through it — 4 of 6 objects landed 0-53% of what was submitted, all misreported as
     "loaded" because Bulk API 2.0 returns HTTP success even when every record fails
     server-side (fixed by `_report()` below, which reads the job's real
     `numberRecordsFailed`, not `len(records)`). This script now seeds a small, ACCOUNT-
     rooted coordinated sample (`--accounts`) instead of independently sampling every
     table, which also fixes a second problem: independent per-table sampling at small N
     gives near-zero client/account overlap in `disp`, so almost nothing would actually
     link — rooting client/disp/trans selection in the same sampled accounts keeps every
     loaded row genuinely connected.
  6. **A Contact needs a primary `Account` before Salesforce will create any ADDITIONAL
     (indirect) `AccountContactRelation` for it** — "Contacts to Multiple Accounts"
     platform rule, live-confirmed via a minimal test (a Contact with no `AccountId` gets
     every `AccountContactRelation` insert rejected with `INVALID_CROSS_REFERENCE_KEY:
     You can't associate a private contact with an account`, even though nothing in Setup
     is actually configuring any contact as "private"). Not an org Setup toggle — a data-
     modeling gap in how this script inserted Contacts. Fixed: for each client, the first
     linked account (OWNER type preferred) is set directly on `Contact.AccountId` via a
     bulk UPDATE; only a client's SECOND+ account (rare in Berka — most clients have
     exactly one) becomes an `AccountContactRelation` row.

Berka's `birth_number`/Berka column values are loaded VERBATIM here (D-05 — Bronze/raw
stays verbatim); the DOB/gender decode (R-12) happens at Silver, not at seed.

Schema assumed from the well-known public Berka/PKDD'99 layout (district A1-A16, client:
client_id/birth_number/district_id, account: account_id/district_id/frequency/date, disp:
disp_id/client_id/account_id/type, trans: trans_id/account_id/date/type/operation/amount/
balance/k_symbol/bank/account) — same "(unverified against the real .asc file)" caveat
`pipeline/gold/mart_cross_sell.py`/`mart_daily_flows.py` already carry for this dataset,
not a new assumption this file introduces.

Env: SALESFORCE_LOGIN_URL/CLIENT_ID/CLIENT_SECRET (Client Credentials Flow, see
pipeline/extract/salesforce_auth.py) — owner-provisioned, never run against a placeholder.
Does not download the CSVs itself — run scripts/fetch_datasets.py first.

Run:  python seed/salesforce/load_berka.py --data-dir data/raw/berka [--accounts N]
      [--trans-per-account N]
"""

from __future__ import annotations

import argparse
import csv
import datetime
import io
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from pipeline.extract.salesforce_auth import get_salesforce_client
from seed.common.seeding_utils import SEED_DAY, rebase_dates, seeded_random

CASE_TYPES = ["Fraud Follow-up", "Card Dispute", "General Inquiry"]
CASE_FRACTION = 0.15  # seeded-random share of Contacts that get one synthetic Case (dev-loop scale)


def _read_full(data_dir: Path, table: str) -> pd.DataFrame:
    asc_path = data_dir / f"{table}.asc"
    if not asc_path.exists():
        raise FileNotFoundError(f"{asc_path} not found — run scripts/fetch_datasets.py first "
                                 f"(the docs are a MAP, this file is the TERRITORY).")
    return pd.read_csv(asc_path, sep=";")


def _insert(sf, sf_object: str, records: list[dict]) -> list[dict]:
    return sf.bulk2.__getattr__(sf_object).insert(records=records) if records else []


def _update(sf, sf_object: str, records: list[dict]) -> list[dict]:
    return sf.bulk2.__getattr__(sf_object).update(records=records) if records else []


def _report(sf_object: str, op: str, job_results: list[dict]) -> int:
    """Real success count from the Bulk API 2.0 job's own `numberRecordsProcessed`/
    `numberRecordsFailed`, NOT `len(records)` — a prior version of this script trusted the
    submitted count as the loaded count and silently reported a 100%-server-side-rejected
    job as fully loaded (live-caught: 4 of 6 objects were actually 0-53% loaded)."""
    if not job_results:
        return 0
    processed = sum(r.get("numberRecordsProcessed", 0) for r in job_results)
    failed = sum(r.get("numberRecordsFailed", 0) for r in job_results)
    if failed:
        job_ids = [r["job_id"] for r in job_results]
        print(f"  WARNING: {sf_object} {op}: {failed}/{processed} record(s) FAILED — "
              f"job(s) {job_ids} (inspect via jobs/ingest/<id>/failedResults for reasons)")
    return processed - failed


def _id_map(sf, sf_object: str, insert_results: list[dict], key_field: str) -> dict[str, str]:
    """Maps native Berka key -> generated Salesforce Id, read back from the Bulk API 2.0
    ingest job's successful-records result (which echoes the original field values
    alongside the new `sf__Id`) — only truly-successful inserts land here, so downstream
    linkage (disp/trans) correctly treats a failed insert the same as a not-sampled row."""
    bulk_type = sf.bulk2.__getattr__(sf_object)
    mapping: dict[str, str] = {}
    for res in insert_results:
        csv_text = bulk_type.get_successful_records(res["job_id"])
        for row in csv.DictReader(io.StringIO(csv_text)):
            mapping[row[key_field]] = row["sf__Id"]
    return mapping


def _rebase_date_column(df: pd.DataFrame, raw_col: str) -> pd.DataFrame:
    dates = pd.to_datetime(df[raw_col], format="%y%m%d", errors="coerce").dt.date
    rebased = rebase_dates(dates.dropna().tolist()) if dates.notna().any() else dates
    return df.assign(_rebased_date=rebased)


def _load_district(sf, data_dir: Path) -> int:
    """District is always loaded in full — only 77 rows, storage-negligible, and every
    other table's `district_id` orphan-check (R-03) depends on the complete set."""
    df = _read_full(data_dir, "district")
    records = [
        {
            "berka_district_id__c": str(row["A1"]),
            "district_name__c": str(row.get("A2", "")),
            "region__c": str(row.get("A3", "")),
        }
        for _, row in df.iterrows()
    ]
    return _report("District__c", "insert", _insert(sf, "District__c", records))


def _sample_accounts(data_dir: Path, n_accounts: int | None) -> pd.DataFrame:
    df = _read_full(data_dir, "account")
    if n_accounts and len(df) > n_accounts:
        rng = seeded_random("berka.account")
        df = df.sample(n=n_accounts, random_state=rng.randint(0, 2**31)).reset_index(drop=True)
    return _rebase_date_column(df, "date")


def _load_account(sf, account_df: pd.DataFrame) -> tuple[int, dict[str, str]]:
    records = [
        {
            "Name": f"Berka Account {row['account_id']}",  # synthetic — Account.Name is required
            "berka_account_id__c": str(row["account_id"]),
            "berka_district_id__c": str(row["district_id"]),
            "berka_frequency__c": str(row["frequency"]),
            "berka_account_open_date__c": str(row["_rebased_date"]),
        }
        for _, row in account_df.iterrows()
    ]
    results = _insert(sf, "Account", records)
    account_id_map = _id_map(sf, "Account", results, "berka_account_id__c")
    return _report("Account", "insert", results), account_id_map


def _load_client(sf, data_dir: Path, linked_client_ids: set[str]) -> tuple[int, dict[str, str]]:
    """Only clients genuinely linked (via `disp`) to a seeded, successfully-inserted
    account are loaded — coordinated sampling (see module docstring point 5), not an
    independent per-table sample that would mostly fail to overlap at small N."""
    df = _read_full(data_dir, "client")
    df = df[df["client_id"].astype(str).isin(linked_client_ids)].reset_index(drop=True)
    records = [
        {
            "LastName": f"Berka Client {row['client_id']}",  # synthetic — Contact requires
            # LastName and Berka's anonymized dataset carries no real names (D-16, no PII invented)
            "berka_client_id__c": str(row["client_id"]),
            "birth_number__c": str(row["birth_number"]),  # verbatim (D-05); decode happens at Silver
            "berka_district_id__c": str(row["district_id"]),
        }
        for _, row in df.iterrows()
    ]
    results = _insert(sf, "Contact", records)
    contact_id_map = _id_map(sf, "Contact", results, "berka_client_id__c")
    return _report("Contact", "insert", results), contact_id_map


def _load_disp(sf, disp_df: pd.DataFrame,
               contact_id_map: dict[str, str], account_id_map: dict[str, str]) -> tuple[int, int]:
    """Resolves each client's PRIMARY account (OWNER type preferred) onto `Contact.
    AccountId` via bulk UPDATE — required before Salesforce allows any additional
    (indirect) `AccountContactRelation` for that Contact (module docstring point 6, live-
    verified). Any further account a client is linked to (rare in Berka — most clients have
    exactly one) becomes an `AccountContactRelation` row, same bridge-table shape as before."""
    resolved: list[tuple[str, str, str, str, str]] = []
    skipped = 0
    for _, row in disp_df.iterrows():
        contact_sf_id = contact_id_map.get(str(row["client_id"]))
        account_sf_id = account_id_map.get(str(row["account_id"]))
        if contact_sf_id is None or account_sf_id is None:
            skipped += 1  # client/account insert didn't succeed — see WARNING above
            continue
        resolved.append((str(row["client_id"]), contact_sf_id, account_sf_id, str(row["disp_id"]), str(row["type"])))
    if skipped:
        print(f"  disp: {skipped} row(s) skipped — linked client/account insert did not succeed")

    primary_by_client: dict[str, tuple[str, str, str, str]] = {}
    for client_id, contact_sf_id, account_sf_id, disp_id, disp_type in resolved:
        current = primary_by_client.get(client_id)
        if current is None or (disp_type == "OWNER" and current[3] != "OWNER"):
            primary_by_client[client_id] = (contact_sf_id, account_sf_id, disp_id, disp_type)

    update_records = [
        {"Id": contact_sf_id, "AccountId": account_sf_id}
        for contact_sf_id, account_sf_id, _disp_id, _disp_type in primary_by_client.values()
    ]
    n_primary = _report("Contact", "update (primary AccountId)", _update(sf, "Contact", update_records))

    # Setting Contact.AccountId auto-creates a "direct" AccountContactRelation record in
    # Salesforce, but that update has no way to populate the auto-created record's OWN
    # custom fields — without this follow-up, every primary relationship's
    # berka_disp_id__c/berka_disp_type__c stays blank. Live-caught: this silently broke
    # fact_txn.py's Berka join downstream (filters `disp.type == "OWNER"` — with every row
    # blank, 100% of Berka transactions resolved to a NULL customer_id). Query the
    # auto-created records back by (ContactId, AccountId) and patch them with the real
    # disp_id/type.
    if primary_by_client:
        contact_ids_csv = ", ".join(f"'{cid}'" for cid, _, _, _ in primary_by_client.values())
        direct_relations = {
            (row["ContactId"], row["AccountId"]): row["Id"]
            for row in sf.query_all(
                f"SELECT Id, ContactId, AccountId FROM AccountContactRelation WHERE ContactId IN ({contact_ids_csv})"
            )["records"]
        }
        disp_patch_records = [
            {"Id": direct_relations[(contact_sf_id, account_sf_id)], "berka_disp_id__c": disp_id, "berka_disp_type__c": disp_type}
            for contact_sf_id, account_sf_id, disp_id, disp_type in primary_by_client.values()
            if (contact_sf_id, account_sf_id) in direct_relations
        ]
        n_patched = _report("AccountContactRelation", "update (primary disp fields)",
                             _update(sf, "AccountContactRelation", disp_patch_records))
        print(f"  disp: {n_patched}/{len(primary_by_client)} primary AccountContactRelation record(s) patched with real disp_id/type")

    acr_records = []
    for client_id, contact_sf_id, account_sf_id, disp_id, disp_type in resolved:
        _primary_contact_sf_id, _primary_account_sf_id, primary_disp_id, _primary_type = primary_by_client[client_id]
        if disp_id == primary_disp_id:
            continue  # already represented via Contact.AccountId, not a second relation
        acr_records.append({
            "ContactId": contact_sf_id,
            "AccountId": account_sf_id,
            "berka_disp_id__c": disp_id,
            "berka_disp_type__c": disp_type,
        })
    n_acr = _report("AccountContactRelation", "insert", _insert(sf, "AccountContactRelation", acr_records))
    return n_primary, n_acr


def _load_trans(sf, data_dir: Path, account_ids: set[str], per_account_cap: int | None) -> int:
    """Filtered to the seeded accounts (not an independent sample — module docstring point
    5), then capped per-account so a handful of high-activity accounts can't alone blow the
    org's 5MB data-storage ceiling."""
    df = _read_full(data_dir, "trans")
    df = df[df["account_id"].astype(str).isin(account_ids)]
    if per_account_cap:
        rng = seeded_random("berka.trans")
        capped_groups = [
            group if len(group) <= per_account_cap
            else group.sample(n=per_account_cap, random_state=rng.randint(0, 2**31))
            for _, group in df.groupby("account_id")
        ]
        df = pd.concat(capped_groups, ignore_index=True) if capped_groups else df
    df = _rebase_date_column(df.reset_index(drop=True), "date")
    records = [
        {
            "berka_trans_id__c": str(row["trans_id"]),
            "berka_account_id__c": str(row["account_id"]),
            "trans_date__c": str(row["_rebased_date"]),
            "trans_type__c": str(row["type"]),
            "operation__c": str(row.get("operation", "")),
            "amount__c": str(row["amount"]),
            "balance__c": str(row["balance"]),
            "k_symbol__c": str(row.get("k_symbol", "")),
            "bank__c": str(row.get("bank", "")),
            "partner_account__c": str(row.get("account", "")),
        }
        for _, row in df.iterrows()
    ]
    return _report("Transaction__c", "insert", _insert(sf, "Transaction__c", records))


def _generate_cases(sf, contact_id_map: dict[str, str]) -> int:
    """Seed-time synthetic Case generation (STTM `sil_crm_case`) — Berka has no native
    ticketing table, so a seeded-random subset of Contacts get one Case each, with a
    seeded-random `CreatedDate` in the 30 days before SEED_DAY. This does NOT correlate
    with any real fraud event (Berka and PaySim are seeded independently, with no
    cross-source event timeline at seed time) — mart_fraud_followup.py's SLA metric
    against this is therefore still a simulated signal, same documented-gap discipline as
    the proxy it replaces (journey/03_DATA_REQUIREMENTS.md BQ-03 row), just backed by a
    real Case object/timestamp instead of a `client.updated_at` touch."""
    rng = seeded_random("salesforce.case")
    records = []
    for berka_client_id, contact_sf_id in contact_id_map.items():
        if rng.random() >= CASE_FRACTION:
            continue
        opened_at = SEED_DAY - datetime.timedelta(days=rng.randint(0, 30))
        records.append({
            "ContactId": contact_sf_id,
            "Type": rng.choice(CASE_TYPES),
            "CreatedDate": opened_at.isoformat() + "T00:00:00.000+0000",  # needs "Set Audit
            # Fields upon Record Creation" org permission (see module docstring) or Salesforce
            # silently overwrites this with the insert timestamp instead.
        })
    return _report("Case", "insert", _insert(sf, "Case", records))


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--data-dir", type=Path, default=Path("data/raw/berka"))
    ap.add_argument("--accounts", type=int, default=None,
                     help="coordinated dev-loop sample size (D-14): number of Berka accounts to seed. "
                         "Every client/disp/transaction loaded is filtered to genuinely link to one of "
                         "these accounts, not independently sampled per table — keeps real relationships "
                         "intact even at small scale. Omit for a full load (needs far more than a "
                         "Developer Edition org's ~5MB data-storage cap).")
    ap.add_argument("--trans-per-account", type=int, default=5,
                     help="max Transaction__c rows sampled per seeded account (storage-budget cap, dev-loop only)")
    args = ap.parse_args()

    sf = get_salesforce_client()

    n_district = _load_district(sf, args.data_dir)
    print(f"  district -> District__c: {n_district} rows loaded")

    account_df = _sample_accounts(args.data_dir, args.accounts)
    n_account, account_id_map = _load_account(sf, account_df)
    print(f"  account -> Account: {n_account} rows loaded")

    seeded_account_ids = set(account_id_map.keys())
    disp_df = _read_full(args.data_dir, "disp")
    disp_df = disp_df[disp_df["account_id"].astype(str).isin(seeded_account_ids)].reset_index(drop=True)
    linked_client_ids = set(disp_df["client_id"].astype(str))

    n_client, contact_id_map = _load_client(sf, args.data_dir, linked_client_ids)
    print(f"  client -> Contact: {n_client} rows loaded (coordinated to {len(seeded_account_ids)} seeded accounts)")

    n_primary, n_acr = _load_disp(sf, disp_df, contact_id_map, account_id_map)
    print(f"  disp -> {n_primary} Contact.AccountId (primary) + {n_acr} AccountContactRelation (secondary)")

    n_trans = _load_trans(sf, args.data_dir, seeded_account_ids, args.trans_per_account)
    print(f"  trans -> Transaction__c: {n_trans} rows loaded")

    n_case = _generate_cases(sf, contact_id_map)
    print(f"  synthetic Case: {n_case} rows generated")

    total = n_district + n_account + n_client + n_primary + n_acr + n_trans + n_case
    print(f"Berka/Salesforce seed complete: {total} total rows actually inserted/updated across 6 objects "
          f"(card/loan intentionally not loaded — ADR-006 Add #2 build-scope note).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
