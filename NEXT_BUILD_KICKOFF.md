# Next Build Kickoff — Live data load into all 5 sources + real Fasa A→D proof

> Paste-able kickoff for a fresh session in the owner's dedicated Codespace.
> The Salesforce CRM swap CODE (source #4) is DONE — `BUILD_REPORT.md` §13, all 4 gates +
> `unittest discover tests` green. **Nothing has actually run against real data or a live DB/org
> yet** — no CSVs downloaded, no Docker containers started, Teradata suspended, Salesforce org
> missing the new custom objects/fields this build's code depends on. This doc is EXECUTION
> (real data, real connections) only. Do NOT re-litigate the Salesforce object model or
> ADR-006 Add #2 — if something looks wrong once you're running it for real, STOP and surface
> it (CLAUDE.md anti-shortcut rule), don't silently improvise a fix.

## What changed and why (one paragraph)
The prior session built all the CODE for the Salesforce swap (extractor, seed loader, Silver
transforms, orchestration) but confirmed via live `describe()` calls that the Salesforce org is
missing everything this code needs beyond the 2 fields already there
(`Contact.berka_client_id__c`/`birth_number__c`) — no `AccountContactRelation` (needs "Contacts
to Multiple Accounts" enabled), no `Transaction__c`/`District__c` custom objects, no new custom
fields, no `Case.Type` picklist values, `Case.CreatedDate` not API-settable. Nothing else (Kaggle
datasets, Postgres/MSSQL containers, Teradata) was touched at all this pass. This session's job
is to actually get real data flowing: download the datasets, stand up the two Docker sources,
get Salesforce's org set up and seeded, resume Teradata, then run promotion→Silver→Gold for real
and capture genuine evidence — not more code.

## Read first, in this order
1. `PROJECT_STATUS.md` "▶ RESUME HERE" (top 2026-07-15 block) — full state.
2. `BUILD_REPORT.md` §13 — the exact live-org gap checklist (Salesforce), install caveats
   (§11: `teradatasql` stale-cache fix), and the flagged-not-fixed `mart_pipeline_health.py` bug.
3. `journey/07_PIPELINE_SPEC.md` "Salesforce / Teradata prerequisites" section.
4. `scripts/fetch_datasets.py`, `docker-compose.yml` — read before running, the Berka Kaggle
   slug inside `fetch_datasets.py` is marked `(unverified)`.

## Prerequisite (owner action — some of this CANNOT be done from code/API)
- **Salesforce org** (Setup UI, owner only): enable "Contacts to Multiple Accounts"; create
  custom objects `Transaction__c` + `District__c`; create the custom fields listed in
  `BUILD_REPORT.md` §13's table (`Contact.berka_district_id__c`; 4 new `Account.*` fields; 2 new
  `AccountContactRelation.*` fields; ~10 `Transaction__c.*` fields; 3 `District__c.*` fields); add
  3 picklist values to `Case.Type` (`Fraud Follow-up`, `Card Dispute`, `General Inquiry`); enable
  "Set Audit Fields upon Record Creation" for the Client Credentials Flow's Run-As user. Confirm
  done before Task 5 below — don't attempt it half-configured, the Bulk API insert will just fail
  partway and leave a mess to clean up.
- **Teradata**: resume the ClearScape Analytics Experience environment in its dashboard (owner
  only — auto-stops on idle, no API to un-suspend it) before Task 6.
- Everything else (Kaggle download, Docker containers, Postgres/MSSQL seed, xwalk build) needs no
  owner action — Kaggle/AWS/etc. credentials are already live in `.env`.

## Task list (dependency order)

1. **Verify the Berka Kaggle dataset slug.** `scripts/fetch_datasets.py`'s `KAGGLE_DATASETS["berka"]
   = "sabrinaputridewi/czech-bank-financial-dataset"` is marked `(unverified)` — search Kaggle for
   the real "Czech bank financial dataset" / Berka slug and correct it in that file if wrong,
   BEFORE running the download (don't download blind against a guessed slug).

2. **Download all 4 raw datasets**: `python scripts/fetch_datasets.py` (Kaggle creds already in
   `.env`, verified working in a prior session). Lands into `data/raw/{home_credit,paysim,berka,
   bank_marketing}/`. Confirm file counts/sizes look sane before moving on (the docs are a MAP,
   the downloaded files are the TERRITORY — check real column names against `journey/05_STTM.md`'s
   assumptions, several of which are tagged `(unverified against the real .asc file)`).

3. **Start Postgres + MSSQL**: `docker-compose up -d`, wait for both healthchecks to pass. Run
   `python seed/postgres/load_home_credit.py` and `python seed/mssql/load_paysim.py` against the
   real downloaded CSVs.

4. **Build the customer crosswalk**: `python seed/build_xwalk.py --data-dir data/raw`. Reads raw
   CSVs directly (not the DBs) — can run right after step 2, doesn't need to wait for step 3.

5. **Salesforce — only after the owner confirms the org setup above is done.** Run
   `python seed/salesforce/load_berka.py --data-dir data/raw/berka` then
   `python pipeline/extract/salesforce_extract.py`. If the org isn't ready, SKIP this and mark it
   UNVERIFIED — do not attempt a partial run and paper over the failures.

6. **Teradata — only after the owner confirms ClearScape is resumed.** Run
   `python seed/teradata/load_bank_marketing.py` then `python pipeline/extract/teradata_extract.py`
   (remember the `teradatasql` stale-pip-cache caveat, `BUILD_REPORT.md` §11, if the driver import
   breaks again).

7. **Promotion gate + all 5 Silver domain pipelines**: `python pipeline/promote/promotion_gate.py`,
   then `pipeline/silver/silver_sales.py` / `silver_fraud.py` / `silver_crm.py` /
   `silver_marketing.py` / `silver_core_banking.py` — or drive the whole thing via
   `python pipeline/orchestrate.py` (use `--only` to scope to whichever sources actually have real
   data if some of steps 5/6 were skipped).

8. **Gold dims/facts/marts**: continue the same `orchestrate.py` run through to the marts. Sanity
   check `mart_pipeline_health.py` (BQ-10) output — remember the flagged-not-fixed Silver-path bug
   (`BUILD_REPORT.md` §13, item 2) will make `silver_row_count`/`reconciled` look wrong for every
   source, not just Salesforce; don't mistake that for a new regression.

9. **Capture real evidence into `journey/08_SERVING_AND_EVIDENCE.md`** — actual command output,
   actual row counts, per source. Mark whatever didn't run (skipped Salesforce/Teradata steps)
   UNVERIFIED explicitly, don't leave the doc implying a full run happened if it didn't.

## Gate before calling ANY of this done
```
python3 gates/journey_completeness.py
python3 gates/boundary_contract.py
python3 gates/doc_reference_contract.py
python3 gates/secrets_scan.py
python3 -m unittest discover tests
```
All four gates green + tests passing, same bar as every prior fasa — plus this time, real command
output in `journey/08_SERVING_AND_EVIDENCE.md`, not just green gates.

## Update before ending the session
`PROJECT_STATUS.md` "▶ RESUME HERE" + `BUILD_REPORT.md` — same discipline as every prior fasa.
