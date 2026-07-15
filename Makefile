.PHONY: gates seed-sources fetch-datasets build-xwalk build-fx-rates seed-postgres seed-mssql seed-sap-hana seed-teradata seed-all drip-feed docker-up docker-down

# Governance gates (gates/framework.yml-driven) — safe to run anywhere, no cloud/creds needed.
gates:
	python3 gates/journey_completeness.py
	python3 gates/boundary_contract.py
	python3 gates/doc_reference_contract.py
	python3 gates/secrets_scan.py

# --- Fasa A: sources + seeding (run in the environment that actually executes the pipeline,
# NOT the planning/build session — see PROJECT_STATUS.md). ---

docker-up:
	docker compose up -d postgres mssql

docker-down:
	docker compose down

fetch-datasets:
	python3 scripts/fetch_datasets.py

build-xwalk:
	python3 seed/build_xwalk.py --data-dir data/raw --out seed/artifacts/dim_customer_xwalk.csv

build-fx-rates:
	python3 seed/build_fx_rates.py --out seed/artifacts/fx_rates.csv

seed-postgres:
	python3 seed/postgres/load_home_credit.py --data-dir data/raw/home_credit

seed-mssql:
	python3 seed/mssql/load_paysim.py --data-dir data/raw/paysim

seed-sap-hana:
	python3 seed/sap_hana/load_berka.py --data-dir data/raw/berka

seed-teradata:
	python3 seed/teradata/load_bank_marketing.py --csv data/raw/bank_marketing/bank-full.csv \
		--xwalk seed/artifacts/dim_customer_xwalk.csv

# Order matters: xwalk needs raw CSVs on disk (not the DBs); Teradata needs the xwalk.
seed-all: fetch-datasets build-xwalk build-fx-rates seed-postgres seed-mssql seed-sap-hana seed-teradata

drip-feed:
	python3 drip_feed.py
