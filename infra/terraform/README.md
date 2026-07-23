# `infra/terraform` — cloud primitives as code

What was click-ops until now: the S3 bucket, the IAM role Unity Catalog assumes, the UC storage
credential and external locations, and the Snowflake serving warehouse's cost guardrails. Those
are the resources that, when changed by hand, change silently.

## Ownership boundary — the point of this directory

| Layer | Owner | Where |
|---|---|---|
| Cloud primitives (S3, IAM), UC storage credential + external locations, storage-level grants, Snowflake warehouse + resource monitor | **Terraform** | this directory |
| The Databricks Job | **Databricks Asset Bundles** | `databricks.yml` |
| Catalog / schema / table grants | **SQL DDL** | `pipeline/gold/grants/uc_grants.sql` |

One owner per resource. There is deliberately **no `databricks_job` resource here** — two tools
reconciling one Job is drift, and the bundle already owns it. If asked "why isn't the job in
Terraform?", that is the answer.

The split between storage-level grants (here) and data-object grants (SQL) follows the same rule:
`READ_FILES`/`WRITE_FILES` on an external location is infrastructure; `SELECT` on
`banking.gold.mart_fraud_daily` is a data permission. They never overlap.

## State

There is no `backend` block and no state of record. This configuration reconciles primitives that
already exist, and the artifact of value is the **plan diff on a pull request**, not a stored
state file — which would rot the moment the disposable trial workspace is replaced (D-14). If
full lifecycle ownership is adopted later, add a backend then; nothing here needs restructuring
for it.

## The first-apply ordering

`var.databricks_unity_catalog_role_arn` is self-referential: Unity Catalog requires the IAM role's
trust policy to name the role it is attached to. On a first run the ARN does not exist yet.

1. Set the variable to the ARN the role *will* have —
   `arn:aws:iam::<account-id>:role/banking-lakehouse-uc-access`.
2. Apply. The role is created with a trust policy naming itself.

Constructing the ARN from known parts rather than reading it back is what breaks the cycle. This
replaces a manual console step that previously had to be repeated from memory.

## Running it

```bash
cd infra/terraform
cp terraform.tfvars.example terraform.tfvars   # gitignored; fill in
export DATABRICKS_TOKEN=...                    # never written to disk in this repo
export SNOWFLAKE_PASSWORD=...
terraform init
terraform plan
```

`terraform fmt -check`, `init -backend=false` and `validate` need **no credentials** and run on
every pull request (`.github/workflows/terraform.yml`). Only `plan` touches a real account.

## Verification status

- `terraform fmt -check` — clean
- `terraform init` + `terraform validate` — **passing**, against pinned providers
  (`aws ~> 5.60`, `databricks ~> 1.50`, `snowflakedb/snowflake ~> 1.0`) with
  `.terraform.lock.hcl` committed
- `terraform plan` against the live account — **not yet run**; it needs credentials this
  repository deliberately does not hold. Until that plan is captured, treat this directory as
  reviewed-and-valid configuration, not as a reconciled record of live infrastructure.
