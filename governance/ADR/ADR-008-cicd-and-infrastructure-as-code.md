# ADR-008 — CI/CD and Infrastructure-as-Code (Databricks Asset Bundles + gated GitHub Actions)

**Status:** Accepted
**Date:** 2026-07-16
**Owners:** owner (directed the build), staff-data-engineer (stack/tool authority + sign-off)
**Context refs:** ADR-002 Addendum #6 (git-native code delivery — this ADR is the promised
"graduates to its own ADR" graduation), ADR-002 Addendum #5 (`SINGLE_USER` cluster + secret-scope
S3A), ADR-007 D7.3 (decoupled config-driven orchestration, D-10 no-private-scheduler), D-14
portability mandate. Governed-doc updates: `journey/07_PIPELINE_SPEC.md`.

## Context
Git-native deployment onto Databricks is proven (ADR-002 Add #6): `git push` → Databricks Repos/
Jobs `git_source` → `run_now`, live-verified `RunResultState.SUCCESS`. But the Databricks Job that
runs it (id `778449103358221`) was created by a **one-off imperative `databricks-sdk`
`w.jobs.create(...)` script** — not infrastructure-as-code. There is no declarative source of truth
for the Job, no reproducible redeploy after the disposable trial is deleted (D-14), and the job
spec drifts silently from git. Separately, existing CI (`.github/workflows/ci.yml`) runs the four
governance gates but does NOT run the unit tests (`tests/`), and there is no CD stage at all. Owner
has directed "full CI/CD, implemented for the whole project." This ADR rules the tool and the
architecture; the build is handed to `@senior-data-engineer`.

**No model veto triggered.** This ADR touches no model, schema, grain, or storage path — it is
pure control-plane tooling. The Clean-ERD doctrine gate does not apply. Stated explicitly per
doctrine ("what's deliberately out stays named": the data model is untouched).

## Decision

### D8.1 — IaC tool: **Databricks Asset Bundles (DAB)**, not Terraform, not keep-SDK-scripts.
A single `databricks.yml` bundle at repo root becomes the declarative source of truth for the
Databricks Job(s). It defines the Job resource (tasks, `depends_on` chain, `git_source`, cluster
binding) as version-controlled YAML. `databricks bundle deploy` reconciles the workspace to it;
`databricks bundle destroy` removes it. The imperative `w.jobs.create` script is retired — DAB
becomes the **sole** owner of Job `banking-lakehouse-berka-salesforce-bronze-silver` (see D8.5
retirement note; two owners = drift = the anti-pattern this ADR closes).

### D8.2 — CI stage (extend `ci.yml`, do not replace): gates **+ unit tests**, $0, no secrets.
Add a `tests` job to the existing workflow running `python -m unittest discover -s tests -v`
(currently `tests/test_birth_number_decode.py`, pure-Python, no Spark/cloud). Gates job unchanged.
Both run on `pull_request` + `push:main`. No cloud, no secrets, no metered compute.

### D8.3 — CD stage (new `.github/workflows/cd.yml`): **manual-dispatch only**, deploy is free, run is gated.
- **Trigger:** `workflow_dispatch` ONLY — never `push`, never `schedule`. A `schedule:` cron here
  would be an in-repo scheduler, which D-10 forbids and which the coming Airflow owns (see D8.6).
- **Inputs:** `action` = `deploy` (default) | `deploy-and-run`; `job_key`; `target` (default `dev`).
- **Secret scoping:** runs against a GitHub **Environment** named `databricks` holding
  `DATABRICKS_HOST` + `DATABRICKS_TOKEN`, with the owner as a required reviewer — so every CD run
  waits on explicit human approval, and the secrets never exist at plain repo scope.
- **Steps:** checkout → install Databricks CLI → `databricks bundle validate` →
  `databricks bundle deploy -t <target>` → (only if `action == deploy-and-run`)
  `databricks bundle run <job_key> -t <target>`.

### D8.4 — Cost-gated trigger policy (finops-designed-for, ceiling deferred to `@finops`).
The metered/free split is architectural, not incidental:
- **Free (control plane):** gates, unit tests, `bundle validate`, `bundle deploy` (uploads the Job
  spec; spins NO cluster). Safe to run often.
- **Metered (data plane):** `bundle run` (spins the `SINGLE_USER` cluster, burns credit) is
  reachable ONLY via a manual `deploy-and-run` dispatch behind the `databricks` Environment's
  owner-approval gate. There is no code path that auto-runs the Databricks job on a commit.
- The actual credit ceiling / budget alarm is `@finops`'s call — this design makes it *gateable*
  (single choke point: the `deploy-and-run` input + Environment approval) without pre-deciding it.

### D8.5 — The one owner-action, and the no-token-in-repo invariant.
A GitHub Actions CD workflow that talks to Databricks needs `DATABRICKS_HOST` + `DATABRICKS_TOKEN`
as GitHub secrets. **The agent cannot and must not set GitHub secrets or push a token.** The single
owner-action to activate CD is: *create the `databricks` Environment and add those two secrets to
it.* Nothing in this design places a secret in the repo — `databricks.yml` references the host via
`${workspace.host}`/CLI auth, never a literal; `secrets_scan.py` stays green. Until the owner does
this, CI is fully live and CD is inert-but-valid (`bundle validate` still lints locally).

### D8.6 — Relationship to D-10 external-orchestration contract + coming Airflow.
DAB defines the Job resource but the bundle carries **NO `schedule`/`trigger` block** — the Job is
deploy-only, triggered by human dispatch (interim) or by the external orchestrator later. This
preserves D-10 exactly: no private scheduler lives in this repo; the DAB Job *is* the control-plane
contract surface that `airflow_dag_running_pipeline` (D-10, `../control_plane_lab/`) will drive via
`run_now` as pipeline #6. GitHub Actions CD is a **deploy tool, not a scheduler** — the distinction
D-10 rests on. When Airflow arrives it owns cadence; CD keeps owning "get the Job spec into the
workspace." No conflict, no ADR amendment needed then.

## Alternatives considered (and rejected — with reason)
| Alternative | Why rejected |
|---|---|
| **Terraform** (`databricks` provider) | The cross-platform IaC answer, and in-demand — but this repo deliberately keeps cloud infra (IAM role, S3 bucket, cluster creation) as owner-console/one-off actions (ADR-002 Add #2–#5), so Terraform's home turf (provisioning cloud primitives) is out of scope here. What remains to codify is *Databricks Jobs*, which is DAB's exact purpose. Terraform adds a stateful `.tfstate` artifact that must be stored/secured somewhere and that rots when the disposable trial (D-14) is deleted — more fragility for a portfolio repo, less Databricks-native signal. |
| **Keep the imperative `w.jobs.create` SDK script** | It works once, but it is click-ops-in-Python: no declarative source of truth, silent drift, not reproducible after workspace deletion (D-14), no diff/validate. This is the anti-pattern being closed. |
| **CD auto-runs the Databricks job on every push to main** | Burns metered cluster credit on every commit; a false-signal spend. Rejected in favor of free-deploy / gated-run (D8.4). |
| **GitHub Actions `schedule:` cron to run the pipeline** | Turns CI/CD into a scheduler, duplicating the planned Airflow and violating D-10 (no in-repo scheduler). Rejected; scheduling is Airflow's lane (D8.6). |
| **DAB uploads the pipeline source (drop `git_source`, use workspace file sync)** | Would make `bundle deploy` ship `pipeline/**` code to the workspace — re-introducing the exact code-shipping shape the harness classifier blocks (ADR-002 Add #6 BANNED clause). Keeping `source: GIT` means Databricks still pulls code from the public git remote itself; the bundle uploads only the *Job spec*, never the transform code. Preserved deliberately. |

## Anti-pattern check (highest-value section)
1. **Imperative-infra / click-ops-in-Python** (the status quo Job `778449103358221`): fails because
   there is no reviewable, reproducible, drift-detectable source of truth. Correct pattern:
   declarative `databricks.yml` in git, `bundle deploy` reconciles.
2. **CI-as-scheduler** (`schedule:` cron running the data pipeline): fails D-10 and pre-empts the
   external Airflow. Correct pattern: `workflow_dispatch` only; Airflow owns cadence.
3. **Deploy-and-run-on-every-commit**: fails finops (unbounded metered spend). Correct pattern:
   free control-plane deploy auto/on-dispatch; metered run behind an explicit input + Environment
   approval.
4. **Token-in-repo**: fails ADR-001/secrets discipline. Correct pattern: GitHub Environment secrets,
   never a literal in `databricks.yml` or a committed file.

## Consequences
- **New tool dependency:** the Databricks CLI (bundle-capable) in CI and locally. Justified: it is
  the Databricks-native, GA IaC standard and a non-repeated portfolio skill (pairs with the
  existing "portable PySpark on Databricks" story rather than re-demonstrating generic IaC).
- **Portability (D-14) preserved and improved:** `pipeline/**` stays pure PySpark; DAB is additive
  YAML. Workspace deletion → `bundle destroy` (or abandon); a fresh trial is reconstituted by one
  `bundle deploy`. The disposable-cluster id is a **bundle variable** (not hardcoded in the Job
  resource), so recreating the cluster is a variable override, not a resource edit.
- **Reversibility / blast-radius: HIGH reversibility, LOW blast-radius.** The entire footprint is
  three additive files (`databricks.yml`, `.github/workflows/cd.yml`, and a `tests` job appended to
  `ci.yml`). No S3 object, no Delta table, no model, no grain, no storage path moves. Undo =
  `bundle destroy` + `git revert`. Nothing in the data plane is touched.
- **Does NOT decide:** the finops credit ceiling for `deploy-and-run` (routed to `@finops`); the
  Airflow adoption itself (D-10, separate repo); whether CD later also runs the *full* 27-stage
  orchestration vs the proven 2-stage Job (that is a build-scope question once a canonical multi-
  source ingest exists — routed to `@scope-guardian`/`@finops`, not decided here).

## Routing (named, not silently assumed)
- **`@scope-guardian`:** CI/CD is infra/tooling, NOT a new mart, BQ, source, or data-scope change;
  it does not touch the 10-BQ v1 scope, the model, or storage paths, and it was owner-directed and
  pre-authorized by ADR-002 Add #6. Ruling: **not scope creep** — informational notice only (new
  tool dependency: Databricks CLI). No hard-veto trigger.
- **`@finops`:** owns the credit ceiling / budget alarm for `deploy-and-run` metered runs (D8.4).
  This ADR makes that gateable at one choke point; it does not set the number.
- **`@senior-data-engineer`:** owns the build (D8.1–D8.5 files), per the build-list handed off.

## Addendum log
(none yet)
