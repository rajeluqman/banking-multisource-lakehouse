# ADR-009 — Two-strike incident protocol: root-cause before re-run, on real evidence of a 6-attempt fix-fail loop

**Status:** Accepted (owner-commissioned 2026-07-17, same session as the incident)
**Date:** 2026-07-17
**Owners:** owner (ratified), @staff-data-engineer (executes the protocol)
**Context refs:** BUILD_REPORT.md §24; PROJECT_STATUS.md ninth-session entry; Databricks job run
`127330185225331` (6 attempts: 1 full run + 5 repairs); PRs #5–#8.

## Context
On 2026-07-17 the first real Gold-layer run entered a fix-fail loop: **six billed cluster
attempts** to reach `23/23 SUCCESS`, where each fix was individually correct but found
reactively, one paid run at a time. The loop had a repeating anatomy:

1. Every fix was "verified" green locally — `py_compile`, 7/7 unit tests, all 4 gates — on every
   one of the six failing attempts. None of those checks exercise the real execution environment
   (Databricks `git_source` `exec()` semantics), real data shapes (JDBC type mapping,
   `"yes"/"no"` strings), or persisted table state (Delta stored schemas). **Green local checks
   were treated as evidence the fix would work; they are not.**
2. Three of the six bugs were not code bugs at all. They were **state bugs** (Delta tables
   written by earlier partial attempts held poisoned schemas — MERGE, append, and plain
   `mode("overwrite")` all enforce the stored schema rather than retyping) and **environment
   bugs** (`__file__` undefined, CWD ≠ repo root, Java 25 vs Spark). Code-only fixes can never
   clear a state bug — which is what made the loop self-sustaining: fix merged → task re-ran →
   reported SUCCESS → symptom persisted.
3. The loop only ended when the method changed, not when a smarter fix landed: terminate the
   cluster first, classify code-vs-state-vs-environment, verify the previous fix actually took
   effect by reading the artifact (`_delta_log` schemaString via boto3) instead of trusting run
   status, enumerate the entire class of the bug across all 16 Gold tables in one audit, and
   reproduce the exact failure with free local Spark before the single final paid run.
4. One relapse inside the same incident proves the discipline must be mechanical, not
   remembered: the full-table audit had already flagged `mart_cross_sell.subscribed_term_deposit
   = string`, but it was skipped on the unverified assumption that overwrite-mode self-heals.
   It does not (`overwrite` keeps the stored schema without `overwriteSchema`), and that skip
   cost attempt #7.

## Decision
1. **Two-strike trigger (mechanical, no judgment):** when (a) the same pipeline stage/task fails
   twice, or (b) a fix was applied and the run reports SUCCESS yet the symptom persists, the
   main session MUST invoke `@staff-data-engineer` as **Incident Commander** BEFORE any further
   paid execution. A first failure is fixed directly by the builder — most bugs are simple, and
   supervising every failure would be pure overhead.
2. **The Incident Commander answers five questions in writing** (full text in
   `.claude/agents/staff-data-engineer.md` §Incident Commander): stop-the-spend; classify
   code/state/environment; verify the last fix took effect at the ARTIFACT level (never run
   status); enumerate the full blast radius of the bug class and act on every audit anomaly or
   record the verified reason to skip; reproduce for free + fix-trade-off analysis
   (fix-at-source vs patch-at-symptom, delete-recreate vs schema-evolve, targeted repair vs full
   re-run). An unanswered question = NO-GO on spend.
3. **Retry budget: exactly ONE paid run per incident analysis**, with its independent
   verification step named in advance. If it fails, that is new information — back to
   classification, never straight to another run.
4. **Diagnostic ladder is binding:** static read (code, logs, `_delta_log` via boto3) → free
   local repro (`JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64` for local Spark in this
   Codespace) → one paid run. Paying to discover what a free probe could tell you is the named
   anti-pattern.
5. **"Verified" means the artifact, not the status.** Run SUCCESS, PR-merged state, green gates,
   and passing unit tests are all necessary-but-insufficient signals. The binding evidence for
   "fixed" is reading the produced artifact independently (stored schema, S3 objects,
   `used_commit`), consistent with the ANTI-SHORTCUT protocol already in CLAUDE.md.

## Alternatives considered (and rejected — with reason)
| Alternative | Why rejected |
|---|---|
| Status quo (builder fixes reactively, N paid runs) | The incident IS the evidence: 6 billed attempts, ~4 of them avoidable. Each fix was locally correct — competence was not the gap; method was. |
| Supervise EVERY failure (zero-strike) | Too heavy. Most failures are trivial (typo, missing import) and a mandatory committee consult per red task would slow the loop that IS working. The expensive pathology is specifically the repeat/`SUCCESS`-but-broken case — trigger on that. |
| Rely on the owner escalating to a stronger model mid-loop | That is vigilance, and this repo's stated rule is "governance is code, not vigilance." Owner escalation remains available and useful, but the protocol must fire mechanically from inside any session, on any model, without the owner watching. |
| Full local-first CI (run whole DAG locally before every deploy) | Right direction, wrong scope for this ADR — that is a build-infrastructure investment (sample datasets, local runner) needing its own @finops/@scope-guardian intake. This ADR fixes the incident-response method now, at zero build cost; a local smoke-DAG can be proposed separately through ADR-000. |

## Consequences
- Second failures now cost one (free) subagent consult before the next paid attempt —
  deliberately trading minutes of analysis for cluster-hours of loop.
- The Incident Commander role is versioned in `.claude/agents/staff-data-engineer.md` (runs on
  Opus), so the discipline applies whichever model drives the main session.
- The loop's reusable environment facts are now doctrine, not tribal memory: Databricks
  `git_source` tasks run under `exec()` with no `__file__` and CWD = script dir; Delta stored
  schemas survive MERGE/append/overwrite; this Codespace needs Java 17 for local Spark.
- This ADR governs incident response only. It does not change any model/schema/grain, add scope,
  or touch the DAB Job — no @scope-guardian or @finops intake required (verified against
  ADR-000's triggers: no new mart/BQ/tool, no metered spend added; it strictly reduces spend).
