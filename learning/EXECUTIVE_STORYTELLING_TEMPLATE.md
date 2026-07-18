# 🧠 Executive Storytelling Template — Technical Q&A → Architect-Level Answer

> **Owner:** @cikgu (teaching apparatus). **Status:** teaching aid, not a build artifact.
> **Language:** English (concepts + the industry register are English).
> **Purpose:** train the owner to answer technical interview questions (troubleshooting,
> optimization, config tuning, design) the way a Staff Engineer / Architect answers a CTO —
> outcome-first, system-level, tradeoff-aware — using the **real solutions logged in
> `cheatsheets/troubleshooting/` and `cheatsheets/optimization/`** as the source of truth.

---

## 0. The one rule that makes this fit THIS project — the honesty gate

This converter sits on top of a library with a **no-fabrication contract** (see both cheatsheet
INDEX files: every ✅ card cites a real `file:line`; 🟡 seed cards are explicitly "target, not
work-done"). The executive-storytelling layer **must not break that contract to sound impressive.**

- **Impact is tagged, never invented.** Every impact statement is labelled `[measured]` or
  `[projected]`. `[measured]` is allowed ONLY when the source card is ✅ DONE and carries a real
  `before → after`. If the card is 🟡 APPLICABLE / "target", the answer says **"projected"**. No
  card → the agent says **"no logged card for this yet"**; it does NOT improvise a war story.
- **No invented numbers, no invented incidents.** A polished narrative around a fabricated metric
  is worse than a plain honest answer — it fails the exact "missing-gap / shortcut" test this
  project is built to prevent (ANTI-SHORTCUT PROTOCOL).
- **Senior ≠ embellished.** Seniority comes from system framing + tradeoff honesty (including
  "here's what I did NOT do and why"), not from inflated outcomes.

This gate overrides any "always inject an impact angle" instinct below.

---

## 1. The framework — C-P-I-D-I-R

Internal reasoning skeleton (the model thinks in this order; the user-facing answer is shaped in §3):

| Step | Meaning | Sourced from the cheatsheet card field |
|------|---------|----------------------------------------|
| **C** — Context | the system situation / environment | card layer/phase + project stack (Databricks PySpark / Delta / S3 / Snowflake / DuckDB) |
| **P** — Problem | the symptom, stated in system/business terms | TS `Symptom` · OPT `Why here` (the pain) |
| **I** — Investigation | how the issue was found (observability-first) | TS `Backward trace` · OPT "how the bottleneck was spotted" |
| **D** — Decision | why this solution over alternatives (the thinking layer) | the tradeoff behind `Fix` / `What` |
| **I** — Implementation | the concrete technical action | TS `Fix / guard: file:line` · OPT `Applied at: file:line` |
| **R** — Result | impact on perf / cost / reliability — **honesty-gated (§0)** | TS (incident resolved) · OPT `Measured effect` |

Mnemonic: **C-P-I-D-I-R.**

---

## 2. Input detection — which cheatsheet library to pull from

cikgu separates these two pedagogies (`.claude/agents/cikgu.md` → "Troubleshooting vs
Optimization"). The converter follows the same split:

- **TYPE A — Troubleshooting** ("Silver row count dropped to zero", "the xwalk lost customers",
  "the cluster run succeeded but Gold is empty") → pull from `cheatsheets/troubleshooting/`. Full
  C-P-I-D-I-R, observability-first (Investigation = the backward trace). Verify at the ARTIFACT
  level, never trust run SUCCESS (ADR-009).
- **TYPE B — Optimization** ("reduce the small-file problem", "speed up the MERGE", "cut cluster
  minutes") → pull from `cheatsheets/optimization/`. Lead with baseline → improved state; Result
  carries the `[measured]`/`[projected]` tag (§0).
- **TYPE C — Configuration / tuning** ("why `spark.sql.shuffle.partitions`?", "why `OPTIMIZE` +
  `ZORDER`?", "why a `SINGLE_USER` cluster for S3A secret-scope?") → translate the knob into
  system reasoning: what bottleneck it relieves, what breaks if mis-set, the tradeoff it encodes.
  Never just state the value.

No logged card → say so and (in drill mode) turn it into a hypothesis exercise, not an invention.

---

## 3. User-facing output format (8 beats)

```
1. Executive summary   — 1–2 lines, OUTCOME FIRST. "The issue was X causing Y; resolved by Z."
2. Context             — the system/environment (this stack, this layer).
3. Problem statement   — what broke / what was inefficient, in system terms.
4. Investigation       — how it was diagnosed (the trace / the metric watched).
5. Root cause          — the actual defect or bottleneck, stated plainly.
6. Solution + logic    — what was done AND why this over the alternative (the decision).
7. Tradeoff            — what was sacrificed / deliberately left out (cite the data model / ADR).
8. Impact              — perf / cost / reliability, TAGGED [measured] or [projected] per §0.
```

Optional closer (only if it adds real signal): **"If I were scaling this in production…"** — one
line of next-level thinking (e.g. "at 100× volume the Gold Delta moves to Iceberg with hidden
partitioning; ADR-010 already scopes that forward path, so the transforms don't change").

---

## 4. Translation layer — raw config → system reasoning

The single highest-value move. Convert the knob into the bottleneck it addresses.

| ❌ Junior (states the config) | ✅ Architect (states the system effect) |
|------------------------------|------------------------------------------|
| "Ran `OPTIMIZE` on the Gold table." | "Compacted the small-file explosion from streaming-style appends into right-sized Parquet, so Snowflake external-table scans stop paying per-file open overhead — the tradeoff is the compaction job's own cluster minutes, justified once read frequency > write frequency (ADR-010)." |
| "Used `MERGE` in Silver." | "Made the Silver load idempotent — a re-run upserts on the business key instead of duplicating rows, so a retried batch is safe. The tradeoff vs overwrite is MERGE's join cost, worth it because the source is incremental (ADR-004)." |
| "Built `dim_customer_xwalk` first." | "The four sources share no key, so identity resolution IS the platform — every Gold fact FKs to a resolved `customer_id`. Building the xwalk first means one human isn't counted as four customers in Customer-360 (D-04)." |

---

## 5. Seniority signal — inject ONE angle, honestly

When the card supports it, frame through at least one of: **scalability · cost · reliability ·
system-bottleneck.** Subject to §0 — if the card has no measured basis for the angle, mark it
`[projected]` or drop it. An honest "we haven't run past N source rows yet" is a senior answer; a
fabricated throughput number is not.

---

## 6. Two modes for cikgu (preserves the re-derive principle)

cikgu's contract is **make the user re-derive, don't hand answers**. So this template is a
**drill**, not an answer dispenser:

### Mode 1 — Interview drill (default)
1. cikgu poses a real question tied to today's module / a logged card
   (e.g. *"why star not snowflake?"* → ADR-005; *"the fix succeeded but the symptom persists"* →
   ADR-009 two-strike).
2. **The user answers first.** No framework shown.
3. cikgu scores the answer against the 8 beats + the honesty gate, then **upgrades** it — showing
   where the user gave a junior (config-only) answer vs an architect (system) answer.
4. Score + `learning/LEARNING_LOG.md` entry (hint = −5; a fabricated impact number = flag it, the
   cardinal sin here).

### Mode 2 — Answer-key converter (reference, post-attempt only)
Given a specific logged card, emit the full 8-beat executive answer as an **answer key** — revealed
only AFTER the user has attempted (same flow as DIY "diff vs answer key"). Calibrate, don't skip.

---

## 7. Worked example (illustrative — from the two-strike incident pattern, ADR-009)

**Question (TYPE A):** *"You fixed a pipeline stage, the run came back SUCCESS, but the symptom
is still there. What do you do?"*

**Junior answer:** "Re-run it and see if it works this time."

**Architect answer (8 beats, honesty-gated):**
> **Summary:** A stage failed twice and a fix that reported SUCCESS didn't actually clear the
> symptom, so I stopped all paid execution and invoked the two-strike incident protocol before
> touching the cluster again.
> **Context:** Databricks trial cluster, medallion pipeline; run status is not proof of a correct
> artifact. **Problem:** repeated failure + a "successful" fix with a persisting symptom — the
> classic fix-fail loop. **Investigation:** classify the failure as code / state / environment,
> then verify the *last* fix at the ARTIFACT level (read the Delta output), never trust run
> SUCCESS. **Root cause:** trusting exit status over artifact state — the fix changed code but the
> bad state persisted. **Solution + logic:** @staff-data-engineer as Incident Commander, enumerate
> the bug-class blast radius, reproduce for free on local Spark, THEN exactly one paid run.
> **Tradeoff:** slower than "just re-run" — deliberately, because a 6th blind paid retry is the
> real waste (ADR-009 was born from a real 6-attempt loop, BUILD_REPORT.md §24). **Impact:**
> prevents runaway cluster spend on a loop `[projected]` — the protocol is preventive, not yet a
> measured before→after.

---

## 8. Honesty check-out
Before an interview answer is "done", run the same reconcile: does every `[measured]` tag trace to
a ✅ card with a real before→after? If not, downgrade to `[projected]` or cut it. Submit resume
bullets / drilled answers to **@data-quality-steward** for the no-fabrication check (the cabinet's
honesty owner) before they leave the room.
