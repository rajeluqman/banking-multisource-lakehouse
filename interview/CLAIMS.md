# Claim Ledger

> One entry per resume/interview claim about this project. Enforced by
> `gates/claim_ledger.py`: a claim may only read `DEFEND` when it has a checked evidence box
> AND a checked teach-back box AND every `code:` citation resolves on disk. No prior art for
> this mechanism existed anywhere checked during design (`RESEARCH_nasrul-crosscheck.md`, D-15)
> — this is the framework's own synthesis, not an adopted pattern. Cross-reference
> `defend-or-drop.md` / `integrity-rules.md` if this project sits under the owner's broader
> DE-Mastery interview prep — never claim evidence here that isn't real.

Run `/de-claim` to add, update, or promote an entry. Never hand-edit a `status:` field to
`DEFEND` without actually doing the evidence-gathering and teach-back first — the gate checks
the boxes are checked, not that you're being honest about why; that part is still on you, same
as the receipt system in the learning vault's Definition of Done.

<!--
## CLAIM-001
- **text:** "one-line claim as it would appear on the resume"
- **code:** path/to/file.py:12-30, other/file.py
- **decision:** ADR-005 (or "none")
- **evidence:** [ ] ran on real data   [ ] row count verified   [ ] screenshot
- **test:** path/to/test.py (or "none")
- **explain:** [ ] teach-back passed (no notes open)
- **status:** DROP
-->

---

> **All four entries below read `DROP`, and that is the correct state, not an oversight.** Each
> has real code and real evidence behind it, but **no teach-back has happened yet** — and the gate
> requires evidence AND teach-back before `DEFEND`. They promote to `DEFEND` after a teach-back,
> not before. Sourced from `learning/INCIDENTS.jsonl` INC-0001…0004, all
> `provenance: discovered` — none was an injected drill, so none is subject to
> `integrity-rules.md` Rule 2's drill-language restriction.

## CLAIM-001
- **text:** "Found and fixed a grain violation that every automated test passed: 47,180 NULL keys in a customer-grain fact, invisible to dbt's `unique` test because SQL treats NULLs as distinct."
- **code:** pipeline/gold/fact_repayment_behavior.py, journey/04_DATA_MODEL.md
- **decision:** ADR-005 Add #5
- **evidence:** [x] ran on real data   [x] row count verified   [ ] screenshot
- **test:** none — the guard is asserted in the builder, not a unit test
- **explain:** [ ] teach-back passed (no notes open)
- **status:** DROP

## CLAIM-002
- **text:** "Established that an upsert-only Silver writer cannot retract rows a defective run wrote, and that recovery therefore needs an explicit retraction path rather than a re-run."
- **code:** pipeline/silver/common.py
- **decision:** D-06 soft-delete semantics; R-25 defers hard-delete replay
- **evidence:** [x] ran on real data   [ ] row count verified   [ ] screenshot
- **test:** none — remediated operationally, no code guard exists
- **explain:** [ ] teach-back passed (no notes open)
- **status:** DROP

## CLAIM-003
- **text:** "Made three non-deterministic one-row-per-key selections reproducible across runs, partition counts and cluster sizes, and covered them with regression tests."
- **code:** pipeline/common/ordering.py, pipeline/silver/common.py, pipeline/gold/common.py
- **decision:** ADR-007 D7.1 (shared primitive, not duplicated per file)
- **evidence:** [ ] ran on real data   [ ] row count verified   [ ] screenshot
- **test:** tests/test_cdc_ordering_determinism.py
- **explain:** [ ] teach-back passed (no notes open)
- **status:** DROP

## CLAIM-004
- **text:** "Converted the currency-conversion path from floating point to fixed-point decimal, so monetary totals reconcile exactly instead of depending on Spark partition order."
- **code:** pipeline/common/money.py, pipeline/gold/common.py, pipeline/gold/dim_fx_rate.py
- **decision:** D-12 (single FX resolution path)
- **evidence:** [ ] ran on real data   [ ] row count verified   [ ] screenshot
- **test:** tests/test_money_precision.py
- **explain:** [ ] teach-back passed (no notes open)
- **status:** DROP
