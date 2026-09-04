---
description: Onboard a new data source through the ADR-000 feature intake protocol.
argument-hint: <source name/description>
---

Load `de-governance`. This is `scope-guardian`'s domain — a new source is scope expansion by
definition, not a drive-by addition.

1. Check `governance/BACKLOG.md`'s "in scope" list. If this source is already listed, skip to
   step 3.
2. If not listed: run `governance/ADR/ADR-000-feature-intake-protocol.md` (create it from
   `ADR-TEMPLATE.md` if this is the project's first intake). `scope-guardian` must sign off
   before any code is written for `$ARGUMENTS`.
3. Delegate the ingestion pattern choice to `de-pipeline`: what shape is this source
   (watermark-batch RDBMS, CDC-poll, CRM change-tracking, paginated REST)? That determines the
   extractor design.
4. Update `journey/01_DATASET_AND_SOURCES.md` with the new source's real schema — read the
   actual source, don't transcribe a spec sheet (map vs territory).
5. If the source introduces a new identity that needs resolving against existing customer/
   entity keys, that's a `staff-data-engineer` model decision — escalate, don't decide it here.
6. Run `python gates/journey_completeness.py` and `python gates/doc_reference_contract.py`
   before considering this done.
