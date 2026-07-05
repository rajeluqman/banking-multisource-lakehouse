# ADR-001 (kit) — Security layer is mandatory, not tiered

- **Status:** Accepted
- **Date:** 2026-07-04
- **Deciders:** owner, Sonnet (build)
- **Context refs:** kit `CHANGELOG.md` v1.1.0; creative_intelligence_lab's ADR-014 (the real-project
  case study this kit section generalizes from); `journey/09_SECURITY_AND_ACCESS.md`;
  `gates/secrets_scan.py`.

## Context

v1.0.0 shipped 8 mandatory journey docs and three gates (journey completeness, boundary, doc
reference) with no security-specific doc or gate. That gap surfaced for real in
creative_intelligence_lab (the repo this kit was extracted from): its RBC simulation lab began
running drills that simulate genuine on-call/security incidents (credential leaks, RBAC
misconfiguration), which meant that project needed a real least-privilege role and a real ADR to
have anything to drill against — not a security *program*, one right-sized artifact (see CIL's
ADR-014). The kit generalizes the SHAPE of that need (a mandatory security journey doc + a
deterministic secrets-scanning gate), not CIL's specific Snowflake role.

## Decision

**A — `journey/09_SECURITY_AND_ACCESS.md` joins the mandatory set (8 → 9).** Same
full-set-mandatory discipline as the original 8 (`00_START_HERE.md` "Why full-set-mandatory"):
every section is individually N/A-able with a reason, but the doc itself may not be missing.
`gates/framework.yml` `journey.required_docs` and `journey_completeness.py` need no code change
— the existing sentinel/N/A logic already covers a 9th doc, it only needed adding to the config
list.

**B — `gates/secrets_scan.py` joins the gate set (3 → 4).** Config-driven (`framework.yml`
`secrets_scan:` section) like the other three gates — no project-specific pattern hardcoded in
the `.py` file; project-specific secret shapes go in `extra_patterns`. Detects literal
password/api_key/secret/token assignments, AWS access-key IDs, private-key headers, and
connection strings with embedded passwords. Wired into `ci.yml.template` alongside the other
three gates.

**C — Adopting this ADR is itself mandatory for any repo already on kit v1.0.0** — not
opt-in per project. Consistent with the v1.0.0 "full-set-mandatory, not tiered" philosophy this
kit already committed to: a project that judges itself too small for a security doc writes
`N/A — <reason>` inside it, the same discipline every other journey doc already requires.

## Rejected alternatives

1. **Tiered/optional security doc** (only for projects handling real PII/secrets) — rejected,
   breaks the kit's existing full-set-mandatory precedent and reintroduces the exact
   per-project judgment call that precedent exists to remove.
2. **A heavier security section** (threat model, incident-response runbook, access-review cadence
   as additional mandatory docs) — rejected as over-engineering for what a portable governance
   kit needs; CIL's own ADR-014 rejected the equivalent 9-file version for the same reason (see
   that ADR's "Rejected alternatives").

## Consequences

- **Positive:** any project adopting this kit now gets a forcing function for "who can access
  what" and a real, runnable secret-leak gate, at the same $0/stdlib-only cost as the rest of the
  kit.
- **Negative / accepted:** one more doc to fill honestly at adoption time; mitigated by the
  N/A-able convention already proven across the other 8.
