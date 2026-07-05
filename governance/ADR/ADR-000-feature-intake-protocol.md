# ADR-000 — Feature Intake Protocol (ad-hoc feature control)

**Status:** Template — adopt as-is, fill `banking-multisource-lakehouse` tokens only.
**Owners:** architect (veto), scope-guardian (veto).

## Context
The failure mode this ADR exists to prevent: someone (owner or an AI session) has an idea mid-
project — "let's also add X" — and it gets implemented same-session, without anyone checking
whether X fits the locked scope, whether it needs a schema/model change, or whether it silently
reintroduces something already rejected. This is exactly the gap the ChatGPT critique of
banking-multisource-lakehouse's sibling project (CIL) identified as missing: no forced clarification step before
ad-hoc work begins.

## Decision
Every feature/change request that is NOT already an item in an approved plan (`governance/plans/`)
must pass through this sequence before any code is written:

1. **Propose** — one paragraph: what, why, which business question (cite journey/02) it serves.
2. **Clarify** — the assistant/session MUST produce a list of unresolved questions (ambiguous
   scope, unclear grain impact, unclear stack fit) BEFORE producing a plan. If genuinely zero
   ambiguity, state that explicitly with a one-line reason — do not skip the step silently.
3. **Ruling** — architect and/or scope-guardian answer: fits current model/stack as-is / needs a
   new ADR / rejected (name the reason, add to `governance/BACKLOG.md` if rejected-but-revisit-able).
4. **ADR (if the ruling changes an architectural decision)** — write it before implementation,
   not after. A retroactive ADR describing code that already exists is a smell, not a record.
5. **Plan** — use `governance/plans/PLAN_TEMPLATE.md`; phase it if larger than one context window.
6. **Gate** — run the relevant `gates/*.py` contracts before considering it done. A green gate,
   not a confident claim, is what "done" means.

## Consequences
- Slower for genuinely trivial changes — mitigated by scope-guardian being allowed to rule
  "fits as-is" in step 3 without a full ADR, as long as steps 1–2 happened.
- This protocol itself is governed the same way: changing it requires the same sequence.

## §2 — Minimum non-negotiable roster
Regardless of how far the agent roster is trimmed for a small project (see
`operating/agents/`), the **architect** (or equivalent solo-owner judgment call, if no
multi-agent setup exists) and **scope-guardian** functions must exist in some form — even if
that "form" is a single person/session applying both hats deliberately. What must not happen is
zero checkpoint at all between idea and code.

## Unresolved questions template (paste into every intake)
- Q: ...? → A: ... (owner, date)
- Q: ...? → A: ...
(If none: "No ambiguity found — <one-line reason>.")
