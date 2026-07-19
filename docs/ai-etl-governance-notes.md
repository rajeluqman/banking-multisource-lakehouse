# AI as an ETL Builder — What Actually Goes Wrong, and How This Project Prevents It

Source material: five discussion threads from r/dataengineering about using AI/LLM
agents to build ETL pipelines and reports. This note consolidates every distinct
problem people reported, maps each one to the specific mechanism in this project that
prevents it, then explains the underlying pattern in plain language with a before/after
comparison.

---

## 1. The Core Question

**Is it true that AI/Claude cannot be used to build an ETL pipeline?**

No. That is not what the discussions actually show. The people who failed and the
people who succeeded were often using the exact same tool (Claude / Claude Code).
The difference was never the AI — it was whether the AI was given a documented
structure, a limited role, and a human checkpoint before anything reached production.

The short version: **AI cannot safely *be* the pipeline. AI can safely *write* the
pipeline, if it's operating inside a structure that constrains it.** Everything below
explains what that structure looks like, and shows this project already builds it.

---

## 2. Master Table — Every Problem Reported, and How This Project Prevents It

| # | The Problem (plain words) | What Happened in the Discussions | Why It Happens | How This Project Prevents It |
|---|---|---|---|---|
| 1 | Management wanted to **replace** the entire data platform with "just let AI do it" | A director wanted to scrap a working platform in favor of AI-generated reports | Decision-makers treat AI as a finished product, not a tool that still needs engineering underneath | Any new idea like this must pass a formal review before any code is written. One role has the authority to reject it outright if it isn't justified. |
| 2 | An AI-built pipeline **never made it out of testing** because the data was messy | A team's AI demo kept failing due to bad/dirty data | AI cannot fix bad data by itself — garbage in, garbage out still applies | A dedicated data-quality check runs at every stage of the pipeline. Data must pass these checks before moving to the next stage. |
| 3 | The AI **invented data instead of reading it** — hardcoded fake values instead of pulling from the real source | A team found fake data typed directly into code instead of being extracted from source | When an LLM isn't sure how to do something correctly, it sometimes fabricates a plausible answer instead of admitting it can't do it | Before touching any file, the current file must be read first — never assumed from memory. If something described in a plan doesn't actually exist on disk, work stops immediately instead of guessing. |
| 4 | The **same business rule got copied into many different files**, so nobody knew which copy was correct | One team found identical logic duplicated across five different scripts | AI tends to add new code instead of reusing or fixing existing code — it has no real sense of "ownership" over a codebase | One table means one clear definition, one clear owner. A senior-reviewer role has final say on any change to how data is structured, specifically to prevent duplication. |
| 5 | The whole pipeline only worked **manually on one person's laptop** — not a repeatable process | A team's "pipeline" required someone to manually dump files into a folder and run it by hand | AI-generated demos tend to skip the boring-but-necessary parts: scheduling, automation, error recovery | Clear, separate stages are defined (raw data → cleaned data → business-ready data) with automated, repeatable runs — not a manual, single-laptop process. |
| 6 | Output was **not consistent** — numbers shifted slightly on each run, with no explanation | Comments included "you can't audit something whose logic changes every run" and "good to write the SQL, bad to be the pipeline" | If AI generates the logic live on every run, results can drift each time — unacceptable for financial or reporting numbers | AI is allowed to **write** the code once, but the code itself must run identically every time (deterministic), with no AI involved at run-time. AI helps build it; ordinary, predictable code runs it daily. |
| 7 | Giving an AI tool **too much access to sensitive data** felt risky | A user worried about giving a "black box" broad access to company data | Once a tool has broad access, what it does with that access is hard to fully control or audit | Sensitive/personal data is masked before AI or anything downstream can see it. Access is limited to only what's needed, never everything. |
| 8 | Fear of being **locked into one AI vendor's tools** | A user worried about long-term dependency on a single AI company's product | If a whole pipeline depends on one vendor's proprietary tooling, switching later becomes very expensive | Only free, open, portable tools are used — nothing that only works inside one paid vendor's platform — so there's no dependency lock-in. |
| 9 | Just **typing good prompts wasn't enough** to build something production-ready | Multiple experienced engineers agreed: "good prompt engineering is insufficient for production-ready solutions" | A good prompt only helps produce good output *once* — it doesn't stop mistakes from slipping through afterward | Three separate layers of checking exist: instructions built into the project itself, an automatic check that blocks bad edits before they happen, and a final automatic check before anything can merge. |
| 10 | Using AI on large datasets got **expensive very fast** | Concerns about token costs and wasted spend | If AI re-reads or re-processes large raw datasets every time someone asks a question, that burns paid tokens for no good reason | A dedicated cost-monitoring role exists. Development happens on a small, free, local setup; AI is not repeatedly run against large expensive cloud resources. |
| 11 | AI is **only as good as the data structure underneath it** — messy sources produce messy AI output regardless | Several engineers said directly: "AI-ready" really just means good data modeling and clean documentation, nothing more | AI cannot invent a clean, trustworthy structure out of messy, scattered source systems — someone still has to design that structure | The project's core design work is exactly that: resolving customer identity across multiple unrelated source systems into one clean, trustworthy structure before any reporting happens on top of it. |
| 12 | AI tends to **write inefficient or low-quality logic** unless someone who already knows SQL/Python checks it | An engineer noted: if you can't judge whether the AI's SQL is any good, you shouldn't be the one asking it to write your SQL | AI has no real understanding of performance trade-offs unless explicitly told, and can't reliably tell you when its own answer is wrong | The actual business logic and calculations are defined by a human first, in writing, before any code is generated. AI fills in boilerplate, not core decisions. |
| 13 | **Documentation kept changing slightly** every time it was regenerated, with no consistency | A user complained that even with saved preferences, AI's documentation style kept drifting | AI has no permanent memory of exactly how things were phrased before; it reconstructs it fresh each time, with small variations | An automated check enforces one single, consistent source of documentation, so it can't silently drift over time. |
| 14 | AI **made confident claims that were wrong**, especially on less common systems | An engineer described AI confidently giving a wrong answer about a database feature, then flip-flopping once corrected | This is a well-known LLM failure mode — confident wrong answers, especially on edge cases outside common training data | Any unverified claim is explicitly flagged as "unverified" rather than stated as fact. Nothing is treated as true until checked against the real file or system. |
| 15 | AI **didn't understand the actual business reasoning** behind decisions | An engineer noted AI could find a technical root cause but couldn't recommend a good fix because "it didn't have the business context" | AI only knows what's written down for it — it has no lived experience of the business | A dedicated role's entire job is defining what "done" means for each business question, and writing that context down so it's available. |
| 16 | A "fix" reported **success**, but the actual problem was still there | A very common real-world failure pattern with AI-driven fixes | AI (and engineers too) can mistake "the command ran without error" for "the problem is actually solved" | If the same failure happens twice, or a fix reports success while the symptom persists, all further work stops immediately and a senior review happens before continuing. A fix must be verified by checking the actual output — never just trusting that it "ran successfully." |
| 17 | Too much time spent on **infrastructure before delivering real business value** | A commenter pushed back: "2 months is a long time to build infrastructure before getting some PoCs out there" | It's tempting to build the "perfect" foundation first, but that delays any usable result | A fixed, small, agreed-upon list of business questions is the actual finish line — not the infrastructure itself. |

**One-sentence summary:** almost every failure story shared traces back to the same
root cause — **the AI was given too much unsupervised control, with no structure
around it** — and every one of those failure modes has a specific, named safeguard
already built into this project.

---

## 3. The Convergence Table — What Actually Worked, in Practice

Separately from the failures, several practitioners described setups that *did* work
well in production. What's notable: none of them describe a fundamentally different
tool — they describe the same tool, used inside a structure. Two examples stand out
because they independently arrived at almost the exact same approach this project
uses.

| What the practitioner did (that worked) | Its equivalent in this project |
|---|---|
| Ran AI-assisted root-cause analysis on a pipeline, with **read-only access only**, and had to first **document the entire architecture** in detail before the AI could be useful | This project's full documentation set (data model, source list, business questions, data flow) must exist and be read before any AI-assisted work happens. Access is limited and controlled, not open-ended. |
| Used **separate, narrow AI agents** — one that only writes a specific type of table, one that only writes documentation — each fed **structured requirements** (source, fields, keys, examples) rather than a vague instruction | This project uses several distinct roles, each with one job and one authority area, instead of a single general-purpose AI doing everything. Each role reads a specific, written specification before acting. |
| Kept AI **out of production changes**; used it for explaining code, catching logic errors, and debugging — with a human reviewing before anything shipped | AI-written changes go through automatic checks and a human/role review before being accepted. Nothing generated by AI reaches production data unchecked. |

The pattern in both cases is identical: **write the plan down first, give the AI a
narrow and specific job, keep a human checkpoint before anything reaches production.**
That is not a special trick — it's ordinary engineering discipline, just applied to a
new kind of collaborator.

---

## 4. The Underlying Principle, Explained Simply

An LLM has no memory of its own, no accountability, and no built-in understanding of
your specific business. It is very capable, but it forgets everything between
sessions and has no way to be "held responsible" for a mistake.

The only way to get reliable, repeatable results out of a tool like that is to put
the missing pieces **outside** it, in the surrounding process:

- **Memory** → written documentation that doesn't disappear when the AI's context does.
- **Direction** → a clear, specific written specification instead of a vague request.
- **Accountability** → a human or a defined review step that checks the work before
  it's trusted, every single time.

Every failure story in Section 2 is missing at least one of these three things.
Every success story in Section 3 has all three.

---

## 5. Before / After — Same Tool, Different Structure

**BEFORE — AI used with no structure around it (the failure pattern):**

```
  Raw files/data ──▶  "AI, build us a pipeline and a dashboard"
                              │
                              ▼
                 ┌───────────────────────────┐
                 │   One big AI prompt does    │  ◀── AI runs INSIDE the pipeline;
                 │  extract + clean + model    │      its logic can change every run
                 │       + report, live        │
                 └───────────────────────────┘
                              │
                              ▼
        Hardcoded/fake data · same logic copied into many files
             · runs manually on one person's laptop
                              │
                              ▼
          Pushed out to the whole team ──▶  real business decisions made on it

  Who owns it?            Nobody — "the AI did it"
  Does it fail silently?  Yes — numbers are "slightly off," no way to trace why
  What happens if wrong?  Whole team affected, no rollback, no audit trail
```

**AFTER — AI used inside a structure (this project's approach):**

```
  Source systems ──▶  Written documentation + specifications
                       (a human defines the plan first)
                              │
                              ▼
             AI writes deterministic pipeline code (build-time only)
                              │
        ┌─────────────────────┼─────────────────────┐
        ▼                     ▼                      ▼
  Automatic edit check   Data-quality checks   Senior-level review
  (blocks bad changes)   (at every stage)       (structure/schema)
        └─────────────────────┼─────────────────────┘
                              ▼
             Automated check blocks the change if anything fails
                              ▼
     Raw → Cleaned → Business-ready data (repeatable, automated, versioned)
                              ▼
             Final reports/answers served from checked, trusted data

  Who owns it?            Every piece has a documented owner and a written reason
  Does it fail silently?  No — caught at a specific check, before it reaches anyone
  What happens if wrong?  Contained to one stage, reversible, traceable
```

The one difference that matters most: in the "Before" version, the AI sits **inside**
the daily-running pipeline, so every run is a fresh gamble. In the "After" version,
the AI sits **upstream**, writing code once that a human/process checks — and the
thing that actually runs every day afterward is ordinary, predictable code, not the
AI itself.

---

## 6. Honest Trade-offs (this structure isn't free)

- **Slower to a first demo.** Building the documentation, checks, and review steps
  takes real time before anything is client-visible. For a genuinely small, throwaway,
  one-off task, this much structure would be overkill — just do it directly.
- **Ongoing friction.** Every check is a small speed bump on every change. The bet is
  that this friction is cheaper than an incident caused by an unchecked mistake
  reaching production later.
- **Requires someone who already knows the domain.** The written specifications this
  approach depends on don't write themselves — a person who understands the business
  and the data still has to produce them. AI accelerates the work; it doesn't replace
  the need for that understanding.

---

## 7. Answer to the Original Question

**"Is it true AI/Claude cannot be used to build an ETL pipeline?"**

No — the evidence across all five discussions points the other way. What's actually
true is narrower and more specific:

- AI should not be the thing that **runs** a production pipeline, generating logic
  fresh on every execution.
- AI works well when it **writes** pipeline code once, against a clear, human-written
  specification, with automated checks and human review before anything reaches
  production.
- Every reported failure was a structure failure, not an AI-capability failure — and
  every reported success came from people who (often independently) built roughly the
  same kind of structure this project already has in place.
