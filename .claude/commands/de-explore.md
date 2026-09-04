---
description: Exploratory data analysis — profile a source, chase a data-source problem, before committing to a design.
argument-hint: <source/table to explore>
---

Read the actual data before proposing anything — this is the "planning docs are a MAP, files
on disk are the TERRITORY" rule applied to data itself, not just documentation.

1. Profile: row count, null rates per column, distinct-value counts on candidate keys, obvious
   type mismatches, date-range sanity. State what you found, not what you expected to find.
2. If this is chasing a reported problem (numbers don't reconcile, a count looks wrong): trace
   it the same way `de-diagnosis` traces an incident — backward from the observed anomaly.
3. Findings that will inform a design decision go into `journey/03_DATA_REQUIREMENTS.md`, cited
   with how you actually derived them (a query, a script, a specific run) — not asserted from
   memory of a similar source elsewhere.
4. If exploration reveals the source doesn't match what a spec sheet or prior doc claimed, stop
   and surface that explicitly rather than quietly building against the doc instead of reality.

This command does not write pipeline code — that's `/de-build`, once the design this exploration
informs has actually been decided.
