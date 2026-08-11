## Selection.md Verdict: REVISE

**Mode:** final  
**Structural role:** inferred h2  
**Stage:** unspecified  
**Independence:** evidence-only — Independence limitation: no author intent was supplied; this review was evidence-only.  
**Filepath:** `docs/source/library-user-guide/dataframe-api/Transformations/selection.md`

## Review boundary

- **Judged:** Entire target page, Rust examples, structure, finish, and repository truth for documented APIs.
- **Not judged:** Other Transformation documentation files.
- **Consistency boundary:** Repository truth; no documentation neighborhood review.
- **Page type applied:** Mixed/action leaf. The packet’s `concept` classification is inconsistent with the target’s method-centered content and path.
- **Final-state requirements not yet applicable:** None.

## Blocking findings

### B1 — MAJOR: Unclosed admonitions break the rendered hierarchy

- **Location:** `docs/source/library-user-guide/dataframe-api/Transformations/selection.md` §“Style Note” (lines 55–68), §“Choosing `.select_columns()` or `.select()`” (lines 92–98), and §“Removing Columns” (lines 432–439)
- **Rule:** `markdown.mdc` §5.2 and §7.4
- **Claim:** These admonitions are bounded callouts followed by ordinary page content.
- **Counter-evidence:** The page contains six admonition openers but only three closing fences. A fresh Sphinx build reports `Non-consecutive header level increase; H1 to H3`.
- **Sources:** Target source; Sphinx dummy build.
- **Impact:** Content, headings, and the conclusion can be absorbed into the wrong callout or rendered with an invalid hierarchy.
- **Direction:** Close each admonition immediately after its intended body, then rerun the parse gate.
- **Verification:** Source inspection — FAIL; Sphinx parse — FAIL.

### B2 — MAJOR: Final-review target still contains draft markers

- **Location:** `docs/source/library-user-guide/dataframe-api/Transformations/selection.md` lines 20–37, 421, and 443
- **Rule:** `markdown.mdc` §5.5; final-review artifact requirements
- **Claim:** The page is ready for final judgment.
- **Counter-evidence:** It retains a “first restructuring draft” checklist, a `TODO:` verification marker, and a provisional-conclusion marker. The SQL `EXCLUDE` claim and doctest registration are already verifiable, making parts of the checklist stale.
- **Sources:** Target source; `datafusion/core/src/lib.rs`; `datafusion/sqllogictest/test_files/select.slt`; SQL wildcard implementation.
- **Impact:** The page’s verification and editorial state remains explicitly unfinished.
- **Direction:** Resolve every checklist item and remove all draft/provisional markers.
- **Verification:** Marker check — FAIL; SQL support and registration — PASS.

## Non-blocking findings

### N1 — MINOR: Schema-inspection reference uses the wrong label

- **Location:** `docs/source/library-user-guide/dataframe-api/Transformations/selection.md` §“Building a Projection from the Schema” line 287
- **Direction:** Use the existing `[schema-inspection]` reference label instead of the undefined `[schema-management/schema-inspection.md]` label.

### N2 — NIT: Stray space before punctuation

- **Location:** `docs/source/library-user-guide/dataframe-api/Transformations/selection.md` line 79
- **Direction:** Remove the space before the period after `` [`DataFrame`] ``.

## Open questions

None.

## Verified strengths

- All five Rust examples compile, execute, and assert their visible output.
- The documented method signatures and lazy-plan behavior match `DataFrame` source.
- `SELECT * EXCLUDE (...)` support is confirmed by implementation and SQL logic tests.
- The projection-pruning claim is appropriately conditional on source capabilities.
- The complete-projection versus targeted-change distinction is technically sound.

## Validation evidence

- **PASS — Rust doctest:** `cargo test --doc -p datafusion dataframe_api_transformations_selection`; 5 passed, 0 failed.
- **PASS — doctest registration:** Target is registered as `dataframe_api_transformations_selection`.
- **PASS — target formatting:** The pinned formatter reported no warning for `selection.md`; the corpus command exited 1 only for five out-of-scope files.
- **FAIL — Sphinx parse:** Fresh dummy build emitted a target-local heading-hierarchy warning.
- **FAIL — markers:** Draft, `TODO:`, and provisional markers remain.
- **NOT RUN — external links:** CI-only link gate was not invoked locally.

## Unverified areas

- Other Transformation pages and external URL reachability were outside the review boundary.

## Recommended next action

- Close the three admonitions, resolve all markers, repair N1, rerun the target gates, and resubmit for final review.

---

## Filtering.md Verdict: ACCEPT

**Mode:** final  
**Structural role:** inferred h2  
**Stage:** unspecified  
**Independence:** evidence-only — Independence limitation: no author intent was supplied; this review was evidence-only.  
**Filepath:** `docs/source/library-user-guide/dataframe-api/Transformations/filtering.md`

## Review boundary

- **Judged:** Entire target, Rust examples, structure, and repository truth for filtering APIs.
- **Not judged:** Other Transformation documentation files.
- **Consistency boundary:** Repository truth.
- **Page type applied:** Mixed/action single-method leaf; the packet’s `concept` classification is unsupported.
- **Final-state requirements not yet applicable:** None.

## Blocking findings

None.

## Non-blocking findings

### N1 — NIT: SQL-equivalence callouts use the cross-reference class

- **Location:** `filtering.md` lines 110–125, 179–195, 294–304, and 393–411
- **Direction:** Prefer `note` for SQL-equivalence comparisons; reserve `seealso` for the final cross-reference.

### N2 — NIT: Rust import order differs from the documentation convention

- **Location:** All five Rust blocks
- **Direction:** Place `use datafusion::prelude::*;` before the assertion-macro import.

## Open questions

None.

## Verified strengths

- Filtering, NULL, comparison, membership, range, pattern, and error-timing claims match source behavior.
- Pushdown semantics for `Unsupported`, `Inexact`, and `Exact` match optimizer implementation and tests.
- All five examples demonstrate bounded input, lazy transformation, execution, and asserted output.
- The no-criteria discussion separates absent and explicitly empty inputs correctly.
- No draft markers or malformed admonitions remain.

## Validation evidence

- **PASS — Rust doctest:** 5 passed, 0 failed.
- **PASS — doctest registration:** Registered as `dataframe_api_transformations_filtering`.
- **PASS — source/API inspection:** Filtering, expression, validation, and pushdown behavior confirmed.
- **PASS — target formatting:** No formatter warning for `filtering.md`; corpus failure was limited to six out-of-scope files.
- **PASS — Sphinx parse:** Fresh dummy build produced no target-local warning or error.
- **PASS — markers:** No TODO or citation markers found.
- **NOT RUN — external links:** CI-only link gate was not invoked locally.

## Unverified areas

- Other Transformation pages and external URL reachability remained outside scope.

## Recommended next action

- Accept the page; N1–N2 may be handled as optional polish.

---

## Sorting-limiting.md Verdict: REVISE

**Mode:** final  
**Structural role:** inferred h2  
**Stage:** unspecified  
**Independence:** evidence-only — Independence limitation: no author intent was supplied; this review was evidence-only.  
**Filepath:** `docs/source/library-user-guide/dataframe-api/Transformations/sorting-limiting.md`

## Review boundary

- **Judged:** Entire target, Rust examples, structure, and repository truth.
- **Not judged:** Other Transformation documentation files.
- **Consistency boundary:** Repository truth.
- **Page type applied:** Action leaf; both content and retained scaffold contradict the packet’s `concept` classification.
- **Final-state requirements not yet applicable:** None.

## Blocking findings

### B1 — MAJOR: Final target retains its workflow scaffold

- **Location:** `sorting-limiting.md` lines 20–50
- **Rule:** `markdown.mdc` §5.5; final-review artifact requirements
- **Claim:** The page is ready for final judgment.
- **Counter-evidence:** A Stage 2 scaffold remains and explicitly lists final cleanup, transition, link-audit, and validation work as unfinished.
- **Sources:** Target source and marker search.
- **Impact:** The page remains explicitly provisional, and completion of its final checks cannot be inferred.
- **Direction:** Resolve the listed work and remove the entire scaffold.
- **Verification:** Marker check — FAIL; format, parse, and doctest gates — PASS.

## Non-blocking findings

### N1 — MINOR: `.sort_by()` example hides its demonstration

- **Location:** `sorting-limiting.md` §“Defining a Sort Order” lines 154–182
- **Direction:** Expose a minimal before state and asserted ordered result; currently only the transformation is visible while setup, execution, and output are hidden.

### N2 — NIT: Visible Rust blocks use nonstandard import order

- **Location:** `sorting-limiting.md` lines 114–116, 228–230, and 280–282
- **Direction:** Place `use datafusion::prelude::*;` before the assertion-macro import.

## Open questions

None.

## Verified strengths

- `.sort()`, `.sort_by()`, `.limit()`, `Expr::sort()`, and `SortExpr` claims match source signatures and behavior.
- `ASC NULLS LAST` is the verified `.sort_by()` policy.
- Limit skip/fetch semantics and optimizer-safe movement are accurate.
- Global Top-N composition, tie-breaking, and method-order consequences are correct.
- Conditional TopK and dynamic-filter wording is supported by current optimizer implementation.

## Validation evidence

- **PASS — Rust doctest:** 4 passed, 0 failed.
- **PASS — doctest registration:** Registered as `dataframe_api_transformations_sorting_limiting`.
- **PASS — source/API inspection:** Sorting, limiting, pushdown, and TopK claims confirmed.
- **PASS — target formatting:** No formatter warning for the target; corpus failure involved six out-of-scope files.
- **PASS — Sphinx parse:** Fresh dummy build produced no target-local warning or error.
- **FAIL — markers:** Stage scaffold remains.
- **NOT RUN — external links:** CI-only link gate was not invoked locally.

## Unverified areas

- Other Transformation pages and external URL reachability remained outside scope.

## Recommended next action

- Resolve and remove the scaffold, expose the `.sort_by()` result, then resubmit the page for final review.

---

## aggregations.md Verdict: ACCEPT

**Mode:** final  
**Structural role:** inferred h2  
**Stage:** unspecified  
**Independence:** evidence-only — Independence limitation: no author intent was supplied; this review was evidence-only.  
**Filepath:** `docs/source/library-user-guide/dataframe-api/Transformations/aggregations.md`

## Review boundary

- **Judged:** Entire target, Rust examples, structure, and repository truth.
- **Not judged:** Other Transformation documentation files.
- **Consistency boundary:** Repository truth.
- **Page type applied:** Mixed/action single-method leaf; the packet’s `concept` classification is unsupported.
- **Final-state requirements not yet applicable:** None.

## Blocking findings

None.

## Non-blocking findings

### N1 — NIT: Divider follows the final section

- **Location:** `aggregations.md` after “Further Reading” around line 572
- **Direction:** Remove the final `---`; dividers belong before H2 sections, not after the last one.

### N2 — NIT: Missing space in the Style Note

- **Location:** `aggregations.md` line 36
- **Direction:** Add a space between “the” and `` `LogicalPlan` ``.

### N3 — NIT: Final Rust block uses inconsistent import order

- **Location:** `aggregations.md` lines 474–477
- **Direction:** Place `use datafusion::prelude::*;` first.

## Open questions

None.

## Verified strengths

- `.aggregate()` grouping, output-grain, receiver, and schema claims match source.
- Empty global aggregates correctly produce one row: `count_all()` returns `0`; other named aggregates return `NULL`.
- Row, non-null-value, and distinct-non-null-value count semantics are accurate.
- HyperLogLog and t-digest descriptions match implementations.
- Aggregate-filter and pre/post-aggregation filtering behavior is correctly distinguished.
- All visible results use suitable assertion oracles.

## Validation evidence

- **PASS — Rust doctest:** 5 passed, 0 failed.
- **PASS — doctest registration:** Registered as `dataframe_api_transformations_aggregations`.
- **PASS — source/API inspection:** Aggregation, count, approximate-function, and filter-placement claims confirmed.
- **PASS — target formatting:** No formatter warning for the target; corpus failure involved six out-of-scope files.
- **PASS — Sphinx parse:** Fresh dummy build produced no target-local warning or error.
- **PASS — markers:** No TODO or citation markers found.
- **NOT RUN — external links:** CI-only link gate was not invoked locally.

## Unverified areas

- Other Transformation pages and external URL reachability remained outside scope.

## Recommended next action

- Accept the page; N1–N3 may be handled as optional polish.

---

## window-functions.md Verdict: ACCEPT WITH MINOR CHANGES

**Mode:** final  
**Structural role:** inferred h2  
**Stage:** unspecified  
**Independence:** evidence-only — Independence limitation: no author intent was supplied; this review was evidence-only.  
**Filepath:** `docs/source/library-user-guide/dataframe-api/Transformations/window-functions.md`

## Review boundary

- **Judged:** Entire target, Rust examples, structure, and repository API truth.
- **Not judged:** Other Transformation documentation files.
- **Consistency boundary:** Repository truth.
- **Page type applied:** Mixed cognitive/action single-method leaf; the packet’s `concept` classification is unsupported by the path and action-oriented body.
- **Final-state requirements not yet applicable:** None.

## Blocking findings

None.

## Non-blocking findings

### N1 — MINOR: Aggregate-window table links to non-window expression helpers

- **Location:** `docs/source/library-user-guide/dataframe-api/Transformations/window-functions.md` §“Choose a Window Calculation” lines 176–183
- **Direction:** Replace the linked `sum()`, `avg()`, and `count()` entries with the actual Rust aggregate-window construction entry points, or present them as unlinked SQL calculation names and direct readers to “Build Aggregate Window Expressions.” Those helpers produce `Expr::AggregateFunction`; they do not themselves create window expressions.

### N2 — NIT: Missing divider before an H2

- **Location:** `docs/source/library-user-guide/dataframe-api/Transformations/window-functions.md` before §“Apply Window Results” line 513
- **Direction:** Add `---` before the H2, consistent with `markdown.mdc` §7.4.

## Open questions

None.

## Verified strengths

- `.window()` is correctly described as a lazy schema-extending transformation.
- Rust-builder frame defaults, ranking behavior, offset defaults, fallback behavior, and `nth_value()` semantics agree with source.
- All five examples visibly assert their claimed output and pass as doctests.
- Tie handling, bounded-frame ordering, missing positions, and filter placement are surfaced as correctness concerns.

## Validation evidence

- **PASS — source/API inspection:** Relevant DataFrame, expression-builder, frame, ranking, and navigation implementations inspected.
- **FAIL — corpus format command:** `ci/scripts/doc_prettier_check.sh` exited 1 for six out-of-scope files; the target was not reported and has no target-local formatting failure.
- **PASS — target parse:** Fresh dummy Sphinx build exited 0 with no target-local warning or error.
- **PASS — Rust doctests:** 5 passed; 0 failed.
- **NOT RUN — external links:** required `lychee` tool is unavailable without installation.
- **PASS — markers:** No `TODO:` or `citation-needed` markers.

## Unverified areas

- External URL reachability was not checked.

## Recommended next action

- Resolve N1 and N2 in one bounded polish pass, rerun the target gates, and resubmit for final review.

---

## transformation-concepts.md Verdict: REVISE → RESOLVED (Author 2026-08-07)

**Mode:** final  
**Structural role:** inferred h2  
**Stage:** unspecified  
**Independence:** evidence-only — Independence limitation: no author intent was supplied; this review was evidence-only.  
**Filepath:** `docs/source/library-user-guide/dataframe-api/Transformations/transformation-concepts.md`

**Disposition:** B1 and N1–N3 addressed on the leaf; sibling `joins.md` reading-path / `.alias()` links also retargeted to `joins/index.md` and `joins/join-workflows.md`. No re-judge required unless Author requests one. Historical finding text kept below for the record.

## Review boundary

- **Judged:** Entire target, concept-page structure, Rust example, and repository API truth.
- **Not judged:** Other Transformation documentation files.
- **Consistency boundary:** Repository truth.
- **Page type applied:** Concept page.
- **Final-state requirements not yet applicable:** None.

## Blocking findings

### ~~B1 — MAJOR: The universal lazy-transformation contract omits `.cache()`~~ **RESOLVED**

- **Location:** `docs/source/library-user-guide/dataframe-api/Transformations/transformation-concepts.md` title/abstract and §§“The Transformation Contract” and “Laziness and the Point of Execution” lines 20–107
- **Rule:** `markdown.mdc` §4 (lazy versus eager); concept pages must own an accurate mental model.
- **Claim:** Every transformation returns a new `DataFrame`, “that shape never varies,” and work begins only at `.collect()`, `.show()`, or `.write_*()`.
- **Counter-evidence:** `DataFrame::cache()` is `async`, returns `Result<DataFrame>`, and its default path creates a physical plan and collects all partitions before constructing the returned frame.
- **Sources:** `datafusion/core/src/dataframe/mod.rs` lines 2412–2425.
- **Impact:** Returning a `DataFrame` is presented as sufficient evidence of laziness, causing readers to misplace execution cost and error handling around `.cache()`.
- **Direction:** Scope the contract explicitly to ordinary lazy transformations and identify `.cache()` as an eager exception/action-like boundary despite its `DataFrame` return type.
- **Verification:** Source inspected — PASS; doctest — PASS but does not exercise `.cache()`.
- **Resolution:** Ordinary lazy contract scoped; `.cache()` named as eager exception in highlight, abstract, contract, laziness section, and conclusion.

## Non-blocking findings

### ~~N1 — MINOR: Semi/anti join explanation assumes left-sided variants~~ **RESOLVED**

- **Location:** `docs/source/library-user-guide/dataframe-api/Transformations/transformation-concepts.md` §“Joins in Brief” lines 178–202
- **Direction:** Describe semi/anti joins as returning only the preserved side. Right-semi and right-anti joins return right-side columns and use the left input for matching.
- **Resolution:** Preserved-side wording covers left and right semi/anti.

### ~~N2 — MINOR: Join references target a nonexistent page~~ **RESOLVED**

- **Location:** `docs/source/library-user-guide/dataframe-api/Transformations/transformation-concepts.md` lines 174, 202, 319 and `[joins]` definition around line 363
- **Direction:** Change `joins.md` to the existing `joins/index.md`; Sphinx reports three unresolved references.
- **Resolution:** Leaf footnote → `joins/index.md`; also `Transformations/index.md` reading path and `dataframe-specifics.md` `.alias()` row retargeted. Toctree uses `joins/index` (folder index — there is no shipping `joins.md`).

### ~~N3 — NIT: Page title uses an awkward plural modifier~~ **RESOLVED**

- **Location:** `docs/source/library-user-guide/dataframe-api/Transformations/transformation-concepts.md` title line 20 and contents title line 51
- **Direction:** Use “Transformation Concepts” consistently.
- **Resolution:** Title and contents label use “Transformation Concepts”.

## Open questions

None.

## Verified strengths

- The page owns a coherent conceptual model rather than acting as a second index.
- The five-dimension lens explicitly identifies value-only and execution-only changes outside its scope.
- Aggregation versus window grain and joins versus set-operation orientation are otherwise supported by source.
- The runnable pipeline demonstrates all five dimensions and asserts its output.

## Validation evidence (at original review)

- **FAIL — source/API truth:** `.cache()` and right-sided semi/anti joins contradict B1 and N1. → **superseded by RESOLVED above**
- **FAIL — target parse:** Three unresolved `joins.md` references. → **superseded by RESOLVED above**
- **FAIL — corpus format command:** Six out-of-scope files failed; the target was not reported.
- **PASS — Rust doctest:** 1 passed; 0 failed.
- **NOT RUN — external links:** required `lychee` tool is unavailable without installation.
- **PASS — markers:** No `TODO:` or `citation-needed` markers.

## Unverified areas

- External URL reachability was not checked.

## Recommended next action

- ~~Resolve B1 and N1–N3, rerun the target gates, and resubmit for final review.~~ **Done for findings.** Optional: re-judge only if Author wants a fresh ACCEPT; otherwise resume `join-conditions` Draft/Polish.

---

## joins/join-concepts.md Verdict: REVISE

**Mode:** final  
**Structural role:** inferred — whole-page H1 target; no heading-subtree role applies  
**Stage:** Polish (post close-out)  
**Independence:** limited — Independence limitation: author intent was visible during the evidence pass; this review was adversarial but not blind.  
**Filepath:** docs/source/library-user-guide/dataframe-api/Transformations/joins/join-concepts.md

## Review boundary

- **Judged:** Current whole-page technical accuracy, concept-page structure, reader path, ownership, finish, and final-mode validation.
- **Not judged:** Named neighbors’ quality beyond ownership; physical-operator details; unnamed documentation.
- **Named neighbors:** All four exist and were inspected only for their stated ownership boundaries.
- **Consistency boundary:** Repository truth — whole page, named neighborhood, and targeted source/API/tests.
- **Final-state requirements not yet applicable:** Formal `page_final` transition and Author approval remain outside this judgment; no bounded quality requirement was deferred.

## Blocking findings

### B1 — MAJOR: A separate core concept is buried under the first H2

- **Location:** `join-concepts.md` §“Fusing DataFrames: What a Join Decides” / §“How Joins Extend the Logical Plan” (lines 65–199)
- **Rule:** `markdown.mdc` §7.4 limits the first H2 to a short topic on-ramp without deep mechanism; `markdown-landing.mdc` §2.1 gives each concept H2 one concept.
- **Claim:** The hierarchy presents logical-plan representation as a facet of “what a join decides.”
- **Counter-evidence:** The orientation table identifies logical-plan representation as a separate concept. Its H3 carries a field table, two API shapes, optimizer behavior, and the logical/physical boundary. The approved storyline likewise treats plan extension as the next step after the four result questions. The acknowledged altitude trade-off supplies no clarity benefit that outweighs these signals.
- **Sources:** Target lines 34–39 and 65–199; `markdown.mdc` §§7.4–7.5; `markdown-landing.mdc` §2.1; supplied intent.
- **Impact:** The first-H2 on-ramp becomes a long mechanism section, while local navigation understates one of the page’s core concepts. This breaks the intended conceptual hierarchy for jump-in readers.
- **Direction:** Promote “How Joins Extend the Logical Plan” to a peer H2 while preserving its bounded content and storyline position.
- **Verification:** Rule and target inspection — PASS; current Sphinx parse — PASS but does not test conceptual altitude; differential parse — NOT RUN; Rust doctest — NOT APPLICABLE.

## Non-blocking findings

None.

## Open questions

### Q1 — QUESTION: Whole-page structural role lacks a canonical token

- **Location:** `join-concepts.md` §“Join Concepts” (whole page)
- **Resolves it:** Coordinator clarification whether whole-page final packets should omit `structural_role` or the protocol should support `page`. This does not affect the review scope or verdict.

## Verified strengths

- The text independently reconstructs the approved arc: matching/preservation/cardinality/payload → lazy plan node → schema → rows → binary composition → operation boundaries.
- Source confirms `.join()` and `.join_on()` create lazy `Join` plans; `.join_on()` AND-combines expressions into `filter`, and equality extraction is optimizer-owned.
- Source confirms join schema ordering, outer-join nullability, one-sided payloads, non-null mark fields, and conditionless inner joins as Cartesian joins.
- Condition construction, variant catalogue, workflow recipes, and diagnosis are handed to their named owners without turning this page into a router.
- No target `TODO:`, `JOIN-TODO`, `citation-needed`, `TBD`, `FIXME`, or `PLACEHOLDER` remains.

## Validation evidence

**PASS — source/API verification:** Inspected DataFrame join methods, logical `Join`, schema construction, optimizer behavior, set-operation APIs, and relevant tests.

**PASS — doctest registration inspection:** `datafusion/core/src/lib.rs` registers `join-concepts.md`.

**FAIL — repository format gate:** `ci/scripts/doc_prettier_check.sh` exited 1 for nine unrelated files; `join-concepts.md` was not reported.

**PASS — current target parse:** Fresh dummy Sphinx build exited 0 with no target warning or error.

**NOT RUN — differential parse comparison:** No pre-edit Polish baseline was supplied.

**NOT APPLICABLE — Rust doctest:** The target contains no reader-visible Rust blocks; its API-shape block is deliberately `text`.

**NOT RUN — external link gate:** Companion classifies it as CI-only; it was not requested.

**PASS — marker gate:** The prescribed target search returned empty output.

## Unverified areas

- External URL availability was not checked.
- A before/after differential parse could not be reconstructed retrospectively.
- Repository-wide formatting remains red for unrelated files; no target formatting defect was localized.

## Recommended next action

Resolve B1 by making “How Joins Extend the Logical Plan” a peer H2.
