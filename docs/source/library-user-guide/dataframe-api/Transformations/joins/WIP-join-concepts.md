<!---
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

<!--
JOIN CONCEPTS CO-AUTHORING RECORD

STATUS (Author-approved 2026-08-04)
Body transferred into join-concepts.md. This file is the co-authoring
record only — not a Sphinx page and not the draft body. Next session: tighten
join-concepts.md with fresh eyes; openings remain provisional until Polish.

CURRENT PURPOSE
Prepare readers to explain what a join represents and reason about the possible
shape and meaning of its result before they encounter action-oriented guidance.

AGREED DECISIONS
- Promote this WIP ownership model into join-concepts.md (Author, 2026-08-04).
- Use a result-first storyline with one running customers-and-orders example.
- Open at the DataFusion/query-engine level; write the abstract last.
- Frame joins as horizontal DataFrame fusion, contrasted with vertical fusion
  through set operations, while preserving the one-sided semi/anti exception.
- Organize the first H2 as an SCQA progression from combining two DataFrames to
  the condition, join type, cardinality, and schema rules that form the result.
- Treat column-wise expansion as a useful intuition rather than a universal rule.
- Keep matching, preservation, cardinality, and payload as the four questions
  that organize reasoning about a join result.
- Place brief API orientation and the logical-plan H3 at the end of the first H2.
- Present the planned result schema before the data-dependent result rows:
  Result Columns owns result shape, while Result Rows owns result population.
- Exclude physical operator catalogs, build/probe, partition modes, and
  performance claims from this page (JOIN-TODO-013 / JOIN-TODO-020).

COMPLETED WORK
- Refined the opening H2 from vertical and horizontal DataFrame fusion through
  the four-question result model, a compact customer/order example, and the
  SQL/DataFrame authoring handoff.
- Expanded the logical-plan H3 to distinguish the lazy, unoptimized binary node
  from physical execution and to explain its equality pairs, optional filter,
  join type, and derived schema.
- Clarified that logical left/right labels govern side-sensitive semantics
  without prescribing physical execution roles.
- Reordered and rewrote the adjacent result subtrees as Result Columns followed
  by Result Rows, with distinct ownership for schema and row behavior.
- Split semi and anti join coverage between one-sided payloads and at-most-once
  row contribution, and reduced cross joins to the bounded all-pairs case.
- Renamed the result H2s and their H3 facets, and revised the preservation
  comparison to describe one dimension without becoming a join-type catalogue.
- Resolved JOIN-TODO-003 and JOIN-TODO-030.
- Moved cross joins after ordinary multiplicity and used literal L × R, closing
  JOIN-TODO-029.
- Transferred the draft body into join-concepts.md (2026-08-04).

OPEN BACKLOG AND QUESTIONS
- JOIN-TODO-001: title-line language, abstract, Concepts Covered table, and
  conclusion remain provisional pending final review.
- JOIN-TODO-013, JOIN-TODO-015, JOIN-TODO-016, JOIN-TODO-022, and JOIN-TODO-026:
  ownership and final-review questions remain open.
- JOIN-TODO-023: confirm logical left/right may derive from the same DataFrame
  is explicit enough after the transfer.
- Stable API and logical-plan claims were checked against the official
  DataFusion 54.1 API/source and current upstream documentation; re-verify
  against the target checkout during the tighten pass.

DEFERRED HANDOFFS
- Detailed behavior for the complete set of join variants, including mark joins,
  remains owned by join-types.md.
- Physical execution belongs outside this conceptual pass (join-validation for
  bounded `.explain()` vocabulary, or a future execution owner).
-->

## join-concepts.md Verdict: REVISE

**Mode:** final  
**Structural role:** inferred h2  
**Stage:** unspecified  
**Independence:** evidence-only — Independence limitation: no author intent was supplied; this review was evidence-only.  
**Filepath:** `docs/source/library-user-guide/dataframe-api/Transformations/joins/join-concepts.md`

## Review boundary

- **Judged:** Entire target, concept-page structure, finish, and repository API truth.
- **Not judged:** Other Transformation files.
- **Consistency boundary:** Repository truth.
- **Page type applied:** Concept page.
- **Final-state requirements not yet applicable:** None.

## Blocking findings

### B1 — MAJOR: Final page retains unresolved workflow scaffolding

- **Location:** `docs/source/library-user-guide/dataframe-api/Transformations/joins/join-concepts.md` lines 20–31, 196, and 399
- **Rule:** `markdown.mdc` §5.5; final review makes all target-file artifacts reviewable.
- **Claim:** Comments declare a draft transfer, a future tightening session, provisional opening/conclusion material, unresolved ownership decisions, and pending doctest registration.
- **Counter-evidence:** The packet requests final review; additionally, the page is already registered in `datafusion/core/src/lib.rs`.
- **Sources:** Target file; `datafusion/core/src/lib.rs` lines 1473–1477.
- **Impact:** The page explicitly disclaims finality and carries stale internal workflow state.
- **Direction:** Resolve or move every `DRAFT TRANSFER`/`JOIN-TODO` item off-page and remove the stale registration TODO.
- **Verification:** Widened marker audit — FAIL; registration inspection — PASS.

## Non-blocking findings

### N1 — MINOR: Logical-plan condition storage is overstated

- **Location:** `docs/source/library-user-guide/dataframe-api/Transformations/joins/join-concepts.md` §“Joins and the Logical Plan” lines 176–180
- **Direction:** Clarify that `.join()` keys populate `on`, while `.join_on()` initially combines its supplied expressions into `filter`; optimization may later extract equality predicates into `on`.

### N2 — MINOR: Unsupported performance ranking

- **Location:** `docs/source/library-user-guide/dataframe-api/Transformations/joins/join-concepts.md` §“Joins: Expanding Columns, Shaping Rows” lines 81–87
- **Direction:** Remove “one of the most computationally demanding transformations”; it is an unqualified performance claim and contradicts the page’s stated exclusion of performance material.

### N3 — MINOR: Target fails the pinned formatter

- **Location:** Entire `docs/source/library-user-guide/dataframe-api/Transformations/joins/join-concepts.md`
- **Direction:** Format the target with the repository’s pinned documentation formatter.

## Open questions

None.

## Verified strengths

- The four-question model—matching, preservation, cardinality, and payload—remains coherent without its navigation links.
- Outer-join nullability, semi/anti cardinality, cross-join multiplication, and binary join-tree claims agree with source.
- Logical versus physical join roles are correctly separated.
- Internal links parse without target-local warnings.

## Validation evidence

- **FAIL — source/API truth:** `.join_on()` contradicts the claimed unconditional `on`/`filter` separation.
- **FAIL — format:** The pinned Prettier check reports the target.
- **PASS — parse:** Fresh dummy Sphinx build produced no target-local warning or error.
- **NOT APPLICABLE — Rust doctests:** The page contains no Rust blocks.
- **NOT RUN — external links:** required `lychee` tool is unavailable without installation.
- **PASS — standard markers:** No literal `TODO:` or `citation-needed` markers.
- **FAIL — widened marker audit:** Draft-transfer, `JOIN-TODO`, and provisional-state comments remain.

## Unverified areas

- External URL reachability was not checked.

## Recommended next action

- Resolve B1 and N1–N3, rerun the final gates, and resubmit for review.
