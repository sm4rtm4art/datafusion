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
JOIN REVISION TODO REGISTER

Purpose: preserve the Stage 1 structural review inside the working source without
publishing editorial notes. Resolve these items during later section-by-section
revision; do not treat this register as page content. Inline comments with the
same JOIN-TODO IDs mark the current passages that need attention.

Iteration policy: revise one H2 subtree at a time. Resolve issues that belong to
the active subtree during that iteration. When a new finding belongs to a later
subtree or needs a broader decision, add a stable JOIN-TODO ID here and a matching
inline marker instead of expanding the current iteration.

Target storyline:
1. Relate rows across DataFrames.
2. Build joins with keys and conditions.
3. Choose what the join preserves.
4. Compose join workflows.
5. Validate and inspect join results.
6. Conclude and hand off to related transformations.

Structure and section ownership

- JOIN-TODO-001 [structural split implemented; finalization pending] Keep the
  approved group title "Joining DataFrames." The landing page now routes to
  five focused leaves. Add each leaf's title-line highlight, abstract, curated
  orientation table where useful, and conclusion only after its body has
  stabilized. Do not restore the inherited Basic/Intermediate/Advanced labels.
- JOIN-TODO-002 [resolved in orientation pass] The opening now recaps the
  frame-boundary model without duplicating transformation-concepts.md, accounts
  for semi/anti schema behavior and match-driven cardinality, and hands whole-row
  combination to set-operations.md.
- JOIN-TODO-003 [resolved in orientation pass] The opening now contains the
  prescribed "Choose the API That Makes the Join Logic Clear" admonition. It
  compares clarity, maintainability, and composition without making execution-
  speed or compile-time column-checking claims.
- JOIN-TODO-004 [construction pass] Separate `.join()` named equality keys from
  `.join_on()` expression conditions. Cover single/composite keys, equal-length
  key arrays, qualified expressions, multiple expressions combined with AND,
  and an explicit OR expression. Do not promise a physical algorithm based on
  method choice.
- JOIN-TODO-005 [construction pass; resolve in subtree] Decide whether NATURAL,
  intentional CROSS, and LATERAL forms receive a bounded H3 or one callout.
  Record honest API gaps and SQL/hybrid routes without becoming an SQL tutorial.
  LATERAL is conceptually a correlated FROM item and should cross-link to the
  subquery documentation. General SQL-dialect guidance belongs to the SQL
  documentation rather than this page.
- JOIN-TODO-006 [preservation pass] Reorganize the JoinType taxonomy around row
  and column preservation: inner; left/right/full outer; left/right semi and
  anti. Explain null extension and row multiplication where first relevant.
- JOIN-TODO-007 [preservation pass; resolve in subtree] Retain public LeftMark and
  RightMark only as a bounded specialist note unless an action-oriented public
  workflow can be supported. Do not label public variants "internal."
- JOIN-TODO-008 [composition pass] Consolidate aliasing, qualification,
  pre-join renaming, post-join projection, duplicate-name ambiguity, and schema
  inspection before applying them to self-joins and chained multi-way joins.
- JOIN-TODO-009 [validation pass] Replace output-count "retention" with separate
  checks for match coverage, input-key uniqueness, duplicate-driven row
  multiplication, unmatched keys, and expected schema. A left join preserves
  left rows but can return more rows than the left input.
- JOIN-TODO-010 [validation pass] State default NULL key behavior precisely.
  Remove sentinel replacement as a universal fix; it can create false matches.
  Treat filter_null_join_keys as an optimization, not a semantic repair.
- JOIN-TODO-011 [validation pass] Correct Cartesian-product guidance: unequal
  key-array lengths are a planning error; an Inner join with no condition takes
  the cross-join path. Do not recommend `.count()` or analyzed plans as safe
  preflight checks for a potentially explosive join.
- JOIN-TODO-012 [validation pass] Convert troubleshooting into a symptom-oriented
  entry point. Prefer executable assertions or bounded inspections over
  unverified `.show()` output and fixed percentage thresholds.
- JOIN-TODO-013 [plan-inspection pass; ownership decision] Keep `.explain()` and
  only enough physical vocabulary to interpret a planned join. State that
  `analyze = true` executes the plan. Move algorithm catalogs, Arrow-kernel
  detail, partition-mode tuning, late-materialization claims, and benchmarks to
  an execution owner once that destination is identified. Verify piecewise
  merge join coverage for range/inequality conditions against the target
  DataFusion version.
- JOIN-TODO-014 [presentation pass] Replace blockquotes, emoji warnings, and
  `<br>` formatting with titled MyST admonitions. Recheck output ordering and
  NULL rendering; reduce repeated fixture setup while keeping examples
  self-contained.
- JOIN-TODO-015 [cleanup pass] Consolidate the overlapping join-family,
  API-comparison, and cheat-sheet tables into the landing routing table and
  one preservation table. Add leaf conclusions, prune duplicate or
  low-authority Further Reading links, and verify anchors and incoming links
  after headings stabilize.

Claims that must be deleted or authoritatively re-verified

- JOIN-TODO-016 [accuracy pass] Remove or verify the temporal "0.004% at
  midnight" claim, "Left Join handles ~90%" claim, "16x faster" benchmark,
  unconditional right-side/build-side prescriptions, "good order reduces
  planning overhead," universal SIMD statement, and broad late-materialization
  claim. Preserve supported workflow advice without unsupported numbers.
- JOIN-TODO-017 [accuracy pass] Replace blanket claims that semi/anti joins or
  DataFusion execution are necessarily faster than alternatives. Explain the
  semantic and schema differences first; make performance conditional and
  sourced only when needed.
- JOIN-TODO-018 [accuracy pass] Treat duplicate column names as qualification,
  ambiguity, renaming, or projection concerns rather than a universal duplicate
  field error. Verify example schemas against the target DataFusion version.

External dependencies and unresolved approvals

- JOIN-TODO-019 [deferred; example-normalization pass] The join group now has
  its own `index.md`, but the parent navigation and shared-dataset owner were
  not supplied. Confirm page order and whether
  customers_df/orders_df/payments_df is the approved running dataset before
  normalizing examples.
- JOIN-TODO-020 [deferred; plan-inspection pass] `join-concepts.md` is the
  approved temporary owner for inherited execution-concept material. Identify
  the long-term owner for deep execution and optimizer material before
  extraction; do not let it dominate the join-concepts page.
- JOIN-TODO-021 [final pass] Confirm scope across the join page group:
  join-concepts.md owns topic-specific join semantics; join-conditions.md owns
  construction; join-types.md owns preservation choices; join-workflows.md
  owns composition and schema shaping; join-validation.md owns correctness and
  bounded plan inspection. transformation-concepts.md retains the broader
  frame-boundary model and set-operations.md owns whole-row combination.
- JOIN-TODO-022 [deferred; execution-ownership pass] Decide whether guidance on
  joining inside a source system versus in DataFusion has a supported owner and
  an action-oriented use case. Do not restore the removed broad Postgres-versus-
  DataFusion performance comparison without authoritative, scenario-specific
  support.
- JOIN-TODO-023 [join-concepts discussion pass] Broaden the unapproved
  conceptual opener so left and right are logical inputs and do not imply two
  distinct source DataFrames; account explicitly for self-derived sides.
- JOIN-TODO-024 [navigation integration pass] Update the parent
  Transformations/index.md toctree and incoming links to route through this
  group, then retire or redirect the old `joins.md` route only after that
  migration is complete. The parent file was not supplied and is outside this
  Stage 2 scope.
- JOIN-TODO-025 [doctest integration pass] Register the five new leaf files in
  datafusion/core/src/lib.rs after Author approval. Repository code and the
  registration file are unavailable in this workspace.
- JOIN-TODO-026 [cross-link pass] Verify cross-page anchors, relative links,
  redirects, and incoming references after leaf headings stabilize.
- JOIN-TODO-027 [landing finalization pass] Revisit the curated landing table
  after the five leaves stabilize; keep only routing scent that adds value
  beyond the toctree.
-->

<!-- MOVE HANDSHAKE: Routing and cheat-sheet material arrived from ../joins.md. The unchanged source is a temporary coordinator comparison artifact, not a seventh published page. -->
<!-- JOIN-TODO-001 JOIN-TODO-024 JOIN-TODO-026 JOIN-TODO-027: Finalize the page group and parent navigation after the leaves stabilize. -->

# Joining DataFrames

**Use this page group to choose how rows relate, what a join preserves, how joined results compose, and how to validate the outcome.**

Joins span a broad set of reader decisions, from expressing a match to diagnosing row multiplication. Start with the conceptual model, or go directly to the page that matches the current task.

```{toctree}
:maxdepth: 1

join-concepts
join-conditions
join-types
join-workflows
join-validation
```

<!-- JOIN-TODO-015 JOIN-TODO-027: The inherited cheat sheet is now a routing table; finalize its rows after the leaves stabilize. -->

| Page                                  | Go here to                                                                 | Routing scent                                            |
| ------------------------------------- | -------------------------------------------------------------------------- | -------------------------------------------------------- |
| [Join Concepts](join-concepts.md)     | Understand logical inputs, matching, preservation, schema, and cardinality | Mental model and API choice                              |
| [Join Conditions](join-conditions.md) | Express the relationship between rows                                      | [`.join()`], [`.join_on()`], composite keys, filters     |
| [Join Types](join-types.md)           | Choose which matches and non-matches survive                               | `Inner`, outer, semi, and anti joins                     |
| [Join Workflows](join-workflows.md)   | Shape schemas and compose larger relationships                             | [`.alias()`], [`.select()`], self-joins, multi-way joins |
| [Join Validation](join-validation.md) | Check coverage, multiplication, `NULL` keys, and plans                     | Cartesian products, troubleshooting, [`.explain()`]      |

[`.alias()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.alias
[`.explain()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.explain
[`.join()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.join
[`.join_on()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.join_on
[`.select()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.select
