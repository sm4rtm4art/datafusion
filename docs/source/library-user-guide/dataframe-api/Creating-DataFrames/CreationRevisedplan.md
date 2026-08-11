## Creating-concepts.md Verdict: REVISE

**Mode:** final  
**Structural role:** inferred whole page  
**Stage:** unspecified  
**Independence:** evidence-only — Independence limitation: no author intent was supplied; this review was evidence-only.
**file:** `docs/source/library-user-guide/dataframe-api/Creating-DataFrames/creating-concepts.md`

## Review boundary

- judged: the complete target page, source accuracy, concept-page structure, proofreading, and final-mode validation
- not judged: other `Concepts/` pages, unnamed neighbors, or author-intent preservation
- consistency boundary: page plus repository truth
- final-state requirements not yet applicable: phase-specific deferral decisions; no workflow phase was supplied

## Blocking findings

### B1 — BLOCKER: Creation is conflated with Arrow materialization

- **Location:** `docs/source/library-user-guide/dataframe-api/Creating-DataFrames/creating-concepts.md` opening and §“From DataSource to DataFrame” (lines 20–66, 145–150, 295–305)
- **Rule:** `markdown.mdc` §4; Gate A lazy/eager accuracy
- **Claim:** Raw data must be converted into Arrow’s in-memory format before a `DataFrame` exists, while creation reads only metadata.
- **Counter-evidence:** Creation builds a `ListingTable` and logical scan without producing Arrow batches. Conversion and scanning occur during physical execution. Conversely, CSV and JSON schema inference can read up to 1,000 data records during creation.
- **Sources:** `execution/context/mod.rs::_read_type`; `catalog-listing/src/table.rs::scan`; `dataframe/mod.rs::collect`; `datasource/src/file_format.rs`
- **Impact:** The page’s central mental model gives readers the wrong phase boundary for I/O, decoding, and lazy execution.
- **Direction:** Separate source resolution/schema inference from execution-time scanning and Arrow `RecordBatch` production.
- **Verification:** source inspection — FAIL

### B2 — MAJOR: Registration is assigned a false caching and optimization advantage

- **Location:** target §“Access Patterns” and §“Creation Choices Ripple Forward” (lines 270–340, 515–523)
- **Rule:** `markdown.mdc` §4; Gate A truth
- **Claim:** Registration inherently caches metadata and optimizer statistics, while direct reads continually re-derive them; therefore Parquet, remote, and multi-file sources should generally be registered.
- **Counter-evidence:** Both paths create a `ListingTable`, infer a schema during creation, and attach the same runtime statistics cache. Registration primarily adds catalog naming and provider reuse across separately created queries. Re-executing one direct-read `DataFrame` does not recreate its provider. File-path SQL also requires `.enable_url_table()`.
- **Sources:** `SessionContext::_read_type`, `register_listing_table`, `enable_url_table`; `ListingTable::with_cache`
- **Impact:** Readers receive unsupported production guidance and may choose an access pattern for optimizer benefits it does not provide.
- **Direction:** Base the comparison on naming, catalog visibility, and provider reuse; distinguish repeated read calls from repeated execution.
- **Verification:** source inspection — FAIL

### B3 — MAJOR: Provider guarantees and capability matrices are materially inaccurate

- **Location:** target §“Table Providers” (lines 157–268)
- **Rule:** `markdown.mdc` §4; Gate A API and feature accuracy
- **Claim:** `scan()` runs once per execution, providers generally cache metadata, DataFusion has only three built-in providers, projection pushdown is an optional trait method, Hive partitions are automatically discovered, and the capability matrices are categorical.
- **Counter-evidence:** `scan()` is invoked during physical planning; caching is implementation-specific; built-ins also include providers such as `ViewTable` and `EmptyTable`; projection is supplied through `scan()`; convenience read options default to no partition columns; statistics precision depends on format, metadata, and configuration. `StreamingTable` defaults to bounded and symmetric joins require both inputs to satisfy additional conditions.
- **Sources:** `catalog/src/table.rs`; `catalog-listing/src/table.rs`; `catalog-listing/src/options.rs`; `catalog/src/streaming.rs`; `physical-optimizer/src/join_selection.rs`
- **Impact:** Custom-provider implementers and users selecting formats receive an incorrect contract.
- **Direction:** Derive guarantees from the trait and label format/provider behavior as conditional and implementation-specific.
- **Verification:** source inspection — FAIL

### B4 — MAJOR: Session-state visibility and determinism are overclaimed

- **Location:** target §“The SessionState Clone” (lines 451–481)
- **Rule:** `markdown.mdc` §4; Gate A truth
- **Claim:** Catalog and runtime mutations remain visible to existing `DataFrame`s, and the immutable `LogicalPlan` makes a query deterministic.
- **Counter-evidence:** A table scan embeds its resolved provider in the logical plan, so later catalog registration does not rewrite that plan. Runtime changes may mutate shared internals or replace the `RuntimeEnv` `Arc`. Immutable plans can still read mutable providers or newly listed files and therefore do not guarantee deterministic results.
- **Sources:** `SessionState` fields; `DefaultTableSource`; `SessionContext::set_runtime_variable`; `ListingTable::list_files_for_scan`
- **Impact:** Readers form unsafe expectations about reproducibility and post-creation configuration changes.
- **Direction:** Distinguish cloned values, shared services, embedded providers, and mutable source data; remove the determinism guarantee.
- **Verification:** source inspection — FAIL

### B5 — BLOCKER: The sole Rust block is unregistered and not a valid doctest

- **Location:** target §“Creation of a DataFrame” (lines 497–501)
- **Rule:** `rust-docs.mdc` Invariants 1–3 and §3.4
- **Claim:** The `no_run` block demonstrates construction with `DataFrame::new(session_state, plan)`.
- **Counter-evidence:** The block has no imports or definitions, uses `no_run` without a named reason, and is an incomplete fragment. The page has no `doc_comment::doctest!` registration in `datafusion/core/src/lib.rs`.
- **Sources:** target block; `DataFrame::new` signature; doctest registration search
- **Impact:** Final validation cannot compile the only Rust example.
- **Direction:** Make it a self-contained executable doctest and register the page, or render it as conceptual text.
- **Verification:** doctest — NOT RUN, no registered target

### B6 — MAJOR: The first H2 overloads the reader before establishing the model

- **Location:** target §“From DataSource to DataFrame” (lines 45–153)
- **Rule:** `markdown.mdc` §7.4 first-H2 altitude and Gate C cognitive load
- **Claim:** The first H2 serves as the page’s topic on-ramp.
- **Counter-evidence:** It spans more than 100 lines and introduces provider variants, method names, catalog internals, state cloning, execution, a dense diagram, and a second matrix before those concepts are explained.
- **Sources:** target structure; `markdown.mdc` §7.4
- **Impact:** Readers must hold nearly the entire page model simultaneously, and later sections repeat the same material.
- **Direction:** Retain a compact organizing principle in the first H2 and move or simplify the detailed architecture artifact.
- **Verification:** structural review — FAIL

## Non-blocking findings

### N1 — MINOR: Required local contents navigation is absent

- **Location:** target opening block after the Concepts Covered table (lines 34–44)
- **Direction:** Add the local `{contents}` navigation required for concept leaves.

### N2 — MINOR: The page breaks an inbound internal anchor

- **Location:** target §“The Catalog: How Registration Connects to Creation”; `Creating-DataFrames/registered-tables.md` line 159
- **Direction:** Restore a compatible `the-catalog-model` anchor or update the inbound link.

### N3 — NIT: H2 dividers are missing

- **Location:** before the H2s at lines 156, 269, 342, and 484
- **Direction:** Add the prescribed `---` dividers.

### N4 — NIT: Access-pattern terminology is inconsistent

- **Location:** target §“Access Patterns” (lines 274–340)
- **Direction:** Use `direct read` and `registration` consistently; remove “registered Named”.

### N5 — NIT: The concept-page Style Note is absent

- **Location:** target opening block
- **Direction:** Add the collapsed Style Note expected by the leaf-page shape.

## Open questions

None.

## Verified strengths

- The page owns an explanatory creation model rather than functioning as a second index.
- `DataFrame` is correctly described structurally as a `LogicalPlan` plus `SessionState`, with transformations remaining lazy.
- Default `datafusion.public` resolution and one-, two-, and three-part table references match repository configuration and resolution code.

## Validation evidence

- **FAIL — repository truth spot-check:** B1–B4 contradict current source behavior.
- **FAIL — format command:** `ci/scripts/doc_prettier_check.sh` exited 1 for three unrelated plan files; the target was not reported.
- **PASS — parse differential:** two fresh dummy builds succeeded and `comm -13` was empty; the persistent inbound-anchor warning produced N2.
- **NOT RUN — targeted Rust doctest:** no registered/runnable target exists.
- **NOT RUN — external links:** required tool is unavailable without installation.
- **PASS — markers:** no `TODO:`, `citation-needed`, `TBD`, `FIXME`, or placeholder markers found.

## Unverified areas

- External URLs were not checked because `lychee` is unavailable.
- The Rust fragment could not be compiled through the documentation gate.
- Other `Concepts/` pages and cross-page terminology ownership were excluded.

## Recommended next action

- Return the page for a truth-first revision addressing B1–B6 before another final review.

---

## Registered-tables.md Verdict: REVISE

**Mode:** final  
**Structural role:** inferred first_h2; whole page reviewed  
**Stage:** unspecified  
**Independence:** evidence-only — Independence limitation: no author intent was supplied; this review was evidence-only.
**file:** `docs/source/library-user-guide/dataframe-api/Creating-DataFrames/registered-tables.md`

## Review boundary

- **Judged:** Entire target page, associated doctest registration, relevant DataFusion APIs, links, structure, and validation.
- **Not judged:** Other Concepts files or broader documentation consistency.
- **Consistency boundary:** Repository truth through targeted source/API checks.
- **Final-state requirements not yet applicable:** None.

## Blocking findings

### B1 — BLOCKER: Registration is incorrectly presented as avoiding query I/O

- **Location:** `docs/source/library-user-guide/dataframe-api/Creating-DataFrames/registered-tables.md` §§“Registering Data Sources,” “When Not to Register,” and “Cached Queries” (lines 68–130, 493–576)
- **Rule:** `markdown.mdc` §4; Truth gate.
- **Claim:** Registration caches metadata, prevents repeated file scanning/I/O, uniquely improves pruning, and direct reads avoid stale metadata.
- **Counter-evidence:** Registered and direct reads both create `ListingTable`s using the session’s shared statistics cache. Every executed scan still lists and reads files. Registration primarily reuses the resolved schema and provider; it does not eliminate query I/O. File visibility, schema freshness, and runtime caches are separate concerns.
- **Sources:** `execution/context/mod.rs` lines 1713–1757 and 1835–1856; `catalog-listing/src/table.rs` lines 775–830.
- **Impact:** Readers receive incorrect performance and freshness expectations.
- **Direction:** Restrict the benefit to naming, API interoperability, and avoiding repeated schema/provider construction; describe file scans and cache behavior separately.
- **Verification:** Source inspection — PASS; documented claim — FAIL.

### B2 — BLOCKER: Hive partition discovery is not automatic

- **Location:** Target §“Registering Data Sources” (lines 95–102)
- **Rule:** `markdown.mdc` §4; Truth gate.
- **Claim:** Pointing registration at a directory automatically discovers Hive-style partitions and enables pruning.
- **Counter-evidence:** `ListingOptions` and format read options default to an empty `table_partition_cols`. Callers must specify partition column names and types.
- **Sources:** `catalog-listing/src/options.rs` lines 100–109 and 169–234; `core/src/datasource/file_format/options.rs` lines 55–121 and 251–329.
- **Impact:** Users may expect missing partition columns and pruning that are not configured.
- **Direction:** Require explicit `table_partition_cols` configuration and distinguish path parsing from automatic discovery.
- **Verification:** Source inspection — PASS; documented claim — FAIL.

### B3 — BLOCKER: Async classification is false

- **Location:** Target §“Registering Data Sources” (lines 87–90)
- **Rule:** `markdown.mdc` §4 (lazy/eager and API accuracy).
- **Claim:** “All registration and query methods are `async`.”
- **Counter-evidence:** `.register_batch()`, `.register_table()`, `.deregister_table()`, and `.table_exist()` are synchronous. File registration, `.table()`, and `.sql()` are asynchronous.
- **Sources:** `execution/context/mod.rs` lines 537–544 and 1937–2000.
- **Impact:** Readers add or omit `.await` incorrectly, causing compilation failures.
- **Direction:** Classify the methods individually.
- **Verification:** Source inspection — PASS; documented claim — FAIL.

### B4 — MAJOR: Opening repeats itself and descends prematurely

- **Location:** Target title block and §“Registering Data Sources” (lines 20–130)
- **Rule:** `markdown.mdc` §§7.2–7.5.
- **Claim:** The title highlight, abstract, and first H2 each orient the reader.
- **Counter-evidence:** All three repeat the named-table/shared-API/cached-metadata thesis. The first H2 then carries async mechanics, performance claims, partition behavior, a decision table, and two admonitions.
- **Sources:** Target page and applicable structure rules.
- **Impact:** The organizing principle is obscured by repetition and mixed abstraction levels.
- **Direction:** Give the abstract the outcome, make the first H2 a short conceptual on-ramp, and move decision/failure details into later content sections.
- **Verification:** Structural inspection — FAIL.

### B5 — BLOCKER: Final page has broken rendering and navigation

- **Location:** Target lines 34–46, 159–160, 268–269, and 669–685
- **Rule:** `markdown.mdc` §§6 and 7.4.
- **Claim:** The Key methods and References entries provide usable navigation in a finished page.
- **Counter-evidence:** The page defines none of its shortcut reference links, so Key methods labels render as text. Sphinx reports one terminal-transition error and four missing-reference warnings: two incorrect `catalogs.md` paths, one incorrect `information_schema.md` path, and the nonexistent `#the-catalog-model` anchor.
- **Sources:** Two fresh Sphinx builds; targeted reference-definition and link-target searches.
- **Impact:** Important API and conceptual links are unusable, and the document emits a parser error.
- **Direction:** Add valid reference definitions, correct the internal paths/anchor, and remove the trailing transition.
- **Verification:** Current-page parse diagnostics — FAIL.

### B6 — BLOCKER: Doctest registration has non-portable path casing

- **Location:** `datafusion/core/src/lib.rs` lines 1342–1345
- **Rule:** `rust-docs.mdc` §8, Invariant 3.
- **Claim:** The target is registered for repository doctesting.
- **Counter-evidence:** Git tracks `Creating-DataFrames/registered-tables.md`, while registration names `creating-dataframes/registered-tables.md`. It resolves on the local case-insensitive filesystem but not on a case-sensitive Linux checkout.
- **Sources:** `git ls-files`; `datafusion/core/src/lib.rs`.
- **Impact:** CI cannot reliably include the page and may fail while resolving the doctest source.
- **Direction:** Match the tracked directory casing exactly.
- **Verification:** Local doctest — PASS; case-sensitive registration inspection — FAIL.

## Non-blocking findings

### N1 — MINOR: File providers do consume memory

- **Location:** Target §“Registration Methods” table (lines 288–297)
- **Direction:** Replace “Memory Impact: None” with wording that data remains lazy while schemas, providers, and caches still occupy memory.

### N2 — MINOR: SQL equivalence uses the wrong admonition class

- **Location:** Target §“Mixing SQL and DataFrame APIs” (lines 652–657)
- **Direction:** Use a `note` rather than unsupported `attention`, as required for SQL-equivalence comparisons.

### N3 — MINOR: The page has no conclusion

- **Location:** Target §“References” (lines 669–685)
- **Direction:** Provide the required short conclusion and place references under optional `### Further Reading`.

## Open questions

### Q1 — QUESTION: Page classification is contradictory

- **Location:** Target as a whole
- **Resolves it:** The Author must choose between a cognitive concept page—which stays cognitive and uses Concepts Covered—or a mixed leaf page with Key Methods and action examples. “Concept (cognitive/action)” does not identify one applicable shape.

## Verified strengths

- The default `datafusion.public` hierarchy and qualified-name explanation match repository configuration.
- `.into_view()` stores a logical plan and executes it using the querying session state.
- `.deregister_table()` correctly returns an optional removed provider.
- All six visible Rust examples execute and assert their output successfully.
- No unresolved `TODO:` or `citation-needed` markers remain.

## Validation evidence

**FAIL — repository format command:** `ci/scripts/doc_prettier_check.sh` exited 1 for four unrelated plan files; the target was not listed.

**PASS — differential Sphinx parse:** Both fresh builds exited 0 and `comm -13` was empty.

**FAIL — current-page parse diagnostics:** Both builds emitted one target error and four target warnings.

**PASS — local Rust doctest:** 6 passed, 0 failed.

**NOT RUN — external link check:** required tool is unavailable without installation.

**PASS — markers:** No target markers found.

## Unverified areas

- External URL availability was not checked because `lychee` is unavailable.
- A case-sensitive CI runner was not executed; the registration mismatch is source-visible.

## Recommended next action

- Return the page for revision of B1–B6 and resolution of Q1 before another final review.

---

## From-sql.md Verdict: REVISE

**Mode:** final  
**Structural role:** inferred first_h2; whole page reviewed  
**Stage:** unspecified  
**Independence:** evidence-only — Independence limitation: no author intent was supplied; this review was evidence-only.
**file:** `docs/source/library-user-guide/dataframe-api/Creating-DataFrames/from-sql.md`

## Review boundary

- **Judged:** Entire target, relevant SQL APIs, doctest registration, links, structure, and validation.
- **Not judged:** Other Concepts files or corpus-wide consistency.
- **Consistency boundary:** Repository truth through targeted source checks.
- **Final-state requirements not yet applicable:** None.

## Blocking findings

### B1 — BLOCKER: The “only SELECT” safety example still permits statements

- **Location:** `docs/source/library-user-guide/dataframe-api/Creating-DataFrames/from-sql.md` §“Controlling Allowed SQL” (lines 126–169)
- **Rule:** `markdown.mdc` §4; Truth and safety gate.
- **Claim:** Disabling DDL and DML produces a configuration that allows only `SELECT` and is sufficient for externally supplied SQL.
- **Counter-evidence:** `SQLOptions::new()` defaults `allow_statements` to `true`; the example never calls `.with_allow_statements(false)`. Moreover, `SQLOptions` only gates logical-plan categories—it does not provide table authorization, resource limits, or a complete untrusted-query sandbox.
- **Sources:** `execution/context/mod.rs` lines 2281–2360.
- **Impact:** Applications may execute `SET`, transaction, or other statements despite expecting query-only behavior.
- **Direction:** Disable all three non-query categories for query-only use and clearly state the remaining authorization/resource risks.
- **Verification:** Source inspection — PASS; example claim — FAIL.

### B2 — BLOCKER: DML execution semantics are false

- **Location:** Target §“Controlling Allowed SQL” (lines 171–176)
- **Rule:** `markdown.mdc` §4 (lazy/eager accuracy).
- **Claim:** DDL and DML execute eagerly inside `.sql()`, returning an empty `DataFrame` with no plan.
- **Counter-evidence:** `execute_logical_plan()` eagerly handles selected DDL and statements. DML and `COPY` fall through to `DataFrame::new()` and remain plans until an action. Even eager DDL returns an empty `DataFrame` backed by an `EmptyRelation` plan.
- **Sources:** `execution/context/mod.rs` lines 687–775 and 814–817.
- **Impact:** Readers trigger—or fail to trigger—side effects at the wrong point.
- **Direction:** Classify DDL, statements, DML, and queries according to their actual execution paths.
- **Verification:** Source inspection — PASS; documented claim — FAIL.

### B3 — BLOCKER: Identical-performance claims violate the documentation invariant

- **Location:** Target title block and §“When to Use Which” (lines 26–30, 406–410)
- **Rule:** `markdown.mdc` §§0, 1, and 4: compare SQL and DataFrame APIs by ergonomics, never speed.
- **Claim:** SQL and builder pipelines have “identical performance.”
- **Counter-evidence:** Both produce `LogicalPlan`s for the same optimizer and execution engine, but SQL additionally performs parsing and planning. End-to-end identical performance is neither guaranteed nor established.
- **Sources:** `SessionContext::sql_with_options()` and applicable invariant.
- **Impact:** Readers receive an unsupported performance guarantee.
- **Direction:** State execution equivalence through the shared plan and compare only ergonomics or safety, once in a `note`.
- **Verification:** Source architecture — PASS; performance claim — FAIL.

### B4 — BLOCKER: A visible Rust example is deliberately skipped

- **Location:** Target §“External Data Sources and Pushdown” (lines 428–457)
- **Rule:** `rust-docs.mdc` Invariants 1–3; `ignore` is never permitted.
- **Claim:** The external-provider block demonstrates equivalent pushdown behavior.
- **Counter-evidence:** The block uses `rust,ignore`, references an unregistered `pg_users` table, executes no action, and asserts neither results nor pushdown.
- **Sources:** Target; targeted Rust gate.
- **Impact:** The principal evidence for the section is untested and cannot demonstrate its claim.
- **Direction:** Replace it with executable evidence or remove the block and link to the owning provider guide.
- **Verification:** Rust gate — FAIL: 5 passed, 1 ignored.

### B5 — BLOCKER: The sorted example displays the wrong order

- **Location:** Target §“Round-Trip: DataFrame → SQL → DataFrame” (lines 380–397)
- **Rule:** `rust-docs.mdc` §§3.1–3.2; Truth gate.
- **Claim:** Sorting `total_revenue` descending produces the displayed North-then-South output.
- **Counter-evidence:** South has `16500` and must precede North with `11000`. `assert_batches_sorted_eq!` re-sorts both sides and masks the contradiction.
- **Sources:** Target code and passing doctest.
- **Impact:** Visible output contradicts the transformation while the test falsely passes.
- **Direction:** Use `assert_batches_eq!` for the explicitly ordered result and place South first.
- **Verification:** Compilation — PASS; output oracle — FAIL.

### B6 — MAJOR: The three opening layers perform the same job

- **Location:** Target title block and §“From SQL String to DataFrame” (lines 20–78)
- **Rule:** `markdown.mdc` §§7.2–7.5.
- **Claim:** The title highlight, abstract, and first H2 independently orient the reader.
- **Counter-evidence:** Each repeats the same-plan/lazy-DataFrame thesis. The first H2 then descends immediately into method usage and safety controls.
- **Sources:** Target and applicable structure rules.
- **Impact:** The organizing principle is repeated rather than progressively developed.
- **Direction:** Separate the page outcome, conceptual on-ramp, and method-level action material.
- **Verification:** Structural inspection — FAIL.

### B7 — BLOCKER: Final rendering and navigation remain broken

- **Location:** Target lines 35–42, 176–177, 477–499
- **Rule:** `markdown.mdc` §§6 and 7.4.
- **Claim:** The Key methods and References entries provide finished navigation.
- **Counter-evidence:** Shortcut-style API labels have no reference definitions. Sphinx reports a terminal-transition error and two missing references to `../../user-guide/sql/index.rst`.
- **Sources:** Two fresh Sphinx builds; reference-definition search.
- **Impact:** API links render as text, SQL Reference links fail, and the page emits a parser error.
- **Direction:** Define the shortcut references, correct the SQL Reference path, and remove the trailing transition.
- **Verification:** Current-page parse diagnostics — FAIL.

### B8 — BLOCKER: Doctest registration has incorrect path casing

- **Location:** `datafusion/core/src/lib.rs` lines 1330–1333
- **Rule:** `rust-docs.mdc` §8, Invariant 3.
- **Claim:** The target is portably registered for doctesting.
- **Counter-evidence:** Git tracks `Creating-DataFrames/from-sql.md`; registration names `creating-dataframes/from-sql.md`. It succeeds locally only on a case-insensitive filesystem.
- **Sources:** `git ls-files`; `datafusion/core/src/lib.rs`.
- **Impact:** Case-sensitive CI cannot reliably resolve the target.
- **Direction:** Match the tracked directory casing exactly.
- **Verification:** Local registration — PASS; case-sensitive portability — FAIL.

## Non-blocking findings

### N1 — MINOR: “Any valid SQL SELECT” overstates language support

- **Location:** Target §“Basic Usage” (lines 81–87)
- **Direction:** Say “supported SQL `SELECT` syntax”; DataFusion does not implement every statement valid in every SQL dialect.

### N2 — MINOR: `.parse_sql_expr()` is promised but never covered

- **Location:** Target Key methods table (lines 35–42)
- **Direction:** Remove the row or add bounded body coverage linked from the table.

### N3 — MINOR: References sit outside the conclusion

- **Location:** Target §§“Bringing It Together” and “References” (lines 463–499)
- **Direction:** Make References an optional `### Further Reading` subsection under the conclusion.

## Open questions

### Q1 — QUESTION: Page classification is contradictory

- **Location:** Target as a whole
- **Resolves it:** The Author must choose a cognitive concept page or a mixed action leaf. The present Key methods table and six workflows match a mixed leaf, while the declared “concept (cognitive/action)” does not identify one applicable shape.

## Verified strengths

- `.sql_with_options()` validates the complete logical plan, including subqueries.
- The single-statement limitation matches `SessionState::sql_to_statement()`.
- `.into_view()` stores the logical plan without materializing data.
- Five executable Rust blocks pass.
- Unordered workflow results generally use `assert_batches_sorted_eq!`.
- No unresolved markers remain.

## Validation evidence

**FAIL — repository format command:** The script exited 1 for four unrelated plan files; the target was not listed.

**PASS — differential Sphinx parse:** Both fresh builds exited 0 and introduced no new diagnostics.

**FAIL — current-page parse diagnostics:** Both builds emitted one target error and two target warnings.

**FAIL — Rust doctest:** 5 passed, 1 ignored; success requires all six visible blocks to execute.

**NOT RUN — external link check:** required tool is unavailable without installation.

**PASS — markers:** No `TODO:` or `citation-needed` markers found.

## Unverified areas

- External URL availability was not checked because `lychee` is unavailable.
- The ignored external-provider example provides no runtime pushdown evidence.

## Recommended next action

- Return the page for revision of B1–B8 and resolution of Q1 before another final review.

---

## From-memory.md Verdict: REVISE

**Mode:** final  
**Structural role:** inferred first_h2; whole page reviewed  
**Stage:** unspecified  
**Independence:** evidence-only — Independence limitation: no author intent was supplied; this review was evidence-only.
**file:** `docs/source/library-user-guide/dataframe-api/Creating-DataFrames/from-memory.md`

## Review boundary

- **Judged:** Entire target, relevant in-memory APIs, doctest registration, links, structure, and validation.
- **Not judged:** Other Concepts files or corpus-wide consistency.
- **Consistency boundary:** Repository truth through targeted source checks.
- **Final-state requirements not yet applicable:** None.

## Blocking findings

### B1 — BLOCKER: The creation paths do not all return an identical `DataFrame`

- **Location:** `docs/source/library-user-guide/dataframe-api/Creating-DataFrames/from-memory.md` title block, §§“From RecordBatch to DataFrame” and “Choosing the Right Method” (lines 20–34, 64–82, 350–365)
- **Rule:** `markdown.mdc` §4; Truth gate.
- **Claim:** Every method wraps batches and returns one identical lazy `DataFrame`.
- **Counter-evidence:** `.read_batch()` and `.read_batches()` return `DataFrame`; `.register_batch()` returns `Result<Option<Arc<dyn TableProvider>>>`; `MemTable::try_new()` returns `MemTable`. Registration requires a subsequent `.table()` call. Named/unnamed scans and partition counts also produce materially different plans.
- **Sources:** `execution/context/mod.rs` lines 537–544 and 1782–1826; `catalog/src/memory/table.rs`.
- **Impact:** Readers misunderstand return types and attempt invalid chaining.
- **Direction:** Describe each method’s actual return value and the separate operation that obtains a `DataFrame`.
- **Verification:** Source inspection — PASS; documented model — FAIL.

### B2 — BLOCKER: Batch schemas need compatibility, not exact identity

- **Location:** Target §“Multiple Batches with `.read_batches()`” (lines 135–200)
- **Rule:** `markdown.mdc` §4.
- **Claim:** Every batch must have exactly the same schema, including nullability and metadata.
- **Counter-evidence:** `MemTable::try_new()` checks `first_schema.contains(batch_schema)`. Arrow’s containment relation permits compatible differences, including a nullable field containing a non-nullable field and compatible metadata subsets.
- **Sources:** `catalog/src/memory/table.rs` lines 80–104; Arrow `Schema::contains()`, `Fields::contains()`, and `Field::contains()`.
- **Impact:** Readers receive an incorrect validation contract.
- **Direction:** Describe schema compatibility according to `Schema::contains()` rather than byte-for-byte identity.
- **Verification:** Source inspection — PASS; documented claim — FAIL.

### B3 — MAJOR: Sorted assertions cannot verify the demonstrated sorts

- **Location:** Target §§“Multiple Batches” and “Explicit MemTable” (lines 170–188, 311–329)
- **Rule:** `rust-docs.mdc` §§3.1–3.2.
- **Claim:** The examples demonstrate ascending result order.
- **Counter-evidence:** Both use `assert_batches_sorted_eq!`, which sorts the output before comparison and therefore passes even if `.sort()` is removed or incorrect.
- **Sources:** Target examples and passing doctests.
- **Impact:** The visible transformation lacks an effective test oracle.
- **Direction:** Use `assert_batches_eq!` where explicit `.sort()` guarantees order.
- **Verification:** Compilation — PASS; output oracle — FAIL.

### B4 — BLOCKER: Final rendering and navigation remain broken

- **Location:** Target lines 35–42, 68, 199–200, and 387–409
- **Rule:** `markdown.mdc` §§6 and 7.4.
- **Claim:** Key methods and References provide finished navigation.
- **Counter-evidence:** Shortcut API references have no definitions. Sphinx reports one terminal-transition error and three missing `../../user-guide/arrow-introduction.md` references.
- **Sources:** Two fresh Sphinx builds; reference-definition search.
- **Impact:** Links render incorrectly and the document emits a parser error.
- **Direction:** Define API references, correct the Arrow Introduction path, and remove the trailing transition.
- **Verification:** Current-page parse diagnostics — FAIL.

### B5 — BLOCKER: Doctest registration uses incorrect path casing

- **Location:** `datafusion/core/src/lib.rs` lines 1324–1327
- **Rule:** `rust-docs.mdc` §8, Invariant 3.
- **Claim:** The page is portably registered for doctesting.
- **Counter-evidence:** Git tracks `Creating-DataFrames/from-memory.md`; registration uses `creating-dataframes/from-memory.md`.
- **Sources:** `git ls-files`; `datafusion/core/src/lib.rs`.
- **Impact:** Case-sensitive CI cannot reliably resolve the page.
- **Direction:** Match the tracked path casing exactly.
- **Verification:** Local doctest — PASS; case-sensitive portability — FAIL.

### B6 — MAJOR: The opening and first H2 duplicate the same model

- **Location:** Target title block and §“From RecordBatch to DataFrame” (lines 20–82)
- **Rule:** `markdown.mdc` §§7.2–7.5.
- **Claim:** The title highlight, abstract, and first H2 provide distinct levels of orientation.
- **Counter-evidence:** All three repeat “RecordBatch → MemTable → lazy DataFrame, without conversion.” The first H2 then owns every method-level workflow through line 347.
- **Sources:** Target and applicable structure rules.
- **Impact:** The page repeats its thesis before descending directly into a large action subtree.
- **Direction:** Separate page outcome, conceptual on-ramp, and method workflows.
- **Verification:** Structural inspection — FAIL.

## Non-blocking findings

### N1 — MINOR: References sit outside the conclusion

- **Location:** Target §§“Bringing It Together” and “References” (lines 374–409)
- **Direction:** Place References under the conclusion as optional `### Further Reading`.

## Open questions

### Q1 — QUESTION: Page classification is contradictory

- **Location:** Target as a whole
- **Resolves it:** The Author must choose a cognitive concept page or a mixed action leaf. The current Key methods table and four executable workflows match a mixed leaf.

## Verified strengths

- `.read_batch()` and `.read_batches()` correctly use a one-partition `MemTable`.
- `.register_batch()` correctly registers a one-batch `MemTable`.
- Explicit `MemTable` partitions are preserved for execution.
- All four Rust blocks compile and execute.
- The explicitly ordered single-batch and SQL examples use order-sensitive assertions.
- No unresolved markers remain.

## Validation evidence

**FAIL — repository format command:** It exited 1 for four unrelated plan files; the target was not listed.

**PASS — differential Sphinx parse:** Both builds exited 0 and introduced no new diagnostics.

**FAIL — current-page parse diagnostics:** Both builds emitted one target error and three target warnings.

**PASS — Rust doctest:** 4 passed, 0 failed, 0 ignored.

**NOT RUN — external link check:** required tool is unavailable without installation.

**PASS — markers:** No unresolved markers found.

## Unverified areas

- External URL availability was not checked because `lychee` is unavailable.

## Recommended next action

- Return the page for revision of B1–B6 and resolution of Q1 before another final review.

---

## Inline-data.md Verdict: REVISE

**Mode:** final  
**Structural role:** inferred first_h2; whole page reviewed  
**Stage:** unspecified  
**Independence:** evidence-only — Independence limitation: no author intent was supplied; this review was evidence-only.
**file:** `docs/source/library-user-guide/dataframe-api/Creating-DataFrames/inline-data.md`

## Review boundary

- **Judged:** Entire target, relevant inline APIs/macros, doctest registration, links, structure, and validation.
- **Not judged:** Other Creation files or corpus-wide consistency.
- **Consistency boundary:** Repository truth through targeted source checks.
- **Final-state requirements not yet applicable:** None.

## Blocking findings

### B1 — BLOCKER: Cross-context composition is incorrectly declared impossible

- **Location:** `docs/source/library-user-guide/dataframe-api/Creating-DataFrames/inline-data.md` admonition “Own SessionContext” (lines 141–151)
- **Rule:** `markdown.mdc` §4; Truth gate.
- **Claim:** A `dataframe!` result cannot be joined with a table obtained from another `SessionContext`; collection and re-import are required.
- **Counter-evidence:** `.join()` and `.union()` combine the two logical plans and retain the left `DataFrame`’s session state; they do not reject differing source contexts. The page’s own empty-schema example successfully unions DataFrames created by separate contexts.
- **Sources:** `dataframe/mod.rs` lines 763–770 and 1299–1319; target lines 389–419.
- **Impact:** Readers perform unnecessary eager collection and copying, while missing the actual caveat that execution uses the left plan’s session state.
- **Direction:** Explain left-state execution and context-dependent resources; present re-import as an option when the target context must own execution, not a requirement.
- **Verification:** Source inspection and target doctest — PASS; documented prohibition — FAIL.

### B2 — BLOCKER: “Fastest” is an unsupported performance guarantee

- **Location:** Target §“Bringing It Together” (lines 482–492)
- **Rule:** `markdown.mdc` §§0, 1, and 4.
- **Claim:** `dataframe!` is the fastest way to create a `DataFrame`.
- **Counter-evidence:** No benchmark or API contract establishes this, and the applicable invariant prohibits unsupported speed comparisons.
- **Sources:** Target and applicable performance invariant.
- **Impact:** Readers choose an API based on an unverified guarantee.
- **Direction:** Describe it as the most concise inline construction path.
- **Verification:** Performance evidence — NOT RUN / not supplied.

### B3 — MAJOR: The first H2 starts at method level

- **Location:** Target §“The `dataframe!` Macro” (lines 68–257)
- **Rule:** `markdown.mdc` §7.4.
- **Claim:** The first H2 provides the page’s topic-level on-ramp.
- **Counter-evidence:** It begins immediately with one method and owns syntax, types, context behavior, nulls, and generation variants. No first-H2 topic model establishes how the three creation paths relate.
- **Sources:** Target and altitude ladder.
- **Impact:** Readers encounter one implementation before receiving the page’s organizing principle.
- **Direction:** Add a compact inline-creation on-ramp, then place individual APIs at content/detail level.
- **Verification:** Structural inspection — FAIL.

### B4 — BLOCKER: Final rendering and navigation remain broken

- **Location:** Target Key methods, inline guide links, and final References (lines 36–46, 137–139, 196–198, 497–518)
- **Rule:** `markdown.mdc` §§6 and 7.4.
- **Claim:** API labels and guide references provide finished navigation.
- **Counter-evidence:** Shortcut API references have no definitions. Sphinx reports a terminal-transition error and four missing-reference warnings for the Data Types and null-values links.
- **Sources:** Two fresh Sphinx builds; reference-definition and link-target searches.
- **Impact:** API labels render as text, guide links fail, and the page emits a parser error.
- **Direction:** Define API references, correct the relative user-guide paths, and remove the trailing transition.
- **Verification:** Current-page parse diagnostics — FAIL.

### B5 — BLOCKER: Doctest registration uses incorrect path casing

- **Location:** `datafusion/core/src/lib.rs` lines 1336–1339
- **Rule:** `rust-docs.mdc` §8, Invariant 3.
- **Claim:** The page is portably registered for doctesting.
- **Counter-evidence:** Git tracks `Creating-DataFrames/inline-data.md`; registration uses `creating-dataframes/inline-data.md`.
- **Sources:** `git ls-files`; `datafusion/core/src/lib.rs`.
- **Impact:** Case-sensitive CI cannot reliably resolve the target.
- **Direction:** Match the tracked path casing exactly.
- **Verification:** Local doctest — PASS; case-sensitive portability — FAIL.

## Non-blocking findings

### N1 — MINOR: Equal-length input requirement is missing

- **Location:** Target §§“The `dataframe!` Macro” and “Explicit Arrow Types” (lines 70–137, 261–308)
- **Direction:** State that all columns/arrays must have equal lengths; otherwise `RecordBatch::try_new()` returns an error.

### N2 — MINOR: References sit outside the conclusion

- **Location:** Target §§“Bringing It Together” and “References” (lines 481–518)
- **Direction:** Place References under the conclusion as optional `### Further Reading`.

## Open questions

### Q1 — QUESTION: Page classification is contradictory

- **Location:** Target as a whole
- **Resolves it:** The Author must choose a cognitive concept page or mixed action leaf. The current Key methods table and six executable workflows match a mixed leaf.

## Verified strengths

- `dataframe!` and `DataFrame::from_columns()` create an internal default `SessionContext`.
- `dataframe!()` correctly creates a zero-row, zero-column `DataFrame`.
- `.read_empty()` correctly creates one row with zero columns.
- Supported `IntoArrayRef` primitive, string, and optional types match the table.
- All six Rust examples execute and assert their results.
- No unresolved markers remain.

## Validation evidence

**FAIL — repository format command:** It exited 1 for four unrelated plan files; the target was not listed.

**PASS — differential Sphinx parse:** Both builds exited 0 and introduced no new diagnostics.

**FAIL — current-page parse diagnostics:** Both builds emitted one target error and four target warnings.

**PASS — Rust doctest:** 6 passed, 0 failed, 0 ignored.

**NOT RUN — external link check:** required tool is unavailable without installation.

**PASS — markers:** No unresolved markers found.

## Unverified areas

- External URL availability was not checked because `lychee` is unavailable.
- No evidence supports the “fastest” performance claim.

## Recommended next action

- Return the page for revision of B1–B5 and resolution of Q1 before another final review.

---

## Streaming.md Verdict: REVISE

**Mode:** final  
**Structural role:** inferred first_h2; whole page reviewed  
**Stage:** unspecified  
**Independence:** evidence-only — Independence limitation: no author intent was supplied; this review was evidence-only.
**file:** `docs/source/library-user-guide/dataframe-api/Creating-DataFrames/streaming.md`

## Review boundary

- **Judged:** Entire target, streaming providers, planner behavior, doctest registration, links, structure, and validation.
- **Not judged:** Other Creation files or corpus-wide consistency.
- **Consistency boundary:** Repository truth through targeted source checks.
- **Final-state requirements not yet applicable:** None.

## Blocking findings

### B1 — BLOCKER: `StreamTable` is incorrectly presented as a tail-following source

- **Location:** `docs/source/library-user-guide/dataframe-api/Creating-DataFrames/streaming.md` Key types, §“File-Based Streams,” and §“Choosing the Right Approach” (lines 37–42, 141–260, 489–497)
- **Rule:** `markdown.mdc` §4; Truth and safety gate.
- **Claim:** `StreamTable` continuously tails ordinary files, while `.collect()` must never be used.
- **Counter-evidence:** `FileStreamProvider` creates one Arrow reader; `StreamRead` iterates until EOF and then terminates without polling for later appends. The example uses a finite regular file and `.collect()`, which succeeds precisely because the actual stream ends despite being marked unbounded.
- **Sources:** `catalog/src/stream.rs` lines 133–228 and 367–388; target doctest.
- **Impact:** Readers may deploy a regular file expecting continuous tailing or copy an unsafe consumption pattern to an actual FIFO.
- **Direction:** Limit the continuous-source claim to blocking sources such as FIFOs, explain finite-file test behavior, and demonstrate true unbounded consumption with `.execute_stream()`.
- **Verification:** Source inspection and doctest — PASS; documented model — FAIL.

### B2 — BLOCKER: Ordinary hash joins can support one unbounded input

- **Location:** Target §“How the Planner Responds” operator table (lines 421–431)
- **Rule:** `markdown.mdc` §4.
- **Claim:** A non-streaming hash join does not support unbounded input.
- **Counter-evidence:** Join selection keeps or swaps inputs so a bounded side becomes the hash-build side while an unbounded incremental side probes it. Several join types can then emit incrementally.
- **Sources:** `physical-optimizer/src/join_selection.rs` lines 493–580; `physical-plan/src/joins/hash_join/exec.rs` lines 1026–1048.
- **Impact:** Readers reject a common valid pattern: joining an unbounded event stream against a bounded dimension table.
- **Direction:** Distinguish one-unbounded/one-bounded hash joins from two-unbounded joins requiring symmetric execution.
- **Verification:** Source inspection — PASS; operator table — FAIL.

### B3 — MAJOR: “Only consumption differs” contradicts planner restrictions

- **Location:** Target §§“Creation Methods” and “Custom Sources” (lines 120–131, 250–252, 391–394)
- **Rule:** `markdown.mdc` §4 and §7.4.
- **Claim:** An unbounded result behaves like any ordinary `DataFrame`; only consumption differs.
- **Counter-evidence:** The physical optimizer can replace operators or reject plans whose unbounded nodes require infinite memory or final emission.
- **Sources:** `physical-optimizer/src/sanity_checker.rs` lines 45–119 and join-selection source.
- **Impact:** Readers infer that every transformation remains valid as long as they call `.execute_stream()`.
- **Direction:** Separate identical API type/surface from different operator validity, planning, and consumption requirements.
- **Verification:** Source inspection — PASS; documented claim — FAIL.

### B4 — BLOCKER: The `no_run` example violates Rust documentation invariants

- **Location:** Target §“Custom Sources with `StreamingTable`” (lines 320–390)
- **Rule:** `rust-docs.mdc` §§3.4 and 5.1.
- **Claim:** The conceptual block is an acceptable `no_run` example.
- **Counter-evidence:** It has no required `// no_run: <reason>` comment and uses visible `.unwrap()` and `.expect()` as normal control flow.
- **Sources:** Target and Rust documentation rules.
- **Impact:** The example is not executable evidence and teaches panic-based source handling.
- **Direction:** State the named live-stream reason and replace panic paths with fallible handling.
- **Verification:** Compilation-only doctest — PASS; documentation invariant — FAIL.

### B5 — BLOCKER: Doctest registration uses incorrect path casing

- **Location:** `datafusion/core/src/lib.rs` lines 1348–1351
- **Rule:** `rust-docs.mdc` §8, Invariant 3.
- **Claim:** The page is portably registered for doctesting.
- **Counter-evidence:** Git tracks `Creating-DataFrames/streaming.md`; registration uses `creating-dataframes/streaming.md`.
- **Sources:** `git ls-files`; `datafusion/core/src/lib.rs`.
- **Impact:** Case-sensitive CI cannot reliably resolve the target.
- **Direction:** Match the tracked path casing exactly.
- **Verification:** Local doctest — PASS; case-sensitive portability — FAIL.

## Non-blocking findings

### N1 — MINOR: SQL DDL references are broken

- **Location:** Target lines 297–299 and 513–515
- **Direction:** Correct both relative links to the tracked `user-guide/sql/ddl.md`.

### N2 — MINOR: H2 finish structure is incomplete

- **Location:** Target lines 118–527
- **Direction:** Add required H2 dividers and a short conclusion; place References beneath it as `### Further Reading`.

### N3 — MINOR: Demonstration imports are hidden

- **Location:** Target §“File-Based Streams” (lines 181–191)
- **Direction:** Make the `Result` and assertion-macro imports visible because visible code uses them.

## Open questions

### Q1 — QUESTION: Page classification is contradictory

- **Location:** Target as a whole
- **Resolves it:** The Author must choose a cognitive concept page or mixed action leaf. The current provider construction and executable workflows match a mixed leaf.

## Verified strengths

- The `Boundedness` and `requires_infinite_memory` explanation matches physical-plan properties.
- `SanityCheckPlan` rejects final or infinite-memory operators over unbounded input.
- `StreamTable` always marks its execution plan unbounded.
- `StreamingTable` defaults to bounded and requires `.with_infinite_table(true)` for unboundedness.
- SQL `UNBOUNDED` delegates through `StreamTableFactory`.
- Both Rust blocks compile; the finite-file example executes successfully.
- No unresolved markers remain.

## Validation evidence

**FAIL — repository format command:** It exited 1 for four unrelated plan files; the target was not listed.

**PASS — differential Sphinx parse:** Both builds exited 0 and introduced no new diagnostics.

**FAIL — current-page parse diagnostics:** Both builds emitted two target warnings for the broken DDL link.

**PASS — Rust doctest:** 2 passed, 0 failed, 0 ignored; one was compilation-only.

**NOT RUN — external link check:** required tool is unavailable without installation.

**PASS — markers:** No unresolved markers found.

## Unverified areas

- External URLs and third-party project descriptions were not checked because `lychee` is unavailable.

## Recommended next action

- Return the page for revision of B1–B5 and resolution of Q1 before another final review.

---

## From-logical-plan.md Verdict: REVISE

**Mode:** final  
**Structural role:** inferred first H2; whole page reviewed  
**Stage:** unspecified  
**Independence:** evidence-only — Independence limitation: no author intent was supplied; this review was evidence-only.  
**Filepath:** `docs/source/library-user-guide/dataframe-api/Creating-DataFrames/from-logical-plan.md`

## Review boundary

- **Judged:** Complete target, linked Building Logical Plans guide for its stated purpose, relevant APIs, doctest registration, structure, and validation.
- **Not judged:** Other Creation pages or author-intent preservation.
- **Consistency boundary:** Page, named guide, and repository truth.
- **Final-state requirements not yet applicable:** None.

## Blocking findings

### B1 — BLOCKER: DML and `COPY` are incorrectly described as immediately executed

- **Location:** Target §“`DataFrame::new()` vs `.execute_logical_plan()`” (lines 170–184).
- **Rule:** `markdown.mdc` §4; Truth gate for lazy/eager behavior.
- **Claim:** `.execute_logical_plan()` executes DDL and DML immediately before wrapping what remains.
- **Counter-evidence:** `SessionContext::execute_logical_plan()` eagerly handles selected DDL and session statements. `LogicalPlan::Dml` and `LogicalPlan::Copy` reach the catch-all branch and are wrapped in a lazy `DataFrame`; execution occurs through a later action.
- **Sources:** `datafusion/core/src/execution/context/mod.rs:687–775`; `datafusion/expr/src/logical_plan/plan.rs:283–288`; `datafusion/core/tests/user_defined/insert_operation.rs:57–61`.
- **Impact:** Readers will misplace side effects and error handling for `INSERT` and `COPY`.
- **Direction:** Distinguish selected eager DDL/session handling from lazy query, DML, and `COPY` plans; identify `.execute_logical_plan()` as a `SessionContext` method.
- **Verification:** Source inspection — PASS; doctests — PASS but do not exercise this claim.

### B2 — MAJOR: The demonstrated modification does not support the page’s distinguishing use cases

- **Location:** Target §“The Core Pattern” and §“When You Need This” (lines 65–139, 187–206).
- **Rule:** Gate C evidence flow; `markdown.mdc` §7.4 unsupported-assertion test.
- **Claim:** The cycle enables tree-wide modification, per-`TableScan` policy injection, and related advanced rewrites.
- **Counter-evidence:** The example’s `LogicalPlanBuilder::filter()` only wraps the plan root—the same operation as `DataFrame::filter()`. The linked Building Logical Plans guide contains no `TreeNodeRewriter` or recursive-rewrite coverage despite the page promising it.
- **Sources:** `datafusion/expr/src/logical_plan/builder.rs:632–638`; `datafusion/expr/src/logical_plan/plan.rs:108–194`; `docs/source/library-user-guide/building-logical-plans.md`.
- **Impact:** Readers cannot reconstruct the mechanism needed for the page’s principal rationale and may mistake a root filter for per-scan enforcement.
- **Direction:** Demonstrate or correctly link the recursive tree transformation mechanism, or narrow the claimed use cases to what the example proves.
- **Verification:** API/source inspection — PASS; doctest — PASS only for the root-filter example.

### B3 — BLOCKER: Optimizer testing is attached to an unoptimized plan

- **Location:** Target §“Read-Only Inspection with `.logical_plan()`” and §“When You Need This” (lines 141–166, 200–201).
- **Rule:** `markdown.mdc` §4; Truth gate.
- **Claim:** The taught inspection path can verify predicate pushdown, projection pruning, and join reordering.
- **Counter-evidence:** `.logical_plan()` explicitly returns the unoptimized plan. `.into_optimized_plan()` applies logical optimization; physical-plan inspection is required for physical optimizer effects.
- **Sources:** `datafusion/core/src/dataframe/mod.rs:1681–1714`.
- **Impact:** Optimizer tests may inspect the wrong planning stage and report false failures or successes.
- **Direction:** State that `.logical_plan()` is unoptimized and name the appropriate optimized logical or physical-plan API for each assertion.
- **Verification:** Source inspection — PASS; doctest — PASS but does not test optimization.

### B4 — BLOCKER: Doctest registration has non-portable path casing

- **Location:** `datafusion/core/src/lib.rs:1317–1321`.
- **Rule:** `rust-docs.mdc` §8, Invariant 3.
- **Claim:** The target is registered for doctesting.
- **Counter-evidence:** Registration uses `creating-dataframes/from-logical-plan.md`; the tracked directory is `Creating-DataFrames`. This succeeds on the current case-insensitive filesystem but fails on case-sensitive systems.
- **Sources:** Target path and `datafusion/core/src/lib.rs`.
- **Impact:** Linux CI cannot reliably load the page.
- **Direction:** Match the tracked directory casing exactly.
- **Verification:** Current macOS doctest — PASS; cross-platform registration — FAIL by path inspection.

## Non-blocking findings

### N1 — MINOR: Building Logical Plans references are broken

- **Location:** Target lines 203–225.
- **Direction:** Correct the relative path; Sphinx reports both uses of `../../building-logical-plans.md` as unresolved.

### N2 — MINOR: Final H2 structure is incomplete

- **Location:** Target §§“When You Need This” and “References” (lines 186–225).
- **Direction:** Add required H2 dividers and finish with a conclusion; place further-reading material beneath it when retained.

### N3 — MINOR: The Key Methods table bypasses page navigation

- **Location:** Target lines 37–44.
- **Direction:** Link table entries to their in-page sections rather than directly to external API references.

## Open questions

### Q1 — QUESTION: Page classification is contradictory

- **Location:** Whole target.
- **Resolves it:** Author decision between an action/mixed leaf, matching the current sequence and examples, or a cognitive concept page with a Concepts Covered table and an owned mental model.

## Verified strengths

- `.into_parts()`, `DataFrame::new()`, and `.logical_plan()` signatures are represented correctly.
- The extract–root-filter–reconstruct example executes and asserts its visible result.
- The borrowing example correctly demonstrates that `.logical_plan()` does not consume the `DataFrame`.

## Validation evidence

- **FAIL — repository format command:** `ci/scripts/doc_prettier_check.sh` reported five unrelated files; the target was not listed.
- **FAIL — current-page parse finish:** Sphinx exited successfully but emitted two target-local unresolved-reference warnings.
- **PASS — Rust doctest:** 2 passed, 0 failed, 0 ignored.
- **NOT RUN — external links:** required tool is unavailable without installation (`lychee`, exit 127).
- **PASS — markers:** No `TODO:` or `citation-needed` markers.
- **FAIL — preliminary parse wrappers:** Two discarded wrapper attempts failed locally before the successful fresh parse run.

## Unverified areas

- External URL health could not be checked without `lychee`.
- No author intent was available.

## Recommended next action

- Return the page for correction of B1–B4 and N1–N3, plus an explicit Q1 classification decision, before another final review.

---

## Ecosystem-sources.md Verdict: REVISE

**Mode:** final  
**Structural role:** inferred first H2; whole page reviewed  
**Stage:** unspecified  
**Independence:** evidence-only — Independence limitation: no author intent was supplied; this review was evidence-only.  
**Filepath:** `docs/source/library-user-guide/dataframe-api/Creating-DataFrames/ecosystem-sources.md`

## Review boundary

- **Judged:** Complete target, relevant DataFusion APIs, current project-owned ecosystem documentation, registration, structure, and validation.
- **Not judged:** Other Creation pages or author-intent preservation.
- **Consistency boundary:** Page plus targeted repository and upstream-project truth.
- **Final-state requirements not yet applicable:** None.

## Blocking findings

### B1 — BLOCKER: The only Rust example is an incomplete placeholder

- **Location:** Target §“The Universal Pattern” (lines 92–121).
- **Rule:** `rust-docs.mdc` Invariants 1–2 and §3.4.
- **Claim:** The `no_run` block demonstrates the universal integration pattern.
- **Counter-evidence:** Provider acquisition is `todo!()`, so the block is deliberately incomplete; `no_run` may not conceal fragments. Its Delta example also names `delta_rs::open_table`, while the project’s Rust crate and documentation use `deltalake::open_table`.
- **Sources:** Target doctest; current delta-rs README.
- **Impact:** Compilation succeeds without proving that any provider can be obtained or registered.
- **Direction:** Replace the placeholder with a complete, compilable pattern or present non-compilable pseudocode as `text`.
- **Verification:** Doctest — PASS, compilation-only; demonstrated integration — FAIL.

### B2 — BLOCKER: Lakehouse providers are incorrectly guaranteed to support writes

- **Location:** Target §“Lakehouse Formats” (lines 145–161).
- **Rule:** `markdown.mdc` §4; Truth gate.
- **Claim:** Every listed crate implements `TableProvider` for both reads and writes.
- **Counter-evidence:** Write methods on `TableProvider` are optional and default to “not implemented.” Hudi’s project-owned DataFusion documentation describes querying and registration but supplies no corresponding DataFusion write interface.
- **Sources:** `datafusion/catalog/src/table.rs:321–380`; current hudi-rs README.
- **Impact:** Readers may design `.write_table()` workflows around capabilities the selected provider does not expose.
- **Direction:** Document read and write support separately for each provider, using project-owned capability references.
- **Verification:** DataFusion source — PASS; project documentation — blanket write guarantee unsupported.

### B3 — MAJOR: The source model is contradicted by the catalog

- **Location:** Target opening and §“Available Sources” (lines 20–33, 142–198).
- **Rule:** `markdown-landing.mdc` §2 concept-page invariant; Gates B–C.
- **Claim:** The listed ecosystem consists of sources integrated through `TableProvider`.
- **Counter-evidence:** Ballista is a distributed execution engine, Comet is a Spark accelerator, DataFusion Federation is an optimizer/framework, and JSON Functions supplies scalar functions. None follows the page’s three-step provider-registration model. The same catalog adds undefined “Production-ready,” “Maturing,” and “Incubating” ratings without project-owned criteria.
- **Sources:** Current Ballista, Comet, Lance, Iceberg, Hudi, delta-rs, and datafusion-table-providers project documentation.
- **Impact:** Readers cannot distinguish data-source adapters from execution engines and function extensions, or safely interpret maturity ratings.
- **Direction:** Keep the owned model limited to actual providers; classify adjacent ecosystem projects separately and source or remove lifecycle ratings.
- **Verification:** Current upstream metadata and READMEs — PASS.

### B4 — MAJOR: SQL DDL omits required factory registration

- **Location:** Target admonition §“SQL pathway: `TableProviderFactory`” (lines 123–136).
- **Rule:** Gate C operational flow; `markdown.mdc` §4.
- **Claim:** Implementing `TableProviderFactory` enables the shown `CREATE EXTERNAL TABLE` statement.
- **Counter-evidence:** A custom factory must first be installed in `SessionState` under the format key, for example through `SessionStateBuilder::with_table_factory()`. The page shows no such prerequisite.
- **Sources:** `datafusion/catalog/src/table.rs:538–550`; `datafusion/core/src/execution/session_state.rs:1445–1463`.
- **Impact:** The SQL fragment cannot work in an ordinary `SessionContext` merely because an external crate exists.
- **Direction:** Show or explicitly require the crate-specific factory-registration step before the DDL.
- **Verification:** DataFusion source — PASS; SQL fragment — untested.

### B5 — BLOCKER: Doctest registration has non-portable path casing

- **Location:** `datafusion/core/src/lib.rs:1311–1315`.
- **Rule:** `rust-docs.mdc` §8, Invariant 3.
- **Claim:** The page is registered for doctesting.
- **Counter-evidence:** Registration uses `creating-dataframes/ecosystem-sources.md`; the tracked directory is `Creating-DataFrames`.
- **Sources:** Target path and `datafusion/core/src/lib.rs`.
- **Impact:** Registration fails on case-sensitive filesystems.
- **Direction:** Match the tracked directory casing exactly.
- **Verification:** Current macOS doctest — PASS; cross-platform registration — FAIL by path inspection.

## Non-blocking findings

### N1 — MINOR: Concepts table bypasses in-page orientation

- **Location:** Target lines 35–41.
- **Direction:** Link each concept to its owning section within the page rather than directly to external API documentation.

### N2 — MINOR: Final H2 structure is incomplete

- **Location:** Target §§“Available Sources” and “References” (lines 142–247).
- **Direction:** Add the required H2 dividers and finish with a conclusion; place references beneath it when retained.

## Open questions

### Q1 — QUESTION: Page classification is contradictory

- **Location:** Whole target.
- **Resolves it:** Author decision between a cognitive concept page owning the `TableProvider` integration model and a reference/action catalog covering ecosystem projects.

## Verified strengths

- The three-step registration flow is valid once a concrete `Arc<dyn TableProvider>` exists.
- `.register_table()` and subsequent DataFrame/SQL access are correctly classified.
- Built-in Arrow IPC support and the cloud-object-store distinction are supported by repository source.

## Validation evidence

- **FAIL — repository format command:** Six unrelated files were reported; the target was not listed.
- **PASS — current-page parse:** Fresh Sphinx dummy build emitted no target diagnostics.
- **PASS — Rust doctest:** 1 compilation-only test passed.
- **NOT RUN — external link gate:** required tool is unavailable without installation (`lychee`, exit 127).
- **PASS — markers:** No `TODO:` or `citation-needed` markers.
- **PASS — external project evidence:** Current metadata and READMEs were retrieved after initial sandbox-denied queries; optional source-level searches blocked by the approval boundary were not used.

## Unverified areas

- Full external-link health could not be checked.
- Some provider-specific write and factory implementations could not be inspected at source level.
- No author intent was available.

## Recommended next action

- Return the page for correction of B1–B5 and N1–N2, plus an explicit Q1 classification decision, before another final review.
