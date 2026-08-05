# Concepts section — structural / ownership / story-arc audit

> **Working backlog** for `docs/.../dataframe-api/Concepts/`.  
> Below: section-level diagnosis (finalize-first). Per-leaf judge verdicts follow.  
> **Not** a substitute for those verdicts — synthesizes them with heading/spine inspection (2026-08-04 review session).

## Author decisions (pending implement)

| Decision                            | Intent                                                                                              |
| ----------------------------------- | --------------------------------------------------------------------------------------------------- |
| Drop `bigger-picture.md` from spine | Park/remove from toctree; merge LLVM blurb elsewhere or drop; roadmap → contributor guide link only |
| Drop `summary.md` from spine        | No dedicated Summary leaf; glance + Birth/Life/Death exit live on `index.md`                        |
| Finalize shape before truth revise  | Closings / spine / refs = P0; Gate A blocker packs = P1 side task                                   |

## Current spine (as shipped)

```text
index
  → architectural-dataframe
  → sessioncontext
  → builder-parser
  → anatomy-dataframe
  → expressions
  → null-handling
  → execution-lifecycle
  → bigger-picture          ← drop candidate
  → summary                 ← drop candidate
```

**Target spine after drops:**

```text
index (routing + glance + section exit)
  → architectural-dataframe
  → sessioncontext
  → builder-parser
  → anatomy-dataframe
  → expressions
  → null-handling
  → execution-lifecycle
```

---

## 1. Structural deficiencies

Shared across leaves (closing inventory + finals):

| Defect                           | Where it shows                                                                                                                                                                                                         |
| -------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Closing stencil ignored          | Almost no leaf uses `## Conclusion` + optional `### Further Reading`. Rivals: `## References`, `## Further Reading` as H2, `Putting It All Together`, `Where to Go Next`, content H2 as terminus, `seealso`-as-closing |
| Double / triple handoffs         | Prose transition **and** resource dump **and** sometimes a third “next section” (esp. `execution-lifecycle`, `null-handling`, `summary`)                                                                               |
| Parse / finish failures          | Trailing `---` before link defs; missing reference-style definitions; dead relative paths (`sessioncontext`, `execution-lifecycle`, `bigger-picture`, `summary`, …)                                                    |
| Concept-page openings incomplete | Missing or weak Concepts Covered / Style Note / answer-first abstract on several leaves; forbidden or weak `## Introduction…` titles (`sessioncontext`, `builder-parser`, `anatomy-dataframe`)                         |
| Landing overreach                | `index.md` restates architecture, SessionContext, two APIs, and lifecycle instead of routing — competes with child leaves                                                                                              |
| Callout / altitude clutter       | Nested admonition walls; mid-page “Further reading” callouts that steal the closing’s job (`execution-lifecycle`)                                                                                                      |
| Example oracle gaps              | Compiling `.show()` / comment tables without asserts (anatomy, execution-lifecycle, …) — structural “proof” failure, not only truth                                                                                    |

**Finalize (P0) structural work:** spine drops → index absorbs exit → normalize every remaining closing to the stencil → fix parse ERROR and closing-link breakage only (not full Gate A rewrite).

---

## 2. Ownership issues

What each leaf **should** own vs what it currently **steals or duplicates**:

| Topic                                               | Rightful owner (intended)                                          | Contested / duplicated on                                                                 |
| --------------------------------------------------- | ------------------------------------------------------------------ | ----------------------------------------------------------------------------------------- |
| OLAP vs OLTP / embeddable fit                       | `architectural-dataframe`                                          | Also taught on `index.md`                                                                 |
| `SessionContext` / `SessionState` clone semantics   | `sessioncontext` (+ Creating `creating-concepts` for clone detail) | Restated on `index`, `anatomy`, `execution-lifecycle` (clone-cost), `summary` cheat-sheet |
| SQL vs DataFrame builder                            | `builder-parser`                                                   | Large duplicate on `index.md` (“Two APIs…”)                                               |
| `LogicalPlan` + `SessionState` + `DFSchema` anatomy | `anatomy-dataframe`                                                | Delayed by clone/reproducibility essay; overlaps builder mapping                          |
| `Expr` vs planning                                  | `expressions`                                                      | Risk of physical-expr conflation (leaf B findings)                                        |
| NULL / three-valued logic                           | `null-handling`                                                    | Relatively clean ownership; closing shape only                                            |
| Lazy → optimize → physical → execute                | `execution-lifecycle`                                              | Re-taught on `index`, `bigger-picture` (Volcano), parts of `anatomy`                      |
| Volcano / async pull model                          | `execution-lifecycle` (one accurate home)                          | **`bigger-picture` re-owns** and gets it wrong (Lazy vs Eager)                            |
| Ecosystem / LLVM analogy                            | Optional short blurb on `architectural` or drop                    | **`bigger-picture`** primary; also `summary` row                                          |
| Project roadmap / epics                             | Contributor roadmap (external)                                     | **`bigger-picture` must not own**                                                         |
| Section exit (Birth / Life / Death)                 | **`index.md`**                                                     | **`summary.md` duplicates** index role                                                    |
| API cheat-sheet                                     | No Concepts home yet (park)                                        | **`summary.md`** misplaced reference dump                                                 |
| Crate SIMD / `RUSTFLAGS` tuning                     | `user-guide/crate-configuration`                                   | **`bigger-picture`** performance callout                                                  |
| Perf / “orders of magnitude” claims                 | **Nobody in Concepts** (invariant)                                 | `architectural`, `execution-lifecycle`, `bigger-picture`, `builder-parser`                |

**Ownership rule for finalize:** one mental-model owner per row; others link in one sentence. Drop leaves that only exist to re-own (`summary`, largely `bigger-picture`).

---

## 3. Story-arc issues

**Intended arc (index claims sequential reading):**  
fit → entry point → two APIs → what’s inside a DataFrame → expressions → nulls → how execution runs → (zoom out) → recap → Birth.

**What breaks the arc:**

1. **`index.md` spoils the plot** — teaches fit, SessionContext, two APIs, and lifecycle before the reader opens those leaves; sequential reading becomes repetition.
2. **Altitude thrash** — early leaves mix product pitch, API surface, and deep internals; `anatomy` delays its own model (B6); `execution-lifecycle` packs Tokio, clone economics, and optimizer into one “lifecycle” story.
3. **False climax** — `bigger-picture` after lifecycle restarts execution-model teaching and appends roadmap (contributor arc, not DataFrame-user arc).
4. **Second ending** — `summary` recaps everything again and adds cheat-sheet / link dump; no clean handoff to Creating.
5. **Broken bridges** — several “continue to X” / reference links are dead or undefined, so the promised sequence fails mechanically.
6. **Promise vs delivery** — e.g. architectural page framed as DataFrame selection guidance but evaluates the engine (leaf B4); builder-parser security story overreaches.

**Story-arc fix (finalize):**  
Landing = map + exit only → seven concept leaves in dependency order → last leaf concludes and points to Creating. No meta-recap leaf. No roadmap leaf.

---

## 4. Per-leaf status (finalize vs revise)

| Leaf                         | Finalize (shape / spine)                                        | Truth revise (side)                                 |
| ---------------------------- | --------------------------------------------------------------- | --------------------------------------------------- |
| `index.md`                   | Slim to routing; absorb glance + exit; drop two toctree entries | Soft: trim duplicated body that belongs on children |
| `architectural-dataframe.md` | Closing + parse                                                 | B-pack in verdict below                             |
| `sessioncontext.md`          | Closing; fix dead refs                                          | B-pack below                                        |
| `builder-parser.md`          | Closing; drop Introduction title                                | B-pack below                                        |
| `anatomy-dataframe.md`       | Closing; openings                                               | B-pack below (heavy)                                |
| `expressions.md`             | Closing H2 → stencil                                            | B-pack below                                        |
| `null-handling.md`           | Closing normalize (has Conclusion)                              | Mostly N-level vs other leaves                      |
| `execution-lifecycle.md`     | Closing + parse/links                                           | **Heaviest** B-pack — side task                     |
| `bigger-picture.md`          | **Remove from spine**                                           | Do not invest full truth fix in-spine               |
| `summary.md`                 | **Remove from spine**                                           | N/A                                                 |

---

## 5. How to use this file

1. **P0 finalize:** execute Author decisions + structural/closing work; tick rows in §4.
2. **P1 side:** work leaf verdicts below (BLOCKER/MAJOR) without blocking section shape.
3. Keep new judge output appended; update §1–§4 when spine decisions land.

---

# Per-leaf judge verdicts

## Anatomy-dataframe.md Verdict: REVISE

**Mode:** final  
**Structural role:** inferred — whole page; no single subtree role applies  
**Stage:** unspecified  
**Independence:** evidence-only — Independence limitation: no author intent was supplied; this review was evidence-only.
**file:** `docs/source/library-user-guide/dataframe-api/Concepts/anatomy-dataframe.md`

## Review boundary

- **Judged:** Entire concept page, technical claims, structure, links, and Rust example.
- **Not judged:** Other Concepts pages.
- **Consistency boundary:** Page and repository truth.
- **Final-state requirements not yet applicable:** None deferred.

## Blocking findings

### B1 — BLOCKER: `SessionState` is incorrectly presented as a frozen, reproducible snapshot

- **Location:** `docs/source/library-user-guide/dataframe-api/Concepts/anatomy-dataframe.md` §“Introduction” and §“Inside a DataFrame” (lines 31–149).
- **Rule:** `markdown.mdc` §4; Judge Gate A.
- **Claim:** Catalog state and the complete execution environment are frozen at DataFrame creation, guaranteeing reproducible execution.
- **Counter-evidence:** `SessionState::clone()` shares the `catalog_list` and runtime through `Arc`. A `TableScan` also stores a shared `TableSource`; providers and external data may change independently of the plan. Only selected fields, such as configuration and registry maps, are copied.
- **Sources:** `datafusion/core/src/execution/session_state.rs` lines 141–210; `datafusion/expr/src/logical_plan/plan.rs` lines 2915–2933; `SessionContext::state()` lines 2036–2051.
- **Impact:** Readers receive the wrong model for catalog visibility, source mutability, and result reproducibility.
- **Direction:** Separate copied query state from shared services and resolved sources; remove the reproducibility guarantee and “frozen catalog” language.
- **Verification:** Source inspection — PASS.

### B2 — MAJOR: Clone-cost claims violate the no-performance invariant

- **Location:** Title block and clone table (lines 20–25, 48–56).
- **Rule:** `markdown.mdc` §0 invariant 1 and §4.
- **Claim:** DataFrames are lightweight and cheaply cloneable.
- **Counter-evidence:** Derived `Clone` copies the boxed `SessionState` and owned `LogicalPlan`; those structures contain maps, vectors, expressions, and strings alongside shared `Arc` values. No evidence establishes a generally cheap cost.
- **Sources:** `datafusion/core/src/dataframe/mod.rs` lines 228–232; `datafusion/core/src/execution/session_state.rs` lines 141–210.
- **Impact:** Readers may treat cloning as a constant-cost operation.
- **Direction:** State only that cloning does not copy underlying table data; avoid an unqualified cost claim.
- **Verification:** Source inspection — PASS; benchmark evidence — NOT RUN and not required for the prohibited claim.

### B3 — BLOCKER: The API and builder-equivalence model contains false mappings

- **Location:** Lifecycle diagram (lines 82–130) and §“Under the Hood” (lines 185–198).
- **Rule:** `markdown.mdc` §4; Judge Gate A.
- **Claim:** `.read_table("sales")` creates a named-table DataFrame, Parquet scans use `ParquetExec`, and each DataFrame method produces the identical plan as its listed `LogicalPlanBuilder` method.
- **Counter-evidence:** Named lookup is `ctx.table("sales").await`; `.read_table()` accepts an `Arc<dyn TableProvider>`. Parquet scans use `DataSourceExec` with `ParquetSource`. `.select()` can add a window plan, while `.aggregate()` enables DataFrame-specific builder options and may add a projection for grouping sets.
- **Sources:** `SessionContext` lines 1784–1789 and 1991–2000; `DataFrame::select()` lines 410–433; `DataFrame::aggregate()` lines 645–675; `datafusion/datasource/src/source.rs` lines 324–352.
- **Impact:** Advanced readers may copy invalid API syntax or expect direct builder use to preserve DataFrame behavior.
- **Direction:** Correct the diagram and describe builder delegation as common rather than one-to-one.
- **Verification:** Source inspection — PASS.

### B4 — BLOCKER: `.collect()` and `.show()` are misrepresented as incremental delivery

- **Location:** End of §“Inside a DataFrame” (lines 120–145).
- **Rule:** `markdown.mdc` §4, including unbounded `.collect()` guidance.
- **Claim:** After `.collect()` or `.show()`, batches arrive one portion at a time and permit datasets larger than memory.
- **Counter-evidence:** `.collect()` returns `Vec<RecordBatch>` after buffering the complete result. `.show()` formats through the same collection path. Incremental delivery requires `.execute_stream()`, and internal operators may still retain substantial state.
- **Sources:** `DataFrame::collect()` lines 1459–1483; `.show()`/`.to_string()` lines 1486–1548; `.execute_stream()` lines 1578–1604.
- **Impact:** Readers can choose an OOM-prone action under the belief that it is streaming.
- **Direction:** Distinguish internal batch execution, result collection, and `.execute_stream()` explicitly.
- **Verification:** Source inspection — PASS.

### B5 — MAJOR: The Rust example passes without verifying its visible result

- **Location:** §“Under the Hood” Rust block (lines 207–239).
- **Rule:** `rust-docs.mdc` §3.2 and §5.1; Judge Gate A oracle calibration.
- **Claim:** The round trip produces rows `5` and `6`.
- **Counter-evidence:** The visible expected table is only a comment; the doctest calls `.show()` and contains no batch assertion.
- **Sources:** Target block; targeted doctest.
- **Impact:** A semantic regression can pass while contradicting the documented output.
- **Direction:** Collect and assert the visible rows with the appropriate batch assertion macro.
- **Verification:** Compilation/runtime — PASS; semantic oracle — FAIL.

### B6 — MAJOR: The concept storyline repeats its premise and delays the owned model

- **Location:** Opening through §“Inside a DataFrame” (lines 20–154), plus ending lines 250–259.
- **Rule:** `markdown.mdc` §7.2–§7.5; `markdown-landing.mdc` §2.
- **Claim:** The page progressively explains DataFrame anatomy.
- **Counter-evidence:** The abstract, oversized “Introduction,” analogy table, and large diagram repeat the same two-component model before reaching `DFSchema`. The first H2 uses the forbidden “Introduction” title, contains detail and a roadmap, and the page lacks Concepts Covered, Style Note, and a conclusion.
- **Sources:** Target; applicable concept-page and altitude rules.
- **Impact:** Readers must process clone semantics, registries, catalogs, runtime, reproducibility, and execution before reaching the component-level anatomy.
- **Direction:** Establish one concise anatomy model, move clone semantics into a dedicated content section, and restore the concept-page opening and conclusion shape.
- **Verification:** Structural inspection — FAIL.

## Non-blocking findings

### N1 — MINOR: Navigation is incomplete

- **Location:** Throughout; broken `../building-logical-plans.md` target at lines 250–254.
- **Direction:** Correct the broken internal path and define or replace the numerous unresolved reference-style API links.

### N2 — MINOR: Arrow `Schema` is mislabeled as an at-rest schema

- **Location:** §“DFSchema: The Schema Layer” (lines 159–167).
- **Direction:** Describe Arrow `Schema` as the schema for Arrow data, including in-memory `RecordBatch` data, rather than specifically “at rest.”

### N3 — MINOR: API casing is inconsistent

- **Location:** Title, TOC title, and H2 headings (lines 20, 27, 31, 65).
- **Direction:** Use `DataFrame` consistently.

### N4 — NIT: A drafting comment remains in the final source

- **Location:** Lines 134–142.
- **Direction:** Remove the contributor-only image note and its proofreading errors.

## Open questions

None.

## Verified strengths

- The central decomposition into an owned `LogicalPlan` and boxed `SessionState` is source-supported.
- Configuration and function-registry maps are copied, while catalog and runtime references are shared.
- `SessionContext::state()` establishes a per-query execution timestamp.
- `DFSchema` wraps an Arrow schema with qualifiers and functional dependencies.
- `.into_parts()`, `DataFrame::new()`, `.into_optimized_plan()`, and `.create_physical_plan()` are valid API paths.
- The page is registered and its single Rust block compiles and executes.

## Validation evidence

- **FAIL — format command:** The corpus-wide script flagged only out-of-scope `Concepts/RevisePlan.md`; the target was not reported.
- **PASS — differential parse gate:** Fresh warning sets matched; `comm -13` was empty.
- **FAIL — current-page parse finish:** `../building-logical-plans.md` remains unresolved.
- **PASS — Rust doctest:** 1 passed, 0 failed.
- **NOT RUN — external links:** required tool is unavailable without installation (`lychee`, exit 127).
- **PASS — markers:** No `TODO:` or `citation-needed` markers.
- **PASS — read-only constraint:** The target remained unchanged.

## Unverified areas

- External URL reachability.
- Cross-page consistency with other Concepts files, excluded by request.

## Recommended next action

- Revise the source-truth blockers first, then rerun the final review gates.

---

## Architectural-dataframe.md Verdict: REVISE

**Mode:** final  
**Structural role:** inferred — whole page; no single subtree role applies  
**Stage:** unspecified  
**Independence:** evidence-only — Independence limitation: no author intent was supplied; this review was evidence-only.
**file:** `docs/source/library-user-guide/dataframe-api/Concepts/architectural-dataframe.md`

## Review boundary

- **Judged:** Entire concept page, technical claims, structure, links, and finish.
- **Not judged:** Other Concepts pages.
- **Consistency boundary:** Page and repository truth.
- **Final-state requirements not yet applicable:** None deferred.

## Blocking findings

### B1 — BLOCKER: The “data flow” diagram reverses DataFusion’s lazy model

- **Location:** `docs/source/library-user-guide/dataframe-api/Concepts/architectural-dataframe.md` §“Data Flow: From Sources to DataFrame” (lines 47–93).
- **Rule:** `markdown.mdc` §4; `markdown-landing.mdc` §2; Judge Gates A and C.
- **Claim:** Data is ingested through `TableProvider` and `SessionContext` into a `DataFrame`, then follows the execution path.
- **Counter-evidence:** A `DataFrame` contains a `LogicalPlan` and `SessionState`, not data. `.collect()` creates a physical plan and triggers computation. `TableProvider::scan()` supplies an execution plan whose streams read data during execution.
- **Sources:** `datafusion/core/src/dataframe/mod.rs` (lines 228–260, 1460–1483); `datafusion/catalog/src/table.rs` (lines 84–110).
- **Impact:** Readers cannot distinguish planning/control flow from runtime `RecordBatch` flow and may expect eager ingestion.
- **Direction:** Relabel this as planning flow or show planning and runtime data flow separately; remove “once ingested” wording.
- **Verification:** Repository source inspected — PASS; target claim — FAIL.

### B2 — BLOCKER: The fit table misstates streaming and provider capabilities

- **Location:** `docs/source/library-user-guide/dataframe-api/Concepts/architectural-dataframe.md` §“Architectural Fit: OLAP vs. OLTP” (lines 110–121).
- **Rule:** `markdown.mdc` §4; Judge Gate A.
- **Claim:** DataFusion has a batch-oriented execution model, no index support, and is designed only for immutable append-only data.
- **Counter-evidence:** DataFusion describes itself as a streaming query engine and supports incremental execution over some unbounded sources. `TableProvider` implementations may use indexes for pushdown and can implement insert, delete, update, and truncate operations.
- **Sources:** `datafusion/execution/src/memory_pool/mod.rs` (lines 43–48); `datafusion-examples/examples/custom_data_source/csv_sql_streaming.rs` (lines 46–65); `datafusion/catalog/src/table.rs` (lines 227–246, 320–380).
- **Impact:** Readers may reject DataFusion for supported use cases or misunderstand which limitations belong to particular providers and query shapes.
- **Direction:** Replace categorical capability claims with provider- and query-dependent limits, including DataFusion’s bounded/unbounded streaming constraints.
- **Verification:** Repository source inspected — PASS; target claims — FAIL.

### B3 — BLOCKER: Unsupported performance thresholds are presented as architectural facts

- **Location:** `docs/source/library-user-guide/dataframe-api/Concepts/architectural-dataframe.md` (lines 24, 37–43, 51–53, 93, 101–121).
- **Rule:** `markdown.mdc` §0 and §4, no-performance-claims invariant; Judge Gate A.
- **Claim:** SQL and DataFrame execution have “the same performance,” planning has a universal `~1–10ms minimum`, and dataset sizes establish suitability.
- **Counter-evidence:** Repository source supports a shared planning/execution pipeline, not universal latency or row-count thresholds. No benchmark context or evidence supports the quantitative claims.
- **Sources:** `datafusion/core/src/lib.rs` (lines 240–270); `datafusion/core/src/execution/session_state.rs` (lines 665–761).
- **Impact:** Architecture guidance becomes hardware-, workload-, and configuration-dependent performance advice presented as invariant.
- **Direction:** Describe execution equivalence and workload characteristics without timing, scale thresholds, or speed guarantees.
- **Verification:** Execution-path source inspected — PASS; quantitative claims — unsupported.

### B4 — MAJOR: The page promises DataFrame API selection guidance but evaluates the engine

- **Location:** `docs/source/library-user-guide/dataframe-api/Concepts/architectural-dataframe.md` title block (lines 20–24) and §“Architectural Fit” (lines 97–121).
- **Rule:** `markdown.mdc` §7.2–§7.4; Judge Gate C.
- **Claim:** The page teaches when the DataFrame API is the right tool.
- **Counter-evidence:** The decision section compares DataFusion as an OLAP engine with databases and stream processors. It does not compare the DataFrame API with SQL or other DataFusion interfaces.
- **Sources:** Target page; `datafusion/core/src/lib.rs` (lines 240–270).
- **Impact:** The reader’s promised decision is never answered.
- **Direction:** Frame the promise consistently as DataFusion engine architecture and OLAP fit; leave API-selection guidance to the linked owner.
- **Verification:** Whole-page narrative inspected — FAIL.

### B5 — BLOCKER: The page emits a Sphinx parse error

- **Location:** `docs/source/library-user-guide/dataframe-api/Concepts/architectural-dataframe.md` line 128.
- **Rule:** `markdown.mdc` §7.4; final Gate D.
- **Claim:** The page is final-form documentation.
- **Counter-evidence:** Sphinx reports `ERROR: Document may not end with a transition`.
- **Sources:** Fresh dummy Sphinx build.
- **Impact:** The source does not parse cleanly.
- **Direction:** Remove the terminal divider and restore a valid closing structure.
- **Verification:** Parse gate — FAIL.

## Non-blocking findings

### N1 — MINOR: Built-in, feature-gated, and ecosystem integrations are blurred

- **Location:** `docs/source/library-user-guide/dataframe-api/Concepts/architectural-dataframe.md` lines 51–53.
- **Direction:** Qualify Avro as feature-gated and distinguish in-tree formats from external JDBC/ODBC, Iceberg, and Delta providers.

### N2 — MINOR: Concept-page framing is incomplete

- **Location:** `docs/source/library-user-guide/dataframe-api/Concepts/architectural-dataframe.md` lines 20–33 and 124–128.
- **Direction:** Supply useful Concepts Covered orientation and apply dividers only before subsequent H2 sections—not before the first H2 or an unheaded closing paragraph.

### N3 — MINOR: The SIGMOD citation is not linked

- **Location:** `docs/source/library-user-guide/dataframe-api/Concepts/architectural-dataframe.md` line 37.
- **Direction:** Define the reference-style citation to the cited paper.

### N4 — MINOR: Final proofreading and visual grammar are unfinished

- **Location:** `docs/source/library-user-guide/dataframe-api/Concepts/architectural-dataframe.md` lines 35, 49, and diagram line 73.
- **Direction:** Correct the missing “is,” “battle-tested,” inappropriate code formatting around “Boring,” `TableProvider` typography, and “API’s” pluralization.

## Open questions

None.

## Verified strengths

- The three design principles match `datafusion/core/src/lib.rs`.
- The `DataFrame` representation as a `LogicalPlan` plus cloned `SessionState` is source-supported.
- SQL and DataFrame queries converge on the same logical and physical planning machinery.
- `TableProvider` predicate-pushdown extensibility is accurately identified.
- The two internal page links resolved without Sphinx cross-reference warnings.

## Validation evidence

- **PASS — target format attribution:** The pinned formatter reported no issue for the target.
- **FAIL — corpus format command:** It exited 1 solely for out-of-scope `Concepts/RevisePlan.md`.
- **FAIL — target parse:** Fresh dummy build emitted the line-128 transition error.
- **NOT APPLICABLE — Rust doctest:** The target contains no visible Rust block.
- **NOT RUN — external link check:** Required tool is unavailable without installation.
- **PASS — markers:** No `TODO:` or `citation-needed` markers.
- **PASS — read-only check:** The target has no worktree diff.

## Unverified areas

- External URLs were not checked by the CI link checker.
- No evidence supports the stated latency and dataset-size thresholds.

## Recommended next action

- Revise B1–B5 from repository truth, then rerun the final gates.

---

## Bigger-picture.md Verdict: REVISE (POTENTIAL OUT-OF-SCOPE)

**Mode:** final  
**Structural role:** inferred — whole page; no single subtree role applies  
**Stage:** unspecified  
**Independence:** evidence-only — Independence limitation: no author intent was supplied; this review was evidence-only.
**file:** `docs/source/library-user-guide/dataframe-api/Concepts/bigger-picture.md`

## Review boundary

- **Judged:** Entire concept page, repository-backed claims, live roadmap issues, structure, links, and finish.
- **Not judged:** Other Concepts pages.
- **Consistency boundary:** Page and repository truth.
- **Final-state requirements not yet applicable:** None deferred.

## Blocking findings

### B1 — BLOCKER: The execution model incorrectly makes every operator demand-driven

- **Location:** `docs/source/library-user-guide/dataframe-api/Concepts/bigger-picture.md` §“Execution Model: Vectorized Volcano” (lines 31–51).
- **Rule:** `markdown.mdc` §4; Judge Gate A.
- **Claim:** Every operator calls `poll_next()` on its children, and DataFrame actions are async specifically to participate in cooperative scheduling rather than block threads.
- **Counter-evidence:** Current `ExecutionPlan` metadata distinguishes demand-driven `Lazy` operators from `Eager` operators that poll inputs independently, spawn tasks, or buffer ahead of demand. `ExecutionPlan::execute()` is synchronous and returns an async stream; cooperative scheduling is a physical-stream concern, with `NonCooperative` the default operator property and optimizer-injected wrappers where needed.
- **Sources:** `datafusion/physical-plan/src/execution_plan.rs` (lines 285–340, 965–1015); `datafusion/physical-optimizer/src/ensure_coop.rs`; `datafusion/physical-plan/src/coop.rs`.
- **Impact:** The central mental model omits exchange boundaries and incorrectly attributes async API design to one scheduling mechanism.
- **Direction:** Explain async streaming with both demand-driven and eager execution boundaries; separate DataFrame awaiting from physical-stream cooperation.
- **Verification:** Current source inspected — PASS; target model — FAIL.

### B2 — MAJOR: Performance tuning displaces the concept model

- **Location:** `docs/source/library-user-guide/dataframe-api/Concepts/bigger-picture.md` §“Execution Model” and final diagram (lines 35–61, 147).
- **Rule:** `markdown.mdc` no-performance-claims invariant; §7.4 altitude; Judge Gates B and D.
- **Claim:** SIMD, cache behavior, `target-cpu=native`, and “significant” filtering/join improvements belong in this architecture concept.
- **Counter-evidence:** The build command is already owned by `docs/source/user-guide/crate-configuration.md`. Its benefit is workload-, compiler-, and hardware-dependent; the local DataFrame documentation contract prohibits performance claims.
- **Sources:** `docs/source/user-guide/crate-configuration.md` (lines 55–84); target page.
- **Impact:** Readers receive procedural build advice and speed promises instead of the scoped execution model.
- **Direction:** Keep only neutral execution mechanics and link to the configuration owner without duplicating its tuning recipe or performance claims.
- **Verification:** Owning documentation inspected — PASS; placement and rule compliance — FAIL.

### B3 — BLOCKER: The page does not parse cleanly

- **Location:** `docs/source/library-user-guide/dataframe-api/Concepts/bigger-picture.md` lines 61 and 153.
- **Rule:** `markdown.mdc` §6 and §7.4; final Gate D.
- **Claim:** The page is final-form documentation.
- **Counter-evidence:** Sphinx reports a missing target for the crate-configuration link and `ERROR: Document may not end with a transition`.
- **Sources:** Fresh dummy Sphinx build.
- **Impact:** Navigation is broken and the document emits a parser error.
- **Direction:** Correct the relative link and remove the terminal divider before reference definitions.
- **Verification:** Parse gate — FAIL.

### B4 — BLOCKER: The roadmap taxonomy is incorrectly attributed to Epic #12723

- **Location:** `docs/source/library-user-guide/dataframe-api/Concepts/bigger-picture.md` §“Future Roadmap” (lines 90–148).
- **Rule:** `markdown.mdc` §4; Judge Gates A and C.
- **Claim:** Epic #12723 distinguishes “frontends” from “interfaces” and supports the diagram placing Python, Flight, Substrait, extension types, and optimizers in those layers.
- **Counter-evidence:** The live epic defines three proposed layers: Frontend, DataFusion main/core, and DataFusion execution. It does not define the page’s “Interfaces” category. Bindings, transport protocols, and plan interchange are distinct roles; the epic also does not place extension types and all custom optimizers inside vectorized execution.
- **Sources:** Live [Epic #12723](https://github.com/apache/datafusion/issues/12723); repository Flight examples, Substrait consumer, extension-type registry, and optimizer source.
- **Impact:** Readers learn an invented architecture while being told it comes from the project roadmap.
- **Direction:** Base the proposed diagram on the epic’s actual frontend/core/execution layers and distinguish bindings, transports, and plan interchange separately.
- **Verification:** Live issue and source inspected — PASS; attribution and diagram — FAIL.

## Non-blocking findings

### N1 — MINOR: The execution-history timeline is inaccurate

- **Location:** `docs/source/library-user-guide/dataframe-api/Concepts/bigger-picture.md` lines 37–43.
- **Direction:** Recognize vectorized Volcano work such as MonetDB/X100 in 2005 and avoid presenting `1000+` rows as a universal vectorized-execution threshold.

### N2 — MINOR: The concept-page opening lacks usable orientation

- **Location:** `docs/source/library-user-guide/dataframe-api/Concepts/bigger-picture.md` lines 20–31 and 90–94.
- **Direction:** Replace the generic title slogan and “Understanding…” abstract with an answer-first thesis, add useful Concepts Covered orientation, and give “Future Roadmap” a section takeaway.

### N3 — MINOR: Final-state drafting residue remains

- **Location:** `docs/source/library-user-guide/dataframe-api/Concepts/bigger-picture.md` line 92 and lines 82–109.
- **Direction:** Resolve and remove the “Risky implementations” comment, then replace presentation-only `<br>` tags with normal Markdown structure.

## Open questions

### Q1 — QUESTION: Are these still the project’s key roadmap initiatives?

- **Location:** `docs/source/library-user-guide/dataframe-api/Concepts/bigger-picture.md` lines 94–105.
- **Resolves it:** Current 2026 roadmap evidence or maintainer confirmation; all three issues remain open, but open state alone does not establish current priority.

## Verified strengths

- The pull-based, partitioned, async `RecordBatch` execution foundation is source-supported.
- Tokio work-stealing and cooperative scheduling are documented DataFusion mechanisms.
- The LLVM ecosystem-role analogy and quotation match the SIGMOD paper.
- Epics #12723, #12644, and #12622 remain open.
- The distinction from LLVM IR and internal JIT compilation survived repository inspection.

## Validation evidence

- **PASS — target format attribution:** The pinned formatter reported no issue for the target.
- **FAIL — corpus format command:** It exited 1 solely for out-of-scope `Concepts/RevisePlan.md`.
- **FAIL — target parse:** Sphinx emitted one missing cross-reference and the terminal-transition error.
- **NOT APPLICABLE — Rust doctest:** The target contains no visible Rust block.
- **NOT RUN — external link check:** Required tool is unavailable without installation.
- **PASS — prescribed markers:** No `TODO:` or `citation-needed` markers.
- **PASS — roadmap issue status:** All three linked epics were retrieved and remain open.
- **PASS — read-only check:** The target has no worktree diff.

## Unverified areas

- External URLs were not checked by the CI link checker.
- Current project priority among the three open epics remains unverified.

## Recommended next action

- Correct B1–B4 from current source and roadmap evidence, then rerun the final gates.

---

## Builder-parser.md Verdict: REVISE

**Mode:** final  
**Structural role:** inferred whole page  
**Stage:** unspecified  
**Independence:** evidence-only — Independence limitation: no author intent was supplied; this review was evidence-only.
**file:** `docs/source/library-user-guide/dataframe-api/Concepts/builder-parser.md`

## Review boundary

- **Judged:** `docs/source/library-user-guide/dataframe-api/Concepts/builder-parser.md`, including technical, cognitive-concept, editorial, and proofreading quality.
- **Not judged:** other Concepts pages, author intent, unrelated corpus defects.
- **Consistency boundary:** page and repository truth.
- **Final-state requirements not yet applicable:** workflow transition and Author approval; no workflow phase was supplied.

## Blocking findings

### B1 — BLOCKER: The security comparison is categorically false

- **Location:** `docs/source/library-user-guide/dataframe-api/Concepts/builder-parser.md` §§“DataFrame API: The Builder Architecture” and “Safety and Security” (lines 50–54, 99–108, 198–211)
- **Rule:** Gate A truth and safety; `markdown.mdc` §§1, 4.
- **Claim:** The DataFrame API never parses text and therefore eliminates injection, while dynamic SQL values require manual sanitization and escaping.
- **Counter-evidence:** `DataFrame::select_exprs()` and `DataFrame::parse_sql_expr()` parse SQL expression strings. Conversely, SQL supports placeholders through `.with_param_values()`. `SessionContext::sql_with_options()` can prohibit DDL, DML, and other statements; unrestricted `sql()` allows them by default.
- **Sources:** `datafusion/core/src/dataframe/mod.rs` lines 352–379, 2327–2390; `datafusion/core/src/execution/context/mod.rs` lines 612–650, 2277–2358.
- **Impact:** Readers may hand-roll escaping, overlook parameter binding, or expose unrestricted user-supplied SQL.
- **Direction:** Scope the advantage to typed `Expr` construction; distinguish parameterized values from arbitrary query text and explain SQL policy controls.
- **Verification:** Source inspection — PASS; doctest security coverage — NOT APPLICABLE.

### B2 — MAJOR: Common representation is overstated as universal identity and performance parity

- **Location:** `docs/source/library-user-guide/dataframe-api/Concepts/builder-parser.md` opening and §§“Introduction,” “LogicalPlan,” “Choosing,” and “In Practice” (lines 22–24, 44–55, 80–145)
- **Rule:** `markdown.mdc` invariant “No performance claims”; Gate A unsupported claims.
- **Claim:** Both APIs always produce the exact same plan, are interchangeable for any task, and have identical performance.
- **Counter-evidence:** Source guarantees a common `LogicalPlan` representation and downstream planning process. Tests establish identity for particular equivalent operations, not a universal identity, feature-parity, or performance contract. The SQL path additionally parses and plans text. No benchmark supports the absolute performance claim.
- **Sources:** `datafusion/expr/src/logical_plan/plan.rs` lines 77–103; `datafusion/core/src/dataframe/mod.rs` lines 165–171; `datafusion/core/tests/dataframe/mod.rs` lines 292–351.
- **Impact:** Readers cannot distinguish shared infrastructure from guaranteed plan identity, feature parity, or measured performance.
- **Direction:** State that both routes produce the same plan type and share downstream optimization/execution; remove structural-identity, interchangeability, and speed absolutes.
- **Verification:** Source inspection — PASS; doctest — PASS but does not inspect plans or performance; benchmark evidence — NOT RUN.

### B3 — BLOCKER: The architecture diagram shows an invalid API call

- **Location:** `docs/source/library-user-guide/dataframe-api/Concepts/builder-parser.md` diagram (lines 66–69)
- **Rule:** Gate A API signature accuracy; `markdown.mdc` §4.
- **Claim:** `ctx.table("t")?` begins the builder path.
- **Counter-evidence:** `SessionContext::table()` is asynchronous and returns `Result<DataFrame>`; it requires `.await?`. The executable example later uses the correct form.
- **Sources:** `datafusion/core/src/execution/context/mod.rs` lines 1983–2001.
- **Impact:** The central diagram teaches non-compiling Rust and hides asynchronous table resolution.
- **Direction:** Show `.await?` and preserve the distinction between the async table lookup and synchronous plan transformations.
- **Verification:** Signature inspection — PASS; diagram accuracy — FAIL; doctest coverage — NOT APPLICABLE.

## Non-blocking findings

### N1 — MINOR: The finalized concept-page shape is incomplete

- **Location:** `builder-parser.md` opening, first H2, and closing (lines 20–43, 214–220)
- **Direction:** Add or justify omitting a useful Concepts Covered table, replace the forbidden “Introduction” heading, and place Further Reading under an appropriate closing structure rather than after a free-standing divider.

### N2 — MINOR: The diagram explanation is a nested callout wall

- **Location:** `builder-parser.md` §“Reading the diagram” (lines 89–116)
- **Direction:** Convert the five consecutive nested admonitions into ordinary explanatory prose or a compact list.

### N3 — MINOR: The result oracle assumes ordering without declaring it

- **Location:** `builder-parser.md` §“In Practice” (lines 158–186)
- **Direction:** Use `assert_batches_sorted_eq!` or introduce explicit ordering.

### N4 — MINOR: `.into_view()` guidance omits the actual registration step

- **Location:** `builder-parser.md` §§“Choosing” and “Single Abstraction Layer” (lines 138–139, 190–194)
- **Direction:** Explain that `.into_view()` returns a `TableProvider` which must be passed to `SessionContext::register_table()`.

### N5 — NIT: Proofreading residue remains

- **Location:** `builder-parser.md` lines 22, 31–33, 52
- **Direction:** Correct “Datafusions,” “Struckt,” “datahandling,” capitalization, and missing punctuation.

## Open questions

None.

## Verified strengths

- Both APIs genuinely create the same `LogicalPlan` type and use the same downstream DataFusion planning process.
- `.filter()` and `.select()` construct plans lazily and return plan-build errors through `Result`.
- The single visible Rust example is registered, executes, and asserts its displayed rows.
- Internal links and MyST syntax produced no target-local parse diagnostics.

## Validation evidence

**FAIL — corpus format command:** `ci/scripts/doc_prettier_check.sh` exited 1 for an unrelated Concepts file.

**PASS — target format localization:** `builder-parser.md` was not reported by the formatter.

**PASS — target parse:** Fresh dummy Sphinx build exited 0 with no warnings or errors for `builder-parser.md`.

**PASS — Rust doctest:** `cargo test --doc -p datafusion dataframe_api_concepts_builder_parser` passed 1/1 visible block.

**NOT RUN — external links:** required tool is unavailable without installation (`lychee`, exit 127).

**PASS — markers:** No `TODO:` or `citation-needed` markers found.

**PASS — read-only constraint:** Target remained unmodified.

## Unverified areas

- External URLs were not checked.
- No evidence supports the page’s absolute performance claims.

## Recommended next action

- Correct B1–B3 from repository truth, then rerun the final gates.

---

## Execution-lifecycle.md Verdict: REVISE

**Mode:** final  
**Structural role:** inferred — whole page; no single subtree role applies  
**Stage:** unspecified  
**Independence:** evidence-only — Independence limitation: no author intent was supplied; this review was evidence-only.
**file:** `docs/source/library-user-guide/dataframe-api/Concepts/execution-lifecycle.md`

## Review boundary

- **Judged:** Entire concept page, its Rust examples, technical claims, structure, links, and finish.
- **Not judged:** Other Concepts pages or cross-page ownership beyond broken link resolution.
- **Consistency boundary:** Page and repository truth.
- **Final-state requirements not yet applicable:** None deferred; workflow phase was not inferred.

## Blocking findings

### B1 — BLOCKER: The page does not parse cleanly and contains broken navigation

- **Location:** `docs/source/library-user-guide/dataframe-api/Concepts/execution-lifecycle.md` throughout; trailing divider line 620.
- **Rule:** `markdown.mdc` §6, §7.4; final Gate D.
- **Claim:** Relative and reference-style links provide working navigation.
- **Counter-evidence:** Both fresh Sphinx runs report one docutils ERROR and ten missing internal targets. The file also contains numerous API-style references without any definitions.
- **Sources:** Target scan; two fresh dummy Sphinx builds.
- **Impact:** The final page renders with dead links and an invalid ending.
- **Direction:** Remove the terminal divider, correct internal paths, and define or replace every unresolved reference-style link.
- **Verification:** Differential parse — PASS because the unchanged warning sets match; current-page finish — FAIL.

### B2 — BLOCKER: Streaming is incorrectly presented as total-memory bounded

- **Location:** Target §“RecordBatches and Partitions” and §“Action methods and batch handling” (lines 329–375), plus lines 203–206 and 565–573.
- **Rule:** `markdown.mdc` §4; Judge Gate A.
- **Claim:** Batch streaming keeps memory bounded regardless of input size, memory remains proportional to `batch_size`, and operators spill whenever limits are exceeded.
- **Counter-evidence:** `.execute_stream()` avoids buffering the final result, but operators can retain state proportional to their input. DataFusion explicitly models plans whose memory may grow unbounded; spilling depends on operator and disk-manager support and may instead return resource exhaustion.
- **Sources:** `datafusion/physical-plan/src/execution_plan.rs` lines 903–923, 1304–1338; `datafusion/core/src/dataframe/mod.rs` lines 1459–1605; physical operator spill implementations.
- **Impact:** Readers may select streaming APIs expecting an OOM guarantee they do not provide.
- **Direction:** Distinguish result buffering from internal operator memory and state spill/error limitations explicitly.
- **Verification:** Source inspection — PASS; doctests — PASS but do not test memory behavior.

### B3 — BLOCKER: `DataFrame::clone()` cost is materially misrepresented

- **Location:** Target §“Ownership vs. Execution” (lines 464–521).
- **Rule:** `markdown.mdc` §4; Judge Gate A.
- **Claim:** A `DataFrame` clone only increments reference counters, costs virtually nothing, and may be used freely.
- **Counter-evidence:** `DataFrame` owns a boxed `SessionState` and a `LogicalPlan`; derived `Clone` clones both. Those structures contain maps, vectors, strings, and plan nodes alongside shared `Arc` values.
- **Sources:** `datafusion/core/src/dataframe/mod.rs` lines 228–232; `datafusion/core/src/execution/session_state.rs` lines 141–210; `datafusion/expr/src/logical_plan/plan.rs` lines 209 onward.
- **Impact:** Readers receive incorrect ownership and cost guidance.
- **Direction:** Describe cloning as duplicating the plan/state snapshot while sharing selected nested resources; remove atomic-operation and “clone freely” guarantees.
- **Verification:** Source inspection — PASS; doctest — PASS but has no cost oracle.

### B4 — BLOCKER: Tokio runtime and cancellation claims exceed the implementation

- **Location:** Target §“The Tokio Async Runtime” (lines 379–460).
- **Rule:** `markdown.mdc` §4; Judge Gate A.
- **Claim:** DataFusion uses separate I/O and CPU runtime pools, every operator cooperatively yields per batch, and Ctrl+C directly cancels queries.
- **Counter-evidence:** `RuntimeEnv` owns memory, disk, cache, and object-store services—not separate Tokio runtimes. Execution uses ambient Tokio tasks and selected blocking tasks. DataFusion documents cancellation through dropping the result stream; Ctrl+C handling belongs to the embedding application.
- **Sources:** `datafusion/execution/src/runtime_env.rs` lines 73–85; `datafusion/physical-plan/src/stream.rs`; `DataFrame::execute_stream()` lines 1596–1604.
- **Impact:** Embedders may design runtime isolation and cancellation around nonexistent guarantees.
- **Direction:** Document ambient-runtime use, scoped blocking work, and cancellation through stream/task ownership.
- **Verification:** Source inspection — PASS; examples — PASS but do not test scheduling or cancellation.

### B5 — BLOCKER: Unsupported performance guarantees violate the documentation invariant

- **Location:** Lines 22–24, 162–165, 219–220, and §“Why the Physical Plan Matters” lines 230–246.
- **Rule:** `markdown.mdc` §0 invariant 1 and §4.
- **Claim:** Laziness produces faster queries, one filter placement is orders of magnitude faster, and particular join algorithms are categorically faster for stated inputs.
- **Counter-evidence:** These outcomes depend on data, statistics, configuration, and plan shape. Join selection is conditional rather than a universal performance ordering.
- **Sources:** `datafusion/core/src/physical_planner.rs` lines 1728–1744; `datafusion/physical-optimizer/src/join_selection.rs` lines 75–84; applicable rule.
- **Impact:** Readers receive unqualified tuning advice and non-portable expectations.
- **Direction:** Describe optimization opportunities and observable plan choices without runtime guarantees.
- **Verification:** Source inspection — PASS; no benchmark evidence supports the claims.

### B6 — MAJOR: The action boundary and execution phases are internally inconsistent

- **Location:** Lines 31–116, 137–148, 183–207, and 302–305.
- **Rule:** `markdown-landing.mdc` §2; `markdown.mdc` §4 and §7.4.
- **Claim:** The page shows the full lifecycle and classifies `.explain()` with `analyze = true` as an eager method.
- **Counter-evidence:** The lifecycle omits the analyzer and later conflates physical planning with physical optimization. `.explain()` always returns another lazy `DataFrame`; execution occurs when that result is collected or shown.
- **Sources:** `datafusion/core/src/execution/session_state.rs` lines 666–760; `datafusion/core/src/physical_planner.rs` lines 269–285; `DataFrame::explain()` lines 1738–1808.
- **Impact:** The page’s central mental model places work on the wrong side of its own boundary.
- **Direction:** Model analysis → logical optimization → physical planning → physical optimization → execution, and classify `.explain()` separately from the action consuming its result.
- **Verification:** Source inspection — PASS.

### B7 — MAJOR: Passing examples provide no semantic oracle

- **Location:** Rust blocks at lines 269–299, 426–443, 498–515, and 532–575.
- **Rule:** `rust-docs.mdc` §3.2 and §5.1; Judge Gate A oracle calibration.
- **Claim:** The examples demonstrate physical-plan inspection, lazy/eager behavior, cloning, and streamed results.
- **Counter-evidence:** All four compile and execute, but none assert rows, plan content, counts, or streamed output. Several use `.show()` or `println!()` as unverified proof.
- **Sources:** Target; targeted doctest result.
- **Impact:** Semantic regressions can pass while the examples continue to appear verified.
- **Direction:** Add stable assertions for the specific concept each block claims to demonstrate.
- **Verification:** Compilation/runtime — PASS; semantic oracle — FAIL.

## Non-blocking findings

### N1 — MINOR: Concept-page opening is incomplete

- **Location:** Lines 20–30.
- **Direction:** Replace the mechanism-heavy abstract with a conclusive page abstract and add the required Concepts Covered orientation and Style Note before the TOC.

### N2 — MINOR: Consecutive nested admonitions form callout walls

- **Location:** Lines 90–118 and 150–176; uppercase class at lines 468–472.
- **Direction:** Flatten repeated callouts into prose or one compact structure and use the defined lowercase admonition classes.

### N3 — MINOR: Conclusion and further-reading material are misplaced

- **Location:** Lines 525–619, plus inline “Further reading” callouts at lines 177–179, 224–228, and 454–460.
- **Direction:** Keep the long example in a content section, provide a short conclusion, and consolidate resources under an optional `### Further Reading`.

### N4 — NIT: Proofreading and typography remain

- **Location:** Lines 35, 164, and 528.
- **Direction:** Replace prose `->` with `→` and correct fragments such as “Needs execution” and “We’ve showed.”

## Open questions

None.

## Verified strengths

- Transformations such as `.filter()` extend the `LogicalPlan`; `.collect()` triggers planning and execution.
- `.collect()` and `.show()` buffer the complete result, while `.execute_stream()` avoids final-result buffering.
- Defaults of 8192 rows for `batch_size` and available CPU parallelism for `target_partitions` match source.
- The page is registered correctly and all four Rust blocks compile and execute.
- The stated minimum optimizer-rule counts are supported by the current default rule lists.

## Validation evidence

- **PASS — format:** `ci/scripts/doc_prettier_check.sh`.
- **PASS — differential parse gate:** Fresh before/after warning sets were identical; `comm -13` was empty.
- **FAIL — current-page parse finish:** One docutils ERROR and ten unresolved internal-reference warnings remain.
- **PASS — Rust doctest:** 4 passed, 0 failed.
- **NOT RUN — external links:** required tool is unavailable without installation (`lychee`, exit 127).
- **PASS — markers:** No `TODO:` or `citation-needed` markers.
- **PASS — read-only constraint:** No target edits were made.

## Unverified areas

- External URL reachability.
- Cross-page consistency with other Concepts files, excluded by request.

## Recommended next action

- Revise the page from repository truth, resolving all blocker findings before another final review.

---

## Expressions.md Verdict: REVISE

**Mode:** final  
**Structural role:** inferred whole page  
**Stage:** unspecified  
**Independence:** evidence-only — Independence limitation: no author intent was supplied; this review was evidence-only.
**file:** `docs/source/library-user-guide/dataframe-api/Concepts/expressions.md`

## Review boundary

- **Judged:** `docs/source/library-user-guide/dataframe-api/Concepts/expressions.md`.
- **Not judged:** other Concepts pages, author intent, unrelated corpus defects.
- **Consistency boundary:** page and repository truth.
- **Final-state requirements not applicable:** workflow transition and Author approval.

## Blocking findings

### B1 — BLOCKER: Logical `Expr` is conflated with executable `PhysicalExpr`

- **Location:** `expressions.md` opening and §§“What is an Expr?” and “Expressions Are Dialect-Agnostic” (lines 22–59, 141–147)
- **Rule:** Gate A truth; `markdown.mdc` §4.
- **Claim:** `Expr` represents row-level logic that DataFusion evaluates directly against Arrow batches.
- **Counter-evidence:** `Expr` is the logical-planning representation. Physical planning converts it to `PhysicalExpr`, which evaluates `RecordBatch`es. Moreover, `Expr` includes aggregate, window, subquery, grouping-set, and parameter variants—not only row-level operations.
- **Sources:** `datafusion/expr/src/expr.rs` lines 90–125, 326–405; `datafusion/physical-expr/src/planner.rs` lines 47–56; `datafusion/physical-expr-common/src/physical_expr.rs` lines 48–88.
- **Impact:** Readers leave with the wrong planning/execution boundary and an incomplete model for aggregate and window expressions.
- **Direction:** Define `Expr` as a logical expression tree, distinguish expression families, and explain conversion to executable physical expressions.
- **Verification:** Source inspection — PASS.

### B2 — MAJOR: `Expr` is incorrectly presented as an input to every transformation

- **Location:** `expressions.md` §“How DataFrame Methods Use Expr” (lines 63–78)
- **Rule:** Gate A API signatures; `markdown.mdc` §4.
- **Claim:** Every transformation method accepts `Expr`.
- **Counter-evidence:** Transformations such as `.limit()`, `.repartition()`, `.drop_columns()`, and `.union()` do not. `.sort()` accepts `Vec<SortExpr>` and `.select()` accepts items convertible to `SelectExpr`.
- **Sources:** `datafusion/core/src/dataframe/mod.rs` lines 410–430, 645–677, 723–731, 1199–1248, 1409–1417.
- **Impact:** Readers cannot distinguish expression-bearing operators from other logical-plan transformations.
- **Direction:** Scope the statement to expression-consuming methods and use their current input types.
- **Verification:** Signature inspection — PASS.

### B3 — BLOCKER: SQL and builder expressions are not necessarily identical

- **Location:** `expressions.md` §“Expressions Are Dialect-Agnostic” (lines 141–147)
- **Rule:** Gate A truth; `markdown.mdc` §4.
- **Claim:** `col("a").gt(lit(10))` creates an expression identical to SQL `a > 10`.
- **Counter-evidence:** Rust infers `lit(10)` as `Int32`; DataFusion initially parses the SQL integer as `Int64`, even with an `Int32` schema. Analysis and coercion may later normalize expressions, but their construction trees are not identical.
- **Sources:** `datafusion/expr/src/expr.rs` lines 237–243; `datafusion/expr/src/literal.rs` line 143; `datafusion/core/src/execution/context/mod.rs` lines 655–675.
- **Impact:** The page obscures literal typing, coercion, and the difference between sharing an IR and constructing identical trees.
- **Direction:** Describe semantic correspondence and possible normalization rather than exact construction identity.
- **Verification:** Source inspection — PASS; target test coverage — NOT APPLICABLE.

### B4 — MAJOR: The example displays output without asserting it

- **Location:** `expressions.md` §“Building Expressions” (lines 85–123)
- **Rule:** `rust-docs.mdc` invariants 1–3 and §3.2; insufficient-oracle calibration.
- **Claim:** The visible table demonstrates the filter and aggregate result.
- **Counter-evidence:** The block calls `.show()` and presents manually written output comments. The doctest executes but never verifies rows, values, or ordering.
- **Sources:** Target block; targeted doctest.
- **Impact:** Incorrect or drifting output can continue passing CI.
- **Direction:** Collect and assert the result, using `assert_batches_sorted_eq!`.
- **Verification:** Doctest execution — PASS; result oracle — FAIL.

## Non-blocking findings

### N1 — MINOR: Final concept-page structure is incomplete

- **Location:** `expressions.md` opening and closing (lines 20–31, 162–174)
- **Direction:** Add or justify omitting Concepts Covered and Style Note elements; make the closing transition a conclusion and place Further Reading beneath it.

### N2 — MINOR: The opening repeats the first H2 and depends on prior reading

- **Location:** `expressions.md` opening and §“What is an Expr?” (lines 22–38)
- **Direction:** Remove “previous section” narration and separate the abstract’s page outcome from the first H2’s definition and organizing model.

### N3 — NIT: “Best practice” uses the wrong admonition class

- **Location:** `expressions.md` §“When Schema Validation Happens” (lines 155–160)
- **Direction:** Use `tip` and a descriptive title.

### N4 — NIT: Visual grammar is inconsistent

- **Location:** `expressions.md` lines 22, 65, 83, 127–136
- **Direction:** Use `.method()`/`function()` notation consistently and avoid labeling methods as functions.

## Open questions

None.

## Verified strengths

- The `BinaryExpr` tree diagram agrees with the enum structure.
- `col()`, `lit()`, aggregation functions, aliases, and logical combinators compile as shown.
- Column names are resolved against schema after raw expression construction.
- Simplification, filter pushdown, and common-subexpression elimination are implemented optimizer behaviors.

## Validation evidence

**FAIL — corpus format command:** `ci/scripts/doc_prettier_check.sh` failed only for an unrelated Concepts file.

**PASS — target format localization:** `expressions.md` was not reported.

**PASS — target parse:** Fresh dummy Sphinx build produced no target-local warnings or errors.

**PASS — Rust doctest execution:** Both targeted runs passed 1/1 visible block.

**FAIL — Rust example oracle:** Displayed output is not asserted.

**NOT RUN — external links:** required tool is unavailable without installation (`lychee`).

**PASS — markers:** No `TODO:` or `citation-needed` markers found.

**PASS — read-only constraint:** Target remained unmodified.

## Unverified areas

- External URLs.
- The example’s displayed rows and ordering.

## Recommended next action

- Correct B1–B4 from repository truth, then rerun the final gates.

---

## Null-handling.md Verdict: REVISE

**Mode:** final  
**Structural role:** inferred whole page  
**Stage:** unspecified  
**Independence:** evidence-only — Independence limitation: no author intent was supplied; this review was evidence-only.
**file:** `docs/source/library-user-guide/dataframe-api/Concepts/null-handling.md`

## Review boundary

- **Judged:** `docs/source/library-user-guide/dataframe-api/Concepts/null-handling.md`.
- **Not judged:** other Concepts pages, author intent, unrelated corpus defects.
- **Consistency boundary:** page and repository truth.
- **Final-state requirements not applicable:** workflow transition and Author approval.

## Blocking findings

### B1 — BLOCKER: The central three-valued-logic rule is false

- **Location:** `null-handling.md` §“The Null Contract” (lines 57–75)
- **Rule:** Gate A truth; `markdown.mdc` §4.
- **Claim:** Unknown combined with anything remains unknown; every logical operation involving NULL propagates NULL.
- **Counter-evidence:** SQL three-valued logic includes `NULL AND FALSE = FALSE` and `NULL OR TRUE = TRUE`. The page’s own table contradicts its highlighting sentence and explanatory paragraph.
- **Sources:** Target truth table; `datafusion/physical-expr/src/expressions/binary.rs` lines 860–960.
- **Impact:** The page’s governing mental model predicts incorrect filter results.
- **Direction:** Replace the propagation absolute with the complete `AND`/`OR` truth rules and distinguish arithmetic/comparison propagation from boolean logic.
- **Verification:** Source and internal-consistency inspection — FAIL.

### B2 — BLOCKER: Schema nullability is misrepresented as validation and a direct physical optimization switch

- **Location:** `null-handling.md` §§“Nullability in the Schema” and “Optimizer Null-Awareness” (lines 82–96, 540–558)
- **Rule:** Gate A truth; `markdown.mdc` §4.
- **Claim:** Declaring `nullable = false` fails fast during plan construction, unlocks statistics pruning, and directly prevents validity-buffer allocation.
- **Counter-evidence:** Planning treats nullability as schema metadata; it does not inspect source values. Reader enforcement, where available, occurs during decoding. `SimplifyExpressions` uses schema nullability, while `PruningPredicate` uses independent file/container statistics. `NullBufferBuilder` omits allocation whenever no null is appended, regardless of field declaration.
- **Sources:** `datafusion/optimizer/src/simplify_expressions/expr_simplifier.rs` lines 1736–1747; `datafusion/pruning/src/pruning_predicate.rs` lines 225–258; Arrow CSV reader lines 331–399; Arrow `NullBufferBuilder` lines 20–49, 65–77.
- **Impact:** Readers may treat a schema declaration as data validation and misunderstand which optimization mechanism it affects.
- **Direction:** Separate schema contracts, read-time validation, expression simplification, statistics pruning, and value-driven bitmap materialization.
- **Verification:** Source inspection — PASS; target claim — FAIL.

### B3 — BLOCKER: Aggregate null handling is overgeneralized

- **Location:** `null-handling.md` §“Null Handling in `.aggregate()`” (lines 190–213)
- **Rule:** Gate A feature behavior; `markdown.mdc` §4.
- **Claim:** DataFusion aggregates ignore NULL values entirely.
- **Counter-evidence:** `COUNT(*)` counts null-bearing rows, as the page later acknowledges. `array_agg()` preserves NULLs by default, and aggregate/window functions can expose configurable null treatment.
- **Sources:** `datafusion/functions-aggregate/src/array_agg.rs` lines 170–224, 2080–2096.
- **Impact:** Readers will infer incorrect behavior for aggregates outside the small numeric set shown.
- **Direction:** Scope the rule to specific aggregates such as `sum()`, `avg()`, `min()`, `max()`, and `count(expr)`, then name exceptions.
- **Verification:** Source inspection — PASS; blanket claim — FAIL.

### B4 — MAJOR: Unsupported performance claims displace the mechanism

- **Location:** `null-handling.md` lines 23, 65, 337–338, 548–555
- **Rule:** `markdown.mdc` invariant “No performance claims.”
- **Claim:** Bitmap operations are SIMD-accelerated, Hash Join is categorically faster, pruning reduces I/O significantly, and schema declarations save CPU cycles.
- **Counter-evidence:** Source confirms optimization mechanisms but supplies no benchmark or universal performance contract. Some claimed causality is also incorrect under B2.
- **Sources:** optimizer, pruning, join-extraction, and Arrow buffer implementations inspected above.
- **Impact:** Readers receive performance guarantees that depend on workload, plan shape, statistics, and implementation.
- **Direction:** Describe observable mechanisms without speed, significance, or hardware-efficiency assertions.
- **Verification:** Mechanism inspection — PASS; benchmark evidence — NOT RUN because none was supplied.

## Non-blocking findings

### N1 — MINOR: The concept page is shaped as a method guide

- **Location:** `null-handling.md` opening and §§“Null Propagation”/“Toolkit” (lines 26–35, 100–537)
- **Direction:** Replace Key Functions with Concepts Covered and retain method examples only where they prove the owned null-semantics model.

### N2 — MINOR: `.sort()` has no implicit null-placement default

- **Location:** `null-handling.md` §“Null Handling in `.sort()`” (lines 352–417)
- **Direction:** Distinguish SQL and `.sort_by()` defaults from `.sort()`, whose `asc` and `nulls_first` arguments are explicit.

### N3 — MINOR: Filtering is repeatedly mislabeled as data loss

- **Location:** `null-handling.md` lines 20–24, 116–128, 263–286
- **Direction:** Describe rows as excluded from a query result; filtering does not mutate or lose source data.

### N4 — MINOR: Key API links target a nonexistent current trait

- **Location:** `null-handling.md` reference definitions (lines 588–589)
- **Direction:** Link `is_null()` and `is_not_null()` to the current inherent `Expr` methods; repository source contains no `ExprFuncExt` trait.

### N5 — NIT: Closing structure continues after the conclusion

- **Location:** `null-handling.md` §“Conclusion” (lines 560–583)
- **Direction:** Consolidate Next Steps and the execution handoff; remove the divider after the final H2.

## Open questions

None.

## Verified strengths

- All six Rust examples compile, execute, and assert visible results.
- Filter, join, sort, union, `CASE`, `DISTINCT`, NaN, and null-safe equality examples match current APIs.
- `ExtractEquijoinPredicate` handles standalone `IS NOT DISTINCT FROM` join predicates with `NullEqualsNull`.
- Union schema nullability correctly widens when any input field is nullable.
- `CASE WHEN` evaluates branch expressions only for selected rows.

## Validation evidence

**FAIL — corpus format command:** `ci/scripts/doc_prettier_check.sh` failed only for an unrelated Concepts file.

**PASS — target format localization:** `null-handling.md` was not reported.

**PASS — target parse:** Fresh dummy Sphinx build produced no target-local warnings or errors.

**PASS — Rust doctests:** 6 passed; 0 failed, matching six visible Rust blocks.

**NOT RUN — external links:** required tool is unavailable without installation (`lychee`).

**PASS — markers:** No `TODO:` or `citation-needed` markers found.

**PASS — read-only constraint:** Target remained unmodified.

## Unverified areas

- External URLs.
- Performance assertions lack benchmark evidence.

## Recommended next action

- Correct B1–B4 from repository truth, then rerun the final gates.

---

## Sessioncontext.md Verdict: REVISE

**Mode:** final  
**Structural role:** inferred — whole page  
**Stage:** unspecified  
**Independence:** Independence limitation: no author intent was supplied; this review was evidence-only.
**file:** `docs/source/library-user-guide/dataframe-api/Concepts/sessioncontext.md`

## Review boundary

- **Judged:** Entire `Concepts/sessioncontext.md`; technical, conceptual, structural, editorial, Rust, formatting, parsing, links, and markers.
- **Not judged:** Other Concepts pages or their content.
- **Consistency boundary:** Repository truth.
- **Final-state requirements not yet applicable:** None.

## Blocking findings

### B1 — BLOCKER: `SessionState` is incorrectly described as immutable and fully frozen

- **Location:** `docs/source/library-user-guide/dataframe-api/Concepts/sessioncontext.md` §“Introduction to SessionContext” (lines 33–50), lines 68 and 98–99.
- **Rule:** `markdown.mdc` §4; truth gate.
- **Claim:** A DataFrame inherits a “frozen snapshot of everything,” and `SessionState` is immutable.
- **Counter-evidence:** `SessionState` exposes mutating configuration, catalog, and function-registry operations. Its catalog and runtime are shared `Arc`s, so the captured environment is explicitly not fully frozen.
- **Sources:** `datafusion/core/src/execution/session_state.rs` lines 141–210, 824–831, 923–930, 2093–2175; `datafusion/core/src/execution/context/mod.rs` lines 2032–2056.
- **Impact:** Readers receive the wrong isolation and reproducibility model.
- **Direction:** Describe an owned `SessionState` clone with independently evolving copy-on-write configuration and registry maps, while explicitly retaining shared catalog/runtime resources.
- **Verification:** Source inspection — PASS.

### B2 — BLOCKER: API table lists nonexistent or misowned methods

- **Location:** `docs/source/library-user-guide/dataframe-api/Concepts/sessioncontext.md` §“The SessionContext API Surface” (lines 60–66).
- **Rule:** `markdown.mdc` §4 and §5.1.
- **Claim:** `SessionContext` exposes `.with_config()` and `.register_table_provider()`.
- **Counter-evidence:** No `SessionContext::with_config` or `SessionContext::register_table_provider` exists. `SessionStateBuilder::with_config()` exists; table providers are registered with `SessionContext::register_table()`.
- **Sources:** Repository-wide Rust symbol search; `execution/context/mod.rs` lines 1932–1947; `execution/session_state.rs` lines 1427–1431.
- **Impact:** Readers cannot find or compile the documented API.
- **Direction:** Replace these entries with methods actually owned by `SessionContext`.
- **Verification:** Source inspection — PASS.

### B3 — BLOCKER: The page fails parse and internal-link validation

- **Location:** `docs/source/library-user-guide/dataframe-api/Concepts/sessioncontext.md` lines 72, 107–108, and 114.
- **Rule:** Final Gate D; `markdown.mdc` §6 and §7.4.
- **Claim:** The page is final and navigable.
- **Counter-evidence:** Sphinx reports four missing cross-reference targets and `ERROR: Document may not end with a transition`.
- **Sources:** Fresh dummy Sphinx build; repository path search.
- **Impact:** Navigation is broken and the document does not parse cleanly.
- **Direction:** Correct the two repeated relative paths and remove the terminal divider.
- **Verification:** Parse — FAIL.

### B4 — BLOCKER: `SessionContext` is overstated as the mandatory single entry point

- **Location:** `docs/source/library-user-guide/dataframe-api/Concepts/sessioncontext.md` lines 20–24.
- **Rule:** `markdown.mdc` §4; truth gate.
- **Claim:** `SessionContext` is the single entry point, required before building any DataFrame.
- **Counter-evidence:** Repository API documentation says most users should use it. `DataFrame::new()` accepts a `SessionState` directly, while `SessionStateBuilder` supports constructing state without a `SessionContext`.
- **Sources:** `execution/context/mod.rs` lines 270–289; `dataframe/mod.rs` lines 251–262; `execution/session_state.rs` lines 100–140.
- **Impact:** Advanced and embedding use cases are incorrectly excluded.
- **Direction:** Call it the primary high-level entry point, not the only possible entry point.
- **Verification:** Source inspection — PASS.

### B5 — BLOCKER: Both APIs do not immediately produce “the same optimized plan”

- **Location:** `docs/source/library-user-guide/dataframe-api/Concepts/sessioncontext.md` lines 112–113.
- **Rule:** `markdown.mdc` §4.
- **Claim:** SQL and the DataFrame builder both produce the same optimized plan.
- **Counter-evidence:** `SessionContext::sql()` creates a logical plan and returns a DataFrame; optimization occurs later. Source documentation promises the same planning and execution process, not necessarily identical optimized-plan structure.
- **Sources:** `execution/context/mod.rs` lines 612–650, 680–775; `dataframe/mod.rs` lines 165–170.
- **Impact:** Readers conflate API equivalence with plan identity and optimization timing.
- **Direction:** State that both interfaces feed DataFusion’s shared planning, optimization, and execution pipeline.
- **Verification:** Source inspection — PASS.

## Non-blocking findings

### N1 — MINOR: The concept page drifts into an action/API guide

- **Location:** `Concepts/sessioncontext.md` §§“The SessionContext API Surface” and “Creating and Configuring SessionContext” (lines 54–99).
- **Direction:** Center the owned context/state mental model; keep method coverage subordinate or route it to action documentation.

### N2 — MINOR: Final concept-page structure is incomplete

- **Location:** `Concepts/sessioncontext.md` lines 31–33 and 101–113.
- **Direction:** Replace the generic “Introduction” on-ramp, resolve abstract repetition, and place the closing synthesis under a proper conclusion instead of after “References.”

### N3 — MINOR: API references lack exact docs.rs targets

- **Location:** `Concepts/sessioncontext.md` lines 60–66 and 103–105.
- **Direction:** Link valid API symbols to their exact current docs.rs anchors using reference-style definitions.

## Open questions

None.

## Verified strengths

- `SessionContext::state()` does clone state for individual queries.
- Function-registry maps are cloned while catalog and runtime handles remain shared.
- The configuration example uses valid constructors and configuration methods.
- The single visible Rust block is registered and executes successfully.

## Validation evidence

**FAIL — corpus formatting command:** `ci/scripts/doc_prettier_check.sh` exited 1 for two unrelated plan files.

**PASS — target formatting:** `sessioncontext.md` was not reported by the pinned formatter.

**FAIL — target parse:** Four unresolved internal links and one terminal-transition error.

**PASS — Rust doctest:** 1 passed; 0 failed.

**NOT RUN — external links:** Required tool is unavailable without installation (`lychee`).

**PASS — markers:** No `TODO:` or `citation-needed` markers.

## Unverified areas

- External URL reachability could not be checked.

## Recommended next action

- Correct B1–B5 from repository truth, address N1–N3, then rerun the final gates.
