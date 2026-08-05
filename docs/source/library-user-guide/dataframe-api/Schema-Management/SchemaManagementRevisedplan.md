## Schema-concepts.md Verdict: REVISE

**Mode:** final  
**Structural role:** inferred whole page  
**Stage:** unspecified  
**Independence:** evidence-only — Independence limitation: no author intent was supplied; this review was evidence-only.
**file:** `docs/source/library-user-guide/dataframe-api/Schema-Management/schema-concepts.md`

## Review boundary

- **Judged:** the complete cognitive concept page, its local links, finish, and technical claims against repository source.
- **Not judged:** other documentation pages, workflow-phase ownership, or external URL contents.
- **Consistency boundary:** repository truth.
- **Final-state requirements not yet applicable:** Author approval and phase-specific ownership only.

## Blocking findings

### B1 — MAJOR: Validation timing is overstated

- **Location:** `docs/source/library-user-guide/dataframe-api/Schema-Management/schema-concepts.md` §“Contract Violations,” §“Expression Validation,” and §“Schema Propagation” (lines 22–24, 97–109, 171–198, 319–347)
- **Rule:** Gate A; `markdown.mdc` §4.
- **Claim:** structural and type errors are universally caught while constructing each plan node, before execution, with no later schema checks.
- **Counter-evidence:** `Filter::try_new_internal` explicitly performs best-effort checking and ignores unresolved-expression errors; `.union()` constructs a loose-typed plan; `TypeCoercion` subsequently rewrites expressions and recomputes schemas during analysis.
- **Sources:** `datafusion/expr/src/logical_plan/plan.rs`; `datafusion/expr/src/logical_plan/builder.rs`; `datafusion/optimizer/src/analyzer/type_coercion.rs`; `datafusion/core/src/dataframe/mod.rs`.
- **Impact:** readers receive incorrect expectations about when errors surface and where error handling belongs.
- **Direction:** distinguish construction-time normalization, analyzer validation/coercion, physical planning, and runtime stream/schema enforcement.
- **Verification:** source inspection — PASS; doctest — NOT APPLICABLE.

### B2 — MAJOR: Type coercion is incorrectly presented as lossless widening

- **Location:** target §“Type Coercion at a Glance” (lines 184–198)
- **Rule:** Gate A; `markdown.mdc` §4.
- **Claim:** coercion prevents data loss, always widens, never narrows, and set operations use strict matching.
- **Counter-evidence:** numeric coercion selects `Float32` whenever either operand is `Float32`, including combinations with `Int64`, which can lose precision. Unions are initially loose-typed and later coerced to a common type; numeric/string unions can become strings.
- **Sources:** `datafusion/expr-common/src/type_coercion/binary.rs` (lines 1487–1513); `datafusion/expr/src/logical_plan/builder.rs`; `datafusion/optimizer/src/analyzer/type_coercion.rs`.
- **Impact:** readers may rely on a nonexistent losslessness guarantee and overlook precision changes.
- **Direction:** describe common-type coercion without promising losslessness; distinguish expression, comparison, join, and union rules.
- **Verification:** source inspection — PASS.

### B3 — MAJOR: Metadata preservation guarantee is false

- **Location:** target §“What the Contract Contains” (lines 86–91)
- **Rule:** Gate A; `markdown.mdc` §4.
- **Claim:** DataFusion preserves secondary metadata throughout processing.
- **Counter-evidence:** binary and Boolean expressions produce fields with empty metadata; scalar and aggregate functions default to empty output metadata. Union metadata is also intersected rather than universally preserved.
- **Sources:** `datafusion/expr/src/expr_schema.rs` (lines 450–461, 520–537); `datafusion/expr/src/logical_plan/plan.rs`.
- **Impact:** users may incorrectly depend on units, PII labels, or lineage metadata surviving transformations.
- **Direction:** document operation-specific propagation and state that arbitrary transformations do not guarantee preservation.
- **Verification:** source inspection — PASS.

### B4 — MAJOR: Logical and physical schema layers are conflated

- **Location:** target §“Logical vs Physical Schema” (lines 353–365)
- **Rule:** Gate A; `markdown-landing.mdc` §2 concept-page mental-model invariant.
- **Claim:** physical batches may invisibly use dictionary types, changed nullability, or implicit casts while still matching the logical schema’s names and types.
- **Counter-evidence:** each `ExecutionPlan` exposes an Arrow `SchemaRef`, and every returned `RecordBatch` must match its stream schema. Dictionary encoding is represented by `DataType::Dictionary`; analyzer-inserted casts recompute the logical schema before physical planning.
- **Sources:** `datafusion/physical-plan/src/execution_plan.rs`; `datafusion/execution/src/stream.rs`; `datafusion/optimizer/src/analyzer/type_coercion.rs`.
- **Impact:** the page’s concluding model is internally contradictory and obscures the distinction between `DFSchema`, Arrow schema, analyzed plans, and array buffers.
- **Direction:** separate qualifiers, Arrow data types, analysis-time schema changes, execution-plan schemas, and physical buffer representation.
- **Verification:** source inspection — PASS.

### B5 — MAJOR: The source-to-schema model is not universal

- **Location:** target §“How the Initial Schema is Determined” and §“Schema Ownership” (lines 205–239)
- **Rule:** Gate A; `markdown-landing.mdc` §2.
- **Claim:** every query and schema originates from a `TableProvider`; self-describing formats require no schema inference and are known instantly.
- **Counter-evidence:** `Values` and `EmptyRelation` construct `DFSchema` directly, and `DataFrame::new` accepts any `LogicalPlan`. Parquet’s `infer_schema()` fetches file metadata and merges schemas across files.
- **Sources:** `datafusion/expr/src/logical_plan/builder.rs`; `datafusion/core/src/dataframe/mod.rs`; `datafusion/datasource-parquet/src/file_format.rs`.
- **Impact:** source-less queries, custom plans, and metadata-based file inference do not fit the page’s central ownership model.
- **Direction:** scope the diagram to scan-backed DataFrames, add the non-scan origin, and distinguish metadata inference from row sampling.
- **Verification:** source inspection — PASS.

## Non-blocking findings

### N1 — MINOR: Four local links do not resolve

- **Location:** target lines 31–36, 66, and 313.
- **Direction:** use the actual MyST anchors or explicit labels for the em-dash headings; rerun Sphinx.

### N2 — MINOR: Required local contents navigation is missing

- **Location:** target opening block (lines 20–40).
- **Direction:** add the leaf/concept-page `{contents}` directive; the Concepts Covered table is orientation, not its replacement.

### N3 — MINOR: `DFSchema` is called immutable without qualification

- **Location:** target §“Schema Immutability” (lines 167–169).
- **Direction:** describe plan schemas as immutable once embedded; `DFSchema` itself exposes `merge(&mut self, ...)`.

### N4 — MINOR: Functional dependencies are attributed an unsupported ordering role

- **Location:** target lines 81–82 and 147.
- **Direction:** name verified uses such as GROUP BY reduction, distinct simplification, and join elimination; remove “ordering” unless supported.

### N5 — NIT: H2 divider placement violates the page stencil

- **Location:** target lines 40 and 411.
- **Direction:** remove the divider before the first H2 and after the final H2.

### N6 — NIT: The conclusion repeats the orientation inventory

- **Location:** target §“Conclusion” (lines 369–409).
- **Direction:** reduce it to a compact recap and transition; avoid two overlapping related-reading blocks.

### N7 — NIT: Proofreading defects remain

- **Location:** target lines 25, 32, and 150.
- **Direction:** correct the broken parallelism, “Schema an overlapping terminology,” and the “Accessed via…” fragment.

## Open questions

None.

## Verified strengths

- The page owns a substantive schema mental model rather than functioning only as a router.
- `DFSchema` structure and the `.inner()`, `.as_arrow()`, and `DataFrame::schema()` signatures are documented correctly.
- Outer-join nullability behavior matches `build_join_schema()`.
- The CSV/JSON default inference limit of 1,000 records is correct.

## Validation evidence

**PASS — source/API inspection:** material claims checked against DataFusion source.

**FAIL — target parse:** fresh dummy Sphinx build reported four `myst.xref_missing` warnings in `schema-concepts.md`.

**FAIL — corpus format gate:** `ci/scripts/doc_prettier_check.sh` exited 1 for two unrelated plan files; the target was not reported.

**PASS — markers:** no `TODO:` or `citation-needed` markers.

**NOT APPLICABLE — Rust doctest:** the page contains no Rust blocks.

**NOT RUN — external links:** required tool is unavailable without installation (`lychee` exit 127).

## Unverified areas

- External URL availability.
- Cross-page consistency with unnamed neighboring documentation.
- No before/after parse differential was applicable because the target has no working-tree change; the current page was parsed directly.

## Recommended next action

- Return the page for a truth-first revision of B1–B5, then repair N1 and rerun the final validation gates.

---

## Schema-anatomy.md Verdict: REVISE

**Mode:** final  
**Structural role:** inferred whole page  
**Stage:** unspecified  
**Independence:** evidence-only — Independence limitation: no author intent was supplied; this review was evidence-only.
**file:** `docs/source/library-user-guide/dataframe-api/Schema-Management/schema-anatomy.md`

## Review boundary

- **Judged:** complete concept page, Rust example, local links, structure, and technical claims against source.
- **Not judged:** other Concepts pages, unnamed neighbors, external URL contents, or workflow-phase ownership.
- **Consistency boundary:** repository truth.
- **Final-state requirements not yet applicable:** Author approval and phase-specific ownership only.

## Blocking findings

### B1 — MAJOR: Displayed schema output lacks a sufficient test oracle

- **Location:** `docs/source/library-user-guide/dataframe-api/Schema-Management/schema-anatomy.md` §“Schema in Practice” (lines 83–153)
- **Rule:** `rust-docs.mdc` Invariant 1, §§3.2–3.3; oracle calibration.
- **Claim:** the two text blocks show the example’s exact `DFSchema` and Arrow schema output.
- **Counter-evidence:** the doctest asserts only field count and one data type. Names, nullability, metadata, qualifiers, and both displayed representations can change without failing.
- **Sources:** target Rust block; targeted doctest.
- **Impact:** passing tests do not verify most of the evidence readers are shown.
- **Direction:** assert the displayed schema properties/output or stop presenting unasserted exact output.
- **Verification:** doctest runtime — PASS; output oracle — FAIL.

### B2 — MAJOR: Qualifier semantics are materially incorrect

- **Location:** target §“Table Qualifiers” (lines 177–187)
- **Rule:** Gate A; `markdown-landing.mdc` §2.
- **Claim:** every field carries a `TableReference`; `Some` identifies a registered table and `None` identifies a computed expression.
- **Counter-evidence:** `DFSchema` stores `Option<TableReference>`. Unqualified source fields are also `None`; the page’s own `dataframe!` example demonstrates this. A qualifier can come from a scan name or alias without catalog registration.
- **Sources:** `datafusion/common/src/dfschema.rs`; `datafusion/expr/src/logical_plan/plan.rs`; target example.
- **Impact:** readers cannot correctly reason about unqualified sources, aliases, or ambiguity resolution.
- **Direction:** define qualifiers as optional relation context, independent of catalog registration or whether an expression is computed.
- **Verification:** source inspection — PASS.

### B3 — MAJOR: Functional-dependency propagation and example are false

- **Location:** target §“Functional Dependencies” (lines 189–203)
- **Rule:** Gate A; `markdown.mdc` §4.
- **Claim:** dependencies propagate through every plan node, and a three-column primary key produces `target_indices: [1, 2]`.
- **Counter-evidence:** union construction explicitly discards functional dependencies. Constraint conversion targets the entire schema, including the determinant, so the example would use `[0, 1, 2]`.
- **Sources:** `datafusion/common/src/functional_dependencies.rs` (lines 198–230); `datafusion/expr/src/logical_plan/plan.rs` (lines 3315–3322, 3393–3399).
- **Impact:** the page gives readers an incorrect internal representation and preservation guarantee.
- **Direction:** explain operation-specific recalculation, preservation, and loss; correct the example to match source.
- **Verification:** source inspection — PASS.

### B4 — MAJOR: Metadata preservation is overstated

- **Location:** target §“Arrow Field Properties” and §“Metadata” (lines 290–295, 349–354)
- **Rule:** Gate A; `markdown.mdc` §4.
- **Claim:** DataFusion preserves metadata through projections and aggregations.
- **Counter-evidence:** binary and Boolean expressions produce empty field metadata; scalar and aggregate functions default to empty output metadata. Only particular projection forms preserve it.
- **Sources:** `datafusion/expr/src/expr_schema.rs` (lines 452–462, 480–537).
- **Impact:** governance metadata such as units, lineage, and PII labels may disappear despite the page’s guarantee.
- **Direction:** describe preservation as expression- and operation-specific.
- **Verification:** source inspection — PASS.

### B5 — MAJOR: Data-type support and coercion are overclaimed

- **Location:** target §“Data Type” and §“Nested Types” (lines 313–330)
- **Rule:** Gate A; `markdown.mdc` §4.
- **Claim:** DataFusion fully supports Arrow nested types and only applies safe widening.
- **Counter-evidence:** nested types are unsupported by operations such as `IN`; `Union` is not hashable and cannot be converted by the SQL unparser. Numeric coercion can combine `Int64` with `Float32` as `Float32`, which may lose precision.
- **Sources:** `datafusion/physical-expr/src/expressions/in_list.rs`; `datafusion/expr/src/utils.rs`; `datafusion/sql/src/unparser/expr.rs`; `datafusion/expr-common/src/type_coercion/binary.rs`.
- **Impact:** readers may design unsupported expressions or rely on nonexistent losslessness guarantees.
- **Direction:** state that nested-type support varies by operation and describe common-type coercion without promising safety or losslessness.
- **Verification:** source inspection — PASS.

### B6 — MAJOR: The opening repeats instead of orienting

- **Location:** target opening through §“DFSchema Components” (lines 20–175)
- **Rule:** `markdown-landing.mdc` §2.1; `markdown.mdc` §§7.2–7.4.
- **Claim:** the page follows an outside-in anatomy.
- **Counter-evidence:** the abstract, first H2, and second H2 repeat the same bridge/components thesis. The first H2 then expands into a long inspection example before the nearly identical “DFSchema Components” section. The required Concepts Covered orientation table is absent.
- **Sources:** target structure and applicable concept-page rules.
- **Impact:** readers traverse roughly 150 lines before the page establishes distinct conceptual layers.
- **Direction:** make the first H2 a short on-ramp, retain component detail once, and restore concept orientation.
- **Verification:** structural review — FAIL.

### B7 — MAJOR: Arrow schema is conflated with exact physical layout

- **Location:** target title thesis, §“Arrow Schema,” §“Nullability,” and conclusion (lines 20–24, 208–226, 332–346, 392–396)
- **Rule:** Gate A; concept-page mental-model invariant.
- **Claim:** Arrow `Schema` defines exact physical layout and its nullable flag controls validity bitmaps.
- **Counter-evidence:** `Schema` describes fields and constraints; `RecordBatch` separately owns arrays and buffers. A nullable field can contain no nulls, while non-nullability prohibits null-containing arrays—it does not determine whether a bitmap exists.
- **Sources:** locked `arrow-array` 59.0.0 `record_batch.rs`; `datafusion/common/src/dfschema.rs`.
- **Impact:** the page’s central model confuses schema contracts with array/buffer representation.
- **Direction:** describe Arrow schema as the typed field contract and reserve physical-layout claims for arrays and buffers.
- **Verification:** API/source inspection — PASS.

## Non-blocking findings

### N1 — MINOR: Two target-local links do not resolve

- **Location:** target lines 187 and 230.
- **Direction:** repair the `aligning-qualifiers` and `dfschema-the-query-planning-layer` anchors confirmed missing by Sphinx.

### N2 — MINOR: Renames are incorrectly described as resilient

- **Location:** target §“Field Order” (lines 241–256).
- **Direction:** remove renamed columns from the resilience claim or require an explicit rename/mapping step.

### N3 — MINOR: Unsupported admonition class

- **Location:** target lines 125–153.
- **Direction:** replace both `information` classes with an allowed admonition class.

### N4 — MINOR: Join field-count description is misleading

- **Location:** target §“Field Count” (lines 259–266).
- **Direction:** describe output by join type; ordinary `ON` joins do not generally remove join-key fields.

## Open questions

None.

## Verified strengths

- The Rust example compiles and executes successfully.
- `DFSchema`’s three stored components and `.inner()`/`.as_arrow()` signatures match source.
- Table constraints are converted into functional dependencies during `TableScan` construction.
- Positional `.union()` requires equal field counts; `.union_by_name()` fills absent columns with NULL.
- Local contents navigation and H2 divider placement conform.

## Validation evidence

**PASS — source/API inspection:** material claims checked against repository and locked Arrow source.

**PASS — Rust doctest:** one visible Rust block passed; both normal and diagnostic `--nocapture` runs succeeded.

**FAIL — target parse:** fresh dummy Sphinx build found two unresolved target-local links.

**FAIL — corpus format gate:** three unrelated planning files failed; `schema-anatomy.md` was not reported.

**PASS — markers:** no `TODO:` or `citation-needed` markers.

**NOT RUN — external links:** required tool is unavailable without installation.

## Unverified areas

- External URL availability.
- Incoming links from unnamed neighboring pages were excluded from judgment.
- Exact displayed schema output remains unverified by the doctest.
- No before/after parse differential applied because the target has no working-tree change.

## Recommended next action

- Return the page for a truth-first revision of B2–B7 and strengthen the B1 oracle before another final review.

---

## Type-coercion.md Verdict: REVISE

**Mode:** final  
**Structural role:** inferred whole page  
**Stage:** unspecified  
**Independence:** evidence-only — Independence limitation: no author intent was supplied; this review was evidence-only.
**file:** `docs/source/library-user-guide/dataframe-api/Schema-Management/type-coercion.md`

## Review boundary

- **Judged:** complete concept page, Rust examples, registration, local links, structure, and technical claims against repository source.
- **Not judged:** other Concepts pages, unnamed neighbors, or external URL contents.
- **Consistency boundary:** repository truth.
- **Final-state requirements not yet applicable:** None; final-state requirements were applied.

## Blocking findings

### B1 — BLOCKER: Doctest registration has incorrect path casing

- **Location:** `datafusion/core/src/lib.rs` (lines 1407–1411); target path.
- **Rule:** `rust-docs.mdc` Invariant 3 and §8.
- **Claim:** the registration makes this page available to `cargo test --doc`.
- **Counter-evidence:** registration uses `schema-management/type-coercion.md`; Git tracks `Schema-Management/type-coercion.md`. The local pass relies on macOS’s case-insensitive filesystem.
- **Sources:** registration source; `git ls-files`; targeted doctest.
- **Impact:** case-sensitive CI cannot resolve the file and the doctest build fails.
- **Direction:** make the registration path exactly match the tracked path.
- **Verification:** local doctest — PASS; portable registration — FAIL.

### B2 — MAJOR: The coercion hierarchy promises false safety and result types

- **Location:** target §“Type Coercion,” §“The Coercion Hierarchy,” and §“Data Type Interaction Rules” (lines 79–193).
- **Rule:** Gate A; `markdown.mdc` Invariants 1–2 and §4.
- **Claim:** coercion always widens losslessly, never narrows, and `Decimal128 + Float__` always produces `Float64`.
- **Counter-evidence:** `Int64 + Float32` resolves to `Float32`, losing integer precision. Decimal arithmetic follows the encountered float width—`Float16`/`Float32` do not automatically become `Float64`. Converting a string array to a view array also constructs one view per element, so the claimed O(1) conversion is false.
- **Sources:** `datafusion/expr-common/src/type_coercion/binary.rs`; Arrow 59 `byte_view_array.rs`.
- **Impact:** readers predict incorrect schemas, precision guarantees, and conversion costs.
- **Direction:** replace the universal hierarchy with context- and operator-specific rules, explicitly identifying lossy paths.
- **Verification:** source inspection — PASS.

### B3 — MAJOR: String/numeric comparison coercion is reversed

- **Location:** target §“Strings” and “Comparison coercion has two variants” (lines 176–199).
- **Rule:** Gate A; `markdown.mdc` §4.
- **Claim:** binary comparisons coerce numeric values to strings, making `"12" < "9"` true; `comparison_coercion_numeric()` provides numeric semantics.
- **Counter-evidence:** current `comparison_coercion()` prefers the numeric type. `type_union_coercion()` prefers strings for union-like contexts. No `comparison_coercion_numeric()` function exists.
- **Sources:** `datafusion/expr-common/src/type_coercion/binary.rs` (lines 874–972); repository symbol search.
- **Impact:** documented comparison results and debugging guidance are wrong.
- **Direction:** document the current comparison/type-union distinction and remove the stale API reference.
- **Verification:** source inspection — PASS.

### B4 — MAJOR: Plan rewriting is conflated with runtime conversion

- **Location:** target §“Type Coercion” and §“Hard Cast — cast()” (lines 82–97, 336–341).
- **Rule:** Gate A; concept-page mental-model invariant.
- **Claim:** coercion and explicit casts operate only at plan level, not during row processing.
- **Counter-evidence:** the analyzer inserts cast expressions into the plan, but physical `CastExpr` and `TryCastExpr` evaluate Arrow cast kernels during execution for each batch.
- **Sources:** `datafusion/optimizer/src/analyzer/type_coercion.rs`; `datafusion/physical-expr/src/expressions/{cast,try_cast}.rs`.
- **Impact:** readers receive the wrong model for execution cost and when cast failures occur.
- **Direction:** distinguish plan-time cast insertion from vectorized runtime conversion.
- **Verification:** source inspection — PASS.

### B5 — MAJOR: Passing doctests do not verify the demonstrated coercion evidence

- **Location:** target §§“Automatic Widening in Expressions,” “Coercion in Set Operations,” and “Literal Coercion” (lines 203–319).
- **Rule:** `rust-docs.mdc` Invariant 1 and §§3.1–3.2; oracle calibration.
- **Claim:** examples prove analyzer-inserted casts, exact plan text, and the displayed union error.
- **Counter-evidence:** the first assertion examines the pre-analysis schema and therefore proves inferred result type, not an inserted cast. Two result assertions are hidden. No example asserts the claimed `EXPLAIN` text, and the union example checks only `is_err()` rather than its displayed error.
- **Sources:** target blocks; `DataFrame::schema()`; `ExprSchemable`; targeted doctest.
- **Impact:** all five tests can pass while central explanatory claims drift or become false.
- **Direction:** expose result assertions and directly assert every displayed plan or error claim.
- **Verification:** doctest runtime — PASS; evidence oracle — FAIL.

### B6 — MAJOR: “Full” nested-type support is false

- **Location:** target §“Arrow Data Types in DataFusion” (lines 48–76).
- **Rule:** Gate A; `markdown.mdc` §4.
- **Claim:** DataFusion fully supports Arrow `List`, `Struct`, `Map`, and `Union` types.
- **Counter-evidence:** nested types are unsupported by `IN`; `Union` cannot be used by hash-based operations, and support varies across operators.
- **Sources:** `datafusion/physical-expr/src/expressions/in_list.rs`; `datafusion/expr/src/utils.rs`.
- **Impact:** readers may design expressions and joins around unsupported combinations.
- **Direction:** describe nested-type support as operation-specific and name important limits.
- **Verification:** source inspection — PASS.

## Non-blocking findings

### N1 — MINOR: Three internal links do not resolve

- **Location:** target lines 54, 67, and 424.
- **Direction:** correct both SQL Data Types paths and use a resolvable Anatomy of a Schema anchor.

### N2 — MINOR: Required H2 dividers are absent

- **Location:** before §“Type Coercion,” §“Explicit Casting,” and §“Conclusion.”
- **Direction:** add the three prescribed `---` dividers.

### N3 — MINOR: Concept-page orientation is missing

- **Location:** target opening through the first H2 (lines 20–78).
- **Direction:** add a Concepts Covered table and make the detailed type catalog follow a short topic on-ramp.
- **AUTHOR COMMENT:** Then the TOC is conflicting

### N4 — NIT: Style Note contains unrelated notation

- **Location:** target §“Style Note” (lines 27–41).
- **Direction:** remove unused `DFSchema` rows and retain only conventions exercised by this page.
- **AUTHOR COMMENT:** this is fine keep the style consistant

## Open questions

None.

## Verified strengths

- All five Rust blocks compile and execute locally.
- Analyzer execution during `SessionState::optimize()` and `.schema()` returning the unoptimized plan schema match source.
- Union and join-key coercion are implemented by the analyzer.
- The visible `cast()` and `try_cast()` examples assert their results correctly.
- NULL arithmetic defaulting to `Int64` matches current coercion logic.

## Validation evidence

**PASS — source/API inspection:** material claims checked against current repository and Arrow 59 source.

**PASS — targeted Rust doctest:** `cargo test --doc -p datafusion dataframe_api_schema_management_type_coercion` passed 5/5 tests.

**FAIL — doctest registration path:** tracked and registered path casing differs.

**FAIL — target parse:** fresh dummy Sphinx build reported three unresolved target-local links.

**FAIL — corpus format gate:** three unrelated planning files failed; the target was not reported.

**PASS — markers:** no `TODO:` or `citation-needed` markers.

**NOT RUN — external links:** required tool is unavailable without installation.

## Unverified areas

- External URL availability.
- Exact displayed `EXPLAIN` and error text.
- A case-sensitive CI run; the registration mismatch was established by tracked-path inspection.
- Incoming links from unnamed neighboring pages.

## Recommended next action

- Return the page for a truth-first revision of B2–B6, fix the B1 registration casing, and strengthen the example oracles before another final review.

---

## Schema-creation.md Verdict: REVISE

**Mode:** final  
**Structural role:** inferred whole page  
**Stage:** unspecified  
**Independence:** limited — Independence limitation: no author intent was supplied; this review was evidence-only.
**file:** `docs/source/library-user-guide/dataframe-api/Schema-Management/schema-creation.md`

## Review boundary

- **Judged:** Entire `Schema-Management/schema-creation.md`, its six Rust blocks, links, registration, structure, and technical claims.
- **Not judged:** Other Concepts files or broader documentation consistency.
- **Classification:** Concept-led mixed leaf, based on its action-heavy construction content.
- **Consistency boundary:** repository truth.
- **Final-state requirements not yet applicable:** None.

## Blocking findings

### B1 — BLOCKER: Doctest registration is not portable

- **Location:** `datafusion/core/src/lib.rs` lines 1401–1405.
- **Rule:** `rust-docs.mdc` §8; final Rust gate.
- **Claim:** The page is registered for doctesting.
- **Counter-evidence:** Registration uses lowercase `schema-management`, while the tracked directory is `Schema-Management`.
- **Sources:** Target path; `datafusion/core/src/lib.rs`.
- **Impact:** It passes on case-insensitive macOS but cannot resolve on case-sensitive CI filesystems.
- **Direction:** Correct the registration path casing separately.
- **Verification:** Local doctest PASS; portable registration FAIL.

### B2 — MAJOR: Explicit schemas are credited with guarantees they do not provide

- **Location:** `schema-creation.md` §“The Case for Explicit Definition” lines 122–147 and §“Conclusion” lines 534–540.
- **Rule:** `markdown.mdc` §4.
- **Claim:** Explicit schemas reject source drift at plan time, control Parquet dictionary encoding, and prevent the listed inference failures.
- **Counter-evidence:** Specified file schemas are attached while planning, but CSV/JSON parsing and mismatch errors occur during execution. Parquet dictionary encoding is configured through writer properties, not the Arrow schema. CSV headers also prevent sparse named fields from simply being “missed” as claimed.
- **Sources:** `execution/context/mod.rs` lines 1743–1757; `datasource-csv/src/source.rs` lines 397–438; `core/src/datasource/file_format/mod.rs` lines 119–130; `parquet_writer.rs` lines 132–139; `datasource-csv/src/file_format.rs` lines 570–665.
- **Impact:** Readers place validation and error handling at the wrong lifecycle point and use the wrong control for output encoding.
- **Direction:** Separate schema declaration, runtime data validation, format-specific inference, and writer configuration.
- **Verification:** Repository source inspected — FAIL claim.

### B3 — MAJOR: Decimal rules and rejection behavior are inaccurate

- **Location:** `schema-creation.md` §“Decimals: Precision and Scale” lines 219–251.
- **Rule:** `markdown.mdc` §4; `rust-docs.mdc` §3.
- **Claim:** Scale must be non-negative and values exceeding precision or scale are rejected.
- **Counter-evidence:** Arrow supports negative decimal scales. Assigning precision and scale does not itself validate stored values; Arrow provides separate value validation, and its own test accepts an oversized value until validation is called.
- **Sources:** Arrow 59 `datatype.rs` lines 386–412; `types.rs` lines 1405–1442; `primitive_array.rs` lines 1610–1640 and 2454–2470.
- **Impact:** Readers learn the wrong valid type domain and may mistake schema construction for value validation.
- **Direction:** Document negative scale and distinguish type-parameter validation, value validation, casting, and overflow behavior.
- **Verification:** Source inspection FAIL; doctest PASS but exercises only field construction.

### B4 — MAJOR: Timestamp coercion guidance contradicts DataFusion

- **Location:** `schema-creation.md` §“Timestamps and Time Zones” lines 253–321.
- **Rule:** `markdown.mdc` §4.
- **Claim:** Zoned and unzoned timestamps cannot be compared without explicit casts, and coercion never reconciles timezone presence.
- **Counter-evidence:** DataFusion comparison coercion explicitly assigns the present timezone when only one operand has one. Strict temporal coercion does the same when only one side is zoned.
- **Sources:** `expr-common/src/type_coercion/binary.rs` lines 1876–1954.
- **Impact:** Readers add unnecessary casts and receive a false model of comparison and arithmetic behavior.
- **Direction:** Describe comparison, arithmetic, and function coercion separately, including the actual different-timezone failure cases.
- **Verification:** Source inspection FAIL; timestamp doctest does not test coercion.

### B5 — MAJOR: Metadata is not preserved end-to-end

- **Location:** `schema-creation.md` §“Attaching Metadata” lines 389–448.
- **Rule:** `markdown.mdc` §4.
- **Claim:** DataFusion preserves field and schema metadata through the plan.
- **Counter-evidence:** Column references, aliases, and casts can preserve field metadata, but binary and Boolean expressions discard it; most scalar and aggregate functions return empty metadata by default.
- **Sources:** `expr/src/expr_schema.rs` lines 449–463.
- **Impact:** Governance metadata may disappear before serialization despite the page promising preservation.
- **Direction:** State the exact preservation boundaries and distinguish plan transformations from file-format round trips.
- **Verification:** Source inspection FAIL; doctest verifies initial attachment only.

### B6 — MAJOR: Direct `DFSchema` construction silently creates no functional dependencies

- **Location:** `schema-creation.md` §“Defining a DFSchema Directly” lines 454–527.
- **Rule:** `markdown.mdc` §4.
- **Claim:** The documented constructors add qualifiers and functional dependencies needed by optimization.
- **Counter-evidence:** Every listed constructor initializes `FunctionalDependencies::empty()`. Dependencies require `.with_functional_dependencies()` or are derived from source constraints when constructing a table scan.
- **Sources:** `common/src/dfschema.rs` lines 122–217, 269–280, 1091–1113; `expr/src/logical_plan/plan.rs` lines 3095–3124.
- **Impact:** Advanced readers may believe uniqueness information and related optimizer benefits exist when they do not.
- **Direction:** Either narrow the section to wrapping and qualification or document the dependency-assignment/constraint path.
- **Verification:** Source inspection FAIL; doctest does not inspect dependencies.

### B7 — MAJOR: The first H2 is a second content chapter, not an on-ramp

- **Location:** `schema-creation.md` §“Schema Creation: From Automatic to Explicit” lines 48–150.
- **Rule:** `markdown.mdc` §7.4.
- **Claim:** The section orients readers before construction.
- **Counter-evidence:** It occupies roughly 100 lines with internal architecture, source classifications, diagrams, failure tables, and writer guidance. Construction does not begin until line 154.
- **Sources:** Target page; `markdown.mdc` §7.4.
- **Impact:** The mixed page delays its primary task and duplicates detail owned by inference and anatomy topics.
- **Direction:** Reduce the on-ramp to the organizing principle and route detailed failure analysis to its owners.
- **Verification:** Page structure inspected — FAIL.

## Non-blocking findings

### N1 — MINOR: Eight internal links resolve to nonexistent anchors

- **Location:** `schema-creation.md` lines 162–163, 327, and 447.
- **Direction:** Replace the `schema-anatomy.md` links with resolvable anchors or explicit MyST targets.

### N2 — MINOR: Constructor-check count and error taxonomy are wrong

- **Location:** `schema-creation.md` §“Constructor Reference” lines 470–484.
- **Direction:** Change “five constructors” to four and account for `AmbiguousReference` when qualified and unqualified names collide.

### N3 — MINOR: `Large*` selection criterion uses per-value rather than total capacity

- **Location:** `schema-creation.md` lines 380–384.
- **Direction:** Explain that ordinary offsets limit total bytes or child elements in one array, not merely one value.

### N4 — MINOR: Every Rust block omits the mandatory prelude import

- **Location:** All six Rust blocks, lines 173–520.
- **Direction:** Begin each block with `use datafusion::prelude::*;` or record a justified rule deviation.

### N5 — NIT: Style Note contains conventions unused by this page

- **Location:** `schema-creation.md` lines 25–42.
- **Direction:** Retain only conventions relevant to schema construction.

### N6 — NIT: Proofreading error

- **Location:** `schema-creation.md` line 392.
- **Direction:** Change “businesslogic” to “business logic.”

## Open questions

None.

## Verified strengths

- CSV and JSON’s default 1,000-record inference budget is accurate.
- The shared multi-file inference budget is accurately described.
- `DFSchema::try_from` intentionally skips duplicate-name checking for partial aggregate schemas.
- The six visible Rust blocks compile and execute.
- Parquet metadata is skipped by default and `.skip_metadata(false)` is the correct override.

## Validation evidence

**FAIL — corpus format gate:** `ci/scripts/doc_prettier_check.sh` failed for four unrelated planning files; the target was not listed.  
**FAIL — target parse:** Sphinx emitted eight target-local `myst.xref_missing` warnings.  
**PASS — local Rust doctests:** 6 passed, 0 failed.  
**FAIL — portable doctest registration:** Path casing differs from the tracked directory.  
**NOT RUN — external links:** required tool is unavailable without installation.  
**PASS — markers:** No `TODO:` or `citation-needed` markers.

## Unverified areas

- External URL availability was not checked because `lychee` is unavailable.
- No case-sensitive CI environment was run; the casing mismatch is established directly from the paths.

## Recommended next action

- Return the page for a truth-first revision of B1–B6, followed by the structural correction in B7 and complete revalidation.

---

## Schema-application.md Verdict: REVISE

**Mode:** final  
**Structural role:** inferred whole page  
**Stage:** unspecified  
**Independence:** limited — Independence limitation: no author intent was supplied; this review was evidence-only.
**file:** `docs/source/library-user-guide/dataframe-api/Schema-Management/schema-application.md`

## Review boundary

- **Judged:** Entire `Schema-Management/schema-application.md`, seven Rust blocks, links, registration, structure, and repository-backed technical claims.
- **Not judged:** Other Concepts files or corpus-wide consistency.
- **Classification:** Concept-led mixed leaf.
- **Consistency boundary:** repository truth.
- **Final-state requirements not yet applicable:** None.

## Blocking findings

### B1 — BLOCKER: Deprecated JSON API breaks two doctests

- **Location:** `schema-application.md` lines 30–36, 123–136, 251–292, 310–369, and 642–644.
- **Rule:** `markdown.mdc` §4; `rust-docs.mdc` §§1, 2, and 8.
- **Claim:** `NdJsonReadOptions` is the current reader-options type available through the prelude.
- **Counter-evidence:** `NdJsonReadOptions` was deprecated in DataFusion 53 in favor of `JsonReadOptions` and is not exported by `datafusion::prelude::*`. Both JSON examples fail to compile. `JsonReadOptions` also supports NDJSON and JSON-array inputs, making the page’s NDJSON-only framing stale.
- **Sources:** `core/src/datasource/file_format/options.rs` lines 443–470; `core/src/prelude.rs` lines 28–33; targeted doctest.
- **Impact:** Reader-visible code does not compile, and linked API guidance points to a deprecated alias with incorrect Rustdoc paths.
- **Direction:** Replace the alias throughout with `JsonReadOptions`, update the format description, links, and examples.
- **Verification:** Doctest FAIL — 5 passed, 2 failed.

### B2 — BLOCKER: Doctest registration path is not portable

- **Location:** `datafusion/core/src/lib.rs` lines 1420–1423.
- **Rule:** `rust-docs.mdc` §8.
- **Claim:** The page has valid test registration.
- **Counter-evidence:** Registration uses `schema-management`, while the tracked directory is `Schema-Management`.
- **Sources:** Target path; `datafusion/core/src/lib.rs`.
- **Impact:** Case-sensitive CI cannot resolve the page even after B1 is corrected.
- **Direction:** Correct the registration path casing separately.
- **Verification:** Local path resolution PASS on macOS; portable registration FAIL.

### B3 — MAJOR: Partition failure behavior is misstated and its example never executes

- **Location:** `schema-application.md` §“Partitioned Datasets with ListingTable” lines 544–604.
- **Rule:** `markdown.mdc` §4; `rust-docs.mdc` §§1 and 3.4.
- **Claim:** Incorrect partition names, types, or nesting may parse incorrectly or merely reduce pruning; the example demonstrates pruning.
- **Counter-evidence:** Incorrect names or incomplete nesting cause files to be ignored, while values that cannot parse as the declared type return an error. The `no_run` example only compiles; its filesystem rationale is insufficient because the page already uses `tempfile` to construct runnable file layouts.
- **Sources:** `catalog-listing/src/helpers.rs` lines 329–360 and 437–461; repository partition-list tests; targeted doctest.
- **Impact:** Readers may unknowingly omit files and receive incomplete results. The claimed physical-plan behavior has no page-level oracle.
- **Direction:** State skipped-file and parse-error behavior explicitly, and replace the `no_run` example with a temporary partitioned dataset whose pruning result is asserted.
- **Verification:** Source inspection FAIL claim; doctest compile-only.

## Non-blocking findings

### N1 — MINOR: CSV inference warning lists incorrect failure modes

- **Location:** `schema-application.md` lines 232–235.
- **Direction:** Replace `Int32` with the actual `Int64` inference behavior and explain that sampled all-null CSV columns remain present as `Null`; they are not omitted like unknown JSON keys.

### N2 — MINOR: Heading altitude is inconsistent

- **Location:** `schema-application.md` lines 62–119 and 296–376.
- **Direction:** Keep the first H2 as a short on-ramp and move its failure example to the applicable format section; reduce or promote the full-size “JSON Nested Fields” H4.

### N3 — MINOR: Hidden example uses a panic-based path conversion

- **Location:** `schema-application.md` lines 420–424.
- **Direction:** Replace `.to_str().unwrap()` with a non-panicking conversion as required by `rust-docs.mdc` §5.1.

### N4 — NIT: Style Note contains conventions unused by the page

- **Location:** `schema-application.md` lines 40–55.
- **Direction:** Retain only conventions used by the read and schema examples.

## Open questions

None.

## Verified strengths

- The page correctly distinguishes lazy schema attachment from execution-time parsing failures.
- CSV positional alignment, null matching, and truncated-row behavior match Arrow’s reader.
- JSON uses name-based alignment, ignores undeclared keys by default, and enforces non-nullable missing fields.
- The Parquet example correctly demonstrates nullable missing-column adaptation and passes.
- Attached CSV schema and field metadata are present on the resulting `DataFrame`.
- Partition predicates are eligible for exact listing-time pruning when they reference only partition columns.

## Validation evidence

**FAIL — corpus format gate:** The command failed for four unrelated planning files; the target was not listed.  
**PASS — target parse:** Sphinx emitted no target-local warnings or errors.  
**FAIL — Rust doctests:** 5 passed and 2 JSON examples failed to compile.  
**FAIL — portable doctest registration:** Registration path casing differs from the tracked directory.  
**NOT RUN — external links:** required tool is unavailable without installation.  
**PASS — markers:** No `TODO:` or `citation-needed` markers.

## Unverified areas

- External URL availability was not checked.
- The partition example was compiled but not executed.
- Avro and Arrow IPC round-trip behavior is not exercised by this page’s examples.

## Recommended next action

- Return the page for correction of B1–B3, then rerun all final gates.

---

## Schema-inference.md Verdict: REVISE

**Mode:** final  
**Structural role:** inferred — whole page, mixed concept/action  
**Independence:** Independence limitation: no author intent was supplied; this review was evidence-only.
**file:** `docs/source/library-user-guide/dataframe-api/Schema-Management/schema-inference.md`

## Review boundary

- **Judged:** complete `schema-inference.md`, including Rust examples, structure, editorial finish, registration, and material claims against source.
- **Not judged:** other Concepts pages or their prose.
- **Consistency boundary:** page plus repository truth.
- **Final-state requirements not yet applicable:** none.
- No edits made.

## Blocking findings

### B1 — BLOCKER: Deprecated JSON API breaks a doctest

- **Location:** `docs/source/library-user-guide/dataframe-api/Schema-Management/schema-inference.md` lines 31, 76, 88–101, 182–196, 323–326.
- **Rule:** `markdown.mdc` §4; `rust-docs.mdc` Invariants 1–2.
- **Claim:** `NdJsonReadOptions` is the current JSON read API and the sample-size example executes.
- **Counter-evidence:** `NdJsonReadOptions` is deprecated since 53.0.0 in favor of `JsonReadOptions`; it is not exported by the prelude. The targeted doctest fails with E0433. The replacement also supports JSON arrays, making the page’s generic “each line” framing too narrow.
- **Sources:** `datafusion/core/src/datasource/file_format/options.rs` lines 446–506; `datafusion/core/src/prelude.rs` lines 31–33; targeted doctest.
- **Impact:** the page fails its Rust gate and teaches an obsolete API.
- **Direction:** replace the alias throughout, update API links, and distinguish NDJSON records from JSON-array elements where relevant.
- **Verification:** source inspection — PASS; doctest — FAIL.

### B2 — BLOCKER: Doctest registration has incorrect path casing

- **Location:** `datafusion/core/src/lib.rs` lines 1413–1417.
- **Rule:** `rust-docs.mdc` §8.
- **Claim:** the page is registered under its tracked path.
- **Counter-evidence:** registration uses `schema-management/`; Git tracks only `Schema-Management/schema-inference.md`.
- **Sources:** registration source; `git ls-files`.
- **Impact:** case-sensitive systems cannot resolve the registration path. The local case-insensitive filesystem masks the defect.
- **Direction:** correct the registration path casing.
- **Verification:** tracked-path comparison — FAIL.

### B3 — MAJOR: Recommended validator does not enforce the stated type contract

- **Location:** `schema-inference.md` §“Validating an Inferred Schema” lines 253–290.
- **Rule:** `markdown.mdc` §4; Judge Gate A.
- **Claim:** `.has_equivalent_names_and_types()` validates inferred names and data types, ignoring only nullability and metadata.
- **Counter-evidence:** it uses `datatype_is_semantically_equal()`, which also ignores decimal precision/scale and timestamp unit/timezone.
- **Sources:** `datafusion/common/src/dfschema.rs` lines 631–663 and 732–806.
- **Impact:** incompatible financial or temporal contracts can pass the page’s recommended production guard.
- **Direction:** state the method’s actual equivalence semantics and require explicit comparisons for properties it ignores.
- **Verification:** source inspection — PASS; example only covers primitive types.

### B4 — MAJOR: Explicit schemas are presented as data-quality guarantees

- **Location:** `schema-inference.md` §“Explicit Schemas vs. Inference” lines 60–70 and conclusion lines 302–305.
- **Rule:** `markdown.mdc` §4; Judge Gate A.
- **Claim:** explicit schemas “eliminate” divergence risk, “ensure data quality,” and “guarantee consistency.”
- **Counter-evidence:** supplying a schema bypasses inference; it does not validate source semantics. Arrow JSON readers ignore undeclared columns by default because strict mode is false.
- **Sources:** `datafusion/core/src/execution/context/mod.rs` lines 1741–1753; `arrow-json` `ReaderBuilder` lines 200–216; `datafusion/datasource-json/src/source.rs` lines 293–346.
- **Impact:** readers may mistake parser shape/type configuration for comprehensive contract validation.
- **Direction:** describe explicit schemas as stabilizing expected shape and types, then separate format-specific enforcement from data-quality validation.
- **Verification:** source inspection — PASS.

### B5 — MAJOR: Sparse CSV inference omits a silent data-loss path

- **Location:** `schema-inference.md` §“Schema Drift” lines 237–247.
- **Rule:** Judge Gates A and C — truthful failure behavior and unhappy paths.
- **Claim:** sparse columns lead to a wrong type or parse failure.
- **Counter-evidence:** an all-null sample produces `DataType::Null`; Arrow’s CSV reader then emits nulls for every later value without parsing the source text.
- **Sources:** `datafusion/datasource-csv/src/file_format.rs` lines 653–671; `arrow-csv` reader lines 817–822.
- **Impact:** later non-null values can be silently discarded rather than producing the expected failure.
- **Direction:** explicitly document this silent-null outcome and its mitigation.
- **Verification:** source inspection — PASS.

## Non-blocking findings

### N1 — MINOR: Inferred nullability behavior is unexplained

- **Location:** `schema-inference.md` lines 23, 88–97, 151–158.
- **Direction:** state that CSV and JSON inference conservatively marks inferred fields nullable; it does not infer requiredness.

### N2 — MINOR: Missing H2 divider before the conclusion

- **Location:** `schema-inference.md` line 300.
- **Direction:** add the required `---` before `## Conclusion`.

### N3 — MINOR: Hidden setup uses panic-based path conversion

- **Location:** `schema-inference.md` lines 129 and 276.
- **Direction:** replace `.to_str().unwrap()` with a non-panicking conversion.

### N4 — MINOR: File-read output uses an ordering-sensitive assertion

- **Location:** `schema-inference.md` lines 137–149.
- **Direction:** use `assert_batches_sorted_eq!` because no ordering contract is established.

### N5 — NIT: Style Note contains unrelated API categories

- **Location:** `schema-inference.md` lines 40–49.
- **Direction:** retain only conventions used by this page.

## Open questions

None.

## Verified strengths

- The 1,000-record default and format-specific zero-sample behavior match source.
- CSV and JSON share the inference budget across files.
- `Schema::try_merge()` conflict behavior and Parquet’s deterministic path sorting are accurately described.
- The two CSV examples compile and execute successfully.

## Validation evidence

- **FAIL — corpus format gate:** `ci/scripts/doc_prettier_check.sh` reported four unrelated planning files; the target was not listed.
- **PASS — target parse:** fresh Sphinx dummy build produced no target-local warning or error.
- **FAIL — Rust doctests:** 2 passed, 1 failed; `NdJsonReadOptions` was undeclared.
- **NOT RUN — external links:** required `lychee` tool is unavailable without installation.
- **PASS — markers:** no `TODO:` or `citation-needed` markers found.
- **FAIL — registration casing:** source path does not match Git’s tracked casing.

## Unverified areas

- External URL availability could not be checked.

## Recommended next action

- Resolve B1–B5, then rerun the format, parse, doctest, link, and marker gates.

---

## schema-inspection.md Verdict: REVISE

**Mode:** final  
**Structural role:** inferred whole-page mixed concept  
**Stage:** unspecified  
**Independence:** limited — Independence limitation: no author intent was supplied; this review was evidence-only.  
**Filepath:** `docs/source/library-user-guide/dataframe-api/Schema-Management/schema-inspection.md`

## Review boundary

- Judged: the complete target page, examples, API claims, registration, structure, and finish.
- Not judged: other Schema Management pages beyond link resolution.
- Consistency boundary: page and repository truth.
- Final-state requirements not yet applicable: none.

## Blocking findings

### B1 — BLOCKER: Doctest registration has incorrect path casing

- **Location:** `datafusion/core/src/lib.rs` lines 1424–1429.
- **Rule:** Gate A; final Rust validation must work on supported case-sensitive environments.
- **Claim:** The target is registered as a runnable doctest.
- **Counter-evidence:** Registration uses `dataframe-api/schema-management/schema-inspection.md`; Git tracks `dataframe-api/Schema-Management/schema-inspection.md`.
- **Sources:** `datafusion/core/src/lib.rs`; `git ls-files`.
- **Impact:** Linux or other case-sensitive builds cannot resolve the registered document.
- **Direction:** Match the registration path to the tracked casing.
- **Verification:** FAIL — path portability; PASS — local case-insensitive doctest run.

### B2 — BLOCKER: Field-metadata example does not compile

- **Location:** `schema-inspection.md` §“Schema-level vs field-level metadata” lines 165–168.
- **Rule:** Gate A; Rust API examples must be callable as written.
- **Claim:** `df.schema().metadata(&Column::from("col_name"))` calls `ExprSchema::metadata()`, and both metadata methods return `&HashMap`.
- **Counter-evidence:** Inherent `DFSchema::metadata()` shadows the trait method, causing E0061 when an argument is supplied. `ExprSchema::metadata()` also returns `Result<&HashMap<...>>`, not a bare reference.
- **Sources:** `datafusion/common/src/dfschema.rs` lines 846–848 and 1192–1206; focused `rustc` method-resolution probe.
- **Impact:** Copied code fails to compile and omits required error handling.
- **Direction:** Use qualified trait syntax or retrieve the field first, and handle the `Result`.
- **Verification:** FAIL — compiler rejected the documented call pattern.

### B3 — BLOCKER: `dataframe!` qualifier model and displayed output are false

- **Location:** `schema-inspection.md` §“Displaying Schemas” lines 105–140 and §“Qualified Field Access” lines 313–360.
- **Rule:** Gate A; visible output and qualifier semantics must match runtime behavior.
- **Claim:** `dataframe!` fields have qualifier `None`, and its displayed/tree schemas contain bare names.
- **Counter-evidence:** `dataframe!` delegates to `DataFrame::from_columns()`, then `read_batch()`, which builds a scan named `?table?`. `DFSchema::Display` and `tree_string()` include present qualifiers. The actual names therefore include `?table?.user_id`, etc.
- **Sources:** `datafusion/core/src/dataframe/mod.rs` lines 2624–2692; `datafusion/core/src/execution/context/mod.rs` lines 1792–1803; `datafusion/common/src/dfschema.rs` lines 892–909 and 1173–1184.
- **Impact:** Readers receive an incorrect qualifier model and incorrect expected output.
- **Direction:** Correct the qualifier explanation and outputs, then assert the complete displayed strings.
- **Verification:** FAIL — source contradicts the prose; doctests pass only because they assert substrings.

### B4 — MAJOR: Index lookup silently chooses the first ambiguous column

- **Location:** `schema-inspection.md` §“Index-Based Lookup” lines 362–402.
- **Rule:** Gate A and Gate C; safety-relevant lookup behavior requires its unhappy path.
- **Claim:** The methods differ principally through `Result` versus `Option` absence handling.
- **Counter-evidence:** Unqualified `index_of_column*` lookup calls `.next()` and silently returns the first matching field. A joined schema may legally contain `users.id` and `orders.id`.
- **Sources:** `datafusion/common/src/dfschema.rs` lines 381–419 and 239–265.
- **Impact:** Code using the returned index can read the wrong `RecordBatch` column without an error.
- **Direction:** Document first-match ambiguity and require qualified `Column` values after joins.
- **Verification:** PASS — implementation inspected.

### B5 — MAJOR: CSV is incorrectly presented as preserving native nullability

- **Location:** `schema-inspection.md` §“Arrow Interop,” “`dataframe!` macro nullability” lines 671–675.
- **Rule:** Gate A; source-format guarantees must be accurate.
- **Claim:** `ctx.read_csv(...)` loads native nullability settings.
- **Counter-evidence:** CSV has no native schema nullability; inferred CSV fields are constructed with `nullable = true`. `read_table()` depends entirely on its provider.
- **Sources:** `datafusion/datasource-csv/src/file_format.rs` lines 640–688.
- **Impact:** Readers may treat a nullable CSV contract as a source-enforced non-null contract.
- **Direction:** State nullability behavior separately for inferred CSV, explicit schemas, Parquet, and providers.
- **Verification:** PASS — CSV implementation inspected.

### B6 — MAJOR: Core qualifier and existence examples lack effective oracles

- **Location:** `schema-inspection.md` §“Validating Column Existence” lines 239–264 and §“Qualified Field Access” lines 318–344.
- **Rule:** `rust-docs.mdc` §3.1; compilation is necessary but insufficient.
- **Claim:** The examples demonstrate missing-column detection and qualified join fields.
- **Counter-evidence:** The existence assertion runs only if the expected condition is already true; the qualifier example asserts nothing about qualifiers or fields.
- **Sources:** Target examples; targeted doctest results.
- **Impact:** Behavioral regressions pass, as already demonstrated by B3’s false output surviving the suite.
- **Direction:** Add unconditional assertions for the missing set and expected qualifier/name pairs.
- **Verification:** FAIL — twelve doctests compile and run, but these claims are not tested.

## Non-blocking findings

### N1 — MINOR: Functional dependencies are assigned the wrong optimizer example

- **Location:** `schema-inspection.md` §“Functional dependencies” lines 665–670.
- **Direction:** Replace “eliminating redundant sorts” with a verified use such as redundant grouping, distinct, or join elimination; physical ordering explicitly lacks `DFSchema` functional-dependency awareness.

### N2 — MINOR: `field_names()` returns qualified names

- **Location:** `schema-inspection.md` §“Accessing Fields and Properties” lines 156–164.
- **Direction:** Describe the result as fully qualified names when qualifiers exist.

### N3 — MINOR: `explain(false, false)` is conflated with direct schema inspection

- **Location:** `schema-inspection.md` §“Pre-analysis vs post-analysis schema” lines 90–94.
- **Direction:** Explain that `explain()` returns a lazy explain DataFrame that must be executed and displays the final plan containing casts, not a schema containing `CAST` nodes.

### N4 — MINOR: Duplicate-name failures are misclassified as runtime errors

- **Location:** `schema-inspection.md` §“Validating Schema Integrity” lines 523–557.
- **Direction:** Describe these primarily as schema/column-resolution or planning failures.

## Open questions

None.

## Verified strengths

- Schema-equivalence behavior correctly distinguishes qualifier-aware logical comparison from positional semantic comparison.
- Decimal, timestamp, nullability, and metadata caveats match `DFSchema` implementation.
- `check_names()` and `matches_arrow_schema()` behavior, including unequal-length `zip()` semantics, is accurately documented.
- Arrow interop return types and qualifier loss are correct.

## Validation evidence

**PASS — targeted Rust doctests:** 12 passed, 0 failed.  
**FAIL — doctest registration portability:** tracked and registered path casing differ.  
**FAIL — field-metadata API probe:** documented call pattern produced E0061.  
**PASS — target parse:** fresh Sphinx dummy build reported no target-local diagnostics.  
**FAIL — corpus format gate:** four unrelated planning files failed; the target was not reported.  
**PASS — source/API inspection:** relevant `DFSchema`, `DataFrame`, CSV, optimizer, and explain implementations inspected.  
**NOT RUN — external links:** required `lychee` tool is unavailable without installation.

## Unverified areas

- External URL availability.
- Consistency with other Schema Management prose, intentionally outside scope.

## Recommended next action

- Return the page to the Author to resolve B1–B6, then rerun the targeted doctest, format, and parse gates.

---

## schema-transformation.md Verdict: REVISE

**Mode:** final  
**Structural role:** inferred whole-page mixed concept  
**Stage:** unspecified  
**Independence:** limited — Independence limitation: no author intent was supplied; this review was evidence-only.  
**Filepath:** `docs/source/library-user-guide/dataframe-api/Schema-Management/schema-transformation.md`

## Review boundary

- Judged: complete target, Rust examples, links, registration, and relevant source behavior.
- Not judged: other Schema Management pages beyond link-resolution evidence.
- Consistency boundary: page and repository truth.
- Final-state requirements not yet applicable: none.

## Blocking findings

### B1 — BLOCKER: Doctest registration has incorrect path casing

- **Location:** `datafusion/core/src/lib.rs` lines 1430–1435.
- **Rule:** Gate A; final Rust validation must work on case-sensitive systems.
- **Claim:** The target is registered as a portable doctest.
- **Counter-evidence:** Registration uses `schema-management`; Git tracks `Schema-Management`.
- **Sources:** `datafusion/core/src/lib.rs`; `git ls-files`.
- **Impact:** Case-sensitive CI cannot resolve the document.
- **Direction:** Match the registered path to tracked casing.
- **Verification:** FAIL — path portability; PASS — local case-insensitive run.

### B2 — BLOCKER: The documented `DFSchema::merge()` contract is false

- **Location:** `schema-transformation.md` §“Schema Transformation” lines 64–68 and §“Joining and Merging Schemas” lines 162–225.
- **Rule:** Gate A; mutation, conflict, and metadata behavior must match implementation.
- **Claim:** Transformations leave the original untouched; `merge()` keeps `self` on field conflicts and always merges metadata.
- **Counter-evidence:** `merge(&mut self, ...)` mutates its receiver. Qualified duplicate detection compares `(qualifier, complete FieldRef)`, so the same qualified name with a different type/nullability is appended, potentially producing an invalid duplicate. An incoming schema with no fields returns before metadata is merged.
- **Sources:** `datafusion/common/src/dfschema.rs` lines 309–360.
- **Impact:** Custom-plan code can silently construct an ambiguous schema while expecting conflict precedence.
- **Direction:** Describe the actual mutating and qualified-field behavior, require post-merge validation, or first align implementation with its intended contract.
- **Verification:** FAIL — source contradicts the page.

### B3 — BLOCKER: The nullability-relaxation example starts nullable

- **Location:** `schema-transformation.md` §“Unioning DataFrames by Column Name” lines 374–392.
- **Rule:** Gate A and `rust-docs.mdc` §3.1; visible before/after evidence must establish the claim.
- **Claim:** `order_id` starts NOT NULL and becomes nullable because the other input omits it.
- **Counter-evidence:** `dataframe!` delegates to `DataFrame::from_columns()`, which constructs every field with `nullable = true`.
- **Sources:** `datafusion/core/src/dataframe/mod.rs` lines 2624–2628 and 2685–2692; targeted doctest.
- **Impact:** The passing assertion proves only that an already-nullable field remains nullable.
- **Direction:** Construct a genuinely non-nullable input and assert both pre- and post-union states.
- **Verification:** FAIL — claim false; doctest oracle insufficient.

### B4 — MAJOR: Immediate union schema is conflated with analyzed schema

- **Location:** `schema-transformation.md` §“Unioning DataFrames by Column Name” lines 303–373.
- **Rule:** Gate A; pre-analysis and post-analysis schemas must be distinguished.
- **Claim:** Differing types are widened and readers can inspect the combined schema before relying on it.
- **Counter-evidence:** `Union::try_new_by_name()` initially takes each column type from its first occurrence. Common-type coercion happens later in the analyzer; `DataFrame::schema()` exposes the unoptimized plan schema.
- **Sources:** `datafusion/expr/src/logical_plan/plan.rs` lines 3187–3322; `datafusion/optimizer/src/analyzer/type_coercion.rs` lines 221–249.
- **Impact:** Pre-execution validation may observe a different type from the analyzed plan.
- **Direction:** Explicitly separate immediate `df.schema()` results from analyzer-produced union types.
- **Verification:** PASS — both planning stages inspected.

### B5 — MAJOR: Functional-dependency optimizer behavior is misstated

- **Location:** `schema-transformation.md` §“Annotating Functional Dependencies” lines 235–292.
- **Rule:** Gate A; optimizer guarantees require direct source support.
- **Claim:** A `DISTINCT` made redundant by an ordinary unique key is dropped.
- **Counter-evidence:** `ReplaceDistinctWithAggregate` removes `DISTINCT` only when dependency source indices cover every output field in order—principally the “same as previous GROUP BY” case. A normal single-column primary-key dependency does not satisfy that condition for a wider schema.
- **Sources:** `datafusion/optimizer/src/replace_distinct_aggregate.rs` lines 89–120; `datafusion/common/src/functional_dependencies.rs`.
- **Impact:** Readers are given the wrong performance and plan-shape expectation.
- **Direction:** Describe verified uses such as implicit grouping fields, redundant sort terms, join optimization, and the narrower DISTINCT case.
- **Verification:** PASS — optimizer rules inspected.

### B6 — MAJOR: Union metadata does not require presence in every branch

- **Location:** `schema-transformation.md` §“`.merge()` is not `.union_by_name()`” lines 226–231.
- **Rule:** Gate A; metadata propagation rules are contract behavior.
- **Claim:** Name-based union keeps only metadata keys identical across every branch.
- **Counter-evidence:** `intersect_metadata_for_union()` skips empty metadata maps. Metadata present in one non-empty branch can therefore survive another branch with no metadata.
- **Sources:** `datafusion/expr/src/expr.rs` lines 648–670; `datafusion/expr/src/logical_plan/plan.rs` lines 3255–3322.
- **Impact:** Readers may incorrectly treat retained metadata as branch-wide evidence.
- **Direction:** Document the implemented non-empty-map intersection behavior or resolve the implementation discrepancy.
- **Verification:** FAIL — implementation contradicts the stated rule.

### B7 — MAJOR: Arrow example does not demonstrate functional-dependency loss or restoration

- **Location:** `schema-transformation.md` §“Handling Schema Transformation at the Arrow Interop Layer” lines 455–499.
- **Rule:** `rust-docs.mdc` §3.1; compilation alone is insufficient for the section’s central claim.
- **Claim:** The example demonstrates rebuilding qualifiers and functional dependencies after Arrow conversion.
- **Counter-evidence:** No dependency is attached before export, asserted absent after reconstruction, or restored afterward. Only qualifiers are tested.
- **Sources:** Target example; targeted doctest results; `DFSchema::try_from` implementation.
- **Impact:** The safety-sensitive positional dependency restoration path remains untested and unexplained.
- **Direction:** Attach a dependency, assert its loss, remap/rebuild it, and assert the restored indices.
- **Verification:** FAIL — example covers only half its stated purpose.

## Non-blocking findings

### N1 — MINOR: Three internal links are broken

- **Location:** `schema-transformation.md` lines 159, 316, and 451.
- **Direction:** Point both Join Patterns links to the existing joins landing page and update the missing `schema-anatomy.md#nullability` anchor.

### N2 — MINOR: Merge dependency drift is underexplained

- **Location:** `schema-transformation.md` §“DFSchema::merge(): Permissive Append” lines 199–225.
- **Direction:** State that incoming dependencies are discarded and existing dependency target indices are not extended for appended fields.

### N3 — MINOR: Arrow is inaccurately called a “physical-only” boundary

- **Location:** `schema-transformation.md` §“Conclusion” lines 507–511.
- **Direction:** Call it an Arrow/interop boundary; Arrow schemas also describe logical data contracts and sources.

## Open questions

None.

## Verified strengths

- Qualifier-transform receiver ownership, collision risks, and required `check_names()` validation match source.
- `DFSchema::join()` concatenation, metadata precedence, validation, and functional-dependency reset are accurate.
- Name-based union ordering, missing-column NULL insertion, nullability, and dependency clearing are correctly demonstrated.
- Functional-dependency bounds validation and derivation from provider constraints are accurate.

## Validation evidence

**PASS — targeted Rust doctests:** 8 passed, 0 failed.  
**FAIL — doctest registration portability:** tracked and registered path casing differ.  
**FAIL — target parse:** three target-local unresolved cross-references.  
**FAIL — corpus format gate:** four unrelated planning files failed; the target was not reported.  
**PASS — source/API inspection:** relevant schema, union, coercion, metadata, and optimizer implementations inspected.  
**NOT RUN — external links:** required `lychee` tool is unavailable without installation.

## Unverified areas

- External URL availability.
- Broader consistency with other Schema Management prose, intentionally outside scope.

## Recommended next action

- Return the page to the Author to resolve B1–B7, then rerun the targeted doctest, parse, and format gates.

---

## schema-dataframe-methods.md Verdict: REVISE

**Mode:** final  
**Structural role:** inferred whole page  
**Stage:** unspecified  
**Independence:** evidence-only — Independence limitation: no author intent was supplied; this review was evidence-only.  
**Filepath:** `docs/source/library-user-guide/dataframe-api/Schema-Management/schema-dataframe-methods.md`

## Review boundary

- **Judged:** complete mixed concept/action page, nine Rust blocks, links, registration, structure, and repository-backed technical claims.
- **Not judged:** other Schema Management pages or their prose.
- **Consistency boundary:** page plus repository truth.
- **Final-state requirements not yet applicable:** None.
- No edits made.

## Blocking findings

### B1 — BLOCKER: Doctest registration is not portable

- **Location:** `datafusion/core/src/lib.rs` lines 1437–1441.
- **Rule:** `rust-docs.mdc` Invariant 3 and §8.
- **Claim:** the page is registered for doctesting.
- **Counter-evidence:** registration uses lowercase `schema-management`; the tracked directory is `Schema-Management`. It works locally on case-insensitive macOS but fails on case-sensitive systems.
- **Sources:** registration; `git ls-files`; targeted doctest.
- **Impact:** Linux CI cannot resolve the registered Markdown path.
- **Direction:** match the tracked path’s exact casing.
- **Verification:** registration portability — FAIL; local doctest — PASS.

### B2 — BLOCKER: `.drop_columns()` does not ignore every unknown name

- **Location:** target §“Key methods,” §“DataFrame Methods and the Schema’s Involvement,” and §“Projection-Backed Column Edits” (lines 30–38, 72–76, 108–117).
- **Rule:** `markdown.mdc` §4.
- **Claim:** `.drop_columns()` silently ignores unknown column names.
- **Counter-evidence:** unqualified unknown names are ignored, but a qualified name follows `qualified_field_from_column()` and an unknown qualified field returns an error.
- **Sources:** `datafusion/core/src/dataframe/mod.rs` lines 467–502; `datafusion/common/src/column.rs` lines 330–347.
- **Impact:** qualified-schema code can fail where the documented table promises a no-op.
- **Direction:** distinguish unqualified unknown names from unresolved qualified names.
- **Verification:** source inspection — PASS; page doctest does not cover this distinction.

### B3 — BLOCKER: `.fill_null()` does not always make a field NOT NULL

- **Location:** target lines 37–38 and §“Normalizing Types and Nulls” (lines 300–337).
- **Rule:** `markdown.mdc` §4.
- **Claim:** every castable filled field becomes NOT NULL because the fill value is non-null.
- **Counter-evidence:** `.fill_null()` accepts nullable `ScalarValue`s. A typed NULL remains nullable through `coalesce(column, NULL)`.
- **Sources:** `datafusion/core/src/dataframe/mod.rs` lines 2468–2474 and 2524–2562; `datafusion/functions/src/core/coalesce.rs` lines 79–89; `datafusion/expr/src/expr_schema.rs` lines 248–286.
- **Impact:** readers can infer a stronger schema contract than the API guarantees.
- **Direction:** condition the NOT NULL result on supplying a successfully cast, non-null fill value.
- **Verification:** source inspection — PASS; doctest covers only non-null `0`.

### B4 — BLOCKER: Wildcards do not produce one field per expression

- **Location:** target §“Selecting Columns” (lines 123–153).
- **Rule:** `markdown.mdc` §4.
- **Claim:** each projection expression, including a wildcard, becomes one output field.
- **Counter-evidence:** projection planning expands wildcards into every matching field before building the output schema.
- **Sources:** `datafusion/expr/src/logical_plan/builder.rs` lines 1962–2017.
- **Impact:** the stated projection model is wrong for a common `.select()`/`.select_exprs()` case.
- **Direction:** state that ordinary expressions produce one field while wildcards expand to zero or more matching fields.
- **Verification:** source inspection — PASS; doctest contains no wildcard.

### B5 — MAJOR: `.explain()` is misclassified as an action

- **Location:** target §“Schema-Preserving Methods” (lines 83–93).
- **Rule:** `markdown.mdc` §4, lazy-versus-eager terminology.
- **Claim:** `.describe()` and `.explain()` are both inspection actions.
- **Counter-evidence:** `.describe()` is async and executes aggregate plans internally; `.explain()` synchronously adds an `Explain`/`Analyze` plan and remains lazy until collected.
- **Sources:** `datafusion/core/src/dataframe/mod.rs` lines 1017–1190 and 1761–1806; `datafusion/expr/src/logical_plan/builder.rs` lines 1332–1355.
- **Impact:** the page collapses materially different execution boundaries.
- **Direction:** describe `.describe()` as eager and `.explain()` as a lazy diagnostic transformation.
- **Verification:** source inspection — PASS.

### B6 — MAJOR: Data-changing examples have insufficient oracles

- **Location:** target §“Normalizing Types and Nulls” (lines 304–329) and §“Reshaping Nested Fields” (lines 399–470).
- **Rule:** `rust-docs.mdc` §§3.1–3.3; oracle calibration.
- **Claim:** the examples demonstrate filled values, struct row preservation, and list element expansion.
- **Counter-evidence:** `.fill_null()` asserts only schema nullability; struct unnest asserts only field names; list unnest asserts only row count and type. None asserts the resulting rows or values.
- **Sources:** target Rust blocks; targeted doctest output.
- **Impact:** the examples compile while data loss, wrong values, or incorrect row replication would remain undetected.
- **Direction:** add visible batch assertions for the value- and row-level claims.
- **Verification:** doctest — PASS, but oracle adequacy — FAIL.

### B7 — MAJOR: Dotted struct-field names lack usable reference syntax

- **Location:** target §“Reshaping Nested Fields” (lines 399–429).
- **Rule:** Gate C reader path and unhappy paths.
- **Claim:** unnesting creates top-level fields such as `person.first`, making members directly usable.
- **Counter-evidence:** these are unqualified field names containing a literal dot. `col("person.first")` parses `person` as a qualifier; readers need `ident("person.first")` or a quoted identifier.
- **Sources:** `datafusion/expr/src/logical_plan/plan.rs` lines 4811–4853; `datafusion/expr/src/expr_fn.rs` lines 68–111.
- **Impact:** the natural follow-up expression fails despite the example’s apparently successful schema.
- **Direction:** demonstrate how to reference an unnested dotted field.
- **Verification:** source inspection — PASS.

## Non-blocking findings

### N1 — MINOR: Invalid contents option

- **Location:** target lines 57–62.
- **Direction:** remove the unsupported empty `:caption:` option.

### N2 — MINOR: Broken joins links

- **Location:** target lines 351 and 391.
- **Direction:** point to the existing `../Transformations/joins/index.md` or an appropriate joins leaf.

### N3 — MINOR: Rename collision failure is omitted

- **Location:** target §“Renaming and Removing Fields” (lines 235–267).
- **Direction:** state that renaming onto an existing field name fails projection validation.

### N4 — NIT: Trailing H2 divider

- **Location:** target line 497.
- **Direction:** remove the divider after the final H2.

## Open questions

None.

## Verified strengths

- All nine visible Rust blocks compile and execute locally.
- The projection, aggregation, window, and unnest schema families largely match their logical-plan implementations.
- Unknown-name behavior for `.select_columns()`, `.fill_null()`, and `.with_column_renamed()` is accurately distinguished.
- The schema-preserving classification for `.filter()`, `.sort()`, `.limit()`, `.distinct()`, and `.repartition()` matches source.

## Validation evidence

- **PASS — targeted Rust doctest:** 9 passed; 0 failed.
- **FAIL — registration portability:** path casing differs from the tracked directory.
- **FAIL — target parse:** unsupported `:caption:` and two unresolved joins links.
- **FAIL — corpus format gate:** five other files failed; the target was not listed.
- **PASS — markers:** no `TODO:` or `citation-needed` markers.
- **NOT RUN — external links:** CI-only link gate was not run locally.
- **PASS — tracked-path check:** `git ls-files` confirms `Schema-Management` casing.

## Unverified areas

- External docs.rs destinations were not network-checked.
- Case-sensitive doctest execution was inferred from tracked paths and registration text, not run on Linux.

## Recommended next action

- Correct B1–B7, then rerun the targeted doctest and target-local parse checks.
