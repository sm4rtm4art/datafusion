## Verdict: REVISE

**Mode:** final  
**Structural role:** inferred — whole page; no subtree role applies  
**Stage:** unspecified  
**Independence:** evidence-only — Independence limitation: no author intent was supplied; this review was evidence-only.

## Review boundary

- **Judged:** complete target page, Rust examples, doctest registration, and relevant Arrow reader/API source.
- **Not judged:** neighboring documentation or unnamed Concepts pages.
- **Consistency boundary:** repository truth.
- **Page classification:** judged as an action leaf because its path, methods, and stated action quadrant outweigh the contradictory `concept` label.
- **Final-state requirements not yet applicable:** phase-specific ownership rules were not inferred.

## Blocking findings

### B1 — BLOCKER: Doctest registration has incorrect path casing

- **Location:** `datafusion/core/src/lib.rs` registration for `arrow-ipc.md` (lines 1360–1363)
- **Rule:** `rust-docs.mdc` §8; validation §2.3.
- **Claim:** The page is registered for doctesting.
- **Counter-evidence:** Registration uses `creating-dataframes`, while Git tracks `Creating-DataFrames`. Darwin’s case-insensitive filesystem masks the error; the lowercase path does not exist in the Git tree.
- **Sources:** `datafusion/core/src/lib.rs`; `git ls-files`; `git cat-file`.
- **Impact:** Case-sensitive CI can fail before the page’s doctests execute.
- **Direction:** Match the registration path’s exact tracked casing.
- **Verification:** Git-tree path check — **FAIL**; local Darwin doctest — **platform-limited PASS**.

### B2 — BLOCKER: Zero-copy and performance claims overstate DataFusion behavior

- **Location:** target title/abstract and §§“When to Use Arrow IPC”, “Production Tips” (lines 20–31, 171–179, 217–230)
- **Rule:** `markdown.mdc` §4 and §7.3.
- **Claim:** Arrow IPC is DataFusion’s “zero-copy,” “fastest” path, enables memory-mapped reads, and starts querying almost immediately.
- **Counter-evidence:** DataFusion uses `FileReader` or materialized object-store bytes, not mmap. Arrow’s reader allocates and reads complete blocks; alignment and compression can require copying. DataFusion writes LZ4-compressed buffers by default, which require decompression.
- **Sources:** `datafusion/datasource-arrow/src/file_format.rs`; `source.rs`; Arrow IPC 59.0.0 reader source.
- **Impact:** Readers may choose IPC based on unsupported runtime and performance guarantees.
- **Direction:** Remove superlatives and zero-copy/mmap claims; describe reduced conversion conditionally.
- **Verification:** Source inspection — **PASS**; benchmark support — **NOT RUN**, none supplied.

### B3 — BLOCKER: Projection is incorrectly described as I/O-level column pruning

- **Location:** target §“Reading Arrow IPC Files” and §“When to Use Arrow IPC” (lines 64–68, 182–185)
- **Rule:** `markdown.mdc` §4.
- **Claim:** Only requested columns are read from disk.
- **Counter-evidence:** DataFusion fetches complete record-batch blocks before applying decoder projection. Projection avoids constructing unselected arrays but does not fetch only their column buffers.
- **Sources:** `datafusion/datasource-arrow/src/source.rs` (record-batch ranges); Arrow IPC reader `read_block()`.
- **Impact:** Readers receive a materially wrong I/O and format-selection model.
- **Direction:** Distinguish decoder projection from Parquet-style column-level I/O pruning.
- **Verification:** Source inspection — **PASS**.

### B4 — MAJOR: Schema override is presented as type normalization

- **Location:** target §“ArrowReadOptions” (lines 134–166)
- **Rule:** `markdown.mdc` §4.
- **Claim:** `.schema()` can normalize types across IPC files and define how their bytes are interpreted.
- **Counter-evidence:** The option supplies the logical table schema and skips inference; the IPC reader still decodes using the embedded physical schema. No coercion layer normalizes incompatible arrays, and projected output schema mismatches error.
- **Sources:** `options.rs`; `source.rs`; datasource and physical-expression projection implementations.
- **Impact:** Compatible-looking configuration can fail during execution rather than convert data.
- **Direction:** State that an explicit schema must be compatible and does not perform type coercion.
- **Verification:** Source inspection — **PASS**; normalization test — **NOT RUN**, none exists.

### B5 — MAJOR: A reader-visible Rust block is explicitly ignored

- **Location:** target §“ArrowReadOptions” (lines 151–156)
- **Rule:** `rust-docs.mdc` Invariants 1–3 and §3.4.
- **Claim:** The options snippet is verified documentation code.
- **Counter-evidence:** It is fenced `rust,ignore`; the targeted gate reports one passing and one ignored test.
- **Sources:** target source; targeted doctest output.
- **Impact:** The incomplete snippet can silently rot while the command exits successfully.
- **Direction:** Make it a complete executable block or incorporate it into the existing example.
- **Verification:** Doctest criterion — **FAIL**.

## Non-blocking findings

### N1 — MINOR: IPC messages are not always self-contained

- **Location:** target §“Reading Arrow IPC Files” (lines 79–87)
- **Direction:** Include dictionary messages and avoid calling dictionary-dependent record batches self-contained.

### N2 — MINOR: Reader-visible `.collect()` is unbounded

- **Location:** target §“Reading Arrow IPC Files” (lines 108–127)
- **Direction:** Bound the demonstration or add the required memory-risk warning.

### N3 — MINOR: API reference links have no definitions

- **Location:** target §“ArrowReadOptions” (lines 132–166)
- **Direction:** Add reference-style definitions to exact latest docs.rs anchors; labels such as `arrowreadoptions::schema()` currently do not resolve.

### N4 — MINOR: Final action-page structure is incomplete

- **Location:** target opening and H2 sequence (lines 20–235)
- **Direction:** Add useful method orientation, required H2 dividers, and a conclusion with references under optional Further Reading.

### N5 — MINOR: See-also callout does not follow the required admonition form

- **Location:** target §“Reading Arrow IPC Files” (lines 79–91)
- **Direction:** Use a titled `admonition` with `:class: seealso`.

## Open questions

### Q1 — QUESTION: Packet page type is contradictory

- **Location:** review packet `page_type`
- **Resolves it:** Author confirms either concept/cognitive or action leaf. This review used action leaf because the path, content, and parenthetical classification agree on that role.

## Verified strengths

- `.read_arrow()` and `.register_arrow()` signatures and default `.arrow` extension match source.
- File/stream auto-detection, parallel file reading, sequential stream reading, and unknown optimizer statistics are supported.
- DataFusion’s Arrow writer uses LZ4 buffer compression by default and rejects outer file compression.
- The primary Rust example executes and asserts its visible rows.
- No target TODO or citation marker remains.

## Validation evidence

- **FAIL — Rust doctest:** exit 0, but only 1 of 2 visible Rust blocks passed; 1 was ignored.
- **FAIL — registration path:** casing differs from the tracked path.
- **FAIL — format command:** corpus-wide checker failed on an unrelated file; the target was not reported.
- **FAIL — parse command:** repository build exited 1 because of unrelated global errors; the target emitted no warning or error.
- **PASS — target parse filter:** no page-local warning or error.
- **PASS — markers:** empty result.
- **NOT RUN — external links:** CI-only link gate.

## Unverified areas

- External URL availability.
- Performance comparisons and “fastest” claims lack benchmark evidence.
- The repository-wide format and parse gates are not green for unrelated reasons.

## Recommended next action

- Return the page and registration for a truth-first revision resolving B1–B5, then repeat final review.

---

## Verdict: REVISE

**Mode:** final  
**Structural role:** inferred — whole page; no subtree role applies  
**Stage:** unspecified  
**Independence:** evidence-only — Independence limitation: no author intent was supplied; this review was evidence-only.

## Review boundary

- **Judged:** complete target, Rust examples, registration, and relevant Avro implementation.
- **Not judged:** neighboring or Concepts documentation.
- **Consistency boundary:** repository truth.
- **Page classification:** judged as a mixed action leaf—cognitive orientation followed by operational guidance—not a formal concept page.
- **Final-state requirements not yet applicable:** phase-specific ownership rules were not inferred.

## Blocking findings

### B1 — BLOCKER: Doctest registration has incorrect path casing

- **Location:** `datafusion/core/src/lib.rs` Avro registration (lines 1366–1369)
- **Rule:** `rust-docs.mdc` §8; validation §2.3.
- **Claim:** The page has a portable doctest registration.
- **Counter-evidence:** Registration uses `creating-dataframes`; Git tracks `Creating-DataFrames`. Darwin masks this mismatch.
- **Sources:** `datafusion/core/src/lib.rs`; Git-tree path checks.
- **Impact:** Case-sensitive CI can fail before executing the page tests.
- **Direction:** Match the tracked path’s exact casing.
- **Verification:** Git-tree check — **FAIL**; local Darwin command — **platform-limited PASS**.

### B2 — BLOCKER: The implementation and schema-evolution guidance predate DataFusion 54

- **Location:** target §§“Reading Avro Files”, “Production Tips” (lines 59–72, 251–270)
- **Rule:** `markdown.mdc` §4.
- **Claim:** Avro uses `apache-avro`, whose schema-resolution rules implement `.schema()` defaults, aliases, and promotions.
- **Counter-evidence:** DataFusion 54 replaced that reader with `arrow-avro`. DataFusion’s current synchronous source passes projection—not a reader schema—to `ReaderBuilder`; explicit table schemas are subsequently handled through DataFusion’s generic batch adapter.
- **Sources:** `datafusion/datasource-avro`; Cargo manifests; [DataFusion 54 release note](https://datafusion.apache.org/blog/output/2026/06/12/datafusion-54.0.0/#new-avro-reader).
- **Impact:** Readers may rely on Avro-specific aliases/defaults that `.schema()` does not invoke as documented.
- **Direction:** Update the dependency identity and distinguish `arrow-avro` capabilities from DataFusion’s explicit-schema adaptation.
- **Verification:** Repository source and release evidence — **PASS**.

### B3 — BLOCKER: Registration does not avoid repeated header reads

- **Location:** target §“When to Use Avro” (lines 232–247)
- **Rule:** `markdown.mdc` §4.
- **Claim:** Registration caches the header and avoids subsequent header reads.
- **Counter-evidence:** Each scan opens every file, constructs a probe reader to obtain its writer schema, rewinds, and constructs the real reader.
- **Sources:** `datafusion/datasource-avro/src/source.rs` (lines 194–240).
- **Impact:** Readers receive a false I/O and repeated-query cost model.
- **Direction:** Remove the header-caching claim; limit the benefit to catalog naming and reused registration configuration.
- **Verification:** Source inspection — **PASS**.

### B4 — MAJOR: Remote whole-object buffering is omitted

- **Location:** target abstract, §§“When to Use Avro”, “Production Tips” (lines 28–34, 210–220, 249–274)
- **Rule:** `markdown.mdc` §4; reader-path unhappy-path requirement.
- **Claim:** Avro works transparently from object stores, while projection significantly reduces memory.
- **Counter-evidence:** For streaming object-store payloads, schema inference and execution call `.bytes().await`, materializing the complete object before decoding.
- **Sources:** `datafusion/datasource-avro/src/file_format.rs` (lines 107–129); `source.rs` (lines 221–240).
- **Impact:** Large remote files can create substantial memory pressure despite projected output.
- **Direction:** Disclose whole-object buffering for this reader path and qualify the projection-memory claim.
- **Verification:** Source inspection — **PASS**.

### B5 — MAJOR: Rust examples are not meaningfully verified

- **Location:** target Rust blocks (lines 87–130, 152–157, 166–193)
- **Rule:** `rust-docs.mdc` Invariants 1–3 and §§3.2, 6.
- **Claim:** The examples execute and verify reading and Hive partition behavior.
- **Counter-evidence:** One block is `rust,ignore`; the other two self-disable without `avro`. The Hive test replaces the visible partition options with defaults, replaces the directory with one file, and uses unasserted `.show()`.
- **Sources:** target source; targeted doctest output.
- **Impact:** The gate tests neither Avro execution nor the partitioning lesson.
- **Direction:** Use executable feature-enabled tests and assert the actual Hive-partition result without replacing the behavior under test.
- **Verification:** Gate criterion — **FAIL**: 2 nominal passes, 1 ignored; Avro paths did not execute.

## Non-blocking findings

### N1 — MINOR: API references do not resolve

- **Location:** target §“AvroReadOptions” and §“Production Tips” (lines 132–157, 259–264)
- **Direction:** Add reference-style definitions to exact latest docs.rs anchors.

### N2 — MINOR: Reader-visible `.collect()` is unbounded

- **Location:** target §“Reading Avro Files” (lines 102–125)
- **Direction:** Bound the demonstration or add the required memory-risk warning.

### N3 — MINOR: Final mixed-page structure is incomplete

- **Location:** target opening and H2 sequence (lines 20–280)
- **Direction:** Resolve the title-highlight fragment, place the first-H2 takeaway before its admonition, add useful method orientation and H2 dividers, and finish with a conclusion/Further Reading structure.

## Open questions

### Q1 — QUESTION: Packet page type is not a recognized single classification

- **Location:** review packet `page_type`
- **Resolves it:** Author confirms “mixed action leaf” or formal concept page; this affects Key Methods versus Concepts Covered expectations.

## Verified strengths

- The `avro` feature genuinely controls `.read_avro()` and `.register_avro()`.
- Embedded-schema inference, compatible multi-file `Schema::try_merge()`, unknown statistics, and lack of predicate pushdown match source.
- `arrow-avro` performs reader-level projection and maps records to structs and enums to dictionary arrays.
- The listed block codecs are enabled by the DataFusion Avro dependency.
- No target TODO or citation marker remains.

## Validation evidence

- **FAIL — Rust doctest:** 2 nominal passes and 1 ignored; the passing blocks self-disabled their Avro code.
- **FAIL — registration path:** casing differs from the tracked path.
- **FAIL — format command:** corpus-wide checker failed on unrelated files; the target was not reported.
- **FAIL — parse command:** repository build exited 1 for unrelated global errors.
- **PASS — target parse filter:** no target warning or error.
- **PASS — markers:** empty result.
- **NOT RUN — feature-enabled Avro doctest:** no feature-enabled argv is defined by the validation owner.
- **NOT RUN — external links:** CI-only gate.

## Unverified areas

- Actual feature-enabled execution of all examples.
- External URL availability.
- Repository-wide format and parse gates remain non-green for unrelated reasons.

## Recommended next action

- Revise the page around the DataFusion 54 `arrow-avro` implementation and resolve B1–B5 before repeating final review.

---

## CSV.md Verdict: REVISE

**Mode:** final  
**Structural role:** inferred — whole page; no subtree role applies  
**Stage:** unspecified  
**Independence:** evidence-only — Independence limitation: no author intent was supplied; this review was evidence-only.
**file:** `docs/source/library-user-guide/dataframe-api/Creating-DataFrames/from-files/csv.md`

## Review boundary

- **Judged:** complete target, Rust examples, registration, and relevant CSV implementation.
- **Not judged:** neighboring or Concepts documentation.
- **Consistency boundary:** repository truth.
- **Page classification:** mixed action leaf—cognitive orientation followed by operational guidance—not a formal concept page.
- **Final-state requirements not yet applicable:** none.

## Blocking findings

### B1 — BLOCKER: Doctest registration has incorrect path casing

- **Location:** `datafusion/core/src/lib.rs` CSV registration (lines 1371–1375)
- **Rule:** `rust-docs.mdc` §8; validation §2.3.
- **Claim:** The page has a portable doctest registration.
- **Counter-evidence:** Registration uses `creating-dataframes`; Git tracks `Creating-DataFrames`. Darwin masks the mismatch.
- **Sources:** `datafusion/core/src/lib.rs`; Git-tree path checks.
- **Impact:** Case-sensitive CI can fail before executing the page tests.
- **Direction:** Match the tracked path’s exact casing.
- **Verification:** Git-tree check — **FAIL**; local Darwin doctest — **platform-limited PASS**.

### B2 — BLOCKER: Null-handling guidance contradicts the current reader

- **Location:** target §§“CsvReadOptions” and “Production Best Practices” (lines 128, 274–304)
- **Rule:** `markdown.mdc` §4.
- **Claim:** `.null_regex()` makes custom strings null during reading, while an empty field fails after integer inference.
- **Counter-evidence:** Arrow CSV treats empty fields as null by default. DataFusion applies `null_regex` during schema inference but does not forward it to `CsvSource`’s execution-time `ReaderBuilder`.
- **Sources:** `datafusion/datasource-csv/src/file_format.rs` (lines 532–552); `source.rs` (lines 186–208); `arrow-csv` reader (default `^$` null regex).
- **Impact:** Custom markers can influence inference and then fail during execution; readers are also incorrectly warned that ordinary empty fields fail.
- **Direction:** Describe default empty-field handling accurately and do not promise execution-time custom-null handling unless the implementation supports it.
- **Verification:** Source inspection — **PASS**; dedicated runtime case — **NOT RUN**.

### B3 — BLOCKER: Explicit schemas do not guarantee drift safety

- **Location:** target §“Production Best Practices” (lines 257–366)
- **Rule:** `markdown.mdc` §4; production-safety requirement.
- **Claim:** An explicit schema “guarantees safety,” avoids data-drift failures, and can normalize headers safely.
- **Counter-evidence:** The CSV decoder skips the header and binds the supplied schema by column position and count; it does not validate header names. Reordered compatible columns can therefore acquire incorrect names without an error.
- **Sources:** `datafusion/datasource-csv/src/source.rs` (lines 186–208); `arrow-csv` `ReaderBuilder::build_decoder()` (lines 1246–1269).
- **Impact:** Schema drift can silently produce semantically incorrect data.
- **Direction:** Present the explicit schema as a positional type contract; require separate header/order validation for drift safety.
- **Verification:** Source inspection — **PASS**.

### B4 — MAJOR: Declared sort order lacks its correctness warning

- **Location:** target §“Formatting and Structure” (line 134)
- **Rule:** Gate C unhappy paths; `markdown.mdc` §5.2.
- **Claim:** `.file_sort_order()` may be used to accelerate merge joins or `ORDER BY`.
- **Counter-evidence:** `ListingTable` trusts the supplied order as output-ordering metadata. Incorrect metadata may let the optimizer omit a required sort.
- **Sources:** `datafusion/catalog-listing/src/table.rs` (lines 356–378).
- **Impact:** An inaccurate declaration can produce incorrectly ordered results.
- **Direction:** State the required ordering guarantees and warn against declaring unverified order.
- **Verification:** Source inspection — **PASS**.

### B5 — MAJOR: Schema-inference output is not verified

- **Location:** target §“Reading CSV Files” Rust block (lines 52–99)
- **Rule:** `rust-docs.mdc` Invariant 1 and §3.3; oracle calibration.
- **Claim:** The example demonstrates immediate inference of `Int64` and `Utf8`.
- **Counter-evidence:** The displayed schema is only a comment after `println!`; the doctest does not assert it, and the row table cannot distinguish numeric values from equivalent strings.
- **Sources:** target block; targeted doctest output.
- **Impact:** The documented schema claim can regress while all tests remain green.
- **Direction:** Assert the inferred fields or an exact stable schema representation.
- **Verification:** Doctest execution — **PASS**; schema oracle — **FAIL**.

### B6 — MAJOR: The I/O and storage cost model uses unsupported absolutes

- **Location:** target abstract and §§“Compression”, “When to use CSVs”, “Production Best Practices” (lines 20–24, 138–157, 219–253)
- **Rule:** `markdown.mdc` Invariants and §4.
- **Claim:** Uncompressed CSV is split across cores, every byte must always be read, compressed CSV must read the entire file, and CSV is typically “5–10x larger.”
- **Counter-evidence:** Splitting depends on configuration and a default 1 MiB threshold; scan limits can be pushed into file scans; no evidence supports the fixed size ratio.
- **Sources:** `datafusion/common/src/config.rs` (lines 1445–1480); `datafusion/datasource/src/file_scan_config/mod.rs` (lines 929–938); CSV source.
- **Impact:** Readers receive an unreliable resource and format-selection model.
- **Direction:** State verified mechanics with query/configuration conditions and remove unsupported quantitative claims.
- **Verification:** Source inspection — **PASS**; quantitative evidence — **NOT RUN / not supplied**.

## Non-blocking findings

### N1 — MINOR: API and internal links are broken

- **Location:** target option table and references (lines 120–134, 301–306, 371–373)
- **Direction:** Define the API reference links and correct the four relative paths reported by Sphinx.

### N2 — MINOR: Hidden examples use panic-based path conversion

- **Location:** target Rust blocks (lines 73 and 340)
- **Direction:** Replace `.to_str().unwrap()` with non-panicking conversion per `rust-docs.mdc` §5.1.

### N3 — MINOR: Final-page structure remains incomplete

- **Location:** target opening and H2 sequence (lines 20–373)
- **Direction:** Make the title highlight one sentence, add required H2 dividers, and finish with a conclusion/Further Reading structure.

## Open questions

### Q1 — QUESTION: Packet page type is not a recognized single classification

- **Location:** review packet `page_type`
- **Resolves it:** Author confirms “mixed action leaf” or formal concept page; this determines Key Methods versus Concepts Covered expectations.

## Verified strengths

- `.read_csv()` resolves the schema asynchronously before returning the lazy `DataFrame`; full execution waits for an action.
- The default inference limit is 1,000 records.
- Projection reaches Arrow’s CSV reader, which materializes projected arrays.
- Compressed CSV and `newlines_in_values(true)` disable intra-file repartitioning.
- Registration resolves and stores the schema once.
- All three visible Rust blocks execute locally; no target markers remain.

## Validation evidence

- **PASS — Rust doctest:** 3 passed, 0 failed, 0 ignored.
- **FAIL — registration path:** lowercase path is absent from the Git tree.
- **FAIL — parse gate:** dummy build exited 0 but emitted four target-local cross-reference warnings.
- **FAIL — format command:** corpus-wide checker failed on two unrelated plan files; `csv.md` was not reported.
- **PASS — target format localization:** target absent from the formatter’s failure list.
- **PASS — markers:** empty result.
- **NOT RUN — external links:** CI-only gate.

## Unverified areas

- External URL availability.
- Dedicated runtime reproduction of the custom `null_regex` execution path.
- No evidence supports the stated 5–10× storage ratio.
- Repository-wide formatting remains non-green for unrelated files.

## Recommended next action

- Resolve B1–B6 and the bounded N1–N3 defects, then repeat final review.

---

## Json.md Verdict: REVISE

**Mode:** final  
**Structural role:** inferred whole page  
**Stage:** unspecified  
**Independence:** evidence-only — Independence limitation: no author intent was supplied; this review was evidence-only.
**file:** `docs/source/library-user-guide/dataframe-api/Creating-DataFrames/from-files/json.md`

## Review boundary

- Judged: complete `Creating-DataFrames/from-files/json.md`.
- Not judged: other Concepts pages or unrelated documentation.
- Consistency boundary: page and repository truth.
- Final-state requirements not yet applicable: None.

## Blocking findings

### B1 — BLOCKER: Doctest registration has incorrect path casing

- **Location:** `datafusion/core/src/lib.rs` (lines 1377–1381)
- **Rule:** `rust-docs.mdc` §8.
- **Claim:** The page is registered for doctesting.
- **Counter-evidence:** Registration uses `creating-dataframes`; Git contains `Creating-DataFrames`. The lowercase path is absent from `HEAD`.
- **Sources:** `datafusion/core/src/lib.rs`; `git cat-file`.
- **Impact:** Doctest registration breaks on case-sensitive systems.
- **Direction:** Correct the registration path casing.
- **Verification:** Exact-path check — FAIL.

### B2 — BLOCKER: Current Rust examples do not compile

- **Location:** `json.md` throughout, especially lines 81–84, 223–225, 354–356
- **Rule:** `rust-docs.mdc` Invariants 1–2.
- **Claim:** Examples use `NdJsonReadOptions`.
- **Counter-evidence:** The current prelude exports `JsonReadOptions`; `NdJsonReadOptions` is deprecated and unavailable through the imported prelude.
- **Sources:** `datafusion/core/src/prelude.rs`; `options.rs`; targeted doctest.
- **Impact:** Three reader-visible examples fail compilation.
- **Direction:** Replace the obsolete type and update associated API names and links.
- **Verification:** Doctest — FAIL: 0 passed, 3 failed, 1 ignored.

### B3 — MAJOR: JSON-array example is explicitly untested

- **Location:** `json.md` §“Reading JSON Files” (lines 116–120)
- **Rule:** `rust-docs.mdc` Invariant 2.
- **Claim:** The example demonstrates array-mode reading.
- **Counter-evidence:** Its `rust,ignore` fence prevents compilation and execution.
- **Sources:** Page inspection; targeted doctest.
- **Impact:** The principal new array-mode workflow has no executable evidence.
- **Direction:** Make it a self-contained executable doctest with asserted rows.
- **Verification:** Doctest — FAIL: one ignored block.

### B4 — BLOCKER: `.newline_delimited(false)` semantics are misreported

- **Location:** `json.md` §“Reading JSON Files” and §“NdJsonReadOptions” (lines 107–175)
- **Rule:** `markdown.mdc` §4.
- **Claim:** The option handles pretty-printed objects and automatically disables range-based parallel scanning.
- **Counter-evidence:** Array mode requires a top-level array. `JsonSource` still advertises repartitioning support; if a range is assigned, `JsonOpener` returns a “does not support range-based file scanning” error.
- **Sources:** `datasource-json/src/source.rs` (lines 247–260); `utils.rs`; array range test.
- **Impact:** Pretty-printed standalone objects remain unsupported, while sufficiently large uncompressed arrays can fail unless repartitioning is disabled separately.
- **Direction:** State the exact top-level-array contract and required repartition configuration.
- **Verification:** Source and tests inspected — PASS; documented claim — FAIL.

### B5 — BLOCKER: `.mark_infinite()` is documented as functional but is ignored

- **Location:** `json.md` §“NdJsonReadOptions” (line 166)
- **Rule:** `markdown.mdc` §4.
- **Claim:** `.mark_infinite(true)` marks FIFOs or streaming inputs as unbounded.
- **Counter-evidence:** `JsonReadOptions::to_listing_options()` never reads `self.infinite`; `_read_type()` only receives the resulting listing options.
- **Sources:** `options.rs` (lines 715–745); `execution/context/mod.rs` (lines 1713–1757).
- **Impact:** Readers may design streaming plans around a no-op option.
- **Direction:** Remove the recommendation or document only behavior supported by an implemented unbounded source.
- **Verification:** Source inspection — PASS.

### B6 — MAJOR: I/O behavior is described in unsupported absolutes

- **Location:** `json.md` §“Reading JSON Files” and §“When to Use JSON” (lines 56–63, 264–278)
- **Rule:** `markdown.mdc` §4.
- **Claim:** Inference reads only the beginning, while execution always reads every line and byte.
- **Counter-evidence:** Stream-backed object-store inference calls `r.bytes()` before sampling, buffering the complete object. Conversely, scan limits and cancellation can terminate execution before every row or byte is consumed.
- **Sources:** `datasource-json/src/file_format.rs` (lines 300–320); `FileScanConfig`; `ListingTable` limit propagation.
- **Impact:** Readers receive an incorrect memory and I/O cost model.
- **Direction:** Qualify behavior by local versus stream-backed stores, and by full scans versus limits/cancellation.
- **Verification:** Source inspection — PASS.

### B7 — BLOCKER: `.file_sort_order()` is recommended without its correctness precondition

- **Location:** `json.md` §“NdJsonReadOptions” (line 164)
- **Rule:** `markdown.mdc` §4 and §5.2.
- **Claim:** Declaring sort order can accelerate joins or `ORDER BY`.
- **Counter-evidence:** `ListingTable` trusts the supplied ordering without validating the data.
- **Sources:** `catalog-listing/src/table.rs` (lines 356–378).
- **Impact:** A false declaration can let the optimizer omit required sorts and produce incorrect results.
- **Direction:** Add a correctness warning requiring verified physical ordering.
- **Verification:** Source inspection — PASS.

### B8 — MAJOR: Explicit schemas are presented as strict input validation

- **Location:** `json.md` §“Production Best Practices” (lines 304–325)
- **Rule:** `markdown.mdc` §4.
- **Claim:** An explicit schema “guarantees safety” and enforces a strict schema contract.
- **Counter-evidence:** Arrow’s JSON reader defaults `strict_mode` to `false`; fields absent from the schema are silently ignored. DataFusion does not enable strict mode.
- **Sources:** `arrow-json` `ReaderBuilder`; `datasource-json/src/source.rs`.
- **Impact:** Production pipelines can silently discard unexpected fields.
- **Direction:** Limit the guarantee to declared-field types/nullability and disclose unknown-field handling.
- **Verification:** Source inspection — PASS.

### B9 — MAJOR: Schema-inference output lacks an oracle

- **Location:** `json.md` §“Reading JSON Files” (lines 84–87)
- **Rule:** `rust-docs.mdc` §3.3; judge oracle calibration.
- **Claim:** The example demonstrates the inferred schema.
- **Counter-evidence:** It only prints `df.schema()`; no schema output is shown or checked.
- **Sources:** Page inspection; targeted doctest.
- **Impact:** Compilation would not verify the documented inferred field types.
- **Direction:** Show the stable schema representation and verify the material inference claim.
- **Verification:** Oracle inspection — FAIL; execution additionally blocked by B2.

## Non-blocking findings

### N1 — MINOR: API and internal links are broken or incomplete

- **Location:** `json.md` §“NdJsonReadOptions” and §“JSON References” (lines 148–166, 390–392)
- **Direction:** Define the missing reference-style API links using `JsonReadOptions`; correct the SQL-options relative path and target.

### N2 — MINOR: Final-page structure is incomplete

- **Location:** `json.md` whole page
- **Direction:** Resolve the missing orientation table, conclusion, and required H2 dividers under the selected page classification.

### N3 — MINOR: Hidden setup uses panic-based error handling

- **Location:** `json.md` Rust examples (lines 83, 229–234, 353)
- **Direction:** Replace normal-path `.unwrap()` calls with `?` or non-panicking conversion.

### N4 — MINOR: Nested-field wording implies incorrect DataFrame access syntax

- **Location:** `json.md` §“Reading JSON Files” (lines 59–61)
- **Direction:** Say nested objects remain Arrow struct columns and show DataFrame access through `col("user").field("name")`, rather than implying `user.name`.

## Open questions

### Q1 — QUESTION: Page classification conflicts with the governing model

- **Location:** `json.md` whole page
- **Resolves it:** Confirm whether this is a normal mixed leaf—cognitive opening followed by actions—or a concept page, which must remain cognitive and use the concept-page shape.

## Verified strengths

- `read_json()` resolves schema before returning a lazy `DataFrame`.
- Current source supports both NDJSON and top-level JSON arrays.
- Projection is pushed into `JsonSource`.
- Compression prevents generic byte-range repartitioning.
- Explicit field nullability is enforced for declared fields.

## Validation evidence

- **FAIL — format:** Corpus check failed only on `ConceptsRevisePlan.md` and `FromFilesRevisePlan.md`; no target-specific formatting failure was reported.
- **FAIL — parse:** `json.md` emits a missing internal-reference warning for the SQL format-options link.
- **FAIL — Rust doctests:** 0 passed, 3 failed, 1 ignored.
- **FAIL — registration integrity:** Registered path casing does not exist in Git.
- **PASS — markers:** No `TODO:` or `citation-needed` markers.
- **NOT RUN — external links:** CI-only link gate was not executed.

## Unverified areas

- External URL availability was not checked.
- No runtime object-store benchmark was run; the I/O findings rely on inspected execution paths.

## Recommended next action

- Return the page to authoring for correction of B1–B9, then rerun final review.

---

## Parquet.md Verdict: REVISE

**Mode:** final  
**Structural role:** inferred whole page  
**Stage:** unspecified  
**Independence:** evidence-only — Independence limitation: no author intent was supplied; this review was evidence-only.
**file:** `docs/source/library-user-guide/dataframe-api/Creating-DataFrames/from-files/parquet.md`

## Review boundary

- Judged: complete `Creating-DataFrames/from-files/parquet.md`.
- Not judged: other Concepts pages or planning documents.
- Consistency boundary: page and repository truth.
- Final-state requirements not yet applicable: None.

## Blocking findings

### B1 — BLOCKER: Doctest registration has incorrect path casing

- **Location:** `datafusion/core/src/lib.rs` (lines 1383–1387)
- **Rule:** `rust-docs.mdc` §8.
- **Claim:** The page has a valid doctest registration.
- **Counter-evidence:** Registration uses `creating-dataframes`; Git contains `Creating-DataFrames`. The lowercase path is absent from `HEAD`.
- **Sources:** `datafusion/core/src/lib.rs`; `git cat-file`.
- **Impact:** Registration breaks on case-sensitive systems.
- **Direction:** Correct the registered path casing.
- **Verification:** Exact-path check — FAIL.

### B2 — MAJOR: Two examples test different code from what readers see

- **Location:** `parquet.md` §“ParquetReadOptions” and §“Explicit schemas” (lines 160–194, 274–297)
- **Rule:** `rust-docs.mdc` §§3.2, 5.1, 6; oracle calibration.
- **Claim:** The examples demonstrate configured and explicit-schema reads.
- **Counter-evidence:** Hidden lines replace each visible `options` value with `ParquetReadOptions::default()`. Both examples use `.show()` without asserted output.
- **Sources:** Page inspection; targeted doctest.
- **Impact:** Passing doctests do not verify either lesson.
- **Direction:** Execute the visible options and collect/assert representative rows.
- **Verification:** Compilation — PASS; demonstration oracle — FAIL.

### B3 — MAJOR: Opening understates schema-resolution and metadata I/O

- **Location:** `parquet.md` §“Reading Parquet Files” and §“Production Best Practices” (lines 57–62, 228–255)
- **Rule:** `markdown.mdc` §4.
- **Claim:** Parquet needs “no inference step” and reads only a few kilobytes.
- **Counter-evidence:** Without an explicit schema, `read_parquet()` invokes `ParquetFormat::infer_schema()`, fetches metadata for every listed file, and merges those schemas. The default remote footer prefetch is 512 KiB per file.
- **Sources:** `execution/context/mod.rs` (lines 1741–1753); `datasource-parquet/src/file_format.rs` (lines 329–403); `config.rs`.
- **Impact:** Readers underestimate multi-file planning latency and I/O.
- **Direction:** Distinguish metadata-only schema inference from row-data sampling and state its per-file cost.
- **Verification:** Source inspection — PASS; documented claim — FAIL.

### B4 — BLOCKER: `.file_sort_order()` lacks its correctness precondition

- **Location:** `parquet.md` §“ParquetReadOptions” (line 127)
- **Rule:** `markdown.mdc` §4 and §5.2.
- **Claim:** Declaring sort order can avoid re-sorting.
- **Counter-evidence:** `ListingTable` trusts user-supplied ordering without validating the file data.
- **Sources:** `catalog-listing/src/table.rs` (lines 356–378).
- **Impact:** A false declaration can produce incorrect results.
- **Direction:** Add a warning requiring verified physical ordering.
- **Verification:** Source inspection — PASS.

### B5 — BLOCKER: Encryption option signature and feature requirement are wrong

- **Location:** `parquet.md` §“ParquetReadOptions” (line 128)
- **Rule:** `markdown.mdc` §4.
- **Claim:** `.file_decryption_properties(Option)` configures modular encryption.
- **Counter-evidence:** The method accepts `ConfigFileDecryptionProperties`, not `Option`. Decryption requires the non-default `parquet_encryption` feature; without it the configured properties are ignored.
- **Sources:** `options.rs` (lines 339–345); `datafusion/core/Cargo.toml`; `datasource-parquet/src/file_format.rs`.
- **Impact:** Following the documented API either fails compilation or fails to decrypt.
- **Direction:** Correct the signature and state the feature requirement.
- **Verification:** Source and feature inspection — PASS; documented contract — FAIL.

### B6 — MAJOR: Statistics-based pruning is presented as universally available

- **Location:** `parquet.md` opening, §“Production Best Practices”, and §“Metadata-based pruning” (lines 25–30, 232–237, 327–343)
- **Rule:** `markdown.mdc` §4.
- **Claim:** Parquet embeds statistics and DataFusion automatically skips irrelevant row groups.
- **Counter-evidence:** Parquet statistics are optional. DataFusion retains row groups when required statistics are missing.
- **Sources:** `datasource-parquet/src/metadata.rs`; `row_group_filter.rs` missing-statistics test.
- **Impact:** Readers may expect pruning from files whose producers omitted statistics.
- **Direction:** Make producer-provided statistics an explicit prerequisite.
- **Verification:** Source and tests inspected — PASS.

### B7 — MAJOR: Advanced-index integration is described through the wrong API

- **Location:** `parquet.md` §“Advanced indexing” (lines 400–412)
- **Rule:** `markdown.mdc` §4.
- **Claim:** A custom `ParquetFileReaderFactory` supplies a prebuilt `ParquetAccessPlan`.
- **Counter-evidence:** Access plans are attached to `PartitionedFile` extensions. `ParquetFileReaderFactory` only creates an `AsyncFileReader`.
- **Sources:** `datasource-parquet/src/access_plan.rs`; `opener/mod.rs`; `core/tests/parquet/external_access_plan.rs`.
- **Impact:** Readers cannot implement the documented integration path.
- **Direction:** Describe the `PartitionedFile::with_extension()` path and separately explain any custom-reader role.
- **Verification:** Source and tests inspected — PASS; documented path — FAIL.

### B8 — MAJOR: Universal performance ranking is unsupported

- **Location:** `parquet.md` opening and §“Why Parquet Performs” (lines 20–30, 300–325)
- **Rule:** Judge Gate A; `markdown.mdc` §4.
- **Claim:** Parquet is DataFusion’s “highest-performing format” and its advantages grow with data volume.
- **Counter-evidence:** Repository evidence establishes workload-specific pruning and projection benefits, not a universal ranking across formats, schemas, storage, or query shapes.
- **Sources:** Parquet source; linked pruning and filter-pushdown analyses.
- **Impact:** Readers may select a format using an unconditional performance promise.
- **Direction:** Qualify the recommendation by analytical workload and selective-query conditions.
- **Verification:** Supporting mechanisms — PASS; universal claim — FAIL.

## Non-blocking findings

### N1 — MINOR: StringView is default, not “always on”

- **Location:** `parquet.md` §“Why Parquet Performs” (lines 309–313)
- **Direction:** State that `schema_force_view_types` defaults to `true` and can be disabled or superseded by the requested table schema.

### N2 — MINOR: API and internal links are incomplete

- **Location:** `parquet.md` §“ParquetReadOptions” and §“Parquet References” (lines 120–130, 426–435)
- **Direction:** Define the missing reference-style method links and correct the SQL format-options relative path.

### N3 — MINOR: Final-page structure is incomplete

- **Location:** `parquet.md` whole page
- **Direction:** After resolving Q1, add the applicable orientation table, H2 dividers, and conclusion; normalize action-subsection highlights.

## Open questions

### Q1 — QUESTION: Page classification conflicts with the governing model

- **Location:** `parquet.md` whole page
- **Resolves it:** Confirm whether this is a normal mixed leaf—cognitive opening followed by actions—or a concept page, which must remain cognitive throughout.

## Verified strengths

- Projection pushdown, row-group pruning, page-index pruning, and Bloom-filter pruning exist as described when their metadata is available.
- Effective defaults are pruning `true`, metadata skipping `true`, metadata hint 512 KiB, filter pushdown `false`, and filter reordering `false`.
- Statistics collection defaults to `true` and `.with_collect_statistics(false)` is current.
- The cited filter-pushdown benchmark supports the contextual “up to 2.2x” result.
- The first read example asserts its rows successfully.

## Validation evidence

- **FAIL — format:** Corpus check failed on three unrelated planning files; `parquet.md` was not identified.
- **FAIL — parse:** `parquet.md` emits a missing internal-reference warning for the SQL format-options link.
- **PASS — Rust doctests:** 4 passed, 0 failed, 0 ignored; B2 remains an oracle failure.
- **FAIL — registration integrity:** Registered path casing does not exist in Git.
- **PASS — markers:** No `TODO:` or `citation-needed` markers.
- **PASS — cited pruning/pushdown pages:** Both referenced pages were available and supported their scoped claims.
- **NOT RUN — full external-link gate:** CI-only link check was not executed.

## Unverified areas

- The StringView 20–200% benchmark and remaining external URLs were not independently verified.

## Recommended next action

- Return the page to authoring for correction of B1–B8, then rerun final review.
