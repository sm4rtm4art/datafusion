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

- JOIN-TODO-001 [iterative structural pass; title and first H2 resolved] Keep the
  approved page title "Joining DataFrames." Add the page abstract and Key
  Methods table when their destinations have stabilized, and continue building
  the approved action-oriented hierarchy one H2 subtree at a time:
  orientation -> construction -> preservation -> composition -> validation ->
  conclusion. Do not preserve the current Basic/Intermediate/Advanced labels.
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
- JOIN-TODO-015 [cleanup pass] Merge the overlapping join-family, API-comparison,
  and cheat-sheet tables into one opening Key Methods table and one preservation
  table. Add a conclusion and prune duplicate or low-authority Further Reading
  links. Verify anchors and incoming links after headings change.

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

- JOIN-TODO-019 [deferred; example-normalization pass] `index.md` was not
  supplied. Confirm page order, shared-dataset ownership, and whether
  customers_df/orders_df/payments_df is the approved running dataset before
  normalizing examples.
- JOIN-TODO-020 [deferred; plan-inspection pass] Identify the documentation
  owner for extracted execution and optimizer material. Do not create
  join-concepts.md by default; transformation-concepts.md already owns the
  condensed conceptual transition.
- JOIN-TODO-021 [final pass] Confirm scope boundaries: transformation-concepts.md
  owns the broader frame-boundary model; set-operations.md owns whole-row
  alignment/combination; the preceding aggregation/window group owns the final
  one-frame across-row stage. This page owns join construction, preservation,
  composition, validation, and bounded plan inspection.
- JOIN-TODO-022 [deferred; execution-ownership pass] Decide whether guidance on
  joining inside a source system versus in DataFusion has a supported owner and
  an action-oriented use case. Do not restore the removed broad Postgres-versus-
  DataFusion performance comparison without authoritative, scenario-specific
  support.
-->

<!-- JOIN-TODO-001: Title resolved; rebuild the remaining hierarchy one H2 subtree at a time. -->

# Joining DataFrames

:::{admonition} Style Note
:class: note
:collapsible: closed

In this document, code elements follow a consistent pattern:

- **DataFrame methods:** `.method()` (e.g., `.select()`, `.filter()`)
- **Standalone functions:** `function()` (e.g., `col()`, `lit()`)
- **Constructors:** `Type::new()` (e.g., `SessionContext::new()`)
- **Types:** `TypeName` (e.g., `SchemaRef`, `RecordBatch`)
- **Lazy transformations:** return a `DataFrame` and build the `LogicalPlan`
- **Actions:** (`.collect()`, `.show()`) trigger execution

:::

```{contents} Table of Content
:local:
:depth: 2
```

<!-- JOIN-TODO-002 JOIN-TODO-003: Resolved in the first-H2 orientation pass. -->

## Relate Rows Across DataFrames

**A join crosses the frame boundary by relating rows from two logical inputs; the matching relationship determines which rows pair, and the join type determines which matches and non-matches the result preserves.**

Where earlier transformations reshape one `DataFrame`, a join introduces a left and a right input. Three choices define the result: the two inputs, the key columns or condition that relate their rows, and the [`JoinType`] that controls preservation. This makes joins useful for enriching records with related data, filtering by whether a relationship exists, and reconciling records across systems.

The matches can change both the schema and the number of rows. Most join types carry columns from both inputs into the result, while semi and anti joins use the other input only to test for a match and return columns from one side. In joins that emit matched row pairs, one-to-many and many-to-many relationships can repeat input rows. A row with no match may disappear or be preserved with `NULL` values, depending on the join type.

This row relationship distinguishes joins from [set operations]. A join correlates rows using keys or a condition and often places columns from the inputs side by side. A set operation aligns complete rows under a compatible schema to concatenate or compare them. See [Transformation Concepts] for the broader frame-boundary model and [Set Operations] when the task is whole-row combination rather than row matching.

:::{admonition} Choose the API That Makes the Join Logic Clear
:class: note

Use the DataFrame API when Rust code needs to generate the relationship conditionally or compose the joined result directly with other transformations. [`.join()`] expresses named equality keys, while [`.join_on()`] accepts expression conditions.

Use SQL when a fixed multi-table relationship or a join form expressed only in SQL is clearer to read and maintain. Both APIs produce DataFusion logical plans and use the same optimizer and execution engine. Choose between them for clarity, maintainability, and composition—not for an assumed execution-speed advantage.

:::

With the frame boundary established, the first practical decision is how to express the matching relationship: as named key columns or as expression conditions.

<!-- JOIN-TODO-004 JOIN-TODO-005: The construction boundary is established; revise this H2 subtree in the next iteration. -->

## Build Joins with Keys and Conditions

<!-- JOIN-TODO-004: Move the signature and method-choice material into separate `.join()` and `.join_on()` construction subsections; preserve AND/OR behavior but remove physical-algorithm guarantees. -->

The [`.join()`] method signature in the datafusion dataframe-API:

```rust
use datafusion::prelude::*;  // Includes JoinType

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let left_df = dataframe!("id" => [1, 2])?;
    let right_df = dataframe!("customer_id" => [1, 2])?;

    let joined = left_df.join(
        right_df,                    // 1. Right DataFrame
        JoinType::Inner,             // 2. Join type
        &["id"],                     // 3. Left key columns
        &["customer_id"],            // 4. Right key columns
        None,                        // 5. Optional filter expression
    )?;

    joined.show().await?;
    Ok(())
}
```

**Two ways to specify joins:**

- [`.join()`] — Pass column names (`&[&str]`) for each side plus an optional `filter: Option<Expr>`. DataFusion builds equality predicates from the columns.
- [`.join_on()`] — Pass the full join condition as `Expr`s. Internally this wraps [`.join()`] with empty key lists and a combined filter expression (`expr_1 AND expr_2 ...`). Optimizer passes then extract equality predicates and treat them as equi-join keys.

<!-- JOIN-TODO-004: Rewrite this claim; construction method does not guarantee a specific physical join algorithm. -->

After optimization, both methods produce equivalent plans—**no performance difference** for standard equi-joins. However, [`.join()`] is the "safer" choice: you explicitly declare equi-join keys, guaranteeing hash/sort-merge algorithms. With [`.join_on()`], if the optimizer can't extract equality predicates from your expression, it may fall back to nested loop joins.

Pick whichever reads better for your use case.

**Gotcha: [`.join_on()`] uses AND, not OR**

Multiple expressions passed to [`.join_on()`] are combined with [`AND`]:

```rust
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let left = dataframe!("a" => [1], "b" => [2])?.alias("l")?;
    let right = dataframe!("a2" => [1], "b2" => [2])?.alias("r")?;
    // This means: a = a2 AND b = b2 (not OR!)
    let joined = left.join_on(right, JoinType::Inner, [col("l.a").eq(col("r.a2")), col("l.b").eq(col("r.b2"))])?;
    joined.show().await?;
    Ok(())
}
```

For [`OR`] logic, build a single expression:

```rust
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let left = dataframe!("a" => [1], "b" => [2])?.alias("l")?;
    let right = dataframe!("a2" => [1], "b2" => [2])?.alias("r")?;
    // Match if EITHER a or b matches
    let joined = left.join_on(right, JoinType::Inner, [col("l.a").eq(col("r.a2")).or(col("l.b").eq(col("r.b2")))])?;
    joined.show().await?;
    Ok(())
}
```

**Trade-off: DataFrame vs SQL**

| DataFrame API Advantages                                                                                                                                                   | SQL Advantages                                                        |
| -------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------- |
| **First-class Semi/Anti joins** — `JoinType::LeftAnti`, `LeftSemi` etc. are explicit; no workarounds needed (unlike PySpark where you'd use `LEFT JOIN` + `WHERE IS NULL`) | **Visual clarity** — Multi-table joins read naturally in SQL syntax   |
| **Type-safe composition** — Build joins conditionally with `if/else`; compiler catches column typos                                                                        | **Familiar syntax** — Standard `ON` clause understood by any SQL user |
| **Chained transformations** — `.join().filter().select()` flows naturally                                                                                                  | Copy-paste ready\*\* — Test queries directly in SQL tools             |
| **Complex conditions** — [`.join_on()`] accepts any `Expr`, not just column equality                                                                                       | **Self-documenting** — SQL is often readable by non-programmers       |

> **DataFusion-specific advantage:** Unlike many DataFrame libraries, DataFusion exposes the _full_ set of join types ([`LeftSemi`], [`RightSemi`], [`LeftAnti`], [`RightAnti`], [`LeftMark`], [`RightMark`]) as first-class operations—no need to emulate anti-joins with outer joins and null checks.

**Performance note:** <br>
For joins via row-based [`TableProvider`], consider whether the join should happen at the source. If both tables are in Postgres with foreign key indexes, the DB's index-backed joins may outperform transferring data to DataFusion. For cross-source joins or large analytical joins without indexes, DataFusion's hash/sort-merge algorithms excel.

<!-- JOIN-TODO-013 JOIN-TODO-016: Trim this to plan interpretation or move it to the execution owner; verify all operator, optimizer, Arrow, SIMD, late-materialization, and benchmark claims. -->

### How Joins Execute

Under the hood, DataFusion selects from [several join algorithms] based on your data:

| Algorithm                  | When Used                                                                                      |
| :------------------------- | :--------------------------------------------------------------------------------------------- |
| [**Hash Join**]            | Default for equi-joins (`=`). Builds a hash table on the smaller side, probes with the larger. |
| [**Sort-Merge Join**]      | Pre-sorted inputs; can spill to disk for huge datasets.                                        |
| [**Symmetric Hash Join**]  | Streaming/unbounded data—both sides build hash tables, rows pruned via sliding windows.        |
| [**Nested Loop Join**]     | General non-equi conditions where hash-based algorithms don't apply.                           |
| [**Piecewise Merge Join**] | Single range filter (`<`, `>`, `<=`, `>=`)—much faster than nested loop for these cases.       |
| [**Cross Join**]           | Cartesian product—used for SQL [`CROSS JOIN`] and [`.join()`] with empty key lists.            |

The optimizer _can_ (based on configuration and statistics):

- **Swap sides** to put the smaller table on the build side
- **Choose partition mode**—broadcast small tables or hash-partition both sides
- **Push dynamic filters**—min/max bounds from the build side skip irrelevant probe data (e.g., Parquet row groups)

These behaviors are tunable via [`datafusion.optimizer`] settings.

All join algorithms leverage [Arrow]'s columnar format: instead of copying rows, DataFusion computes index arrays and uses vectorized [`take()`] operations to assemble results efficiently.

> **Why DataFusion Joins Are Fast**
>
> Unlike traditional row-based databases, DataFusion combines several modern techniques:
>
> | Technique                          | Benefit                                                                                                                                          |
> | ---------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------ |
> | **Columnar format (Arrow)**        | Read only the columns you need; SIMD instructions process thousands of keys in parallel\*                                                        |
> | **Vectorized execution**           | Joins process batches of rows, not one at a time—simple inner loops let CPUs parallelize at the instruction level                                |
> | **SQL = DataFrame**                | Both compile to the same `LogicalPlan`—identical optimizer benefits regardless of API choice                                                     |
> | **Statistics-driven optimization** | Table metadata (row counts, min/max) guide join order and algorithm selection—[**16x faster** on TPC-H benchmarks][datafusion join optimization] |
> | **Late materialization**           | During joins, only key columns + row indices are processed; other columns are fetched afterward                                                  |
>
> \*SIMD requires `RUSTFLAGS='-C target-cpu=native'`. See [Crate Configuration](../../../user-guide/crate-configuration.md).
>
> The result: you describe _what_ to join, and the optimizer handles _how_—often matching or exceeding hand-tuned imperative code.

<!-- JOIN-TODO-006 JOIN-TODO-007 JOIN-TODO-015: Rebuild this as the single preservation decision table and add only a bounded specialist note for mark joins. -->

### Join Types at a Glance

Joins control how rows from two tables are matched and combined. The key decisions are:

1. what happens to rows that _don't_ match
2. which columns appear in the result.

Inner joins discard non-matches; outer joins preserve them with NULLs. Semi and Anti joins answer existence questions without adding columns from the right table.

| Join Type            | Returns                   | Use Case                                    |
| :------------------- | :------------------------ | :------------------------------------------ |
| [`Inner`]            | Matches from both sides   | Standard join—only matching rows            |
| [`Left`]             | All left + matching right | Keep all left rows (NULL if no match)       |
| [`Right`]            | All right + matching left | Keep all right rows (NULL if no match)      |
| [`Full`]             | Everything from both      | See all data, matched or not                |
| [`LeftSemi`]         | Left rows WITH matches    | "Which left rows have a match?"             |
| [`LeftAnti`]         | Left rows WITHOUT matches | "Which left rows have NO match?"            |
| ~~Cross~~ (SQL only) | Cartesian product         | All combinations (see Anti-Pattern section) |

> **Note:** The DataFrame API has no `JoinType::Cross`. Cartesian products are represented as `Inner` joins with empty key lists or as [`CROSS JOIN`] in SQL.

> **Learn more:** You may want to check out this source [Join tutorial] or [Semi and Anti joins explained].

<!-- JOIN-TODO-006 JOIN-TODO-009: Rename and place this under preservation semantics; explain possible row multiplication and route coverage checks to validation. -->

### Basic: The Inner Join

This example establishes `customers_df` and `orders_df`—used throughout this section. Note: Carol has no orders, and order 104 has no matching customer (orphan).

```rust
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    // Sample data: customers_df [id, name] — Carol has no orders
    let customers_df = dataframe!(
        "id" => [1, 2, 3],
        "name" => ["Alice", "Bob", "Carol"]
    )?;

    // Sample data: orders_df [order_id, customer_id, amount] — order 104 is orphaned
    let orders_df = dataframe!(
        "order_id" => [101, 102, 103, 104],
        "customer_id" => [1, 1, 2, 99],
        "amount" => [100, 200, 150, 300]
    )?;

    // Inner join: only matching rows (Carol excluded, order 104 excluded)
    let result = customers_df.clone().join(
        orders_df.clone(),
        JoinType::Inner,
        &["id"],
        &["customer_id"],
        None
    )?;

    result.show().await?;
    // +----+-------+----------+-------------+--------+
    // | id | name  | order_id | customer_id | amount |
    // +----+-------+----------+-------------+--------+
    // | 1  | Alice | 101      | 1           | 100    |
    // | 1  | Alice | 102      | 1           | 200    |
    // | 2  | Bob   | 103      | 2           | 150    |
    // +----+-------+----------+-------------+--------+

    Ok(())
}
```

**When to use Inner Join:**

- **Enrich data** — Attach related information (customer details → their orders)
- **Filter by relationship** — Keep only rows that have a match on the other side
- **Combine normalized tables** — Reassemble data split across multiple tables

**Not for set intersections!** <br>
If you need rows that exist in _both_ DataFrames (identical schemas, all columns compared), use [`.intersect()`] instead—that's a set operation, not a join. <br> For set operations like intersection and difference, see [Set Operations](set-operations.md#intersection-and-difference).

> **⚠️ The hidden cost: [Survivorship bias][survivorship_bias]**
>
> Many join types silently drop non-matching rows—Inner, Semi, and Anti joins all filter out data. In the example above, Carol and order 104 simply vanish. Chain several such joins together and you may lose 60% of your data without noticing—you only see the "survivors" (rows that matched at every step).
>
> As a sanity check, if you need to see what's _missing_, use [Outer Joins](#intermediate-leftrightfull-joins) (or other oposit joins like left vs. right) instead—`NULL` values reveal exactly where data gaps exist.

<!-- JOIN-TODO-004 JOIN-TODO-008 JOIN-TODO-016 JOIN-TODO-018: Move composite keys to construction, schema-name handling to composition, and remove the unsupported temporal percentage claim. -->

### Intermediate: Multi-Key Joins

Join on multiple columns when a single key isn't enough to uniquely identify matches—common with composite keys or temporal constraints.

```rust
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    // Regional sales: same product_id can exist in different regions
    let inventory_df = dataframe!(
        "product_id" => [1, 1, 2, 2],
        "region" => ["East", "West", "East", "West"],
        "stock" => [100, 50, 200, 75]
    )?;

    // Use different column names to avoid duplicate field error
    let sales_df = dataframe!(
        "sale_product_id" => [1, 1, 2],
        "sale_region" => ["East", "West", "East"],
        "sold" => [30, 20, 80]
    )?;

    // Multi-key join: match on BOTH product_id AND region
    let joined = inventory_df.join(
        sales_df,
        JoinType::Left,  // Keep all inventory, even unsold
        &["product_id", "region"],
        &["sale_product_id", "sale_region"],
        None
    )?;

    joined.show().await?;
    // +------------+--------+-------+-----------------+-------------+------+
    // | product_id | region | stock | sale_product_id | sale_region | sold |
    // +------------+--------+-------+-----------------+-------------+------+
    // | 1          | East   | 100   | 1               | East        | 30   |
    // | 1          | West   | 50    | 1               | West        | 20   |
    // | 2          | East   | 200   | 2               | East        | 80   |
    // | 2          | West   | 75    |                 |             |      |
    // +------------+--------+-------+-----------------+-------------+------+

    Ok(())
}
```

**To avoid duplicate columns**, use different column names on the right side, then select only what you need:

```rust
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let inventory_df = dataframe!(
        "product_id" => [1, 1, 2, 2],
        "region" => ["East", "West", "East", "West"],
        "stock" => [100, 50, 200, 75]
    )?;

    // Use different column names for join keys on right side
    let sales_df = dataframe!(
        "sale_product_id" => [1, 1, 2],
        "sale_region" => ["East", "West", "East"],
        "sold" => [30, 20, 80]
    )?;

    let joined = inventory_df.join(
        sales_df,
        JoinType::Left,
        &["product_id", "region"],
        &["sale_product_id", "sale_region"],
        None
    )?;

    // Select only the columns you need (left-side keys + data)
    let result = joined.select(vec![
        col("product_id"),
        col("region"),
        col("stock"),
        col("sold"),
    ])?;

    result.show().await?;
    // +------------+--------+-------+------+
    // | product_id | region | stock | sold |
    // +------------+--------+-------+------+
    // | 1          | East   | 100   | 30   |
    // | 1          | West   | 50    | 20   |
    // | 2          | East   | 200   | 80   |
    // | 2          | West   | 75    |      |  ← No sales
    // +------------+--------+-------+------+

    Ok(())
}
```

**⚠️ Handling Same-Named Columns**

DataFusion's [`.join()`] preserves columns from both sides. When join keys share names, use one of these patterns:

| Pattern                        | When to use                                                |
| ------------------------------ | ---------------------------------------------------------- |
| **[`.select()`] after join**   | Simple joins—pick the columns you need                     |
| **[`.alias()`] before join**   | Complex multi-way joins—qualify with `col("alias.column")` |
| **[`.with_column_renamed()`]** | Rename conflicting columns before joining                  |

**Tip:** Call [`.schema()`] after joining to see actual column names.

> **Pro tip for time-dependent data:** <br>

<!-- JOIN-TODO-016: Delete or replace this unsupported temporal mismatch statistic with a scenario-specific, sourced example. -->

> Multi-key joins on temporal columns work well when truncated to appropriate granularity using [`date_trunc()`]. Joining on `DATE` (day) has minimal edge cases (~0.004% at midnight); joining on raw `TIMESTAMP` (milliseconds) risks silent mismatches.

(intermediate-leftrightfull-joins)=

<!-- JOIN-TODO-006 JOIN-TODO-016: Consolidate outer-join preservation and null extension here; remove the unsupported "~90%" claim. -->

### Intermediate: Left/Right/Full Joins

Where Inner Join keeps only the intersection (rows matching on both sides), **"partial" outer joins (left, right and full) preserve rows that don't match**—filling missing columns with `NULL`. This makes data gaps visible instead of silently dropping them.

| Join Type | Keeps                                           | Typical Use Case                                        |
| :-------- | :---------------------------------------------- | :------------------------------------------------------ |
| **Left**  | All left rows, matching right data if available | Customer reports—keep all customers, show orders if any |
| **Right** | All right rows, matching left data if available | Orphan detection—find orders without valid customers    |
| **Full**  | Everything from both sides                      | Data reconciliation—find ALL discrepancies              |

<!-- JOIN-TODO-016: Remove the unsourced "~90%" generalization; retain only neutral selection guidance. -->

Left Join handles ~90% of outer join use cases. Right Join can usually be rewritten as Left Join by swapping tables. Full Join is for reconciliation scenarios.

#### Left Join — Enrich Your Primary Data

Keep **all rows from the left table**, enrich with matching data from the right table. No match? Right-side columns become `NULL`.

```rust
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    // customers_df:
    // +----+-------+
    // | id | name  |
    // +----+-------+
    // | 1  | Alice |
    // | 2  | Bob   |
    // | 3  | Carol |  ← Has no orders
    // +----+-------+
    let customers_df = dataframe!(
        "id" => [1, 2, 3],
        "name" => ["Alice", "Bob", "Carol"]
    )?;

    // orders_df:
    // +----------+-------------+--------+
    // | order_id | customer_id | amount |
    // +----------+-------------+--------+
    // | 101      | 1           | 100    |
    // | 102      | 1           | 200    |
    // | 103      | 2           | 150    |
    // | 104      | 99          | 300    |  ← Orphan
    // +----------+-------------+--------+
    let orders_df = dataframe!(
        "order_id" => [101, 102, 103, 104],
        "customer_id" => [1, 1, 2, 99],
        "amount" => [100, 200, 150, 300]
    )?;

    // "All customers with their orders (if any)"
    let left_result = customers_df.clone().join(
        orders_df.clone(),
        JoinType::Left,
        &["id"],
        &["customer_id"],
        None
    )?;

    left_result.show().await?;
    // +----+-------+----------+-------------+--------+
    // | id | name  | order_id | customer_id | amount |
    // +----+-------+----------+-------------+--------+
    // | 1  | Alice | 101      | 1           | 100    |
    // | 1  | Alice | 102      | 1           | 200    |
    // | 2  | Bob   | 103      | 2           | 150    |
    // | 3  | Carol |          |             |        |  ← Preserved with NULLs
    // +----+-------+----------+-------------+--------+

    Ok(())
}
```

(self-joins-and-qualified-columns)=

<!-- JOIN-TODO-008: Move self-joins to composition after the shared aliasing, qualification, renaming, and projection guidance. -->

#### Self-Joins and Qualified Columns

A **self-join** joins a table with itself—essential for hierarchical data. Left Join preserves all rows even if they have no match (like Alice, who has no referrer).

```rust
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    // Extended customers: add referred_by (which customer referred them)
    let customers_referrals = dataframe!(
        "id" => [1, 2, 3],
        "name" => ["Alice", "Bob", "Carol"],
        "referred_by" => [None::<i64>, Some(1), Some(1)]  // Alice referred Bob and Carol
    )?;

    // Self-join: alias both sides to disambiguate
    let customer = customers_referrals.clone().alias("customer")?;
    let referrer = customers_referrals.clone().alias("referrer")?;

    let with_referrers = customer.join(
        referrer,
        JoinType::Left,  // Keep customers without referrers (Alice)
        &["referred_by"],
        &["id"],
        None
    )?.select(vec![
        col("customer.name").alias("customer"),
        col("referrer.name").alias("referred_by"),
    ])?;

    with_referrers.show().await?;
    // +----------+-------------+
    // | customer | referred_by |
    // +----------+-------------+
    // | Alice    | NULL        |  ← No referrer
    // | Bob      | Alice       |
    // | Carol    | Alice       |
    // +----------+-------------+

    Ok(())
}
```

**Key pattern:** <br>
Use [`.alias()`] to create two "views" of the same DataFrame, then join with qualified column names (`customer.name`, `referrer.name`).

**Common self-join patterns:**

- **Hierarchy traversal:** employees → managers, categories → parent categories
- **Sequential comparison:** this_year.sales vs last_year.sales (join on product_id)
- **Finding pairs:** "Which products are often bought together?" (order_items self-join)

#### Right Join — Find Orphaned Records

Keep all rows from the right table—useful for finding records that reference non-existent parents (like order 104 referencing customer 99).

```rust
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let customers_df = dataframe!(
        "id" => [1, 2, 3],
        "name" => ["Alice", "Bob", "Carol"]
    )?;

    let orders_df = dataframe!(
        "order_id" => [101, 102, 103, 104],
        "customer_id" => [1, 1, 2, 99],
        "amount" => [100, 200, 150, 300]
    )?;

    // "All orders, showing customer info (if customer exists)"
    let right_result = customers_df.clone().join(
        orders_df.clone(),
        JoinType::Right,
        &["id"],
        &["customer_id"],
        None
    )?;

    right_result.show().await?;
    // +----+-------+----------+-------------+--------+
    // | id | name  | order_id | customer_id | amount |
    // +----+-------+----------+-------------+--------+
    // | 1  | Alice | 101      | 1           | 100    |
    // | 1  | Alice | 102      | 1           | 200    |
    // | 2  | Bob   | 103      | 2           | 150    |
    // |    |       | 104      | 99          | 300    |  ← Orphan! No customer 99
    // +----+-------+----------+-------------+--------+

    Ok(())
}
```

**Tip:** <br>
Right Join is just Left Join with swapped tables. `A.join(B, Right)` = `B.join(A, Left)`. Most teams use Left Join exclusively for consistency—put your "main" table first.

#### Full Join — Complete Reconciliation

Keep **all rows from both tables**. Where there's no match, fill the "other side" with NULLs. This is the only join that guarantees you see _everything_—matched, unmatched left, AND unmatched right.

**When to use Full Join:**

- **Data reconciliation** — Comparing two data sources to find ALL discrepancies
- **Migration validation** — Ensuring old and new systems have the same records
- **Audit trails** — "Show me what's in A but not B, what's in B but not A, and what's in both"

```rust
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let customers_df = dataframe!(
        "id" => [1, 2, 3],
        "name" => ["Alice", "Bob", "Carol"]
    )?;

    let orders_df = dataframe!(
        "order_id" => [101, 102, 103, 104],
        "customer_id" => [1, 1, 2, 99],
        "amount" => [100, 200, 150, 300]
    )?;

    // "Show me everything: matched, unmatched left, AND unmatched right"
    let full_result = customers_df.clone().join(
        orders_df.clone(),
        JoinType::Full,
        &["id"],
        &["customer_id"],
        None
    )?;

    full_result.show().await?;
    // +----+-------+----------+-------------+--------+
    // | id | name  | order_id | customer_id | amount |
    // +----+-------+----------+-------------+--------+
    // | 1  | Alice | 101      | 1           | 100    |  ← Matched
    // | 1  | Alice | 102      | 1           | 200    |  ← Matched
    // | 2  | Bob   | 103      | 2           | 150    |  ← Matched
    // | 3  | Carol |          |             |        |  ← Left only (no orders)
    // |    |       | 104      | 99          | 300    |  ← Right only (orphan)
    // +----+-------+----------+-------------+--------+

    Ok(())
}
```

**Data Quality Pattern:** Full Join + NULL filters = powerful reconciliation tool:

```rust
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let customers_df = dataframe!(
        "id" => [1, 2, 3],
        "name" => ["Alice", "Bob", "Carol"]
    )?;

    let orders_df = dataframe!(
        "order_id" => [101, 102, 103, 104],
        "customer_id" => [1, 1, 2, 99],
        "amount" => [100, 200, 150, 300]
    )?;

    let full_result = customers_df.join(
        orders_df,
        JoinType::Full,
        &["id"],
        &["customer_id"],
        None
    )?;

    // Find customers WITHOUT orders (left-only)
    let inactive = full_result.clone().filter(col("order_id").is_null())?;

    // Find orphaned orders (right-only, invalid customer_id)
    let orphans = full_result.clone().filter(col("id").is_null())?;

    // Find matched records (both sides present)
    let matched = full_result.filter(
        col("id").is_not_null().and(col("order_id").is_not_null())
    )?;

    inactive.show().await?;
    orphans.show().await?;
    matched.show().await?;

    Ok(())
}
```

This pattern is invaluable for ETL pipelines, data migration validation, and debugging referential integrity issues.

<!-- JOIN-TODO-006 JOIN-TODO-007 JOIN-TODO-017: Reframe these as existence/non-existence preservation choices; keep right variants, bound mark variants, and remove unconditional efficiency claims. -->

### Intermediate: Semi and Anti Joins

**What makes them special?** <br>
Semi and Anti joins are **filtering joins**—they filter the left table based on existence in the right table, but **never add columns** from the right table. This is fundamentally different from Inner/Left/Right/Full joins which combine data.

| Join Type    | Question                         | Returns                           | SQL Equivalent                               |
| :----------- | :------------------------------- | :-------------------------------- | :------------------------------------------- |
| **LeftSemi** | "Which left rows HAVE a match?"  | Left columns only, matched rows   | `WHERE EXISTS (SELECT 1 FROM right ...)`     |
| **LeftAnti** | "Which left rows have NO match?" | Left columns only, unmatched rows | `WHERE NOT EXISTS (SELECT 1 FROM right ...)` |

**Why use them instead of alternatives?**

| Alternative                | Problem                                                             | Semi/Anti Advantage                                              |
| :------------------------- | :------------------------------------------------------------------ | :--------------------------------------------------------------- |
| Inner Join + Distinct      | Creates duplicates if right has multiple matches, then removes them | Semi join handles this automatically—one output row per left row |
| Left Join + WHERE NULL     | Joins everything first, then filters                                | Anti join filters during join—more efficient                     |
| `IN (SELECT ...)` subquery | Can be slower, harder to optimize                                   | Semi join is the optimized physical plan for `IN`                |

#### LeftSemi — "Which Rows Have Matches?"

Returns left rows that have **at least one match** in the right table. Even if a customer has 10 orders, they appear only once.

```rust
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    // customers_df:                 orders_df:
    // +----+-------+                +----------+-------------+--------+
    // | id | name  |                | order_id | customer_id | amount |
    // +----+-------+                +----------+-------------+--------+
    // | 1  | Alice |                | 101      | 1           | 100    |
    // | 2  | Bob   |                | 102      | 1           | 200    |
    // | 3  | Carol |                | 103      | 2           | 150    |
    // +----+-------+                | 104      | 99          | 300    |
    //                               +----------+-------------+--------+
    let customers_df = dataframe!(
        "id" => [1, 2, 3],
        "name" => ["Alice", "Bob", "Carol"]
    )?;

    let orders_df = dataframe!(
        "order_id" => [101, 102, 103, 104],
        "customer_id" => [1, 1, 2, 99],
        "amount" => [100, 200, 150, 300]
    )?;

    // "Which customers have placed at least one order?"
    let active_customers = customers_df.clone().join(
        orders_df.clone(),
        JoinType::LeftSemi,
        &["id"],
        &["customer_id"],
        None
    )?;

    active_customers.show().await?;
    // +----+-------+
    // | id | name  |
    // +----+-------+
    // | 1  | Alice |  ← Has 2 orders, appears once
    // | 2  | Bob   |  ← Has 1 order
    // +----+-------+
    // Note: Carol (id=3) excluded—no orders
    // Note: No order columns! Just filtered customers.

    Ok(())
}
```

**Use cases for LeftSemi:**

- Find active customers (have placed orders)
- Find products that have been sold (exist in order_items)
- Filter to "things that are referenced somewhere"

#### LeftAnti — "Which Rows Have No Matches?"

Returns left rows that have **zero matches** in the right table. The inverse of Semi join.

```rust
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    // customers_df:                 orders_df:
    // +----+-------+                +----------+-------------+--------+
    // | id | name  |                | order_id | customer_id | amount |
    // +----+-------+                +----------+-------------+--------+
    // | 1  | Alice |                | 101      | 1           | 100    |
    // | 2  | Bob   |                | 102      | 1           | 200    |
    // | 3  | Carol |                | 103      | 2           | 150    |
    // +----+-------+                | 104      | 99          | 300    |
    //                               +----------+-------------+--------+
    let customers_df = dataframe!(
        "id" => [1, 2, 3],
        "name" => ["Alice", "Bob", "Carol"]
    )?;

    let orders_df = dataframe!(
        "order_id" => [101, 102, 103, 104],
        "customer_id" => [1, 1, 2, 99],
        "amount" => [100, 200, 150, 300]
    )?;

    // "Which customers have NEVER placed an order?"
    let inactive_customers = customers_df.clone().join(
        orders_df.clone(),
        JoinType::LeftAnti,
        &["id"],
        &["customer_id"],
        None
    )?;

    inactive_customers.show().await?;
    // +----+-------+
    // | id | name  |
    // +----+-------+
    // | 3  | Carol |  ← No orders found
    // +----+-------+
    // Alice and Bob excluded—they have orders

    Ok(())
}
```

**Use cases for LeftAnti:**

- Find inactive customers (never ordered)
- Find dead inventory (products never sold)
- Data cleanup: "Find records missing required relationships"
- Complement of Semi: `Semi ∪ Anti = Full Left Table`

#### Why Not Just Use Left Join + Filter?

A common question: "Can't I just do Left Join and filter for NULLs?"

```rust
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let customers_df = dataframe!(
        "id" => [1, 2, 3],
        "name" => ["Alice", "Bob", "Carol"]
    )?;

    let orders_df = dataframe!(
        "order_id" => [101, 102, 103, 104],
        "customer_id" => [1, 1, 2, 99],
        "amount" => [100, 200, 150, 300]
    )?;

    // ❌ Less efficient: Join everything, then filter
    let inactive_v1 = customers_df.clone()
        .join(orders_df.clone(), JoinType::Left, &["id"], &["customer_id"], None)?
        .filter(col("order_id").is_null())?;

    // ✅ More efficient: Anti join filters during the join
    let inactive_v2 = customers_df.clone()
        .join(orders_df.clone(), JoinType::LeftAnti, &["id"], &["customer_id"], None)?;

    // Both produce the same result:
    // +----+-------+
    // | id | name  |
    // +----+-------+
    // | 3  | Carol |
    // +----+-------+
    inactive_v1.show().await?;
    inactive_v2.show().await?;

    Ok(())
}
```

Both produce the same result, but Anti join:

- Doesn't create intermediate joined rows
- Doesn't add (and then ignore) right-side columns
- Optimizer can use more efficient algorithms (e.g., hash-based existence check)

> **Learn more:** See [Semi and Anti joins explained] for why these deserve first-class syntax in SQL.

> **Other variants:** <br>
> DataFusion's [`JoinType`] also includes `RightSemi`, `RightAnti`, and mark variants for advanced use cases. For most DataFrame work, stick to left variants and swap input tables if needed.

> **Mark joins:** <br> > [`LeftMark`]/[`RightMark`] are used internally to decorrelate `EXISTS` subqueries. They return all rows from one side plus an extra boolean "mark" column indicating whether any match exists on the other side. Most DataFrame code won't use them directly, but you may see them in `EXPLAIN` plans for complex SQL with `EXISTS` predicates.

<!-- JOIN-TODO-008 JOIN-TODO-016: Move to composition; teach preservation and readability at each leg without prescribing a physical build side or unsupported planning benefits. -->

### Advanced: Multi-Way Joins

**Chain [`.join()`] calls to combine 3+ tables—each join produces a new DataFrame that feeds into the next.**

Real-world data is often normalized across multiple tables. A business question like "which customers have paid orders?" requires combining customers → orders → payments. Each chained Inner Join acts as a filter—only rows matching _all_ join conditions survive.

```rust
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    // customers_df:                 orders_df:
    // +----+-------+                +----------+-------------+--------+
    // | id | name  |                | order_id | customer_id | amount |
    // +----+-------+                +----------+-------------+--------+
    // | 1  | Alice |                | 101      | 1           | 100    |
    // | 2  | Bob   |                | 102      | 1           | 200    |
    // | 3  | Carol |                | 103      | 2           | 150    |
    // +----+-------+                | 104      | 99          | 300    |
    //                               +----------+-------------+--------+
    let customers_df = dataframe!(
        "id" => [1, 2, 3],
        "name" => ["Alice", "Bob", "Carol"]
    )?;

    let orders_df = dataframe!(
        "order_id" => [101, 102, 103, 104],
        "customer_id" => [1, 1, 2, 99],
        "amount" => [100, 200, 150, 300]
    )?;

    // Introduce a payments table: only orders 101 and 103 have payment records
    // payments_df:
    // +------------------+---------+
    // | payment_order_id | status  |
    // +------------------+---------+
    // | 101              | paid    |
    // | 103              | pending |
    // +------------------+---------+
    let payments_df = dataframe!(
        "payment_order_id" => [101, 103],
        "status" => ["paid", "pending"]
    )?;

    // 3-way join: customers → orders → payments
    // Use .select() to keep only the columns we need (avoids duplicates)
    let result = customers_df.clone()
        .join(orders_df.clone(), JoinType::Inner, &["id"], &["customer_id"], None)?
        .join(payments_df.clone(), JoinType::Inner, &["order_id"], &["payment_order_id"], None)?
        .select(vec![ // for better visibility of the result df
            col("name"),
            col("order_id"),
            col("amount"),
            col("status"),
        ])?;

    result.show().await?;
    // +-------+----------+--------+---------+
    // | name  | order_id | amount | status  |
    // +-------+----------+--------+---------+
    // | Alice | 101      | 100    | paid    |
    // | Bob   | 103      | 150    | pending |
    // +-------+----------+--------+---------+
    //
    // What got filtered out:
    // - Alice's order 102: no payment record
    // - Carol: no orders at all
    // - Order 104: orphan (customer_id 99 doesn't exist)

    Ok(())
}
```

> **Tip:** <br>
> Use Left Joins at intermediate steps if you need to preserve unmatched rows (e.g., customers without payments).

<!-- JOIN-TODO-016: Keep logical sequencing/readability advice only; verify or remove optimizer-reordering, build-right, and planning-overhead prescriptions. -->

#### Join Order Matters

The order you chain joins affects both **readability** and **performance**. General principles:

| Principle                                     | Why                                                                        |
| --------------------------------------------- | -------------------------------------------------------------------------- |
| **Start with your "main" table (Left-Table)** | Reads naturally: "customers with their orders and payments"                |
| **Filter early**                              | Reduces intermediate result size before expensive joins                    |
| **Smaller tables on the right**               | Hash joins build from the right side—smaller = faster                      |
| **Let the optimizer help**                    | DataFusion may reorder joins, but good initial order reduces planning work |

```rust
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let customers_df = dataframe!(
        "id" => [1, 2, 3],
        "name" => ["Alice", "Bob", "Carol"]
    )?;

    let orders_df = dataframe!(
        "order_id" => [101, 102, 103, 104],
        "customer_id" => [1, 1, 2, 99],
        "amount" => [100, 200, 150, 300]
    )?;

    let payments_df = dataframe!(
        "payment_order_id" => [101, 103],
        "status" => ["paid", "pending"]
    )?;

    // ✅ Good: Start with the table you're "asking about"
    // "Which customers have payments?"
    let result = customers_df.clone()
        .join(orders_df.clone(), JoinType::Inner, &["id"], &["customer_id"], None)?
        .join(payments_df.clone(), JoinType::Inner, &["order_id"], &["payment_order_id"], None)?;

    result.show().await?;

    // ✅ Also good: Start with filtered data to reduce intermediate size
    let high_value_orders = orders_df.clone().filter(col("amount").gt(lit(100)))?;
    let result = high_value_orders
        .join(customers_df.clone(), JoinType::Inner, &["customer_id"], &["id"], None)?
        .join(payments_df.clone(), JoinType::Inner, &["order_id"], &["payment_order_id"], None)?;

    result.show().await?;

    Ok(())
}
```

> **Performance tip:** <br>
> The optimizer reorders joins when beneficial, but good initial ordering reduces planning overhead. Use [`.explain()`] to see the actual execution plan.

<!-- JOIN-TODO-008 JOIN-TODO-018: Merge with the shared result-schema guidance and distinguish qualification/ambiguity from actual schema errors. -->

#### Managing Column Proliferation

Multi-way joins accumulate columns from every table. With each join, you get **all columns from both sides**—including duplicate key columns. Chain [`.select()`] at the end to keep only what you need:

```rust
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let customers_df = dataframe!(
        "id" => [1, 2, 3],
        "name" => ["Alice", "Bob", "Carol"]
    )?;

    let orders_df = dataframe!(
        "order_id" => [101, 102, 103, 104],
        "customer_id" => [1, 1, 2, 99],
        "amount" => [100, 200, 150, 300]
    )?;

    let payments_df = dataframe!(
        "payment_order_id" => [101, 103],
        "status" => ["paid", "pending"]
    )?;

    // Without .select(): 2 + 3 + 2 = 7 columns (with redundant id, customer_id, order_id, payment_order_id)
    // With .select(): pick only the 4 columns that matter
    let result = customers_df.clone()
        .join(orders_df.clone(), JoinType::Inner, &["id"], &["customer_id"], None)?
        .join(payments_df.clone(), JoinType::Inner, &["order_id"], &["payment_order_id"], None)?
        .select(vec![     // <--- This select removes the clutter
            col("name").alias("customer"),
            col("order_id"),
            col("amount"),
            col("status").alias("payment_status"),
        ])?;

    result.show().await?;
    // +----------+----------+--------+----------------+
    // | customer | order_id | amount | payment_status |
    // +----------+----------+--------+----------------+
    // | Alice    | 101      | 100    | paid           |
    // | Bob      | 103      | 150    | pending        |
    // +----------+----------+--------+----------------+

    Ok(())
}
```

> **When SQL might be clearer:** <br>
> Multi-way joins with 4+ tables can become hard to read as chained method calls. Consider [`SessionContext::sql()`] for complex [star-schema queries][databricks_star_schema] where SQL's visual structure helps.

<!-- JOIN-TODO-004: Move to `.join_on()` construction; explain qualified conditions, AND reduction, explicit OR, and optimizer extraction without method-level performance promises. -->

### Advanced: Join with Complex Conditions

Sometimes you need more than simple column equality. Range joins ("orders placed within 7 days of signup"), inequality predicates ("amount > threshold"), or compound logic ("match on id AND status = 'active'") require expressions that [`.join()`] can't express with just column names.

[`.join_on()`] accepts arbitrary boolean expressions as join conditions. Internally it wraps [`.join()`] with empty key lists and passes your expressions as a filter—the optimizer then extracts any equality predicates for efficient hash/sort-merge execution.

| Use Case           | Example Condition                                                       |
| ------------------ | ----------------------------------------------------------------------- |
| **Range join**     | `order_date.between(start_date, end_date)`                              |
| **Inequality**     | `col("amount").gt(col("threshold"))`                                    |
| **Compound logic** | `col("id").eq(col("customer_id")).and(col("status").eq(lit("active")))` |

```rust
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let customers_df = dataframe!(
        "id" => [1, 2, 3],
        "name" => ["Alice", "Bob", "Carol"]
    )?;

    let orders_df = dataframe!(
        "order_id" => [101, 102, 103, 104],
        "customer_id" => [1, 1, 2, 99],
        "amount" => [100, 200, 150, 300]
    )?;

    // Give DataFrames aliases so we can qualify column references
    let customers = customers_df.clone().alias("customers")?;
    let orders = orders_df.clone().alias("orders")?;

    // Join with compound condition: match on id AND filter amount > 100
    let high_value = customers.join_on(
        orders,
        JoinType::Inner,
        [col("customers.id").eq(col("orders.customer_id"))
            .and(col("orders.amount").gt(lit(100)))]
    )?;

    high_value.show().await?;
    // +----+-------+----------+-------------+--------+
    // | id | name  | order_id | customer_id | amount |
    // +----+-------+----------+-------------+--------+
    // | 1  | Alice | 102      | 1           | 200    |
    // | 2  | Bob   | 103      | 2           | 150    |
    // +----+-------+----------+-------------+--------+
    // Alice's order 101 (amount=100) excluded—doesn't meet amount > 100
    // Carol excluded—no orders at all

    Ok(())
}
```

> **Tip:** <br>
> When using [`.join_on()`], column names may clash between tables. Use [`.alias()`] to qualify references: `col("customers.id")` vs `col("orders.id")`.

<!-- JOIN-TODO-004 JOIN-TODO-006: Promote this correctness boundary within construction and show why an ON-like filter differs from a later `.filter()` for outer joins. -->

#### The `filter` Argument on Outer Joins

The [`.join()`] method's fifth parameter is [`filter: Option<Expr>`][join_filter_param]—easy to overlook in the signature but powerful for outer joins. This filter has **subtle but important semantics**: it applies only to _matched_ rows, not to preserved unmatched rows.

This distinction matters because:

- A [`WHERE`] clause **after** the join would filter out unmatched rows (turning your Left Join into an Inner Join)
- The `filter` argument applies **during** the join, controlling which matches are considered valid while still preserving unmatched rows

| Approach                                      | Behavior                         | Result                              |
| --------------------------------------------- | -------------------------------- | ----------------------------------- |
| [`.join(..., Some(filter))`][`.join()`]       | Filter is part of join condition | Unmatched rows preserved with NULLs |
| [`.join(..., None).filter(...)`][`.filter()`] | Filter applied after join        | Unmatched rows may be removed       |

```rust
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let customers_df = dataframe!(
        "id" => [1, 2, 3],
        "name" => ["Alice", "Bob", "Carol"]
    )?;

    let orders_df = dataframe!(
        "order_id" => [101, 102, 103, 104],
        "customer_id" => [1, 1, 2, 99],
        "amount" => [100, 200, 150, 300]
    )?;

    // Left join with filter: Carol still appears, but only orders > 100 attach
    let result = customers_df.clone().join(
        orders_df.clone(),
        JoinType::Left,
        &["id"],                           // left_cols
        &["customer_id"],                  // right_cols
        Some(col("amount").gt(lit(100))),  // filter (5th param) - applied only to matched rows
    )?;

    result.show().await?;
    // +----+-------+----------+-------------+--------+
    // | id | name  | order_id | customer_id | amount |
    // +----+-------+----------+-------------+--------+
    // | 1  | Alice | 102      | 1           | 200    |  ←  > 100 attached
    // | 2  | Bob   | 103      | 2           | 150    |
    // | 3  | Carol |          |             |        |  ← Preserved!
    // +----+-------+----------+-------------+--------+
    // Alice's order 101 (amount=100) excluded by filter
    // Carol preserved because Left Join keeps all left rows

    Ok(())
}
```

> **Mental model:** <br>
> Think of `filter` as part of the _join condition_, not a `WHERE` after the join. It controls which matches are valid during the join itself.

> **Also applies to [`.join_on()`]:** <br>
> Since [`.join_on()`] is implemented as [`.join()`] with empty key lists and combined `on_exprs` as `filter`, the same semantics apply.

<!-- JOIN-TODO-005 JOIN-TODO-011 JOIN-TODO-014: Correct the trigger and safe-diagnosis advice; distinguish intentional SQL CROSS JOIN; convert warning styling to MyST. -->

### Anti-Pattern: Accidental Cartesian Product

Empty join keys produce a **Cartesian product**—every left row paired with every right row. This is almost never intentional and can crash your query or exhaust memory.

| Left rows | Right rows | Result rows       | Scale                            |
| --------- | ---------- | ----------------- | -------------------------------- |
| 3         | 4          | 12                | Tiny dataset, still 4× larger    |
| 1,000     | 1,000      | 1,000,000         | 1 million rows                   |
| 1,000,000 | 1,000,000  | 1,000,000,000,000 | **1 trillion rows** — will crash |

```rust
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let customers_df = dataframe!(
        "id" => [1, 2, 3],
        "name" => ["Alice", "Bob", "Carol"]
    )?;

    let orders_df = dataframe!(
        "order_id" => [101, 102, 103, 104],
        "customer_id" => [1, 1, 2, 99],
        "amount" => [100, 200, 150, 300]
    )?;

    // ❌ DANGEROUS: Empty keys = Cartesian product
    let cartesian = customers_df.clone().join(
        orders_df.clone(),
        JoinType::Inner,
        &[],  // No join keys!
        &[],
        None
    )?;

    cartesian.show().await?;
    // +----+-------+----------+-------------+--------+
    // | id | name  | order_id | customer_id | amount |
    // +----+-------+----------+-------------+--------+
    // | 1  | Alice | 101      | 1           | 100    |  ← Alice × order 101
    // | 1  | Alice | 102      | 1           | 200    |  ← Alice × order 102
    // | 1  | Alice | 103      | 2           | 150    |  ← Alice × order 103 (not her order!)
    // | 1  | Alice | 104      | 99          | 300    |  ← Alice × order 104 (not her order!)
    // | 2  | Bob   | 101      | 1           | 100    |  ← Bob × order 101 (not his order!)
    // | ... 7 more rows ... |
    // +----+-------+----------+-------------+--------+
    // Total: 3 × 4 = 12 rows — every combination!

    // ✅ CORRECT: Always specify join keys
    let correct = customers_df.clone().join(
        orders_df.clone(),
        JoinType::Inner,
        &["id"],         // left key
        &["customer_id"], // right key
        None
    )?;

    println!("Row count: {}", correct.clone().count().await?);
    // Row count: 3  ← Only matching rows (Alice×2, Bob×1)

    Ok(())
}
```

<!-- JOIN-TODO-011: Unequal key-array lengths error during planning; only a genuinely empty inner-join condition becomes a cross join. `.count()` still executes the explosive plan. -->

**⚠️ Warning:** <br>
If a join returns unexpectedly many rows, check your keys. An empty or mismatched key array silently produces a Cartesian product. Use [`.count()`] before [`.collect()`] to verify.

**If you need a Cartesian product:** <br>
Use SQL via [`ctx.sql("SELECT ... FROM a CROSS JOIN b")`][`sessioncontext::sql()`]. The DataFrame API has no `JoinType::Cross`—empty keys with `Inner` produces the same result but reads like a bug.

<!-- JOIN-TODO-009 JOIN-TODO-010 JOIN-TODO-011 JOIN-TODO-012: Rebuild this as validation by symptom: coverage, multiplication, NULL policy, Cartesian risk, schema, and wrong matches. -->

### Join Troubleshooting

Joins can silently produce unexpected results. When something looks wrong, check these common issues:

| Symptom             | Common Causes                                                             | Diagnosis                                                                   |
| :------------------ | :------------------------------------------------------------------------ | :-------------------------------------------------------------------------- |
| **Empty result**    | Key values don't match, trailing whitespace, case mismatch, NULLs in keys | Inspect both sides: `.select(vec![col("key")]).distinct().show().await?`    |
| **Too many rows**   | Duplicate keys create row multiplication, accidental Cartesian product    | Check key uniqueness: `.select(vec![col("key")]).distinct().count().await?` |
| **Missing columns** | Wrong column names after join, schema mismatch                            | Inspect schema: [`.schema()`] and use [`.alias()`] to qualify               |
| **Wrong matches**   | Keys have different types (string vs int), encoding issues                | Compare types: `df.schema().field_with_name("key")?.data_type()`            |

<!-- JOIN-TODO-009 JOIN-TODO-012: Delete output/left "retention" percentages and fixed thresholds; validate distinct matched keys, unmatched keys, multiplicity, and schema separately. -->

#### Sanity Check: Did the Join Drop Too Much Data?

DataFusion doesn't have built-in join validation, but you can build a simple check to catch silent data loss:

```rust
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let customers_df = dataframe!(
        "id" => [1, 2, 3],
        "name" => ["Alice", "Bob", "Carol"]
    )?;

    let orders_df = dataframe!(
        "order_id" => [101, 102, 103, 104],
        "customer_id" => [1, 1, 2, 99],
        "amount" => [100, 200, 150, 300]
    )?;

    // Count rows before join
    let left_count = customers_df.clone().count().await?;

    // Perform the join
    let joined = customers_df.clone().join(
        orders_df.clone(),
        JoinType::Inner,
        &["id"],
        &["customer_id"],
        None
    )?;
    let joined_count = joined.clone().count().await?;

    // Calculate retention rate
    let retention_pct = (joined_count as f64 / left_count as f64) * 100.0;
    println!("Rows: {} → {} ({:.1}% retention)", left_count, joined_count, retention_pct);

    // ⚠️ Alert if too much data was dropped
    if retention_pct < 50.0 {
        eprintln!("WARNING: Join dropped {:.1}% of rows! Check keys, NULLs, types.",
            100.0 - retention_pct);
    }

    Ok(())
}
```

| Join Type    | Expected Retention     | Warning Sign                       |
| ------------ | ---------------------- | ---------------------------------- |
| **Inner**    | Varies by data overlap | < 50% often indicates key mismatch |
| **Left**     | 100% of left rows      | < 100% means something is wrong    |
| **LeftSemi** | ≤ 100% (filtered)      | 0% = no matches at all             |
| **LeftAnti** | Complement of Semi     | 100% = nothing matched             |

#### **Quick Debugging Steps**

**Step 1:** Inspect inputs before joining

Before joining, verify that key values actually overlap. Use [`.distinct()`] to see the unique key values on each side—if they don't match, your join will produce empty or unexpected results.

```rust
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let customers_df = dataframe!(
        "id" => [1, 2, 3],
        "name" => ["Alice", "Bob", "Carol"]
    )?;

    let orders_df = dataframe!(
        "order_id" => [101, 102, 103, 104],
        "customer_id" => [1, 1, 2, 99],
        "amount" => [100, 200, 150, 300]
    )?;

    // Verify key values exist and match on both sides
    println!("Left keys:");
    customers_df.clone()
        .select(vec![col("id")])?
        .distinct()?  // Unique values only
        .show().await?;
    // +----+
    // | id |
    // +----+
    // | 1  |
    // | 2  |
    // | 3  |
    // +----+

    println!("Right keys:");
    orders_df.clone()
        .select(vec![col("customer_id")])?
        .distinct()?
        .show().await?;
    // +-------------+
    // | customer_id |
    // +-------------+
    // | 1           |
    // | 2           |
    // | 99          |  ← No matching customer! Will be dropped in Inner join
    // +-------------+

    Ok(())
}
```

<!-- JOIN-TODO-010: Preserve default NULL non-matching behavior, but remove sentinel replacement as a universal fix and describe filter_null_join_keys only as an optimization. -->

**Step 2:** Check for NULL keys

In SQL semantics, `NULL = NULL` returns `UNKNOWN` (not `TRUE`), so NULL keys **never match**. This silently drops rows.

```rust
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let df = dataframe!(
        "customer_id" => [Some(1), Some(2), None],
        "value" => [100, 200, 300]
    )?;

    // Count NULLs in join key
    let null_count = df.clone()
        .filter(col("customer_id").is_null())?
        .count().await?;
    println!("NULL keys: {}", null_count);

    // Fix: Replace NULLs with sentinel value before joining
    let df = df.with_column("customer_id", coalesce(vec![col("customer_id"), lit(-1)]))?;
    df.show().await?;

    Ok(())
}
```

<!-- JOIN-TODO-010: Explain this setting as an optimizer behavior, not a semantic NULL-matching option. -->

> **Config option:** <br>
> DataFusion has [`datafusion.optimizer.filter_null_join_keys`][`datafusion.optimizer`] to automatically filter NULL keys.

<!-- JOIN-TODO-013: Turn this into bounded `.explain()` guidance; `analyze = true` executes the plan and is not a safe first diagnostic for suspected explosion. -->

**Step 3: Examine the execution plan**

DataFusion's [`.explain()`] is your window into how the query optimizer transformed your join. It reveals which algorithm was selected, whether predicates were pushed down, and potential performance issues.

```rust
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let customers_df = dataframe!(
        "id" => [1, 2, 3],
        "name" => ["Alice", "Bob", "Carol"]
    )?;

    let orders_df = dataframe!(
        "order_id" => [101, 102],
        "customer_id" => [1, 2],
        "amount" => [100, 200]
    )?;

    let joined_df = customers_df.join(
        orders_df,
        JoinType::Inner,
        &["id"],
        &["customer_id"],
        None
    )?;

    // explain(verbose, analyze) - verbose=true shows optimized plan
    joined_df.explain(true, false)?.show().await?;

    Ok(())
}
```

**What to look for in the plan:**

| Node                 |                       Meaning                       | Performance                       |
| -------------------- | :-------------------------------------------------: | --------------------------------- |
| `HashJoinExec`       | Hash-based join (builds hash table from right side) | ✅ Fast for equi-joins            |
| `SortMergeJoinExec`  |             Sort both sides, then merge             | ✅ Good for large sorted data     |
| `NestedLoopJoinExec` |               Compares every row pair               | ⚠️ Slow — only for non-equi joins |
| `CrossJoinExec`      |                  Cartesian product                  | ❌ Usually a bug                  |

**Signs of a healthy plan:**

- Predicates pushed into `ParquetExec` or `CsvExec` (filter early)
- `HashJoinExec` or `SortMergeJoinExec` for equi-joins
- Smaller table on the **build side** (right side of hash join)

**⚠️ Warning signs:**

- `NestedLoopJoinExec` when you expected equi-join → check if optimizer couldn't extract equality predicates
- `CrossJoinExec` → accidental Cartesian product
- Filters appearing **after** the join instead of pushed down

```text
Example output (simplified):
HashJoinExec: mode=Partitioned, join_type=Inner
  left: ParquetExec: file=customers.parquet, predicate=id IS NOT NULL
  right: ParquetExec: file=orders.parquet, predicate=customer_id IS NOT NULL
                      ↑ Good! NULL filter pushed down
```

<!-- JOIN-TODO-013: Retain only with an explicit execution warning and after non-executing plan inspection. -->

> **Pro tip:** Use `.explain(true, true)?` (analyze=true) to see actual row counts and timing after execution—helps identify which join leg is the bottleneck.

<!-- JOIN-TODO-015: Merge this duplicated table into the opening Key Methods and preservation tables. -->

### **Join Cheat Sheet**

Quick reference for choosing the right join pattern:

| Goal                        | Method                     | JoinType         |
| :-------------------------- | :------------------------- | :--------------- |
| Standard lookup             | [`.join()`]                | `Inner`          |
| Keep all primary records    | [`.join()`]                | `Left`           |
| Filter by existence         | [`.join()`]                | `LeftSemi`       |
| Filter by non-existence     | [`.join()`]                | `LeftAnti`       |
| See all data (reconcile)    | [`.join()`]                | `Full`           |
| Range/inequality conditions | [`.join_on()`]             | `Inner`          |
| Self-join (hierarchies)     | [`.alias()`] + [`.join()`] | `Inner/Left`     |
| Cartesian product           | Prefer SQL `CROSS JOIN`    | Empty keys = bug |

<!-- JOIN-TODO-015 JOIN-TODO-016: Add the missing conclusion before Further Reading; prune links and remove unsupported promotional descriptions. -->

### **Further Reading**

Joins are fundamental yet often misunderstood. These resources provide deeper understanding:

**DataFrame APIs** — Similar concepts in other libraries:

| Resource                 | Focus                                                        |
| :----------------------- | :----------------------------------------------------------- |
| [Spark Join Guide]       | Conceptually similar API with extensive examples             |
| [Polars Join Operations] | Rust-native DataFrame library, closest to DataFusion's model |
| [DataFusion `.join()`]   | Official Rust API documentation                              |

**Join Algorithms & Optimization** — How joins execute under the hood:

| Resource                           | Focus                                                                                    |
| :--------------------------------- | :--------------------------------------------------------------------------------------- |
| [Optimizing SQL & DataFrames Pt 1] | Andrew Lamb on DataFusion's optimizer—why SQL and DataFrames compile to the same plan    |
| [Optimizing SQL & DataFrames Pt 2] | Deep dive: predicate pushdown, projection pushdown, join ordering in DataFusion          |
| [DataFusion Join Optimization]     | How DataFusion uses table statistics to choose build/probe sides—**16x faster** on TPC-H |
| [CMU Join Algorithms]              | Andy Pavlo's database course—excellent video lectures on hash/sort-merge joins           |
| [Hash Join (Wikipedia)]            | How hash tables enable O(n+m) equi-joins                                                 |
| [Sort-Merge Join]                  | Why pre-sorted data enables efficient streaming joins                                    |
| [Join optimization strategies]     | How databases choose algorithms and what you can control                                 |

**SQL Semantics** — Conceptual foundations:

| Resource                                                                  | Focus                                                              |
| :------------------------------------------------------------------------ | :----------------------------------------------------------------- |
| [Visual JOIN guide]                                                       | Interactive visualization of all join types with animated examples |
| [Join tutorial]                                                           | Why Venn diagrams are misleading for understanding joins           |
| [Semi and Anti joins explained]                                           | First-class existence checks that SQL forgot                       |
| [PostgreSQL JOIN docs]                                                    | Authoritative reference—DataFusion follows PostgreSQL semantics    |
| [NULL handling in joins]                                                  | Why `NULL = NULL` is `UNKNOWN`, not `TRUE`                         |
| [Understanding SQL Dialects][understanding sql dialects (medium-article)] | Medium article about different SQL dialects                        |

[`.alias()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.alias
[`.collect()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.collect
[`.count()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.count
[`.distinct()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.distinct
[`.explain()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.explain
[`.filter()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.filter
[`.intersect()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.intersect
[`.join()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.join
[`.join_on()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.join_on
[`.schema()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.schema
[`.select()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.select
[`.with_column_renamed()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.with_column_renamed
[datafusion `.join()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.join
[join_filter_param]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.join "See the 'filter' parameter in the join() signature"
[`jointype`]: https://docs.rs/datafusion/latest/datafusion/logical_expr/enum.JoinType.html
[`full`]: https://docs.rs/datafusion/latest/datafusion/logical_expr/enum.JoinType.html#variant.Full "All rows from both tables (NULL where no match)"
[`inner`]: https://docs.rs/datafusion/latest/datafusion/logical_expr/enum.JoinType.html#variant.Inner "Only rows with matches in both tables"
[`left`]: https://docs.rs/datafusion/latest/datafusion/logical_expr/enum.JoinType.html#variant.Left "All left rows + matching right rows (NULL if no match)"
[`right`]: https://docs.rs/datafusion/latest/datafusion/logical_expr/enum.JoinType.html#variant.Right "All right rows + matching left rows (NULL if no match)"
[`leftanti`]: https://docs.rs/datafusion/latest/datafusion/logical_expr/enum.JoinType.html#variant.LeftAnti "Left rows that have NO match (no right columns)"
[`leftsemi`]: https://docs.rs/datafusion/latest/datafusion/logical_expr/enum.JoinType.html#variant.LeftSemi "Left rows that have a match (no right columns)"
[`leftmark`]: https://docs.rs/datafusion/latest/datafusion/logical_expr/enum.JoinType.html#variant.LeftMark "Mark join for EXISTS subquery decorrelation"
[`rightanti`]: https://docs.rs/datafusion/latest/datafusion/logical_expr/enum.JoinType.html#variant.RightAnti
[`rightsemi`]: https://docs.rs/datafusion/latest/datafusion/logical_expr/enum.JoinType.html#variant.RightSemi
[`rightmark`]: https://docs.rs/datafusion/latest/datafusion/logical_expr/enum.JoinType.html#variant.RightMark "Mark join for EXISTS subquery decorrelation"
[`logicalplan`]: https://docs.rs/datafusion/latest/datafusion/logical_expr/enum.LogicalPlan.html
[`tableprovider`]: https://docs.rs/datafusion/latest/datafusion/catalog/trait.TableProvider.html
[`datafusion.optimizer`]: https://docs.rs/datafusion/latest/datafusion/optimizer/index.html
[`sqlparser`]: https://docs.rs/datafusion/latest/datafusion/logical_expr/sqlparser/dialect/index.html "DataFusion's SQL parser supports multiple dialects"
[`sessioncontext::sql()`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.sql
[`date_trunc()`]: https://docs.rs/datafusion/latest/datafusion/functions/datetime/expr_fn/fn.date_trunc.html
[`take()`]: https://docs.rs/arrow/latest/arrow/compute/kernels/take/fn.take.html "Arrow kernel: select elements by index"
[arrow]: https://arrow.apache.org/ "Apache Arrow: columnar in-memory format"
[`cross join`]: ../../../user-guide/sql/select.md#cross-join
[`inner join`]: ../../../user-guide/sql/select.md#inner-join
[`full outer join`]: ../../../user-guide/sql/select.md#full-outer-join
[`natural join`]: ../../../user-guide/sql/select.md#natural-join
[`left anti join`]: ../../../user-guide/sql/select.md#left-anti-join
[`left semi join`]: ../../../user-guide/sql/select.md#left-semi-join
[`where`]: ../../../user-guide/sql/select.md#where-clause
[`or`]: ../../../user-guide/sql/operators.md#logical-operators
[**cross join**]: https://docs.rs/datafusion/latest/datafusion/physical_plan/joins/struct.CrossJoinExec.html "Cartesian product of two tables"
[**hash join**]: https://docs.rs/datafusion/latest/datafusion/physical_plan/joins/struct.HashJoinExec.html "Equi-join using hash table on build side"
[**nested loop join**]: https://docs.rs/datafusion/latest/datafusion/physical_plan/joins/struct.NestedLoopJoinExec.html "General non-equi join conditions"
[**piecewise merge join**]: https://docs.rs/datafusion/latest/datafusion/physical_plan/joins/struct.PiecewiseMergeJoinExec.html "Optimized for single range conditions"
[**sort-merge join**]: https://docs.rs/datafusion/latest/datafusion/physical_plan/joins/struct.SortMergeJoinExec.html "Join pre-sorted inputs with optional spilling"
[**symmetric hash join**]: https://docs.rs/datafusion/latest/datafusion/physical_plan/joins/struct.SymmetricHashJoinExec.html "Streaming join for unbounded data"
[several join algorithms]: https://docs.rs/datafusion/latest/datafusion/physical_plan/joins/index.html "DataFusion join implementations"
[cmu join algorithms]: https://www.youtube.com/watch?v=YIdIaPopfpk&list=PLSE8ODhjZXjYMAgsGH-GtY5rJYZ6zjsd5&index=12 "CMU 15-445 Lecture 11: Join Algorithms (Andy Pavlo)"
[databricks_star_schema]: https://www.databricks.com/glossary/star-schema
[datafusion join optimization]: https://xebia.com/blog/making-joins-faster-in-datafusion-based-on-table-statistics/ "Making Joins Faster in DataFusion Based on Table Statistics"
[hash join (wikipedia)]: https://en.wikipedia.org/wiki/Hash_join "Hash join algorithm explanation"
[join optimization strategies]: https://use-the-index-luke.com/sql/join "How databases optimize joins and what you can control"
[join tutorial]: https://blog.jooq.org/say-no-to-venn-diagrams-when-explaining-joins/ "Why Venn diagrams mislead when explaining joins"
[null handling in joins]: https://modern-sql.com/concept/null "Why NULL comparisons return UNKNOWN, not TRUE/FALSE"
[optimizing sql & dataframes pt 1]: https://www.influxdata.com/blog/optimizing-sql-dataframes-part-one/ "Optimizing SQL (and DataFrames) in DataFusion: Part 1"
[optimizing sql & dataframes pt 2]: https://www.influxdata.com/blog/optimizing-sql-dataframes-part-two/ "Optimizing SQL (and DataFrames) in DataFusion: Part 2"
[polars join operations]: https://docs.pola.rs/user-guide/transformations/joins/ "Polars DataFrame join operations"
[postgresql join docs]: https://www.postgresql.org/docs/current/queries-table-expressions.html#QUERIES-JOIN "Authoritative reference for join semantics"
[semi and anti joins explained]: https://blog.jooq.org/semi-join-and-anti-join-should-have-its-own-syntax-in-sql/ "Why Semi/Anti joins deserve first-class syntax"
[sort-merge join]: https://en.wikipedia.org/wiki/Sort-merge_join "Sort-merge join algorithm"
[spark join guide]: https://spark.apache.org/docs/latest/sql-ref-syntax-qry-select-join.html "Apache Spark SQL join syntax and examples"
[survivorship_bias]: https://en.wikipedia.org/wiki/Survivorship_bias
[understanding sql dialects (medium-article)]: https://medium.com/@abhapratiti27/understanding-sql-dialects-a-deeper-dive-into-the-linguistic-variations-of-sql-e7e2fdb7509b
[visual join guide]: https://joins.spathon.com/ "Interactive visual guide to SQL joins"

<!-- Internal pages -->

[transformation concepts]: transformation-concepts.md#combining-multiple-dataframes
[set operations]: set-operations.md
