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

<!--TODO (restructuring notes, agreed 2026-07-06)

1. ABSTRACT
2. INTRODUCTION
3. LATERAL JOINS (consideration of implementation) — add a condensed mention: SQL planner supports LATERAL
   (derived tables / table functions, incl. APPLY syntax; see
   datafusion/sql/src/relation/join.rs and issue #10048); no DataFrame API
   method exists — honest "SQL shines" note, DataFrame route is hybrid via
   `ctx.sql()`. Conceptually a correlated subquery in FROM → cross-link
   subqueries.md.
4. EXECUTION UPDATE — "How Joins Execute" gains a note on the piecewise
   merge join operator (datafusion/physical-plan/src/joins/piecewise_merge_join)
   for range/inequality join conditions.
5. EXTRACTION CANDIDATE — the cognitive head (Why Joins Matter / How Joins
   Work / How Joins Execute / Join Types at a Glance) is ~250 lines; if
   transformation-concepts.md outgrows its budget, extract join-concepts.md.
   transformation-concepts.md carries only a condensed "Joins in Brief" recap
   linking here (handshake recorded 2026-07-10: that recap now exists at
   transformation-concepts.md#joins-in-brief).
6. FRAME-BOUNDARY ALIGNMENT (future) — the ## Introduction (currently "a join
   takes two DataFrames … produces a wider table") defines joins primarily by
   widening. Align with the frame-boundary model now used by
   transformation-concepts.md#joins-in-brief: a join crosses the frame boundary
   (two input frames); MANY joins widen the schema by carrying columns from both;
   semi/anti are the exception (existence test, no right columns). Do NOT define
   joins primarily as "widening the result."
7. EXECUTION/OPTIMIZER DEPTH REVIEW (future) — "How Joins Execute" plus the
   scattered optimizer/perf claims (build-side swap, dynamic filters, "16x
   faster", partition mode) exceed leaf-page recap altitude. Review for accuracy
   against source and decide ownership (this page vs.
   Concepts/architectural-dataframe.md / execution owner). No performance claims
   beyond what source supports (markdown.mdc §1, §4).
8. TAXONOMY/API/TROUBLESHOOTING OWNERSHIP (future) — this page owns the full
   JoinType taxonomy, .join()/.join_on() API guide, and troubleshooting;
   transformation-concepts.md#joins-in-brief only recaps the frame-boundary
   concept and links here. When the per-method SQL equivalents are distributed
   from transformation-concepts.md Method Families, the join SQL equivalents land
   on this page.
-->

# When DataFrames Collide: Join Patterns



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

## Introduction (placeholder)

**Joins are the backbone of relational data processing—the operation that links separate tables into unified, queryable datasets by matching rows on shared keys.**

A join takes two DataFrames (or SQL tables) and produces a new one by comparing values in designated **key columns**—when values match (e.g., `customer.id = 1` on the left finds `order.customer_id = 1` on the right), the corresponding rows are stitched together. The result is a wider table combining columns from both sides, where related data now sits in the same row. Think of it as a lookup: for each row on the left, scan the right table for rows with matching key values, then concatenate them.

Whether you're enriching customer records with their orders, filtering products by inventory status, or reconciling data across systems, joins are the workhorse behind nearly every real-world data pipeline.

Master joins, and you unlock the full power of relational data processing.

> **DataFrame API coverage:** The DataFrame API supports all common join types ([`Inner`], [`Left`], [`Right`], [`Full`], [`LeftSemi`], [`LeftAnti`], and their right variants). Two SQL join types have **no direct DataFrame equivalent**:
>
> - [`NATURAL JOIN`] — use [`ctx.sql()`][`sessioncontext::sql()`] or specify keys explicitly with [`.join()`]
> - [`CROSS JOIN`] — use [`.join()`] with empty key lists, or [`ctx.sql("... CROSS JOIN ...")`][`sessioncontext::sql()`]
>
> For most workflows, the DataFrame API is fully sufficient. Fall back to SQL for these edge cases.

### Why Joins Matter

Real-world data rarely lives in a single table. Customers are in one file, orders in another, products in a third. Joins let you:

- **Enrich** records by attaching related data (customer name → their orders)
- **Filter** by relationships (only customers _with_ orders, or _without_)
- **Reconcile** datasets (find what's in A but not B, or in both)
- **Validate** data quality—anti-joins reveal orphaned records (orders referencing non-existent customers), broken foreign keys, or rows dropped during ETL

Without joins, you'd be stuck writing nested loops or manual lookups. DataFusion's join engine handles the matching efficiently—you describe _what_ to combine, not _how_.

### How Joins Work

Every join has three ingredients:

1. **Two tables** — left (your starting DataFrame) and right (the one you're joining)
2. **Join keys** — which columns to match (`customers.id = orders.customer_id`)
3. **Join type** — what to do with matches and non-matches

A very basic example is shown as the following as common in SQL :

```sql
-- SQL equivalent
SELECT *
FROM customers           -- left table
JOIN orders              -- right table
  ON customers.id = orders.customer_id   -- join keys
```

Since we cannot cover a tutorial for joins, please follow other tutorials as but not only the following resources:

| Resource                        | Focus                                                              |
| :------------------------------ | :----------------------------------------------------------------- |
| [Visual JOIN guide]             | Interactive visualization of all join types with animated examples |
| [Join tutorial]                 | Why Venn diagrams are misleading for understanding joins           |
| [Semi and Anti joins explained] | First-class existence checks that SQL forgot                       |
| [PostgreSQL JOIN docs]          | Authoritative reference—DataFusion follows PostgreSQL semantics    |
| [NULL handling in joins]        | Why `NULL = NULL` is `UNKNOWN`, not `TRUE`                         |

### DataFrame API vs SQL

**Two paths, same destination.** Both APIs compile to the same internal [`LogicalPlan`] and benefit from identical optimizer passes—the difference is _how_ you construct the query:

| Aspect           | DataFrame API                                                 | SQL API                                           |
| ---------------- | ------------------------------------------------------------- | ------------------------------------------------- |
| **Construction** | Builder pattern—chain methods like [`.join()`], [`.filter()`] | Parser—write a query string, DataFusion parses it |
| **Type safety**  | Compile-time checks; typos caught by `rustc`                  | Runtime errors; typos discovered at execution     |
| **Composition**  | Programmatic; easy to build queries conditionally             | String-based; dynamic SQL requires concatenation  |
| **Result**       | [`LogicalPlan`] → Optimizer → Execution                       | [`LogicalPlan` ]→ Optimizer → Execution           |

**The multiplicity of SQL-Dialects**<br>
DataFusion's SQL parser ([`sqlparser`]) accepts syntax from multiple dialects—PostgreSQL, MySQL, Snowflake, and others. Throughout this documentation, we use **PostgreSQL syntax** as the reference standard: it's widely understood, well-documented, and DataFusion's join semantics (NULL handling, outer join behavior) closely follow PostgreSQL conventions. <br>
For more deeper insights follow [SQL Dialects][understanding sql dialects (medium-article)]

Both SQL and the DataFrame API support the standard join families:

| Family      | SQL syntax                                   | DataFrame [`JoinType`]        | Purpose                                     |
| ----------- | -------------------------------------------- | ----------------------------- | ------------------------------------------- |
| **Inner**   | [`INNER JOIN`]                               | [`Inner`]                     | Only matching rows                          |
| **Outer**   | [`LEFT`] / [`RIGHT`] / [`FULL OUTER JOIN`]   | [`Left`], [`Right`], [`Full`] | Keep non-matches from one or both sides     |
| **Semi**    | [`LEFT / RIGHT SEMI JOIN`][`left semi join`] | [`LeftSemi`], [`RightSemi` ]  | Filter by existence (no columns from right) |
| **Anti**    | [`LEFT / RIGHT ANTI JOIN`][`left anti join`] | [`LeftAnti`], [`RightAnti` ]  | Filter by non-existence                     |
| **Cross**   | [`CROSS JOIN`]                               | _(none)_                      | Cartesian product (use empty keys)          |
| **Natural** | [`NATURAL JOIN`]                             | _(none)_                      | Auto-match same-named columns               |
| **Mark**    | _(internal)_                                 | [`LeftMark`], [`RightMark`]   | Adds boolean column for `EXISTS` subqueries |

> **SQL-only joins:**<br> > [`NATURAL JOIN`] and [`CROSS JOIN`] have no direct [`JoinType`] variant.
> Use [`ctx.sql()`][`sessioncontext::sql()`] for natural joins; for cross joins, call [`.join()`] with empty key lists (see Anti-Pattern section).

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
> \*SIMD requires `RUSTFLAGS='-C target-cpu=native'`. See [Crate Configuration](../../user-guide/crate-configuration.md).
>
> The result: you describe _what_ to join, and the optimizer handles _how_—often matching or exceeding hand-tuned imperative code.

### Join Types at a Glance

Joins control how rows from two tables are matched and combined. The key decisions are:

1.  what happens to rows that _don't_ match
2.  which columns appear in the result.

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
If you need rows that exist in _both_ DataFrames (identical schemas, all columns compared), use [`.intersect()`] instead—that's a set operation, not a join.
<br> For more see the subsection [Dataframes unique methods](#dataframe-unique-methods)

> **⚠️ The hidden cost: [Survivorship bias][survivorship_bias]**
>
> Many join types silently drop non-matching rows—Inner, Semi, and Anti joins all filter out data. In the example above, Carol and order 104 simply vanish. Chain several such joins together and you may lose 60% of your data without noticing—you only see the "survivors" (rows that matched at every step).
>
> As a sanity check, if you need to see what's _missing_, use [Outer Joins](#intermediate-leftrightfull-joins) (or other oposit joins like left vs. right) instead—`NULL` values reveal exactly where data gaps exist.

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
> Multi-key joins on temporal columns work well when truncated to appropriate granularity using [`date_trunc()`]. Joining on `DATE` (day) has minimal edge cases (~0.004% at midnight); joining on raw `TIMESTAMP` (milliseconds) risks silent mismatches.

(intermediate-leftrightfull-joins)=

### Intermediate: Left/Right/Full Joins

Where Inner Join keeps only the intersection (rows matching on both sides), **"partial" outer joins (left, right and full) preserve rows that don't match**—filling missing columns with `NULL`. This makes data gaps visible instead of silently dropping them.

| Join Type | Keeps                                           | Typical Use Case                                        |
| :-------- | :---------------------------------------------- | :------------------------------------------------------ |
| **Left**  | All left rows, matching right data if available | Customer reports—keep all customers, show orders if any |
| **Right** | All right rows, matching left data if available | Orphan detection—find orders without valid customers    |
| **Full**  | Everything from both sides                      | Data reconciliation—find ALL discrepancies              |

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

**Use Case: Self-Joins (Customer Referrals)**

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

**⚠️ Warning:** <br>
If a join returns unexpectedly many rows, check your keys. An empty or mismatched key array silently produces a Cartesian product. Use [`.count()`] before [`.collect()`] to verify.

**If you need a Cartesian product:** <br>
Use SQL via [`ctx.sql("SELECT ... FROM a CROSS JOIN b")`][`sessioncontext::sql()`]. The DataFrame API has no `JoinType::Cross`—empty keys with `Inner` produces the same result but reads like a bug.

### Join Troubleshooting

Joins can silently produce unexpected results. When something looks wrong, check these common issues:

| Symptom             | Common Causes                                                             | Diagnosis                                                                   |
| :------------------ | :------------------------------------------------------------------------ | :-------------------------------------------------------------------------- |
| **Empty result**    | Key values don't match, trailing whitespace, case mismatch, NULLs in keys | Inspect both sides: `.select(vec![col("key")]).distinct().show().await?`    |
| **Too many rows**   | Duplicate keys create row multiplication, accidental Cartesian product    | Check key uniqueness: `.select(vec![col("key")]).distinct().count().await?` |
| **Missing columns** | Wrong column names after join, schema mismatch                            | Inspect schema: [`.schema()`] and use [`.alias()`] to qualify               |
| **Wrong matches**   | Keys have different types (string vs int), encoding issues                | Compare types: `df.schema().field_with_name("key")?.data_type()`            |

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

> **Config option:** <br>
> DataFusion has [`datafusion.optimizer.filter_null_join_keys`][`datafusion.optimizer`] to automatically filter NULL keys.

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

> **Pro tip:** Use `.explain(true, true)?` (analyze=true) to see actual row counts and timing after execution—helps identify which join leg is the bottleneck.

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
