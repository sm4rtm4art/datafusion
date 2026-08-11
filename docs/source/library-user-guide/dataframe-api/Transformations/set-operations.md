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

<!--TODO (restructuring map — moves EXECUTED 2026-07-06, see handshake below)

1. ABSTRACT
2. INTRODUCTION
3. DONE — monolith-split tail redistributed:
   - Hybrid methods (parse_sql_expr, select_exprs, param binding) → hybrid-sql.md
   - "Describing Data" → data-quality.md
   - "Filling Null Values" → deleted; owner Concepts/null-handling.md
     (§Null-Handling Toolkit). VERIFY during rework: the empty-column-list
     variant (`fill_null(value, vec![])` = all compatible columns) is covered
     by the owner; if not, add it there.
   - "Caching DataFrames" → deleted; owner
     Writing-DataFrames/executing-dataframes.md (§Caching Results)
   - "Execution Control" / "Streaming Results" / "Partition-Aware Execution"
     → deleted; owner Writing-DataFrames/executing-dataframes.md
     (§Partitioned & Streaming Execution covers all three methods)
   - "Creating from Columns" → deleted; owner
     Creating-DataFrames/inline-data.md (§Explicit Arrow Types)
   - Unnesting sections → reshaping.md
4. MISSING CONTENT — placeholder sections below: `.union()`,
   `.union_distinct()`, `.intersect()`, `.except()`, `.distinct()`.
5. DONE — "DISTINCT ON" arrived from hybrid-sql.md (raw material).
6. RENAME — title "Set Operations by Name" narrows to the by-name variants;
   the page now covers all set operations → retitle (e.g. "Set Operations")
   and reorder: positional first, by-name second, dedup third.
7. LINK DEFINITIONS — all reference-style links in this file (e.g.
   [`.union_by_name()`], [`.with_column_renamed()`]) lost their definitions
   in the monolith split; restore before Stage 6.
8. HANDSHAKE (2026-07-10) — the concept-level set-operation recap now lives at
   transformation-concepts.md#set-operations-in-brief (frame-boundary model:
   cross the boundary, align + stack/compare whole rows, change cardinality,
   contrast with joins = no schema widening; links here). This page owns the
   method mechanics, examples, SQL equivalents, compatibility, and dedup.
   Single-frame dedup (.distinct(), .distinct_on()) is routed here from the
   concept page as related cardinality reading.
9. FRAME-BOUNDARY ALIGNMENT (future) — the ## Introduction currently leads with
   the by-name / Arrow-columnar angle (artifact of the old "by Name" title).
   When reworked (see rename/reorder in item 6), align with the frame-boundary
   model: set operations cross the frame boundary and combine like-shaped frames
   by aligning and then stacking or comparing whole rows; positional vs. by-name
   is the alignment axis. Do NOT define set operations primarily as an
   Arrow/by-name feature.
-->

# Set Operations by Name

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

**This is where DataFusion's [Arrow columnar design](../../../user-guide/arrow-introduction.md) shines.**

SQL's `UNION` matches columns by _position_, not name. If two tables have the same columns in different orders, SQL silently produces incorrect results—a common source of bugs when combining data from different sources.

Because Arrow schemas carry column names as metadata, DataFusion can align DataFrames by name instead of position. This is impossible in traditional row-based databases where columns are just offsets.

| Method                        | Duplicate Rows | SQL Equivalent      |
| ----------------------------- | -------------- | ------------------- |
| [`.union_by_name()`]          | Keeps all      | `UNION ALL BY NAME` |
| [`.union_by_name_distinct()`] | Removes        | `UNION BY NAME`     |

### Union by Column Name

[`.union_by_name()`] aligns DataFrames by column _name_, not position:

```rust
use datafusion::prelude::*;
use datafusion::error::Result;
use datafusion::assert_batches_sorted_eq;

#[tokio::main]
async fn main() -> Result<()> {
    let q1 = dataframe!(
        "product" => ["A", "B"],
        "revenue" => [100, 200]
    )?;

    let q2 = dataframe!(
        "revenue" => [100, 300],  // Different column order! Row (A, 100) duplicates q1
        "product" => ["A", "C"]
    )?;

    // union_by_name keeps ALL rows (including duplicates)
    let combined = q1.union_by_name(q2)?;

    let results = combined.collect().await?;
    assert_batches_sorted_eq!(
        &[
            "+---------+---------+",
            "| product | revenue |",
            "+---------+---------+",
            "| A       | 100     |",
            "| A       | 100     |",  // Duplicate kept!
            "| B       | 200     |",
            "| C       | 300     |",
            "+---------+---------+",
        ],
        &results
    );
    Ok(())
}
```

[`.union_by_name_distinct()`] removes duplicate rows after aligning by name:

```rust
use datafusion::prelude::*;
use datafusion::error::Result;
use datafusion::assert_batches_sorted_eq;

#[tokio::main]
async fn main() -> Result<()> {
    let q1 = dataframe!(
        "product" => ["A", "B"],
        "revenue" => [100, 200]
    )?;

    let q2 = dataframe!(
        "revenue" => [100, 300],  // First row duplicates q1
        "product" => ["A", "C"]
    )?;

    // Combine and deduplicate
    let combined = q1.union_by_name_distinct(q2)?;

    let results = combined.collect().await?;
    assert_batches_sorted_eq!(
        &[
            "+---------+---------+",
            "| product | revenue |",
            "+---------+---------+",
            "| A       | 100     |",
            "| B       | 200     |",
            "| C       | 300     |",
            "+---------+---------+",
        ],
        &results
    );
    Ok(())
}
```

:::{admonition} When to use
:class: tip

Data pipelines combining sources with inconsistent column ordering — different Parquet writers, or CSV exports from different tools.
:::

**Schema handling.** `.union_by_name()` is the permissive variant: it resolves the three ways two frames can disagree instead of rejecting them.

| Disagreement                      | What DataFusion does                                                        |
| --------------------------------- | --------------------------------------------------------------------------- |
| Different column order            | Aligns by name                                                              |
| A column missing on one side      | Adds it as `NULL` for the rows from the frame that lacks it                 |
| Different types for the same name | Coerces to a common type; a numeric/string pair becomes the **string** type |

:::{admonition} Permissive alignment hides mismatches
:class: caution

Both conveniences are silent. A misspelled column name raises no error — it becomes an extra column that is `NULL` for every row from the other frame. A numeric column unioned with a text column does not fail either; it widens to `Utf8`, so `revenue` can arrive as strings. Inspect the resulting schema when the inputs are not under your control, and use `.with_column_renamed()` when two frames name the same field differently.
:::

A union fails only when no coercion rule covers the pair of types found for a shared column name.

:::{admonition} SQL equivalent
:class: note

`UNION ALL BY NAME`, and `UNION BY NAME` for the deduplicating form. The plain positional `UNION` is the one that cannot express this.
:::

<!-- MOVED OUT (monolith-split repair, 2026-07-06): everything that followed
this point was the body of the old "Advanced DataFrame Patterns" —
redistributed per the file-top TODO map (hybrid-sql.md, data-quality.md,
reshaping.md; duplicates of Concepts/null-handling.md,
Writing-DataFrames/executing-dataframes.md, and
Creating-DataFrames/inline-data.md were deleted). -->

### Positional Union

[`.union()`] pairs columns by _position_: the first column of one frame meets the first column of the other, whatever either one is called. The result carries the left frame's column names, which makes positional union the natural choice when the same code produced both schemas.

```rust
use datafusion::prelude::*;
use datafusion::error::Result;
use datafusion::assert_batches_sorted_eq;

#[tokio::main]
async fn main() -> Result<()> {
    let west_sales = dataframe!(
        "product" => ["A", "B"],
        "revenue" => [100, 200]
    )?;

    // Same columns, same order — row (A, 100) duplicates west_sales
    let east_sales = dataframe!(
        "product" => ["A", "C"],
        "revenue" => [100, 300]
    )?;

    // union keeps ALL rows (including duplicates)
    let all_sales = west_sales.union(east_sales)?;

    let results = all_sales.collect().await?;
    assert_batches_sorted_eq!(
        &[
            "+---------+---------+",
            "| product | revenue |",
            "+---------+---------+",
            "| A       | 100     |",
            "| A       | 100     |",  // Duplicate kept!
            "| B       | 200     |",
            "| C       | 300     |",
            "+---------+---------+",
        ],
        &results
    );
    Ok(())
}
```

[`.union_distinct()`] aligns by position the same way, then discards duplicate rows:

```rust
use datafusion::prelude::*;
use datafusion::error::Result;
use datafusion::assert_batches_sorted_eq;

#[tokio::main]
async fn main() -> Result<()> {
    let west_sales = dataframe!(
        "product" => ["A", "B"],
        "revenue" => [100, 200]
    )?;

    let east_sales = dataframe!(
        "product" => ["A", "C"],
        "revenue" => [100, 300]
    )?;

    // Combine and deduplicate
    let all_sales = west_sales.union_distinct(east_sales)?;

    let results = all_sales.collect().await?;
    assert_batches_sorted_eq!(
        &[
            "+---------+---------+",
            "| product | revenue |",
            "+---------+---------+",
            "| A       | 100     |",
            "| B       | 200     |",
            "| C       | 300     |",
            "+---------+---------+",
        ],
        &results
    );
    Ok(())
}
```

**Schema handling.** Positional union is strict about width and permissive about everything else.

| Input property | What DataFusion does                                                                      |
| -------------- | ----------------------------------------------------------------------------------------- |
| Column count   | The only hard requirement — a mismatch fails on the `.union()` call itself                |
| Column names   | Takes them from the left frame; the right frame's names are discarded                     |
| Column types   | Coerces each position to a common type; a numeric/string pair becomes the **string** type |
| Nullability    | Marks a column nullable when the column is nullable on either side                        |

Only the column count is checked while the plan is built, so `.union()` returns an error immediately when the widths disagree. Type coercion happens later, during analysis, which means an irreconcilable pair of types surfaces when an action runs rather than at the call site.

Column count is also the _only_ structural check, so two frames holding the same columns in opposite order pass it. Positional union then stacks `product` onto `revenue`, widens both to `Utf8` to accommodate the clash, and returns rows whose values have traded places:

```rust
use datafusion::prelude::*;
use datafusion::error::Result;
use datafusion::assert_batches_sorted_eq;

#[tokio::main]
async fn main() -> Result<()> {
    let west_sales = dataframe!(
        "product" => ["A", "B"],
        "revenue" => [100, 200]
    )?;

    // The same two columns, declared in the opposite order
    let east_sales = dataframe!(
        "revenue" => [100, 300],
        "product" => ["A", "C"]
    )?;

    let all_sales = west_sales.union(east_sales)?;

    let results = all_sales.collect().await?;
    assert_batches_sorted_eq!(
        &[
            "+---------+---------+",
            "| product | revenue |",
            "+---------+---------+",
            "| 100     | A       |",  // east_sales revenue landed in product
            "| 300     | C       |",
            "| A       | 100     |",
            "| B       | 200     |",
            "+---------+---------+",
        ],
        &results
    );
    Ok(())
}
```

:::{admonition} Positional misalignment produces wrong rows, not an error
:class: warning

Nothing marks that result as damaged: no error, no warning, and a schema that still reads `product, revenue`. Inspecting the schema beforehand does not expose it either — immediately after `.union()`, `DataFrame::schema()` reports the left frame's types, and the widening to `Utf8` shows up only once an action produces the result. Position-based alignment is trustworthy only while you own the column order on both sides; reach for [`.union_by_name()`] as soon as one schema arrives from somewhere you do not control.
:::

:::{admonition} SQL equivalent
:class: note

`UNION ALL`, and `UNION` for the deduplicating form. Both align by position, and both carry the same misalignment risk as these methods.
:::

### Intersection and Difference

<!-- PLACEHOLDER (content backlog): `.intersect()` (SQL `INTERSECT`) and
`.except()` (SQL `EXCEPT`) — rows in both / rows in one but not the other. -->

### Deduplication

<!-- PLACEHOLDER (content backlog): `.distinct()` (SQL `DISTINCT`, all
columns) as the lead-in to the DISTINCT ON material below. -->

<!-- MOVED HERE from hybrid-sql.md (2026-07-06) — raw material; rework
pending: the example demonstrates the SQL syntax, not the `.distinct_on()`
method — add a DataFrame-method example. -->

### DISTINCT ON (PostgreSQL-Style)

[`.distinct_on()`] keeps the first row for each unique value in specified columns. DataFusion also supports this via SQL (`SELECT DISTINCT ON (...)`), but the DataFrame method integrates naturally into pipelines:

```rust
use datafusion::prelude::*;
use datafusion::error::Result;

#[tokio::main]
async fn main() -> Result<()> {
    let ctx = SessionContext::new();

    // Create and register a table for the SQL DISTINCT ON example
    let df = dataframe!(
        "customer" => ["Alice", "Alice", "Bob", "Bob"],
        "order_date" => ["2024-01-01", "2024-01-15", "2024-01-05", "2024-01-02"],
        "amount" => [100, 200, 150, 75]
    )?;
    ctx.register_table("orders", df.into_view())?;

    // Use SQL DISTINCT ON which DataFusion supports
    let first_orders = ctx.sql("
        SELECT DISTINCT ON (customer) customer, order_date, amount
        FROM orders
        ORDER BY customer, order_date ASC
    ").await?;

    first_orders.show().await?;
    // +----------+------------+--------+
    // | customer | order_date | amount |
    // +----------+------------+--------+
    // | Alice    | 2024-01-01 | 100    |
    // | Bob      | 2024-01-02 | 75     |
    // +----------+------------+--------+
    Ok(())
}
```

**SQL equivalent:**

```sql
SELECT DISTINCT ON (customer) customer, order_date, amount
FROM orders
ORDER BY customer, order_date ASC;
```

<!-- TODO (link definitions, see file-top item 7): the definitions below cover
only the methods referenced from `### Positional Union`. The remaining
reference-style links on this page (`.union_by_name_distinct()`,
`.with_column_renamed()`, `.distinct_on()`) are still undefined. -->

[`.union()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.union
[`.union_distinct()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.union_distinct
[`.union_by_name()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.union_by_name
