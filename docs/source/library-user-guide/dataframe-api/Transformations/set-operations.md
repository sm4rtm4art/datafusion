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

**This is where DataFusion's [Arrow columnar design](../../user-guide/arrow-introduction.md) shines.**

SQL's `UNION` matches columns by _position_, not name. If two tables have the same columns in different orders, SQL silently produces incorrect results—a common source of bugs when combining data from different sources.

Because Arrow schemas carry column names as metadata, DataFusion can align DataFrames by name instead of position. This is impossible in traditional row-based databases where columns are just offsets.

| Method                        | Duplicate Rows | SQL Equivalent                |
| ----------------------------- | -------------- | ----------------------------- |
| [`.union_by_name()`]          | Keeps all      | `UNION ALL` + reorder columns |
| [`.union_by_name_distinct()`] | Removes        | `UNION` + reorder columns     |

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

> **When to use:** Data pipelines combining sources with inconsistent column ordering (e.g., different Parquet writers, CSV exports from different tools).
>
> **SQL equivalent:** None—SQL `UNION` is strictly positional. You'd need to manually reorder columns in one of the queries to match.

**Limitations:**

| Requirement       | Description                                                      | Workaround                             |
| ----------------- | ---------------------------------------------------------------- | -------------------------------------- |
| Same column names | Both DataFrames must have identical column names                 | Rename with [`.with_column_renamed()`] |
| Compatible types  | Types must be castable (`Int32` ↔ `Int64` ✓, `Int32` ↔ `Utf8` ✗) | Cast columns first                     |
| No extra columns  | Columns in one but not the other cause errors                    | Use [`.drop_columns()`] to align       |

<!-- MOVED OUT (monolith-split repair, 2026-07-06): everything that followed
this point was the body of the old "Advanced DataFrame Patterns" —
redistributed per the file-top TODO map (hybrid-sql.md, data-quality.md,
reshaping.md; duplicates of Concepts/null-handling.md,
Writing-DataFrames/executing-dataframes.md, and
Creating-DataFrames/inline-data.md were deleted). -->

### Positional Union

<!-- PLACEHOLDER (content backlog): `.union()` (SQL `UNION ALL`) and
`.union_distinct()` (SQL `UNION`) — position-based alignment, schema
compatibility requirements, contrast with the by-name variants above. -->

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
