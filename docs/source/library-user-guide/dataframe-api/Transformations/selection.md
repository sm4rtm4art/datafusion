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

# Selection and Projection Mastery

<!--TODO

1. ABSTRACT
2. INTRODUCTION
-->

```{contents}
:local:
:depth: 2
:caption: Selection and Projection Mastery
```

## Introduction (placeholder)

**Projection controls the shape of your output—choosing which columns to keep, computing derived values, and reducing memory footprint by discarding what you don't need.**

DataFusions DataFrame-API provides methods for every projection need: simple name-based selection via [`.select_columns()`], expression-based computation with [`.select()`], adding columns with [`.with_column()`], renaming via [`.with_column_renamed()`], and removal with [`.drop_columns()`]. Projection pushdown ensures only requested columns are read from the data source.

**SQL equivalent:**

```sql
SELECT
col_a,
col_b AS col_b_renamed,
col_a + col_b AS col_sum
FROM table
```

<!--  SPHINX CODE WITH EMPHASIZING THE CODE (DON'T GET TESTED)

```{code-block} sql
:caption: **SQL equivalent:**
:emphasize-lines: 0
SELECT
    col_a,
    col_b AS col_b_renamed,
    col_a + col_b AS col_sum
FROM table
```
-->

:::{admonition} **Trade-off: DataFrame vs SQL**
:class: important

- **DataFrame shines:** Type-safe column references catch typos at compile time; programmatic column selection from schema; projection pushdown happens automatically
- **SQL shines:** Familiar [`SELECT`] syntax; more readable for simple projections; [`SELECT *`][`select`] for quick exploration

:::

:::{admonition} **Performance note:**
:class: important

Projection is where **columnar vs row-based [`TableProvider`]** differ most — columnar sources (i.e. Parquet, Delta Lake ...) read only requested columns, while row-based sources (i.e. Postgres, MySQL, Oracle...) read full rows and discard unwanted columns during transfer.
:::

#### Basic Selection

**Which method to use:**

- **[`.select_columns()`]** — Pass column names as strings: `select_columns(&["a", "b"])`
- **[`.select()`]** — Pass expressions for computation: `select(vec![col("a"), (col("b") * lit(2)).alias("b_doubled")])`

Use [`.select_columns()`] when you just need existing columns by name. Use [`.select()`] when you need to compute new values, rename with [`.alias()`], or apply functions.

```rust
use datafusion::prelude::*;
use datafusion::assert_batches_eq;

// Creating the dataframe with the dataframe! macro
#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let sample_df = dataframe!(
        "product" => ["Laptop", "Mouse", "Keyboard"],
        "price" => [1200, 25, 75],
        "quantity" => [5, 50, 30],
        "category" => ["Electronics", "Accessories", "Accessories"]
    )?;

    // select_columns(): simple name-based selection
    let result = sample_df.select_columns(&["product", "price"])?.collect().await?;

    assert_batches_eq!(
        &[
            "+----------+-------+",
            "| product  | price |",
            "+----------+-------+",
            "| Laptop   | 1200  |",
            "| Mouse    | 25    |",
            "| Keyboard | 75    |",
            "+----------+-------+",
        ],
        &result
    );

    Ok(())
}
```

#### Intermediate: Expressions and Computed Columns

[`.select()`] accepts any expression—arithmetic, conditionals, function calls—letting you compute new columns inline. Use [`.alias()`] to name computed results, [`.with_column()`] to add columns while keeping existing ones, and [`.with_column_renamed()`] to rename without recomputing.

```rust
use datafusion::prelude::*;
use datafusion::assert_batches_eq;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let sample_df = dataframe!(
        "product" => ["Laptop", "Mouse", "Keyboard"],
        "price" => [1200, 25, 75],
        "quantity" => [5, 50, 30],
        "category" => ["Electronics", "Accessories", "Accessories"]
    )?;

    // select() with computed columns — replaces schema, only keeps what you list
    let df = sample_df.clone().select(vec![
        col("product"),
        (col("price") * col("quantity")).alias("revenue"),
    ])?;
    assert_batches_eq!(&[
        "+----------+---------+",
        "| product  | revenue |",
        "+----------+---------+",
        "| Laptop   | 6000    |",
        "| Mouse    | 1250    |",
        "| Keyboard | 2250    |",
        "+----------+---------+",
    ], &df.clone().collect().await?);

    // with_column() adds column while keeping all existing
    let df = df.with_column("tag", lit("2026"))?;
    assert_batches_eq!(&[
        "+----------+---------+------+",
        "| product  | revenue | tag  |",
        "+----------+---------+------+",
        "| Laptop   | 6000    | 2026 |",
        "| Mouse    | 1250    | 2026 |",
        "| Keyboard | 2250    | 2026 |",
        "+----------+---------+------+",
    ], &df.clone().collect().await?);

    // with_column_renamed() renames without recomputing
    let df = df.with_column_renamed("revenue", "total")?;
    assert_batches_eq!(&[
        "+----------+-------+------+",
        "| product  | total | tag  |",
        "+----------+-------+------+",
        "| Laptop   | 6000  | 2026 |",
        "| Mouse    | 1250  | 2026 |",
        "| Keyboard | 2250  | 2026 |",
        "+----------+-------+------+",
    ], &df.clone().collect().await?);

    // drop_columns() removes specific columns
    let df = df.drop_columns(&["tag"])?;
    assert_batches_eq!(&[
        "+----------+-------+",
        "| product  | total |",
        "+----------+-------+",
        "| Laptop   | 6000  |",
        "| Mouse    | 1250  |",
        "| Keyboard | 2250  |",
        "+----------+-------+",
    ], &df.collect().await?);

    Ok(())
}
```

> _Quick Debug_: **Columns missing after [`.select()`]?** <br>
> Unlike [`.with_column()`], [`.select()`] only keeps columns you explicitly list.

> **Ugly column names?** Without [`.alias()`], column names are the expression's string representation: `col("a") * col("b")` becomes `"a * b"`, `col("x").gt(lit(5))` becomes `"x > Int32(5)"` see [**g**rater **t**hen => `.gt()`][`.gt()`]. Always alias computed columns for readable output.

#### Advanced: Dynamic Column Selection

When column names aren't known until runtime—or you want to select by type—use [`.schema()`] to inspect the DataFrame's structure, then build expressions programmatically. This is where DataFrames shine over SQL: Rust's type system and iterators let you construct queries that would require dynamic SQL generation otherwise.

```rust
use datafusion::prelude::*;
use datafusion::assert_batches_eq;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let sample_df = dataframe!(
        "product" => ["Laptop", "Mouse", "Keyboard"],
        "price" => [1200, 25, 75],
        "quantity" => [5, 50, 30],
        "category" => ["Electronics", "Accessories", "Accessories"]
    )?;

    // Get schema and filter to numeric columns only
    let numeric_cols: Vec<_> = sample_df
        .schema()
        .fields()
        .iter()
        .filter(|f| f.data_type().is_numeric())
        .map(|f| col(f.name()))
        .collect();

    let result = sample_df.clone().select(numeric_cols)?.collect().await?;
    assert_batches_eq!(&[
        "+-------+----------+",
        "| price | quantity |",
        "+-------+----------+",
        "| 1200  | 5        |",
        "| 25    | 50       |",
        "| 75    | 30       |",
        "+-------+----------+",
    ], &result);

    Ok(())
}
```

#### Anti-Pattern: Over-Selection

Selecting all columns with [`col("*")`][`col()`] defeats **projection pushdown**—an optimization where DataFusion tells the data source to only read requested columns. With Parquet files, this can mean reading 2 columns instead of 200, dramatically reducing I/O. Common SQL-Rule of not using [`SELECT * FROM big_table`][`select`]

```rust
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let sample_df = dataframe!(
        "product" => ["Laptop", "Mouse", "Keyboard"],
        "price" => [1200, 25, 75],
        "quantity" => [5, 50, 30],
        "category" => ["Electronics", "Accessories", "Accessories"]
    )?;

    // ❌ DON'T: Select all columns then filter (defeats projection pushdown)
    // In practice, avoid selecting columns you don't need

    // ✅ DO: Select only what you need
    let good = sample_df.clone()
        .select(vec![col("product"), col("price")])?  // Projection pushdown!
        .filter(col("price").gt(lit(100)))?;

    // 'good' reads less data from columnar sources
    good.show().await?;
    // +---------+-------+
    // | product | price |
    // +---------+-------+
    // | Laptop  | 1200  |
    // +---------+-------+

    Ok(())
}
```

_Quick Debug_: **Column not found error?** DataFusion is case-sensitive. Use [`sample_df.schema().field_names()`][`.schema()`] to list available columns.

#### Escape Hatch: SQL Syntax in Rust

Sometimes SQL syntax is just cleaner—especially for complex [`CASE`] expressions or nested functions. [`.select_exprs()`] parses SQL strings into expressions, giving you SQL's brevity with DataFrame's composability.

```rust
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let sample_df = dataframe!(
        "product" => ["Laptop", "Mouse", "Keyboard"],
        "price" => [1200, 25, 75],
        "quantity" => [5, 50, 30],
        "category" => ["Electronics", "Accessories", "Accessories"]
    )?;

    // SQL syntax inside Rust — parsed at runtime
    sample_df.clone()
        .select_exprs(&[
            "product",
            "price * 1.1 AS taxed_price",
            "CASE WHEN price > 100 THEN 'Premium' ELSE 'Standard' END AS tier"
        ])?
        .show().await?;
    // +----------+-------------+----------+
    // | product  | taxed_price | tier     |
    // +----------+-------------+----------+
    // | Laptop   | 1320.0      | Premium  |
    // | Mouse    | 27.5        | Standard |
    // | Keyboard | 82.5        | Standard |
    // +----------+-------------+----------+

    Ok(())
}
```

> **Warning:** This loses compile-time safety. A typo like `"prodict"` compiles fine but fails at runtime. Use when SQL is genuinely clearer, not as a shortcut to avoid learning the expression API.

> **Going deeper:** See [`expr_api`] for complex expression patterns combining both approaches.

---

**Best Practice: Pick a Lane (or Document the Bridge)**

Mixing method chains with embedded SQL strings violates the [Single Level of Abstraction Principle][slap]—readers must context-switch constantly between abstraction levels. Choose _one_ approach:

| Approach                                                | Best For                               | Trade-off                            |
| :------------------------------------------------------ | :------------------------------------- | :----------------------------------- |
| **Full DataFrame**                                      | App logic, refactoring, IDE support    | Type-safe, but more verbose          |
| **Full SQL** via [`ctx.sql()`][`sessioncontext::sql()`] | Ad-hoc queries, portability            | Familiar, but no compile-time checks |
| **Documented Constants**                                | Complex expressions reused across code | Traceable, but requires discipline   |

If you choose the third approach, extract SQL strings into named constants with doc comments:

```rust
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    /// Pricing tier based on unit price.
    /// Business rule: ACME-1234
    const TIER_EXPR: &str = "\
        CASE WHEN price > 100 \
             THEN 'Premium' \
             ELSE 'Standard' \
        END AS tier";

    let sample_df = dataframe!(
        "product" => ["Laptop", "Mouse", "Keyboard"],
        "price" => [1200, 25, 75]
    )?;

    let df = sample_df.select_exprs(&["product", "price", TIER_EXPR])?;
    df.show().await?;

    Ok(())
}
```

This makes SQL expressions discoverable, testable, and traceable—rather than buried inline where they drift and multiply.
