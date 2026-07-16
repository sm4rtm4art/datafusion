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
3. DONE (2026-07-16) — this page owns column-enrichment usage through
   `.with_column()`, `.with_column_renamed()`, and `.drop_columns()`.
4. DONE (2026-07-16) — Transformations owns enrichment usage; schema effects
   are owned by Schema-Management/schema-dataframe-methods.md.
5. POSITION — first action page in the reading order; opens on the
   running-example dataset (introduced in transformation-concepts.md).
-->

# Selection and Projection Mastery

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

[`.select()`] accepts any expression—arithmetic, conditionals, function calls—letting you compute new columns inline. Use [`.alias()`] to name computed results.

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

    Ok(())
}
```

> _Quick Debug_: **Columns missing after [`.select()`]?** <br>
> Unlike [`.with_column()`], [`.select()`] only keeps columns you explicitly list.

> **Ugly column names?** Without [`.alias()`], column names are the expression's string representation: `col("a") * col("b")` becomes `"a * b"`, `col("x").gt(lit(5))` becomes `"x > Int32(5)"` see [**g**rater **t**hen => `.gt()`][`.gt()`]. Always alias computed columns for readable output.

(adding-renaming-and-dropping-columns)=

#### Adding, Renaming, and Dropping Columns

These methods edit the column set while passing other columns through unchanged—unlike `.select()`, which requires listing every column you keep.

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

    let enriched = sample_df
        .with_column("revenue", col("price") * col("quantity"))?
        .with_column("price", col("price") + lit(100))?
        .with_column_renamed("revenue", "total")?
        .drop_columns(&["quantity", "category"])?;

    let batches = enriched.collect().await?;
    assert_batches_eq!(
        &[
            "+----------+-------+-------+",
            "| product  | price | total |",
            "+----------+-------+-------+",
            "| Laptop   | 1300  | 6000  |",
            "| Mouse    | 125   | 1250  |",
            "| Keyboard | 175   | 2250  |",
            "+----------+-------+-------+",
        ],
        &batches
    );
    Ok(())
}
```

These methods avoid re-listing every column that SQL `SELECT` requires. `.with_column_renamed()` is like `SELECT price AS unit_price` but touches only the renamed field, and `.drop_columns()` has no direct SQL keyword because SQL requires listing the columns to keep.

For how these methods reshape the schema, including replace-in-place type and nullability drift, unknown-name no-ops, and rename case sensitivity, see [Changing Schemas with DataFrame Methods](../Schema-Management/schema-dataframe-methods.md#adding-and-replacing-fields) and [Renaming and Removing Fields](../Schema-Management/schema-dataframe-methods.md#renaming-and-removing-fields).

(advanced-dynamic-column-selection)=

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

For detailed `.select_exprs()` behavior and API-mixing guidance, see [Mixing SQL and DataFrames](hybrid-sql.md#selecting-with-sql-expressions).

[`.select()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.select
[`.select_columns()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.select_columns
[`.select_exprs()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.select_exprs
[`.with_column()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.with_column
[`.with_column_renamed()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.with_column_renamed
[`.drop_columns()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.drop_columns
[`.alias()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.alias
[`.schema()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.schema
[`.gt()`]: https://docs.rs/datafusion/latest/datafusion/prelude/enum.Expr.html#method.gt
[`col()`]: https://docs.rs/datafusion/latest/datafusion/prelude/fn.col.html
[`case`]: https://docs.rs/datafusion/latest/datafusion/logical_expr/fn.when.html
[`select`]: ../../../user-guide/sql/select.md
[`tableprovider`]: https://docs.rs/datafusion/latest/datafusion/catalog/trait.TableProvider.html
