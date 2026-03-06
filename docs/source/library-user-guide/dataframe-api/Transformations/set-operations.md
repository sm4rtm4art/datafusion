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

# Set Operations by Name

<!--TODO

1. ABSTRACT
2. INTRODUCTION
-->

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

### SQL-DataFrame Hybrid Methods

These methods let you combine SQL's familiar syntax with DataFrame's programmatic power—the best of both worlds.

| Method                | Input             | Output      | Use Case                                  |
| --------------------- | ----------------- | ----------- | ----------------------------------------- |
| [`.parse_sql_expr()`] | SQL string        | `Expr`      | Single expression from config/user input  |
| [`.select_exprs()`]   | SQL strings array | `DataFrame` | Multiple computed columns with SQL syntax |

### Parsing SQL Expressions

[`.parse_sql_expr()`] converts a SQL expression string into a DataFusion `Expr`. Useful when you want SQL syntax for complex expressions but DataFrame chaining for the overall pipeline:

```rust
use datafusion::prelude::*;
use datafusion::error::Result;
use datafusion::assert_batches_eq;

#[tokio::main]
async fn main() -> Result<()> {
    let ctx = SessionContext::new();

    let df = ctx.sql("SELECT 1 as a, 2 as b, 3 as c").await?;

    // Parse SQL expression string into an Expr
    let expr = df.parse_sql_expr("a + b * 2")?;

    // Use it in DataFrame operations
    let df = df.select(vec![col("a"), col("b"), expr.alias("computed")])?;

    let results = df.collect().await?;
    assert_batches_eq!(
        &[
            "+---+---+----------+",
            "| a | b | computed |",
            "+---+---+----------+",
            "| 1 | 2 | 5        |",
            "+---+---+----------+",
        ],
        &results
    );
    Ok(())
}
```

> **Use case:** Dynamically building expressions from user input or configuration files while maintaining type safety in the rest of your pipeline.

### Selecting with SQL Expressions

[`.select_exprs()`] takes an array of SQL expression strings and projects them—combining SQL's concise syntax with DataFrame chaining:

```rust
use datafusion::prelude::*;
use datafusion::error::Result;

#[tokio::main]
async fn main() -> Result<()> {
    let ctx = SessionContext::new();

    let df = ctx.sql("SELECT 'Alice' as name, 100 as price, 0.1 as tax_rate").await?;

    // Use SQL syntax for complex expressions
    let df = df.select_exprs(&[
        "UPPER(name) AS upper_name",
        "price * (1 + tax_rate) AS total",
        "CASE WHEN price > 50 THEN 'expensive' ELSE 'cheap' END AS category"
    ])?;

    df.show().await?;
    // +------------+-------+-----------+
    // | upper_name | total | category  |
    // +------------+-------+-----------+
    // | ALICE      | 110.0 | expensive |
    // +------------+-------+-----------+
    Ok(())
}
```

> **Why use this over pure SQL?** You get SQL's expression syntax while keeping DataFrame's:
>
> - **Chaining:** `.filter()`, `.join()`, `.aggregate()` flow naturally
> - **Composition:** Build pipelines programmatically
> - **Type checking:** Rust compiler catches method name typos

### Parameter Binding

[`.with_param_values()`] binds parameter values to placeholders (`$1`, `$2`, ...) in a plan—useful for prepared statement patterns and preventing SQL injection:

```rust
use datafusion::prelude::*;
use datafusion::error::Result;
use datafusion::common::ScalarValue;

#[tokio::main]
async fn main() -> Result<()> {
    // Create data and register it
    let df = dataframe!(
        "name" => ["Alice", "Bob", "Carol"],
        "age" => [30, 25, 35]
    )?;

    // Filter using a runtime parameter
    let min_age = 28;  // Could come from user input, config, etc.
    let filtered = df.filter(col("age").gt(lit(min_age)))?;

    filtered.show().await?;
    // +-------+-----+
    // | name  | age |
    // +-------+-----+
    // | Alice | 30  |
    // | Carol | 35  |
    // +-------+-----+

    Ok(())
}
```

> **Use cases:**
>
> - **Reusable templates** — Build query once, bind different values
> - **User input** — Safely inject user-supplied values without SQL injection risk
> - **Dynamic filtering** — Change filter values without rebuilding the plan

### Describing Data

[`.describe()`] generates summary statistics for all columns—similar to pandas' `df.describe()`. No single SQL statement can do this:

```rust
use datafusion::prelude::*;
use datafusion::error::Result;

#[tokio::main]
async fn main() -> Result<()> {
    let df = dataframe!(
        "product" => ["A", "B", "C", "D", "E"],
        "price" => [10.0, 25.0, 15.0, 30.0, 20.0],
        "quantity" => [100, 50, 75, 25, 60]
    )?;

    // Get summary statistics
    let stats = df.describe().await?;
    stats.show().await?;

    // Output includes: count, null_count, mean, std, min, max, median
    // +------------+---------+-------+----------+
    // | describe   | product | price | quantity |
    // +------------+---------+-------+----------+
    // | count      | 5.0     | 5.0   | 5.0      |
    // | null_count | 0.0     | 0.0   | 0.0      |
    // | mean       | null    | 20.0  | 62.0     |
    // | std        | null    | 7.9   | 27.4     |
    // | min        | A       | 10.0  | 25       |
    // | max        | E       | 30.0  | 100      |
    // | median     | null    | 20.0  | 60.0     |
    // +------------+---------+-------+----------+
    Ok(())
}
```

> **SQL equivalent:** Would require 7+ separate aggregate queries unioned together—tedious and error-prone.

### Convenience Methods

These methods wrap common patterns into single, ergonomic calls.

| Method           | Purpose                    | SQL Equivalent        |
| ---------------- | -------------------------- | --------------------- |
| [`.fill_null()`] | Replace nulls with default | `COALESCE` per column |
| [`.cache()`]     | Materialize in memory      | None                  |

### Filling Null Values

[`.fill_null()`] replaces null values with a default—in SQL you'd need `COALESCE` for each column:

```rust
use datafusion::prelude::*;
use datafusion::error::Result;
use datafusion_common::ScalarValue;
use std::sync::Arc;
use arrow::array::{Int32Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

#[tokio::main]
async fn main() -> Result<()> {
    // Create data with nulls
    let schema = Arc::new(Schema::new(vec![
        Field::new("name", DataType::Utf8, true),
        Field::new("score", DataType::Int32, true),
    ]));

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec![Some("Alice"), None, Some("Carol")])),
            Arc::new(Int32Array::from(vec![Some(95), Some(87), None])),
        ],
    )?;

    let ctx = SessionContext::new();
    let df = ctx.read_batch(batch)?;

    // Fill nulls in specific columns
    let df = df.fill_null(ScalarValue::from("Unknown"), vec!["name".to_string()])?;

    // Or fill all compatible columns
    let df = df.fill_null(ScalarValue::from(0i32), vec![])?;

    df.show().await?;
    // +---------+-------+
    // | name    | score |
    // +---------+-------+
    // | Alice   | 95    |
    // | Unknown | 87    |
    // | Carol   | 0     |
    // +---------+-------+
    Ok(())
}
```

> **SQL equivalent:** `SELECT COALESCE(name, 'Unknown'), COALESCE(score, 0) FROM ...`—must list each column explicitly.

### Caching DataFrames

[`.cache()`] materializes a DataFrame into memory, useful when you need to reuse intermediate results:

```rust
use datafusion::prelude::*;
use datafusion::error::Result;

#[tokio::main]
async fn main() -> Result<()> {
    let ctx = SessionContext::new();

    // Imagine this is an expensive computation
    let df = ctx.sql("SELECT value AS id FROM generate_series(1, 1000)").await?
        .filter(col("id").gt(lit(500)))?;

    // Cache the result in memory
    let cached = df.cache().await?;

    // Now reuse without recomputing
    let count1 = cached.clone().count().await?;
    let count2 = cached.clone().filter(col("id").lt(lit(750)))?.count().await?;

    println!("Total: {}, Filtered: {}", count1, count2);
    Ok(())
}
```

> **When to use:** Iterative algorithms, multiple aggregations over the same filtered data, or when the source is expensive to read (remote storage, complex joins).

### Execution Control

These methods provide fine-grained control over how query results are produced—essential for memory management and parallel processing.

| Method                            | Returns                 | Use Case                         |
| --------------------------------- | ----------------------- | -------------------------------- |
| [`.execute_stream()`]             | Single stream           | Large datasets, memory-efficient |
| [`.collect_partitioned()`]        | `Vec<Vec<RecordBatch>>` | Process partitions independently |
| [`.execute_stream_partitioned()`] | Multiple streams        | Parallel streaming               |

### Streaming Results

[`.execute_stream()`] returns results as a stream rather than collecting into memory:

```rust
use datafusion::prelude::*;
use datafusion::error::Result;
use futures::StreamExt;

#[tokio::main]
async fn main() -> Result<()> {
    let df = dataframe!(
        "id" => [1, 2, 3, 4, 5],
        "value" => [10, 20, 30, 40, 50]
    )?;

    // Process results as a stream (memory-efficient for large datasets)
    let mut stream = df.execute_stream().await?;

    let mut total_rows = 0;
    while let Some(batch) = stream.next().await {
        let batch = batch?;
        total_rows += batch.num_rows();
        println!("Processed batch with {} rows", batch.num_rows());
    }
    println!("Total: {} rows", total_rows);
    Ok(())
}
```

### Partition-Aware Execution

[`.collect_partitioned()`] and [`.execute_stream_partitioned()`] preserve the underlying data partitioning:

```rust
use datafusion::prelude::*;
use datafusion::error::Result;

#[tokio::main]
async fn main() -> Result<()> {
    let ctx = SessionContext::new();

    let df = ctx.sql("SELECT value AS id FROM generate_series(1, 100)").await?;

    // Collect preserving partitions (useful for parallel processing)
    let partitioned_batches = df.collect_partitioned().await?;

    println!("Number of partitions: {}", partitioned_batches.len());
    for (i, partition) in partitioned_batches.iter().enumerate() {
        let rows: usize = partition.iter().map(|b| b.num_rows()).sum();
        println!("Partition {}: {} rows", i, rows);
    }
    Ok(())
}
```

> **When to use:**
>
> - `.execute_stream()` — Large datasets that don't fit in memory
> - `.collect_partitioned()` — When you need to process partitions independently
> - `.execute_stream_partitioned()` — Parallel streaming across partitions

### Creating from Columns

[`.from_columns()`] creates a DataFrame directly from column arrays—useful for programmatic data construction:

```rust
use datafusion::prelude::*;
use datafusion::error::Result;
use datafusion::assert_batches_eq;
use std::sync::Arc;
use arrow::array::{ArrayRef, Int32Array, StringArray};

#[tokio::main]
async fn main() -> Result<()> {
    // Build columns programmatically
    let ids: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3]));
    let names: ArrayRef = Arc::new(StringArray::from(vec!["Alice", "Bob", "Carol"]));

    let df = DataFrame::from_columns(vec![
        ("id", ids),
        ("name", names),
    ])?;

    let results = df.collect().await?;
    assert_batches_eq!(
        &[
            "+----+-------+",
            "| id | name  |",
            "+----+-------+",
            "| 1  | Alice |",
            "| 2  | Bob   |",
            "| 3  | Carol |",
            "+----+-------+",
        ],
        &results
    );
    Ok(())
}
```

> **Alternative:** The `dataframe!` macro is more concise for literals, but `from_columns()` is better when building from existing `ArrayRef` data.

### Unnesting Arrays

[`.unnest_columns()`] explodes array (list) columns into multiple rows—one row per array element. SQL's `UNNEST` syntax varies significantly across databases; DataFusion provides a consistent API.

```rust
use datafusion::prelude::*;
use datafusion::error::Result;
use datafusion::assert_batches_eq;
use std::sync::Arc;
use arrow::array::{ArrayRef, StringArray, ListArray, Int32Array};
use arrow::datatypes::{DataType, Field};
use arrow::buffer::OffsetBuffer;

#[tokio::main]
async fn main() -> Result<()> {
    // Create a DataFrame with a list column
    let tags_field = Arc::new(Field::new_list_field(DataType::Utf8, true));
    let tags = ListArray::new(
        tags_field,
        OffsetBuffer::from_lengths([2, 1]),  // Alice has 2 tags, Bob has 1
        Arc::new(StringArray::from(vec!["vip", "early", "new"])),
        None
    );

    let df = DataFrame::from_columns(vec![
        ("customer", Arc::new(StringArray::from(vec!["Alice", "Bob"])) as ArrayRef),
        ("tags", Arc::new(tags) as ArrayRef),
    ])?;

    // Explode the tags array into rows
    let expanded = df.unnest_columns(&["tags"])?;

    let results = expanded.collect().await?;
    assert_batches_eq!(
        &[
            "+----------+-------+",
            "| customer | tags  |",
            "+----------+-------+",
            "| Alice    | vip   |",
            "| Alice    | early |",
            "| Bob      | new   |",
            "+----------+-------+",
        ],
        &results
    );
    Ok(())
}
```

### Controlling Unnest Behavior with Options

[`.unnest_columns_with_options()`] provides fine-grained control via [`UnnestOptions`]:

```rust
use datafusion::prelude::*;
use datafusion::error::Result;
use datafusion::assert_batches_eq;
use datafusion_common::UnnestOptions;
use std::sync::Arc;
use arrow::array::{ArrayRef, StringArray, ListArray};
use arrow::datatypes::{DataType, Field};
use arrow::buffer::OffsetBuffer;

#[tokio::main]
async fn main() -> Result<()> {
    // Create data with null and empty arrays
    let tags_field = Arc::new(Field::new_list_field(DataType::Utf8, true));

    // Alice: ["vip"], Bob: null, Carol: [] (empty)
    let tags = ListArray::new(
        tags_field,
        OffsetBuffer::from_lengths([1, 0, 0]),
        Arc::new(StringArray::from(vec!["vip"])),
        Some(vec![true, false, true].into())  // Bob's entry is null
    );

    let df = DataFrame::from_columns(vec![
        ("customer", Arc::new(StringArray::from(vec!["Alice", "Bob", "Carol"])) as ArrayRef),
        ("tags", Arc::new(tags) as ArrayRef),
    ])?;

    // Default: preserve_nulls = true (keeps null rows)
    let with_nulls = df.clone()
        .unnest_columns_with_options(&["tags"], UnnestOptions::new())?;

    let results = with_nulls.collect().await?;
    assert_batches_eq!(
        &[
            "+----------+------+",
            "| customer | tags |",
            "+----------+------+",
            "| Alice    | vip  |",
            "| Bob      |      |",
            "+----------+------+",
        ],
        &results
    );

    // Skip nulls and empty arrays
    let without_nulls = df
        .unnest_columns_with_options(
            &["tags"],
            UnnestOptions::new().with_preserve_nulls(false)
        )?;

    let results = without_nulls.collect().await?;
    assert_batches_eq!(
        &[
            "+----------+------+",
            "| customer | tags |",
            "+----------+------+",
            "| Alice    | vip  |",
            "+----------+------+",
        ],
        &results
    );
    Ok(())
}
```

**UnnestOptions fields:**

| Option           | Default | Effect                                                               |
| ---------------- | ------- | -------------------------------------------------------------------- |
| `preserve_nulls` | `true`  | Keep rows where the array is `NULL` (outputs `NULL` for that column) |
| `recursions`     | `[]`    | For nested arrays, specify recursion depth per column                |

> **Nested arrays:** For deeply nested structures (e.g., `List<List<Int>>`), use `RecursionUnnestOption` to control how many levels to flatten.

[`unnestoptions`]: https://docs.rs/datafusion/latest/datafusion/common/struct.UnnestOptions.html
