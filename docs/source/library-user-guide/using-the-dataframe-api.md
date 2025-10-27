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

# Using the DataFrame API

This guide provides comprehensive documentation for using DataFusion's [`DataFrame`] API in Rust applications. For an introduction to DataFrames and their conceptual foundation, see the [Users Guide].

## Overview

DataFusion [`DataFrame`]s are modeled after the [Pandas DataFrame] interface and implemented as a thin wrapper over a [`LogicalPlan`] that adds functionality for building and executing query plans.

**Key properties:**

- **Lazy evaluation**: Transformations build up an optimized query plan that executes only when you call an action method like [`.collect()`] or [`.show()`]
- **Arrow-based columnar structure**: DataFrames represent data in Apache Arrow's columnar format, where each column is stored as a contiguous, typed array
- **Strict type uniformity**: Each column enforces a single Arrow [`DataType`]—heterogeneous types within a column are not permitted, maintaining Arrow's type safety and vectorization guarantees
- **Immutable transformations**: DataFrame methods return new DataFrames, leaving the original unchanged (functional programming style)

```{contents}
:local:
:depth: 2
```

## SessionContext: The Entry Point for DataFrames

The [`SessionContext`] is the main interface for executing queries with DataFusion. It maintains the state of the connection between a user and an instance of the DataFusion engine.

### SessionContext Method Categories

Like DataFrame, SessionContext exposes a large API surface that becomes easier to navigate once you understand the main categories:

| Category               | Purpose                                | Examples                                                         |
| ---------------------- | -------------------------------------- | ---------------------------------------------------------------- |
| **DataFrame Creation** | Create DataFrames from various sources | `read_parquet()`, `read_csv()`, `sql()`, `table()`               |
| **Table Management**   | Register and manage tables by name     | `register_table()`, `register_parquet()`, `deregister_table()`   |
| **Configuration**      | Control execution behavior             | `with_config()`, `state()`                                       |
| **Catalog Operations** | Manage schemas and databases           | `catalog()`, `catalog_names()`                                   |
| **Extensions**         | Add custom functionality               | `register_udf()`, `register_udaf()`, `register_table_provider()` |

**Common pattern:** Most workflows follow this sequence:

1. **Create context** → Configure if needed
2. **Load/register data** → Files, tables, or in-memory data
3. **Create DataFrame** → From files, tables, or SQL
4. **Execute** → Call action methods

**Query execution flow:**

```
SessionContext
  ↓ creates
DataFrame (lazy)
  ↓ wraps
LogicalPlan
  ↓ optimizes
Optimized LogicalPlan
  ↓ plans into
ExecutionPlan (Pysical Plan)
  ↓ optimizes
Optimized ExecutionPlan
  ↓ executes
RecordBatch streams
```

### Configuration and Setup

You can create a [`SessionContext`] with default settings or customize it for your needs:

```rust
use datafusion::prelude::*;
use datafusion::execution::config::SessionConfig;

// Default context
let ctx = SessionContext::new();

// Customized context for performance tuning
let config = SessionConfig::new()
    .with_batch_size(8192)
    .with_target_partitions(num_cpus::get());
let ctx = SessionContext::with_config(config);
```

See the [Performance and Best Practices](#performance-and-best-practices) section for more on configuration options.

## Relationship between [`LogicalPlan`]s and `DataFrame`s

A DataFusion [`DataFrame`] is best understood as a pairing of two components:

- **[`LogicalPlan`]**: A tree representation of your query operations (like `.select()`, `.filter()`, `.join()`) that describes **what** computation to perform without specifying how to execute it
- **[`SessionState`]**: A snapshot of the session's configuration and runtime context (like timezone, memory limits, registered functions) that defines **how** and **when** to execute the plan

This pairing ensures consistent behavior even as the session evolves—for example, time-dependent functions like [`now()`] use the state captured when the DataFrame was created, not when it executes.

**DataFrame composition and origins:**

A `DataFrame` is an immutable handle that bundles a [`LogicalPlan`] (the "what") with a [`SessionState`] snapshot (the "how/when") taken from the creating [`SessionContext`]. Transformations like `.select()`, `.filter()`, and `.join()` derive a new `LogicalPlan` and return a new `DataFrame` that carries forward the same `SessionState`. Actions (for example, `.collect()`, `.show()`, or `create_physical_plan()`) materialize the plan; until then, nothing executes.

```
SessionContext
  ├─ provides → SessionState (snapshot)
  └─ creates  → DataFrame
                   ├─ SessionState ← from SessionContext at creation time
                   └─ LogicalPlan  ← built via:
                       - DataFrame ops: .select(), .filter(), .join(), ...
                       - SQL: ctx.sql("...") → DataFrame
                       - LogicalPlanBuilder: build plan → DataFrame::new(state, plan)

Key API paths
DataFrame ↔ into_parts() ↔ (SessionState, LogicalPlan)
DataFrame → into_optimized_plan() → Optimized LogicalPlan
DataFrame → create_physical_plan() → ExecutionPlan
```

### Converting Between `DataFrame` and `LogicalPlan`

While the DataFrame API covers most use cases, you may need to work directly with the `LogicalPlan`—for example, to apply custom optimizer rules, integrate with advanced query rewriting logic, or programmatically inspect/modify the query structure before execution.

**DataFrame vs LogicalPlan - Same query, different representations:**

| **Using DataFrame API**            | **Using LogicalPlan directly**                                    |
| ---------------------------------- | ----------------------------------------------------------------- |
| Fluent, high-level                 | Low-level, explicit tree building                                 |
| `df.select(...).filter(...)`       | `LogicalPlanBuilder::from(plan).project(...).filter(...).build()` |
| Automatically carries SessionState | You must manage SessionState separately                           |
| Easy to read and write             | More verbose, requires understanding plan structure               |

The API makes it easy to move between representations:

```rust
use datafusion::prelude::*;
use datafusion::error::Result;
use datafusion::logical_expr::LogicalPlanBuilder;

#[tokio::main]
async fn main() -> Result<()>{
    let ctx = SessionContext::new();
    let df = ctx.read_csv("tests/data/example.csv", CsvReadOptions::new()).await?;

    // Extract both SessionState and LogicalPlan from the DataFrame
    let (state, plan) = df.into_parts();

    // Manipulate the plan using LogicalPlanBuilder
    let modified_plan = LogicalPlanBuilder::from(plan)
        .filter(col("a").gt(lit(5)))?
        .build()?;

    // Reconstruct a DataFrame with the modified plan
    let new_df = DataFrame::new(state, modified_plan);

    Ok(())
}
```

### DataFrame and LogicalPlanBuilder Equivalence

Using [`DataFrame`] methods produces the same [`LogicalPlan`] as using [`LogicalPlanBuilder`] directly:

```rust
use datafusion::prelude::*;
use datafusion::error::Result;
use datafusion::logical_expr::LogicalPlanBuilder;

#[tokio::main]
async fn main() -> Result<()>{
    let ctx = SessionContext::new();

    // Build a plan using DataFrame API
    let df_from_api = ctx.read_csv("tests/data/example.csv", CsvReadOptions::new()).await?;
    let df_from_api = df_from_api.select(vec![col("a"), col("b")])?
        .sort(vec![col("a").sort(true, true)])?;
    let (_, plan_from_api) = df_from_api.into_parts();

    // Build the same plan using LogicalPlanBuilder
    // Equivalent to: SELECT a, b FROM example.csv ORDER BY a
    let df_for_builder = ctx.read_csv("tests/data/example.csv", CsvReadOptions::new()).await?;
    let (_state, base_plan) = df_for_builder.into_parts();
    let plan_from_builder = LogicalPlanBuilder::from(base_plan)
        .project(vec![col("a"), col("b")])?
        .sort(vec![col("a").sort(true, true)])?
        .build()?;

    // Both approaches produce identical logical plans
    assert_eq!(plan_from_api, plan_from_builder);
    Ok(())
}
```

### Key API Methods

When working with the `DataFrame`/`LogicalPlan` boundary, be aware of these methods:

- **[`into_parts()`]**: Returns `(SessionState, LogicalPlan)`. Use this when you need to manipulate the plan while preserving the exact session snapshot. This is the recommended way to extract both components.

- **[`into_unoptimized_plan()`]**: Returns the unoptimized `LogicalPlan` but **loses the `SessionState` snapshot**. Useful for plan inspection or tests; for production, prefer [`into_parts()`] (or executing the `DataFrame`) to preserve session state.

  > ⚠️ **Warning: Perils of Lost State**
  >
  > Discarding `SessionState` means the execution context can change unexpectedly. For example, the `now()` function returns the query execution start time captured in the session state—if you lose that snapshot and re-execute the plan later, `now()` will return a different timestamp. Similarly, changes to session configuration (like timezone settings) will affect re-execution. Always use [`into_parts()`] when you intend to execute the plan later.
  >
  > See the [scalar functions documentation](../user-guide/sql/scalar_functions.md#now) for details on how `now()` and other time-dependent functions work.

- **[`into_optimized_plan()`]**: Returns the optimized `LogicalPlan` after running query optimization rules. Also **loses session state**. Useful for plan inspection/tests; for production re-use, prefer [`into_parts()`].

- **[`create_physical_plan()`]**: Converts the `LogicalPlan` to an [`ExecutionPlan`] (doesn't execute). Actions like [`collect()`] and [`show()`] handle this internally—you rarely need to call this directly.

- **[`into_view()`]**: Converts a `DataFrame` into a [`TableProvider`] that can be registered as a view using [`SessionContext::register_table()`].

> **See also:**
>
> - [DataFrame Execution](#dataframe-execution) for execution methods
> - [DataFrame Transformations](#dataframe-transformations) for building queries
> - [How DataFrames Work](../user-guide/dataframe.md#how-dataframes-work-lazy-evaluation-and-arrow-output) for lazy evaluation details

## How to Create a DataFrame

DataFrames can be created in several ways, each suited to different use cases:

1. **From files**: Read Parquet, CSV, JSON, Avro, or Arrow files — [From Parquet Files](#1-from-parquet-files)
2. **From a registered table**: Access tables by name — [From a Registered Table](#2-from-a-registered-table)
3. **From SQL queries**: Execute SQL and get a DataFrame — [From SQL Queries](#3-from-sql-queries)
4. **From in-memory data**: Create from Arrow RecordBatches — [From In-Memory Data](#4-from-in-memory-data)
5. **From inline data**: Quick examples and tests with the macro — [From Inline Data](#5-from-inline-data-using-the-dataframe-macro)
6. **Advanced**: Construct directly from a LogicalPlan — [Constructing from a LogicalPlan](#6-advanced-constructing-from-a-logicalplan)

You can also mix these approaches—for example, creating a DataFrame from SQL and then applying DataFrame transformations. See [Mixing SQL and DataFrames](#mixing-sql-and-dataframes) for examples.

### 1. From Files

Read files like Parquet, CSV, JSON, Avro, or Arrow files directly into a `DataFrame`.

**Common pattern:** `ctx.read_X(path, options).await?`

```rust
use datafusion::prelude::*;
use datafusion::error::Result;

#[tokio::main]
async fn main() -> Result<()> {
    let ctx = SessionContext::new();

    // Read Parquet file
    let df = ctx.read_parquet("data.parquet", ParquetReadOptions::default()).await?;
    df.show().await?;

    Ok(())
}
```

**All supported file formats:**

| Format        | Method             | Notes                                   |
| ------------- | ------------------ | --------------------------------------- |
| **Parquet**   | [`read_parquet()`] | Columnar format, best performance       |
| **CSV**       | [`read_csv()`]     | Comma-separated values                  |
| **JSON**      | [`read_json()`]    | NDJSON (newline delimited)              |
| **Avro**      | [`read_avro()`]    | Binary row-based format                 |
| **Arrow IPC** | [`read_arrow()`]   | Arrow's native format (.arrow/.feather) |

```rust
use datafusion::prelude::*;
use datafusion::datasource::arrow::ArrowReadOptions;

// CSV with custom options
let df = ctx.read_csv("data.csv", CsvReadOptions::new()
    .has_header(true)
    .delimiter(b';')).await?;

// JSON (NDJSON - newline delimited JSON)
let df = ctx.read_json("data.ndjson", NdJsonReadOptions::default()).await?;

// Arrow IPC (.arrow / .feather files)
let df = ctx.read_arrow("data.arrow", ArrowReadOptions::default()).await?;

// Multiple files (works for all formats)
let df = ctx.read_csv(vec!["data1.csv", "data2.csv"], CsvReadOptions::new()).await?;
```

> **Note on NDJSON**: DataFusion uses NDJSON (Newline Delimited JSON), where each line is a separate JSON object. This differs from standard JSON (a single array/object) and enables efficient streaming of large datasets. Common file extensions: `.ndjson`, `.jsonl`, or `.json`.

> **Note on Arrow IPC**: The Arrow IPC (Inter-Process Communication) format is Arrow's native serialization format. If you've heard of "Feather v2", that's the same thing—Feather v2 is now synonymous with Arrow IPC.

### 2. From a Registered Table

Register tables by name in the [`SessionContext`], then access them with [`table()`]. This is useful for:

- **Reusing data** across multiple queries without re-reading files
- **Sharing data** between SQL and DataFrame operations
- **Creating virtual tables** from in-memory data or custom sources

**Common registration methods:**

| Method                 | Purpose                                              |
| ---------------------- | ---------------------------------------------------- |
| [`register_batch()`]   | Register a single Arrow RecordBatch                  |
| [`register_table()`]   | Register any TableProvider (custom sources)          |
| [`register_csv()`]     | Register CSV file(s) without reading into memory     |
| [`register_parquet()`] | Register Parquet file(s) without reading into memory |

**Example:**

```rust
use std::sync::Arc;
use datafusion::prelude::*;
use datafusion::arrow::array::{ArrayRef, Int32Array, StringArray};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::Result;

#[tokio::main]
async fn main() -> Result<()> {
    let ctx = SessionContext::new();

    // Register in-memory data as a table
    let data = RecordBatch::try_from_iter(vec![
        ("id", Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef),
        ("name", Arc::new(StringArray::from(vec!["Alice", "Bob", "Carol"])) as ArrayRef),
    ])?;
    ctx.register_batch("users", data)?;

    // Register a Parquet file without loading it
    ctx.register_parquet("orders", "orders.parquet", ParquetReadOptions::default()).await?;

    // Now access as DataFrames
    let users_df = ctx.table("users").await?;
    let orders_df = ctx.table("orders").await?;

    // Or use in SQL
    let result = ctx.sql("SELECT * FROM users JOIN orders ON users.id = orders.user_id").await?;
    result.show().await?;

    Ok(())
}
```

See [`SessionContext`] methods for more registration options.

### 3. From SQL Queries

Execute SQL queries and get the result as a `DataFrame` using [`sql()`]. This is powerful for:

- **Mixing SQL and DataFrame APIs**: Use SQL's declarative syntax for complex joins/aggregations, then DataFrame methods for programmatic transformations
- **Migrating from SQL**: Gradually transition SQL-heavy codebases to DataFrames
- **Leveraging SQL expertise**: Write familiar SQL while gaining DataFrame flexibility

**Key method:**

| Method    | Purpose                                  |
| --------- | ---------------------------------------- |
| [`sql()`] | Execute SQL query and return a DataFrame |

**Example - Mixing SQL and DataFrame operations:**

```rust
use std::sync::Arc;
use datafusion::prelude::*;
use datafusion::arrow::array::{ArrayRef, Int32Array, StringArray};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::Result;

#[tokio::main]
async fn main() -> Result<()> {
    let ctx = SessionContext::new();

    // Register sample data
    let users = RecordBatch::try_from_iter(vec![
        ("id", Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef),
        ("name", Arc::new(StringArray::from(vec!["Alice", "Bob", "Carol"])) as ArrayRef),
        ("department", Arc::new(StringArray::from(vec!["Engineering", "Sales", "Engineering"])) as ArrayRef),
    ])?;
    ctx.register_batch("users", users)?;

    // Start with SQL for complex logic
    let df = ctx.sql("
        SELECT
            department,
            COUNT(*) as employee_count
        FROM users
        WHERE name != 'Bob'
        GROUP BY department
    ").await?;

    // Continue with DataFrame API for programmatic filtering
    let df = df.filter(col("employee_count").gt(lit(1)))?
              .sort(vec![col("employee_count").sort(false, true)])?;

    df.show().await?;
    // Outputs:
    // +-------------+----------------+
    // | department  | employee_count |
    // +-------------+----------------+
    // | Engineering | 2              |
    // +-------------+----------------+

    Ok(())
}
```

> **Tip**: SQL queries in DataFusion can reference any registered table, view, or file. See [Mixing SQL and DataFrames](#mixing-sql-and-dataframes) for more advanced patterns.

### 4. From In-Memory Data

Create DataFrames from Arrow [`RecordBatch`]es using [`read_batch()`] or [`read_batches()`]. This is useful for:

- **Testing and prototyping**: Quickly create sample data for development
- **Integrating with Arrow ecosystem**: Process data from Arrow Flight, Parquet readers, or other Arrow-based systems
- **In-process analytics**: Analyze data already in Arrow format without serialization overhead
- **Joining external data**: Combine programmatically-generated data with existing tables

**Key methods:**

| Method             | Purpose                                      |
| ------------------ | -------------------------------------------- |
| [`read_batch()`]   | Create DataFrame from a single RecordBatch   |
| [`read_batches()`] | Create DataFrame from multiple RecordBatches |

**Example - Creating DataFrame from RecordBatch:**

```rust
use std::sync::Arc;
use datafusion::prelude::*;
use datafusion::arrow::array::{ArrayRef, Int32Array, Float64Array};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::Result;

#[tokio::main]
async fn main() -> Result<()> {
    let ctx = SessionContext::new();

    // Create a RecordBatch (e.g., from Arrow Flight, Parquet, or generated)
    let batch = RecordBatch::try_from_iter(vec![
        ("product_id", Arc::new(Int32Array::from(vec![1, 2, 3, 4])) as ArrayRef),
        ("revenue", Arc::new(Float64Array::from(vec![1200.0, 450.0, 890.0, 2100.0])) as ArrayRef),
    ])?;

    // Create DataFrame and apply transformations
    let df = ctx.read_batch(batch)?
        .filter(col("revenue").gt(lit(500.0)))?
        .sort(vec![col("revenue").sort(false, true)])?;

    df.show().await?;
    // Outputs:
    // +------------+---------+
    // | product_id | revenue |
    // +------------+---------+
    // | 4          | 2100.0  |
    // | 1          | 1200.0  |
    // | 3          | 890.0   |
    // +------------+---------+

    Ok(())
}
```

**Example - Multiple batches:**

```rust
// Process multiple RecordBatches as a single DataFrame
let batches = vec![batch1, batch2, batch3];
let df = ctx.read_batches(batches)?;
```

> **Tip**: If you need to reuse the same RecordBatch data multiple times, consider registering it as a table with [`register_batch()`] instead. See [From a Registered Table](#2-from-a-registered-table).

### 5. From Inline Data (using the `dataframe!` macro)

Create DataFrames from inline literals. This is ideal for quick examples, unit tests, prototyping, and learning without external files.

#### Method 1: `DataFrame::from_columns()` - Pandas-like approach\*\*

If you're coming from pandas, [`DataFrame::from_columns()`] provides a familiar dictionary-like pattern:

```rust
use std::sync::Arc;
use datafusion::prelude::*;
use datafusion::arrow::array::{ArrayRef, Int32Array, StringArray};
use datafusion::error::Result;

#[tokio::main]
async fn main() -> Result<()> {
    let df = DataFrame::from_columns(vec![
        ("id", Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef),
        ("name", Arc::new(StringArray::from(vec!["Alice", "Bob", "Carol"])) as ArrayRef),
    ])?;
    df.show().await?;
    Ok(())
}
```

#### Method 2: `dataframe!` macro - Simplified syntax\*\*

The [`dataframe!`] macro provides the same functionality with much less boilerplate:

```rust
use datafusion::prelude::*;
use datafusion::error::Result;

#[tokio::main]
async fn main() -> Result<()> {
    let df = dataframe!(
        "id" => [1, 2, 3],
        "name" => ["Alice", "Bob", "Carol"]
    )?;
    df.show().await?;
    Ok(())
}
```

**Handling null values:**

Both approaches use Rust's `Option` type for nullable values:

```rust
// With dataframe! macro
let df = dataframe!(
    "id" => [1, 2, 3],
    "value" => [Some("foo"), None, Some("bar")],  // Nullable string column
    "score" => [Some(100), Some(200), None]       // Nullable int column
)?;
```

> **Note on null values**: In Rust, `Some(value)` represents a present value and `None` represents a null (SQL `NULL`). For important distinctions between `None`, `Null`, and `NaN`, see [Understanding Null Values](../../user-guide/dataframe.md#understanding-null-values-none-null-and-nan) in the Users Guide.

**Key differences:**

| Aspect                | `from_columns()`                           | `dataframe!` macro         |
| --------------------- | ------------------------------------------ | -------------------------- |
| **Syntax**            | Requires `Arc`, `ArrayRef`, type imports   | Minimal, infers everything |
| **Use case**          | When you have existing Arrow arrays        | Quick inline data creation |
| **Type inference**    | Manual (`Int32Array`, `StringArray`, etc.) | Automatic from literals    |
| **Verbosity**         | ~10 lines for simple example               | ~3 lines for same result   |
| **Nullability**       | Use `Some(value)` / `None`                 | Same - `Some()` / `None`   |
| **Pandas similarity** | Dictionary-like `vec![("col", array)]`     | Cleaner column syntax      |

#### Testing with `assert_batches_eq!`

When writing tests, the `dataframe!` macro pairs perfectly with [`assert_batches_eq!`] for validating results. While `dataframe!` creates test data, `assert_batches_eq!` validates that your DataFrame operations produce the expected output.

The [`assert_batches_eq!`] macro compares the pretty-formatted output of RecordBatches with an expected vector of strings. It's designed so that **failure output can be directly copy/pasted** into your test code as expected results—making test maintenance simple. This works with **any** DataFrame (from files, SQL, in-memory, etc.), not just those created with `dataframe!`.

**How it works:**

- **On success**: Returns `()` silently (test passes)
- **On mismatch**: Panics with a readable diff showing expected vs actual tables
- **Comparison**: Exact string match of the pretty-printed table (order-sensitive)
- **Error location**: Being a macro, errors appear on the correct line in your test

**Signature:** `assert_batches_eq!(expected_lines: &[&str], batches: &[RecordBatch])`

**Example - Testing DataFrame transformations:**

```rust
use datafusion::prelude::*;
use datafusion::assert_batches_eq;

#[tokio::test]
async fn test_filter_and_aggregate() -> datafusion::error::Result<()> {
    // Create test data
    let df = dataframe!(
        "department" => ["Sales", "Sales", "Engineering", "Engineering"],
        "salary" => [50000, 55000, 80000, 85000]
    )?;

    // Apply transformations
    let result = df
        .aggregate(vec![col("department")], vec![sum(col("salary")).alias("total")])?
        .filter(col("total").gt(lit(100000)))?
        .sort(vec![col("total").sort(false, true)])?;

    // Collect and validate
    let batches = result.collect().await?;
    datafusion::assert_batches_eq!(
        &[
            "+-------------+--------+",
            "| department  | total  |",
            "+-------------+--------+",
            "| Engineering | 165000 |",
            "| Sales       | 105000 |",
            "+-------------+--------+",
        ],
        &batches
    );

    Ok(())
}
```

**Key features:**

- **Failure output is copy-pasteable**: When tests fail, you can copy the actual output directly into your expected result
- **Handles NULL and NaN distinctly**: Nulls display as empty cells or `NULL`, NaN displays as `NaN`
- **Works with any data source**: Files, SQL, in-memory, etc.—not just `dataframe!` macro

**Variants:**

- [`assert_batches_sorted_eq!`]: For order-insensitive comparisons

### 6. Advanced: Constructing from a LogicalPlan

For advanced integrations, you can construct a `DataFrame` directly from a pre-built [`LogicalPlan`] using [`DataFrame::new`]. This low-level approach is useful for:

- **Building custom query builders**: Create your own DSL or API that compiles to DataFusion plans
- **Integrating with other systems**: Convert plans from Substrait, Spark, or other query engines
- **Programmatic plan construction**: Use [`LogicalPlanBuilder`] for complex plan manipulations
- **Query rewriting**: Intercept and modify plans before execution
- **Testing optimizer rules**: Create specific plan structures for testing

**Example - Basic construction:**

```rust
use datafusion::prelude::*;
use datafusion::logical_expr::LogicalPlanBuilder;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();

    // Build a plan using LogicalPlanBuilder
    let plan = LogicalPlanBuilder::empty(true).build()?;

    // Construct DataFrame from plan and session state
    let df = DataFrame::new(ctx.state(), plan);

    df.show().await?;
    Ok(())
}
```

**Example - Building a plan programmatically:**

```rust
use datafusion::logical_expr::LogicalPlanBuilder;

// Load a table and build a complex plan
let table = ctx.table("users").await?;
let (_state, base_plan) = table.into_parts();

let plan = LogicalPlanBuilder::from(base_plan)
    .filter(col("age").gt(lit(21)))?
    .project(vec![col("name"), col("age")])?
    .sort(vec![col("age").sort(false, true)])?
    .limit(0, Some(10))?
    .build()?;

let df = DataFrame::new(ctx.state(), plan);
```

**Going deeper:**

- **[`DataFrame::new`]**: Low-level constructor documentation
- **[`LogicalPlanBuilder`]**: Builder API for constructing plans
- **[`LogicalPlan`]**: Plan node types and structure
- **[Query Optimizer guide](query-optimizer.md)**: Understanding how plans are optimized
- **Relationship between `LogicalPlan`s and `DataFrame`s**: See [section below](#relationship-between-logicalplans-and-dataframes) for converting between them

Most users won't need this level of control—the higher-level creation methods (1-5 above) cover typical use cases. Use this approach when you need to work directly with DataFusion's query plan representation.

## DataFrame Execution

DataFusion [`DataFrame`]s use **lazy evaluation**: transformations build a query plan without processing data. Execution only happens when you call an action method like [`collect()`] or [`show()`]. This allows DataFusion to optimize the entire query before touching any data.

**Key properties:**

- **Reusable**: DataFrames can be executed multiple times—each execution re-runs the optimized query
- **Async**: All execution methods are async and require an async runtime (e.g., `tokio`)
- **Non-destructive**: Executing a DataFrame doesn't consume it; you can execute the same DataFrame repeatedly

> **Learn more about lazy evaluation**: See [How DataFrames Work](../user-guide/dataframe.md#how-dataframes-work-lazy-evaluation-and-arrow-output) in the Users Guide for a detailed explanation of lazy evaluation and optimization.

### Execution Methods

Execution methods trigger query execution and either return results to your application or write results to external sinks.

See [Debugging Techniques](#debugging-techniques) to inspect logical/physical plans and use EXPLAIN.

- For methods that return results, see the table below.
- For writing to files or tables (sinks), see Write DataFrame to Files.

#### Methods that Return Results

These methods execute the query and return data to your application:

| Method                               | Pattern & Behavior                                                                 | Best For                                                                            |
| ------------------------------------ | ---------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------- |
| **[`collect()`]**                    | Get all data - buffers entire result                                               | Small-medium results, in-memory processing                                          |
| **[`collect_partitioned()`]**        | Get all data - preserves partition structure                                       | Parallel processing/writing                                                         |
| **[`execute_stream()`]**             | Process lazily - yields batches incrementally (one batch ~8K rows)                 | Large results, ETL, memory-constrained                                              |
| **[`execute_stream_partitioned()`]** | Process lazily - one stream per partition                                          | Parallel streaming with partition awareness                                         |
| **[`count()`]**                      | Metadata only - may skip reading actual data (Parquet statistics when available)   | Quick row counts (optimized when possible)                                          |
| **[`show()`]**                       | Display (prints); collects **all** rows then pretty-prints                         | Interactive preview on small data; use `to_string()` for programmatic string output |
| **[`show_limit(n)`]**                | Display (prints); injects `LIMIT n` into the plan, then collects and pretty-prints | Quick preview on large data; prefer over `show()`                                   |
| **[`cache()`]**                      | Materialize - stores entire result in memory for reuse                             | Expensive queries used multiple times                                               |

### Execution Patterns and Trade-offs

Choose your execution method based on these key trade-offs:

```
Need to execute a DataFrame?
  ├─ Result fits in memory and you need all rows → collect()
  │
  ├─ Result is large and can be processed incrementally → execute_stream()
  │    └─ Need partition awareness → execute_stream_partitioned()
  │
  ├─ Quick preview for interactive work → show_limit(n)
  │
  ├─ You will reuse results multiple times → cache() then operate
  │
  └─ You need only metadata (row count) → count()
```

**Trade-off 1: Collect vs Streaming (and show/show_limit)**

`show()` collects all rows before printing; `show_limit(n)` injects LIMIT n and then collects. Prefer `show_limit(n)` for quick previews on large datasets. Use `to_string()` to get the same formatted table as a `String` for logging or testing.

Use `collect()` when the result fits in memory and you need random access to all rows. Use `execute_stream()` for large datasets that exceed available RAM or when you can process data incrementally.

**Trade-off 2: Cache vs Re-execution**

Cache results when you'll query the same DataFrame multiple times. The upfront execution cost pays off when amortized across multiple operations, but only use caching when the result fits comfortably in memory. Cached data lives in executor memory and is invalidated when the `SessionContext` is dropped.

```rust
// Without cache - re-executes query each time
let df = ctx.read_csv("large.csv", CsvReadOptions::new()).await?
    .filter(col("amount").gt(lit(1000)))?
    .aggregate(...)?;
let count1 = df.clone().count().await?; // Executes full query
let preview = df.clone().limit(0, Some(10))?.collect().await?; // Executes again

// With cache - execute once, reuse many times
let cached_df = df.cache().await?; // Execute and store in memory
let count2 = cached_df.clone().count().await?; // Uses cached data
let preview2 = cached_df.clone().limit(0, Some(10))?.collect().await?; // Uses cached data
```

**Trade-off 3: Unified vs Partitioned Processing**

Use partitioned methods when you need to process data in parallel while maintaining partition boundaries. This is valuable for parallel writing to separate files or when processing each partition independently across multiple threads.

```rust
// Unified - simpler code
let batches = df.collect().await?;

// Partitioned - enables parallel processing
let partitions = df.collect_partitioned().await?;
// Process each partition in parallel threads/tasks
```

> **Performance tips:**
>
> - Batch size (~8K rows by default) can be tuned via `SessionConfig`
> - For partition tuning, see [Performance and Best Practices](#performance-and-best-practices)

## DataFrame Transformations

Once you have a DataFrame, you can transform it using a rich set of operations. DataFrames are immutable—each transformation returns a new DataFrame, allowing you to chain operations together. All transformations are lazily evaluated, meaning they build up a query plan that executes only when you call an action like [`collect()`] or [`show()`].

**In this section:**

- Selection and projection
- Filtering with predicates
- Aggregations and grouping
- Joins and set operations
- Sorting and limiting

### Selection and Projection

Select specific columns or create computed columns using [`select()`], [`select_columns()`], and [`drop_columns()`]:

```rust
use datafusion::prelude::*;
use datafusion::error::Result;

#[tokio::main]
async fn main() -> Result<()> {
    let ctx = SessionContext::new();
    let df = ctx.read_csv("tests/data/example.csv", CsvReadOptions::new()).await?;

    // Select columns by name
    let df = df.select_columns(&["a", "b"])?;

    // Select with expressions
    let df = df.select(vec![
        col("a"),
        col("b").alias("b_renamed"),
        (col("a") + col("b")).alias("sum")
    ])?;

    // Drop specific columns
    let df = df.drop_columns(&["c"])?;

    Ok(())
}
```

### Filtering

```rust
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();
    let df = ctx.read_csv("tests/data/example.csv", CsvReadOptions::new()).await?;

    // Simple filter
    let df = df.filter(col("a").gt(lit(5)))?;

    // Complex predicates
    let df = df.filter(
        col("a").gt(lit(5))
            .and(col("b").lt(lit(100)))
            .or(col("c").is_null())
    )?;

    Ok(())
}
```

### Aggregations and Grouping

Use [`aggregate()`] for GROUP BY operations with aggregate functions:

```rust
use datafusion::prelude::*;
use datafusion::functions_aggregate::expr_fn::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let df = dataframe!(
        "department" => ["Sales", "Sales", "Engineering", "Engineering"],
        "employee" => ["Alice", "Bob", "Carol", "Dave"],
        "salary" => [50000, 55000, 80000, 85000]
    )?;

    // Group by with multiple aggregations
    let result = df.aggregate(
        vec![col("department")],
        vec![
            sum(col("salary")).alias("total_salary"),
            avg(col("salary")).alias("avg_salary"),
            count(col("employee")).alias("employee_count")
        ]
    )?;

    result.show().await?;
    Ok(())
}
```

### Joins

DataFusion supports all standard SQL join types:

```rust
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let customers = dataframe!(
        "id" => [1, 2, 3],
        "name" => ["Alice", "Bob", "Carol"]
    )?;

    let orders = dataframe!(
        "order_id" => [101, 102, 103],
        "customer_id" => [1, 1, 2],
        "amount" => [100, 200, 150]
    )?;

    // Inner join
    let df = customers.join(
        orders,
        JoinType::Inner,
        &["id"],
        &["customer_id"],
        None
    )?;

    df.show().await?;
    Ok(())
}
```

Available join types: `Inner`, `Left`, `Right`, `Full`, `LeftSemi`, `RightSemi`, `LeftAnti`, `RightAnti`.

### Sorting and Limiting

```rust
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let df = dataframe!(
        "name" => ["Alice", "Bob", "Carol", "Dave"],
        "score" => [85, 92, 78, 95]
    )?;

    // Sort by multiple columns
    let df = df.sort(vec![
        col("score").sort(false, true), // DESC, nulls last
        col("name").sort(true, false)   // ASC, nulls first
    ])?;

    // Pagination: skip 1, take 2
    let df = df.limit(1, Some(2))?;

    df.show().await?;
    Ok(())
}
```

### Set Operations

```rust
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let df1 = dataframe!("id" => [1, 2, 3])?;
    let df2 = dataframe!("id" => [3, 4, 5])?;

    // Union (preserves duplicates)
    let df = df1.clone().union(df2.clone())?;

    // Union with duplicate removal
    let df = df1.clone().union_distinct(df2.clone())?;

    // Distinct values
    let df = df1.distinct()?;

    Ok(())
}
```

## Subqueries

Subqueries allow you to use the result of one query within another query. DataFusion supports scalar subqueries (returning a single value), IN subqueries (checking membership), and EXISTS subqueries (checking existence).

### Scalar Subqueries

```rust
use datafusion::prelude::*;
use datafusion::logical_expr::expr::ScalarSubquery;
use datafusion::functions_aggregate::expr_fn::avg;
use std::sync::Arc;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();

    // Register tables
    let orders = dataframe!(
        "order_id" => [1, 2, 3, 4],
        "amount" => [100, 200, 150, 300]
    )?;
    ctx.register_table("orders", orders.clone().into_view())?;

    // Find orders above average
    let avg_subquery = ctx.table("orders").await?
        .aggregate(vec![], vec![avg(col("amount"))])?
        .select(vec![avg(col("amount"))])?
        .into_unoptimized_plan();

    let result = ctx.table("orders").await?
        .filter(col("amount").gt(scalar_subquery(Arc::new(avg_subquery))))?;

    result.show().await?;
    Ok(())
}
```

### IN Subqueries

```rust
use datafusion::prelude::*;
use std::sync::Arc;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();

    let customers = dataframe!(
        "id" => [1, 2, 3],
        "name" => ["Alice", "Bob", "Carol"]
    )?;

    let premium_customers = dataframe!(
        "customer_id" => [1, 3]
    )?;

    ctx.register_table("customers", customers.into_view())?;
    ctx.register_table("premium", premium_customers.into_view())?;

    // Find customers who are premium
    let premium_ids = ctx.table("premium").await?
        .select(vec![col("customer_id")])?
        .into_unoptimized_plan();

    let result = ctx.table("customers").await?
        .filter(in_subquery(col("id"), Arc::new(premium_ids)))?;

    result.show().await?;
    Ok(())
}
```

## Mixing SQL and DataFrames

One of DataFusion's strengths is the ability to seamlessly mix SQL and DataFrame APIs. You can start with SQL and refine with DataFrames, or build DataFrames and query them with SQL.

### SQL to DataFrame

Execute SQL to get initial results, then use DataFrame methods for additional transformations:

```rust
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();

    // Register a table
    ctx.sql("CREATE EXTERNAL TABLE users (
        id INT,
        name VARCHAR,
        age INT,
        city VARCHAR
    ) STORED AS CSV LOCATION 'users.csv'").await?.collect().await?;

    // Start with SQL, continue with DataFrame API
    let df = ctx.sql("SELECT * FROM users WHERE age > 21").await?
        .filter(col("city").eq(lit("NYC")))?
        .select(vec![col("name"), col("age")])?
        .sort(vec![col("age").sort(false, true)])?;

    df.show().await?;
    Ok(())
}
```

### DataFrame to SQL

Build a DataFrame, register it as a view, then query it with SQL:

```rust
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();

    // Create DataFrame programmatically
    let df = dataframe!(
        "product" => ["Laptop", "Mouse", "Keyboard", "Monitor"],
        "price" => [1200, 25, 75, 300],
        "quantity" => [5, 50, 30, 10]
    )?
    .filter(col("price").gt(lit(50)))?;

    // Register as a view for SQL access
    ctx.register_table("filtered_products", df.into_view())?;

    // Now query with SQL
    let result = ctx.sql("
        SELECT
            product,
            price * quantity as total_value
        FROM filtered_products
        ORDER BY total_value DESC
    ").await?;

    result.show().await?;
    Ok(())
}
```

### Combining Both Approaches

You can alternate between SQL and DataFrame operations as needed:

```rust
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();

    // Step 1: SQL for initial complex join
    let df1 = ctx.sql("
        SELECT o.order_id, o.customer_id, p.product_name, o.quantity
        FROM orders o
        JOIN products p ON o.product_id = p.id
    ").await?;

    // Step 2: DataFrame for programmatic filtering
    let df2 = df1.filter(col("quantity").gt(lit(10)))?;

    // Step 3: Register and use SQL for aggregation
    ctx.register_table("large_orders", df2.into_view())?;

    let final_result = ctx.sql("
        SELECT customer_id, COUNT(*) as order_count
        FROM large_orders
        GROUP BY customer_id
    ").await?;

    final_result.show().await?;
    Ok(())
}
```

## Write DataFrame to Files

You can write the contents of a `DataFrame` to files or tables. When writing,
DataFusion executes the `DataFrame` and streams the results to the output.

### Choosing a File Format

| Method                  | Format Type | Schema Preservation | Key Features                      | Best For                     |
| ----------------------- | ----------- | ------------------- | --------------------------------- | ---------------------------- |
| **[`write_parquet()`]** | Columnar    | Strong              | Partitioning, compression, schema | Analytics, long-term storage |
| **[`write_csv()`]**     | Row-based   | Weak                | Delimiters, compression, portable | Data exchange, spreadsheets  |
| **[`write_json()`]**    | Row-based   | Moderate            | NDJSON format, streaming-friendly | Web APIs, log processing     |
| **[`write_table()`]**   | Varies      | Strong              | INSERT/APPEND operations          | Updating registered tables   |

**SQL Equivalents:**

- `write_table()` is similar to `INSERT INTO table_name ...`
- `write_parquet()`, `write_csv()`, etc. are similar to `COPY TO` in some SQL dialects
- DataFusion also supports `COPY TO` via SQL for file writes

DataFusion comes with support for writing `csv`, `json` `arrow` `avro`, and
`parquet` files, and supports writing custom file formats via API (see
[`custom_file_format.rs`] for an example)

For example, to read a CSV file and write it to a parquet file, use the
[`DataFrame::write_parquet`] method

```rust
use datafusion::prelude::*;
use datafusion::error::Result;
use datafusion::dataframe::DataFrameWriteOptions;

#[tokio::main]
async fn main() -> Result<()> {
    let ctx = SessionContext::new();
    // read example.csv file into a DataFrame
    let df = ctx.read_csv("tests/data/example.csv", CsvReadOptions::new()).await?;
    // stream the contents of the DataFrame to the `example.parquet` file
    let target_path = tempfile::tempdir()?.path().join("example.parquet");
    df.write_parquet(
        target_path.to_str().unwrap(),
        DataFrameWriteOptions::new(),
        None, // writer_options
    ).await;
    Ok(())
}
```

The output file will look like (Example Output):

```sql
> select * from '../datafusion/core/example.parquet';
+---+---+---+
| a | b | c |
+---+---+---+
| 1 | 2 | 3 |
+---+---+---+
```

### Advanced Write Options

You can control partitioning, compression, and other aspects of file writing:

**Partitioned Parquet Files:**

```rust
use datafusion::prelude::*;
use datafusion::dataframe::DataFrameWriteOptions;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();
    let df = dataframe!(
        "year" => [2023, 2023, 2024, 2024],
        "month" => [12, 12, 1, 1],
        "sales" => [100, 200, 150, 300]
    )?;

    // Write partitioned by year and month
    df.write_parquet(
        "output/sales/",
        DataFrameWriteOptions::new()
            .with_partition_by(vec!["year".to_string(), "month".to_string()]),
        Some(
            WriterProperties::builder()
                .set_compression(Compression::SNAPPY)
                .build()
        )
    ).await?;

    // Creates structure: output/sales/year=2023/month=12/...
    //                    output/sales/year=2024/month=1/...
    Ok(())
}
```

**CSV with Custom Delimiters and Compression:**

```rust
use datafusion::prelude::*;
use datafusion::dataframe::DataFrameWriteOptions;
use datafusion::common::config::CsvOptions;
use datafusion::common::parsers::CompressionTypeVariant;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();
    let df = ctx.read_csv("input.csv", CsvReadOptions::new()).await?;

    // Write CSV with tab delimiter and gzip compression
    df.write_csv(
        "output.csv.gz",
        DataFrameWriteOptions::new(),
        Some(
            CsvOptions::default()
                .with_delimiter(b'\t')
                .with_header(true)
                .with_compression(CompressionTypeVariant::GZIP)
        )
    ).await?;

    Ok(())
}
```

**Writing to a Registered Table:**

```rust
use datafusion::prelude::*;
use datafusion::dataframe::{DataFrameWriteOptions, InsertOp};

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();

    // Create a table
    ctx.sql("CREATE EXTERNAL TABLE my_table (id INT, name TEXT)
             STORED AS PARQUET LOCATION './data/my_table/'")
        .await?
        .collect()
        .await?;

    // Write DataFrame to the table
    let df = dataframe!(
        "id" => [1, 2, 3],
        "name" => ["Alice", "Bob", "Carol"]
    )?;

    df.write_table(
        "my_table",
        DataFrameWriteOptions::new()
            .with_insert_operation(InsertOp::Append)
    ).await?;

    Ok(())
}
```

**Single File Output:**

By default, DataFusion may write multiple files (one per partition). To force a single output file:

```rust
use datafusion::prelude::*;
use datafusion::dataframe::DataFrameWriteOptions;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();
    let df = ctx.read_csv("input.csv", CsvReadOptions::new()).await?;

    df.write_parquet(
        "output/data.parquet",
        DataFrameWriteOptions::new()
            .with_single_file_output(true),
        None
    ).await?;

    Ok(())
}
```

This flexibility allows you to use the best tool for each part of your query - SQL for declarative operations and DataFrames for programmatic logic.

## Performance and Best Practices

Understanding how DataFusion optimizes and executes queries is key to building efficient data applications. For details on the optimizer framework, see the [Query Optimizer guide](query-optimizer.md).

### Automatic Optimizations

DataFusion applies many optimizations automatically:

- **Projection pushdown**: Only reads columns that are actually used
- **Predicate pushdown**: Filters data as early as possible
- **Partition pruning**: Skips reading irrelevant partitions
- **Join reordering**: Optimizes join order for performance

These happen transparently due to lazy evaluation.

### Physical Optimizer Controls

Configure the execution environment for optimal performance:

```rust
use datafusion::prelude::*;
use datafusion::execution::config::SessionConfig;

let config = SessionConfig::new()
    // Number of rows per batch (default: 8192)
    .with_batch_size(8192)
    // Number of parallel partitions (default: num_cpus)
    .with_target_partitions(8)
    // Enable repartitioning of file scans
    .with_repartition_file_scans(true)
    // Minimum file size to trigger repartitioning (64MB)
    .with_repartition_file_min_size(64 * 1024 * 1024);

let ctx = SessionContext::with_config(config);
```

**When to tune these settings:**

- **Increase `batch_size`** for better throughput with large datasets (but uses more memory)
- **Increase `target_partitions`** to utilize more CPU cores for parallel processing
- **Enable `repartition_file_scans`** when reading large files to parallelize I/O

### Common Pitfalls

**Schema mismatches in unions:**

```rust
// This will fail - column names must match exactly
let df1 = dataframe!("id" => [1, 2])?;
let df2 = dataframe!("ID" => [3, 4])?; // Different case!
// df1.union(df2)?; // ERROR

// Use union_by_name for flexibility
let df = df1.union_by_name(df2)?; // Works if types match
```

**Memory issues with large collects:**

```rust
// DON'T: Collect millions of rows into memory
// let all_data = huge_df.collect().await?;

// DO: Stream the results
let mut stream = huge_df.execute_stream().await?;
while let Some(batch) = stream.next().await {
    // Process batch by batch
}
```

**Avoiding Cartesian joins:**

```rust
// This creates a Cartesian product (dangerous with large tables!)
let df = left.join(right, JoinType::Inner, &[], &[], None)?;

// Always specify join conditions
let df = left.join(right, JoinType::Inner, &["id"], &["user_id"], None)?;
```

### Debugging Techniques

Inspect query plans to understand what DataFusion will execute:

```rust
use datafusion::prelude::*;
use datafusion::physical_plan::displayable;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();
    let df = ctx.read_csv("tests/data/example.csv", CsvReadOptions::new()).await?
        .filter(col("a").gt(lit(5)))?
        .select(vec![col("a"), col("b")])?;

    // View the logical plan
    println!("Logical Plan:\n{}", df.logical_plan().display_indent());

    // View the optimized physical plan
    let physical_plan = df.create_physical_plan().await?;
    println!("\nPhysical Plan:\n{}",
        displayable(physical_plan.as_ref()).indent(true));

    Ok(())
}
```

Use `EXPLAIN` to analyze queries:

```rust
let explain_df = df.explain(false, false)?; // (verbose, analyze)
explain_df.show().await?;
```

### Feature Flags

DataFusion's DataFrame API supports optional features. Enable them in `Cargo.toml`:

```toml
[dependencies]
datafusion = { version = "38", features = ["json", "avro", "compression"] }
```

Common features:

- `json`: Enables `read_json()` and `write_json()`
- `avro`: Enables `read_avro()`
- `parquet`: Parquet support (enabled by default)
- `compression`: Compression support for various formats

## Common Operations Quick Reference

- SELECT a,b → `df.select(vec![col("a"), col("b")])?`
- WHERE a > 5 → `df.filter(col("a").gt(lit(5)))?`
- GROUP BY a, SUM(b) → `df.aggregate(vec![col("a")], vec![sum(col("b"))])?`
- ORDER BY a DESC → `df.sort(vec![col("a").sort(false, true)])?`
- LIMIT 10 → `df.limit(0, Some(10))?`
- JOIN USING (id) → `left.join(right, JoinType::Inner, &["id"], &["id"], None)?`
- DISTINCT → `df.distinct()?`
- UNION → `df1.union(df2)?`
- SHOW → `df.show().await?`
- EXPLAIN → `df.explain(false, false)?`

## I/O Quick Reference

- Read CSV → `ctx.read_csv("data.csv", CsvReadOptions::new()).await?`
- Read JSON (NDJSON) → `ctx.read_json("data.json", NdJsonReadOptions::default()).await?`
- Read Parquet → `ctx.read_parquet("data.parquet", ParquetReadOptions::default()).await?`
- Write Parquet (partitioned) → `df.write_parquet("out/", DataFrameWriteOptions::new().with_partition_by(vec!["year".to_string()]), None).await?`
- Write Parquet (single file) → `df.write_parquet("out/file.parquet", DataFrameWriteOptions::new().with_single_file_output(true), None).await?`
- Write CSV (gzip, tab) → `df.write_csv("out.csv.gz", DataFrameWriteOptions::new(), Some(CsvOptions::default().with_delimiter(b'\t').with_has_header(true).with_compression(CompressionTypeVariant::GZIP))).await?`
- Register in-memory table → `ctx.register_table("t", Arc::new(MemTable::try_new(schema, vec![batches])?))?`

[users guide]: ../user-guide/dataframe.md
[pandas dataframe]: https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html
[datafusion ballista]: https://datafusion.apache.org/ballista/
[datafusion comet]: https://datafusion.apache.org/comet/#
[`dataframe`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html
[`dataframe!`]: https://docs.rs/datafusion/latest/datafusion/macro.dataframe.html
[`dataframe::new`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.new
[`dataframe::from_columns()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.from_columns
[`assert_batches_eq!`]: https://docs.rs/datafusion/latest/datafusion/macro.assert_batches_eq.html
[`assert_batches_sorted_eq!`]: https://docs.rs/datafusion/latest/datafusion/macro.assert_batches_sorted_eq.html
[`count()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.count
[`collect()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.collect
[`SessionConfig`]: https://docs.rs/datafusion/latest/datafusion/execution/config/struct.SessionConfig.html
[`sessioncontext`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html
[`sessioncontext` api docs]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html
[`logicalplan`]: https://docs.rs/datafusion/latest/datafusion/logical_expr/enum.LogicalPlan.html
[`logicalplanbuilder`]: https://docs.rs/datafusion/latest/datafusion/logical_expr/struct.LogicalPlanBuilder.html
[`dataframe::write_parquet`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.write_parquet
[`dataframe::write_csv`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.write_csv
[`dataframe::write_json`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.write_json
[`.collect()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.collect
[`.show()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.show
[`show_limit()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.show_limit
[`recordbatch`]: https://docs.rs/arrow-array/latest/arrow_array/struct.RecordBatch.html
[`schema`]: https://docs.rs/arrow-schema/latest/arrow_schema/struct.Schema.html
[`datatype`]: https://docs.rs/arrow-schema/latest/arrow_schema/enum.DataType.html
[`select()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.select
[`select_columns()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.select_columns
[`drop_columns()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.drop_columns
[`filter()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.filter
[`aggregate()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.aggregate
[`join()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.join
[`union()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.union
[`sort()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.sort
[`limit()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.limit
[`csvreadoptions`]: https://docs.rs/datafusion/latest/datafusion/execution/options/struct.CsvReadOptions.html
[`ndjsonreadoptions`]: https://docs.rs/datafusion/latest/datafusion/execution/options/struct.NdJsonReadOptions.html
[`parquetreadoptions`]: https://docs.rs/datafusion/latest/datafusion/execution/options/struct.ParquetReadOptions.html
[`CsvOptions`]: https://docs.rs/datafusion/latest/datafusion/common/config/struct.CsvOptions.html
[`CompressionTypeVariant`]: https://docs.rs/datafusion/latest/datafusion/common/parsers/enum.CompressionTypeVariant.html
[`MemTable`]: https://docs.rs/datafusion/latest/datafusion/datasource/struct.MemTable.html
[`custom_file_format.rs`]: https://github.com/apache/datafusion/blob/main/datafusion-examples/examples/custom_file_format.rs
[`dataframe` api docs]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html
[`union_distinct()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.union_distinct
[`distinct()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.distinct
[`intersect()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.intersect
[`except()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.except
[`collect_partitioned()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.collect_partitioned
[`execute_stream()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.execute_stream
[`execute_stream_partitioned()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.execute_stream_partitioned
[`cache()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.cache
[`show()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.show
[`show_limit()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.show_limit
[`schema()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.schema
[`explain()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.explain
[`write_parquet()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.write_parquet
[`write_csv()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.write_csv
[`write_json()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.write_json
[`write_table()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.write_table
[`read_csv()`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.read_csv
[`read_parquet()`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.read_parquet
[`read_json()`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.read_json
[`read_avro()`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.read_avro
[`read_arrow()`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.read_arrow
[`read_batch()`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.read_batch
[`read_batches()`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.read_batches
[`table()`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.table
[`sql()`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.sql
[`register_table()`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.register_table
[`register_batch()`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.register_batch
[`register_csv()`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.register_csv
[`register_parquet()`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.register_parquet
[`deregister_table()`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.deregister_table
[`with_config()`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.with_config
[`state()`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.state
[`register_table_provider()`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.register_table_provider
[`sessioncontext`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html
[`sessioncontext::register_table()`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.register_table
[`read_csv`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.read_csv
[`read_parquet`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.read_parquet
[`sessionstate`]: https://docs.rs/datafusion/latest/datafusion/execution/session_state/struct.SessionState.html
[`into_parts()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.into_parts
[`into_unoptimized_plan()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.into_unoptimized_plan
[`into_optimized_plan()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.into_optimized_plan
[`create_physical_plan()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.create_physical_plan
[`into_view()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.into_view
[`executionplan`]: https://docs.rs/datafusion/latest/datafusion/physical_plan/trait.ExecutionPlan.html
[`tableprovider`]: https://docs.rs/datafusion/latest/datafusion/catalog/trait.TableProvider.html
[`read_json`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.read_json
[`read_avro`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.read_avro
[`read_arrow`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.read_arrow
[`now()`]: ../user-guide/sql/scalar_functions.md#now
