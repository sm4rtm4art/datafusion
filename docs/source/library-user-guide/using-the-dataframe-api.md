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

The [`SessionContext`] is the main interface for executing queries with DataFusion. It maintains the state of the connection between a user and an instance of the DataFusion engine, serving as:

- **DataFrame factory**: Create DataFrames from CSV, Parquet, JSON, Avro files, or in-memory Arrow data
- **Table catalog**: Register and manage tables, views, and custom data sources by name
- **Execution environment**: Holds configuration, optimizer rules, and runtime state
- **Query coordinator**: Manages optimization and execution of both DataFrame and SQL queries

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
ExecutionPlan
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

Create DataFrames from inline literals using the [`dataframe!`] macro. This is ideal for:

- **Quick examples and documentation**: Demonstrate DataFusion features without external files
- **Unit tests**: Provide test data inline without fixtures
- **Prototyping**: Experiment with transformations on small datasets
- **Learning**: Follow tutorials and examples without setup overhead

**Syntax:** `dataframe!("column_name" => [values], ...)?`

**Example - Basic usage:**

```rust
use datafusion::prelude::*;
use datafusion::error::Result;

#[tokio::main]
async fn main() -> Result<()> {
    let df = dataframe!(
        "id" => [1, 2, 3],
        "name" => ["Alice", "Bob", "Carol"],
        "age" => [25, 30, 35]
    )?;
    df.show().await?;
    // Outputs:
    // +----+-------+-----+
    // | id | name  | age |
    // +----+-------+-----+
    // | 1  | Alice | 25  |
    // | 2  | Bob   | 30  |
    // | 3  | Carol | 35  |
    // +----+-------+-----+
    Ok(())
}
```

**Handling null values:**

The macro automatically handles `Option` types for nullable columns:

```rust
let df = dataframe!(
    "id" => [1, 2, 3],
    "value" => [Some("foo"), None, Some("bar")],  // String column with nulls
    "score" => [Some(100), Some(200), None]       // Int column with nulls
)?;
```

### 6. Advanced: Constructing from a LogicalPlan

For advanced integrations, you can construct a `DataFrame` directly from a pre-built [`LogicalPlan`] and [`SessionState`] using [`DataFrame::new`]. This is useful when you build plans with [`LogicalPlanBuilder`] or when adapting plans from other systems. Most users won't need this; prefer the higher-level creation methods above.

```rust
use datafusion::logical_expr::LogicalPlanBuilder;

// Build a plan using LogicalPlanBuilder
let plan = LogicalPlanBuilder::empty(true).build()?;

// Construct DataFrame from plan and session state
let df = DataFrame::new(ctx.state(), plan);
```

See [`DataFrame::new`] and [`LogicalPlanBuilder`] documentation for details.

**For testing:** DataFusion provides the [`assert_batches_eq!`] macro to validate query results in tests:

```rust
use datafusion::assert_batches_eq;

let results = dataframe.collect().await?;
assert_batches_eq!(
    &[
        "+----+--------------+",
        "| id | bank_account |",
        "+----+--------------+",
        "| 1  | 9000         |",
        "| 2  | 8000         |",
        "+----+--------------+",
    ],
    &results
);
```

Now let's explore the different ways to execute DataFrames and retrieve results.

## Collect / Streaming Exec

DataFusion [`DataFrame`]s are "lazy", meaning they do no processing until
they are executed, which allows for additional optimizations.

### Execution Methods: Triggering Query Execution

**Execution methods** are what actually run your query. Until you call one of these methods, DataFusion just builds up a plan without touching any data. The main execution methods are:

1. **[`collect()`]**: Executes the query and buffers all output into a `Vec<RecordBatch>` in memory
2. **[`execute_stream()`]**: Begins execution and returns a stream that incrementally computes output
3. **[`show()`]**: Executes the query and prints results to stdout (great for exploration)
4. **[`cache()`]**: Executes the query and buffers the output into a new in-memory DataFrame

**For testing:** After collecting results with [`collect()`], you can validate them using [`assert_batches_eq!`] (as shown in the previous section).

### Collecting Results into Memory

To collect all outputs into a memory buffer, use the [`collect()`] method:

```rust
use datafusion::prelude::*;
use datafusion::error::Result;

#[tokio::main]
async fn main() -> Result<()> {
    let ctx = SessionContext::new();
    // read the contents of a CSV file into a DataFrame
    let df = ctx.read_csv("tests/data/example.csv", CsvReadOptions::new()).await?;
    // execute the query and collect the results as a Vec<RecordBatch>
    let batches = df.collect().await?;
    for record_batch in batches {
        println!("{record_batch:?}");
    }
    Ok(())
}
```

### Streaming Results

Use [`execute_stream()`] to incrementally generate output one `RecordBatch` at a time:

```rust
use datafusion::prelude::*;
use datafusion::error::Result;
use futures::stream::StreamExt;

#[tokio::main]
async fn main() -> Result<()> {
    let ctx = SessionContext::new();
    // read example.csv file into a DataFrame
    let df = ctx.read_csv("tests/data/example.csv", CsvReadOptions::new()).await?;
    // begin execution (returns quickly, does not compute results)
    let mut stream = df.execute_stream().await?;
    // results are returned incrementally as they are computed
    while let Some(record_batch) = stream.next().await {
        println!("{record_batch:?}");
    }
    Ok(())
}
```

### Advanced Execution Methods

For more control over execution, DataFusion provides additional methods:

**Partitioned Collection:**

When you need to preserve partition boundaries (e.g., for parallel writing):

```rust
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();
    let df = ctx.read_csv("tests/data/example.csv", CsvReadOptions::new()).await?;

    // Collect results preserving partition structure
    let partitions: Vec<Vec<RecordBatch>> = df.collect_partitioned().await?;

    for (i, partition) in partitions.iter().enumerate() {
        println!("Partition {}: {} batches", i, partition.len());
    }

    Ok(())
}
```

**Partitioned Streaming:**

Stream results while maintaining partition information:

```rust
use datafusion::prelude::*;
use futures::stream::StreamExt;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();
    let df = ctx.read_csv("tests/data/example.csv", CsvReadOptions::new()).await?;

    // Get a stream per partition
    let mut streams = df.execute_stream_partitioned().await?;

    for (i, mut stream) in streams.into_iter().enumerate() {
        println!("Processing partition {i}");
        while let Some(batch) = stream.next().await {
            // Process batch from this partition
        }
    }

    Ok(())
}
```

**Caching Results:**

For expensive DataFrames that you'll reuse multiple times:

```rust
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();

    let df = ctx.read_csv("large_file.csv", CsvReadOptions::new()).await?
        .filter(col("value").gt(lit(1000)))?
        .aggregate(
            vec![col("category")],
            vec![sum(col("value")).alias("total")]
        )?;

    // Cache the results in memory
    let cached_df = df.cache().await?;

    // Now you can reuse cached_df multiple times without re-execution
    let result1 = cached_df.clone().filter(col("total").gt(lit(10000)))?;
    let result2 = cached_df.clone().sort(vec![col("total").sort(false, true)])?;

    Ok(())
}
```

# Write DataFrame to Files

You can write the contents of a `DataFrame` to files or tables. When writing,
DataFusion executes the `DataFrame` and streams the results to the output.

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

## Relationship between `LogicalPlan`s and `DataFrame`s

The `DataFrame` struct is defined like this:

```rust
use datafusion::execution::session_state::SessionState;
use datafusion::logical_expr::LogicalPlan;
pub struct DataFrame {
    // state required to execute a LogicalPlan
    session_state: Box<SessionState>,
    // LogicalPlan that describes the computation to perform
    plan: LogicalPlan,
}
```

As shown above, `DataFrame` is a thin wrapper of `LogicalPlan`, so you can
easily go back and forth between them.

```rust
use datafusion::prelude::*;
use datafusion::error::Result;
use datafusion::logical_expr::LogicalPlanBuilder;

#[tokio::main]
async fn main() -> Result<()>{
    let ctx = SessionContext::new();
    // read example.csv file into a DataFrame
    let df = ctx.read_csv("tests/data/example.csv", CsvReadOptions::new()).await?;
    // You can easily get the LogicalPlan from the DataFrame
    let (_state, plan) = df.into_parts();
    // Just combine LogicalPlan with SessionContext and you get a DataFrame
    // get LogicalPlan in dataframe
    let new_df = DataFrame::new(ctx.state(), plan);
    Ok(())
}
```

In fact, using the [`DataFrame`]s methods you can create the same
[`LogicalPlan`]s as when using [`LogicalPlanBuilder`]:

```rust
use datafusion::prelude::*;
use datafusion::error::Result;
use datafusion::logical_expr::LogicalPlanBuilder;

#[tokio::main]
async fn main() -> Result<()>{
    let ctx = SessionContext::new();
    // read example.csv file into a DataFrame
    let df = ctx.read_csv("tests/data/example.csv", CsvReadOptions::new()).await?;
    // Create a new DataFrame sorted by  `id`, `bank_account`
    let new_df = df.select(vec![col("a"), col("b")])?
        .sort_by(vec![col("a")])?;
    // Build the same plan using the LogicalPlanBuilder
    // Similar to `SELECT a, b FROM example.csv ORDER BY a`
    let df = ctx.read_csv("tests/data/example.csv", CsvReadOptions::new()).await?;
    let (_state, plan) = df.into_parts(); // get the DataFrame's LogicalPlan
    let plan = LogicalPlanBuilder::from(plan)
        .project(vec![col("a"), col("b")])?
        .sort_by(vec![col("a")])?
        .build()?;
    // prove they are the same
    assert_eq!(new_df.logical_plan(), &plan);
    Ok(())
}
```

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
[`assert_batches_eq!`]: https://docs.rs/datafusion/latest/datafusion/macro.assert_batches_eq.html
[`sessioncontext`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html
[`sessioncontext` api docs]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html
[`logicalplan`]: https://docs.rs/datafusion/latest/datafusion/logical_expr/enum.LogicalPlan.html
[`logicalplanbuilder`]: https://docs.rs/datafusion/latest/datafusion/logical_expr/struct.LogicalPlanBuilder.html
[`dataframe::write_parquet`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.write_parquet
[`dataframe::write_csv`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.write_csv
[`dataframe::write_json`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.write_json
[`.collect()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.collect
[`.show()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.show
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
[`read_csv`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.read_csv
[`read_parquet`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.read_parquet
[`read_json`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.read_json
[`read_avro`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.read_avro
[`read_arrow`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.read_arrow
