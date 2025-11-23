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

# Writing and Executing DataFrames

This guide covers how to execute DataFrames to get results and persist them to files (Parquet, CSV, JSON) and registered tables. DataFusion uses **lazy evaluation**: transformations build a query plan without processing data until you call an action method.

> **Style Note:** In this guide, all code elements are highlighted with backticks. DataFrame methods are written as `.method()` (e.g., `.select()`) to reflect the chaining syntax central to the API. This distinguishes them from standalone functions (e.g., `col()`) and static constructors (e.g., `SessionContext::new()`). Rust types are formatted as `TypeName` (e.g., `SchemaRef`).

```{contents}
:local:
:depth: 2
```

## DataFrame Execution

DataFusion [`DataFrame`]s use **lazy evaluation**: transformations build a query plan without processing data. Execution only happens when you call an action method like [`.collect()`] or [`.show()`].

> **For conceptual background**, see [Concepts § Execution Model](concepts.md#execution-model-actions-vs-transformations), which covers the theory behind transformations vs actions.

### Action Methods

| Method                      | Purpose                                             | When to use                           |
| --------------------------- | --------------------------------------------------- | ------------------------------------- |
| [`.collect()`]              | Materialize into `Vec<RecordBatch>` (all in memory) | Small/medium results, tests           |
| [`.show()`]                 | Pretty-print to stdout                              | Debugging, exploration, examples      |
| [`.execute_stream()`]       | Backpressure-aware `SendableRecordBatchStream`      | Large results, services, pipelines    |
| [`.create_physical_plan()`] | Build ExecutionPlan without running it              | Custom execution, metrics, inspection |

> **Key insight**: Nothing runs until an action. The optimizer sees the full pipeline (SQL + DataFrame transformations) and applies pushdown/projection/limit rules before execution.

### Choosing an Action

- Use [`.show()`] for examples and quick checks
- Use [`.collect()`] when results are bounded and fit in memory
- Prefer [`.execute_stream()`] for large results, production services, or when you need backpressure control

> **Memory Warning**: [`.collect()`] loads all results into memory at once. For large datasets (>1GB), use [`.execute_stream()`] to process batches incrementally. Configure memory limits via `RuntimeEnv::memory_pool`.

### Example: Execute a DataFrame

```rust
use datafusion::prelude::*;
use datafusion::error::Result;
use futures::StreamExt; // for .next()

#[tokio::main]
async fn main() -> Result<()> {
    let ctx = SessionContext::new();

    // Build lazily (no I/O yet)
    let df = ctx.read_parquet("sales.parquet", ParquetReadOptions::default()).await?
        .filter(col("amount").gt(lit(1000)))?
        .select(vec![col("region"), col("amount")])?;

    // 1) Pretty print (dev/debugging)
    df.show().await?;

    // 2) Collect all results (bounded datasets)
    let batches = df.clone().collect().await?;
    println!("Collected {} batches", batches.len());

    // 3) Stream results with error handling (recommended for services)
    let mut stream = df.execute_stream().await?;
    while let Some(batch) = stream.next().await {
        match batch {
            Ok(batch) => {
                println!("Processing {} rows", batch.num_rows());
                // ... process batch incrementally
            }
            Err(e) => {
                eprintln!("Stream error: {}", e);
                break; // or implement retry logic
            }
        }
    }

    Ok(())
}
```

### Example: Execute SQL (Same Mechanics)

```rust
// SQL queries return DataFrames; same execution methods apply
let df = ctx.sql("SELECT region, SUM(amount) AS total FROM sales GROUP BY region").await?;

df.show().await?;
// or: let batches = df.collect().await?;
// or: let stream = df.execute_stream().await?;
```

### Inspect the Plan Before Execution

```rust
// Show both logical and physical plans (verify pushdown, sort/limit position, etc.)
df.explain(true, true).await?;
```

> **Tip**: Use `explain(true, true)` while tuning performance; it shows both the optimized logical and physical plans, confirming that filters/projections were pushed down.

### Advanced: Partitioned Execution

For parallel processing of results across partitions:

```rust
use datafusion::physical_plan::collect_partitioned;

let (state, plan) = df.into_parts();
let physical = state.create_physical_plan(&plan).await?;
let partitions = collect_partitioned(physical, state.task_ctx()).await?;

// Each partition is a Vec<RecordBatch> that was processed independently
for (i, partition) in partitions.iter().enumerate() {
    println!("Partition {}: {} batches", i, partition.len());
}
```

### Efficient Counting and Existence Checks

Don't use [`.collect()`] just to count rows or check existence:

```rust
use datafusion::arrow::array::Int64Array;

// Efficient row count (pushes COUNT down to source when possible)
let count_df = df.clone().aggregate(vec![], vec![count(lit(1))])?;
let batches = count_df.collect().await?;
let count = batches[0]
    .column(0)
    .as_any()
    .downcast_ref::<Int64Array>()
    .unwrap()
    .value(0);
println!("Row count: {}", count);

// Check if any rows exist (early termination after first row)
let exists = df.clone().limit(0, Some(1))?.collect().await?
    .iter()
    .any(|batch| batch.num_rows() > 0);
```

### Execution Best Practices

- **Project early, filter early**: Minimize I/O and memory by selecting only needed columns and applying filters before expensive operations
- **Prefer streaming**: Use [`.execute_stream()`] for large or unbounded outputs to avoid memory exhaustion
- **Avoid `.show()` in production**: Use [`.execute_stream()`] and format/log explicitly with proper error handling
- **Verify plans**: Use `.explain(true, true)` to confirm pushdown/projection/limit placements
- **Handle cancellation**: Wrap awaits with `tokio::time::timeout` or propagate cancellation tokens for long-running queries
- **Memory limits**: Configure `RuntimeEnv::with_memory_pool` to prevent OOM on large aggregations

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
> SELECT * FROM '../datafusion/core/example.parquet';
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

---

> **Prerequisites**:
>
> - [Creating DataFrames](creating-dataframes.md) - Know how to create and execute DataFrames
> - [Transformations](transformations.md) - Understand how to transform your data
>
> **Next Steps**:
>
> - [Best Practices](best-practices.md) - Optimize write performance
> - [Index](index.md) - Return to overview

TODO: https://github.com/apache/datafusion/blob/main/datafusion-examples/examples/external_dependency/dataframe-to-s3.rs

<!-- DataFrame execution method references -->

[`.collect()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.collect
[`.show()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.show
[`.execute_stream()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.execute_stream
[`.create_physical_plan()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.create_physical_plan

<!-- DataFrame write method references -->

[`write_parquet()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.write_parquet
[`write_csv()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.write_csv
[`write_json()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.write_json
[`write_table()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.write_table
[`DataFrame::write_parquet`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.write_parquet
[`custom_file_format.rs`]: https://github.com/apache/datafusion/blob/main/datafusion-examples/examples/custom_file_format.rs
