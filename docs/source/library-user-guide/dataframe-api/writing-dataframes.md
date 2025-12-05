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

**The final phase of the DataFrame lifecycle: from lazy plan to materialized results.**

This guide covers how to **execute** DataFrames to obtain results and **persist** them to files or tables. In the [DataFrame lifecycle metaphor](./index.md), this is the "death" phase—where the lazy query plan is consumed and transformed into concrete output, whether in-memory `RecordBatch`es, pretty-printed tables, or persistent storage.

DataFusion uses **lazy evaluation**: all transformations ([`.filter()`], [`.select()`], [`.aggregate()`]) merely build a [`LogicalPlan`] without processing data. Execution only happens when you call an **action method**—and once executed, the DataFrame is consumed.

> **Style Note:** In this guide, all code elements are highlighted with backticks. DataFrame methods are written as `.method()` (e.g., `.collect()`) to reflect the chaining syntax central to the API. This distinguishes them from standalone functions (e.g., `col()`) and static constructors (e.g., `SessionContext::new()`). Rust types are formatted as `TypeName` (e.g., `RecordBatch`).

```{contents}
:local:
:depth: 2
```

## Actions vs Transformations: A Quick Recap

Before diving into execution methods, it's essential to understand the distinction between **transformations** and **actions**—the two categories of DataFrame operations.

| Category            | Returns      | Executes? | Examples                                               |
| ------------------- | ------------ | --------- | ------------------------------------------------------ |
| **Transformations** | `DataFrame`  | No (lazy) | [`.filter()`], [`.select()`], [`.join()`], [`.sort()`] |
| **Actions**         | Results/Side | Yes       | [`.collect()`], [`.show()`], [`.write_parquet()`]      |

> **Key insight**: The optimizer sees the **complete pipeline** before execution. This means filters, projections, and limits from your entire chain (including SQL operations) are optimized together—pushdowns, pruning, and reordering happen automatically.

For deeper conceptual coverage, see [Concepts § Execution Model](concepts.md#execution-model-actions-vs-transformations).

## DataFrame Execution (In-Memory Results)

**Execution actions consume the DataFrame and materialize results into memory or streams.** Unlike SQL clients that implicitly execute and display results, DataFusion's Rust API gives you explicit control over _how_ results are retrieved.

### Action Methods Reference

| Method                            | Returns                          | Memory Model   | Best For                              |
| --------------------------------- | -------------------------------- | -------------- | ------------------------------------- |
| [`.collect()`]                    | `Vec<RecordBatch>`               | All in memory  | Small/medium results, tests           |
| [`.collect_partitioned()`]        | `Vec<Vec<RecordBatch>>`          | All in memory  | Parallel post-processing              |
| [`.execute_stream()`]             | `SendableRecordBatchStream`      | Streaming      | Large results, backpressure           |
| [`.execute_stream_partitioned()`] | `Vec<SendableRecordBatchStream>` | Streaming      | Parallel streaming pipelines          |
| [`.show()`]                       | `()` (prints to stdout)          | Buffered       | Debugging, exploration                |
| [`.show_limit(n)`]                | `()` (prints first n rows)       | Bounded buffer | Quick inspection                      |
| [`.to_string()`]                  | `String`                         | Buffered       | Logging, custom display               |
| [`.cache()`]                      | `DataFrame` (new, materialized)  | All in memory  | Reusing expensive computations        |
| [`.create_physical_plan()`]       | `Arc<dyn ExecutionPlan>`         | Plan only      | Custom execution, inspection, metrics |

**SQL Equivalents:**

- `.show()` / `.collect()` → Implicit in SQL clients (e.g., `SELECT * FROM ...` displays results)
- `.cache()` → Similar to `CREATE TEMP TABLE AS SELECT ...`
- `.create_physical_plan()` → Similar to `EXPLAIN` (plan inspection without execution)

### Quick Results: .collect() and .show()

**[`.collect()`] materializes all results into memory as a `Vec<RecordBatch>`**—the most common action for programmatic access to query results.

> **Trade-off: .collect() vs .execute_stream()**
>
> - **`.collect()` shines**: Simple code, easy debugging, random access to all results
> - **`.execute_stream()` shines**: Bounded memory, backpressure, handles results larger than RAM

```rust
use datafusion::prelude::*;
use datafusion::error::Result;

#[tokio::main]
async fn main() -> Result<()> {
    let ctx = SessionContext::new();

    // Create a DataFrame with inline data (self-contained example)
    let df = ctx.sql("SELECT * FROM (VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Carol')) AS t(id, name)").await?;

    // Collect all results into memory (executes the query)
    let batches: Vec<_> = df.collect().await?;

    // Process the results
    for batch in &batches {
        println!("Batch has {} rows", batch.num_rows());
    }

    Ok(())
}
```

> **Memory Warning**: [`.collect()`] loads **all results** into memory. For datasets larger than available RAM, use [`.execute_stream()`] to process batches incrementally.

**[`.show()`] pretty-prints results to stdout**—ideal for debugging and exploration:

```rust
use datafusion::prelude::*;
use datafusion::error::Result;

#[tokio::main]
async fn main() -> Result<()> {
    let ctx = SessionContext::new();
    let df = ctx.sql("SELECT * FROM (VALUES (1, 'Alice'), (2, 'Bob')) AS t(id, name)").await?;

    // Pretty-print all results
    df.clone().show().await?;

    // Pretty-print only first 10 rows (more memory-efficient for large results)
    df.clone().show_limit(10).await?;

    // Get string representation for logging or custom display
    let output = df.to_string().await?;
    println!("Query results:\n{}", output);

    Ok(())
}
```

### Streaming Execution

**For large or unbounded results, streaming execution provides backpressure-aware processing** without loading everything into memory.

[`.execute_stream()`] returns a single merged stream:

```rust
use datafusion::prelude::*;
use datafusion::error::Result;
use futures::StreamExt;

#[tokio::main]
async fn main() -> Result<()> {
    let ctx = SessionContext::new();

    // Create a DataFrame (in production, this might be a large Parquet file)
    let df = ctx.sql("SELECT * FROM (VALUES (1, 'a'), (2, 'b'), (3, 'c')) AS t(id, val)").await?;

    // Stream results with backpressure
    let mut stream = df.execute_stream().await?;

    while let Some(result) = stream.next().await {
        match result {
            Ok(batch) => {
                println!("Processing batch with {} rows", batch.num_rows());
                // Process incrementally—memory stays bounded
            }
            Err(e) => {
                eprintln!("Stream error: {}", e);
                break;
            }
        }
    }

    Ok(())
}
```

**[`.execute_stream_partitioned()`] preserves partition boundaries**—useful for parallel processing:

```rust
use datafusion::prelude::*;
use datafusion::error::Result;

#[tokio::main]
async fn main() -> Result<()> {
    let ctx = SessionContext::new();
    let df = ctx.sql("SELECT * FROM (VALUES (1), (2), (3)) AS t(id)").await?;

    let streams = df.execute_stream_partitioned().await?;
    println!("Processing {} partitions in parallel", streams.len());

    // Each stream can be processed by a separate task
    for (i, _stream) in streams.into_iter().enumerate() {
        println!("Would spawn task for partition {}", i);
        // tokio::spawn(async move { /* process partition */ });
    }

    Ok(())
}
```

> **Production Pattern**: For services, wrap stream processing with `tokio::time::timeout` to handle hung queries, and use `tokio::select!` for cancellation support.

### Caching and Reuse

**[`.cache()`] materializes results and wraps them in a new `DataFrame`**—useful when you need to reuse expensive computation results multiple times.

```rust
use datafusion::prelude::*;
use datafusion::error::Result;
use datafusion::functions_aggregate::sum::sum;

#[tokio::main]
async fn main() -> Result<()> {
    let ctx = SessionContext::new();

    // Create source data
    let df = ctx.sql("
        SELECT * FROM (VALUES
            ('A', 10), ('A', 20), ('B', 30), ('B', 40)
        ) AS t(grp, value)
    ").await?;

    // Expensive computation: aggregate by group
    let aggregated_df = df.aggregate(vec![col("grp")], vec![sum(col("value"))])?;

    // Cache the results (executes the query once, stores in memory)
    let cached = aggregated_df.cache().await?;

    // Reuse without re-executing the aggregation
    let group_a = cached.clone().filter(col("grp").eq(lit("A")))?.collect().await?;
    let group_b = cached.clone().filter(col("grp").eq(lit("B")))?.collect().await?;

    println!("Group A batches: {}", group_a.len());
    println!("Group B batches: {}", group_b.len());

    Ok(())
}
```

> **Trade-off: .cache() vs write-then-read**
>
> - **`.cache()` shines**: Fast access, no I/O overhead, great for iterative analysis
> - **Write-then-read shines**: Results persist across sessions, handles data larger than RAM, enables sharing between processes

### Plan Inspection Without Execution

**[`.create_physical_plan()`] builds the execution plan without running it**—valuable for debugging, metrics collection, or custom execution strategies:

```rust
use datafusion::prelude::*;
use datafusion::error::Result;

#[tokio::main]
async fn main() -> Result<()> {
    let ctx = SessionContext::new();
    let df = ctx.sql("SELECT * FROM (VALUES (1, 'a'), (2, 'b')) AS t(id, name)").await?;

    // Build the physical plan without executing
    let physical_plan = df.clone().create_physical_plan().await?;

    // Inspect plan properties
    println!("Output partitioning: {:?}", physical_plan.output_partitioning());

    // For human-readable plan output, use .explain() instead:
    df.explain(true, true)?.show().await?;

    Ok(())
}
```

### Advanced: Partitioned Collection

For parallel post-processing while maintaining partition structure:

```rust
use std::sync::Arc;
use datafusion::prelude::*;
use datafusion::error::Result;
use datafusion::physical_plan::collect_partitioned;

#[tokio::main]
async fn main() -> Result<()> {
    let ctx = SessionContext::new();
    let df = ctx.sql("SELECT * FROM (VALUES (1), (2), (3)) AS t(id)").await?;

    // Get state and plan separately for advanced control
    let (state, plan) = df.into_parts();
    let physical = state.create_physical_plan(&plan).await?;
    let task_ctx = Arc::new(state.task_ctx());

    // Collect each partition independently
    let partitions = collect_partitioned(physical, task_ctx).await?;

    for (i, partition_batches) in partitions.iter().enumerate() {
        println!("Partition {}: {} batches", i, partition_batches.len());
    }

    Ok(())
}
```

### Efficient Counting and Existence Checks

Don't use [`.collect()`] just to count rows or check for existence—use optimized patterns:

```rust
use datafusion::prelude::*;
use datafusion::error::Result;
use datafusion::arrow::array::Int64Array;
use datafusion::functions_aggregate::count::count;

#[tokio::main]
async fn main() -> Result<()> {
    let ctx = SessionContext::new();
    let df = ctx.sql("SELECT * FROM (VALUES (1), (2), (3), (4), (5)) AS t(id)").await?;

    // Efficient row count (optimizer pushes COUNT to source when possible)
    let count_df = df.clone().aggregate(vec![], vec![count(lit(1))])?;
    let batches = count_df.collect().await?;
    let row_count = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    println!("Total rows: {}", row_count);

    // Check existence efficiently (stops after first row)
    let exists = !df.limit(0, Some(1))?.collect().await?.is_empty();
    println!("Has rows: {}", exists);

    Ok(())
}
```

## Writing DataFrames (Persistent Storage)

**Write actions execute the DataFrame and stream results to persistent storage.** This is how you export query results to files or insert data into registered tables.

### Write Methods Reference

| Method               | Output Format | SQL Equivalent                       | Key Features                     |
| -------------------- | ------------- | ------------------------------------ | -------------------------------- |
| [`.write_parquet()`] | Parquet       | `COPY ... TO '...' (FORMAT PARQUET)` | Columnar, schema, partitioning   |
| [`.write_csv()`]     | CSV           | `COPY ... TO '...' (FORMAT CSV)`     | Delimiters, headers, compression |
| [`.write_json()`]    | NDJSON        | `COPY ... TO '...' (FORMAT JSON)`    | Line-delimited JSON              |
| [`.write_table()`]   | Table         | `INSERT INTO ...`                    | Append/overwrite to tables       |

> **Trade-off: DataFrame vs SQL for Writes**
>
> - **SQL shines**: `COPY TO` syntax is more concise for simple exports: `COPY (SELECT ...) TO 'output.parquet'`
> - **DataFrame shines**: Programmatic control over write options, dynamic paths, and integration with Rust logic

### Choosing a File Format

| Format      | Type     | Schema Preservation | Compression | Best For                        |
| ----------- | -------- | ------------------- | ----------- | ------------------------------- |
| **Parquet** | Columnar | Strong (embedded)   | Excellent   | Analytics, data lakes, archives |
| **CSV**     | Row      | Weak (no types)     | Optional    | Interchange, spreadsheets       |
| **JSON**    | Row      | Moderate            | Optional    | APIs, logs, human-readable      |

### DataFrameWriteOptions

All write methods accept `DataFrameWriteOptions` to control output behavior:

```rust
use datafusion::prelude::*;
use datafusion::dataframe::DataFrameWriteOptions;

// Configure write options (can be reused across writes)
let options = DataFrameWriteOptions::new()
    .with_single_file_output(true)      // Force single file (default: one per partition)
    .with_partition_by(vec!["year".to_string(), "month".to_string()])  // Hive-style partitioning
    .with_sort_by(vec![col("timestamp").sort(true, false)]);           // Sort within files
```

| Option                       | Effect                                             | Default |
| ---------------------------- | -------------------------------------------------- | ------- |
| `.with_single_file_output()` | Write one file instead of per-partition            | `false` |
| `.with_partition_by()`       | Create directory structure by column values        | `[]`    |
| `.with_sort_by()`            | Sort rows within each output file                  | `[]`    |
| `.with_insert_operation()`   | Control INSERT behavior (Append/Overwrite/Replace) | Append  |

### Writing to Parquet

**Parquet is the recommended format for analytical workloads**—it preserves schema, compresses well, and enables predicate pushdown on re-read.

```rust
use datafusion::prelude::*;
use datafusion::dataframe::DataFrameWriteOptions;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();

    // Create sample data
    let df = ctx.sql("
        SELECT * FROM (VALUES
            (1, 'East', 100),
            (2, 'West', 200),
            (3, 'East', 150)
        ) AS t(id, region, amount)
    ").await?;

    // Use a temp directory for output
    let temp_dir = tempfile::tempdir()?;

    // Basic write (single file)
    let output_path = temp_dir.path().join("data.parquet");
    df.clone().write_parquet(
        output_path.to_str().unwrap(),
        DataFrameWriteOptions::new().with_single_file_output(true),
        None,
    ).await?;

    // With compression (ZSTD provides excellent compression ratios)
    let compressed_path = temp_dir.path().join("compressed.parquet");
    df.write_parquet(
        compressed_path.to_str().unwrap(),
        DataFrameWriteOptions::new().with_single_file_output(true),
        Some(
            WriterProperties::builder()
                .set_compression(Compression::ZSTD(Default::default()))
                .build()
        ),
    ).await?;

    Ok(())
}
```

### Writing to CSV

```rust
use datafusion::prelude::*;
use datafusion::dataframe::DataFrameWriteOptions;
use datafusion::common::config::CsvOptions;
use datafusion::common::parsers::CompressionTypeVariant;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();

    // Create sample data
    let df = ctx.sql("
        SELECT * FROM (VALUES
            (1, 'Alice', 95.5),
            (2, 'Bob', 87.3),
            (3, 'Carol', 92.1)
        ) AS t(id, name, score)
    ").await?;

    // Write to temp file with custom delimiter (tab) and compression
    let temp_dir = tempfile::tempdir()?;
    let output_path = temp_dir.path().join("output.tsv.gz");

    df.write_csv(
        output_path.to_str().unwrap(),
        DataFrameWriteOptions::new().with_single_file_output(true),
        Some(
            CsvOptions::default()
                .with_delimiter(b'\t')
                .with_has_header(true)
                .with_compression(CompressionTypeVariant::GZIP)
        ),
    ).await?;

    Ok(())
}
```

### Writing to JSON (NDJSON)

**DataFusion writes newline-delimited JSON (NDJSON)**—one JSON object per line, suitable for streaming ingestion:

```rust
use datafusion::prelude::*;
use datafusion::dataframe::DataFrameWriteOptions;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();

    // Create sample data
    let df = ctx.sql("
        SELECT * FROM (VALUES
            (1, 'Alice', '2024-01-15'),
            (2, 'Bob', '2024-01-16'),
            (3, 'Carol', '2024-01-17')
        ) AS t(id, name, created_at)
    ").await?;

    // Write to temp file as NDJSON
    let temp_dir = tempfile::tempdir()?;
    let output_path = temp_dir.path().join("users.ndjson");

    df.write_json(
        output_path.to_str().unwrap(),
        DataFrameWriteOptions::new().with_single_file_output(true),
        None,
    ).await?;

    // Output format (one JSON object per line):
    // {"id":1,"name":"Alice","created_at":"2024-01-15"}
    // {"id":2,"name":"Bob","created_at":"2024-01-16"}
    // {"id":3,"name":"Carol","created_at":"2024-01-17"}

    Ok(())
}
```

### Writing to Registered Tables

**[`.write_table()`] inserts DataFrame contents into a registered table**—useful for ETL pipelines and incremental updates:

```rust
use std::sync::Arc;
use datafusion::prelude::*;
use datafusion::dataframe::{DataFrameWriteOptions, InsertOp};

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();
    let temp_dir = tempfile::tempdir()?;
    let table_path = temp_dir.path().join("events");

    // Create target table (external Parquet table)
    ctx.sql(&format!(
        "CREATE EXTERNAL TABLE events (id INT, event TEXT)
         STORED AS PARQUET LOCATION '{}'",
        table_path.display()
    )).await?.collect().await?;

    // Prepare new data to insert
    let new_events = ctx.sql("
        SELECT * FROM (VALUES
            (1, 'login'),
            (2, 'purchase'),
            (3, 'logout')
        ) AS t(id, event)
    ").await?;

    // Append to table
    new_events.write_table(
        "events",
        DataFrameWriteOptions::new()
            .with_insert_operation(InsertOp::Append),
    ).await?;

    // Verify the data was written
    ctx.table("events").await?.show().await?;

    Ok(())
}
```

| InsertOp    | Behavior                                    |
| ----------- | ------------------------------------------- |
| `Append`    | Add rows to existing data (default)         |
| `Overwrite` | Replace all existing data                   |
| `Replace`   | Upsert semantics (if supported by provider) |

## Execution Best Practices

**Production-ready DataFrame execution requires attention to memory, timeouts, and error handling.** The patterns below help you avoid common pitfalls when moving from development to production workloads.

### Quick Reference Checklist

- **Project early, filter early**: Select only needed columns and apply filters before expensive operations
- **Prefer streaming for large results**: Use [`.execute_stream()`] over [`.collect()`] for datasets > 1GB
- **Avoid `.show()` in production**: Use [`.execute_stream()`] with explicit formatting and error handling
- **Verify execution plans**: Use `.explain(true, true)` to confirm pushdown and optimization
- **Handle timeouts**: Wrap long-running queries with `tokio::time::timeout`
- **Configure memory limits**: Set `RuntimeEnv::with_memory_pool` to prevent OOM on large aggregations
- **Use `.cache()` wisely**: Only for genuinely reused intermediate results; it consumes memory

### Memory Management

```rust
use std::sync::Arc;
use datafusion::prelude::*;
use datafusion::error::Result;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::execution::memory_pool::FairSpillPool;

#[tokio::main]
async fn main() -> Result<()> {
    // Configure memory limits to prevent OOM (1GB limit)
    let runtime = RuntimeEnvBuilder::default()
        .with_memory_pool(Arc::new(FairSpillPool::new(1024 * 1024 * 1024)))
        .build_arc()?;

    // Create session with memory-limited runtime
    let config = SessionConfig::new();
    let ctx = SessionContext::new_with_config_rt(config, runtime);

    // Queries will now respect the memory limit
    let df = ctx.sql("SELECT 1 as id").await?;
    df.show().await?;

    Ok(())
}
```

### Cancellation and Timeouts

```rust
use datafusion::prelude::*;
use datafusion::error::Result;
use tokio::time::{timeout, Duration};

#[tokio::main]
async fn main() -> Result<()> {
    let ctx = SessionContext::new();
    let df = ctx.sql("SELECT * FROM (VALUES (1), (2), (3)) AS t(id)").await?;

    // Wrap execution with timeout (30 seconds)
    let result = timeout(
        Duration::from_secs(30),
        df.collect()
    ).await;

    match result {
        Ok(Ok(batches)) => println!("Success: {} batches", batches.len()),
        Ok(Err(e)) => eprintln!("Query error: {}", e),
        Err(_) => eprintln!("Query timed out after 30s"),
    }

    Ok(())
}
```

---

> **Prerequisites**:
>
> - [Creating DataFrames](creating-dataframes.md) — Know how to create DataFrames
> - [Transformations](transformations.md) — Understand how to transform your data
>
> **Next Steps**:
>
> - [Best Practices](best-practices.md) — Performance tuning and optimization
> - [Advanced Topics](dataframes-advance.md) — Custom execution, S3/remote storage

TODO: Add example for writing to S3: https://github.com/apache/datafusion/blob/main/datafusion-examples/examples/external_dependency/dataframe-to-s3.rs

<!-- Core type references -->

[`DataFrame`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html
[`LogicalPlan`]: https://docs.rs/datafusion/latest/datafusion/logical_expr/enum.LogicalPlan.html
[`RecordBatch`]: https://docs.rs/arrow-array/latest/arrow_array/struct.RecordBatch.html
[`SessionContext`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html

<!-- Transformation method references -->

[`.filter()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.filter
[`.select()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.select
[`.aggregate()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.aggregate
[`.join()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.join
[`.sort()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.sort

<!-- Execution action references -->

[`.collect()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.collect
[`.collect_partitioned()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.collect_partitioned
[`.execute_stream()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.execute_stream
[`.execute_stream_partitioned()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.execute_stream_partitioned
[`.show()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.show
[`.show_limit(n)`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.show_limit
[`.to_string()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.to_string
[`.cache()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.cache
[`.create_physical_plan()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.create_physical_plan

<!-- Write method references -->

[`.write_parquet()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.write_parquet
[`.write_csv()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.write_csv
[`.write_json()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.write_json
[`.write_table()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.write_table

<!-- Example references -->

[`custom_file_format.rs`]: https://github.com/apache/datafusion/blob/main/datafusion-examples/examples/custom_file_format.rs
