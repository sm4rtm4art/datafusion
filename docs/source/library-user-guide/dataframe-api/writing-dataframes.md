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

DataFusion uses **lazy evaluation**: all transformations ([`.filter()`], [`.select()`], [`.aggregate()`]) build a [`LogicalPlan`] without processing data. Execution only happens when you call an **action method**—and because action methods take ownership, the DataFrame is consumed (use [`df.clone()`][`.clone()`] when you need multiple actions).

> **Style Note:** <br> In this guide, all code elements are highlighted with backticks. DataFrame methods are written as `.method()` (e.g., `.collect()`) to reflect the chaining syntax central to the API. This distinguishes them from standalone functions (e.g., `col()`) and static constructors (e.g., `SessionContext::new()`). Rust types are formatted as `TypeName` (e.g., `RecordBatch`).

```{contents}
:local:
:depth: 2
```

Unlike SQL clients where every query implicitly executes and displays results, DataFusion's Rust API gives you **explicit control** over the final phase. This design enables memory-conscious patterns: collect small results entirely, stream large datasets batch-by-batch, cache expensive computations for reuse, or write directly to storage without intermediate buffering.

The following diagram illustrates where execution fits in the DataFrame architecture:

```text
           +------------------+
           |    DataFrame     |
           |   (lazy plan)    |
           +------------------+
                    |
                    | action method called
                    v
           +------------------+
           |   LogicalPlan     |  ← Rewrites plan (pushdowns, pruning)
           +------------------+
                    |
                    v
           +------------------+
           |  ExecutionPlan   |  ← Physical operators (parallel)
           +------------------+
                    |
          +---------+---------+
          |                   |
          v                   v
    +-----------+       +-------------+
    |  In-Memory |      |  Persistent  |
    |  Results   |      |   Storage    |
    +-----------+       +-------------+
    .collect()           .write_parquet()
    .show()              .write_csv()
    .cache()             .write_table()
                         .write_json()
```

DataFusion uses **vectorized execution**: operators process data in columnar batches (`RecordBatch`), not tuple-at-a-time like the classic Volcano model. This design enables SIMD optimizations and cache-friendly memory access.

In this guide, you will learn how to:

- **Materialize results** with [`.collect()`] and inspect them with [`.show()`] / [`.to_string()`]
- **Stream large results** with [`.execute_stream()`] (bounded memory, backpressure-aware)
- **Reuse computed results** with [`.cache()`]
- **Persist results** with [`.write_parquet()`], [`.write_csv()`], [`.write_json()`], and [`.write_table()`]

> **Ownership Note:** <br>
> All action methods in this guide take **ownership** of the DataFrame (`self`, not `&self`). After any action—whether [`.collect()`], [`.show()`], or [`.write_parquet()`]—the DataFrame is **consumed** and cannot be reused. This is Rust's ownership system at work: a compile-time guarantee, not a runtime restriction. Use [`.clone()`] before an action if you need the DataFrame for subsequent operations.

### For deeper coverage, see:

- [Concepts § Execution Model](concepts.md#execution-model-actions-vs-transformations) — DataFrame lifecycle and async runtime
- [Architecture Guide] — Official documentation on planner → logical → physical flow
- [SIGMOD 2024 Paper]— Academic paper on DataFusion's design
- [How Query Engines Work] — Beginner-friendly book by DataFusion's creator

## Actions vs Transformations: A Quick Recap

Before diving into execution methods, it's essential to understand the distinction between **transformations** and **actions**—the two categories of DataFrame operations.

| Category            | Returns      | Executes? | Examples                                               |
| ------------------- | ------------ | --------- | ------------------------------------------------------ |
| **Transformations** | `DataFrame`  | No (lazy) | [`.filter()`], [`.select()`], [`.join()`], [`.sort()`] |
| **Actions**         | Results/Side | Yes       | [`.collect()`], [`.show()`], [`.write_parquet()`]      |

**This document focuses entirely on actions**—the methods that materialize your query into memory, streams, or persistent storage.

> **Key insight**:<br> The optimizer sees the **complete pipeline** before execution. This means filters, projections, and limits from your entire chain (including SQL operations) are optimized together—pushdowns, pruning, and reordering happen automatically.

**Ownership and cloning**: <br>

> Actions take ownership of the `DataFrame`, consuming it. If you need to perform multiple actions on the same plan, call [`.clone()`] first:

```rust
# use datafusion::prelude::*;
# use datafusion::error::Result;
# #[tokio::main]
# async fn main() -> Result<()> {
# let ctx = SessionContext::new();
# let df = ctx.sql("SELECT 1 as id").await?;
// ✗ Won't compile: df is consumed by first action
// df.show().await?;
// df.collect().await?;

// ✓ Clone for multiple actions
df.clone().show().await?;    // Preview
df.collect().await?;          // Collect for processing
# Ok(())
# }
```

> **SQL contrast**: <br>
> In SQL clients, every query implicitly executes—there's no distinction between building a query and running it. The DataFrame API's explicit action methods give you control over _when_ and _how_ execution happens.

For deeper conceptual coverage, see [Concepts § Execution Model](concepts.md#execution-model-actions-vs-transformations).

## DataFrame Execution

**Execution actions consume the DataFrame and materialize results into memory (RAM).**

This section covers methods that keep results in memory—as `RecordBatch` objects (Arrow's columnar data unit). The differences are briefly summarized in the following table.

| Category    | Destination       | Methods                                                                       |
| ----------- | ----------------- | ----------------------------------------------------------------------------- |
| **Execute** | RAM (in-memory)   | [`.collect()`], [`.execute_stream()`], [`.show()`], [`.cache()`]              |
| **Write**   | Disk (persistent) | [`.write_parquet()`], [`.write_csv()`], [`.write_json()`], [`.write_table()`] |

> **Note:** Both execute and write methods process data internally as `RecordBatch` streams—Arrow's fundamental unit of columnar data. The difference is where results end up: memory (RAM) or storage (disk). With the individual I/O costs.

Unlike traditional SQL clients (i.e. Postgres, MySQL, Oracle...) where every `SELECT` implicitly executes and displays results, **both** DataFusion APIs—SQL and DataFrame—return a lazy `DataFrame` that requires an explicit action to execute. This design gives you control over _when_ and _how_ results are retrieved, enabling memory-conscious patterns: collect small results entirely, stream large datasets batch-by-batch, or cache expensive computations for reuse.

The following table summarizes the **9 execution methods** available on `DataFrame`:

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

- [`.show()`] / [`.collect()`] → Implicit in SQL clients (e.g., `SELECT * FROM ...` displays results)
- [`.cache()`] → Similar to `CREATE TEMP TABLE AS SELECT ...`
- [`.explain()`] → `EXPLAIN` / `EXPLAIN ANALYZE` (plan inspection)
- [`.create_physical_plan()`] → No direct SQL equivalent; programmatic access to the `ExecutionPlan` object

### Basic Execution Results: [`.collect()`], [`.show()`], [`.show_limit()`], [`.to_string()`]

**These methods execute the query and return all results immediately—the simplest way to get data out of a DataFrame.**

- [`.collect()`] returns a `Vec<RecordBatch>` for programmatic processing, for human-readable output
- [`.show()`] prints all rows to stdout,
- [`.show_limit(n)`][`.show_limit()`] prints the first n rows
- [`.to_string()`] captures the formatted output as a `String` (useful for logging or tests).

**SQL equivalent:** <br>
In SQL clients, `SELECT * FROM ...` implicitly executes and displays results. The DataFrame API separates these concerns—you explicitly choose _how_ to retrieve results.

```rust
use datafusion::prelude::*;
use datafusion::error::Result;
use datafusion::assert_batches_eq;

#[tokio::main]
async fn main() -> Result<()> {
    let df = dataframe!(
        "id"   => [1, 2, 3],
        "name" => ["Alice", "Bob", "Carol"]
    )?;

    // .show() executes and prints to stdout — returns () (no data handle)
    df.clone().show().await?;
    // +----+-------+
    // | id | name  |
    // +----+-------+
    // | 1  | Alice |
    // | 2  | Bob   |
    // | 3  | Carol |
    // +----+-------+

    // .collect() returns data — you can process, test, or pass it to other code
    let batches = df.collect().await?;

    assert_batches_eq!([
        "+----+-------+",
        "| id | name  |",
        "+----+-------+",
        "| 1  | Alice |",
        "| 2  | Bob   |",
        "| 3  | Carol |",
        "+----+-------+",
    ], &batches);

    Ok(())
}
```

The key difference: `.show()` executes and outputs to stdout (returns `()`), `.collect()` executes and returns data to RAM (returns `Vec<RecordBatch>`). That explains the different return types and the representation as comment and the use of the [`.assert_batches_eq!`] macro.

> **Memory note:** These methods load all results into memory (RAM). For datasets larger than available RAM, see [Partitioned & Streaming Execution](#partitioned--streaming-execution) below.

### Choosing Between Collect and Stream

**Before calling `.collect()`, consider whether your results will fit in memory—and whether you need parallelism or streaming.**

DataFusion offers four execution methods that combine two **orthogonal concepts**:

| Concept          | Controls    | About                                                   |
| ---------------- | ----------- | ------------------------------------------------------- |
| **Partitioning** | Parallelism | How data is _divided_ for parallel processing           |
| **Streaming**    | Memory      | How results are _consumed_ (incremental vs all-at-once) |

The four combinations represented as the following:

```text
                    │ Merged output      │ Partitioned output
────────────────────┼────────────────────┼─────────────────────────
Buffered (all RAM)  │ .collect()         │ .collect_partitioned()
────────────────────┼────────────────────┼─────────────────────────
Streaming (batch)   │ .execute_stream()  │ .execute_stream_partitioned()
```

To get an overview of the four combinations, the following table can be used:

| Method                            | Returns                          | Partitions | Memory    |
| --------------------------------- | -------------------------------- | ---------- | --------- |
| [`.collect()`]                    | `Vec<RecordBatch>`               | Merged     | All RAM   |
| [`.collect_partitioned()`]        | `Vec<Vec<RecordBatch>>`          | Preserved  | All RAM   |
| [`.execute_stream()`]             | `SendableRecordBatchStream`      | Merged     | Streaming |
| [`.execute_stream_partitioned()`] | `Vec<SendableRecordBatchStream>` | Preserved  | Streaming |

Each method suits different scenarios:

- **[`.collect()`]**: Simple cases, small data, need random access to all results
- **[`.collect_partitioned()`]**: Parallel post-processing, preserve partition structure
- **[`.execute_stream()`]**: Large data, bounded memory, backpressure support
- **[`.execute_stream_partitioned()`]**: Maximum throughput with parallel consumers

**SQL equivalent:** <br>
Traditional SQL clients buffer all results with no partition visibility. Partitioned and streaming execution is a DataFrame API advantage—you control memory and parallelism explicitly.

#### Estimating Memory Requirements

**Not sure if your data fits in memory? When in doubt, use streaming.**

Streaming ([`.execute_stream()`]) works for any data size with bounded memory. Start there for production workloads, and use [`.collect()`] when you know your results are small or need random access.

For more precise decisions, you can estimate memory usage:

- **Before execution:** Use [`df.clone().count().await?`][`.count()`] to get row count, then multiply by average row size from your schema.
- **After execution:** Use [`get_record_batch_memory_size()`] on collected batches to measure actual usage and calibrate future estimates.

**What happens if memory runs out?**

Your source data is always safe—DataFusion reads in a read-only fashion. Only intermediate results being computed are affected. What happens next depends on your configuration:

| Configuration            | Behavior                                                     | Outcome                 |
| ------------------------ | ------------------------------------------------------------ | ----------------------- |
| **Default**              | OS terminates process when system memory exhausted           | Process crash           |
| **With [`MemoryPool`]**  | DataFusion returns `ResourcesExhausted` error before OS acts | Graceful error handling |
| **With [`DiskManager`]** | Some operators (sort, joins) spill to disk automatically     | Continues with disk I/O |

Most developers use [`.collect()`] during development, then switch to streaming when data grows. For critical pipelines, default to streaming from the start.

### Partitioned & Streaming Execution

**This section shows how to use the partitioned and streaming methods introduced above.**

**Partitioned collection** maintains partition boundaries for parallel processing:

```rust
use datafusion::prelude::*;
use datafusion::error::Result;

#[tokio::main]
async fn main() -> Result<()> {
    let df = dataframe!(
        "id"   => [1, 2, 3, 4, 5, 6],
        "name" => ["A", "B", "C", "D", "E", "F"]
    )?;

    // Collect results preserving partition structure
    let partitions: Vec<Vec<_>> = df.collect_partitioned().await?;

    for (i, partition_batches) in partitions.iter().enumerate() {
        println!("Partition {}: {} batches", i, partition_batches.len());
    }

    Ok(())
}
```

**Streaming execution** processes batches incrementally with backpressure:

```rust
use datafusion::prelude::*;
use datafusion::error::Result;
use futures::StreamExt;

#[tokio::main]
async fn main() -> Result<()> {
    let df = dataframe!(
        "id"  => [1, 2, 3],
        "val" => ["a", "b", "c"]
    )?;

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

**Partitioned streaming** combines both—parallel streams for maximum throughput:

```rust
use datafusion::prelude::*;
use datafusion::error::Result;

#[tokio::main]
async fn main() -> Result<()> {
    let df = dataframe!(
        "id" => [1, 2, 3]
    )?;

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

### Caching Results: [`.cache()`]

**[`.cache()`] materializes results and wraps them in a new `DataFrame`—useful when you need to reuse expensive computation results multiple times.**

The cached DataFrame can be filtered, projected, or aggregated further without re-executing the original computation.

**SQL equivalent:** <br>
Similar to `CREATE TEMP TABLE AS SELECT ...`, but in-memory only.

> **Trade-off: .cache() vs write-then-read**
>
> - **`.cache()` shines**: Fast access, no I/O overhead, great for iterative analysis
> - **Write-then-read shines**: Results persist across sessions, handles data larger than RAM, enables sharing between processes

```rust
use datafusion::prelude::*;
use datafusion::error::Result;
use datafusion::functions_aggregate::sum::sum;

#[tokio::main]
async fn main() -> Result<()> {
    let df = dataframe!(
        "grp"   => ["A", "A", "B", "B"],
        "value" => [10, 20, 30, 40]
    )?;

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

### Common Patterns

**Efficient counting**—don't collect just to count rows:

```rust
use datafusion::prelude::*;
use datafusion::error::Result;
use datafusion::functions_aggregate::count::count;

#[tokio::main]
async fn main() -> Result<()> {
    let df = dataframe!(
        "id" => [1, 2, 3, 4, 5]
    )?;

    // Use COUNT aggregation (optimizer can push to source)
    let count_result = df.clone()
        .aggregate(vec![], vec![count(lit(1))])?
        .collect().await?;

    println!("Row count: {:?}", count_result);

    // Check existence efficiently (stops after first row)
    let exists = !df.limit(0, Some(1))?.collect().await?.is_empty();
    println!("Has rows: {}", exists);

    Ok(())
}
```

---

<!-- New references to sort -->

[`MemoryPool`]: https://docs.rs/datafusion/latest/datafusion/execution/memory_pool/trait.MemoryPool.html
[`DiskManager`]: https://docs.rs/datafusion/latest/datafusion/execution/disk_manager/struct.DiskManager.html
[`.count()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.count
[`get_record_batch_memory_size()`]: https://docs.rs/datafusion/latest/datafusion/physical_plan/spill/fn.get_record_batch_memory_size.html

<!-- Datafusion methods -->

[`.clone()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.clone
[`.explain()`]: ../../user-guide/explain-usage.md
[`.create_physical_plan()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.create_physical_plan

<!-- External Datafusion documentation -->

[Architecture Guide]: https://docs.rs/datafusion/latest/datafusion/#architecture
[SIGMOD 2024 Paper]: https://andrew.nerdnetworks.org/pdf/SIGMOD-2024-lamb.pdf
[How Query Engines Work]: https://howqueryengineswork.com/00-introduction.html

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

**Choose the output format based on downstream consumers and schema needs.** Parquet is the default choice for analytics; CSV and JSON are useful for interoperability and debugging.

| Format      | Type     | Schema Preservation | Compression | Best For                        |
| ----------- | -------- | ------------------- | ----------- | ------------------------------- |
| **Parquet** | Columnar | Strong (embedded)   | Excellent   | Analytics, data lakes, archives |
| **CSV**     | Row      | Weak (no types)     | Optional    | Interchange, spreadsheets       |
| **JSON**    | Row      | Moderate            | Optional    | APIs, logs, human-readable      |

### DataFrameWriteOptions

**Write options control output layout (files, partitions, and sorting).** All write methods accept `DataFrameWriteOptions` to control output behavior:

```rust
use datafusion::prelude::*;
use datafusion::dataframe::DataFrameWriteOptions;
use datafusion::error::Result;

#[tokio::main]
async fn main() -> Result<()> {
    // Configure write options (can be reused across writes)
    let _options = DataFrameWriteOptions::new()
        .with_single_file_output(true)      // Force single file (default: one per partition)
        .with_partition_by(vec!["year".to_string(), "month".to_string()])  // Hive-style partitioning
        .with_sort_by(vec![col("timestamp").sort(true, false)]);           // Sort within files

    Ok(())
}
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

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();

    // Create sample data
    let sales_df = ctx.sql("
        SELECT * FROM (VALUES
            (1, 'East', 100),
            (2, 'West', 200),
            (3, 'East', 150)
        ) AS t(id, region, amount)
    ").await?;

    // Use a temp directory for output
    let temp_dir = tempfile::tempdir()?;

    // Basic write (single file)
    let output_path = temp_dir.path().join("sales.parquet");
    let output_path = output_path.to_string_lossy();
    sales_df.clone().write_parquet(
        output_path.as_ref(),
        DataFrameWriteOptions::new().with_single_file_output(true),
        None,  // Use default parquet options
    ).await?;

    // Write to directory (multiple files, better for parallel reads)
    let dir_path = temp_dir.path().join("sales_parquet_dir");
    std::fs::create_dir_all(&dir_path)?;
    let dir_path = dir_path.to_string_lossy();
    sales_df.write_parquet(
        dir_path.as_ref(),
        DataFrameWriteOptions::new(),
        None,
    ).await?;

    Ok(())
}
```

### Writing to CSV

**CSV is a row-oriented interchange format.** Use it for exports to tools like spreadsheets; prefer Parquet for analytics and stable schemas.

```rust
use datafusion::prelude::*;
use datafusion::dataframe::DataFrameWriteOptions;
use datafusion::common::config::CsvOptions;
use datafusion::common::parsers::CompressionTypeVariant;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();

    // Create sample data
    let sales_df = ctx.sql("
        SELECT * FROM (VALUES
            (1, 'East', 100),
            (2, 'West', 200),
            (3, 'East', 150)
        ) AS t(id, region, amount)
    ").await?;

    // Write to temp file with custom delimiter (tab) and compression
    let temp_dir = tempfile::tempdir()?;
    let output_path = temp_dir.path().join("sales.tsv.gz");
    let output_path = output_path.to_string_lossy();

    sales_df.write_csv(
        output_path.as_ref(),
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
    let sales_df = ctx.sql("
        SELECT * FROM (VALUES
            (1, 'East', 100),
            (2, 'West', 200),
            (3, 'East', 150)
        ) AS t(id, region, amount)
    ").await?;

    // Write to temp file as NDJSON
    let temp_dir = tempfile::tempdir()?;
    let output_path = temp_dir.path().join("sales.ndjson");
    let output_path = output_path.to_string_lossy();

    sales_df.write_json(
        output_path.as_ref(),
        DataFrameWriteOptions::new().with_single_file_output(true),
        None,
    ).await?;

    // Output format (one JSON object per line):
    // {"id":1,"region":"East","amount":100}
    // {"id":2,"region":"West","amount":200}
    // {"id":3,"region":"East","amount":150}

    Ok(())
}
```

### Writing to Registered Tables

**[`.write_table()`] inserts DataFrame contents into a registered table**—useful for ETL pipelines and incremental updates:

```rust
use datafusion::prelude::*;
use datafusion::dataframe::DataFrameWriteOptions;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();
    let temp_dir = tempfile::tempdir()?;
    let table_path = temp_dir.path().join("sales");
    std::fs::create_dir_all(&table_path)?;  // Must exist as directory

    // Create target table (external Parquet table pointing to a directory)
    ctx.sql(&format!(
        "CREATE EXTERNAL TABLE sales (id INT, region TEXT, amount INT)
         STORED AS PARQUET LOCATION '{}'",
        table_path.display()
    )).await?.collect().await?;

    // Prepare new data to insert (cast to match table schema)
    let new_sales = ctx.sql("
        SELECT
            CAST(id AS INT) AS id,
            region,
            CAST(amount AS INT) AS amount
        FROM (VALUES
            (1, 'East', 100),
            (2, 'West', 200),
            (3, 'East', 150)
        ) AS t(id, region, amount)
    ").await?;

    // Append to table (default behavior)
    new_sales.write_table(
        "sales",
        DataFrameWriteOptions::new(),
    ).await?;

    // Verify the data was written
    ctx.table("sales").await?.show().await?;

    Ok(())
}
```

Write options include `with_single_file_output()`, `with_partition_by()`, and `with_sort_by()` for controlling output format.

## Execution Quick Reference

<!-- TODO: Update link once best-practices.md is finalized -->

**For production-ready patterns including error handling, logging, memory management, and timeouts, see [Best Practices § Robust Execution](best-practices.md#robust-execution).**

This quick reference summarizes key execution patterns:

- **Project early, filter early**: Select only needed columns and apply filters before expensive operations
- **Prefer streaming for large results**: Use [`.execute_stream()`] over [`.collect()`] for datasets > 1GB
- **Avoid `.show()` in production**: Use [`.execute_stream()`] with explicit formatting and error handling
- **Verify execution plans**: Use `.explain(true, true)` to confirm pushdown and optimization
- **Handle timeouts**: Wrap long-running queries with `tokio::time::timeout` <!-- TODO: verify anchor -->
- **Configure memory limits**: Set `RuntimeEnv::with_memory_pool` to prevent OOM <!-- TODO: verify anchor -->
- **Use `.cache()` wisely**: Only for genuinely reused intermediate results; it consumes memory

---

> **Prerequisites**:
>
> - [Creating DataFrames](creating-dataframes.md) — Know how to create DataFrames
> - [Transformations](transformations.md) — Understand how to transform your data
>
> **Next Steps**:
>
> <!-- TODO: Update links once these documents are finalized -->
>
> - [Best Practices](best-practices.md) — Error handling, logging, memory management, performance tuning
> - [Advanced Topics](dataframes-advance.md) — Custom execution, object store integration (S3, Azure, GCS)

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

[`.assert_batches_eq!`]: https://docs.rs/datafusion/latest/datafusion/macro.assert_batches_eq.html
[`.collect()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.collect
[`.collect_partitioned()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.collect_partitioned
[`.execute_stream()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.execute_stream
[`.execute_stream_partitioned()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.execute_stream_partitioned
[`.show()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.show
[`.show_limit(n)`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.show_limit
[`.show_limit()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.show_limit
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
