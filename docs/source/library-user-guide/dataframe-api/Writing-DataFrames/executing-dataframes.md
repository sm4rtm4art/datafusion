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

<!--
TODO(Docs): executing-dataframes.md - Materializing Results to RAM

1. MOVE CONTENT HERE:
   - "Actions vs Transformations: A Quick Recap" (Ownership, `.clone()`).
   - "Basic Execution Results": `.show()`, `.show_limit()`, `.collect()`, `.to_string()`.
   - "Estimating Memory Requirements" (using `.count()` for pre-flight sizing).
   - "Caching Results": `.cache()` (explain it creates a MemTable and acts as an optimization barrier).

2. CORE FOCUS:
   - Explain that these methods buffer data into application memory (RAM).
   - Show how to configure a bounded `MemoryPool` to prevent OOM (Out Of Memory) crashes.
   - Keep the distinction between `.collect()` (merges everything) and `.collect_partitioned()` (keeps Vec<Vec<RecordBatch>> for parallel post-processing).

3. LINK OUT:
   - Add a note: "If your dataset is larger than RAM, see [Streaming Execution](streaming-execution.md) or write directly to disk [Writing DataFrames](writing-dataframes.md)."
-->

## DataFrame Execution


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

```{contents} Table of Contents
:local:
:depth: 2
```

## Placeholder Introduction

**Execution actions consume the DataFrame and execute the plan, producing results as `RecordBatch`es (either buffered in memory or streamed batch-by-batch).**

This section covers methods that keep results in memory—as `RecordBatch` objects (Arrow's columnar data unit). The differences are briefly summarized in the following table.

| Category    | Destination       | Methods                                                                       |
| ----------- | ----------------- | ----------------------------------------------------------------------------- |
| **Execute** | RAM (in-memory)   | [`.collect()`], [`.execute_stream()`], [`.show()`], [`.cache()`]              |
| **Write**   | Disk (persistent) | [`.write_parquet()`], [`.write_csv()`], [`.write_json()`], [`.write_table()`] |

> **Note:** Both execute and write methods process data internally as `RecordBatch` streams—Arrow's fundamental unit of columnar data. The difference is where results end up: memory (RAM) or storage (disk). With the individual I/O costs.



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

```{contents} Table of Contents DataFrame Execution
:local:
:depth: 2
:caption: Writing Concepts
```

Unlike traditional SQL clients (i.e. Postgres, MySQL, Oracle...) where every `SELECT` implicitly executes and displays results, **both** DataFusion APIs—SQL and DataFrame—return a lazy `DataFrame` that requires an explicit action to execute. This design gives you control over _when_ and _how_ results are retrieved, enabling memory-conscious patterns: collect small results entirely, stream large datasets batch-by-batch, or cache expensive computations for reuse.

The following table summarizes common **execution-related action methods** available on `DataFrame` (not exhaustive—for example, [`.count()`] is also an action):

### Action Methods Reference

| Method                            | Returns                          | Memory Model   | Best For                              |
| --------------------------------- | -------------------------------- | -------------- | ------------------------------------- |
| [`.collect()`]                    | `Vec<RecordBatch>`               | All in memory  | Small/medium results, tests           |
| [`.collect_partitioned()`]        | `Vec<Vec<RecordBatch>>`          | All in memory  | Parallel post-processing              |
| [`.execute_stream()`]             | `SendableRecordBatchStream`      | Streaming      | Large results, backpressure           |
| [`.execute_stream_partitioned()`] | `Vec<SendableRecordBatchStream>` | Streaming      | Parallel streaming pipelines          |
| [`.show()`]                       | `()` (prints to stdout)          | All in memory  | Debugging small results               |
| [`.show_limit(n)`]                | `()` (prints first n rows)       | Bounded buffer | Quick preview / inspection            |
| [`.to_string()`]                  | `String`                         | All in memory  | Logging/tests (small results)         |
| [`.cache()`]                      | `DataFrame` (new, materialized)  | All in memory  | Reusing expensive computations        |
| [`.create_physical_plan()`]       | `Arc<dyn ExecutionPlan>`         | Plan only      | Custom execution, inspection, metrics |

**SQL Equivalents:**

- [`.show()`] / [`.collect()`] → Implicit in SQL clients (e.g., `SELECT * FROM ...` displays results)
- [`.cache()`] → Similar to `CREATE TEMP TABLE AS SELECT ...`
- [`.explain()`] → `EXPLAIN` / `EXPLAIN ANALYZE` (plan inspection)
- [`.create_physical_plan()`] → No direct SQL equivalent; programmatic access to the `ExecutionPlan` object

### Basic Execution Results: [`.collect()`], [`.show()`], [`.show_limit()`], [`.to_string()`]

**These methods execute the query and materialize results for immediate inspection—the simplest way to get data out of a DataFrame.**

- [`.collect()`] returns a `Vec<RecordBatch>` for programmatic processing (buffers all results in memory)
- [`.show()`] prints all rows to stdout (internally collects all results)
- [`.show_limit(n)`][`.show_limit()`] prints the first n rows (applies a limit before collecting)
- [`.to_string()`] captures the formatted output as a `String` (internally collects all results; useful for logging or tests).

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

The key difference: [`.show()`] executes and outputs to stdout (returns `()`), [`.collect()`] executes and returns data to RAM (returns `Vec<RecordBatch>`). That explains the different return types and the representation as comment and the use of the [`.assert_batches_eq!`] macro.

> **Memory note:** <br>
> These methods generally load results into memory (RAM) (except [`.show_limit()`], which limits rows before collecting). For datasets larger than available RAM, see [Partitioned & Streaming Execution](#partitioned--streaming-execution) below.

### Choosing Between Collect and Stream

**These `DataFrame` action methods let you choose a memory model (buffered vs streaming) and an output shape (merged vs partitioned) before you run the query.**

The choice comes down to two independent decisions:

- **Memory (buffered vs streaming):** <br>
  Can the full result set fit in RAM, or should results be consumed batch-by-batch via a stream?
- **Partitions (merged vs preserved):** <br>
  Do you want a single merged result, or per-partition output for parallel post-processing?

DataFusion offers four execution methods that combine two **orthogonal concepts**:

| Concept          | Controls    | About                                                   |
| ---------------- | ----------- | ------------------------------------------------------- |
| **Partitioning** | Parallelism | How data is _divided_ for parallel processing           |
| **Streaming**    | Memory      | How results are _consumed_ (incremental vs all-at-once) |

The four combinations look like this:

|                        | Merged output         | Partitioned output                |
| ---------------------- | --------------------- | --------------------------------- |
| **Buffered (all RAM)** | [`.collect()`]        | [`.collect_partitioned()`]        |
| **Streaming (batch)**  | [`.execute_stream()`] | [`.execute_stream_partitioned()`] |

> **Tip:** <br>
> When in doubt, start with [`.execute_stream()`] and switch to [`.collect()`] only when you know the result set is small and you need random access in memory.

To get the return types and memory model at a glance, use the following table:

| Method                            | Returns                          | Partitions | Memory    |
| --------------------------------- | -------------------------------- | ---------- | --------- |
| [`.collect()`]                    | `Vec<RecordBatch>`               | Merged     | All RAM   |
| [`.collect_partitioned()`]        | `Vec<Vec<RecordBatch>>`          | Preserved  | All RAM   |
| [`.execute_stream()`]             | `SendableRecordBatchStream`      | Merged     | Streaming |
| [`.execute_stream_partitioned()`] | `Vec<SendableRecordBatchStream>` | Preserved  | Streaming |

Each method suits different scenarios:

- **[`.collect()`]**: Simple cases, small data, need random access to all results
- **[`.collect_partitioned()`]**: Parallel post-processing, preserve partition structure
- **[`.execute_stream()`]**: Large results, avoids buffering the full result set, backpressure support
- **[`.execute_stream_partitioned()`]**: Maximum throughput with parallel consumers

> **Common misconception: execution parallelism vs result shape** <br>
> [`.collect()`] and [`.collect_partitioned()`] both execute the physical plan (often in parallel across partitions). The difference is the output shape: [`.collect()`] merges partitions into one buffer, while [`.collect_partitioned()`] keeps a separate buffer per partition. Execution parallelism is primarily controlled by partitioning (for example, `datafusion.execution.target_partitions`) and explicit [`.repartition()`] steps.

These are action methods on [`DataFrame`], so they work the same whether the `DataFrame` was created from SQL (via `SessionContext::sql(...)`) or built via the DataFrame API (via builder methods like [`.filter()`] and [`.select()`]).

**SQL equivalent:** <br>
Traditional SQL clients typically expose a single merged result stream with no partition visibility. Partitioned and streaming execution is a DataFrame API advantage—you control memory and parallelism explicitly.

#### Estimating Memory Requirements

**Not sure if your data fits in memory? When in doubt, use streaming.**

Streaming ([`.execute_stream()`]) avoids buffering the full result set in your application and supports backpressure. However, queries may still require substantial memory for intermediate operators (i.e. sort/join/aggregate), and may spill or error depending on your [`MemoryPool`] and [`DiskManager`] configuration. Start there for production workloads, and use [`.collect()`] when you know your results are small or need random access.

For more precise decisions, you can estimate memory usage:

- **Before collecting full results:** Use [`df.clone().count().await?`][`.count()`] to get a row count (this is an action and executes a separate plan), then multiply by average row size from your schema.
- **After collecting:** Use [`get_record_batch_memory_size()`] on `RecordBatch`es to measure actual usage and calibrate future estimates.

> **Pre-flight sizing with [`.count()`]:** <br>
> If you have no information about output size, you can run a count first and choose between buffered and streaming execution.
>
> **Performance note:** <br>
> [`.count()`] executes a plan. If you call [`.count()`] and then call [`.collect()`] on the same `DataFrame`, you are effectively running the query twice. For large datasets, default to [`.execute_stream()`] or estimate size from file metadata (file sizes, Parquet row group statistics) instead.
>
> ```rust
> use datafusion::prelude::*;
> use datafusion::error::Result;
>
> #[tokio::main]
> async fn main() -> Result<()> {
>     let df = dataframe!(
>         "order_id" => [10, 11, 12],
>         "amount"   => [100, 250, 175]
>     )?;
>
>     // Execute eagerly: a "pre-flight" count to decide on a retrieval strategy
>     let row_count = df.clone().count().await?;
>
>     // Threshold is illustrative — tune based on schema, memory budget, and workload.
>     if row_count < 100_000 {
>         let _batches = df.collect().await?;
>     } else {
>         let _stream = df.execute_stream().await?;
>     }
>
>     Ok(())
> }
> ```

**Configuring a bounded [`MemoryPool`]:** <br>
`MemoryPool` and [`DiskManager`] are configured on the [`RuntimeEnv`]. Create a shared `RuntimeEnv` (via [`RuntimeEnvBuilder`]) and pass it to `SessionContext::new_with_config_rt(...)` to enforce limits across queries and sessions.

```rust
use datafusion::prelude::*;
use datafusion::error::Result;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;

#[tokio::main]
async fn main() -> Result<()> {
    // Leave headroom for allocations that are not tracked by the MemoryPool.
    let max_memory = 100 * 1024 * 1024; // 100MiB total budget
    let memory_fraction = 0.9; // track up to ~90MiB (best-effort)

    let runtime_env = RuntimeEnvBuilder::new()
        .with_memory_limit(max_memory, memory_fraction)
        .build_arc()?;

    // Reuse the same RuntimeEnv across SessionContext instances to enforce global limits.
    let ctx = SessionContext::new_with_config_rt(SessionConfig::new(), runtime_env);

    // Execute eagerly (small example query)
    ctx.sql("SELECT 1 AS id").await?.collect().await?;

    Ok(())
}
```

> **Warning:** <br>
> DataFusion does not yet respect memory limits in all cases: the `MemoryPool` limits large operator allocations (sort/join/aggregate) but not every allocation is tracked. Leave headroom and treat the limit as best-effort.

**What happens if memory runs out?**

Your source data is always safe—DataFusion reads in a read-only fashion. Only intermediate results being computed are affected. What happens next depends on your configuration:

| Configuration                                     | Behavior                                                                     | Outcome                             |
| ------------------------------------------------- | ---------------------------------------------------------------------------- | ----------------------------------- |
| **Default** (unbounded [`MemoryPool`])            | May allocate until the OS terminates the process                             | Possible crash / OS kill            |
| **With bounded [`MemoryPool`]**                   | Can return `ResourcesExhausted` before the OS acts (best-effort)             | Graceful error handling             |
| **With bounded [`MemoryPool`] + [`DiskManager`]** | Spillable operators (sort/join) can spill to disk when memory is constrained | Continues with disk I/O (when able) |

> **Spill note:** <br>
> A [`DiskManager`] enables spillable operators to write temporary files, but spilling is typically triggered by memory pressure (a bounded [`MemoryPool`]).

Most developers use [`.collect()`] during development, then switch to streaming when data grows. For critical pipelines, default to streaming from the start.

### Partitioned & Streaming Execution

**Partitioned and streaming execution controls how results leave a `DataFrame`—either as partition-preserving in-memory `RecordBatch`es or as `RecordBatch` streams with backpressure.**

The DataFrame API exposes three action methods for this:

- [`.collect_partitioned()`] returns `Vec<Vec<RecordBatch>>` (partition-preserving, buffered)
- [`.execute_stream()`] returns `SendableRecordBatchStream` (merged, streaming)
- [`.execute_stream_partitioned()`] returns `Vec<SendableRecordBatchStream>` (partition-preserving, streaming)

**SQL equivalent:** <br>
Most SQL clients return a single merged cursor/stream of rows and do not expose partition boundaries.

> **Warning:** <br>
> Streaming reduces application-side buffering, but operators like joins, sorts, and aggregations can still require substantial memory for intermediate state. Configure [`MemoryPool`] and [`DiskManager`] for production workloads that run near memory limits.

#### Preserve partition boundaries in memory: [`.collect_partitioned()`]

[`.collect_partitioned()`] executes the plan and returns one `Vec<RecordBatch>` per output partition. Each inner `Vec<RecordBatch>` contains the batches produced for a single partition, which is useful for parallel post-processing.

Unlike [`.collect()`] (which merges partitions into a single `Vec<RecordBatch>`), [`.collect_partitioned()`] keeps partitions separate. Use [`.repartition()`] with [`Partitioning`] to create partitions explicitly.

> **Warning:** <br>
> [`.collect_partitioned()`] still buffers the full result set in memory (just partitioned). For large outputs, prefer [`.execute_stream_partitioned()`] to stream per-partition results.

```rust
use datafusion::prelude::*;
use datafusion::error::Result;

#[tokio::main]
async fn main() -> Result<()> {
    let df = dataframe!(
        "customer_id" => [1, 2, 3, 4, 5, 6],
        "event_type"  => ["login", "login", "purchase", "logout", "purchase", "logout"]
    )?;

    // Build a lazy plan: create 2 output partitions to preserve during collection
    let df = df.repartition(Partitioning::RoundRobinBatch(2))?;

    // Execute eagerly: collect results preserving partition structure
    let partitions = df.collect_partitioned().await?;

    // Partition count depends on data size and optimizer decisions
    for (partition, batches) in partitions.iter().enumerate() {
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        println!(
            "Partition {}: {} batches, {} rows",
            partition,
            batches.len(),
            rows
        );
    }

    Ok(())
}
```

#### Stream merged results: [`.execute_stream()`]

[`.execute_stream()`] executes the plan and yields a merged stream of `RecordBatch`es. Use this when the result set does not fit in RAM.

The `SendableRecordBatchStream` yields `RecordBatch`es incrementally, so your application can process results batch-by-batch without materializing all output in memory. Because the output is _merged_, you do not see partition boundaries—use [`.execute_stream_partitioned()`] when you want one stream per partition for parallel consumers.

> **Warning:** <br>
> Streaming reduces output buffering in your application, but the query may still need memory for intermediate state (for example, joins, sorts, and aggregations).

```rust
use datafusion::prelude::*;
use datafusion::error::Result;
use futures::StreamExt;

#[tokio::main]
async fn main() -> Result<()> {
    let df = dataframe!(
        "order_id" => [10, 11, 12],
        "amount"   => [100, 250, 175]
    )?;

    // Execute eagerly: stream `RecordBatch`es with backpressure
    let mut stream = df.execute_stream().await?;

    let mut total_rows = 0usize;
    while let Some(batch) = stream.next().await.transpose()? {
        total_rows += batch.num_rows();
        println!("Processing batch with {} rows", batch.num_rows());
        // Process incrementally—avoid buffering the full result set in your application.
    }

    assert_eq!(total_rows, 3);
    Ok(())
}
```

#### Stream partitions in parallel: [`.execute_stream_partitioned()`]

[`.execute_stream_partitioned()`] returns one stream per output partition, enabling parallel consumers.

The number of returned streams depends on the plan's output partitioning. Use [`.repartition()`] with [`Partitioning`] to choose a partition count explicitly (as shown below).

```rust
use datafusion::prelude::*;
use datafusion::error::Result;
use futures::StreamExt;

#[tokio::main]
async fn main() -> Result<()> {
    let df = dataframe!(
        "device_id" => [1, 2, 3, 4],
        "reading"   => [0.1_f64, 0.2, 0.3, 0.4]
    )?;

    // Build a lazy plan: create 2 output partitions to stream independently
    let df = df.repartition(Partitioning::RoundRobinBatch(2))?;

    let streams = df.execute_stream_partitioned().await?;

    // Stream count depends on data size and optimizer decisions.
    // Each stream can be consumed in a separate task (shown here sequentially)
    for (partition, mut stream) in streams.into_iter().enumerate() {
        let mut partition_rows = 0usize;
        while let Some(batch) = stream.next().await.transpose()? {
            partition_rows += batch.num_rows();
        }
        println!("Partition {}: {} rows", partition, partition_rows);
    }

    Ok(())
}
```

> **Warning:** <br>
> Partitioned streaming does not provide a global row order across partitions. If you need ordered output, add an explicit [`.sort()`] and consume a merged stream via [`.execute_stream()`] (or merge the partitioned streams in your application).

> **Tip:** <br>
> For services, wrap execution in `tokio::time::timeout` and coordinate cancellation with `tokio::select!` so long-running queries do not hang request handlers.

### Caching Results: [`.cache()`]

**[`.cache()`] materializes results and wraps them in a new `DataFrame`—useful when you need to reuse expensive computation results multiple times.**

[`.cache()`] is an action: it executes the plan once and stores the output as an in-memory `MemTable`. Subsequent transformations read from the cached table instead of re-executing the original plan.

**SQL equivalent:** <br>
Similar to `CREATE TEMP TABLE AS SELECT ...`, but in-memory only.

> **Trade-off: .cache() vs write-then-read**
>
> - **`.cache()` shines**: Fast access, no I/O overhead, great for iterative analysis
> - **Write-then-read shines**: Results persist across sessions, handles data larger than RAM, enables sharing between processes

```rust
use datafusion::prelude::*;
use datafusion::error::Result;
use datafusion::assert_batches_eq;
use datafusion::functions_aggregate::sum::sum;

#[tokio::main]
async fn main() -> Result<()> {
    let df = dataframe!(
        "region" => ["East", "East", "West", "West"],
        "amount" => [10, 20, 30, 40]
    )?;

    // Build a lazy plan: compute total amount per region
    let totals_df = df.aggregate(
        vec![col("region")],
        vec![sum(col("amount")).alias("total_amount")],
    )?;

    // Execute eagerly once and cache the results in memory
    let cached = totals_df.cache().await?;

    // Reuse without re-executing the aggregation (each collect reads from the cache)
    let east = cached
        .clone()
        .filter(col("region").eq(lit("East")))?
        .collect()
        .await?;
    let west = cached
        .clone()
        .filter(col("region").eq(lit("West")))?
        .collect()
        .await?;

    assert_batches_eq!(
        [
            "+--------+--------------+",
            "| region | total_amount |",
            "+--------+--------------+",
            "| East   | 30           |",
            "+--------+--------------+",
        ],
        &east
    );

    assert_batches_eq!(
        [
            "+--------+--------------+",
            "| region | total_amount |",
            "+--------+--------------+",
            "| West   | 70           |",
            "+--------+--------------+",
        ],
        &west
    );

    Ok(())
}
```

> **Warning:** <br>
> Caching everything is usually an anti-pattern: it forces materialization, consumes RAM, and can reduce pushdown opportunities to the original data source.
>
> **Warning: the optimization barrier** <br>
> [`.cache()`] creates an in-memory `MemTable`, which becomes the new data source. Filters and projections applied _after_ caching run against the cached data and cannot be pushed down to the original file readers (for example, Parquet row group pruning).
>
> - **Good:** <br>
>   Apply filters/projections first, then cache: <br>
>   `let cached = df.filter(col("region").eq(lit("East")))?.cache().await?;`
> - **Bad:** <br>
>   Cache first, then filter (reads the full cached result): <br>
>   `let filtered = df.cache().await?.filter(col("region").eq(lit("East")))?;`
>
> **Cache when:** <br>
>
> - The same expensive sub-plan feeds multiple branches or multiple actions (for example, two different filters).
> - The cached result is small enough to fit in RAM and reused enough times to amortize the cache cost.
>
> **Avoid caching when:** <br>
>
> - The `DataFrame` is consumed once (or can be consumed via streaming).
> - The data source supports pushdown that would be lost by caching (projection/filter/limit).
> - The cached result is large or high-cardinality (risking memory pressure or spill).

> **Tip:** <br>
> Use [`.cache()`] when the same expensive sub-plan feeds multiple branches or multiple actions (for example, two different filters). Apply projections and filters before caching to reduce the cached footprint.

### Common Patterns

**These patterns execute small “control queries” (counts and existence checks) without collecting full result sets.**

Use [`.count()`] for row counts and `.limit(0, Some(1))` + [`.count()`] for existence checks (the limit can be pushed down, stopping early at the source).

**SQL equivalent:** <br>
`SELECT COUNT(*) ...` and `SELECT EXISTS(SELECT 1 ...)`.

```rust
use datafusion::prelude::*;
use datafusion::error::Result;

#[tokio::main]
async fn main() -> Result<()> {
    let df = dataframe!("id" => [1, 2, 3, 4, 5])?;

    // Execute eagerly: count rows (buffers only the aggregation result)
    let row_count = df.clone().count().await?;
    assert_eq!(row_count, 5);

    // Execute eagerly: stop after the first row
    let has_rows = df.clone().limit(0, Some(1))?.count().await? > 0;
    assert!(has_rows);

    Ok(())
}
```

### Putting It Together: Execution Checklist

**This table helps you choose an execution action based on result size, parallelism needs, and reuse—without memorizing all return types.**

| Scenario                                | Recommended action                                                                                              | Notes                                                                                                                                                                  |
| --------------------------------------- | --------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Small results (tests / interactive)** | [`.collect()`] <br> [`.collect_partitioned()`]                                                                  | Use buffered collection when results fit in RAM. Use partitioned collection when you want per-partition buffers for parallel post-processing.                          |
| **Large results (avoid buffering)**     | [`.execute_stream()`] <br> [`.execute_stream_partitioned()`]                                                    | Stream `RecordBatch`es to avoid buffering full output in your application. Use partitioned streaming when you want one stream per partition for parallel consumers.    |
| **Parallel post-processing**            | [`.repartition()`] + [`Partitioning`] <br> then [`.collect_partitioned()`] or [`.execute_stream_partitioned()`] | Control the number of partitions explicitly, then preserve partitions in the output.                                                                                   |
| **Reuse expensive results**             | [`.cache()`]                                                                                                    | Cache only when the same expensive sub-plan is reused across multiple branches or actions. Apply projection and filters before caching to reduce the cached footprint. |
| **Production safety**                   | [`RuntimeEnvBuilder`] + bounded [`MemoryPool`] (+ [`DiskManager`]) <br> prefer [`.execute_stream()`]            | Enforce resource limits across queries and default to streaming when output size is unknown.                                                                           |

### References

- **Runtime limits & spill:** <br>
  [`RuntimeEnvBuilder`], [`MemoryPool`], [`DiskManager`]
- **Memory measurement:** <br>
  [`get_record_batch_memory_size()`]
- **Plan inspection:** <br>
  [`.explain()`], [`.create_physical_plan()`]
- **Execution & partitioning (DataFusion):** <br>
  [Configuration Settings], [Streaming Execution (crate docs)], [Ordering Analysis (DataFusion blog)]
- **Caching (DataFusion):** <br>
  [`.cache()`] (in-memory `MemTable`, pushdown barrier)
- **Background (optional):** <br>
  [How Query Engines Work], [Morsel-Driven Parallelism], [Apache Arrow Columnar Format]
- **Cross-ecosystem intuition (optional):** <br>
  [Databricks KB: Spark cache recomputation pitfalls], [Understanding Spark Execution Planning], [Understanding Lazy Evaluation in Polars]

```

```
