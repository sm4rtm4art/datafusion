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
           |   LogicalPlan    |  ← Rewrites plan (pushdowns, pruning)
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
    +------------+       +--------------+
    |  In-Memory |       |  Persistent  |
    |  Results   |       |    Storage   |
    +------------+       +--------------+
     .collect()           .write_parquet()
     .show()              .write_csv()
     .cache()             .write_table()
                          .write_json()
```

DataFusion uses **vectorized execution**: operators process data in columnar batches (`RecordBatch`), not tuple-at-a-time like the classic Volcano model. This design enables SIMD optimizations and cache-friendly memory access.

> **Note:** SIMD requires compiling with CPU-specific flags (`RUSTFLAGS='-C target-cpu=native'`). See [Crate Configuration](../../user-guide/crate-configuration.md#generate-code-with-cpu-specific-instructions) for details.

In this guide, you will learn how to:

- **Materialize results** with [`.collect()`] and inspect them with [`.show_limit()`] / [`.show()`] / [`.to_string()`] (best for small results)
- **Stream large results** with [`.execute_stream()`] (streaming output, backpressure-aware; avoids buffering the full result set in your application)
- **Reuse computed results** with [`.cache()`]
- **Persist results** with [`.write_parquet()`], [`.write_csv()`], [`.write_json()`], and [`.write_table()`]

> **Ownership Note:** <br>
> All action methods in this guide take **ownership** of the DataFrame (`self`, not `&self`). After any action—whether [`.collect()`], [`.show()`], or [`.write_parquet()`]—the `DataFrame` value has been **moved** and the same variable cannot be reused. This does **not** delete your data or invalidate the query plan; it’s simply Rust’s move semantics. Use [`.clone()`] before an action if you need to run multiple actions (or retry after an error).

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

**Execution actions consume the DataFrame and execute the plan, producing results as `RecordBatch`es (either buffered in memory or streamed batch-by-batch).**

This section covers methods that keep results in memory—as `RecordBatch` objects (Arrow's columnar data unit). The differences are briefly summarized in the following table.

| Category    | Destination       | Methods                                                                       |
| ----------- | ----------------- | ----------------------------------------------------------------------------- |
| **Execute** | RAM (in-memory)   | [`.collect()`], [`.execute_stream()`], [`.show()`], [`.cache()`]              |
| **Write**   | Disk (persistent) | [`.write_parquet()`], [`.write_csv()`], [`.write_json()`], [`.write_table()`] |

> **Note:** Both execute and write methods process data internally as `RecordBatch` streams—Arrow's fundamental unit of columnar data. The difference is where results end up: memory (RAM) or storage (disk). With the individual I/O costs.

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

> **Common misconception: execution parallelism vs result shape** <br> > [`.collect()`] and [`.collect_partitioned()`] both execute the physical plan (often in parallel across partitions). The difference is the output shape: [`.collect()`] merges partitions into one buffer, while [`.collect_partitioned()`] keeps a separate buffer per partition. Execution parallelism is primarily controlled by partitioning (for example, `datafusion.execution.target_partitions`) and explicit [`.repartition()`] steps.

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
> **Performance note:** <br> > [`.count()`] executes a plan. If you call [`.count()`] and then call [`.collect()`] on the same `DataFrame`, you are effectively running the query twice. For large datasets, default to [`.execute_stream()`] or estimate size from file metadata (file sizes, Parquet row group statistics) instead.
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

> **Warning:** <br> > [`.collect_partitioned()`] still buffers the full result set in memory (just partitioned). For large outputs, prefer [`.execute_stream_partitioned()`] to stream per-partition results.

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
> **Warning: the optimization barrier** <br> > [`.cache()`] creates an in-memory `MemTable`, which becomes the new data source. Filters and projections applied _after_ caching run against the cached data and cannot be pushed down to the original file readers (for example, Parquet row group pruning).
>
> - **Good:** <br>
>   Apply filters/projections first, then cache: <br> > `let cached = df.filter(col("region").eq(lit("East")))?.cache().await?;`
> - **Bad:** <br>
>   Cache first, then filter (reads the full cached result): <br> > `let filtered = df.cache().await?.filter(col("region").eq(lit("East")))?;`
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

---

## Writing DataFrames (Persistent Storage)

**Writing moves DataFrame results from transient memory (RAM) to durable storage (disk), converting in-flight `RecordBatch`es into persistent files or table records.**

Up to this point in the DataFrame lifecycle, data exists only in memory: <br>
Execution methods like [`.collect()`] materialize query results as Arrow `RecordBatch`es in RAM—fast to access, but lost when the process ends. Write methods bridge the gap between ephemeral compute and durable storage, serializing those in-memory batches to files or tables that survive restarts and can be shared across systems.

```text
┌─────────────────────────────────────────────────────────────────────────────┐
│                        DataFrame Lifecycle                                  │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌─────────────┐      ┌─────────────┐      ┌─────────────────────────────┐  │
│  │   Define    │ ──▶  │   Execute   │ ──▶  │          Write              │  │
│  │  (lazy)     │      │   (RAM)     │      │         (DISK)              │  │
│  └─────────────┘      └─────────────┘      └─────────────────────────────┘  │
│                                                                             │
│  .filter()            .collect()            .write_parquet()                │
│  .select()            .show()               .write_csv()                    │
│  .aggregate()         .cache()              .write_json()                   │
│                                             .write_table()                  │
│                                                                             │
│  LogicalPlan          RecordBatch           Files / TableProvider           │
│  (no I/O)             (in-memory)           (persistent storage)            │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

> **Performance reality: RAM is fast, disk is slow** <br>
> In-memory execution operates at nanosecond to microsecond latencies—CPU and memory bandwidth are the limits. Writing to disk introduces millisecond latencies (local SSD) to hundreds of milliseconds (network object stores like S3). A query that executes in seconds may take minutes to write if output is large or the target is remote. Plan write operations accordingly: batch outputs, choose efficient formats (Parquet over CSV), and consider whether you truly need to persist results or if in-memory caching suffices.

Writing matters when results must outlive the current session—whether exporting analytics to a data lake, building ETL pipelines, or populating downstream tables for other consumers.

### General Considerations for Writing

**Writing to disk introduces challenges that don't exist in memory.**

Understanding these concerns upfront helps you design robust pipelines and avoid surprises in production.

In-memory operations benefit from the controlled environment of your process: <br>
Memory is fast, failures are immediate, and there's no format negotiation. Once you cross the boundary to persistent storage, you inherit the complexities of the outside world—network failures, storage quotas, file system semantics, and format compatibility with downstream consumers.

The table below summarizes the key concerns. Think of these as a mental checklist before any write operation:

| Concern              | What it means                                                                                                                                                      |
| -------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| **Durability**       | Write operations are I/O-bound. Local SSDs add milliseconds; object stores (S3, GCS, Azure) add network round-trips. Budget time accordingly.                      |
| **Atomicity**        | File writes are not transactional. A crash mid-write leaves partial files. Defensive pattern: write to a temp path, then rename on success.                        |
| **Storage capacity** | Disk quotas are larger than RAM but still finite. Partitioned writes can create many small files—the "small files problem" that hurts downstream read performance. |
| **Format lock-in**   | Your format choice affects every downstream consumer. Parquet preserves schema and compresses well; CSV loses types. Choose based on who reads the data next.      |
| **Permissions**      | File system permissions and object store IAM policies must allow writes. These errors often surface late, during execution rather than planning.                   |

> **Tip:** <br>
> For production pipelines, validate write targets early (check permissions, available space) and implement retry logic for transient failures. See [Best Practices](best-practices.md) for patterns.

### Write Methods Overview

**DataFusion provides four write methods that persist DataFrame results to files or tables.** <br>
All write methods are **actions**—they execute eagerly, consume the DataFrame, and return a one-row `RecordBatch` with a `count` column indicating how many rows were written.

Each DataFrame write method has a SQL equivalent that produces the same [`LogicalPlan`]. Choose the API that fits your workflow—DataFrame for programmatic control, SQL for ad-hoc exports—without sacrificing functionality or performance.

| DataFrame API        | Destination                               | SQL Equivalent                                  | Under the hood                                    |
| -------------------- | ----------------------------------------- | ----------------------------------------------- | ------------------------------------------------- |
| [`.write_parquet()`] | Parquet files (filesystem/object store)   | `COPY (SELECT ...) TO 'path' STORED AS PARQUET` | `LogicalPlan::Copy` → file sink                   |
| [`.write_csv()`]     | CSV files (filesystem/object store)       | `COPY (SELECT ...) TO 'path' STORED AS CSV`     | `LogicalPlan::Copy` → file sink                   |
| [`.write_json()`]    | NDJSON files (filesystem/object store)    | `COPY (SELECT ...) TO 'path' STORED AS JSON`    | `LogicalPlan::Copy` → file sink                   |
| _(none)_             | Arrow IPC files (filesystem/object store) | `COPY (SELECT ...) TO 'path' STORED AS ARROW`   | SQL-only; no DataFrame method                     |
| [`.write_table()`]   | Registered table ([`TableProvider`])      | `INSERT INTO target_table SELECT ...`           | `LogicalPlan::Dml` → `TableProvider::insert_into` |

For SQL syntax details, see:

- [DML: `COPY` and `INSERT`](../../user-guide/sql/dml.md).

**File writes:** ([`.write_parquet()`], [`.write_csv()`], [`.write_json()`]) stream `RecordBatch`es directly to storage using format-specific serializers. <br>
**Table writes;** ([`.write_table()`]) delegate to the target provider's `insert_into` implementation, which may write files, send data over a network, or perform custom logic.

> **Note: Format gaps** <br>
>
> - **Arrow (IPC)** is SQL-only—use `ctx.sql("COPY ... STORED AS ARROW")` for Arrow output.\
> - **Avro** is read-only in DataFusion; writing Avro is not supported.

> **Trade-off: DataFrame vs SQL for writes** <br>
>
> - **SQL shines:** Ad-hoc exports and one-off pipelines—concise and familiar.\
> - **DataFrame shines:** Paths and options are dynamic Rust values, enabling programmatic branching and integration with application logic.

> **Tip:** <br>
> Write methods consume the DataFrame. <br>
> Use [`df.clone()`][`.clone()`] if you need to write to multiple destinations or run another action like [`.collect()`].

### Choosing the destination: filesystem vs object stores

**Write destinations are URLs resolved through an object store registry.**

The same write code can target:

- local disk (`/tmp/...`)
- in-memory storage (`memory:///...` for tests)
- or cloud stores (`s3://...`, `gs://...`, `az://...`)

Once configured, you pass a path or URL to the write method; DataFusion selects the appropriate `ObjectStore` implementation based on the URL scheme.

Filesystem paths (no scheme) use the local filesystem object store.

**SQL equivalent:** <br>

```sql
COPY (SELECT ...)
TO 'memory:///sales.parquet' STORED
AS PARQUET
```

The example below writes to an in-memory object store and reads the file back.

> **Note:** <br>
> We use `ctx.sql()` here instead of `dataframe!` because the DataFrame must share the same `SessionContext` where the object store is registered.

```rust
use std::sync::Arc;

use datafusion::assert_batches_eq;
use datafusion::dataframe::DataFrameWriteOptions;
use datafusion::error::Result;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::object_store::{memory::InMemory, ObjectStore};
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> Result<()> {
    let ctx = SessionContext::new();

    // Register an in-memory object store for the `memory://` scheme.
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    ctx.runtime_env()
        .register_object_store(ObjectStoreUrl::parse("memory://")?.as_ref(), store);

    let sales_df = ctx
        .sql(
            "SELECT * FROM (VALUES
                (1, 'East', 100),
                (2, 'West', 200)
            ) AS sales(id, region, amount)",
        )
        .await?;

    // Writing executes eagerly and returns a single `count` batch.
    let write_result = sales_df
        .write_parquet("memory:///sales.parquet",
                         DataFrameWriteOptions::new(), None)
        .await?;

    assert_batches_eq!(
        &[
            "+-------+",
            "| count |",
            "+-------+",
            "| 2     |",
            "+-------+",
        ],
        &write_result
    );

    // Read it back from the same object store.
    let row_count = ctx
        .read_parquet("memory:///sales.parquet",
                     ParquetReadOptions::default())
        .await?
        .count()
        .await?;
    assert_eq!(row_count, 2);

    Ok(())
}
```

> **Warning:** <br>
> The `memory://` object store is process-local and not durable. <br>
> Use `memory://` for tests and examples, **not for persistent storage.**

> **Tip:** <br>
> For S3 / Azure / GCS configuration, see [Advanced Topics](dataframes-advance.md).
> <br>
> DataFusion SQL-API users can use `COPY ... TO 's3://...'` with the same object store configuration.

### Choosing a File Format

**Choose the output format based on downstream consumers and schema needs.**

Parquet is the default choice for analytics. Other formats like CSV and JSON are useful for interoperability (for example, log pipelines or NoSQL ingestion) and debugging. Arrow IPC is ideal for zero-copy interchange with other Arrow-based tools.

| Format          | Type     | Schema Preservation | Compression | DataFrame API        | Best For                         |
| --------------- | -------- | ------------------- | ----------- | -------------------- | -------------------------------- |
| **Parquet**     | Columnar | Strong (embedded)   | Excellent   | [`.write_parquet()`] | Analytics, data lakes, archives  |
| **CSV**         | Row      | Weak (no types)     | Optional    | [`.write_csv()`]     | Interchange, spreadsheets        |
| **JSON**        | Row      | Moderate            | Optional    | [`.write_json()`]    | APIs, logs, human-readable       |
| **Arrow (IPC)** | Columnar | Strong (embedded)   | Optional    | SQL only             | Zero-copy Arrow tool interchange |

#### Format Comparison

Here a small overview of the different behaviour of the common files should be given:

|                        | Parquet | CSV | JSON |
| ---------------------- | :-----: | :-: | :--: |
| Type fidelity          |   ✓✓    |  ✗  |  ○   |
| Columnar pruning       |   ✓✓    |  ✗  |  ✗   |
| Compression            |   ✓✓    |  ○  |  ○   |
| Schema in metadata     |   ✓✓    |  ✗  |  ✗   |
| Human readable         |    ✗    | ✓✓  |  ✓✓  |
| Universal tool support |    ○    | ✓✓  |  ✓✓  |

<small>✓✓ excellent · ✓ supported · ○ partial · ✗ not supported</small>

> **Note:** <br>
> Arrow (IPC) writing is available via SQL `COPY ... STORED AS ARROW` but has no DataFrame method.\
> If you start from a `DataFrame`, register the DataFrame as a SQL view using [`SessionContext::register_table`] and [`df.into_view()`][`.into_view()`], then execute a SQL `COPY` statement via `ctx.sql("COPY ...")`.\
> This is different from [`.registry()`], which is for function (UDF) lookup when building expressions.

For format-specific writer options (compression codecs, delimiters, etc.), see:

- [Format Options](../../user-guide/sql/format_options.md) — SQL-level options for `COPY` statements
- Format-specific sections below for DataFrame API options

### Configuring Writes

**Configuring writes means choosing a destination, common write behavior, and format-specific writer settings.** This matters because most production writes need at least one policy decision (partitioning, sorting, overwrite semantics, compression) beyond “write to this path”. DataFusion expresses these choices directly in the DataFrame API via [`DataFrameWriteOptions`] plus optional writer options.

To apply the configuration, call a write action (for example, [`.write_parquet()`]) with a `path`, `options`, and optional format-specific `writer_options`.

**SQL equivalent:** <br>
Use a `COPY ... STORED AS ...` statement (and `INSERT INTO ...` for table writes). See [DML: `COPY` and `INSERT`](../../user-guide/sql/dml.md).

| Parameter        | Type                      | Required | Purpose                                        |
| ---------------- | ------------------------- | -------- | ---------------------------------------------- |
| `path`           | `&str`                    | Yes      | Destination path or URL (file or object store) |
| `options`        | `DataFrameWriteOptions`   | Yes      | Partitioning, sorting, insert operation        |
| `writer_options` | `Option<Format-specific>` | No       | Compression, delimiters, row group size, etc.  |

**Call shape (method signatures):** <br>
These examples show the parameter structure; they omit setup like creating `df` and choosing real paths.

```text
// Parquet — None uses internal defaults, or pass explicit options
df.write_parquet("out.parquet", DataFrameWriteOptions::new(), None).await?
df.write_parquet("out.parquet", DataFrameWriteOptions::new(), Some(TableParquetOptions::default())).await?

// CSV — uses CsvOptions::default()
df.write_csv("out.csv", DataFrameWriteOptions::new(), Some(CsvOptions::default())).await?

// JSON — uses JsonOptions::default()
df.write_json("out.json", DataFrameWriteOptions::new(), Some(JsonOptions::default())).await?

// Table — no writer_options, format determined by TableProvider
df.write_table("my_table", DataFrameWriteOptions::new()).await?
```

> **Note:**<br>
>
> - `TableParquetOptions` provides both `::new()` and `::default()` (equivalent).
> - `CsvOptions` and `JsonOptions` only provide `::default()`.

#### DataFrameWriteOptions

[`DataFrameWriteOptions`] controls partitioning, sorting, and table insert behavior. Not every option applies to every write target:

- **File writes** ([`.write_parquet()`], [`.write_csv()`], [`.write_json()`]): use `.with_partition_by(...)` and `.with_sort_by(...)`
- **Table writes** ([`.write_table()`]): use `.with_insert_operation(...)` (and optionally `.with_sort_by(...)`)

An example of how to configure write options is shown below.

```rust
use datafusion::prelude::*;
use datafusion::dataframe::DataFrameWriteOptions;
use datafusion::error::Result;

#[tokio::main]
async fn main() -> Result<()> {
    // Configure write options (can be reused across writes)
    let _options = DataFrameWriteOptions::new()
        .with_partition_by(vec!["year".to_string(), "month".to_string()]) // Hive-style partitioning
        .with_sort_by(vec![col("timestamp").sort(true, false)]); // Sort rows before writing

    Ok(())
}
```

| Option                         | Used by                                                                       | Effect                                                                        | Default |
| ------------------------------ | ----------------------------------------------------------------------------- | ----------------------------------------------------------------------------- | ------- |
| [`.with_partition_by()`]       | [`.write_parquet()`], [`.write_csv()`], [`.write_json()`]                     | Create hive-style output directories by column values                         | `[]`    |
| [`.with_sort_by()`]            | [`.write_parquet()`], [`.write_csv()`], [`.write_json()`], [`.write_table()`] | Sort rows before writing (may add a global sort, which can be expensive)      | `[]`    |
| [`.with_insert_operation()`]   | [`.write_table()`]                                                            | Control insert behavior (Append/Overwrite/Replace), if the target supports it | Append  |
| [`.with_single_file_output()`] | [`.write_parquet()`], [`.write_csv()`], [`.write_json()`]                     | Request coalescing output into a single file                                  | `false` |

> **Note: <br>
> Output layout (files and partitions)** <br>
>
> - **Single file vs dataset directory:** <br>
>   For file writes, `path` determines the default layout. A non-collection path with a file extension (for example `output.parquet`) writes a single file; a directory/collection path (for example `output_dir/`) writes multiple files.\
> - **Partitioned writes:** <br> > [`.with_partition_by()`] creates hive-style directories such as `region=East/`. By default, DataFusion drops partition columns from the file payload and recovers them from directory names on read. Set `execution.keep_partition_by_columns = true` to keep partition columns in the written files.\

> **Warning:** <br> > [`.write_parquet()`], [`.write_csv()`], and [`.write_json()`] currently support `InsertOp::Append` only. <br>
> Use [`.with_insert_operation()`] with [`.write_table()`] when writing to a table sink.

### Writing to Parquet

**Parquet is the go-to file format for analytical workloads in DataFusion—columnar, compressed, and self-describing.**

Parquet's deep Apache Arrow integration enables columnar access, embedded schema/statistics, and compression—unlocking projection pushdown, predicate pruning, and partition elimination on read. Use [`.write_parquet()`] with [`DataFrameWriteOptions`]; for Parquet-specific settings (compression, row-group size), pass [`TableParquetOptions`].

**SQL equivalent (DataFusion SQL API):**

```sql
COPY (SELECT ...) TO 'path' STORED AS PARQUET
```

#### When Parquet Shines

| Use Case                 | Why Parquet Works                                 |
| ------------------------ | ------------------------------------------------- |
| Data lake storage        | Columnar pruning skips irrelevant data on read    |
| Analytics pipelines      | Schema + statistics enable query optimization     |
| Large dataset archival   | Excellent compression ratios (zstd, snappy, gzip) |
| Cross-system interchange | Arrow-native format—zero-copy reads in many tools |

> **Trade-off:** <br>
> Parquet is not human-readable and requires tooling to inspect. For quick debugging or spreadsheet exports, use CSV.

#### Configuration Options

| Option      | Method / Path                     | Example                           | Parallel I/O  |
| ----------- | --------------------------------- | --------------------------------- | :-----------: |
| Single file | File path with extension          | `sales.parquet`                   |       ✗       |
| Multi-file  | Directory path                    | `sales_dataset/`                  |       ✓       |
| Partitioned | [`.with_partition_by()`]          | `["region"]` → `region=East/...`  |       ✓       |
| Sorted      | [`.with_sort_by()`]               | `[col("date").sort(true, false)]` | ✓ (expensive) |
| Compression | `TableParquetOptions.compression` | `"zstd(3)"`, `"snappy"`, `"gzip"` |       —       |

> **File naming:** <br>
> Each write generates a unique random ID → `{write_id}_{part}.parquet`. Writing to the same directory **accumulates files**. To replace data, delete the directory first or use [`.write_table()`] with `InsertOp::Overwrite`.

> **Tip:** <br>
> Prefer directory output for large results—multiple files enable parallel reads.

```rust
use datafusion::prelude::*;
use datafusion::dataframe::DataFrameWriteOptions;
use datafusion::config::TableParquetOptions;
use datafusion::error::Result;

#[tokio::main]
async fn main() -> Result<()> {
    let ctx = SessionContext::new();
    let output = tempfile::tempdir()?;

    // Sample dataset: sales transactions
    let sales_df = ctx.sql("
        SELECT * FROM (VALUES
            (1, 'East',  100, '2024-01-15'),
            (2, 'West',  200, '2024-01-16'),
            (3, 'East',  150, '2024-01-17'),
            (4, 'West',  300, '2024-02-01')
        ) AS t(id, region, amount, sale_date)
    ").await?;

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // 1) Single file — simple export
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    let path = output.path().join("sales.parquet");
    sales_df.clone().write_parquet(
        path.to_str().unwrap(),
        DataFrameWriteOptions::new(),
        None,
    ).await?;

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // 2) Directory output — enables parallel I/O on read
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    let dir = output.path().join("sales_multi/");
    std::fs::create_dir_all(&dir)?;
    sales_df.clone().write_parquet(
        dir.to_str().unwrap(),
        DataFrameWriteOptions::new(),
        None,
    ).await?;
    // Result: sales_multi/{write_id}_0.parquet, {write_id}_1.parquet, ...

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // 3) Partitioned dataset — hive-style directories for partition pruning
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    let partitioned = output.path().join("sales_by_region/");
    std::fs::create_dir_all(&partitioned)?;
    sales_df.clone().write_parquet(
        partitioned.to_str().unwrap(),
        DataFrameWriteOptions::new()
            .with_partition_by(vec!["region".into()]),
        None,
    ).await?;
    // Result: sales_by_region/region=East/{id}.parquet
    //         sales_by_region/region=West/{id}.parquet

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // 4) Sorted + compressed — optimized for range scans
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    let optimized = output.path().join("sales_optimized.parquet");
    let mut parquet_opts = TableParquetOptions::default();
    parquet_opts.global.compression = Some("zstd(3)".into());

    sales_df.write_parquet(
        optimized.to_str().unwrap(),
        DataFrameWriteOptions::new()
            .with_sort_by(vec![col("sale_date").sort(true, false)]),
        Some(parquet_opts),
    ).await?;

    Ok(())
}
```

#### Why This Matters: The Read-Time Payoff

Writing Parquet is an investment that pays dividends every time you query the data:

| What you wrote          | What DataFusion does on read                                            |
| ----------------------- | ----------------------------------------------------------------------- |
| Columnar layout         | Reads only the columns your query needs (projection pushdown)           |
| Embedded statistics     | Skips entire row-groups where min/max don't match your filter           |
| Partitioned directories | Skips entire partition folders that don't match `WHERE region = 'East'` |
| Sorted output           | Enables efficient range scans and early termination                     |

This is why Parquet is the default choice for analytical workloads: the write-time overhead is small, but the read-time savings compound with every query.

> **Deep dive in the benefits of parquet files:**

- [Predicate Pushdown]
- [Partitioned Datasets]
- [Parquet Pruning (blog)][parquet-pruning]
- [Parquet Pushdown (blog)][parquet-pushdown]

### Writing to CSV

**CSV is the universal interchange format—human-readable, supported everywhere, but schema-less and row based.**

CSV trades type fidelity for simplicity: every value becomes a string, there's no embedded schema, and consumers must infer or know the structure. This makes CSV ideal for quick exports, debugging, and integration with tools like Excel, but less suitable for analytics pipelines where Parquet preserves types and enables query optimizations.

Use [`.write_csv()`] with [`DataFrameWriteOptions`] to control layout; pass [`CsvOptions`] to customize delimiters, headers, quoting, and compression.

**SQL equivalent (DataFusion SQL API):**

```sql
COPY (SELECT ...) TO 'path' STORED AS CSV
```

#### When CSV Shines

| Use Case                  | Why CSV Works                          |
| ------------------------- | -------------------------------------- |
| Spreadsheet export        | Excel, Google Sheets open CSV natively |
| Debugging                 | Human-readable—open in any text editor |
| Legacy system integration | Decades of tooling support             |
| Simple ETL handoffs       | No special libraries needed to parse   |

> **Trade-off:** <br>
> CSV loses type information. Integers, floats, dates, and booleans all become strings. On re-read, DataFusion must infer or be told the schema. For type-preserving round-trips, use Parquet.

#### Configuration Options

| Option           | Method                     | Example                                  |
| ---------------- | -------------------------- | ---------------------------------------- |
| Delimiter        | [`.with_delimiter()`]      | `b'\t'` for TSV, `b';'` for European CSV |
| Header row       | [`.with_has_header()`]     | `true` (default) or `false`              |
| Quote character  | [`.with_quote()`]          | `b'"'` (default)                         |
| Escape character | [`.with_escape()`]         | `Some(b'\\')` for backslash escaping     |
| Double quote     | [`.with_double_quote()`]   | `true` (default) — escapes `"` as `""`   |
| Terminator       | [`.with_terminator()`]     | `Some(b'\n')` for Unix line endings      |
| Truncated rows   | [`.with_truncated_rows()`] | `true` to pad short rows with nulls      |
| Compression      | [`.with_compression()`]    | `GZIP`, `ZSTD`, `BZIP2`, `XZ`            |

> **How it works:** <br> > [`CsvOptions`] wraps Arrow's [`WriterBuilder`] for CSV serialization. Most options above have builder methods; fields like i.e. [`date_format`], [`timestamp_format`], [`null`], and [`quote`] must be set directly on the struct. [Compression][`.with_compression()`] is applied by DataFusion as a wrapper around the serialized output.

```rust
use datafusion::prelude::*;
use datafusion::dataframe::DataFrameWriteOptions;
use datafusion::config::CsvOptions;
use datafusion::common::parsers::CompressionTypeVariant;
use datafusion::error::Result;

#[tokio::main]
async fn main() -> Result<()> {
    let ctx = SessionContext::new();
    let output = tempfile::tempdir()?;

    let sales_df = ctx.sql("
        SELECT * FROM (VALUES
            (1, 'East',  100.50, '2024-01-15'),
            (2, 'West',  200.75, '2024-01-16'),
            (3, 'East',  150.00, '2024-01-17')
        ) AS t(id, region, amount, sale_date)
    ").await?;

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // 1) Standard CSV — comma-delimited with header
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    let path = output.path().join("sales.csv");
    sales_df.clone().write_csv(
        path.to_str().unwrap(),
        DataFrameWriteOptions::new(),
        None,  // defaults: comma, header, no compression
    ).await?;

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // 2) TSV with compression — for log ingestion pipelines
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    let path = output.path().join("sales.tsv.gz");
    sales_df.clone().write_csv(
        path.to_str().unwrap(),
        DataFrameWriteOptions::new(),
        Some(CsvOptions::default()
            .with_delimiter(b'\t')
            .with_compression(CompressionTypeVariant::GZIP)),
    ).await?;

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // 3) European-style CSV — semicolon delimiter, no header
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    let path = output.path().join("sales_eu.csv");
    sales_df.write_csv(
        path.to_str().unwrap(),
        DataFrameWriteOptions::new(),
        Some(CsvOptions::default()
            .with_delimiter(b';')
            .with_has_header(false)),
    ).await?;

    Ok(())
}
```

### Writing to JSON (NDJSON)

**JSON is the lingua franca of web APIs and data pipelines—human-readable, self-describing, but row-oriented.**

DataFusion writes newline-delimited JSON (NDJSON): one JSON object per line, not a JSON array. This format streams naturally into log aggregators, message queues, and REST APIs. Unlike Parquet, JSON preserves nested structures as-is but lacks columnar optimizations—every read scans the full file. Use [`.write_json()`] with [`DataFrameWriteOptions`]; pass [`JsonOptions`] for compression settings.

**SQL equivalent (DataFusion SQL API):**

```sql
COPY (SELECT ...) TO 'path' STORED AS JSON
```

#### When JSON Shines

| Use Case           | Why JSON Works                                  |
| ------------------ | ----------------------------------------------- |
| API responses      | Native format for REST/GraphQL endpoints        |
| NoSQL databases    | MongoDB, CouchDB, DynamoDB speak JSON natively  |
| Log ingestion      | NDJSON streams into Elasticsearch, Splunk, Loki |
| Message queues     | Kafka, RabbitMQ consumers expect JSON payloads  |
| Quick debugging    | Human-readable in any text editor               |
| Nested data export | Preserves complex structures without flattening |

> **Trade-off:** <br>
> JSON is row-oriented with no embedded schema or statistics. On re-read, DataFusion must scan the entire file—no column pruning, no predicate pushdown, no row-group skipping. For analytical queries over large datasets, use Parquet; for interchange and streaming, JSON excels.

#### Configuration Options

| Option      | Method / Field              | Example                   |
| ----------- | --------------------------- | ------------------------- |
| Compression | [`JsonOptions.compression`] | `GZIP`, `ZSTD`, `BZIP2`   |
| Partitioned | [`.with_partition_by()`]    | `["region"]` → hive-style |

> **Note:** <br> > [`JsonOptions`] is minimal—just :

- [`compression`][`JsonOptions.compression`]
- [`schema_infer_max_rec`]

No builder methods; set fields directly.

```rust
use datafusion::prelude::*;
use datafusion::dataframe::DataFrameWriteOptions;
use datafusion::config::JsonOptions;
use datafusion::common::parsers::CompressionTypeVariant;
use datafusion::error::Result;

#[tokio::main]
async fn main() -> Result<()> {
    let ctx = SessionContext::new();
    let output = tempfile::tempdir()?;

    let events_df = ctx.sql("
        SELECT * FROM (VALUES
            (1, 'click',  '2024-01-15T10:30:00'),
            (2, 'view',   '2024-01-15T10:31:00'),
            (3, 'purchase','2024-01-15T10:32:00')
        ) AS t(id, event_type, timestamp)
    ").await?;

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // 1) Standard NDJSON — one JSON object per line
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    let path = output.path().join("events.ndjson");
    events_df.clone().write_json(
        path.to_str().unwrap(),
        DataFrameWriteOptions::new(),
        None,
    ).await?;
    // Output:
    // {"id":1,"event_type":"click","timestamp":"2024-01-15T10:30:00"}
    // {"id":2,"event_type":"view","timestamp":"2024-01-15T10:31:00"}
    // ...

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // 2) Compressed NDJSON — for log shipping
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    let path = output.path().join("events.ndjson.gz");
    let mut json_opts = JsonOptions::default();
    json_opts.compression = CompressionTypeVariant::GZIP;

    events_df.write_json(
        path.to_str().unwrap(),
        DataFrameWriteOptions::new(),
        Some(json_opts),
    ).await?;

    Ok(())
}
```

### Writing to Registered Tables

**[`.write_table()`] is the universal sink—write to any storage backend that implements [`TableProvider`].**

Unlike file-specific methods (`.write_parquet()`, `.write_csv()`), `.write_table()` abstracts the destination entirely. The target [`TableProvider`] controls format, partitioning, transactions, and error handling. This enables writing to databases, cloud warehouses, custom connectors, or in-memory tables with the same API.

Under the hood, DataFusion calls [`TableProvider::insert_into`], which returns an [`ExecutionPlan`] streaming `RecordBatch`es into the target—the provider converts them to its native format.

**SQL equivalent (DataFusion SQL API):**

```sql
INSERT INTO sales SELECT * FROM new_data
```

#### When to Use [`.write_table()`]

| Use Case                | Why [`.write_table()`] Works                      |
| ----------------------- | ------------------------------------------------- |
| Database connectors     | Provider handles connection, batching, retries    |
| Cloud data warehouses   | Snowflake, BigQuery, Redshift via custom provider |
| Multi-format pipelines  | Same code writes to Parquet, Delta, Iceberg       |
| In-memory staging       | [`MemTable`] for intermediate results             |
| Transactional semantics | Provider controls commit/rollback behavior        |

> **Key difference from file methods:** <br> > `.write_parquet()` / `.write_csv()` write directly to object storage. [`.write_table()` ]delegates to a registered table—the provider decides how and where data lands.

#### Insert Operations

Use [`DataFrameWriteOptions::with_insert_operation(...)`][`with_insert_operation()`] to control insert semantics:

| Operation                            | SQL Equivalent     | Behavior                                    | Provider Support               |
| ------------------------------------ | ------------------ | ------------------------------------------- | ------------------------------ |
| [`InsertOp::Append`][`Append`]       | `INSERT INTO`      | Add new rows to existing data               | Most providers                 |
| [`InsertOp::Overwrite`][`Overwrite`] | `INSERT OVERWRITE` | Replace all existing rows                   | Some providers (e.g., Parquet) |
| [`InsertOp::Replace`][`Replace`]     | `REPLACE INTO`     | Replace conflicting rows (upsert semantics) | Few providers                  |

> **Warning:** <br>
> Not all providers support all operations. <br> > [`MemTable`] currently supports [`Append`] only. Check your provider's documentation for supported operations. See [docs.rs][`MemTable::insert_into`]

#### Schema Compatibility

The DataFrame schema must be **logically equivalent** to the target table's schema—column names (case-sensitive), data types (exact or implicitly castable), and column order must align.

If schemas don't match, use [`.select()`] with [`.alias()`] to reorder/rename, or [`.cast_to()`] to align types before writing.

> **See also:** <br> > [Schema Management](./schema-management.md) covers type coercion rules, validation patterns, and debugging mismatches in depth.

#### Built-in TableProvider Implementations

| Provider                    | Supports [`insert_into`] | Supported Operations | Notes                                           |
| --------------------------- | ------------------------ | -------------------- | ----------------------------------------------- |
| [`MemTable`]                | Yes                      | Append               | In-memory; data lost on session end             |
| [`ListingTable`] (Parquet)  | Yes                      | Append               | Writes new Parquet files to the table directory |
| [`ListingTable`] (CSV/JSON) | Yes                      | Append               | Writes new files to the table directory         |
| Custom [`TableProvider`]    | Implementation-defined   | Varies               | Database connectors, object stores, etc.        |

> **Tip:** <br>
> For custom database connectors, implement [`TableProvider::insert_into`] to handle data ingestion. The input [`ExecutionPlan`] provides a stream of [`RecordBatch`]es that your implementation converts to the target format.

```rust
use datafusion::prelude::*;
use datafusion::dataframe::DataFrameWriteOptions;
use datafusion::assert_batches_eq;
use datafusion::error::Result;
use datafusion::logical_expr::dml::InsertOp;

#[tokio::main]
async fn main() -> Result<()> {
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
        ) AS sales(id, region, amount)
    ").await?;

    // Append to table (default behavior)
    let write_options = DataFrameWriteOptions::new().with_insert_operation(InsertOp::Append);
    let write_result = new_sales.write_table(
        "sales",
        write_options,
    ).await?;

    assert_batches_eq!(
        &[
            "+-------+",
            "| count |",
            "+-------+",
            "| 3     |",
            "+-------+",
        ],
        &write_result
    );

    // Verify the data was written (read from the table)
    let batches = ctx
        .sql("SELECT COUNT(*) AS count FROM sales")
        .await?
        .collect()
        .await?;
    assert_batches_eq!(
        &[
            "+-------+",
            "| count |",
            "+-------+",
            "| 3     |",
            "+-------+",
        ],
        &batches
    );

    Ok(())
}
```

#### Prerequisites

The target table must be registered before calling `.write_table()`:

| Method               | Example                                                |
| -------------------- | ------------------------------------------------------ |
| SQL DDL              | `CREATE EXTERNAL TABLE sales ... LOCATION '...'`       |
| [`register_table()`] | `ctx.register_table("sales", Arc::new(my_provider))?;` |

#### Performance Tip

For row-oriented sinks (database connectors), **Arrow-to-row conversion and network I/O dominate cost**—not DataFrame vs. SQL syntax. Push filters and projections into the [`TableProvider`] where possible.

### When to Consider Lakehouse Table Formats

**Raw file writes are simple but brittle—lakehouse formats add transactional guarantees.**

For one-off exports, raw Parquet/CSV/JSON is fine. But as pipelines grow—concurrent writers, schema changes, failure recovery—the cracks show. [Apache Iceberg], [Delta Lake], and [Apache Hudi] add a metadata layer that provides database-like semantics on top of object storage.

#### Raw Files vs. Table Formats

| Capability              | Raw Files                   | Lakehouse Formats                |
| ----------------------- | --------------------------- | -------------------------------- |
| **Atomic commits**      | ✗ Partial writes on failure | ✓ All-or-nothing                 |
| **Concurrent writers**  | ✗ Risk of corruption        | ✓ Conflict detection             |
| **Schema evolution**    | ✗ Manual coordination       | ✓ Add/rename/drop columns safely |
| **Time travel**         | ✗ Manual snapshots only     | ✓ Query any historical version   |
| **Compaction**          | ✗ Small files accumulate    | ✓ Automatic or on-demand         |
| **Partition evolution** | ✗ Full rewrite required     | ✓ Change without rewriting       |

#### Decision Guide

| Scenario                                  | Recommendation |
| ----------------------------------------- | -------------- |
| One-off exports, ad-hoc analysis          | Raw files ✓    |
| Single-writer, append-only logs           | Raw files ✓    |
| Systems that only read raw Parquet/CSV    | Raw files ✓    |
| Multiple concurrent ETL jobs              | Table format ✓ |
| Need failure recovery without cleanup     | Table format ✓ |
| Schema evolution over time                | Table format ✓ |
| Audit/compliance (lineage, point-in-time) | Table format ✓ |
| Large lakes with frequent small updates   | Table format ✓ |

#### DataFusion Integration

DataFusion provides the [`TableProvider`] and [`TableProviderFactory`] traits—external crates implement them for specific formats. See [Catalogs, Schemas, and Tables] for the full architecture.

| Format         | Rust Crate                                             | Status           |
| -------------- | ------------------------------------------------------ | ---------------- |
| Delta Lake     | [delta-rs](https://github.com/delta-io/delta-rs)       | Production-ready |
| Apache Iceberg | [iceberg-rust](https://github.com/apache/iceberg-rust) | Maturing         |
| Lance          | [lance](https://github.com/lancedb/lance)              | Production-ready |

Once registered, [`.write_table()`] works unchanged—your DataFrame code stays the same, only storage semantics differ.

> **Build your own:** <br>
> See [Custom Table Provider] for implementing data sources/sinks, or [datafusion-contrib](https://github.com/datafusion-contrib) for community integrations.

[Catalogs, Schemas, and Tables]: https://datafusion.apache.org/library-user-guide/catalogs.html
[Custom Table Provider]: https://datafusion.apache.org/library-user-guide/custom-table-providers.html
[Apache Iceberg]: https://iceberg.apache.org/
[Delta Lake]: https://delta.io/
[Apache Hudi]: https://hudi.apache.org/
[`TableProviderFactory`]: https://docs.rs/datafusion/latest/datafusion/catalog/trait.TableProviderFactory.html

## Quick Reference

| Practice                        | Why                                                    |
| ------------------------------- | ------------------------------------------------------ |
| Prefer Parquet                  | Schema + compression + columnar pruning on read        |
| Use directory outputs           | Enables parallel reads; avoids single-file bottlenecks |
| Partition by filter columns     | `with_partition_by(["region"])` → partition pruning    |
| Register object stores first    | Required for `s3://`, `gcs://`, `memory:///` URLs      |
| Use `.with_sort_by()` sparingly | Enables range scans but adds expensive global sort     |

---

## Further Reading

### DataFusion & Arrow Ecosystem

| Resource                                                                                              | Description                                                                                                                 |
| ----------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------- |
| [How Query Engines Work](https://howqueryengineswork.com/)                                            | Book by Andy Grove (DataFusion creator) — from parsing to execution                                                         |
| [DataFusion Architecture](https://datafusion.apache.org/contributor-guide/architecture.html)          | Official guide to DataFusion internals                                                                                      |
| [Catalogs, Schemas, and Tables](https://datafusion.apache.org/library-user-guide/catalogs.html)       | Implementing custom `CatalogProvider` / `TableProvider`                                                                     |
| [Custom Table Provider](https://datafusion.apache.org/library-user-guide/custom-table-providers.html) | Building your own data sources and sinks                                                                                    |
| [Apache Arrow Overview](https://arrow.apache.org/overview/)                                           | The in-memory format powering DataFusion                                                                                    |
| [ADBC (Arrow Database Connectivity)](https://arrow.apache.org/docs/format/ADBC.html)                  | Arrow-native database connectivity APIs (see [Rust quickstart](https://arrow.apache.org/adbc/current/rust/quickstart.html)) |
| [Parquet Pruning in DataFusion][parquet-pruning]                                                      | Blog: How DataFusion skips data at read time                                                                                |
| [Parquet Predicate Pushdown][parquet-pushdown]                                                        | Blog: Filter pushdown into the Parquet reader                                                                               |
| [DataFusion Blog](https://datafusion.apache.org/blog/)                                                | Release notes, deep dives, and performance insights                                                                         |

### Format Specifications

- [Apache Parquet Format](https://parquet.apache.org/docs/) — Columnar storage specification
- [Apache Arrow Columnar Format](https://arrow.apache.org/docs/format/Columnar.html) — In-memory format DataFusion uses
- [Delta Lake Protocol](https://github.com/delta-io/delta/blob/master/PROTOCOL.md) — ACID transactions on object storage
- [Apache Iceberg Spec](https://iceberg.apache.org/spec/) — Table format for huge analytic datasets

### Comparable Systems

| System                                                                                             | Focus              | Write Model                  |
| -------------------------------------------------------------------------------------------------- | ------------------ | ---------------------------- |
| [Apache Spark](https://spark.apache.org/docs/latest/sql-data-sources-parquet.html)                 | Distributed ETL    | Partitioned datasets         |
| [DuckDB](https://duckdb.org/docs/data/parquet/overview)                                            | Embedded analytics | `COPY TO` / `INSERT INTO`    |
| [ClickHouse](https://clickhouse.com/docs/en/engines/table-engines/mergetree-family)                | Real-time OLAP     | MergeTree engines            |
| [Polars](https://docs.pola.rs/api/python/stable/reference/api/polars.DataFrame.write_parquet.html) | Fast DataFrames    | `write_parquet()` / `sink_*` |

### Academic Background

- Abadi et al., [_Column-Stores vs. Row-Stores_](https://dl.acm.org/doi/10.1145/1376616.1376712) (SIGMOD 2008) — Why columnar formats matter
- Melnik et al., [_Dremel: Interactive Analysis of Web-Scale Datasets_](https://research.google/pubs/pub36632/) (VLDB 2010) — Nested columnar storage (inspired Parquet)
- Armbrust et al., [_Delta Lake: High-Performance ACID Table Storage_](https://www.vldb.org/pvldb/vol13/p3411-armbrust.pdf) (VLDB 2020) — Lakehouse architecture

### Books

- Kleppmann, [_Designing Data-Intensive Applications_](https://dataintensive.net/) — Encoding, storage, and batch processing
- Chambers & Zaharia, [_Spark: The Definitive Guide_](https://www.oreilly.com/library/view/spark-the-definitive/9781491912201/) — DataFrame patterns at scale
- Petrov, [_Database Internals_](https://www.databass.dev/) — Storage engines and distributed systems

---

**Navigation:** [Creating DataFrames](creating-dataframes.md) ← **You are here** → [Best Practices](best-practices.md)

<!-- Tobe sorted Later (Ongoing tomorrow job ;D) -->

[`ListingTable`]: https://docs.rs/datafusion/latest/datafusion/datasource/listing/struct.ListingTable.html
[`date_format`]: https://docs.rs/datafusion/latest/datafusion/common/arrow/csv/struct.WriterBuilder.html#method.date_format
[`timestamp_format`]: https://docs.rs/datafusion/latest/datafusion/common/arrow/csv/struct.WriterBuilder.html#method.timestamp_format
[`null`]: https://docs.rs/datafusion/latest/datafusion/common/arrow/csv/struct.WriterBuilder.html#method.null
[`quote`]: https://docs.rs/datafusion/latest/datafusion/common/arrow/csv/struct.WriterBuilder.html#method.quote
[`WriterBuilder`]: https://docs.rs/datafusion/latest/datafusion/common/arrow/csv/struct.WriterBuilder.html
[Predicate Pushdown]: creating-dataframes.md#predicate-pushdown-filtering-at-source
[Partitioned Datasets]: schema-management.md#strategy-3-partitioned-datasets--pruning-with-listingtable
[parquet-pruning]: https://datafusion.apache.org/blog/2025/03/20/parquet-pruning/
[parquet-pushdown]: https://datafusion.apache.org/blog/2025/03/21/parquet-pushdown/
[`.with_escape()`]: https://docs.rs/datafusion/latest/datafusion/config/struct.CsvOptions.html#method.with_escape
[`.with_double_quote()`]: https://docs.rs/datafusion/latest/datafusion/config/struct.CsvOptions.html#method.with_double_quote
[`.with_terminator()`]: https://docs.rs/datafusion/latest/datafusion/config/struct.CsvOptions.html#method.with_terminator
[`.with_truncated_rows()`]: https://docs.rs/datafusion/latest/datafusion/config/struct.CsvOptions.html#method.with_truncated_rows
[`.with_compression()`]: https://docs.rs/datafusion/latest/datafusion/config/struct.CsvOptions.html#method.with_compression
[`JsonOptions.compression`]: https://docs.rs/datafusion/latest/datafusion/common/config/struct.JsonOptions.html#structfield.compression
[`schema_infer_max_rec`]: https://docs.rs/datafusion/latest/datafusion/common/config/struct.JsonOptions.html#structfield.schema_infer_max_rec
[`Append`]: https://docs.rs/datafusion/latest/datafusion/logical_expr/logical_plan/dml/enum.InsertOp.html#variant.Append
[`Overwrite`]: https://docs.rs/datafusion/latest/datafusion/logical_expr/logical_plan/dml/enum.InsertOp.html#variant.Overwrite
[`Replace`]: https://docs.rs/datafusion/latest/datafusion/logical_expr/logical_plan/dml/enum.InsertOp.html#variant.Replace
[`with_insert_operation()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrameWriteOptions.html#method.with_insert_operation
[`ExecutionPlan`]: https://docs.rs/datafusion/latest/datafusion/physical_plan/trait.ExecutionPlan.html
[`MemTable`]: https://docs.rs/datafusion/latest/datafusion/catalog/struct.MemTable.html
[`MemTable::insert_into`]: https://docs.rs/datafusion-catalog/51.0.0/src/datafusion_catalog/memory/table.rs.html#274-298

<!-- Local references for this section -->

[`MemoryPool`]: https://docs.rs/datafusion/latest/datafusion/execution/memory_pool/trait.MemoryPool.html
[`DiskManager`]: https://docs.rs/datafusion/latest/datafusion/execution/disk_manager/struct.DiskManager.html
[`RuntimeEnv`]: https://docs.rs/datafusion/latest/datafusion/execution/runtime_env/struct.RuntimeEnv.html
[`RuntimeEnvBuilder`]: https://docs.rs/datafusion/latest/datafusion/execution/runtime_env/struct.RuntimeEnvBuilder.html
[`Partitioning`]: https://docs.rs/datafusion/latest/datafusion/logical_expr/enum.Partitioning.html
[`.count()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.count
[`.limit()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.limit
[`.repartition()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.repartition
[`get_record_batch_memory_size()`]: https://docs.rs/datafusion/latest/datafusion/physical_plan/spill/fn.get_record_batch_memory_size.html

<!-- DataFusion methods -->

[`.clone()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.clone
[`.into_view()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.into_view
[`.explain()`]: ../../user-guide/explain-usage.md
[`.create_physical_plan()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.create_physical_plan
[`.registry()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.registry
[`.with_partition_by()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrameWriteOptions.html#method.with_partition_by
[`.with_sort_by()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrameWriteOptions.html#method.with_sort_by
[`.with_insert_operation()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrameWriteOptions.html#method.with_insert_operation
[`.with_single_file_output()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrameWriteOptions.html#method.with_single_file_output

<!-- External Datafusion documentation -->

[Architecture Guide]: https://docs.rs/datafusion/latest/datafusion/#architecture
[SIGMOD 2024 Paper]: https://andrew.nerdnetworks.org/pdf/SIGMOD-2024-lamb.pdf
[How Query Engines Work]: https://howqueryengineswork.com/00-introduction.html
[Apache Arrow Columnar Format]: https://arrow.apache.org/docs/format/Columnar.html
[Morsel-Driven Parallelism]: https://db.in.tum.de/~leis/papers/morsels.pdf
[Configuration Settings]: ../../user-guide/configs.md
[Streaming Execution (crate docs)]: https://docs.rs/datafusion/latest/datafusion/#streaming-execution
[Ordering Analysis (DataFusion blog)]: https://datafusion.apache.org/blog/2025/03/11/ordering-analysis
[Databricks KB: Spark cache recomputation pitfalls]: https://kb.databricks.com/python/expensive-transformation-on-dataframe-is-recalculated-even-when-cached
[Understanding Spark Execution Planning]: https://medium.com/@aj.patil9292/understanding-spark-execution-planning-from-code-to-cluster-66ea1bd372df
[Understanding Lazy Evaluation in Polars]: https://medium.com/data-science/understanding-lazy-evaluation-in-polars-b85ccb864d0c

<!-- Core type references -->

[`DataFrame`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html
[`LogicalPlan`]: https://docs.rs/datafusion/latest/datafusion/logical_expr/enum.LogicalPlan.html
[`RecordBatch`]: https://docs.rs/arrow-array/latest/arrow_array/struct.RecordBatch.html
[`SessionContext`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html
[`SessionContext::register_table`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.register_table
[`register_table()`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.register_table
[`TableProvider`]: https://docs.rs/datafusion/latest/datafusion/datasource/trait.TableProvider.html
[`DataFrameWriteOptions`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrameWriteOptions.html
[`InsertOp`]: https://docs.rs/datafusion/latest/datafusion/logical_expr/dml/enum.InsertOp.html
[`TableParquetOptions`]: https://docs.rs/datafusion/latest/datafusion/common/config/struct.TableParquetOptions.html
[`CsvOptions`]: https://docs.rs/datafusion/latest/datafusion/config/struct.CsvOptions.html
[`JsonOptions`]: https://docs.rs/datafusion/latest/datafusion/config/struct.JsonOptions.html

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
[`.with_column_renamed()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.with_column_renamed
[`TableProvider::insert_into`]: https://docs.rs/datafusion/latest/datafusion/datasource/trait.TableProvider.html#method.insert_into
[`.with_delimiter()`]: https://docs.rs/datafusion/latest/datafusion/config/struct.CsvOptions.html#method.with_delimiter
[`.with_has_header()`]: https://docs.rs/datafusion/latest/datafusion/config/struct.CsvOptions.html#method.with_has_header
[`.with_quote()`]: https://docs.rs/datafusion/latest/datafusion/config/struct.CsvOptions.html#method.with_quote
[`.with_compression()`]: https://docs.rs/datafusion/latest/datafusion/config/struct.CsvOptions.html#method.with_compression

<!-- Example references -->

[`custom_file_format.rs`]: https://github.com/apache/datafusion/blob/main/datafusion-examples/examples/custom_file_format.rs
