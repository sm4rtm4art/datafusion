<!--
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

# Execution Lifecycle

**Nothing runs until you ask for results — lazy execution lets the optimizer rewrite your query before a single byte is read.**

You've seen that a [`DataFrame`] pairs a [`LogicalPlan`] with a [`SessionState`] clone (see [Anatomy of a DataFrame](anatomy-dataframe.md)). DataFrames are lazy: calling [`.filter()`] or [`.join()`] merely extends the plan — the recipe describing _what_ to compute. The `SessionState` clone provides the execution environment, with config and functions independently copied while the catalog and runtime remain shared via `Arc` (see [The SessionState Clone](../Creating-DataFrames/creating-concepts.md#the-sessionstate-clone)). This section follows the plan from construction through optimization to streaming results, explaining what happens at each stage and why deferring execution produces faster queries.

```{contents} Table of Contents for Execution Lifecycle
:local:
:depth: 2
```

## The Lifecycle at a Glance

**The journey from lazy plan to streaming results has one clear boundary: the action call.**

Everything above the **ACTION line** is **lazy** (building a plan -> no execution / work); everything below happens **only when you call [`.collect()`], [`.show()`], or [`.write_*()`][`.write_parquet()`]**. The diagram traces the full journey, showing why deferring execution lets the optimizer reorder operations, push predicates to data sources, and select efficient algorithms:

```text
┌───────────────────────────────────────────────────────────────┐
│                        SessionContext                         │
│   (Primary Entry Point: Catalog, Function Registry, Config)   │
└─────────────┬───────────────────────────────┬─────────────────┘
              │                               │
  ┌───────────▼───────────┐       ┌───────────▼───────────────┐
  │        SQL API        │       │       DataFrame API       │
  │     (Declarative)     │       │       (Programmatic)      │
  ├───────────────────────┤       ├───────────────────────────┤
  │  "SELECT a, b FROM t  │       │ ctx.table("t")            │
  │   WHERE a > 10"       │       │   .filter(col("a").gt(10))│
  └───────────┬───────────┘       └───────────┬───────────────┘
              │                               │
              │ parse / plan                  │ build
              └───────────────┬───────────────┘
                              │
                              ▼
        ┌──────────────────────────────────────────┐
        │                DataFrame                 │
        │        (Immutable Query Handle)          │
        ├──────────────────────────────────────────┤
        │  1. LogicalPlan (Abstract Query)         │
        │  2. SessionState (Clone of Context)      │
        └─────────────────────┬────────────────────┘
                              │                          LAZY
══════════════════════════════╪═══════════════════ ACTION BOUNDARY
                              │ (.collect / .show / .write)
                              ▼                          EAGER
        ┌──────────────────────────────────────────┐
        │            Logical Optimizer             │
        │ (Predicate Pushdown, Projection Pruning) │
        └─────────────────────┬────────────────────┘
                              │
                              ▼
        ┌──────────────────────────────────────────┐
        │             Physical Planner             │
        │  (Map Logical nodes to Physical Exec)    │
        └─────────────────────┬────────────────────┘
                              │
                              ▼
        ┌──────────────────────────────────────────┐
        │            Physical Optimizer            │
        │ (Coalesce Batches, Pipeline Parallelism) │
        └─────────────────────┬────────────────────┘
                              │
                              ▼
        ┌──────────────────────────────────────────┐
        │            Execution (Tokio)             │
        │   (Pull-based Stream of RecordBatches)   │
        └──────────────────────────────────────────┘
```

::::::{admonition} Reading the diagram
:class: seealso

:::{admonition} SessionContext (top container)
:class: note
The primary entry point holding your catalog, function registry, and configuration.
:::

:::{admonition} SQL API / DataFrame API → DataFrame
:class: note
Both paths converge to the same `DataFrame` structure — a lazy handle wrapping a `LogicalPlan` (what to compute) and a `SessionState` clone (execution environment).
:::

:::{admonition} ACTION boundary
:class: note
Nothing executes until you call `.collect()`, `.show()`, or `.write_*()`. Above the line is lazy; below is eager.
:::

:::{admonition} Logical Optimizer → Physical Planner → Physical Optimizer
:class: note
The logical optimizer rewrites the plan (predicate pushdown, projection pruning), the physical planner chooses algorithms (hash join vs. sort-merge), and the physical optimizer adds parallelism and batching.
:::

:::{admonition} Execution (Tokio)
:class: note
Pull-based streaming via `poll_next()` — data flows as `RecordBatch` chunks through operators in parallel.
:::

::::::

:::{admonition} Glossary snapshot
:class: seealso

- **[`SessionContext`]**: Entry point for creating DataFrames, configuring execution, and registering tables/functions.
- **[`SessionState`]**: Structural clone of context configuration and execution environment used when executing a DataFrame.
- **[`DataFrame`]**: Lazy wrapper pairing a `LogicalPlan` with a `SessionState` clone; transformations build plans, actions execute them.
- **[`LogicalPlan`]**: Tree describing _what_ to compute (projection, filter, join, etc.).
- **[`ExecutionPlan`]**: Physical operator tree describing _how_ to compute (hash aggregate, parquet scan, shuffle, etc.).
- **[`RecordBatch`]**: Arrow data structure representing a chunk of rows in columnar form; execution produces streams of batches.

For deeper architectural details — thread scheduling, memory management, crate organization — see the [Architecture section] in the API documentation.
:::

---

## DataFrame Method Categories

**Every DataFrame method is either a lazy transformation that builds the plan, or an eager action that triggers execution.**

Understanding which methods are **lazy** and which trigger **eager** execution is essential — it determines when work actually happens.

| Category              | Lazy/Eager | Purpose                              | Examples                                                                        |
| --------------------- | ---------- | ------------------------------------ | ------------------------------------------------------------------------------- |
| **Transformations**   | Lazy       | Build/extend the `LogicalPlan`       | [`.select()`], [`.filter()`], [`.aggregate()`], [`.join()`], [`.with_column()`] |
| **Execution Actions** | Eager      | Trigger optimization → execution     | [`.collect()`], [`.show()`], [`.execute_stream()`], [`.count()`], [`.cache()`]  |
| **Write Actions**     | Eager      | Execute and persist results to files | [`.write_parquet()`], [`.write_csv()`], [`.write_table()`]                      |
| **Introspection**     | Lazy\*     | Inspect plan metadata                | [`.schema()`], [`.explain()`], [`.logical_plan()`], [`.into_optimized_plan()`]  |

\* **Introspection methods** access or transform the plan itself without scanning data. The exception is [`.explain()`] with `analyze = true`, which triggers full execution to gather runtime statistics.

::::::{admonition} Additional context
:class: seealso

:::{admonition} Transformations
:class: note
Return a new `DataFrame` wrapping an extended `LogicalPlan`. Chain as many as you like—no data moves until you call an action.
:::

:::{admonition} Execution Actions
:class: note
Cross the **ACTION boundary** from the lifecycle diagram: they trigger the Optimizer, create an `ExecutionPlan`, and run it. Results flow back as `RecordBatch`es.
:::

:::{admonition} Write Actions
:class: note
Needs execution for processing, but stream results to files instead of returning them to your code. **Higher computation is to be expected due to I/O and disk-writing costs.**
:::

:::{admonition} Introspection
:class: note
Methods access plan metadata without executing. Exception: [`.explain()`] with `analyze = true` \*does\* execute to gather runtime statistics.
:::

::::::

:::{admonition} Further reading
:class: seealso

For the complete method reference, see [Transformations](transformations.md).
:::

---

## What Happens During Execution?

**DataFusion's out-of-the-box optimizer rewrites your query — often dramatically — before a single row is processed.**

When you call an action like [`.collect()`], the lazy plan crosses the ACTION boundary and enters a multi-phase pipeline. What looks like a simple filter to you triggers a series of optimizations that DataFusion handles automatically in the background. Only the **applicable rules fire** based on your specific plan structure:

1. **Logical Optimization** ([21+ optimizer rules][optimizer-rules], multiple passes):

   - Predicate pushdown (move filters closer to scans)
   - Projection pruning (remove unused columns)
   - Common subexpression elimination
   - Constant folding and simplification

2. **Physical Planning** ([19+ physical rules][physical-rules]):

   - Choose concrete algorithms (HashJoin vs SortMergeJoin)
   - Insert repartitioning for parallelism
   - Add sorts where needed
   - Select scan strategies (parallel file readers)

3. **Execution** (parallel, streaming):
   - Stream data through operators in chunks (`RecordBatch`es)
   - Execute partitions in parallel via Tokio
   - Spill to disk if memory limits exceeded

:::{admonition} Memory vs. Streaming
:class: note
[`.collect()`] buffers all results in memory—convenient but risky for large datasets. Use [`.execute_stream()`] for incremental processing, or write directly to files with [`.write_parquet()`].
:::

### Optimizer Architecture

DataFusion uses a **pragmatic hybrid approach** — not a Cascades-style optimizer with memoized search over equivalence classes, but a deterministic, debuggable pipeline:

- **Logical optimization:**<br> Rule-based iterative rewrites (predicate pushdown, projection pruning, etc.).
- **Physical planning:** <br>Statistics-informed decisions where beneficial (join algorithm selection, partition count).
- **Design philosophy:** <br>"Solid heuristic optimizer as default + extension points for experimentation" ([#1972](https://github.com/apache/datafusion/issues/1972)).

The same DataFrame builder chain produces the same optimized plan every time — predictable and debuggable. While statistics are used, there's no exhaustive cost-based enumeration.

:::{admonition} Further reading
:class: note

- [Query Optimizer guide](../query-optimizer.md) — optimization phases and rules
- [DataFusion paper (SIGMOD 2024)](https://dl.acm.org/doi/10.1145/3626246.3653368) — academic foundation
  :::

---

## Why the Physical Plan Matters

**The `ExecutionPlan` is your window into what DataFusion will actually do — inspecting it before running on large data catches inefficiencies early.**

During query development, the `LogicalPlan` you build describes **what** you want — the `ExecutionPlan` reveals **how** it happens. Most of the time DataFusion's optimizer handles this automatically, but understanding the physical plan helps when you need to diagnose performance issues or verify that optimizations fired as expected.

**Common scenarios where understanding the plan helps:**

| Scenario                 | What to look for                      | Impact                                                              |
| ------------------------ | ------------------------------------- | ------------------------------------------------------------------- |
| **Filter placement**     | Is the filter pushed before the join? | Filtering 1M→1K rows _before_ joining is orders of magnitude faster |
| **Join algorithm**       | `HashJoinExec` vs `SortMergeJoinExec` | Hash joins are faster for unsorted data; sort-merge for pre-sorted  |
| **Build side selection** | Which table builds the hash table?    | Smaller table should be the build side (less memory)                |
| **Projection pruning**   | Are unused columns eliminated early?  | Reading fewer columns = less I/O, especially for Parquet            |

**Example: Filter placement matters**

```text
Filter EARLY (optimized):  Filter LATE (naive):

┌─────────┐                 ┌─────────┐
│  Scan   │ 1M rows         │  Scan   │ 1M rows
└────┬────┘                 └────┬────┘
     ▼                           ▼
┌─────────┐                 ┌─────────┐
│ Filter  │ → 1K rows       │  Join   │ 1M × 100K rows
└────┬────┘                 └────┬────┘
     ▼                           ▼
┌─────────┐                 ┌─────────┐
│  Join   │ 1K × 100K rows  │ Filter  │ filter AFTER join
└─────────┘                 └─────────┘
```

DataFusion's optimizer usually pushes filters down automatically (predicate pushdown), but it can't always—e.g., when the filter references columns from both sides of a join. Understanding the plan helps you restructure queries when automatic optimization isn't enough.

Use `.explain()` to inspect your plan:

```rust
use datafusion::prelude::*;
use datafusion::error::Result;
use datafusion::functions_aggregate::expr_fn::sum;

#[tokio::main]
async fn main() -> Result<()> {
    let orders = dataframe!(
        "order_id" => [1, 2, 3, 4],
        "customer_id" => [100, 101, 100, 102],
        "amount" => [50, 75, 120, 200]
    )?;

    let customers = dataframe!(
        "id" => [100, 101, 102],
        "name" => ["Alice", "Bob", "Carol"]
    )?;

    // Build a query: join, filter, aggregate
    let df = orders
        .join(customers, JoinType::Inner, &["customer_id"], &["id"], None)?
        .filter(col("amount").gt(lit(60)))?
        .aggregate(vec![col("name")], vec![sum(col("amount")).alias("total")])?;

    // Inspect the physical plan
    df.clone().explain(false, false)?.show().await?;

    // Execute
    df.show().await?;
    Ok(())
}
```

:::{admonition} Performance tip
:class: tip
[`.explain(true, false)`][`.explain()`] shows the optimized logical plan; [`.explain(true, true)`][`.explain()`] adds runtime statistics (actually runs the query). Start with the plan, profile if needed. For more details, see [`.explain()` examples].
:::

### Data source matters

For in-memory data (like the [`dataframe!`] macro), optimizations focus on operation order and algorithm selection. For file-based sources (Parquet, CSV), additional optimizations kick in—predicate pushdown to skip row groups, projection pushdown to read only needed columns. See [Creating DataFrames: From Files](creating-dataframes.md#1-from-files) for file-specific tuning.

### Execution-Level Optimizations

The physical plan enables execution-level optimizations that go beyond planning. For Parquet sources, DataFusion applies:

- **Pruning** — skip entire files/row groups based on statistics ([blog: Parquet Pruning])
- **Filter pushdown with late materialization** — read filter columns first, selectively decode matching rows ([blog: Filter Pushdown])

These are advanced topics for readers tuning file-based workloads.

### Execution-Level Optimizations References

- [Optimizer rules (source)][optimizer-rules]
- [Physical optimizer rules (source)][physical-rules]

---

## RecordBatches and Partitions: How Data Actually Flows

**A DataFrame doesn't hold data as one giant table in memory — execution streams it as partitioned, columnar chunks.**

When an action triggers execution, DataFusion doesn't materialize a single monolithic result. Instead, data flows through the operator pipeline in two key units:

- **Partitions** — the unit of parallelism. DataFusion splits work into multiple independent partitions (controlled by [`target_partitions`], defaulting to the number of CPU cores). Each partition runs as a separate async stream on Tokio's thread pool. This is why DataFusion scales across cores without you writing threading code.

- **RecordBatches** — the unit of data. Each partition yields a stream of `RecordBatch` values — Arrow's columnar data format holding up to `batch_size` rows (default: 8192). Operators process one batch at a time: decode, filter, aggregate, then yield to the next operator. This keeps memory usage bounded regardless of total dataset size.

```text
                     ExecutionPlan
                          │
       ┌──────────────────┼──────────────────┐
       ▼                  ▼                  ▼
 Partition 0        Partition 1        Partition 2
       │                  │                  │
  ┌────┴────┐       ┌────┴────┐       ┌────┴────┐
  │  Batch  │       │  Batch  │       │  Batch  │
  │ (8192)  │       │ (8192)  │       │ (8192)  │
  ├─────────┤       ├─────────┤       ├─────────┤
  │  Batch  │       │  Batch  │       │  Batch  │
  │ (8192)  │       │ (8192)  │       │ (4501)  │
  ├─────────┤       └─────────┘       └─────────┘
  │  Batch  │
  │ (2047)  │  ← last batch may be smaller
  └─────────┘

Each partition streams independently on Tokio's thread pool
```

**Last batch may be smaller:**
The final batch in each partition may contain fewer rows than batch_size — it simply holds whatever rows remain.

### Action methods and batch handling

Since execution produces multiple streams of batches across partitions, the action method you choose determines how those batches are collected — all at once into memory, or incrementally as a stream. This choice directly impacts memory usage as shown below:

| Action                | How it handles batches                                                | Memory profile          |
| :-------------------- | :-------------------------------------------------------------------- | :---------------------- |
| [`.collect()`]        | Buffers **all** batches from all partitions into a `Vec<RecordBatch>` | Entire result in memory |
| [`.execute_stream()`] | Yields batches **one at a time** as they're produced                  | Bounded, streaming      |
| [`.write_parquet()`]  | Streams batches directly to file writers                              | Bounded, streaming      |
| [`.show()`]           | Collects all batches, then formats as a table                         | Entire result in memory |

:::{admonition} Best practice
:class: tip
Use [`.collect()`] for small results or when you need all data at once (e.g., assertions in tests). Use [`.execute_stream()`] for production workloads processing large datasets — the streaming approach keeps memory usage proportional to `batch_size`, not to the total result size.
:::

---

## The Tokio Async Runtime

**DataFusion uses Tokio as an async runtime for both I/O and CPU-bound query execution.**

Every DataFrame action (`.collect()`, `.show()`, `.execute_stream()`) is an `async` function. DataFusion is built on [Tokio], Rust's most widely used async runtime, which serves as a work-stealing thread pool for both I/O and CPU-bound work.

### Why Tokio?

DataFusion uses Tokio not just for network I/O (reading from S3, serving gRPC) but also for **CPU-bound work** like decoding Parquet, filtering rows, and computing aggregates. This might seem surprising—async is typically associated with I/O—but Tokio's work-stealing scheduler combined with Rust's zero-cost `async`/`await` makes it an excellent choice for parallelizing compute-heavy workloads.

:::{admonition} Design decision Tokio
:class: seealso

Older Tokio docs advised against using it for CPU-bound tasks, causing confusion. The actual guidance is: don't use the _same_ Runtime instance for both I/O and CPU-heavy work. DataFusion uses separate thread pools. Alternatives like [Rayon] were considered but rejected — Rayon has no async support, making I/O integration painful. See [Using Rustlang's Async Tokio Runtime for CPU-Bound Tasks] for the full rationale.
:::

### How Tokio Works Under the Hood

When you call `.collect()` or `.execute_stream()`:

1. **Partitioned Streams**:<br>
   DataFusion creates multiple async [`Stream`]s (one per partition, controlled by [`target_partitions`])
2. **Work Stealing**:<br>
   Tokio's scheduler distributes work across threads—if one thread finishes early, it "steals" work from others
3. **Cooperative Scheduling**: <br>
   Each operator yields control after processing a batch, preventing any single task from monopolizing a thread. This enables **query cancellation**—when you press Ctrl+C, DataFusion can stop gracefully because operators regularly yield control back to Tokio (see [Cooperative scheduling module])

This order is illustrated in the following diagram:

```text
┌─────────────┐           ┏━━━━━━━━━━━━━━━━━━━┓┏━━━━━━━━━━━━━━━━━━━┓
│             │thread 1   ┃     Decoding      ┃┃     Filtering     ┃
│Tokio Runtime│           ┗━━━━━━━━━━━━━━━━━━━┛┗━━━━━━━━━━━━━━━━━━━┛
│(thread pool)│thread 2   ┏━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━┓
│             │           ┃   Decoding   ┃     Filtering     ┃       ...
│             │     ...   ┗━━━━━━━━━━━━━━┻━━━━━━━━━━━━━━━━━━━┛
│             │thread N   ┏━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━┓
└─────────────┘           ┃     Decoding      ┃     Filtering     ┃
                          ┗━━━━━━━━━━━━━━━━━━━┻━━━━━━━━━━━━━━━━━━━┛
                         ────────────────────────────────────────────▶ time
```

### Tokio in practice with await

**In practice: No additional configuration needed, just add `.await`**

For most users, the async details are invisible—you `await` your DataFrame operations and DataFusion handles parallelism automatically:

```rust
use datafusion::prelude::*;
use datafusion::error::Result;

#[tokio::main]  // Creates the Tokio runtime
async fn main() -> Result<()> {
    // Operations that need .await: anything that might do I/O or parallel work
    let df = dataframe!(
        "id" => [1, 2, 3],
        "value" => [100, 200, 300]
    )?
    .filter(col("value").gt(lit(150)))?;  // Transformation: no .await (lazy)

    let results = df.collect().await?;    // Action: .await (triggers execution)

    Ok(())
}
```

**Key configuration:**

| Setting               | Purpose                        | Default             |
| --------------------- | ------------------------------ | ------------------- |
| [`target_partitions`] | Number of parallel streams     | Number of CPU cores |
| `batch_size`          | Rows processed before yielding | 8192                |

:::{admonition} Further reading
:class: seealso

- [Using Rustlang's Async Tokio Runtime for CPU-Bound Tasks] — why async works for compute
- [Using Rust async for Query Execution][async-blog] — deep dive into cooperative scheduling and query cancellation
- [Thread Scheduling documentation] — complete technical details
- [Crate Configuration](../../user-guide/crate-configuration.md) — SIMD flags, LTO, PGO, and allocator tuning for maximum performance
  :::

---

## Ownership vs. Execution: Why You See `.clone()` Everywhere

**Rust's ownership model means action methods consume the DataFrame handle — cloning gives you multiple handles to the same plan.**

:::{admonition} Rust-Specific
:class: Important
This section explains Rust ownership semantics. If you're calling DataFusion from Python or another language, these details are handled automatically.
:::

The DataFusion DataFrame-API is written in Rust, enabling Rust's ownership model with all its safety guarantees. Most action methods take `self` (not `&self`), meaning calling an action **transfers ownership** of the DataFrame handle into the method. After the call, Rust's compiler won't let you use that variable again—not because the DataFrame was mutated, but because ownership moved elsewhere. This is why you'll see `.clone()` calls throughout DataFusion code: cloning creates a second handle so you can use one and keep the other.

### What's actually happening under the hood

- A `DataFrame` is a **lightweight handle:** <br>
  Just a `LogicalPlan` + `SessionState` clone.
- **Transformations are immutable:** <br>
  Methods like `.filter()` and `.select()` return _new_ DataFrames; they don't mutate the original.
- **Actions consume the handle;** <br>
  Actions like `.collect()` take ownership of the handle, but your source data (Parquet files, tables) remains untouched (read only).
- **Cloning is cheap:** <br>
  Cloning is cheap because you're cloning reference-counted pointers, not copying data.

### Clone costs in Rust — what's cheap vs. expensive:

| Type               | Clone operation             | Cost                         | Example                  |
| ------------------ | --------------------------- | ---------------------------- | ------------------------ |
| `Arc<T>`, `Rc<T>`  | Increment reference counter | **Cheap** (single atomic op) | `DataFrame`, `SchemaRef` |
| `String`, `Vec<T>` | Allocate + copy all bytes   | **Expensive** (O(n))         | Avoid in hot paths       |
| `RecordBatch`      | Clone `Arc`-wrapped arrays  | **Cheap**                    | Arrow data sharing       |

DataFusion's `DataFrame` wraps its internals in `Arc`, so `df.clone()` is a standard Rust pattern that costs virtually nothing—clone freely when you need multiple handles to the same plan.

```rust
use datafusion::prelude::*;
use datafusion::error::Result;

#[tokio::main]
async fn main() -> Result<()> {
    let df = dataframe!(
        "id" => [1, 2, 3],
        "value" => ["a", "b", "c"]
    )?;

    // Clone the handle to use the same plan twice
    // (cheap: just incrementing Arc reference counts)
    df.clone().show().await?;   // First execution
    let count = df.count().await?;  // Second execution (re-runs the plan)

    println!("Count: {count}");

    Ok(())
}
```

:::{admonition} Re-execution note
:class: note
Each action re-runs the full plan from source data. If you need to reuse computed results across multiple actions, materialize them first with [`.cache()`] or write to storage, then run subsequent actions on the materialized output.
:::

---

## Putting It All Together

**From lazy plan to streaming results — the complete DataFrame lifecycle in action.**

We've showed **what** DataFrames are (lazy handles wrapping `LogicalPlan` + `SessionState`), **why** laziness matters (optimization before execution), and **how** actions trigger the pipeline. Now let's see the full lifecycle in one example—from building the plan, through introspection, to execution:

```rust
use datafusion::prelude::*;
use datafusion::error::Result;
use datafusion::functions_aggregate::sum::sum;
use futures::StreamExt;

#[tokio::main]
async fn main() -> Result<()> {
    // ┌─────────────────────────────────────────────────────────────────┐
    // │ LAZY PHASE: Building the LogicalPlan                           │
    // └─────────────────────────────────────────────────────────────────┘

    // Transformations chain → each returns a NEW DataFrame (immutable)
    let df = dataframe!(
        "product_id" => [1, 2, 1, 2, 3],
        "region" => ["EMEA", "EMEA", "APAC", "EMEA", "EMEA"],
        "revenue" => [100, 200, 150, 250, 300]
    )?
    .filter(col("region").eq(lit("EMEA")))?      // LogicalPlan grows
    .aggregate(vec![col("product_id")], vec![sum(col("revenue"))])?;

    // Nothing has executed yet! df is just a recipe (LogicalPlan + SessionState)

    // ┌─────────────────────────────────────────────────────────────────┐
    // │ INTROSPECTION: Peek at the plan before executing               │
    // └─────────────────────────────────────────────────────────────────┘

    // Clone because .explain() consumes the handle (Rust ownership)
    df.clone().explain(false, false)?.show().await?;

    // ┌─────────────────────────────────────────────────────────────────┐
    // │ ACTION: Cross the boundary → Optimizer → ExecutionPlan → Data  │
    // └─────────────────────────────────────────────────────────────────┘

    // Option A: Buffer everything (convenient, watch memory on large data)
    let _batches = df.clone().collect().await?;

    // Option B: Stream incrementally (memory-efficient for large results)
    let mut stream = df.execute_stream().await?;
    while let Some(batch) = stream.next().await {
        let batch = batch?;
        println!("Received {} rows", batch.num_rows());
    }

    Ok(())
}
```

**What you just saw:**

| Code                               | Concept from this section                                          |
| ---------------------------------- | ------------------------------------------------------------------ |
| `.filter().aggregate()`            | Transformations are **lazy** — build the plan, don't execute       |
| `df.clone()`                       | **Ownership** — clone the handle to use it multiple times          |
| `.explain()`                       | **Introspection** — see the plan before committing to execution    |
| `.collect()` / `.execute_stream()` | **Actions** — cross the boundary, trigger optimization + execution |

You now understand how DataFrames defer work until an action, why [`.clone()`] appears everywhere, and how to inspect plans before running them. For the complete method reference, see [Transformations](transformations.md). For hands-on query building, continue to [Creating DataFrames](creating-dataframes.md).

With the execution lifecycle understood — from lazy plan through optimization to streaming results — the next section places DataFusion in its broader historical and architectural context: [The Bigger Picture](bigger-picture.md).

---

## References

**DataFrame-API Guides:**

- [Transformations](transformations.md) — complete method reference
- [Creating DataFrames](creating-dataframes.md) — sources, registration, creation patterns
- [Writing DataFrames](writing-dataframes.md) — output formats and sinks

**Architecture & Internals:**

- [Query Optimizer guide](../query-optimizer.md) — optimization phases and rules
- [Optimizer rules (source)][optimizer-rules] — logical optimizer implementation
- [Physical optimizer rules (source)][physical-rules] — physical planning rules
- [`.explain()` usage guide][`.explain()` examples] — understanding execution plans

**Deep Dives:**

- [DataFusion paper (SIGMOD 2024)](https://dl.acm.org/doi/10.1145/3626246.3653368) — academic foundation
- [blog: Parquet Pruning] — file/row group/page skipping
- [blog: Filter Pushdown] — late materialization for row-level filtering

**API Documentation:**

- [`DataFrame`](https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html) — struct reference
- [`SessionContext`](https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html) — entry point
- [`LogicalPlan`](https://docs.rs/datafusion-expr/latest/datafusion_expr/logical_plan/enum.LogicalPlan.html) — plan structure

---
