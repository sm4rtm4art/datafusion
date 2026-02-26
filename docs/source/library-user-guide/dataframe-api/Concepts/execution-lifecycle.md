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

## Execution Model: Actions vs. Transformations

**Nothing runs until you ask for results.**

DataFusion distinguishes between **transformations** (lazy operations that build a query plan) and **actions** (eager operations that trigger execution). This separation enables whole-query optimization: the optimizer sees your entire pipeline before processing any data, applying rewrites like predicate pushdown and projection pruning. Understanding when execution actually happens—and what triggers it—is key to writing efficient queries and debugging performance issues.

```{contents} Table of Contents for Exucition Model
:local:
:depth: 2
```

---

### The DataFrame Lifecycle

The journey from "build a query" to "get results" has a clear boundary: the **action call**. Everything above is **lazy** (just building a plan); everything below happens **only when you call [`.collect()`], [`.show()`], or [`.write_*()`][`.write_parquet()`]**. The lifecycle of a Dataframe in Datafusion is shown illustative in the following:

```text
PHASE             COMPONENT                  WHAT HAPPENS
──────────────────────────────────────────────────────────────────────────────
                 ┌────────────────────┐
  CONSTRUCTION   │   SessionContext   │      Entry point, holds config
    (User)       └─────────┬──────────┘
                           ▼
                 ┌────────────────────┐
                 │     DataFrame      │      Wraps LogicalPlan + SessionState
                 └─────────┬──────────┘
                           ▼
                 ┌────────────────────┐
      LAZY       │    LogicalPlan     │      The "what" — built by transforms
   (no work)     └─────────┬──────────┘      (.filter, .select, .join, etc.)
                           │
  ═══════════════════════════════════════════ ACTION (.collect/.show/.write) ═
                           │
                           ▼
                 ┌────────────────────┐
                 │     Optimizer      │      Rewrites plan (pushdown, pruning)
                 └─────────┬──────────┘
                           ▼
                 ┌────────────────────┐
    EAGER        │   ExecutionPlan    │      The "how" — concrete algorithms
   (work!)       └─────────┬──────────┘
                           ▼
                 ┌────────────────────┐
                 │    Task Runner     │      Parallel execution (Tokio)
                 └─────────┬──────────┘
                           ▼
                 ┌────────────────────┐
                 │   RecordBatches    │      Streaming Arrow data chunks
                 └────────────────────┘
```

---

### DataFrame Method Categories

Understanding which methods are **lazy** and which trigger **eager** execution is essential—it determines when work actually happens.

| Category              | Lazy/Eager | Purpose                              | Examples                                                                        |
| --------------------- | ---------- | ------------------------------------ | ------------------------------------------------------------------------------- |
| **Transformations**   | Lazy       | Build/extend the `LogicalPlan`       | [`.select()`], [`.filter()`], [`.aggregate()`], [`.join()`], [`.with_column()`] |
| **Execution Actions** | Eager      | Trigger optimization → execution     | [`.collect()`], [`.show()`], [`.execute_stream()`], [`.count()`], [`.cache()`]  |
| **Write Actions**     | Eager      | Execute and persist results to files | [`.write_parquet()`], [`.write_csv()`], [`.write_table()`]                      |
| **Introspection**     | Lazy\*     | Inspect plan metadata                | [`.schema()`], [`.explain()`], [`.logical_plan()`], [`.into_optimized_plan()`]  |

**How to read this:**

- **Transformations** <br>
  Return a new `DataFrame` wrapping an extended `LogicalPlan`. Chain as many as you like—no data moves until you call an action.
- **Execution Actions** <br>
  Cross the **ACTION boundary** from the lifecycle diagram: they trigger the Optimizer, create an `ExecutionPlan`, and run it. Results flow back as `RecordBatch`es.
- **Write Actions** <br>
  Do the same as execution actions, but stream results to files instead of returning them to your code. _Higher computation is to expected due to I/O and disc-writing costs._
- **Introspection** (*) <br>
  Methods access plan metadata without executing. Exception: [`.explain()`] with `analyze = true` *does\* execute to gather runtime statistics.

For the complete method reference, see [Transformations](transformations.md).

---

### Ownership vs. Execution: Why You See `.clone()` Everywhere

> **Rust-Specific:** <br>
> This section explains Rust ownership semantics. If you're calling DataFusion from Python or another language, these details are handled automatically.

The DataFusion DataFrame-API is written in Rust, enabling Rust's ownership model with all its safety guarantees. Most action methods take `self` (not `&self`), meaning calling an action **transfers ownership** of the DataFrame handle into the method. After the call, Rust's compiler won't let you use that variable again—not because the DataFrame was mutated, but because ownership moved elsewhere. This is why you'll see `.clone()` calls throughout DataFusion code: cloning creates a second handle so you can use one and keep the other.

**What's actually happening:**

- A `DataFrame` is a **lightweight handle:** <br>
  Just an `Arc`-wrapped `LogicalPlan` + `SessionState` snapshot.
- **Transformations are immutable:** <br>
  Methods like `.filter()` and `.select()` return _new_ DataFrames; they don't mutate the original.
- **Actions consume the handle;** <br>
  Actions or executions like `.collect()` takes ownership of the handle, but your source data (Parquet files, tables) remains untouched (read only).
- **Cloning is cheap:** <br>
  Cloning is cheap because you're cloning reference-counted pointers, not copying data.

**Clone costs in Rust — what's cheap vs. expensive:**

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

> **Re-execution note:** Each action re-runs the full plan from source data. If you need to reuse computed results across multiple actions, materialize them first with [`.cache()`] or write to storage, then run subsequent actions on the materialized output.

---

### The Tokio Async Runtime: Understanding Tokio

**Datafusion uses Tokio as an async runtime for CPU-bound work.**

Every DataFrame action (`.collect()`, `.show()`, `.execute_stream()`) is an `async` function. But why? DataFusion is built on [Tokio], Rust's most widely used async runtime, which serves as a work-stealing thread pool for both I/O and CPU-bound work.

**Why Tokio?**

DataFusion uses Tokio not just for network I/O (reading from S3, serving gRPC) but also for **CPU-bound work** like decoding Parquet, filtering rows, and computing aggregates. This might seem surprising—async is typically associated with I/O—but Tokio's work-stealing scheduler combined with Rust's zero-cost `async`/`await` makes it an excellent choice for parallelizing compute-heavy workloads.

> **Design decision:** <br>
> Older Tokio docs advised against using it for CPU-bound tasks, causing confusion. The actual guidance is: don't use the _same_ Runtime instance for both I/O and CPU-heavy work. DataFusion uses separate thread pools. Alternatives like [Rayon] were considered but rejected—Rayon has no async support, making I/O integration painful.<br>
> See:

- [Using Rustlang's Async Tokio Runtime for CPU-Bound Tasks] for the full rationale.

**How It Works**

When you call `.collect()` or `.execute_stream()`:

1. **Partitioned Streams**:<br>
   DataFusion creates multiple async [`Stream`]s (one per partition, controlled by [`target_partitions`])
2. **Work Stealing**:<br>
   Tokio's scheduler distributes work across threads—if one thread finishes early, it "steals" work from others
3. **Cooperative Scheduling**: <br>
   Each operator yields control after processing a batch, preventing any single task from monopolizing a thread. This enables **query cancellation**—when you press Ctrl+C, DataFusion can stop gracefully because operators regularly yield control back to Tokio (see [Cooperative scheduling module])

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

> **Further reading:** <br>
>
> - [Using Rustlang's Async Tokio Runtime for CPU-Bound Tasks] — why async works for compute
> - [Using Rust async for Query Execution][async-blog] — deep dive into cooperative scheduling and query cancellation
> - [Thread Scheduling documentation] — complete technical details
> - [Crate Configuration](../../user-guide/crate-configuration.md) — SIMD flags, LTO, PGO, and allocator tuning for maximum performance

---

### What Happens During Execution?

**Datafusion the out of the box query engine, optimizes your query for a performant execution**

When you call an action like [`.collect()`], the lazy plan crosses the ACTION boundary and enters a multi-phase pipeline. What seems to be a simple filter operation to you, is followed by a series of optimizations and transformations by DataFusion's optimizer. Most of the time you don't have to care for this, since the out of the box query engine deals in most of the cases automatically in the background with the optimizers. DataFusion maintains a large set of optimizer rules—only the **applicable ones fire** based on your specific plan structure:

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

> **Memory vs. Streaming:** [`.collect()`] buffers all results in memory—convenient but risky for large datasets. Use [`.execute_stream()`] for incremental processing, or write directly to files with [`.write_parquet()`].

---

### Optimizer Architecture (For the Curious)

The `LogicalPlan` you construct via the DataFrame builder pattern is just the starting point. When you call an action, DataFusion's optimizer transforms it—often dramatically—before execution.

DataFusion uses a **pragmatic hybrid approach**:

- **Logical optimization:** <br>
  Rule-based iterative rewrites (predicate pushdown, projection pruning, etc.).
- **Physical planning:** <br>
  Statistics-informed decisions where beneficial (join algorithm selection, partition count)
- **Design philosophy:** <br>
  "Solid heuristic optimizer as default + extension points for experimentation" ([#1972](https://github.com/apache/datafusion/issues/1972))

This is **not** a Cascades-style optimizer (no memoized search over equivalence classes). Plans are deterministic for a given query structure, and while statistics are used, there's no exhaustive cost-based enumeration. This means: the same DataFrame builder chain produces the same optimized plan every time—predictable and debuggable.

> **Further reading:** <br>
> For details on DataFusion's optimizer architecture and design philosophy, see:

- [Query Optimizer guide](../query-optimizer.md)
- [DataFusion paper (SIGMOD 2024)](https://dl.acm.org/doi/10.1145/3626246.3653368).

---

### Why the Physical Plan Matters

During query development, the `ExecutionPlan` is your window into what DataFusion will actually do. The `LogicalPlan` you build describes _what_ you want—the `ExecutionPlan` reveals _how_ it happens. Inspecting the plan before running on large data catches inefficiencies early.

**Common scenarios where understanding the plan helps:**

| Scenario                 | What to look for                      | Impact                                                              |
| ------------------------ | ------------------------------------- | ------------------------------------------------------------------- |
| **Filter placement**     | Is the filter pushed before the join? | Filtering 1M→1K rows _before_ joining is orders of magnitude faster |
| **Join algorithm**       | `HashJoinExec` vs `SortMergeJoinExec` | Hash joins are faster for unsorted data; sort-merge for pre-sorted  |
| **Build side selection** | Which table builds the hash table?    | Smaller table should be the build side (less memory)                |
| **Projection pruning**   | Are unused columns eliminated early?  | Reading fewer columns = less I/O, especially for Parquet            |

**Example: Filter placement matters**

```text
Filter EARLY (optimized):       Filter LATE (naive):

┌─────────┐                     ┌─────────┐
│  Scan   │ 1M rows             │  Scan   │ 1M rows
└────┬────┘                     └────┬────┘
     ▼                               ▼
┌─────────┐                     ┌─────────┐
│ Filter  │ → 1K rows           │  Join   │ 1M × 100K rows
└────┬────┘                     └────┬────┘
     ▼                               ▼
┌─────────┐                     ┌─────────┐
│  Join   │ 1K × 100K rows      │ Filter  │ filter AFTER join
└─────────┘                     └─────────┘
```

DataFusion's optimizer usually pushes filters down automatically (predicate pushdown), but it can't always—e.g., when the filter references columns from both sides of a join. Understanding the plan helps you restructure queries when automatic optimization isn't enough.

**Use `.explain()` to inspect your plan:**

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

> **Performance tip:** <br> > [`.explain(true, false)`][`.explain()`] shows the optimized logical plan; [`.explain(true, true)`][`.explain()`] adds runtime statistics (actually runs the query). Start with the plan, profile if needed. For more details, see [`.explain()` examples].

**Data source matters:** <br>
For in-memory data (like [`dataframe!]` a datafusion macro), optimizations focus on operation order and algorithm selection. For file-based sources (Parquet, CSV), additional optimizations kick in—predicate pushdown to skip row groups, projection pushdown to read only needed columns. See [Creating DataFrames: From Files](creating-dataframes.md#1-from-files) for file-specific tuning.

**Execution-Level Optimizations** <br>

> The physical plan enables execution-level optimizations that go beyond planning. For Parquet sources, DataFusion applies:
>
> - **Pruning** — skip entire files/row groups based on statistics ([blog: Parquet Pruning])
> - **Filter pushdown with late materialization** — read filter columns first, selectively decode matching rows ([blog: Filter Pushdown])
>
> These are advanced topics for readers tuning file-based workloads.

#### References

- [Optimizer rules (source)][optimizer-rules]
- [Physical optimizer rules (source)][physical-rules]

---

### Putting It All Together

**From lazy plan to streaming results—the complete DataFrame lifecycle in action.**

We've showed _what_ DataFrames are (lazy handles wrapping `LogicalPlan` + `SessionState`), _why_ laziness matters (optimization before execution), and _how_ actions trigger the pipeline. Now let's see the full lifecycle in one example—from building the plan, through introspection, to execution:

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

**You now understand:** <br>
How DataFrames defer work until an action, why [`.clone()`] appears everywhere, and how to inspect plans before running them. For the complete method reference, see [Transformations](transformations.md). For hands-on query building, continue to [Creating DataFrames](creating-dataframes.md).

---

### References

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
