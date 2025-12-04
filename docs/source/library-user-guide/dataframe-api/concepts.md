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

# DataFrame Concepts

This guide covers the core concepts you need to understand when working with DataFusion DataFrames: SessionContext, LogicalPlan relationships, lazy evaluation, and handling null values. It should show interested, how and where the DatFrame concept fits in. As described elsewhere the DataFrame concept is mainly derived from the DataScience community, where it is nessessary and wantet do transform datastructure, for getting more insight. This concept brings several benefits conidering datastructures. One of the main driver for improving this concept is the ["Apache Arrow"] project. For a small introduction, please follow ["Arrow Introdiction"](../../user-guide/arrow-introduction.md).

> **Conceptual Foundation**: For high-level concepts about why DataFrames exist and their role in the data science ecosystem, see the [User Guide](../../user-guide/dataframe.md#how-dataframes-work-lazy-evaluation-and-arrow-output).

```{contents}
::local:
::depth: 2
```

## Introduction

To understand where DataFrames fit in DataFusion, consider the following architectural overview. This diagram is central to understanding the inner workings—how decisions you make interact with query execution.

**The architectural overview schema:**

```
                    ┌──────────────────┐
                    │  SessionContext  │
                    └────────┬─────────┘
                             │
                             │ captures state
                             ▼
                    ┌──────────────────┐
                    │   SessionState   │
                    │    (snapshot)    │
                    └────────┬─────────┘
                             │
               ┌─────────────┴─────────────┐
               │                           │
               ▼                           ▼
        ┌─────────────┐             ┌─────────────┐
        │     SQL     │             │  DataFrame  │
        │   ctx.sql   │             │  API (lazy) │
        │   (parse)   │             │   (build)   │
        └──────┬──────┘             └──────┬──────┘
               │                           │
               │ produces                  │ produces
               │                           │
               └───────────┬───────────────┘
                           ▼
               ┌───────────────────────────┐
               │        LogicalPlan        │
               └─────────────┬─────────────┘
                             │
                             │ optimize (projection/predicate pushdown, etc.)
                             ▼
               ┌───────────────────────────┐
               │   Optimized LogicalPlan   │
               └─────────────┬─────────────┘
                             │
                             │ plan (physical planner)
                             ▼
               ┌───────────────────────────┐
               │ ExecutionPlan (Physical)  │
               └─────────────┬─────────────┘
                             │
                             │ optimize (physical optimizer)
                             ▼
               ┌───────────────────────────┐
               │  Optimized ExecutionPlan  │
               └─────────────┬─────────────┘
                             │
                             │ execute (Tokio + CPU runtimes)
                             ▼
               ┌───────────────────────────┐
               │    RecordBatch streams    │
               └───────────────────────────┘
```

> **Glossary snapshot**
>
> - **`SessionContext`**: Entry point for creating DataFrames, configuring execution, and registering tables/functions.
> - **`SessionState`**: Captured snapshot of context configuration and catalog state used when executing a DataFrame.
> - **`DataFrame`**: Lazy wrapper pairing a `LogicalPlan` with a `SessionState` snapshot; transformations build plans, actions execute them.
> - **`LogicalPlan`**: Tree describing _what_ to compute (projection, filter, join, etc.).
> - **`ExecutionPlan`**: Physical operator tree describing _how_ to compute (hash aggregate, parquet scan, shuffle, etc.).
> - **`RecordBatch`**: Arrow data structure representing a chunk of rows in columnar form; execution produces streams of batches.

## Two Paths to the Same Plan: Parser vs Builder

The diagram above reveals DataFusion's most important architectural insight: **SQL and the DataFrame API are two different ways to construct the same thing—a `LogicalPlan`**.

```
┌─────────────────────────────────────────────────────────────────────┐
│                        Two Construction Paths                       │
├─────────────────────────────┬───────────────────────────────────────┤
│         SQL (Parser)        │       DataFrame API (Builder)         │
├─────────────────────────────┼───────────────────────────────────────┤
│  "SELECT a, b FROM t        │  ctx.table("t")?                      │
│   WHERE a > 10"             │     .filter(col("a").gt(lit(10)))?    │
│         │                   │     .select(vec![col("a"), col("b")])│
│         │ parse             │         │                             │
│         ▼                   │         │ build                       │
│    ┌─────────┐              │         ▼                             │
│    │  AST    │              │    (no intermediate AST)              │
│    └────┬────┘              │         │                             │
│         │ plan              │         │                             │
│         ▼                   │         ▼                             │
│  ┌─────────────┐            │  ┌─────────────┐                      │
│  │ LogicalPlan │ ═══════════╪══│ LogicalPlan │  ← Identical!        │
│  └─────────────┘            │  └─────────────┘                      │
└─────────────────────────────┴───────────────────────────────────────┘
```

### Why This Matters

Understanding this distinction is fundamental because it explains:

1. **Why both APIs exist**: They serve different use cases with different ergonomics
2. **Why mixing them is free**: Both produce the same `LogicalPlan`—no translation overhead
3. **Why optimization is holistic**: The optimizer sees one unified plan, regardless of how you built it

### Parser Path (SQL)

```rust
let df = ctx.sql("SELECT a, b FROM t WHERE a > 10").await?;
```

- **Input**: Query string
- **Process**: Lexer → Parser → AST → Logical planner → `LogicalPlan`
- **Strengths**: Familiar to SQL users, portable queries, easy to load from config files
- **Trade-offs**: Runtime parsing overhead, string-based (no compile-time checks)

### Builder Path (DataFrame API)

```rust
let df = ctx.table("t").await?
    .filter(col("a").gt(lit(10)))?
    .select(vec![col("a"), col("b")])?;
```

- **Input**: Method calls
- **Process**: Direct `LogicalPlan` node construction (no parsing)
- **Strengths**: Compile-time type checking, IDE autocomplete, programmatic composition
- **Trade-offs**: More verbose for complex queries, Rust-specific

### The Dataframe Abstraction

This architecture follows principles established in the broader data science ecosystem. As described in [Towards Scalable Dataframe Systems][dataframe-paper], dataframes provide a powerful abstraction for data manipulation that has proven successful across languages (Python pandas, R data.frame, Spark DataFrame).

DataFusion's contribution is providing **both** the programmatic builder pattern _and_ SQL parsing, unified through a common `LogicalPlan` representation. This means you can:

- Write SQL for complex analytical queries (window functions, CTEs)
- Use the DataFrame API for dynamic query construction in application code
- Mix both in a single pipeline with zero overhead

> **Key insight**: When you understand that SQL parsing and DataFrame building are just two construction paths to the same `LogicalPlan`, everything else—lazy evaluation, optimization, mixing APIs—follows naturally.

[dataframe-paper]: https://arxiv.org/abs/2001.00888

## SessionContext: The Entry Point for DataFrames

```
SessionContext   <== You are here
  ↓ creates
DataFrame
  ↓ builds
LogicalPlan
  ↓ optimizes & plans
ExecutionPlan
  ↓ executes
RecordBatches
```

The [`SessionContext`] is the main interface for executing queries with DataFusion. It maintains the state of the connection between a user and an instance of the DataFusion engine and is the place where every DataFrame journey starts.

**Common ways to produce a `DataFrame`:**

1. **Read data sources** – call methods such as [`read_parquet()`], [`read_csv()`], or [`read_json()`] to **scan\*** files or object stores. (\* scan => lazy execution!)
2. **Query registered tables** – register data with [`register_table()`], [`register_parquet()`], etc., then fetch it with [`table()`].
3. **Run SQL** – issue SQL statements with [`sql()`] to obtain the results as a DataFrame, mixing SQL and the programmatic API freely.
4. **Inline or in-memory data** – register in-memory `MemTable` instances built from Arrow `RecordBatch`es for tests and quick experiments.

> **Learn more:** The [Creating DataFrames](creating-dataframes.md) guide provides end-to-end examples of each approach and when to choose them.

### SessionContext Method Categories

Like DataFrame, SessionContext exposes a large API surface that becomes easier to navigate once you understand the main categories:

| Category               | Purpose                                | Examples                                                         |
| ---------------------- | -------------------------------------- | ---------------------------------------------------------------- |
| **DataFrame Creation** | Create DataFrames from various sources | `read_parquet()`, `read_csv()`, `sql()`, `table()`               |
| **Table Management**   | Register and manage tables by name     | `register_table()`, `register_parquet()`, `deregister_table()`   |
| **Configuration**      | Control execution behavior             | `with_config()`, `state()`                                       |
| **Catalog Operations** | Manage schemas and databases           | `catalog()`, `catalog_names()`                                   |
| **Extensions**         | Add custom functionality               | `register_udf()`, `register_udaf()`, `register_table_provider()` |

> **Learn more about creation methods**: The table above shows the API surface—for complete examples of each pattern, see [Creating DataFrames](creating-dataframes.md).

### Creating and Configuring SessionContext

The `SessionContext` is your starting point. You can use defaults or tune it for your workload:

```rust
use datafusion::prelude::*;
use datafusion::execution::config::SessionConfig;

// Default context (good for getting started)
let ctx = SessionContext::new();

// Customized context for performance tuning
let config = SessionConfig::new()
    .with_batch_size(8192)               // Rows per batch
    .with_target_partitions(num_cpus::get());  // Parallelism
let ctx = SessionContext::with_config(config);
```

Once you have a `SessionContext`, you can create DataFrames, register tables, and execute queries. The context maintains all state (configuration, catalogs, registered tables/UDFs) that DataFrames need during execution.

> **Configuration tuning**: Batch size and partitions significantly affect performance. See [Best Practices](best-practices.md) and [Creating DataFrames § Configuration Impact](creating-dataframes.md#configuration-impact-on-dataframe-creation) for detailed guidance.

### Core Design Principles

Every DataFrame operation follows these foundational principles:

1.  **Lazy evaluation**: Transformations build up a query plan that executes only when you call an action method like [`.collect()`] or [`.show()`]. This allows the entire query to be optimized holistically before touching any data.
2.  **Immutable transformations**: DataFrame methods return new DataFrames, leaving the original unchanged. This functional style makes pipelines easier to reason about and compose.
3.  **Arrow-native data model**: Data is represented in memory using Apache Arrow's columnar format. This enables efficient vectorized processing and zero-copy data sharing with other Arrow-based systems.

## Data Model & Schema

```
SessionContext
  ↓ creates
DataFrame
  ↓ builds
LogicalPlan    <= Schema (types + nullability) defined here
  ↓ optimizes & plans
ExecutionPlan  <= Null handling semantics implemented here
  ↓ executes
RecordBatches  <= Actual null values (bitmaps) stored here
```

DataFrames work with structured, typed data following Apache Arrow's columnar model. Understanding schemas and null handling is crucial for effective DataFrame usage:

- **Schema** (field names, types, nullability) is part of the LogicalPlan
- **Null semantics** (three-valued logic, aggregation rules) are implemented in ExecutionPlan operators
- **Null values** themselves are stored as bitmaps in Arrow RecordBatches

### Schemas and Data Types

A DataFrame's schema is the schema of its underlying [`LogicalPlan`]. Sources (files, tables, SQL) provide or infer a base schema, and each transformation (e.g., `.select()`, `.with_column()`, `.aggregate()`, `.join()`) derives a new output schema from its inputs and expressions. The [`.schema()`] method simply exposes the plan’s current schema (names, order, [`DataType`], and nullability).

- Base schemas come from data sources (e.g., Parquet metadata, explicit CSV schema, MemTable schema)
- Transformations derive new schemas (rename, add/drop columns, expression output types, aggregation output)
- Schema validation drives type coercion, pushdown, and operator selection during planning

```rust
use datafusion::prelude::*;
use datafusion::error::Result;

#[tokio::main]
async fn main() -> Result<()> {
    let ctx = SessionContext::new();
    let df = ctx.read_parquet("tests/data/alltypes_plain.parquet", ParquetReadOptions::default()).await?;
    let schema = df.schema();
    for field in schema.fields() {
        println!("{}: {:?} (nullable: {})", field.name(), field.data_type(), field.is_nullable());
    }
    Ok(())
}
```

> **Reference:** For a deeper tour of Arrow schemas and RecordBatches, see [Introduction to Arrow & RecordBatches](../../user-guide/arrow-introduction.md). For SQL type compatibility and coercion rules, see [SQL Data Types](../../user-guide/sql/data_types.md).

### Handling Null Values

Arrow represents [nulls via a bitmap], so DataFusion handles null values consistently following SQL standards. Understanding null behavior is essential for correct queries.

> **New to SQL NULL semantics?** If you're coming from other programming languages, SQL's NULL behavior may surprise you. Unlike `null` in most languages (which is just a special value), SQL's `NULL` represents _"unknown"_ and propagates through expressions in non-intuitive ways. We recommend reading the [PruningPredicate Boolean Tri-state logic section] for detailed truth tables and examples.

**Three-Valued Logic & Filters**

SQL uses three-valued logic: expressions can be `TRUE`, `FALSE`, or `NULL` (unknown).

- Comparisons with `NULL` return `NULL` (unknown), not true or false
  - `5 > NULL` → `NULL` (we don't know)
  - `NULL = NULL` → `NULL` (even comparing NULL to itself is unknown!)
- `WHERE` filters keep **only** rows that evaluate to `TRUE` — rows that are `NULL` or `FALSE` are filtered out
- Example: `WHERE age > 18` excludes both rows where `age ≤ 18` AND rows where `age IS NULL`

```rust
use datafusion::prelude::*;
use datafusion::error::Result;

#[tokio::main]
async fn main() -> Result<()> {
    let ctx = SessionContext::new();

    // Sample data with NULL values
    // +-------+------+
    // | name  | age  |
    // +-------+------+
    // | Alice | 25   |
    // | Bob   | 17   |
    // | Carol | NULL |
    // +-------+------+
    let df = ctx.read_csv("people.csv", CsvReadOptions::default()).await?;

    // Query 1: Standard filter (excludes NULLs)
    let adults = df.clone().filter(col("age").gt(lit(18)))?;
    // Result: Only Alice (age=25)
    // Bob filtered out: 17 > 18 is FALSE
    // Carol filtered out: NULL > 18 is NULL (treated as FALSE by WHERE)

    // Query 2: Explicitly include NULLs
    let adults_or_unknown = df.filter(
        col("age").gt(lit(18)).or(col("age").is_null())
    )?;
    // Result: Alice (age=25) AND Carol (age=NULL)
    // Bob still filtered out: (17 > 18 OR 17 IS NULL) is FALSE

    Ok(())
}
```

**Conditional Helpers**
DataFusion provides several tools to handle nulls explicitly:

- [`is_null()`] and `is_not_null()` expressions test for null values
- [`coalesce()`] returns the first non-null argument: `coalesce(col("price"), lit(0))`
- [`nullif()`] returns null if two values are equal
- `nvl(expr, default)` and `ifnull(expr, default)` provide default values for nulls
- [`.fill_null()`] DataFrame method replaces nulls in all columns

**Aggregations**

- Functions like `sum`, `avg`, `min`, `max` skip null inputs entirely
- `count(*)` counts all rows; `count(column)` counts only non-null values
- `count_distinct` also ignores nulls by default

**Joins**

- Standard joins treat `NULL = NULL` as `FALSE` (nulls don't match)
- Use `IS NOT DISTINCT FROM` for null-safe equality (treats `NULL = NULL` as `TRUE`)
- See [`datafusion::common::NullEquality`] for join null-handling modes

**Sorting**

- Default behavior (following PostgreSQL): `ORDER BY col ASC` places nulls **last**
- Control null placement with the `nulls_first` parameter:
  - `col("score").sort(false, false)` // DESC NULLS LAST
  - `col("score").sort(false, true)` // DESC NULLS FIRST
- See [`DataFrame::sort()`] and [configuration] for customization

```rust
use datafusion::prelude::*;

// Replace nulls with defaults
let cleaned = df
    .with_column("revenue", coalesce(vec![col("revenue"), lit(0)]))?
    .filter(col("country").is_not_null())?;

// Null-safe join (IS NOT DISTINCT FROM)
use datafusion::common::NullEquality;
let result = left.join_detailed(
    right,
    JoinType::Inner,
    (vec!["id"], vec!["id"]),
    None,
    NullEquality::NullEqualsNull, // NULL = NULL is TRUE
)?;
```

> **References**:
>
> - Three-valued logic: [`PruningPredicate`] documentation (see "Boolean Tri-state logic" section)
> - Aggregation null handling: [`GroupsAccumulator::accumulate`]
> - Join null semantics: [`datafusion::common::NullEquality`]
> - Sort behavior: [`DataFrame::sort()`], [PostgreSQL ORDER BY rules](https://www.postgresql.org/docs/current/queries-order.html)
> - Practical patterns: [Transformations guide](transformations.md#dataframe-transformations)

[PruningPredicate Boolean Tri-state logic section]: https://docs.rs/datafusion-pruning/latest/datafusion_pruning/struct.PruningPredicate.html#background
[nulls via a bitmap]: https://arrow.apache.org/docs/format/Columnar.html#validity-bitmaps
[`PruningPredicate`]: https://docs.rs/datafusion-pruning/latest/datafusion_pruning/struct.PruningPredicate.html
[`datafusion::common::NullEquality`]: https://docs.rs/datafusion-common/latest/datafusion_common/enum.NullEquality.html
[`GroupsAccumulator::accumulate`]: https://docs.rs/datafusion-functions-aggregate-common/latest/datafusion_functions_aggregate_common/aggregate/groups_accumulator/fn.accumulate.html
[`DataFrame::sort()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.sort
[configuration]: ../../user-guide/configs.md#default-null-ordering

## Execution Model: Actions vs. Transformations

### The DataFrame Lifecycle

```
SessionContext
  ↓ creates
DataFrame       ← Lazy: holds LogicalPlan, no execution yet
  ↓ transforms (select, filter, join...)
LogicalPlan     ← Abstract query representation
  ↓ on action (collect, show, write...)
Optimizer       ← Rewrites plan (predicate pushdown, projection pruning, etc.)
  ↓
Physical Planner ← Converts to ExecutionPlan with concrete algorithms
  ↓
ExecutionPlan   ← Physical operators (HashJoin, ParquetExec, etc.)
  ↓ executes
RecordBatches   ← Streaming Arrow data chunks
```

DataFrames follow a **lazy execution model**: transformations build up a query plan without processing data, while actions trigger optimization and execution.

### DataFrame Method Categories

`DataFrame` methods fall into four categories:

| Category              | Purpose                              | Examples                                                                        | SQL Analogy                   |
| --------------------- | ------------------------------------ | ------------------------------------------------------------------------------- | ----------------------------- |
| **Transformations**   | Build/modify the logical plan (lazy) | [`.select()`], [`.filter()`], [`.aggregate()`], [`.join()`], [`.with_column()`] | `SELECT`, `WHERE`, `GROUP BY` |
| **Execution Actions** | Trigger execution and return data    | [`.collect()`], [`.show()`], [`.execute_stream()`], [`.count()`]                | Running the query             |
| **Write Actions**     | Execute and persist results          | [`.write_parquet()`], [`.write_csv()`], [`.write_table()`]                      | `CREATE TABLE AS`, `COPY TO`  |
| **Introspection**     | Inspect without executing            | [`.schema()`], [`.explain()`], [`.logical_plan()`], [`.into_optimized_plan()`]  | `EXPLAIN`, metadata queries   |

### What Happens During Execution?

When you call an action like `.collect()`:

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

3. **Execution**:
   - Stream data through operators in chunks (RecordBatches)
   - Execute operators in parallel when possible
   - Spill to disk if memory limits exceeded
   - Collect statistics for adaptive optimization

> **Memory vs. Streaming**: `.collect()` buffers all results in memory—convenient but risky for large datasets. Use `.execute_stream()` for incremental processing or write directly to files.

### The Async Runtime: Understanding Tokio

Every DataFrame action (`.collect()`, `.show()`, `.execute_stream()`) is an `async` function. But why? DataFusion is built on [Tokio], Rust's most widely used async runtime, which serves as a work-stealing thread pool for both I/O and CPU-bound work.

**Why Tokio?**

DataFusion uses Tokio not just for network I/O (reading from S3, serving gRPC) but also for **CPU-bound work** like decoding Parquet, filtering rows, and computing aggregates. This might seem surprising—async is typically associated with I/O—but Tokio's work-stealing scheduler combined with Rust's zero-cost `async`/`await` makes it an excellent choice for parallelizing compute-heavy workloads. For a deeper dive, see [Using Rustlang's Async Tokio Runtime for CPU-Bound Tasks].

**How It Works**

When you call `.collect()` or `.execute_stream()`:

1. **Partitioned Streams**: DataFusion creates multiple async [`Stream`]s (one per partition, controlled by [`target_partitions`])
2. **Work Stealing**: Tokio's scheduler distributes work across threads—if one thread finishes early, it "steals" work from others
3. **Cooperative Scheduling**: Each operator yields control after processing a batch, preventing any single task from monopolizing a thread (see [`coop` module])

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

**Practical Implications**

For most users, the async details are invisible—you just `await` your DataFrame operations and DataFusion handles parallelism automatically:

```rust
#[tokio::main]  // Creates the Tokio runtime
async fn main() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();

    // All these operations are async—they may do I/O or spawn parallel work
    let df = ctx.read_parquet("data.parquet", ParquetReadOptions::default()).await?;
    let results = df.filter(col("id").gt(lit(100)))?.collect().await?;

    Ok(())
}
```

**Advanced: Separating I/O and CPU Runtimes**

For production systems with latency-sensitive I/O (e.g., serving queries while reading from S3), mixing I/O and CPU work on the same runtime can cause issues—CPU-heavy decoding can delay network packet processing, triggering TCP congestion control and throttling.

The solution is using separate Tokio runtimes:

- **I/O Runtime**: Handles network requests (object store reads, gRPC serving)
- **CPU Runtime**: Handles compute-heavy work (decoding, filtering, aggregating)

```text
┌─────────────┐    ┌─────────────┐
│ IO Runtime  │    │ CPU Runtime │
│ (network)   │───▶│ (compute)   │───▶ Results
└─────────────┘    └─────────────┘
```

> **Example**: See [`thread_pools.rs`] for a complete example of running DataFusion with separate I/O and CPU runtimes, including how to configure `ObjectStore` to use a specific runtime.

**Key Configuration**

| Setting               | Purpose                            | Default             |
| --------------------- | ---------------------------------- | ------------------- |
| [`target_partitions`] | Number of parallel streams/threads | Number of CPU cores |
| Batch size            | Rows processed before yielding     | 8192                |

> **Deep Dive**: For the complete technical details including ASCII diagrams of thread scheduling, see the [Thread Scheduling documentation] in the main crate docs.

[Tokio]: https://tokio.rs
[`Stream`]: https://docs.rs/futures/latest/futures/stream/trait.Stream.html
[`target_partitions`]: ../../user-guide/configs.md#target_partitions
[`coop` module]: https://docs.rs/datafusion-physical-plan/latest/datafusion_physical_plan/coop/index.html
[`thread_pools.rs`]: https://github.com/apache/datafusion/blob/main/datafusion-examples/examples/thread_pools.rs
[Thread Scheduling documentation]: https://docs.rs/datafusion/latest/datafusion/index.html#thread-scheduling-cpu--io-thread-pools-and-tokio-runtimes
[Using Rustlang's Async Tokio Runtime for CPU-Bound Tasks]: https://thenewstack.io/using-rustlangs-async-tokio-runtime-for-cpu-bound-tasks/

### Optimizer architecture note

DataFusion uses a **pragmatic hybrid approach**:

- **Logical optimization**: Rule-based iterative rewrites (21+ rules like predicate pushdown, projection pruning)
- **Physical planning**: Statistics-informed decisions where beneficial (e.g., join algorithm selection, partition count)
- **Design philosophy**: "Solid heuristic optimizer as default + extension points for experimentation" (from [#1972](https://github.com/apache/datafusion/issues/1972))

This is **not** a Cascades-style optimizer (no memoized search over equivalence classes). Plans are deterministic for a given query structure, and while statistics are used, there's no exhaustive cost-based enumeration.

### Historical context—different optimizer approaches

- **Volcano/Cascades** (1990s): Exhaustive cost-based search with memoization ([Graefe 1993](https://15799.courses.cs.cmu.edu/spring2025/papers/04-volcano/graefe-icde1993.pdf), [1995](https://15721.courses.cs.cmu.edu/spring2016/papers/graefe-ieee1995.pdf))
- **Rule-based systems** (2000s): Pattern-matching rewrites without cost models (Spark Catalyst, early DataFusion)
- **E-graphs** (2020s): Equality saturation—explore all rewrites simultaneously then extract optimal ([egg](https://egraphs-good.github.io/))
- **Multi-level IRs (MLIR)**: Compiler infrastructure for layered IRs; recent work proposes open IRs and "optimization-as-passes" to enable cross-domain optimization and lower compilation latency ([Lattner et al. 2021](https://rcs.uwaterloo.ca/~ali/cs842-s23/papers/mlir.pdf), [PVLDB 2022](https://db.in.tum.de/~jungmair/papers/p2485-jungmair.pdf))

> **Positioning ("LLVM of databases")**: This analogy refers to DataFusion's modular, embeddable architecture and extensibility (types, operators, planner/optimizer/execution), not to using LLVM IR/JIT. Today, execution is vectorized over Arrow and the optimizer is hybrid (rule-based logical rewrites + statistics-informed physical planning). DataFusion serves as a reusable backend in systems such as InfluxDB 3.0; comparable systems like DuckDB (vectorized execution) and Apache Spark (Catalyst rule-based optimizer) illustrate the broader design space. See the [DataFusion paper (SIGMOD 2024)](https://dl.acm.org/doi/10.1145/3626246.3653368) and the [Query Optimizer guide](https://datafusion.apache.org/library-user-guide/query-optimizer.html). Ongoing discussions: [#1972](https://github.com/apache/datafusion/issues/1972), [#15878](https://github.com/apache/datafusion/issues/15878).

### Performance Example: Why the Physical Plan Matters

The optimizer makes critical performance decisions when translating your [`LogicalPlan`] to an [`ExecutionPlan`]. Consider this simple filter:

```rust
let df = ctx.read_parquet("sales.parquet", ParquetReadOptions::default()).await?
    .filter(col("region").eq(lit("EMEA")))?;
```

**The optimizer can choose different physical strategies:**

- **Efficient ExecutionPlan**: Filter pushed to Parquet reader—only _EMEA_ row groups are read (minimizes I/O)
- **Naive ExecutionPlan**: Read entire file, then filter in memory (slow for large files)

DataFusion chooses the efficient plan automatically, but you can inspect what it chose using [`.explain()`]:

```rust
df.explain(false, false)?.show().await?;
```

The output shows chosen algorithms ([`HashJoinExec`], [`SortMergeJoinExec`]), operation order, and applied optimizations. Understanding the physical plan is essential for diagnosing bottlenecks.

> **Performance tip**: Use `.explain()` to verify that filters are pushed down, unused columns are pruned, and efficient join algorithms are selected. See [Creation-Time Optimizations](creating-dataframes.md#creation-time-optimizations) for tuning techniques.

**References:**

- [Optimizer rules (source)](https://github.com/apache/datafusion/blob/main/datafusion/optimizer/src/optimizer.rs#L230-L257)
- [Physical optimizer rules (source)](https://github.com/apache/datafusion/blob/main/datafusion/physical-optimizer/src/optimizer.rs#L86-L134)
- [Parquet pruning deep-dive](https://datafusion.apache.org/blog/2025/03/20/parquet-pruning/)

### Complete Example: DataFrame Lifecycle

Context: The example below demonstrates the full lifecycle: build a lazy plan, inspect it with `.explain()`, then either buffer everything with `.collect()` or stream `RecordBatch`es with `.execute_stream()`. This mirrors the lifecycle described above (logical plan → optimization → physical plan → execution).

```rust
use datafusion::prelude::*;
use datafusion::error::Result;
use futures::StreamExt;

#[tokio::main]
async fn main() -> Result<()> {
    let ctx = SessionContext::new();

    let df = ctx
        .read_parquet("sales_2024.parquet", ParquetReadOptions::default()).await?
        .filter(col("region").eq(lit("EMEA")))?
        .aggregate(vec![col("product_id")], vec![sum(col("revenue"))])?;

    // Nothing has executed yet. Inspect the plan if desired.
    df.clone().explain(false, false)?.show().await?;

    // Trigger execution and buffer everything in memory (convenient, but be mindful of size).
    let _batches = df.clone().collect().await?;

    // Or stream RecordBatches incrementally.
    let mut stream = df.execute_stream().await?;
    while let Some(batch) = stream.next().await {
        let batch = batch?;
        println!("{} rows", batch.num_rows());
    }
    Ok(())
}
```

> **Mind the trade-offs:** `.collect()` is convenient but buffers the entire result set. Prefer streaming or writing to sinks for large outputs.

See also:

- Query Optimizer overview: ../query-optimizer.md
- DataFrame methods: [`.collect()`], [`.execute_stream()`], [`.explain()`]
- Parquet pruning background (projection, stats, page pruning): [Parquet Pruning]

[Parquet Pruning]: https://datafusion.apache.org/blog/2025/03/20/parquet-pruning/

## Relationship between [`LogicalPlan`]s and `DataFrame`s

A DataFusion `DataFrame` is a lazy recipe for a computation. Both the SQL API ([`sql()`]) and the DataFrame API compile to the same underlying [`LogicalPlan`] representation and are optimized/executed identically. To understand how this works, consider these three components:

At a high level:

- **[`SessionContext`]** — mutable session environment (catalog, UDFs, configuration)
- **[`LogicalPlan`]** — immutable description of what to compute
- **[`DataFrame`]** — [`LogicalPlan`] + [`SessionState`] snapshot at creation

A `DataFrame` consists of two immutable parts:

- A **[`LogicalPlan`]** describing what to compute
- A **[`SessionState`] snapshot** capturing the session configuration and resources at creation time

The [`SessionContext`] is mutable, but each `DataFrame` holds a frozen view of it. Transformations like `.select()`, `.filter()`, and `.join()` return a new `DataFrame` with a new [`LogicalPlan`] and the same [`SessionState`] snapshot. Actions such as `.collect()`, `.show()`, or `.create_physical_plan()` materialize the computation; until then, nothing executes.

> Quick analogy: [`SessionContext`] = kitchen (mutable environment), [`LogicalPlan`] = recipe (immutable), [`DataFrame`] = recipe + kitchen snapshot. The diagram below uses this terminology.

### The DataFrame Lifecycle: Step by Step

```
[ Step 1 · The Kitchen (mutable) ]
┌──────────────────────────────────────────────┐
│ SessionContext                               │
│  • Config: target_partitions=8, batch_size=8192 │
│  • UDFs: "my_custom_func"                    │
│  • Catalog: table "sales"                    │
└──────────────────────────────────────────────┘
              │
              │ .read_table("sales")
              ▼

[ Step 2 · The Snapshot (immutable) ]
┌──────────────────────────────────────────────┐
│ DataFrame                                    │
│  ├─ SessionState (snapshot of the kitchen)   │
│  │   • Config: target_partitions=8, batch_size=8192 │
│  │   • UDFs: "my_custom_func"               │
│  │   • Catalog: includes table "sales"      │
│  └─ LogicalPlan                              │
│      • TableScan("sales")                   │
└──────────────────────────────────────────────┘
              │
              │ .filter(col("amount").gt(100))
              ▼

[ Step 3 · Transformation (recipe changes; snapshot doesn't) ]
┌──────────────────────────────────────────────┐
│ New DataFrame                                │
│  ├─ SessionState (same immutable snapshot)   │
│  │   • Config/UDFs/Catalog unchanged         │
│  └─ LogicalPlan (new immutable plan)         │
│      • Filter(amount > 100)                  │
│         └─ TableScan("sales")               │
└──────────────────────────────────────────────┘
              │
              │ .collect() or .show()  (action)
              ▼

[ Step 4 · Execution ]
┌──────────────────────────────────────────────┐
│ ExecutionPlan (created from frozen SessionState) │
│  → Runs with original config/resources        │
│  → Produces RecordBatches                     │
└──────────────────────────────────────────────┘
```

**Key API paths:**

```
DataFrame ↔ into_parts() ↔ (SessionState, LogicalPlan)
DataFrame → into_optimized_plan() → Optimized LogicalPlan
DataFrame → create_physical_plan() → ExecutionPlan
```

### Why This Matters: The Guarantees of the Snapshot

This design provides critical guarantees for **predictable and reproducible queries**:

**1. Configuration Reproducibility**

- Batch size, target partitions, timezone, and optimizer settings are frozen at DataFrame creation.
- Your DataFrame will execute with the same performance characteristics even if you change global [`SessionContext`] settings later.

**2. Resource Safety**

- Catalogs, schemas, registered tables, and UDFs available at creation time are preserved.
- Prevents errors where a query fails because a table or UDF it depends on was deregistered after the DataFrame was built.

**3. Point-in-Time Consistency**

- The query execution start timestamp is captured in the snapshot.
- Functions like [`.now()`] return the **same value** every time you execute the DataFrame, based on the snapshot time, not wall-clock time.

**Practical implications:**

- **Reproducibility**: Re-running a DataFrame later happens in the exact same context—critical for debugging and testing.
- **Safety**: You can safely modify the [`SessionContext`] (e.g., to prepare for a different query) without breaking existing DataFrame objects.
- **Best practice**: Always register UDFs and tables **before** creating the DataFrames that rely on them.

> **Learn more:** See [SessionContext and SessionState relationship][SessionContext and SessionState] for implementation details.

[SessionContext and SessionState]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#relationship-between-sessioncontext-sessionstate-and-taskcontext

### Converting Between `DataFrame` and `LogicalPlan`

While the DataFrame API covers most use cases, you may need direct [`LogicalPlan`] access for custom optimizer rules, query rewriting systems, or programmatic plan inspection/modification.

**When to use each:**

| **DataFrame API**                    | **LogicalPlan directly**                 |
| ------------------------------------ | ---------------------------------------- |
| Standard queries and transformations | Custom optimizer rules                   |
| Automatic SessionState management    | Fine-grained control over plan structure |
| Rapid prototyping                    | Query rewriting systems                  |

Extract and modify plans using [`.into_parts()`]:

```rust
use datafusion::prelude::*;
use datafusion::logical_expr::LogicalPlanBuilder;

let ctx = SessionContext::new();
let df = ctx.read_csv("data.csv", CsvReadOptions::new()).await?;

// Extract state + plan, modify, rebuild DataFrame
let (state, plan) = df.into_parts();
let modified_plan = LogicalPlanBuilder::from(plan)
    .filter(col("a").gt(lit(5)))?
    .build()?;
let new_df = DataFrame::new(state, modified_plan);
```

### DataFrame and LogicalPlanBuilder Equivalence

[`DataFrame`] methods and [`LogicalPlanBuilder`] produce identical plans:

```rust
// These produce the same LogicalPlan:
let df = ctx.read_csv("data.csv", CsvReadOptions::new()).await?
    .select(vec![col("a"), col("b")])?;

let builder_plan = LogicalPlanBuilder::from(base_plan)
    .project(vec![col("a"), col("b")])?.build()?;

// assert_eq!(plan_from_df, builder_plan);  ✅ Identical
```

> See [Building Logical Plans](../building-logical-plans.md) for advanced [`LogicalPlanBuilder`] usage.

### Mixing SQL and DataFrames for Powerful Workflows

One of DataFusion's most powerful features is the ability to seamlessly combine the programmatic `DataFrame` API with the declarative power of SQL. Because both APIs lower to the same [`LogicalPlan`] and are optimized/executed identically, this hybrid approach gives you zero-cost interop while leveraging each API's strengths in a single, unified workflow.

**The Best of Both Worlds:**

- **Use the DataFrame API** for tasks that benefit from programmatic control, such as:

  - Dynamically constructing queries with conditional logic (`if/else`)
  - Integrating with other Rust functions and libraries
  - Step-by-step data preparation and cleaning

- **Use SQL** for complex analytical queries that are often more readable and familiar, such as:
  - Aggregations (`GROUP BY`), window functions (`OVER(...)`), and complex joins
  - Queries that are loaded from configuration files or provided by non-developer users

A common and highly effective pattern is to perform initial data preparation with the `DataFrame` API, and then register the result as a temporary view to be consumed by a final SQL query.

```rust
use datafusion::prelude::*;

let ctx = SessionContext::new();

// 1. Prepare data programmatically with the DataFrame API
let sales = ctx.read_parquet("sales.parquet", ParquetReadOptions::default()).await?
    .filter(col("region").eq(lit("EMEA")))?;

// 2. Register the DataFrame as a temporary view.
// .into_view() consumes 'sales'; use .clone() if needed later (cheap Arc clone)
ctx.register_table("sales_emea", sales.into_view())?;

// 3. Use SQL for the final, complex analytical query.
let top_products = ctx.sql(
    "SELECT product_id, SUM(revenue) AS rev
     FROM sales_emea
     GROUP BY product_id
     ORDER BY rev DESC
     LIMIT 10"
).await?;

top_products.show().await?;
```

**Zero-Cost Abstraction:**

Because both the `DataFrame` API and the SQL engine produce a [`LogicalPlan`] under the hood, this pattern is a **zero-cost abstraction**. Before execution, the DataFusion optimizer performs holistic, end-to-end optimization across the entire workflow, regardless of how it was constructed. The final [`ExecutionPlan`] will be just as efficient as if you had written the entire query in a single API.

> Use [`.into_view()`] to compose multi-step pipelines across SQL and DataFrame APIs.

## Summary: The Big Picture

The DataFusion DataFrame is more than just a table—it's a powerful recipe for computation. By understanding its core principles, you can build complex, efficient, and predictable data pipelines:

- **Stay lazy & immutable** – build a [`LogicalPlan`] first; nothing executes until an action
- **Execute reproducibly** – every DataFrame carries its own [`SessionState`] snapshot
- **Mix APIs freely** – SQL and DataFrame compile to the _same_ [`LogicalPlan`], so interop is zero-cost

Together these properties let you write declarative SQL for clarity, drop to Rust for control, and still get one optimized execution pipeline.

### Where to Go Next

With these concepts understood, you're ready to build data pipelines:

1. **[Create DataFrames](creating-dataframes.md)** – load Parquet, CSV, in-memory data
2. **[Transform DataFrames](transformations.md)** – select, filter, aggregate, join
3. **[Write / Execute](writing-dataframes.md)** – collect, stream, or persist results

### Advanced Reference: API Cheat-Sheet

For experienced users, this quick reference helps you pick the right method for advanced tasks:

| Goal                         | Primary API(s)                              | Keeps SessionState? | Typical Follow-up                                   |
| ---------------------------- | ------------------------------------------- | :-----------------: | --------------------------------------------------- |
| **Re-use plan later**        | [`.into_parts()`]                           |         ✅          | mutate plan → [`.create_physical_plan()`] → execute |
| **Inspect optimizer output** | [`.explain()`], [`.into_optimized_plan()`]  |         ⚠️          | check pushdown/pruning, join choice                 |
| **Inspect unoptimized plan** | [`.into_unoptimized_plan()`]                |         ⚠️          | verify pre-optimization structure                   |
| **Mix SQL & DataFrame**      | [`.into_view()`] + [`.sql()`]               |         ✅          | iterate between SQL & DF, then [`.collect()`]       |
| **Stream large result**      | [`.execute_stream()`], [`.write_parquet()`] |         ✅          | pipe to Parquet/CSV, Kafka, etc.                    |
| **Quick interactive result** | [`.collect()`], [`.show()`]                 |         ✅          | debug, notebooks, CLI                               |

> **SessionState matters**: Methods marked ⚠️ drop the snapshot. They're great for inspection, but to execute later use [`.into_parts()`] to preserve deterministic semantics (timestamps, timezone, config, UDF catalog). See "Re-use plan later" in the cheat-sheet for the safest way to extract and modify a plan.

## References

- Internal guides

  - [Using the DataFrame API](../using-the-dataframe-api.md)
  - [Creating DataFrames](creating-dataframes.md)
  - [Transformations](transformations.md)
  - [Writing DataFrames](writing-dataframes.md)
  - [Best Practices](best-practices.md)
  - [Building Logical Plans](../building-logical-plans.md)
  - [Arrow Introduction](../../user-guide/arrow-introduction.md)
  - [SQL Data Types](../../user-guide/sql/data_types.md)
  - [Scalar Functions](../../user-guide/sql/scalar_functions.md)
  - [How DataFrames Work](../../user-guide/dataframe.md#how-dataframes-work-lazy-evaluation-and-arrow-output)

- API docs

  - [`SessionContext` (datafusion)](https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html)
  - [`SessionState` (datafusion)](https://docs.rs/datafusion/latest/datafusion/execution/session_state/struct.SessionState.html)
  - [`DataFrame` (datafusion)](https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html)
  - [`LogicalPlan` (datafusion-expr)](https://docs.rs/datafusion-expr/latest/datafusion_expr/logical_plan/enum.LogicalPlan.html)
  - [`LogicalPlanBuilder` (datafusion-expr)](https://docs.rs/datafusion-expr/latest/datafusion_expr/logical_plan/builder/struct.LogicalPlanBuilder.html)
  - [`ExecutionPlan` (datafusion)](https://docs.rs/datafusion/latest/datafusion/physical_plan/trait.ExecutionPlan.html)
  - [`TableProvider` (datafusion)](https://docs.rs/datafusion/latest/datafusion/datasource/trait.TableProvider.html)

- External

  - Apache Arrow DataFusion (SIGMOD 2024): [Apache Arrow DataFusion: A Fast, Embeddable, Modular Analytic Query Engine](https://dl.acm.org/doi/10.1145/3626246.3653368)
  - How Query Engines Work — DataFrames: https://howqueryengineswork.com/06-dataframe.html

- Functions mentioned in this guide
  - SessionContext
    - [`with_config`](https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.with_config)
    - [`state`](https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.state)
    - [`read_parquet`](https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.read_parquet)
    - [`read_csv`](https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.read_csv)
    - [`read_json`](https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.read_json)
    - [`sql`](https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.sql)
    - [`table`](https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.table)
    - [`register_table`](https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.register_table)
    - [`register_parquet`](https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.register_parquet)
    - [`register_udf`](https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.register_udf)
    - [`register_udaf`](https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.register_udaf)
    - [`register_table_provider`](https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.register_table_provider)
    - [`catalog`](https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.catalog)
    - [`catalog_names`](https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.catalog_names)
  - DataFrame
    - [`select`](https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.select)
    - [`filter`](https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.filter)
    - [`aggregate`](https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.aggregate)
    - [`join`](https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.join)
    - [`limit`](https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.limit)
    - [`sort`](https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.sort)
    - [`with_column`](https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.with_column)
    - [`schema`](https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.schema)
    - [`collect`](https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.collect)
    - [`show`](https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.show)
    - [`explain`](https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.explain)
    - [`execute_stream`](https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.execute_stream)
    - [`write_parquet`](https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.write_parquet)
    - [`write_csv`](https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.write_csv)
    - [`write_json`](https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.write_json)
    - [`write_table`](https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.write_table)
    - [`into_parts`](https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.into_parts)
    - [`into_unoptimized_plan`](https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.into_unoptimized_plan)
    - [`into_optimized_plan`](https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.into_optimized_plan)
    - [`create_physical_plan`](https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.create_physical_plan)
    - [`into_view`](https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.into_view)

[optimizer-rules]: https://github.com/apache/datafusion/blob/main/datafusion/optimizer/src/optimizer.rs#L230-L257
[physical-rules]: https://github.com/apache/datafusion/blob/main/datafusion/physical-optimizer/src/optimizer.rs#L86-L162
[`PushDownFilter`]: https://docs.rs/datafusion-optimizer/latest/datafusion_optimizer/push_down_filter/struct.PushDownFilter.html
[`OptimizeProjections`]: https://docs.rs/datafusion-optimizer/latest/datafusion_optimizer/optimize_projections/index.html
[`CommonSubexprEliminate`]: https://docs.rs/datafusion-optimizer/latest/datafusion_optimizer/common_subexpr_eliminate/struct.CommonSubexprEliminate.html
[`SimplifyExpressions`]: https://docs.rs/datafusion-optimizer/latest/datafusion_optimizer/simplify_expressions/struct.SimplifyExpressions.html
[`JoinSelection`]: https://docs.rs/datafusion-physical-optimizer/latest/datafusion_physical_optimizer/join_selection/struct.JoinSelection.html
[`HashJoinExec`]: https://docs.rs/datafusion/latest/datafusion/physical_plan/joins/struct.HashJoinExec.html
[`SortMergeJoinExec`]: https://docs.rs/datafusion/latest/datafusion/physical_plan/joins/struct.SortMergeJoinExec.html
[`SymmetricHashJoinExec`]: https://docs.rs/datafusion/latest/datafusion/physical_plan/joins/struct.SymmetricHashJoinExec.html
[`ParquetExec`]: https://docs.rs/datafusion/latest/datafusion/datasource/physical_plan/parquet/struct.ParquetExec.html
[`EnforceDistribution`]: https://docs.rs/datafusion-physical-optimizer/latest/datafusion_physical_optimizer/enforce_distribution/struct.EnforceDistribution.html
[`EnforceSorting`]: https://docs.rs/datafusion-physical-optimizer/latest/datafusion_physical_optimizer/enforce_sorting/struct.EnforceSorting.html
[`FairSpillPool`]: https://docs.rs/datafusion-execution/latest/datafusion_execution/memory_pool/struct.FairSpillPool.html
[`DiskManager`]: https://docs.rs/datafusion-execution/latest/datafusion_execution/disk_manager/struct.DiskManager.html
[`CoalesceBatches`]: https://docs.rs/datafusion-physical-optimizer/latest/datafusion_physical_optimizer/coalesce_batches/struct.CoalesceBatches.html
[config-dynamic]: ../../user-guide/configs.md#enable_join_dynamic_filter_pushdown
[config-partitions]: ../../user-guide/configs.md#target_partitions
[`EliminateJoin`]: https://docs.rs/datafusion-optimizer/latest/datafusion_optimizer/eliminate_join/struct.EliminateJoin.html
[`ExtractEquijoinPredicate`]: https://docs.rs/datafusion-optimizer/latest/datafusion_optimizer/extract_equijoin_predicate/struct.ExtractEquijoinPredicate.html

# Reference-style links for methods and types used above

# Types

[SessionContext]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html
[SessionState]: https://docs.rs/datafusion/latest/datafusion/execution/session_state/struct.SessionState.html
[`SessionConfig`]: https://docs.rs/datafusion/latest/datafusion/config/struct.SessionConfig.html
[DataFrame]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html
[LogicalPlan]: https://docs.rs/datafusion-expr/latest/datafusion_expr/logical_plan/enum.LogicalPlan.html
[LogicalPlanBuilder]: https://docs.rs/datafusion-expr/latest/datafusion_expr/logical_plan/builder/struct.LogicalPlanBuilder.html
[ExecutionPlan]: https://docs.rs/datafusion/latest/datafusion/physical_plan/trait.ExecutionPlan.html
[TableProvider]: https://docs.rs/datafusion/latest/datafusion/datasource/trait.TableProvider.html

# SessionContext methods

[`with_config()`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.with_config
[`state()`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.state
[`read_parquet()`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.read_parquet
[`read_csv()`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.read_csv
[`read_json()`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.read_json
[`sql()`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.sql
[`table()`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.table
[`register_table()`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.register_table
[`register_parquet()`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.register_parquet
[`register_udf()`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.register_udf
[`register_udaf()`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.register_udaf
[`register_table_provider()`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.register_table_provider
[`catalog()`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.catalog
[`catalog_names()`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.catalog_names

# DataFrame methods

[`select()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.select
[`filter()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.filter
[`aggregate()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.aggregate
[`join()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.join
[`limit()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.limit
[`sort()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.sort
[`with_column()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.with_column
[`schema()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.schema
[`collect()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.collect
[`show()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.show
[`explain()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.explain
[`execute_stream()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.execute_stream
[`write_parquet()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.write_parquet
[`write_csv()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.write_csv
[`write_json()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.write_json
[`write_table()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.write_table
[`.into_parts()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.into_parts
[`into_unoptimized_plan()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.into_unoptimized_plan
[`into_optimized_plan()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.into_optimized_plan
[`create_physical_plan()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.create_physical_plan
[`.into_view()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.into_view
