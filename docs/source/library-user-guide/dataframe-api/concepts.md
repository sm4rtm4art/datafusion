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

This guide covers the core concepts you need to understand when working with DataFusion DataFrames: SessionContext, LogicalPlan relationships, lazy evaluation, and handling null values.

> **Conceptual Foundation**: For high-level concepts about why DataFrames exist and their role in the data science ecosystem, see the [User Guide](../../user-guide/dataframe.md#how-dataframes-work-lazy-evaluation-and-arrow-output).

```{contents}
::local:
::depth: 2
```

```
SessionContext
  ↓ creates
DataFrame (lazy)
  ↓ wraps
LogicalPlan
  ↓ optimizes
Optimized LogicalPlan
  ↓ plans into
ExecutionPlan (Physical Plan)
  ↓ optimizes
Optimized ExecutionPlan
  ↓ executes
RecordBatch streams
```

> **Glossary snapshot**
>
> - **`SessionContext`**: Entry point for creating DataFrames, configuring execution, and registering tables/functions.
> - **`SessionState`**: Captured snapshot of context configuration and catalog state used when executing a DataFrame.
> - **`DataFrame`**: Lazy wrapper pairing a `LogicalPlan` with a `SessionState` snapshot; transformations build plans, actions execute them.
> - **`LogicalPlan`**: Tree describing _what_ to compute (projection, filter, join, etc.).
> - **`ExecutionPlan`**: Physical operator tree describing _how_ to compute (hash aggregate, parquet scan, shuffle, etc.).
> - **`RecordBatch`**: Arrow data structure representing a chunk of rows in columnar form; execution produces streams of batches.

## SessionContext: The Entry Point for DataFrames

```
[SessionContext] -> DataFrame -> LogicalPlan -> ExecutionPlan -> RecordBatches
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

You can create a `SessionContext` with default settings or customize it for your needs using the configuration methods:

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

> See [Best Practices](best-practices.md) for more on configuration options and tuning knobs that affect execution.

### Core Design Principles

DataFrames are built on three foundational ideas:

1.  **Lazy evaluation**: Transformations build up a query plan that executes only when you call an action method like [`.collect()`] or [`.show()`]. This allows the entire query to be optimized holistically.
2.  **Immutable transformations**: DataFrame methods return new DataFrames, leaving the original unchanged. This functional style makes pipelines easier to reason about.
3.  **Arrow-native data model**: Data is represented in memory using Apache Arrow's columnar format. This enables efficient, vectorized processing and zero-copy data sharing with other systems.

## Schemas and Data Types

```
SessionContext -> DataFrame -> [LogicalPlan] -> ExecutionPlan -> RecordBatches
```

A DataFrame’s schema is the schema of its underlying [`LogicalPlan`]. Sources (files, tables, SQL) provide or infer a base schema, and each transformation (e.g., `select`, `with_column`, `aggregate`, `join`) derives a new output schema from its inputs and expressions. [`DataFrame::schema()`] simply exposes the plan’s current schema (names, order, [`DataType`], and nullability).

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

## Handling Null Values

```
SessionContext -> DataFrame -> LogicalPlan -> ExecutionPlan -> [RecordBatches]
```

Arrow represents nulls via a bitmap, so DataFusion treats null-handling consistently across operators. Keep these rules in mind:

- **Three-valued logic**: comparisons involving nulls become `NULL` (unknown). Filters keep only rows where the predicate is `TRUE`.
- **Conditional helpers**: functions like [`is_null()`], [`is_not_null()`], [`coalesce()`], `fill_null`, and `nvl` help eliminate or replace nulls explicitly.
- **Aggregations**: aggregators such as `sum` and `avg` skip null inputs; use `count` vs. `count_distinct` to control null semantics.
- **Sorting and joins**: joins treat `NULL = NULL` as `FALSE` (except for `IS NOT DISTINCT FROM` style comparisons); sorting places nulls last by default but you can control this via [`sort()`] expressions.
- **Window & set ops**: window frames and set operations follow Arrow semantics—consider filling nulls first when they carry business meaning.

```rust
let cleaned = df
    .with_column("revenue", coalesce(vec![col("revenue"), lit(0)]))?
    .filter(col("country").is_not_null())?;
```

> **Tip:** The [Transformations guide](transformations.md#dataframe-transformations) shows end-to-end patterns for filtering, imputing, and aggregating null-aware data.

## Execution Model: Actions vs. Transformations

```
SessionContext -> DataFrame -> LogicalPlan -> [ExecutionPlan] -> [RecordBatches]
```

[`DataFrame`] exposes three broad groups of methods that mirror the lifecycle of building and executing a query. Knowing which bucket a method belongs to makes the API surface less overwhelming.

| Category                     | Purpose                                                  | Representative methods                                                                  | Closest SQL concept                       |
| ---------------------------- | -------------------------------------------------------- | --------------------------------------------------------------------------------------- | ----------------------------------------- |
| **Transformations**          | Build/modify the logical plan without executing          | [`select()`], [`filter()`], [`aggregate()`], [`join()`], [`limit()`], [`with_column()`] | `SELECT`, `WHERE`, `JOIN`, `GROUP BY`     |
| **Result-producing actions** | Trigger execution and return data to the caller          | [`collect()`], [`show()`], [`limit()` with action], [`explain()`]                       | Materializing a query result              |
| **Sink actions**             | Execute and persist results to external systems          | [`write_parquet()`], [`write_csv()`], [`write_json()`], [`write_table()`]               | `CREATE TABLE AS`, `COPY TO`, file writes |
| **Introspection/Admin**      | Inspect plans, schema, or reuse pieces in other contexts | [`schema()`], [`into_parts()`], [`into_view()`], [`into_optimized_plan()`]              | `EXPLAIN`, catalog metadata               |

> Expressions such as `col("a").gt(lit(5))` come from the logical expression API. If you're new to building expressions, see [Working with Expressions](../working-with-exprs.md) for a guided tour.

DataFrames are **lazy**. Transformations mutate the logical plan, while actions trigger execution and stream [`RecordBatch`]es through an [`ExecutionPlan`].

1.  **Transformations** return new DataFrames: `select`, `filter`, `with_column`, `aggregate`, `sort`, `limit`, etc.
2.  **Actions** execute the plan:

- **Return data**: [`collect()`] gathers the entire result into memory; [`show()`] pretty prints; [`explain()`] produces a textual plan.
- **Stream batches**: [`execute_stream()`] (via `SessionContext`) returns a `SendableRecordBatchStream` for incremental processing.
- **Write results**: `write_parquet`, `write_csv`, `write_json`, `write_table` persist to sinks.

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

> **Mind the trade-offs:** `collect()` is convenient but buffers the entire result set. Prefer streaming or writing to sinks for large outputs.

### Why the Execution Plan Matters: From Recipe to Reality

It is crucial to understand the difference between the `LogicalPlan` and the `ExecutionPlan` because all performance tuning and debugging happens at the physical level.

- The **DataFrame** and its **`LogicalPlan`** are the **recipe**: they describe **what** result you want in abstract terms (e.g., "join A and B, then filter").
- The **`ExecutionPlan`** (or "physical plan") is the **factory assembly line**: it describes **how** to get that result with specific algorithms and operations.

The query optimizer's job is to translate the logical recipe into an efficient physical assembly line. For the same DataFrame, the optimizer might choose very different plans based on data statistics, available memory, and configuration.

**Example: A Simple Query, Two Potential Plans**

Imagine this simple DataFrame:

```rust
let df = ctx.read_parquet("sales.parquet", ParquetReadOptions::default()).await?
    .filter(col("region").eq(lit("EMEA")))?;
```

- **Plan A (Efficient):** The optimizer "pushes down" the filter. The `ExecutionPlan` instructs the Parquet reader to **only** read the row groups relevant to the 'EMEA' region and to skip all others. This is fast because it minimizes I/O.
- **Plan B (Inefficient):** A naive plan reads the _entire_ Parquet file into memory and **then** applies the filter. This is vastly slower for large files.

You can inspect the chosen `ExecutionPlan` for any DataFrame using [`explain()`]:

```rust
df.explain(false, false)?.show().await?;
```

The output of `explain` shows you the chosen algorithms (e.g., `HashJoin`, `SortMergeJoin`), the order of operations, and how filters are applied. Understanding this plan is the key to diagnosing bottlenecks and improving query performance.

## Relationship between [`LogicalPlan`]s and `DataFrame`s

A DataFusion [`DataFrame`] is a lazy recipe for a computation, defined by two key components:

- A **`LogicalPlan`**: An immutable tree describing **what** to compute (e.g., scan a file, filter by a predicate, project some columns).
- A **`SessionState` snapshot**: An immutable capture of the session's configuration and resources (e.g., registered tables, UDFs, timezone settings) at the moment the DataFrame was created.

This design ensures that a DataFrame's behavior is reproducible and predictable. From a DataFrame's perspective, the world is frozen in time. The `SessionContext` is a mutable, high-level control surface you use to manage the session, but each DataFrame holds an immutable `SessionState` snapshot. Every transformation you apply to a DataFrame (like `.select()` or `.filter()`) doesn't change the original; it returns a **new** DataFrame with an updated `LogicalPlan`.

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

> **SessionState snapshot details**
>
> A `SessionState` captures the exact execution context at DataFrame creation time, including:
>
> - Configuration (e.g., batch size, target partitions, optimizer toggles, timezone)
> - Registered catalogs, schemas, tables, UDFs/UDTFs/UDAFs
> - The query execution start timestamp used by time-dependent functions
>
> Practical implications:
>
> - `now()` and similar functions use the captured start time; reusing a plan later will not “advance time”.
> - Changing configuration (e.g., timezone) after creating a DataFrame will not affect that DataFrame’s execution.
> - Register UDFs before building DataFrames that rely on them.

### Converting Between `DataFrame` and `LogicalPlan`

While the DataFrame API covers most use cases, you may need to work directly with the `LogicalPlan`—for example, to apply custom optimizer rules, integrate with advanced query rewriting logic, or programmatically inspect/modify the query structure before execution.

**DataFrame vs LogicalPlan — Same query, different representations:**

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
async fn main() -> Result<()> {
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
async fn main() -> Result<()> {
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

> See also: [Building Logical Plans](../building-logical-plans.md) for a deeper dive into `LogicalPlanBuilder` and manual plan construction.

### Mixing SQL and DataFrames

It is common to mix programmatic DataFrames with SQL. One ergonomic pattern is to turn a `DataFrame` into a view and query it with SQL:

```rust
use std::sync::Arc;
use datafusion::prelude::*;

let ctx = SessionContext::new();

// Build a DataFrame programmatically
let sales = ctx.read_parquet("sales.parquet", ParquetReadOptions::default()).await?
    .filter(col("region").eq(lit("EMEA")))?;

// Register as a temporary view
let view = sales.clone().into_view();
ctx.register_table("sales_emea", view)?;

// Now query with SQL and continue with the DataFrame API
let top = ctx.sql("SELECT product_id, SUM(revenue) AS rev FROM sales_emea GROUP BY product_id ORDER BY rev DESC LIMIT 10").await?;
top.show().await?;
```

> You can also register arbitrary `TableProvider`s or use `into_view()` to compose multi-step pipelines across SQL and DataFrames.

## Key API Methods

When working with the `DataFrame`/`LogicalPlan` boundary, be aware of these methods:

- **[`into_parts()`]**: Returns `(SessionState, LogicalPlan)`. Use this when you need to manipulate the plan while preserving the exact session snapshot. This is the recommended way to extract both components.

- **[`into_unoptimized_plan()`]**: Returns the unoptimized `LogicalPlan` but **loses the `SessionState` snapshot**. Useful for plan inspection or tests; for production, prefer [`into_parts()`] (or executing the `DataFrame`) to preserve session state.

  > ⚠️ **Warning: Perils of Lost State**
  >
  > Discarding `SessionState` means the execution context can change unexpectedly. For example, the `now()` function returns the query execution start time captured in the session state—if you lose that snapshot and re-execute the plan later, `now()` will return a different timestamp. Similarly, changes to session configuration (like timezone settings) will affect re-execution. Always use [`into_parts()`] when you intend to execute the plan later.
  >
  > See the [scalar functions documentation](../../user-guide/sql/scalar_functions.md) for details on how `now()` and other time-dependent functions work.

- **[`into_optimized_plan()`]**: Returns the optimized `LogicalPlan` after running query optimization rules. Also **loses session state**. Useful for plan inspection/tests; for production re-use, prefer [`into_parts()`].

- **[`create_physical_plan()`]**: Converts the `LogicalPlan` to an [`ExecutionPlan`] (doesn't execute). Actions like [`collect()`] and [`show()`] handle this internally—you rarely need to call this directly.

- **[`into_view()`]**: Converts a `DataFrame` into a [`TableProvider`] that can be registered as a view using [`SessionContext::register_table()`].

> **See also:**
>
> - [DataFrame Execution](#dataframe-execution) for execution methods
> - [DataFrame Transformations](#dataframe-transformations) for building queries
> - [How DataFrames Work](../../user-guide/dataframe.md#how-dataframes-work-lazy-evaluation-and-arrow-output) for lazy evaluation details

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
