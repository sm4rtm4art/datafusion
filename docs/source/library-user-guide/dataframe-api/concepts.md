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

**What DataFrames are, where they live, and why they matter in the query engine landscape.**

Data-driven projects demand efficient processing of large, diverse datasets. DataFusion addresses this as a query engine—connecting data sources from Parquet files to object stores to custom providers—through two primary APIs: the DataFrame API and SQL. Both compile to the same `LogicalPlan`, so performance is equivalent; the choice is ergonomics.

The DataFrame API's builder architecture, originated in the known python pandas library, makes it uniquely suited for programmatic query construction: readable, maintainable pipelines with dynamic filters, conditional logic, and seamless Rust integration. What the builder pattern can't express elegantly (complex window functions, CTEs), the SQL-API covers. And when Apache Arrows columnar OLAP processing isn't the right fit, DataFusion's `TableProvider` interface lets you integrate row-oriented systems with predicate pushdown.

This guide explores the conceptual foundation: what a DataFrame _is_ (a `LogicalPlan` paired with a frozen `SessionState`), how it flows through the execution pipeline, and where DataFusion fits in the broader data systems landscape. For hands-on examples, see: [Create](creating-dataframes.md) → [Transform](transformations.md) → [Write](writing-dataframes.md).

> **Style Note:** <br>
> DataFrame methods use `.method()` syntax (e.g., `.collect()`) to reflect chaining. Standalone functions use `func()` (e.g., `col()`), constructors use `Type::new()`. Rust types use `PascalCase` (e.g., `RecordBatch`).

```{contents}
::local:
::depth: 2
```

## Introduction

### Why the DataFrame-API if the SQL-API is available?

DataFusion provides two entry points to the same query engine: the **SQL-API** (SQL strings parsed via [`sqlparser`] with [configurable dialect]) and the **DataFrame-API** (a builder pattern constructing plans programmatically). Both compile to the same [`LogicalPlan`] and execute identically—the choice is about ergonomics, not performance. Throughout this documentation, we use PostgreSQL syntax when comparing the APIs—it's well documented, widely understood, and DataFusion's default semantics (NULL handling, sort order) closely follow PostgreSQL conventions.

**But why a builder-API alongside a parser-based SQL-API?** <br>
The builder pattern offers ergonomics that parser patterns like the SQL-API cannot match—readable pipelines that flow top-to-bottom, composable query fragments you can extract into functions and reuse, and Rust's type system catching schema errors at compile time. When your query depends on runtime conditions or maintainability matters as much as correctness, the builder pattern shines.

These trade-offs—when to choose SQL, when to choose the DataFrame-API, and how to mix them freely—are explored in [Two Paths to the Same Plan](#two-paths-to-the-same-plan-parser-vs-builder) and [Mixing SQL and DataFrames](#mixing-sql-and-dataframes).

### What is a DataFrame?

In DataFusion, a Data**Frame** is not your data—it's the _frame_ around your data. Think of it literally: a framework defining where data lives, how it flows through the query engine, and the environment in which transformations execute.

DataFusion's DataFrames are lazy—not in a bad way, but in an efficient way. When you call [`.filter()`] or [`.join()`], nothing happens yet. You're constructing a [`LogicalPlan`]—the recipe describing _what_ to compute. The DataFrame **wraps** this plan together with a [`SessionState`] snapshot that freezes _how_ to compute it. This pairing ensures reproducibility: re-executing a DataFrame uses the same configuration, catalogs, and query start timestamp, even if the [`SessionContext`] has since changed. (For the technical details, see [Relationship between `LogicalPlan`s and `DataFrame`s](#relationship-between-logicalplans-and-dataframes).)

**But what happens when you finally call `.collect()` and execute the plan?** <br>
The diagram below traces the journey—from lazy plan to concrete results—and shows why deferring execution lets the optimizer reorder operations, push predicates to data sources, and select efficient algorithms.

### How Queries Flow Through DataFusion

```text
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

**Reading the diagram:**

- **SessionContext → SessionState**: Your context's configuration is captured as an immutable snapshot, ensuring the DataFrame executes consistently even if the context changes later.
- **SQL / DataFrame API → LogicalPlan**: Both paths produce the same abstract plan—this is why you can mix APIs freely with no performance penalty.
- **Optimize → Physical Plan → Execute**: The logical optimizer rewrites the plan (predicate pushdown, projection pruning), the physical planner chooses algorithms (hash join vs. sort-merge), and Tokio executes partitions in parallel.

> **Glossary snapshot**
>
> - **[`SessionContext`]**: Entry point for creating DataFrames, configuring execution, and registering tables/functions.
> - **[`SessionState`]**: Captured snapshot of context configuration and catalog state used when executing a DataFrame.
> - **[`DataFrame`]**: Lazy wrapper pairing a `LogicalPlan` with a `SessionState` snapshot; transformations build plans, actions execute them.
> - **[`LogicalPlan`]**: Tree describing _what_ to compute (projection, filter, join, etc.).
> - **[`ExecutionPlan`]**: Physical operator tree describing _how_ to compute (hash aggregate, parquet scan, shuffle, etc.).
> - **[`RecordBatch`]**: Arrow data structure representing a chunk of rows in columnar form; execution produces streams of batches.
>
> For deeper architectural details—thread scheduling, memory management, crate organization—see the [Architecture section] in the API documentation.

## Two Paths to the Same Plan: Parser vs Builder

**SQL and the DataFrame API are two front-ends to the same query engine**<br>

Both compile to identical [`LogicalPlan`] representations, receive the same optimizations, and execute with the same performance. This unified architecture means you can choose whichever API fits your workflow without sacrificing speed, and you can freely mix both in a single pipeline.

```text
┌────────────────────────────────────────────────────────────────────────────┐
│                              SessionState                                  │
│           (Catalog, Function Registry, Config, Query Planner)              │
├──────────────────────────────────┬─────────────────────────────────────────┤
│         SQL (Parser)             │          DataFrame API (Builder)        │
├──────────────────────────────────┼─────────────────────────────────────────┤
│                                  │                                         │
│  "SELECT a, b FROM t             │  ctx.table("t")?                        │
│   WHERE a > 10"                  │     .filter(col("a").gt(lit(10)))?      │
│                                  │     .select(vec![col("a"), col("b")])?  │
│           │                      │              │                          │
│           │ parse                │              │ build                    │
│           ▼                      │              │                          │
│      ┌─────────┐                 │              │ (no AST step)            │
│      │   AST   │                 │              │                          │
│      └────┬────┘                 │              │                          │
│           │ plan                 │              │                          │
│           ▼                      │              ▼                          │
├───────────┴──────────────────────┴──────────────┴──────────────────────────┤
│                                                                            │
│                          LogicalPlan (Identical!)                          │
│                                                                            │
├────────────────────────────────────────────────────────────────────────────┤
│                                      │                                     │
│                                      ▼                                     │
│                              Further Execution                             │
└────────────────────────────────────────────────────────────────────────────┘
```

**Reading the diagram:**

- **SessionState (top container)**:<br>
  Both paths operate within the same execution environment. The [`SessionState`] provides the catalog (table definitions), function registry (UDFs, aggregates), configuration, and query planner. This shared context is why both APIs have access to the same tables and functions.

- **SQL (Parser) path**:<br>
  A query string goes through `sqlparser`'s lexer and parser to produce an [Abstract Syntax Tree (AST)], then the logical planner converts the AST into a [`LogicalPlan`]. This extra step enables familiar SQL syntax but means errors surface at runtime.

- **DataFrame API (Builder) path**:<br>
  Method calls like [`.filter()`] and [`.select()`] construct [`LogicalPlan`] nodes directly—no parsing, no AST. This is why you get IDE autocomplete and why Rust can catch type errors at compile time (though schema errors remain runtime).

- **LogicalPlan (convergence point)**:<br>
  Both paths produce the _exact same_ [`LogicalPlan`] structure. There's no "SQL flavor" vs "DataFrame flavor"—just one unified representation. This is the key insight that makes mixing APIs free.

- **Further Execution**: <br>
  From here, the [`LogicalPlan`] flows through optimization, physical planning, and execution—identically regardless of which path created it.

> **Key takeaway**: <br>
> Parser vs Builder is purely a construction choice—once you have a `LogicalPlan`, DataFusion doesn't know or care how you built it.

### In Practice: Two Paths, One Result

The following example demonstrates the interchangeability of both APIs. We query the same table using SQL (parser path) and the DataFrame API (builder path), then verify that both produce identical results. This is the core promise of DataFusion's unified architecture—choose the API that fits your workflow, knowing the outcome is the same.

```rust
use datafusion::prelude::*;
use datafusion::error::Result;
use datafusion::assert_batches_eq;

#[tokio::main]
async fn main() -> Result<()> {
    let ctx = SessionContext::new();

    // Set up a table with sample data
    ctx.sql("CREATE TABLE sales (region VARCHAR, amount INT) AS VALUES
        ('north', 1500), ('south', 800), ('east', 2000)").await?;

    // Parser path: SQL string → parse → AST → LogicalPlan → DataFrame
    let sql_df = ctx.sql(
        "SELECT region, amount FROM sales WHERE amount > 1000"
    ).await?;

    // Builder path: method calls → LogicalPlan nodes → DataFrame
    let builder_df = ctx.table("sales").await?
        .filter(col("amount").gt(lit(1000)))?
        .select(vec![col("region"), col("amount")])?;

    // Both paths produce this identical result
    let expected = [
        "+--------+--------+",
        "| region | amount |",
        "+--------+--------+",
        "| north  | 1500   |",
        "| east   | 2000   |",
        "+--------+--------+",
    ];

    let sql_result = sql_df.collect().await?;
    let builder_result = builder_df.collect().await?;

    assert_batches_eq!(expected, &sql_result);
    assert_batches_eq!(expected, &builder_result);

    Ok(())
}
```

### When to Choose Which?

Both APIs produce identical plans, so choose based on ergonomics:

| Choose **DataFrame API** when...                         | Choose **SQL** when...                              |
| -------------------------------------------------------- | --------------------------------------------------- |
| Query logic depends on runtime conditions                | Query is static and well-defined                    |
| You want reusable query fragments (extract to functions) | You need complex analytics (window functions, CTEs) |
| Security matters (no SQL injection by construction)      | Query comes from config files or user input         |
| IDE refactoring and autocomplete matter                  | Team is SQL-fluent, Rust is secondary               |

Neither is "better"—they're tools for different situations. And since you can [mix them freely](#mixing-sql-and-dataframes), you don't have to choose just one.

> **Further reading**:<br>
> This dual-API architecture follows principles established in the broader data science ecosystem. For the theoretical foundation, see [Towards Scalable Dataframe Systems][dataframe-paper].

---

## SessionContext: The Entry Point for DataFrames

**The [`SessionContext`] is your reproducible gateway to DataFusion**<br>

The `SessionContext` itself is **mutable**—designed to hold session information: [`ConfigOptions`] (batch size, parallelism, timezone), registered tables and catalogs, user-defined functions, and runtime resources (memory limits, object stores). But every DataFrame you create captures an **immutable** [`SessionState`] snapshot of the context at that moment. This ensures queries execute with consistent settings even if you modify the context later.

You'll use the `SessionContext` to load data, run SQL, register tables, and configure execution behavior.

As an illustration the following schema should give an overview.

```text
SessionContext (mutable, evolves over session lifetime)
├── ConfigOptions       ← batch_size, target_partitions, timezone, SQL options
├── RuntimeEnv          ← memory pool, disk manager, object stores
├── Catalog             ← registered tables, schemas, databases
└── Function Registry   ← UDFs, UDAFs, UDWFs
        │
        ↓ snapshot at DataFrame creation
SessionState (immutable) ← frozen environment captured by DataFrame
```

This separation ensures reproducibility: changes to the `SessionContext` after DataFrame creation don't affect existing DataFrames—each continues to execute with the `SessionState` snapshot it captured. Only newly created DataFrames will see the updated configuration, tables, or functions.

### Common ways to create a DataFrame using the SessionContext

Like DataFrame, SessionContext exposes a large API surface that becomes easier to navigate once you understand the main categories:

| Category               | Purpose                                | Examples                                                                  |
| ---------------------- | -------------------------------------- | ------------------------------------------------------------------------- |
| **DataFrame Creation** | Create DataFrames from various sources | [`.read_parquet()`], [`.read_csv()`], [`.sql()`], [`.table()`]            |
| **Table Management**   | Register and manage tables by name     | [`.register_table()`], [`.register_parquet()`], [`.deregister_table()`]   |
| **Configuration**      | Control execution behavior             | [`.with_config()`], [`.state()`]                                          |
| **Catalog Operations** | Manage schemas and databases           | [`.catalog()`], [`.catalog_names()`]                                      |
| **Extensions**         | Add custom functionality               | [`.register_udf()`], [`.register_udaf()`], [`.register_table_provider()`] |

> **Learn more:** <br>
> For complete examples of each pattern, see [Creating DataFrames](creating-dataframes.md). For all available configuration options, see [Configuration Settings](../../user-guide/configs.md).

### Creating and Configuring SessionContext

Before using any of the methods above, you need a `SessionContext`. In most cases, `SessionContext::new()` with defaults is all you need. For performance-critical workloads, you can tune execution parameters via [`SessionConfig`]:

```rust
use datafusion::prelude::*;
use datafusion::execution::config::SessionConfig;

fn main() {
    // Default context (good for getting started)
    let ctx = SessionContext::new();

    // Customized context for performance tuning
    let config = SessionConfig::new()
        .with_batch_size(8192)               // Rows per batch
        .with_target_partitions(8);          // Parallelism (typically num_cpus::get())
    let ctx = SessionContext::new_with_config(config);
}
```

Once you have a `SessionContext`, you can create DataFrames, register tables, and execute queries—the context maintains all state that DataFrames need during execution.

For more detailed explanation and examples of the [`SessionContext`] see:

- [`SessionContext`] documentation (API reference)
- [`SessionState`] documentation (snapshot semantics)
- [Configuration Settings](../../user-guide/configs.md) (all configuration options)
- [Creating DataFrames](creating-dataframes.md) (practical examples)

## Data Model & Schema

**Schemas are the contract between your query and your data—get them wrong, and everything downstream breaks.**

Every optimization DataFusion performs—predicate pushdown, projection pruning, join ordering—relies on knowing column types and nullability upfront. Schemas also catch errors early: mismatched types or missing columns surface during planning, not mid-execution when debugging is harder.
A schema is an ordered collection of [`Field`]s.
Each `Field` describes one column:

- **`name`**: the column name
- **[`DataType`]**: the Arrow data type (Int64, Utf8, Struct, etc.)
- **`nullable`**: whether the column can contain null values
- **`metadata`**: optional key-value pairs for custom information ([field-level metadata])

These components **originate** in data sources—Parquet file metadata, Arrow IPC schemas, the `SessionContext` catalog for registered tables, or programmatic construction via [`Field::new().with_metadata()`][with_metadata]. As data flows through the query pipeline, each `LogicalPlan` node derives an output schema from its inputs; transformations like `.select()` or `.join()` produce new nodes with new schemas. The [`.schema()`] method exposes the current plan's output schema, and execution produces Arrow `RecordBatch`es conforming to that schema.

Understanding how schemas flow—and how null values behave within them—prevents subtle bugs in filters, joins, and aggregations.

Recall that a `DataFrame` wraps a `LogicalPlan` (see [What is a DataFrame?](#what-is-a-dataframe)). The diagram below shows how schemas propagate through this structure—from source catalog to final execution:

[`DataType`]: https://docs.rs/arrow/latest/arrow/datatypes/enum.DataType.html

```text
SessionContext (catalog)
  ↓ stores table schemas, provides source metadata
DataFrame
  ↓ wraps LogicalPlan
LogicalPlan    ← Each node derives output schema from inputs
  ↓ optimizes & plans
ExecutionPlan  ← Null handling semantics implemented here
  ↓ executes
RecordBatches  ← Actual null values (bitmaps) in Arrow format
```

### Inspecting Schemas

The [`.schema()`] method exposes the current `LogicalPlan`'s output schema—use it to verify column names, types, and nullability before execution. Since each transformation derives a new schema, `.schema()` always reflects what you'd get if you executed now.

- **Before transformations**: Inspect source schema to understand available columns
- **After transformations**: Verify derived schema matches expectations (especially after joins, aggregations)
- **During planning**: The optimizer uses schema information for type coercion, pushdown decisions, and operator selection

```rust
use datafusion::prelude::*;
use datafusion::error::Result;

fn main() -> Result<()> {
    let df = dataframe!(
        "id" => [1],
        "name" => ["Alice"],
        "age" => [25]
    )?;

    // Quick overview: just field names
    println!("{}", df.schema());
    // Output: fields:[id, name, age], metadata:{}

    // Detailed inspection: types, nullability, metadata
    for field in df.schema().fields() {
        println!(
            "  {}: {:?} (nullable: {}, metadata: {:?})",
            field.name(), field.data_type(), field.is_nullable(), field.metadata()
        );
    }
    // Output:
    //   id: Int64 (nullable: true, metadata: {})
    //   name: Utf8 (nullable: true, metadata: {})
    //   age: Int64 (nullable: true, metadata: {})
    //
    // Metadata would contain key-value pairs from Parquet files or Arrow extension types:
    //   customer_id: Int64 (nullable: false, metadata: {"pii": "false"})

    Ok(())
}
```

> **Reference:** <br>
> For a deeper tour of Arrow schemas and RecordBatches, see [Introduction to Arrow & RecordBatches](../../user-guide/arrow-introduction.md). For SQL type compatibility and coercion rules, see [SQL Data Types](../../user-guide/sql/data_types.md).

### Handling Null Values

**Null values are the silent source of most query bugs.**

A `.filter()` that seems correct might silently drop rows; a `.join()` that should match doesn't; an `.aggregate()` returns unexpected results. Understanding how nulls propagate through DataFrame methods prevents these surprises.

Arrow represents [nulls via a bitmap]—each column has a validity buffer marking which rows contain null. DataFusion follows SQL-standard null semantics, which affects how every DataFrame method behaves.

#### **How `.filter()` Handles Nulls**

When you call `.filter(predicate)`, DataFusion evaluates the predicate for each row. The result can be `TRUE`, `FALSE`, or `NULL` (unknown)—and this three-valued logic is the root of most null-related surprises.

**The key insight:** <br>
Any operation involving `NULL` produces `NULL`, because "unknown" combined with anything is still "unknown."

| Expression      | Result | Why                                             |
| --------------- | ------ | ----------------------------------------------- |
| `5 > NULL`      | `NULL` | We don't know what NULL is, so we can't compare |
| `NULL = NULL`   | `NULL` | Two unknowns aren't necessarily equal!          |
| `NULL AND TRUE` | `NULL` | Unknown AND anything = unknown                  |
| `NULL OR TRUE`  | `TRUE` | TRUE OR anything = TRUE (short-circuit)         |
| `NULL OR FALSE` | `NULL` | Unknown OR FALSE = unknown                      |

**Filter behavior:**<br>
`WHERE` keeps **only** rows that evaluate to `TRUE`. Rows where the condition is `FALSE` or `NULL` are both filtered out. This catches many developers off guard:

```text
WHERE age > 18
  ├── age = 25  → TRUE  → kept ✓
  ├── age = 17  → FALSE → filtered out
  └── age = NULL → NULL → filtered out (!)
```

If you want to include rows with unknown age, you must explicitly ask for them using [`is_null()`] (or exclude nulls with [`is_not_null()`]):

```rust
use datafusion::prelude::*;
use datafusion::error::Result;

fn main() -> Result<()> {
    // Sample data with NULL values (use Option<T> for nullable columns)
    let df = dataframe!(
        "name" => ["Alice", "Bob", "Carol"],
        "age" => [Some(25i32), Some(17i32), None]  // None = NULL
    )?;
    // +-------+------+
    // | name  | age  |
    // +-------+------+
    // | Alice | 25   |
    // | Bob   | 17   |
    // | Carol | NULL |
    // +-------+------+

    // Standard filter: excludes NULLs (Carol filtered out!)
    let adults = df.clone().filter(col("age").gt(lit(18)))?;
    // Result: Only Alice (age=25)
    // - Bob: 17 > 18 = FALSE → filtered out
    // - Carol: NULL > 18 = NULL → filtered out (!)

    // Explicitly include NULLs
    let adults_or_unknown = df.filter(
        col("age").gt(lit(18)).or(col("age").is_null())
    )?;
    // Result: Alice AND Carol
    // - Bob still filtered: (17 > 18) OR (17 IS NULL) = FALSE

    Ok(())
}
```

#### **Conditional Helpers**

Aside ofqqq [`is_null()`] , DataFusion provides tools for explicitly handling nulls. Choose based on your intent:

| Function                          | Use When                                        | Example                                                              |
| --------------------------------- | ----------------------------------------------- | -------------------------------------------------------------------- |
| [`is_null()`] / [`is_not_null()`] | Testing for null in filters                     | `.filter(col("email").is_not_null())`                                |
| [`coalesce()`]                    | Providing fallback values (first non-null wins) | `coalesce(col("nickname"), col("name"), lit("Anonymous"))`           |
| [`nullif()`]                      | Converting specific values to null              | `nullif(col("status"), lit("UNKNOWN"))` → null if status = "UNKNOWN" |
| [`nvl()`] / [`ifnull()`]          | Simple two-argument fallback                    | `nvl(col("price"), lit(0))`                                          |

#### **How `.aggregate()` Handles Nulls**

Aggregation functions skip null inputs—they don't contribute to the result. This is usually what you want, but watch for these surprises:

- **`avg()` divides by non-null count**: With `[10, NULL, 20]`, average is `15` (sum 30 ÷ 2 values), not `10` (÷ 3 rows)
- **`count(*)` vs `count(col)`**: `count(*)` counts rows; `count(col)` counts non-null values
- **All-null columns**: Most aggregates return `NULL`, not zero—wrap with `coalesce()` if you need a default

| Expression   | With data `[10, NULL, 20]` | Notes                       |
| ------------ | :------------------------: | --------------------------- |
| `sum(col)`   |            `30`            | Nulls skipped               |
| `avg(col)`   |            `15`            | Average of 2 values, not 3  |
| `count(*)`   |            `3`             | Counts all rows             |
| `count(col)` |            `2`             | Counts only non-null values |
| `min(col)`   |            `10`            | Nulls ignored               |

#### **How `.join()` Handles Nulls**

When joining DataFrames, `NULL = NULL` evaluates to `FALSE`—two unknown values are not considered equal. This surprises many developers:

```text
LEFT TABLE        RIGHT TABLE       INNER JOIN RESULT
id | value        id | data
---|-------       ---|------        Only (1, 'a', 'x') matches!
1  | 'a'          1  | 'x'          ← 1 = 1, match ✓
2  | 'c'          NULL| 'y'         ← 2 ≠ NULL, no match
NULL| 'b'         3  | 'z'          ← NULL ≠ 3, no match
```

For null-safe equality (where `NULL = NULL` is `TRUE`), use SQL's `IS NOT DISTINCT FROM`:

```rust
use datafusion::prelude::*;
use datafusion::error::Result;

#[tokio::main]
async fn main() -> Result<()> {
    let ctx = SessionContext::new();

    // Create tables with NULL keys
    let left = dataframe!(
        "left_id" => [Some(1i32), Some(2i32), None],
        "value" => ["a", "b", "c"]
    )?;
    let right = dataframe!(
        "right_id" => [Some(1i32), None, Some(3i32)],
        "data" => ["x", "y", "z"]
    )?;

    // Register for SQL access
    ctx.register_table("left", left.clone().into_view())?;
    ctx.register_table("right", right.clone().into_view())?;

    // Standard join: NULL ≠ NULL (only id=1 matches)
    let standard = left.join(right, JoinType::Inner, &["left_id"], &["right_id"], None)?;
    standard.show().await?;
    // +---------+-------+----------+------+
    // | left_id | value | right_id | data |
    // +---------+-------+----------+------+
    // | 1       | a     | 1        | x    |  ← only match!
    // +---------+-------+----------+------+

    // Null-safe join via SQL: NULL = NULL is TRUE
    let null_safe = ctx.sql(
        "SELECT * FROM left l JOIN right r ON l.left_id IS NOT DISTINCT FROM r.right_id"
    ).await?;
    null_safe.show().await?;
    // +---------+-------+----------+------+
    // | left_id | value | right_id | data |
    // +---------+-------+----------+------+
    // | 1       | a     | 1        | x    |
    // |         | c     |          | y    |  ← NULL = NULL matches!
    // +---------+-------+----------+------+

    Ok(())
}
```

For programmatic control over null equality, see [`NullEquality`] when working with the lower-level [`LogicalPlanBuilder`] API.

#### **How `.sort()` Handles Nulls**

Where do nulls appear in sorted output? DataFusion follows PostgreSQL conventions:

| Sort Order          | Default Null Position | Override                             |
| ------------------- | --------------------- | ------------------------------------ |
| `ASC` (ascending)   | Nulls **last**        | `.sort(true, true)` for nulls first  |
| `DESC` (descending) | Nulls **first**       | `.sort(false, false)` for nulls last |

**Understanding `.sort(asc, nulls_first)`:**

Since `NULL` can't be compared (is `NULL < 5`?), sorting must define where NULLs go. The two booleans control this:

- **`asc`**: `true` = values ascending (1→9), `false` = values descending (9→1)
- **`nulls_first`**: `true` = NULLs before all values, `false` = NULLs after all values

PostgreSQL defaults: ASC puts nulls last, DESC puts nulls first.

[`NullEquality`]: https://docs.rs/datafusion/latest/datafusion/common/enum.NullEquality.html

```rust
use datafusion::prelude::*;
use datafusion::error::Result;

fn main() -> Result<()> {
    let df = dataframe!(
        "name" => ["Alice", "Bob", "Carol"],
        "score" => [Some(85i32), None, Some(92i32)]
    )?;

    // ASC: nulls last (default)
    let asc_nulls_last = df.clone().sort(vec![col("score").sort(true, false)])?;
    // score: 85, 92, NULL

    // ASC: nulls first
    let asc_nulls_first = df.clone().sort(vec![col("score").sort(true, true)])?;
    // score: NULL, 85, 92

    // DESC: nulls first (default)
    let desc_nulls_first = df.clone().sort(vec![col("score").sort(false, true)])?;
    // score: NULL, 92, 85

    // DESC: nulls last
    let desc_nulls_last = df.sort(vec![col("score").sort(false, false)])?;
    // score: 92, 85, NULL

    Ok(())
}
```

> **References**:
>
> - Null semantics: [PostgreSQL NULL Handling](https://www.postgresql.org/docs/current/functions-comparison.html) (DataFusion follows these conventions)
> - Join null equality: [`NullEquality`]
> - Sort API: [`DataFrame::sort()`], [PostgreSQL ORDER BY](https://www.postgresql.org/docs/current/queries-order.html)
> - Practical patterns: [Transformations guide](transformations.md#dataframe-transformations)

## DataFrame Structure: LogicalPlan + SessionState

**DataFusion—the out-of-the-box query engine—provides the DataFrame with both a recipe (the query plan) and a fully-equipped kitchen (the execution environment) for reproducible results.**

Understanding what a `DataFrame` actually _contains_ explains why queries are reproducible and why certain patterns (like registering UDFs before creating DataFrames) matter.

Every [`DataFrame`] pairs two components:

- **[`LogicalPlan`]** — the query recipe (_what_ to compute)
- **[`SessionState`]** — a frozen snapshot of the execution environment (_how_ to compute it)

The [`SessionContext`] is mutable and evolves over your session, but each `DataFrame` captures an **immutable snapshot** the [`SessionState`] at creation time. Transformations return new DataFrames with updated plans but the same snapshot; actions execute using that frozen state.

### Inside a DataFrame: Step by Step

**Think of query execution like cooking—the recipe alone isn't enough; you need the kitchen too.**

The concepts might come clearer with an everyday analogy of a kitchen.

| Concept            | Cooking Analogy    | What it holds                                       |
| ------------------ | ------------------ | --------------------------------------------------- |
| [`SessionContext`] | Kitchen (mutable)  | Tools, ingredients, configuration—changes over time |
| [`LogicalPlan`]    | Recipe (immutable) | Step-by-step instructions—what to compute           |
| [`SessionState`]   | Kitchen snapshot   | Photo of the kitchen at recipe start—frozen in time |
| [`DataFrame`]      | Recipe + snapshot  | Everything needed to cook the dish reproducibly     |

The [`SessionState`] defines the enviroment the data are processed in: if you add new tools to the kitchen after starting a dish, the dish-in-progress still uses the original setup. This prevents surprises ("where did my UDF go?") and ensures reproducibility.

Here's how this flows through the system in a nutshell:

```text
[ STEP 1: THE KITCHEN ]
┌──────────────────────────────────────────────────────────────────────────────┐
│                                SessionContext                                │
│                    (Mutable kitchen: tools & ingredients)                    │
├──────────────────────────────────────────────────────────────────────────────┤
│  • Config:  target_partitions=8, batch_size=8192                             │
│  • UDFs:    "my_custom_func"                                                 │
│  • Catalog: table "sales"                                                    │
└──────────────────────────────────────┬───────────────────────────────────────┘
                                       │
                                       │ .read_table("sales")
                                       ▼
[ STEP 2: START COOKING ]
┌──────────────────────────────────────────────────────────────────────────────┐
│                                  DataFrame                                   │
│              (Workstation setup: what's available when you start)            │
├──────────────────────────────────────┬───────────────────────────────────────┤
│           SessionState               │             LogicalPlan               │
│    (frozen tools & ingredients)      │          (first instruction)          │
├──────────────────────────────────────┼───────────────────────────────────────┤
│ • Config/UDFs at creation time       │                                       │
│ • Catalog state when cooking began   │      TableScan("sales")               │
└──────────────────────────────────────┴───────────────────┬───────────────────┘
                                                           │
                                                           │ .filter(amount > 100)
                                                           ▼
[ STEP 3: ADD INSTRUCTIONS ]
┌──────────────────────────────────────────────────────────────────────────────┐
│                                New DataFrame                                 │
│                  (Same workstation, extended recipe)                         │
├──────────────────────────────────────┬───────────────────────────────────────┤
│           SessionState               │             LogicalPlan               │
│         (unchanged snapshot)         │           (more steps added)          │
├──────────────────────────────────────┼───────────────────────────────────────┤
│ • Still the original setup           │      Filter(amount > 100)             │
│ • (New kitchen tools don't appear)   │        └─ TableScan("sales")          │
└──────────────────────────────────────┴───────────────────┬───────────────────┘
                                                           │
                                                           │ .collect() / .show()
                                                           ▼
[ STEP 4: SERVE THE DISH ]
┌──────────────────────────────────────────────────────────────────────────────┐
│                                ExecutionPlan                                 │
│                     (Cooking happens with given setup)                      │
├──────────────────────────────────────────────────────────────────────────────┤
│  • Recipe optimized using snapshot's rules                                   │
│  • Physical operators created (ParquetExec, FilterExec, etc.)                │
│  • Output: RecordBatches (the meal!)                                         │
└──────────────────────────────────────────────────────────────────────────────┘
```

<!--NOTICE TO THE PCM, CONTRIBUTER: OR Who ever!
I'm really tempted to add a image of alphabet soup here like

https://media.istockphoto.com/id/1210366546/de/foto/tomatensuppe-mit-buchstabennudeln-auf-l%C3%B6ffel.jpg?s=2048x2048&w=is&k=20&c=SaZ0yj4WLabqvd41RKJZlS7dRgZw_A-jVtuGrfGgvZo=

Of cause without copyright etc. !
-->

**Key API paths:**

```text
DataFrame ↔ into_parts() ↔ (SessionState, LogicalPlan)
DataFrame → into_optimized_plan() → Optimized LogicalPlan
DataFrame → create_physical_plan() → ExecutionPlan
```

### Why the Snapshot Matters

**The frozen `SessionState` provides guarantees that matter for production workloads.**

| Guarantee                         | What it means                                       | Why it matters                                                            |
| --------------------------------- | --------------------------------------------------- | ------------------------------------------------------------------------- |
| **Configuration Reproducibility** | Batch size, partitions, timezone frozen at creation | Same performance characteristics even if you change global settings later |
| **Resource Safety**               | Catalogs, tables, UDFs preserved from creation time | Queries don't fail because a dependency was deregistered                  |
| **Point-in-Time Consistency**     | Query start timestamp captured in snapshot          | Functions like [`.now()`] return the same value every execution           |

**Practical implications:**

- **Reproducibility** — re-running a DataFrame happens in the exact same context (critical for debugging)
- **Safety** — you can modify the [`SessionContext`] without breaking existing DataFrames
- **Best practice** — always register UDFs and tables **before** creating DataFrames that rely on them

> **Learn more:** See [SessionContext and SessionState relationship][SessionContext and SessionState] for implementation details.

### Advanced: Converting Between `DataFrame` and `LogicalPlan`

**For most users, the DataFrame API is sufficient. This section is for advanced use cases.**

Sometimes you need direct [`LogicalPlan`] access—custom optimizer rules, query rewriting systems, or programmatic plan inspection. DataFusion lets you move freely between the two:

| Use DataFrame API for...             | Use LogicalPlan directly for... |
| ------------------------------------ | ------------------------------- |
| Standard queries and transformations | Custom optimizer rules          |
| Automatic SessionState management    | Fine-grained plan manipulation  |
| Rapid prototyping                    | Query rewriting systems         |

**Extract and modify plans using [`.into_parts()`]:**

```rust
use datafusion::prelude::*;
use datafusion::error::Result;
use datafusion::logical_expr::LogicalPlanBuilder;

#[tokio::main]
async fn main() -> Result<()> {
    // Create a DataFrame with sample data
    let df = dataframe!(
        "a" => [1, 5, 10, 15],
        "b" => [2, 6, 11, 16]
    )?;

    // Extract state + plan, modify, rebuild DataFrame
    let (state, plan) = df.into_parts();
    let modified_plan = LogicalPlanBuilder::from(plan)
        .filter(col("a").gt(lit(5)))?
        .build()?;
    let new_df = DataFrame::new(state, modified_plan);

    new_df.show().await?;
    Ok(())
}
```

### DataFrame vs. LogicalPlanBuilder

[`DataFrame`] methods are thin wrappers around [`LogicalPlanBuilder`]—they produce identical plans:

| DataFrame method | LogicalPlanBuilder equivalent |
| ---------------- | ----------------------------- |
| `.select()`      | `.project()`                  |
| `.filter()`      | `.filter()`                   |
| `.aggregate()`   | `.aggregate()`                |
| `.join()`        | `.join()`                     |

This means you can mix approaches—use DataFrame for convenience, drop to LogicalPlanBuilder when you need fine-grained control, then wrap back in a DataFrame for execution:

```rust
use datafusion::prelude::*;
use datafusion::error::Result;
use datafusion::logical_expr::LogicalPlanBuilder;

#[tokio::main]
async fn main() -> Result<()> {
    let df = dataframe!("a" => [1, 2, 3], "b" => [4, 5, 6])?;

    // Extract plan, modify with builder, wrap back in DataFrame
    let (state, plan) = df.into_parts();
    let modified = LogicalPlanBuilder::from(plan)
        .filter(col("a").gt(lit(1)))?
        .project(vec![col("b")])?
        .build()?;
    let new_df = DataFrame::new(state, modified);

    new_df.show().await?;
    Ok(())
}
```

> **Further reading:** See [Building Logical Plans](../building-logical-plans.md) for advanced [`LogicalPlanBuilder`] usage.

## Execution Model: Actions vs. Transformations

**Nothing runs until you ask for results.**

DataFusion distinguishes between **transformations** (lazy operations that build a query plan) and **actions** (eager operations that trigger execution). This separation enables whole-query optimization: the optimizer sees your entire pipeline before processing any data, applying rewrites like predicate pushdown and projection pruning. Understanding when execution actually happens—and what triggers it—is key to writing efficient queries and debugging performance issues.

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

### The Async Runtime: Understanding Tokio

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
   Each operator yields control after processing a batch, preventing any single task from monopolizing a thread (see [Cooperative scheduling module])

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
> - [Thread Scheduling documentation] — complete technical details

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
For in-memory data (like [`dataframe!]` a datafusion macro), optimizations focus on operation order and algorithm selection. For file-based sources (Parquet, CSV), additional optimizations kick in—predicate pushdown to skip row groups, projection pushdown to read only needed columns. See [Creation-Time Optimizations](creating-dataframes.md#creation-time-optimizations) for file-specific tuning.

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

<!-- TODO: To be sorted references -->

[Tokio]: https://tokio.rs
[`Stream`]: https://docs.rs/futures/latest/futures/stream/trait.Stream.html
[`target_partitions`]: ../../user-guide/configs.md#target_partitions
[Cooperative scheduling module]: https://docs.rs/datafusion-physical-plan/latest/datafusion_physical_plan/coop/index.html
[Thread Scheduling documentation]: https://docs.rs/datafusion/latest/datafusion/index.html#thread-scheduling-cpu--io-thread-pools-and-tokio-runtimes
[Using Rustlang's Async Tokio Runtime for CPU-Bound Tasks]: https://www.influxdata.com/blog/using-rustlangs-async-tokio-runtime-for-cpu-bound-tasks/
[SessionContext and SessionState]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#relationship-between-sessioncontext-sessionstate-and-taskcontext
[blog: Parquet Pruning]: https://datafusion.apache.org/blog/2025/03/20/parquet-pruning/
[blog: Filter Pushdown]: https://datafusion.apache.org/blog/2025/03/21/parquet-pushdown/
[Rayon]: https://docs.rs/rayon/latest/rayon/
[`dataframe!]: https://docs.rs/datafusion/latest/datafusion/macro.dataframe.html
[optimizers_index]: https://docs.rs/datafusion/latest/datafusion/optimizer/index.html
[`.explain()` examples]: ../../user-guide/explain-usage.md
[Parquet Pruning]: https://datafusion.apache.org/blog/2025/03/20/parquet-pruning/
[PruningPredicate Boolean Tri-state logic section]: https://docs.rs/datafusion-pruning/latest/datafusion_pruning/struct.PruningPredicate.html#background
[nulls via a bitmap]: https://arrow.apache.org/docs/format/Columnar.html#validity-bitmaps
[`PruningPredicate`]: https://docs.rs/datafusion-pruning/latest/datafusion_pruning/struct.PruningPredicate.html
[`datafusion::common::NullEquality`]: https://docs.rs/datafusion-common/latest/datafusion_common/enum.NullEquality.html
[`GroupsAccumulator::accumulate`]: https://docs.rs/datafusion-functions-aggregate-common/latest/datafusion_functions_aggregate_common/aggregate/groups_accumulator/fn.accumulate.html
[`DataFrame::sort()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.sort
[configuration]: ../../user-guide/configs.md#default-null-ordering
[`Field`]: https://docs.rs/arrow/latest/arrow/datatypes/struct.Field.html
[field-level metadata]: https://docs.rs/arrow/latest/arrow/datatypes/struct.Field.html#method.metadata
[with_metadata]: https://docs.rs/arrow/latest/arrow/datatypes/struct.Field.html#method.with_metadata
[dataframe-paper]: https://arxiv.org/abs/2001.00888
[`sqlparser`]: https://crates.io/crates/sqlparser
[configurable dialect]: https://docs.rs/datafusion/latest/datafusion/common/config/enum.Dialect.html
[Architecture section]: https://docs.rs/datafusion/latest/datafusion/#architecture
[Abstract Syntax Tree (AST)]: https://en.wikipedia.org/wiki/Abstract_syntax_tree

<!-- TODO: Core methods and types -->

[`ConfigOptions`]: https://docs.rs/datafusion/latest/datafusion/common/config/struct.ConfigOptions.html

<!-- TODO: Functional  methods -->

[`is_null()`]: https://docs.rs/datafusion/latest/datafusion/prelude/fn.is_null.html
[`is_not_null()`]: https://docs.rs/datafusion/latest/datafusion/prelude/fn.is_not_null.html
[`coalesce()`]: https://docs.rs/datafusion/latest/datafusion/prelude/fn.coalesce.html
[`nullif()`]: https://docs.rs/datafusion/latest/datafusion/prelude/fn.nullif.html
[`nvl()`]: https://docs.rs/datafusion/latest/datafusion/prelude/fn.nvl.html
[`ifnull()`]: https://docs.rs/datafusion/latest/datafusion/prelude/fn.ifnull.html

<!-- TODO: Dataframe creation and manipulation methods -->

[`.read_parquet()`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.read_parquet
[`.read_csv()`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.read_csv
[`.sql()`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.sql
[`.table()`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.table
[`.register_table()`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.register_table
[`.register_parquet()`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.register_parquet
[`.deregister_table()`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.deregister_table
[`.with_config()`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.with_config
[`.state()`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.state
[`.catalog()`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.catalog
[`.catalog_names()`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.catalog_names
[`.register_udf()`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.register_udf
[`.register_udaf()`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.register_udaf
[`.register_table_provider()`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.register_table_provider

<!-- TODO: Add something about the execution plan and how Dataframe affec this ! -->

<!-- TODO: THink of  removint this or adding content to the ### In Practice: Two Paths, One Result
  -->

### Mixing SQL and DataFrames

Since both APIs produce the same [`LogicalPlan`], you can freely combine them. A common pattern: prepare data with the DataFrame API, then register the result as a view for SQL queries.

**When to use each:**

- **DataFrame API**: Dynamic query building, conditional logic, integration with Rust code
- **SQL**: Complex analytics (window functions, CTEs), user-provided queries, readable aggregations

```rust
use datafusion::prelude::*;
use datafusion::error::Result;

#[tokio::main]
async fn main() -> Result<()> {
    let ctx = SessionContext::new();

    // 1. Prepare data programmatically with the DataFrame API
    let sales = dataframe!(
        "product_id" => [1, 2, 1, 3, 2, 1],
        "region" => ["EMEA", "EMEA", "APAC", "EMEA", "EMEA", "EMEA"],
        "revenue" => [100, 200, 150, 300, 250, 175]
    )?
    .filter(col("region").eq(lit("EMEA")))?;

    // 2. Register the DataFrame as a temporary view.
    // .into_view() consumes 'sales'; use .clone() if needed later (clones the plan handle, not the data)
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
    Ok(())
}
```

> **Tip:** Use [`.into_view()`] to register any DataFrame as a table for SQL queries. The optimizer sees the combined pipeline as one plan.

## Historical Context & Future Direction

Understanding where DataFusion comes from—and where it's going—helps you make informed architectural decisions. This section covers the execution model heritage, DataFusion's role in the broader ecosystem, and the active roadmap.

### Execution Model: Vectorized Volcano

DataFusion implements a **vectorized Volcano model**, combining the classic iterator-based execution with modern batch processing. As described in the [DataFusion blog on repartitioning][volcano-blog]:

> "DataFusion implements a vectorized Volcano Model, similar to other state of the art engines such as ClickHouse. The Volcano Model is built on the idea that each operation is abstracted into an operator, and a DAG can represent an entire query. Each operator implements a `next()` function that returns a batch of tuples."

**The evolution:**

| Era   | Model                      | Characteristics                                                   |
| ----- | -------------------------- | ----------------------------------------------------------------- |
| 1990s | Volcano (Graefe)           | Pull-based iterators, single-tuple `next()` calls                 |
| 2010s | Vectorized                 | Batch processing (1000+ rows), SIMD, cache efficiency             |
| Today | Vectorized Volcano + Async | Batches (8192 rows default) + Tokio work-stealing for parallelism |

DataFusion's hybrid approach provides:

- **Composability**: Operators form a DAG; each calls `poll_next()` on children
- **Vectorized efficiency**: Processing batches enables SIMD and cache locality
- **Async concurrency**: Tokio's work-stealing scheduler parallelizes across partitions

This is why all DataFrame actions are `async fn`—they participate in cooperative scheduling rather than blocking threads.

[volcano-blog]: https://datafusion.apache.org/blog/2025/12/15/avoid-consecutive-repartitions/#parallel-execution-in-datafusion

### The LLVM Parallel: Ecosystem Role

The [SIGMOD 2024 paper][sigmod-paper] draws a parallel between DataFusion and LLVM—not in internal architecture, but in **ecosystem role**. From Section 4.1:

> "Just as LLVM's modular design catalyzed the development of system programming languages, DataFusion catalyzes the development of data systems."

**The transformation:**

| Aspect      | Compiler World                                           | Data Systems World                                                                   |
| ----------- | -------------------------------------------------------- | ------------------------------------------------------------------------------------ |
| **Before**  | Monolithic compilers (IBM, Solaris, AIX)                 | Monolithic databases (Oracle, SQL Server, DB2)                                       |
| **After**   | Modular compilers sharing LLVM (Rust, Swift, Zig, Julia) | Modular data systems sharing DataFusion (InfluxDB 3.0, GreptimeDB, Coralogix, Comet) |
| **Benefit** | Language authors focus on language features              | Data system authors focus on domain-specific features                                |

**What this enables:** <br>
Query engine authors can focus on value-added, domain-specific features while DataFusion provides SQL parsing, plan representations, optimizations, storage format support, and standard relational operators.

**What this is NOT:** <br>
DataFusion does not use LLVM IR or JIT compilation internally. The parallel is about the role DataFusion plays as reusable infrastructure—like LLVM is for compilers, DataFusion is for query engines.

[sigmod-paper]: https://dl.acm.org/doi/10.1145/3626246.3653368

### Future Roadmap

DataFusion is actively evolving. Key initiatives include:

- **[Epic #12723: Reliable Foundation][epic-12723]** — Separating Frontend (SQL, DataFrame API) from Core (dialect-agnostic IR, optimizers) from Execution (physical planning). This layering makes DataFusion more reusable as infrastructure.

- **[Epic #12644: Extension Types][epic-12644]** — User-defined types that flow through the entire query lifecycle, enabling domain-specific type systems.

- **Logical/Physical Type Decoupling** — Separating logical types (what the query describes) from physical types (how data is stored), enabling runtime-adaptive execution.

For the complete roadmap and quarterly planning discussions, see the [Contributor Guide: Roadmap][roadmap].

[epic-12723]: https://github.com/apache/datafusion/issues/12723
[epic-12644]: https://github.com/apache/datafusion/issues/12644
[roadmap]: https://datafusion.apache.org/contributor-guide/roadmap.html

## Architectural Fit: When to Use DataFusion

DataFusion excels at **analytical workloads** over large, immutable datasets—the OLAP (Online Analytical Processing) pattern. Understanding when it's the right tool prevents architectural mismatches.

**DataFusion shines when:**

- Scanning and aggregating millions to billions of rows
- Queries touch many columns (star schemas, data warehousing)
- Data is immutable or append-only (Parquet, Delta Lake, Iceberg)
- You need embeddable query execution (edge analytics, custom databases)
- Building domain-specific query engines on reusable infrastructure

**Consider alternatives when:**

| Use Case                  | Why DataFusion May Not Fit                 | Better Alternatives                                        |
| ------------------------- | ------------------------------------------ | ---------------------------------------------------------- |
| Single-row lookups by key | Columnar format overhead; no index support | PostgreSQL, DynamoDB, Redis                                |
| Sub-millisecond latency   | Query planning overhead (~1-10ms minimum)  | Pre-compiled queries, KV stores                            |
| Heavy UPDATE/DELETE       | Designed for immutable, append-only data   | OLTP database, or lakehouse format (Delta, Iceberg) on top |
| Small datasets for EDA    | Simpler APIs may suffice                   | pandas, Polars                                             |
| Real-time streaming       | Batch-oriented execution model             | Kafka Streams, Flink, RisingWave                           |

> **Rule of thumb:** <br>
> "Find one row by ID" → use a database with indexes. <br>
> "Aggregate a billion rows" → use DataFusion.

**The OLAP sweet spot:** <br>
DataFusion is optimized for read-heavy analytical queries where you scan large amounts of data, filter aggressively, and aggregate results. If your workload involves frequent small writes, point lookups, or requires sub-millisecond response times, a different architecture is likely a better fit.

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

[`SessionContext`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html
[`SessionState`]: https://docs.rs/datafusion/latest/datafusion/execution/session_state/struct.SessionState.html
[`DataFrame`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html
[`LogicalPlan`]: https://docs.rs/datafusion-expr/latest/datafusion_expr/logical_plan/enum.LogicalPlan.html
[`ExecutionPlan`]: https://docs.rs/datafusion/latest/datafusion/physical_plan/trait.ExecutionPlan.html
[`RecordBatch`]: https://docs.rs/arrow/latest/arrow/record_batch/struct.RecordBatch.html
[Arrow Introduction]: ../../user-guide/arrow-introduction.md
[Arrow Columnar Format]: https://arrow.apache.org/docs/format/Intro.html#arrow-columnar-format
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

[`.select()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.select
[`.filter()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.filter
[`.aggregate()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.aggregate
[`.join()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.join
[`.limit()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.limit
[`.sort()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.sort
[`.with_column()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.with_column
[`.schema()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.schema
[`.logical_plan()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.logical_plan
[`.collect()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.collect
[`.show()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.show
[`.execute_stream()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.execute_stream
[`.count()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.count
[`.cache()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.cache
[`.explain()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.explain
[`.write_parquet()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.write_parquet
[`.write_csv()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.write_csv
[`.write_json()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.write_json
[`.write_table()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.write_table
[`.into_parts()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.into_parts
[`.into_unoptimized_plan()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.into_unoptimized_plan
[`.into_optimized_plan()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.into_optimized_plan
[`.create_physical_plan()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.create_physical_plan
[`.into_view()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.into_view
