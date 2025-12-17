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

# DataFrame Transformations

**The “life” phase of the DataFrame lifecycle: build and refine a lazy query plan.**

Transformations are where you shape and analyze data: once you [create](./creating-dataframes.md) a DataFrame, you can filter, select, join, aggregate, sort, and enrich data by composing methods that build a [`LogicalPlan`]. In the [DataFrame lifecycle metaphor](./index.md#the-dataframe-lifecycle), this is the "life" phase—execution and persistence happen later (see [Writing & Executing](./writing-dataframes.md)).

This guide compares the DataFrame API to DataFusion's [SQL API](../using-the-sql-api.md). Both compile to the same [`LogicalPlan`], so the choice is primarily about ergonomics: DataFrames shine for programmatic composition and IDE tooling; SQL shines for concise, declarative queries and portability. For the conceptual model, see [DataFrame Concepts](./concepts.md#introduction).

> **Style Note:** In this guide, all code elements are highlighted with backticks. DataFrame methods are written as `.method()` (e.g., `.select()`) to reflect the chaining syntax central to the API. This distinguishes them from standalone functions (e.g., `col()`) and static constructors (e.g., `SessionContext::new()`). Rust types are formatted as `TypeName` (e.g., `SchemaRef`).

```{contents}
:local:
:depth: 2
```

Transformations allow you to shape, filter, enrich, and analyze your data through a series of composable, type-safe operations. Unlike SQL, where queries are often monolithic strings, DataFrames allow you to **build queries programmatically**. This approach shines when you need to:

- **Chain operations** into readable, logical pipelines.
- **Build queries dynamically** based on runtime conditions or configuration.
- **Leverage the Rust type system** to catch errors at compile time.
- **Reuse logic** by encapsulating complex transformations into functions.

The following diagram illustrates the conceptual position in the DataFrame architecture. Both the SQL API and the DataFrame API share the same [`SessionContext`] and converge to the same [`LogicalPlan`] with column-based operations.<br>

```text
           +------------------+
           |  SessionContext  |
           +------------------+
                    |
          +---------+---------+
          |                   |
          v                   v
    +-----------+       +-------------+
    |  SQL API  |       |  DataFrame  |
    | ctx.sql() |       |  API (lazy) |
    +-----------+       +-------------+
          \                   /
           \                 /
            v               v
       +------------------------+
       |      LogicalPlan       |  ← Same!
       +------------------------+
                  |
                  | Further processing...
                  v
```

For a deeper dive, see:

- [DataFrame Concepts](./concepts.md#introduction)
- [SIGMOD 2024 Paper][datafusion paper],
- [architecture overview on docs.rs][docs.rs]

For most data transformations, the choice between SQL-API and DataFrame-APIs is primarily about ergonomics—both produce identical execution plans. However, the DataFrame API is more than just "SQL with different syntax." The DataFrame API is a programmatic **builder** for query plans, whereas the SQL API is a declarative **parser** for query strings. Research on [DataFrame Algebra][dataframe algebra] shows that the DataFrame paradigm offers a distinct way of _expressing_ data transformations. These patterns were established by the [pandas library][pandas] and continuously refined by projects like [Apache Spark] for efficient parallel, multi-node computation.

## API Ergonomics: DataFrame vs SQL

**The DataFusion DataFrame-API and SQL-API share the same execution engine—but the _experience_ of writing them is fundamentally different.**

This table maps SQL operations to their DataFrame equivalents. Methods marked **Unique** have no direct SQL counterpart—these are where the DataFrame API provides capabilities beyond standard SQL.

| Category        |       SQL Operation        |             DataFrame Method             | Key Differences & Superpowers                                                               |
| --------------- | :------------------------: | :--------------------------------------: | ------------------------------------------------------------------------------------------- |
| **Filtering**   |         [`WHERE`]          |              [`.filter()`]               | Chainable predicates; programmatic filter building.                                         |
| **Selection**   |         [`SELECT`]         |   [`.select()`], [`.select_columns()`]   | [`.select()`] supports expressions; [`.select_columns()`] is a simple projection.           |
| **Selection**   |       [`AS`] (alias)       | [`.alias()`], [`.with_column_renamed()`] | [`.with_column_renamed()`] renames existing columns without expressions.                    |
| **Mutation**    |   (No direct equivalent)   |            [`.with_column()`]            | **Unique**: Add or replace a column while keeping all others.                               |
| **Aggregation** |        [`GROUP BY`]        |             [`.aggregate()`]             | Groups data and applies aggregate functions.                                                |
| **Joins**       |          [`JOIN`]          |       [`.join()`], [`.join_on()`]        | [`.join_on()`] allows arbitrary boolean expressions for join conditions.                    |
| **Sorting**     |        [`ORDER BY`]        |               [`.sort()`]                | Sort by one or multiple expressions.                                                        |
| **Limiting**    |   [`LIMIT`] / [`OFFSET`]   |               [`.limit()`]               | [`limit(skip, fetch)`][`.limit()`] handles both offset and limit.                           |
| **Set Ops**     |   [`UNION ALL`][`UNION`]   |               [`.union()`]               | concatenates DataFrames.                                                                    |
| **Set Ops**     |         [`UNION`]          |          [`.union_distinct()`]           | concatenates and removes duplicates.                                                        |
| **Set Ops**     |   (No direct equivalent)   |           [`.union_by_name()`]           | **Unique**: Unions based on column names, forgiving column order mismatches.                |
| **Distinct**    |   [`DISTINCT`][`SELECT`]   |             [`.distinct()`]              | Removes duplicate rows based on all columns.                                                |
| **Distinct**    | [`DISTINCT ON`] (Postgres) |            [`.distinct_on()`]            | **Unique**: Deduplicates based on specific columns, keeping the "first" row per sort order. |

> **Note:** While you can mix SQL and DataFrames (see [Concepts](../concepts.md)), mastering these native methods unlocks the full power of programmatic data manipulation.

### The Methodical Differences: Why DataFrames Feel Different

**The DataFusion DataFrame API isn't SQL with different syntax—it's a fundamentally different way of expressing data transformations.**

While both produce the same [`LogicalPlan`], DataFrames integrate with Rust's type system, control flow, and tooling in ways SQL strings cannot. As the [Towards Scalable Dataframe Systems][dataframe algebra] paper notes, SQL's declarative nature makes it "awkward to develop and debug queries in a piecewise, modular fashion."

| Aspect              |             DataFrame API              | SQL API                          |
| ------------------- | :------------------------------------: | :------------------------------- |
| **Interface**       | Method chaining (`.select().filter()`) | Declarative string (`SELECT...`) |
| **Error Detection** |     Compile-time (syntax & types)      | Runtime (parsing)                |
| **Composability**   |   High (variables, loops, functions)   | Low (string concatenation)       |
| **IDE Tooling**     | Full support (autocomplete, refactor)  | Limited (opaque strings)         |
| **Custom Logic**    | Can inject custom `LogicalPlan` nodes  | Limited to SQL grammar           |

Consider building a search API where filters depend on user input:

**SQL approach** — string concatenation:

```rust
fn main() {
    let filter_department: Option<&str> = Some("Sales");

    let mut query = "SELECT * FROM employees WHERE 1=1".to_string();
    if let Some(dept) = filter_department {
        query.push_str(&format!(" AND department = '{}'", dept));
    }
}
```

This pattern has three problems:

1. **Injection vulnerability** — if `dept` contains [`'; DROP TABLE students; --`](https://xkcd.com/327/), you're in trouble.
2. **Runtime-only errors** — typos like `"deprtment"` won't surface until execution.
3. **The `WHERE 1=1` hack** — exists solely to simplify conditional string building.

**DataFrame approach** — type-safe, composable:

```rust
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let employees_df = dataframe!("department" => ["Sales", "Engineering"])?;
    let filter_department: Option<&str> = Some("Sales");

    let mut result = employees_df;
    if let Some(dept) = filter_department {
        result = result.filter(col("department").eq(lit(dept)))?;
    }
    result.show().await?;
    Ok(())
}
```

Values pass through [`lit()`], never interpolated into strings — injection-proof by design. The optimizer automatically combines multiple [`.filter()`] calls into a single predicate.

### Finding Balance: When to Use Which

**Both APIs share far more than they differ.** They use the same expressions ([`col()`], [`lit()`], [`sum()`]), the same optimizer, and produce identical execution plans. The choice isn't "DataFrame vs SQL" — it's about picking the right tool for each situation.

#### DataFrame API Strengths

| Strength                     | What it means                                                                |
| ---------------------------- | ---------------------------------------------------------------------------- |
| **Type safety**              | Rust's type system catches errors before runtime                             |
| **Compile-time validation**  | No runtime "column not found" surprises — the compiler checks your code      |
| **No SQL injection risk**    | Values pass through [`lit()`], never string-interpolated — secure by design  |
| **IDE support**              | Full autocomplete, refactoring, go-to-definition for methods and expressions |
| **Programmatic composition** | Build queries with loops, conditionals, and reusable functions               |

#### Where SQL Shines

The DataFrame API isn't always the best choice. Be honest about trade-offs:

| Feature              | SQL API Strength                           | DataFrame Consideration             |
| -------------------- | ------------------------------------------ | ----------------------------------- |
| **Window Functions** | Concise `OVER (PARTITION BY ... ORDER BY)` | More verbose builder pattern        |
| **Complex Logic**    | Readable `CASE WHEN`, `GROUPING SETS`      | Deeply nested function calls        |
| **Prototyping**      | Instant feedback via REPL/CLI              | Requires Rust compilation cycle     |
| **Tool Integration** | Works with BI tools, JDBC/ODBC             | Requires custom integration         |
| **Team Familiarity** | Universal SQL knowledge                    | Rust + DataFrame API learning curve |

#### When Row-Based ['TableProviders'] Outperform Columnar

DataFusion's columnar engine excels at analytical workloads, but **row-based databases (i.e. Postgres, MySQL, Oracle...) via TableProvider can be faster** for certain operations:

When to use [`TableProvider`] instead of DataFusion's columnar engine:

| Operation               | Row-Based DB Wins When...                    | Why                                               |
| :---------------------- | :------------------------------------------- | :------------------------------------------------ |
| **Point lookups**       | `WHERE id = 123` on indexed column           | B-tree index → O(log n), no scan needed           |
| **Small result sets**   | Highly selective filters return few rows     | Less data to transfer than scanning columns       |
| **Set operations**      | DISTINCT, UNION, INTERSECT on indexed tables | DB's hash/sort algorithms + indexes already built |
| **Transactional reads** | ACID guarantees required                     | Row DBs are built for transactional consistency   |

When to use DataFusion's columnar engine instead of [`TableProvider`]:

| Operation                    | Columnar (DataFusion) Wins When...     | Why                                            |
| :--------------------------- | :------------------------------------- | :--------------------------------------------- |
| **Projection**               | Selecting few columns from wide tables | Reads only requested columns (97% I/O savings) |
| **Full scans + aggregation** | COUNT, SUM, AVG over millions of rows  | Vectorized ops on compressed data              |
| **Complex predicates**       | Multi-column filters, OR conditions    | Parallel evaluation, no index limitations      |
| **Cross-source joins**       | Federating data from multiple sources  | DataFusion handles the coordination            |

**Practical guidance:**

- **Push down what you can:** DataFusion's TableProvider interface supports predicate and projection pushdown — filters and column lists reach the source DB
- **Consider the transfer cost:** If 90% of data would be filtered at the source, let the source do it
- **Profile, don't assume:** Use [`.explain()`] to see what gets pushed down vs. executed in DataFusion

#### The Bigger Picture: DataFusion's Architecture

DataFusion is a **columnar query engine** optimized for analytical workloads. But not every operation suits columnar processing — some queries perform better on row-based systems like PostgreSQL or Oracle.

This is where DataFusion's [`TableProvider`] abstraction shines: you can **connect diverse data sources** and let each system do what it does best. Query your PostgreSQL tables alongside Parquet files, and DataFusion orchestrates the execution. The DataFrame API and SQL API are just two ways to express queries over this unified architecture.

> **Think outside the box:** If a particular query pattern is awkward in DataFrames _and_ slow in DataFusion's columnar engine, consider whether a row-based system or specialized database might be the right tool — and use DataFusion to connect them.

#### Use Case Recommendations

| Use Case                                   | Recommended API | Why                                      |
| ------------------------------------------ | :-------------: | ---------------------------------------- |
| Ad-hoc exploration, quick queries          |     SQL-API     | Familiar syntax, zero boilerplate        |
| Dynamic filters, user-driven queries       |  DataFrame-API  | Type-safe composition, no injection risk |
| Complex business logic, reusable pipelines |  DataFrame-API  | Composable functions, testable units     |
| Migrating existing SQL queries             |     SQL-API     | Copy-paste, immediate results            |
| IDE-heavy development                      |  DataFrame-API  | Autocomplete, refactoring support        |
| Complex window functions                   |     SQL-API     | More concise, readable syntax            |
| Rapid prototyping                          |     SQL-API     | No compilation required                  |

The true power emerges when you **mix both APIs**: use SQL for complex joins or window functions where declarative syntax shines, then switch to DataFrames for dynamic filtering or pipeline composition. DataFusion makes this seamless — a SQL query returns a DataFrame you can continue transforming.

### What's Ahead

This guide walks through DataFrame transformations with a practical lens:

1. **[Shared Transformations](#shared-transformations-a-data-cleaning-journey)** — A narrative data-cleaning journey showing transformations in context, not isolation.
2. **[Deep Dive: Transformation Reference](#deep-dive-transformation-reference)** — The shared expression vocabulary: filtering, selection, aggregation, joins — concepts that apply to both SQL and DataFrames.
3. **[Advanced DataFrame Patterns](#advanced-dataframe-patterns)** — Unique DataFrame capabilities: `.with_column()`, `.union_by_name()`, window functions, and more.
4. **[Mixing SQL and DataFrames](#mixing-sql-and-dataframes)** — Hybrid workflows that leverage the strengths of both APIs.

---

<!-- TODO: Place in the Dataframe user guide -->

## Shared Transformations: A Data Cleaning Journey

**These are the core operations that exist in both SQL and the DataFrame API — same logic, different syntax.**

If you know SQL, you already know _what_ these operations do. The difference is _how_ you express them. The naming conventions come from different traditions: SQL follows the ANSI standard, while DataFrame methods inherit from [Apache Spark] and [pandas] — hence `ORDER BY` becomes `.sort()`, and `WHERE` becomes `.filter()`.

| SQL Clause                         | DataFrame Method                             | Origin / Note                              |
| ---------------------------------- | -------------------------------------------- | ------------------------------------------ |
| [`WHERE condition`][`WHERE`]       | [`.filter(expr)`][`.filter()`]               | Functional programming tradition           |
| [`SELECT columns`][`SELECT`]       | [`.select(exprs)`][`.select()`]              | Same concept, expression-based             |
| [`SELECT col AS name`][`AS`]       | [`.alias("name")`][`.alias()`]               | Rename via expression                      |
| [`GROUP BY ... AGG()`][`GROUP BY`] | [`.aggregate(groups, aggs)`][`.aggregate()`] | Explicit grouping + aggregation separation |
| [`ORDER BY col`][`ORDER BY`]       | [`.sort(exprs)`][`.sort()`]                  | Spark naming convention                    |
| `ORDER BY col DESC`                | `.sort(col.sort(false, true))`               | `(ascending, nulls_first)` parameters      |
| `LIMIT n OFFSET m`                 | [`.limit(skip, Some(fetch))`][`.limit()`]    | Combined into single method                |
| `DISTINCT`                         | [`.distinct()`]                              | Same concept                               |
| `UNION ALL`                        | [`.union()`]                                 | Keeps duplicates                           |
| `UNION`                            | [`.union_distinct()`]                        | Removes duplicates                         |

<!--TODO: References !-->

> **Going deeper:** For comprehensive method documentation, see [docs.rs]. For SQL semantics, refer to [PostgreSQL docs] or [Spark SQL documentation][spark docs].

Rather than covering each operation in isolation, we'll demonstrate them together in a realistic **data cleaning workflow** — the kind of pipeline you'd actually build. This approach shows how DataFrame **ergonomics** shine: operations like [`.describe()`] give you summary statistics in one line, while SQL would require multiple aggregations (see below) and many more.

Along the way, you'll see **method chaining** in action — more on that pattern in [Advanced DataFrame Patterns](#advanced-dataframe-patterns). For a full comparison of when to use DataFrames vs SQL, see [Finding Balance](#finding-balance-when-to-use-which) above.

<!-- TODO moeve to the user guide Dataframes.md -->

## Deep Dive: Transformation Reference

**Every DataFrame transformation has a SQL equivalent—but the experience of writing them differs fundamentally.**

This reference covers operations that exist in _both_ the DataFrame API and SQL. For each operation, you'll find:

- **SQL equivalent** — The familiar SQL syntax as an anchor point
- **DataFrame implementation** — How to express it using method chaining
- **Trade-off analysis** — Where DataFrame shines vs where SQL shines
- **Quick Debug tips** — Common pitfalls and how to diagnose them

Use this section when you know _what_ operation you need but want to understand _how_ to express it idiomatically in DataFusion—and when to prefer one API over the other.

_For operations unique to DataFrames (no SQL equivalent), see [DataFrame-Unique Methods](#dataframe-unique-methods)._

**Jump to:**

| Data Shaping                                                  | Filtering & Combining                               | Advanced                              |
| ------------------------------------------------------------- | --------------------------------------------------- | ------------------------------------- |
| [Selection and Projection](#selection-and-projection-mastery) | [Filtering](#filtering-excellence)                  | [Window Functions](#window-functions) |
| [Aggregation](#aggregation-patterns)                          | [Joins](#when-dataframes-collide-join-patterns)     | [Subqueries](#subqueries)             |
| [Sorting and Limiting](#sorting-and-limiting)                 | [Set Operations](#set-operations-and-deduplication) | [Reshaping Data](#reshaping-data)     |

### Selection and Projection Mastery

**Projection controls the shape of your output—choosing which columns to keep, computing derived values, and reducing memory footprint by discarding what you don't need.**

DataFusions DataFrame-API provides methods for every projection need: simple name-based selection via [`.select_columns()`], expression-based computation with [`.select()`], adding columns with [`.with_column()`], renaming via [`.with_column_renamed()`], and removal with [`.drop_columns()`]. Projection pushdown ensures only requested columns are read from the data source.

[`.drop_columns()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.drop_columns

**SQL equivalent:**

```sql
SELECT
col_a,
col_b AS col_b_renamed,
col_a + col_b AS col_sum
FROM table
```

> **Trade-off: DataFrame vs SQL**
>
> - **DataFrame shines:** Type-safe column references catch typos at compile time; programmatic column selection from schema; projection pushdown happens automatically
> - **SQL shines:** Familiar [`SELECT`] syntax; more readable for simple projections; [`SELECT *`][`SELECT`] for quick exploration

**Performance note:**
<br> Projection is where **columnar vs row-based TableProviders** differ most — columnar sources (i.e. Parquet, Delta Lake ...) read only requested columns, while row-based sources (i.e. Postgres, MySQL, Oracle...) read full rows and discard unwanted columns during transfer.

#### Basic Selection

**Which method to use:**

- **[`.select_columns()`]** — Pass column names as strings: `select_columns(&["a", "b"])`
- **[`.select()`]** — Pass expressions for computation: `select(vec![col("a"), (col("b") * lit(2)).alias("b_doubled")])`

Use [`.select_columns()`] when you just need existing columns by name. Use [`.select()`] when you need to compute new values, rename with [`.alias()`], or apply functions.

```rust
use datafusion::prelude::*;
use datafusion::assert_batches_eq;

// Creating the dataframe with the dataframe! macro
#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let sample_df = dataframe!(
        "product" => ["Laptop", "Mouse", "Keyboard"],
        "price" => [1200, 25, 75],
        "quantity" => [5, 50, 30],
        "category" => ["Electronics", "Accessories", "Accessories"]
    )?;

    // select_columns(): simple name-based selection
    let result = sample_df.select_columns(&["product", "price"])?.collect().await?;

    assert_batches_eq!(
        &[
            "+----------+-------+",
            "| product  | price |",
            "+----------+-------+",
            "| Laptop   | 1200  |",
            "| Mouse    | 25    |",
            "| Keyboard | 75    |",
            "+----------+-------+",
        ],
        &result
    );

    Ok(())
}
```

#### Intermediate: Expressions and Computed Columns

[`.select()`] accepts any expression—arithmetic, conditionals, function calls—letting you compute new columns inline. Use [`.alias()`] to name computed results, [`.with_column()`] to add columns while keeping existing ones, and [`.with_column_renamed()`] to rename without recomputing.

```rust
use datafusion::prelude::*;
use datafusion::assert_batches_eq;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let sample_df = dataframe!(
        "product" => ["Laptop", "Mouse", "Keyboard"],
        "price" => [1200, 25, 75],
        "quantity" => [5, 50, 30],
        "category" => ["Electronics", "Accessories", "Accessories"]
    )?;

    // select() with computed columns — replaces schema, only keeps what you list
    let df = sample_df.clone().select(vec![
        col("product"),
        (col("price") * col("quantity")).alias("revenue"),
    ])?;
    assert_batches_eq!(&[
        "+----------+---------+",
        "| product  | revenue |",
        "+----------+---------+",
        "| Laptop   | 6000    |",
        "| Mouse    | 1250    |",
        "| Keyboard | 2250    |",
        "+----------+---------+",
    ], &df.clone().collect().await?);

    // with_column() adds column while keeping all existing
    let df = df.with_column("tag", lit("2026"))?;
    assert_batches_eq!(&[
        "+----------+---------+------+",
        "| product  | revenue | tag  |",
        "+----------+---------+------+",
        "| Laptop   | 6000    | 2026 |",
        "| Mouse    | 1250    | 2026 |",
        "| Keyboard | 2250    | 2026 |",
        "+----------+---------+------+",
    ], &df.clone().collect().await?);

    // with_column_renamed() renames without recomputing
    let df = df.with_column_renamed("revenue", "total")?;
    assert_batches_eq!(&[
        "+----------+-------+------+",
        "| product  | total | tag  |",
        "+----------+-------+------+",
        "| Laptop   | 6000  | 2026 |",
        "| Mouse    | 1250  | 2026 |",
        "| Keyboard | 2250  | 2026 |",
        "+----------+-------+------+",
    ], &df.clone().collect().await?);

    // drop_columns() removes specific columns
    let df = df.drop_columns(&["tag"])?;
    assert_batches_eq!(&[
        "+----------+-------+",
        "| product  | total |",
        "+----------+-------+",
        "| Laptop   | 6000  |",
        "| Mouse    | 1250  |",
        "| Keyboard | 2250  |",
        "+----------+-------+",
    ], &df.collect().await?);

    Ok(())
}
```

> _Quick Debug_: **Columns missing after [`.select()`]?** <br>
> Unlike [`.with_column()`], [`.select()`] only keeps columns you explicitly list.

> **Ugly column names?** Without [`.alias()`], column names are the expression's string representation: `col("a") * col("b")` becomes `"a * b"`, `col("x").gt(lit(5))` becomes `"x > Int32(5)"` see [**g**rater **t**hen => `.gt()`][`.gt()`]. Always alias computed columns for readable output.

#### Advanced: Dynamic Column Selection

When column names aren't known until runtime—or you want to select by type—use [`.schema()`] to inspect the DataFrame's structure, then build expressions programmatically. This is where DataFrames shine over SQL: Rust's type system and iterators let you construct queries that would require dynamic SQL generation otherwise.

```rust
use datafusion::prelude::*;
use datafusion::assert_batches_eq;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let sample_df = dataframe!(
        "product" => ["Laptop", "Mouse", "Keyboard"],
        "price" => [1200, 25, 75],
        "quantity" => [5, 50, 30],
        "category" => ["Electronics", "Accessories", "Accessories"]
    )?;

    // Get schema and filter to numeric columns only
    let numeric_cols: Vec<_> = sample_df
        .schema()
        .fields()
        .iter()
        .filter(|f| f.data_type().is_numeric())
        .map(|f| col(f.name()))
        .collect();

    let result = sample_df.clone().select(numeric_cols)?.collect().await?;
    assert_batches_eq!(&[
        "+-------+----------+",
        "| price | quantity |",
        "+-------+----------+",
        "| 1200  | 5        |",
        "| 25    | 50       |",
        "| 75    | 30       |",
        "+-------+----------+",
    ], &result);

    Ok(())
}
```

#### Anti-Pattern: Over-Selection

Selecting all columns with [`col("*")`][`col()`] defeats **projection pushdown**—an optimization where DataFusion tells the data source to only read requested columns. With Parquet files, this can mean reading 2 columns instead of 200, dramatically reducing I/O. Common SQL-Rule of not using [`SELECT *  FROM big_table`][`SELECT`]

```rust
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let sample_df = dataframe!(
        "product" => ["Laptop", "Mouse", "Keyboard"],
        "price" => [1200, 25, 75],
        "quantity" => [5, 50, 30],
        "category" => ["Electronics", "Accessories", "Accessories"]
    )?;

    // ❌ DON'T: Select all columns then filter (defeats projection pushdown)
    // In practice, avoid selecting columns you don't need

    // ✅ DO: Select only what you need
    let good = sample_df.clone()
        .select(vec![col("product"), col("price")])?  // Projection pushdown!
        .filter(col("price").gt(lit(100)))?;

    // 'good' reads less data from columnar sources
    good.show().await?;
    // +---------+-------+
    // | product | price |
    // +---------+-------+
    // | Laptop  | 1200  |
    // +---------+-------+

    Ok(())
}
```

_Quick Debug_: **Column not found error?** DataFusion is case-sensitive. Use [`sample_df.schema().field_names()`][`.schema()`] to list available columns.

#### Escape Hatch: SQL Syntax in Rust

Sometimes SQL syntax is just cleaner—especially for complex [`CASE`] expressions or nested functions. [`.select_exprs()`] parses SQL strings into expressions, giving you SQL's brevity with DataFrame's composability.

```rust
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let sample_df = dataframe!(
        "product" => ["Laptop", "Mouse", "Keyboard"],
        "price" => [1200, 25, 75],
        "quantity" => [5, 50, 30],
        "category" => ["Electronics", "Accessories", "Accessories"]
    )?;

    // SQL syntax inside Rust — parsed at runtime
    sample_df.clone()
        .select_exprs(&[
            "product",
            "price * 1.1 AS taxed_price",
            "CASE WHEN price > 100 THEN 'Premium' ELSE 'Standard' END AS tier"
        ])?
        .show().await?;
    // +----------+-------------+----------+
    // | product  | taxed_price | tier     |
    // +----------+-------------+----------+
    // | Laptop   | 1320.0      | Premium  |
    // | Mouse    | 27.5        | Standard |
    // | Keyboard | 82.5        | Standard |
    // +----------+-------------+----------+

    Ok(())
}
```

> **Warning:** This loses compile-time safety. A typo like `"prodict"` compiles fine but fails at runtime. Use when SQL is genuinely clearer, not as a shortcut to avoid learning the expression API.

> **Going deeper:** See [`expr_api`] for complex expression patterns combining both approaches.

---

**Best Practice: Pick a Lane (or Document the Bridge)**

Mixing method chains with embedded SQL strings violates the [Single Level of Abstraction Principle][SLAP]—readers must context-switch constantly between abstraction levels. Choose _one_ approach:

| Approach                                                | Best For                               | Trade-off                            |
| :------------------------------------------------------ | :------------------------------------- | :----------------------------------- |
| **Full DataFrame**                                      | App logic, refactoring, IDE support    | Type-safe, but more verbose          |
| **Full SQL** via [`ctx.sql()`][`SessionContext::sql()`] | Ad-hoc queries, portability            | Familiar, but no compile-time checks |
| **Documented Constants**                                | Complex expressions reused across code | Traceable, but requires discipline   |

If you choose the third approach, extract SQL strings into named constants with doc comments:

```rust
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    /// Pricing tier based on unit price.
    /// Business rule: ACME-1234
    const TIER_EXPR: &str = "\
        CASE WHEN price > 100 \
             THEN 'Premium' \
             ELSE 'Standard' \
        END AS tier";

    let sample_df = dataframe!(
        "product" => ["Laptop", "Mouse", "Keyboard"],
        "price" => [1200, 25, 75]
    )?;

    let df = sample_df.select_exprs(&["product", "price", TIER_EXPR])?;
    df.show().await?;

    Ok(())
}
```

This makes SQL expressions discoverable, testable, and traceable—rather than buried inline where they drift and multiply.

---

### Filtering Excellence

**Filtering controls which rows survive—applying predicates to discard irrelevant data early, before expensive joins or aggregations consume resources.**

Where [projection](#selection-and-projection-mastery) shapes columns, filtering shapes rows. The [`.filter()`] method accepts any boolean expression built from these building blocks:

| Predicate Type   | Methods                           | Example                                   |
| :--------------- | :-------------------------------- | :---------------------------------------- |
| Comparisons      | [`.gt()`], [`.lt()`], [`.eq()`]   | `col("price").gt(lit(100))`               |
| Logical          | [`.and()`], [`.or()`], [`.not()`] | `condition_a.and(condition_b)`            |
| Set membership   | [`in_list()`]                     | `col("status").in_list(vec![...], false)` |
| Pattern matching | [`.like()`], [`.ilike()`]         | `col("name").like(lit("A%"))`             |
| Range            | [`.between()`]                    | `col("age").between(lit(18), lit(65))`    |

Predicate pushdown ensures filters reach the data source, letting formats like Parquet skip entire row groups.

**SQL equivalent:** [`WHERE condition`][`WHERE`]

> **Trade-off: DataFrame vs SQL**
>
> - **DataFrame shines:** Composable predicates built programmatically, Rust control flow for conditional logic, compile-time column checking, dynamic filters from runtime values
> - **SQL shines:** Familiar `WHERE` syntax, more readable for simple static conditions, clearer `AND`/`OR` precedence

**Performance note:** <br>
DataFusion excels at **predicate pushdown** — filters reach data sources so Parquet skips entire row groups and databases apply indexes. For highly selective point lookups (`WHERE id = 123`) on indexed row-based databases, the source DB may be faster. For complex multi-column predicates or full scans, DataFusion's vectorized evaluation wins.

#### Basic Filtering

**Start simple:** most filters are single-column comparisons. <br>
Build the predicate with [`col()`] for the column, a comparison method like [`.gt()`], and [`lit()`] for the literal value. The pattern reads naturally: `col("price").gt(lit(100))` means "price greater than 100".

```rust
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let sample_df = dataframe!(
        "product" => ["Laptop", "Mouse", "Keyboard"],
        "price" => [1200, 25, 75],
        "quantity" => [5, 50, 30],
        "category" => ["Electronics", "Accessories", "Accessories"]
    )?;

    // Filter: keep only rows where price > 100
    sample_df.clone()
        .filter(col("price").gt(lit(100)))?
        .show().await?;
    // Only Laptop (1200) survives — Mouse (25) and Keyboard (75) are filtered out
    // +---------+-------+----------+-------------+
    // | product | price | quantity | category    |
    // +---------+-------+----------+-------------+
    // | Laptop  | 1200  | 5        | Electronics |
    // +---------+-------+----------+-------------+

    Ok(())
}
```

#### Intermediate: Complex Predicates

**Real-world filters combine multiple conditions.** <br>
Chain predicates with [`.and()`] and [`.or()`], check set membership with [`in_list()`], match patterns with [`.like()`] (case-sensitive) or [`.ilike()`] (case-insensitive), and validate ranges with [`.between()`].

```rust
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let sample_df = dataframe!(
        "product" => ["Laptop", "Mouse", "Keyboard"],
        "price" => [1200, 25, 75],
        "quantity" => [5, 50, 30],
        "category" => ["Electronics", "Accessories", "Accessories"]
    )?;

    // AND/OR: (price > 50 AND quantity < 40) OR product = "Laptop"
    sample_df.clone()
        .filter(
            col("price").gt(lit(50))
                .and(col("quantity").lt(lit(40)))
                .or(col("product").eq(lit("Laptop")))
        )?
        .show().await?;
    // Keyboard matches (price=75 > 50, quantity=30 < 40)
    // Laptop matches via OR clause
    // +---------+-------+----------+-------------+
    // | product | price | quantity | category    |
    // +---------+-------+----------+-------------+
    // | Laptop  | 1200  | 5        | Electronics |
    // | Keyboard| 75    | 30       | Accessories |
    // +---------+-------+----------+-------------+

    Ok(())
}
```

**More examples for predicate patterns:**

```rust
use datafusion::prelude::*;
use datafusion::functions::string::expr_fn::lower;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let sample_df = dataframe!(
        "product" => ["Laptop", "Mouse", "Keyboard"],
        "price" => [1200, 25, 75],
        "quantity" => [5, 50, 30],
        "category" => ["Electronics", "Accessories", "Accessories"]
    )?;

    // IN list: product IN ("Laptop", "Mouse")
    println!("IN list example:");
    sample_df.clone()
        .filter(in_list(col("product"), vec![lit("Laptop"), lit("Mouse")], false))?
        .show().await?;

    // Pattern matching: product LIKE '%board%' (contains "board")
    println!("LIKE example:");
    sample_df.clone()
        .filter(col("product").like(lit("%board%")))?
        .show().await?;

    // Case-insensitive matching: ILIKE or lower()
    // Method 1: .ilike() — SQL's ILIKE equivalent
    println!("ILIKE example:");
    sample_df.clone()
        .filter(col("product").ilike(lit("%BOARD%")))?  // Matches "Keyboard"
        .show().await?;

    // Method 2: Normalize both sides with lower()
    println!("lower() example:");
    sample_df.clone()
        .filter(lower(col("product")).eq(lit("keyboard")))?
        .show().await?;

    // Range: price BETWEEN 50 AND 500
    println!("BETWEEN example:");
    sample_df.clone()
        .filter(col("price").between(lit(50), lit(500)))?
        .show().await?;

    // Null safety: price IS NOT NULL (all rows pass — no nulls in sample_df)
    println!("IS NOT NULL example:");
    sample_df.clone()
        .filter(col("price").is_not_null())?
        .show().await?;

    Ok(())
}
```

> **Operator precedence:** [`.and()`] binds tighter than [`.or()`], just like SQL. Use parentheses (method chaining order) to make intent explicit: `a.and(b).or(c)` means `(a AND b) OR c`.

#### Advanced: Dynamic Filter Building

**This is where DataFrames truly outshine SQL.** When filter criteria come from user input, configuration, or runtime logic, building queries dynamically showcases two critical safety advantages:

1. **Rust's type system catches errors at compile time.** <br> _Misspell a column name?_ <br>
   The compiler tells you. Pass a string where a number is expected? Caught before your code ever runs. With dynamic SQL, these errors surface at runtime—often in production.

2. **SQL injection becomes impossible by design.** <br> Values flow through [`lit()`] as typed data, not string fragments. There's no way for user input like [`"; DROP TABLE users;--"`](https://xkcd.com/327/) to escape into query structure. You don't need to remember to sanitize—the API makes unsafe patterns unrepresentable.

```rust
use datafusion::prelude::*;

/// Builds a filter expression from optional criteria.
/// Returns `lit(true)` if no criteria provided (matches all rows).
fn build_filter(min_price: Option<i32>, max_quantity: Option<i32>) -> Expr {
    let mut conditions: Vec<Expr> = Vec::new();

    if let Some(price) = min_price {
        conditions.push(col("price").gt_eq(lit(price)));
    }

    if let Some(qty) = max_quantity {
        conditions.push(col("quantity").lt_eq(lit(qty)));
    }

    // Fold conditions with AND; default to lit(true) if empty
    conditions
        .into_iter()
        .reduce(|acc, cond| acc.and(cond))
        .unwrap_or_else(|| lit(true))
}

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let sample_df = dataframe!(
        "product" => ["Laptop", "Mouse", "Keyboard"],
        "price" => [1200, 25, 75],
        "quantity" => [5, 50, 30],
        "category" => ["Electronics", "Accessories", "Accessories"]
    )?;

    // Example: min_price=50, no max_quantity constraint
    let filter_expr = build_filter(Some(50), None);
    sample_df.clone().filter(filter_expr)?.show().await?;
    // Only Laptop (1200) and Keyboard (75) have price >= 50
    // +---------+-------+----------+-------------+
    // | product | price | quantity | category    |
    // +---------+-------+----------+-------------+
    // | Laptop  | 1200  | 5        | Electronics |
    // | Keyboard| 75    | 30       | Accessories |
    // +---------+-------+----------+-------------+

    // Example: both constraints — price >= 50 AND quantity <= 10
    let filter_expr = build_filter(Some(50), Some(10));
    sample_df.clone().filter(filter_expr)?.show().await?;
    // Only Laptop matches (price=1200 >= 50, quantity=5 <= 10)
    // +---------+-------+----------+-------------+
    // | product | price | quantity | category    |
    // +---------+-------+----------+-------------+
    // | Laptop  | 1200  | 5        | Electronics |
    // +---------+-------+----------+-------------+

    Ok(())
}
```

> **Why [`unwrap_or_else`] instead of [`unwrap()`]?** <br>
> Calling [`unwrap()`] on `None` panics—crashing your program. Here, [`reduce()`] returns `None` when the conditions vector is empty (no filters provided). Instead of panicking, [`unwrap_or_else`] lets us provide a fallback: [`lit(true)`][`lit()`] matches all rows. This is a common Rust pattern for gracefully handling "no input" cases.

#### Anti-Pattern: Multiple Sequential Filters

**Each [`.filter()`] call creates a separate node in the logical plan.** While DataFusion's optimizer _can_ merge adjacent filters, combining them yourself is clearer, guarantees a single predicate evaluation, and makes your intent explicit.

```rust
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let sample_df = dataframe!(
        "product" => ["Laptop", "Mouse", "Keyboard"],
        "price" => [1200, 25, 75],
        "quantity" => [5, 50, 30],
        "category" => ["Electronics", "Accessories", "Accessories"]
    )?;

    // ❌ DON'T: Chain multiple filter calls
    let _fragmented = sample_df.clone()
        .filter(col("price").gt(lit(50)))?
        .filter(col("quantity").lt(lit(100)))?
        .filter(col("product").is_not_null())?;

    // ✅ DO: Combine into single filter
    let combined = sample_df.clone()
        .filter(
            col("price").gt(lit(50))
                .and(col("quantity").lt(lit(100)))
                .and(col("product").is_not_null())
        )?;

    // Both return the same result — Keyboard (price=75, quantity=30)
    combined.show().await?;
    // +----------+-------+----------+-------------+
    // | product  | price | quantity | category    |
    // +----------+-------+----------+-------------+
    // | Keyboard | 75    | 30       | Accessories |
    // +----------+-------+----------+-------------+

    Ok(())
}
```

> **Why it matters:** The fragmented version creates 3 filter nodes; the combined version creates 1. In complex queries, this compounds—affecting plan readability and optimization opportunities.

#### Filter Troubleshooting

| Symptom          | Cause                                                                  | Fix                                                 |
| :--------------- | :--------------------------------------------------------------------- | :-------------------------------------------------- |
| No rows returned | [three-valued logic]: `NULL > 5` → `NULL` (filtered out)               | Use [`.is_not_null()`] or [`coalesce()`]            |
| Nulls vanishing  | `col("x").eq(lit(false))` removes `NULL` too (`NULL = false` → `NULL`) | Add `.or(col("x").is_null())`                       |
| Slow filter      | Predicate not pushed to data source                                    | Check [`.explain()`]—filter should be _inside_ scan |

---

---

### Aggregation Patterns

**Aggregation collapses rows into summary statistics—transforming thousands of individual records into meaningful totals, averages, and counts that reveal patterns in your data.**

Aggregate functions like [`sum()`], [`avg()`], [`count()`], [`min()`], and [`max()`] reduce multiple values to a single result. The [`.aggregate()`] method takes two arguments:

1. **Grouping columns** — partition rows into groups (like SQL's `GROUP BY`)
2. **Aggregate expressions** — compute summaries per group

Without grouping columns (empty `vec![]`), aggregations summarize the entire DataFrame.

**SQL equivalent:** `SELECT dept, SUM(salary) FROM employees GROUP BY dept`

> **Trade-off: DataFrame vs SQL**
>
> - **DataFrame shines:** Programmatic grouping keys, conditional aggregations via [`when()`], multiple aggregations in one call
> - **SQL shines:** Declarative [`GROUP BY`] syntax, [`HAVING`] clause more intuitive than chained [`.filter()`]

**Performance note:** <br>
Aggregations are **column-wise analytics** — exactly where DataFusion's columnar approach excels. Vectorized operations on compressed Arrow arrays outperform row-by-row processing for large datasets. However, if your data resides in a row-based database via [`TableProvider`] and you're doing a simple `COUNT(*)` or `SUM` on an indexed column, pushing the aggregation to the source may avoid data transfer entirely.

#### Basic Aggregation

Group rows by one or more columns, then compute summary statistics for each group. Import aggregate functions from `datafusion::functions_aggregate::expr_fn` and use [`.alias()`] to name the output columns.

This example establishes `employees_df`—used throughout this section.

```rust
use datafusion::prelude::*;
use datafusion::functions_aggregate::expr_fn::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    // Sample data: employees_df [department, employee, salary]
    let employees_df = dataframe!(
        "department" => ["Sales", "Sales", "Engineering", "Engineering"],
        "employee" => ["Alice", "Bob", "Carol", "Dave"],
        "salary" => [50000, 55000, 80000, 85000]
    )?;

    // Group by department, compute multiple aggregations
    let result = employees_df.clone().aggregate(
        vec![col("department")],
        vec![
            sum(col("salary")).alias("total_salary"),
            avg(col("salary")).alias("avg_salary"),
            count(col("employee")).alias("employee_count")
        ]
    )?;

    result.show().await?;
    // +-------------+--------------+------------+----------------+
    // | department  | total_salary | avg_salary | employee_count |
    // +-------------+--------------+------------+----------------+
    // | Engineering | 165000       | 82500.0    | 2              |
    // | Sales       | 105000       | 52500.0    | 2              |
    // +-------------+--------------+------------+----------------+

    Ok(())
}
```

> **Limitation:** The _result_ of [`.aggregate()`] contains only grouping columns and aggregated expressions—the `employee` column is gone. If you need it, include it in the group-by or aggregate it (e.g., `array_agg(col("employee"))`). The original DataFrame is immutable; `employees_df` still has all columns.

#### Intermediate: Multi-Level Grouping and HAVING-Style Filtering

To replicate SQL's [`HAVING`] clause, chain [`.filter()`] _after_ [`.aggregate()`]—the filter sees the aggregated column names.

```rust
use datafusion::prelude::*;
use datafusion::functions_aggregate::expr_fn::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let employees_df = dataframe!(
        "department" => ["Sales", "Sales", "Engineering", "Engineering"],
        "employee" => ["Alice", "Bob", "Carol", "Dave"],
        "salary" => [50000, 55000, 80000, 85000]
    )?;

    // HAVING equivalent: filter on aggregated values
    let high_budget_depts = employees_df.clone()
        .aggregate(
            vec![col("department")],
            vec![sum(col("salary")).alias("total_salary")]
        )?
        .filter(col("total_salary").gt(lit(100000)))?;  // HAVING total_salary > 100000

    high_budget_depts.show().await?;
    // +-------------+--------------+
    // | department  | total_salary |
    // +-------------+--------------+
    // | Engineering | 165000       |
    // +-------------+--------------+
    // Sales (105000) filtered out — doesn't meet HAVING condition

    Ok(())
}
```

> **Key insight:** In SQL, [`HAVING`] filters _after_ grouping while [`WHERE`] filters _before_ ([SQL clause order]). In DataFrames, method order achieves the same: `.filter().aggregate()` = WHERE, `.aggregate().filter()` = HAVING.

#### Advanced: All Aggregate Functions

DataFusion provides a comprehensive set of aggregate functions beyond the basics. Import them from [`datafusion::functions_aggregate::expr_fn`][expr_fn] and combine multiple aggregations in a single [`.aggregate()`] call for efficiency.

| Category    | Functions                                                                          |
| :---------- | :--------------------------------------------------------------------------------- |
| Basic       | [`count()`], [`sum()`], [`avg()`], [`min()`], [`max()`]                            |
| Statistical | [`stddev()`], [`var_sample()`], [`var_pop()`], [`median()`], [`approx_median()`]\* |
| Distinct    | [`count_distinct()`], [`approx_distinct()`]\*                                      |
| Conditional | `sum(when(...).otherwise(...))` — aggregate only matching rows                     |
| Collection  | [`array_agg()`], [`string_agg()`]                                                  |

_\*Approximate functions_ use probabilistic algorithms (e.g., [HyperLogLog] for [`approx_distinct()`]) that trade exactness for speed and memory. Use them on large datasets where exact results would be too expensive—typical error is <2%.

The following example demonstrates several aggregate functions in a single call:

```rust
use datafusion::prelude::*;
use datafusion::functions_aggregate::expr_fn::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let employees_df = dataframe!(
        "department" => ["Sales", "Sales", "Engineering", "Engineering"],
        "employee" => ["Alice", "Bob", "Carol", "Dave"],
        "salary" => [50000, 55000, 80000, 85000]
    )?;

    let stats = employees_df.clone().aggregate(
        vec![col("department")],
        vec![
            count(col("employee")).alias("count"),
            sum(col("salary")).alias("sum"),
            avg(col("salary")).alias("avg"),
            min(col("salary")).alias("min"),
            max(col("salary")).alias("max"),
            stddev(col("salary")).alias("stddev"),
            // Conditional: count employees earning > 70k
            sum(when(col("salary").gt(lit(70000)), lit(1))
                .otherwise(lit(0))?).alias("high_earners")
        ]
    )?;

    stats.show().await?;
    // +-------------+-------+--------+---------+-------+-------+---------+--------------+
    // | department  | count | sum    | avg     | min   | max   | stddev  | high_earners |
    // +-------------+-------+--------+---------+-------+-------+---------+--------------+
    // | Engineering | 2     | 165000 | 82500.0 | 80000 | 85000 | 3535.53 | 2            |
    // | Sales       | 2     | 105000 | 52500.0 | 50000 | 55000 | 3535.53 | 0            |
    // +-------------+-------+--------+---------+-------+-------+---------+--------------+

    Ok(())
}
```

> **Tip:** See the full list of aggregate functions in the [Aggregate Functions Reference](../../user-guide/sql/aggregate_functions.md).

#### Advanced: Aggregation Without Grouping

Pass an empty `vec![]` as the grouping columns to aggregate the entire DataFrame into a single row—equivalent to SQL without a `GROUP BY` clause.

```rust
use datafusion::prelude::*;
use datafusion::functions_aggregate::expr_fn::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let employees_df = dataframe!(
        "department" => ["Sales", "Sales", "Engineering", "Engineering"],
        "employee" => ["Alice", "Bob", "Carol", "Dave"],
        "salary" => [50000, 55000, 80000, 85000]
    )?;

    // Aggregate entire DataFrame (no GROUP BY) — summarize all rows
    let company_totals = employees_df.clone().aggregate(
        vec![],  // Empty group by = entire DataFrame
        vec![
            sum(col("salary")).alias("company_total"),
            avg(col("salary")).alias("company_avg")
        ]
    )?;

    company_totals.show().await?;
    // +---------------+-------------+
    // | company_total | company_avg |
    // +---------------+-------------+
    // | 270000        | 67500.0     |
    // +---------------+-------------+

    Ok(())
}
```

#### Aggregation Troubleshooting

| Symptom          | Cause                                        | Fix                                                                           |
| :--------------- | :------------------------------------------- | :---------------------------------------------------------------------------- |
| Wrong results    | Unexpected grouping keys                     | Verify with `df.select(vec![col("key")]).distinct()?.show().await?`           |
| Column not found | Non-aggregated columns disappear             | Include in group-by or aggregate (e.g., [`array_agg()`])                      |
| Nulls skipped    | Aggregate functions ignore `NULL` by default | Use [`count(*)`][`count()`] for row count, or [`coalesce()`] to replace nulls |

#### Further Reading

**DataFusion Resources:**

- [Aggregate Functions Reference (SQL)](../../user-guide/sql/aggregate_functions.md) — Complete list of built-in aggregate functions with SQL examples
- [`datafusion-functions-aggregate` crate](https://docs.rs/datafusion-functions-aggregate/latest/datafusion_functions_aggregate/) — Rust API docs for all aggregate function implementations

**Concepts & Theory:**

- [SQL GROUP BY (PostgreSQL docs)](https://www.postgresql.org/docs/current/queries-table-expressions.html#QUERIES-GROUP) — Canonical reference for grouping semantics
- [HyperLogLog Algorithm](https://en.wikipedia.org/wiki/HyperLogLog) — How `approx_distinct()` achieves O(1) memory

---

---

### When DataFrames Collide: Join Patterns

**Joins are the backbone of relational data processing—the operation that links separate tables into unified, queryable datasets by matching rows on shared keys.**

A join takes two DataFrames (or SQL tables) and produces a new one by comparing values in designated **key columns**—when values match (e.g., `customer.id = 1` on the left finds `order.customer_id = 1` on the right), the corresponding rows are stitched together. The result is a wider table combining columns from both sides, where related data now sits in the same row. Think of it as a lookup: for each row on the left, scan the right table for rows with matching key values, then concatenate them.

Whether you're enriching customer records with their orders, filtering products by inventory status, or reconciling data across systems, joins are the workhorse behind nearly every real-world data pipeline.

Master joins, and you unlock the full power of relational data processing.

> **DataFrame API coverage:** The DataFrame API supports all common join types ([`Inner`], [`Left`], [`Right`], [`Full`], [`LeftSemi`], [`LeftAnti`], and their right variants). Two SQL join types have **no direct DataFrame equivalent**:
>
> - [`NATURAL JOIN`] — use [`ctx.sql()`][`SessionContext::sql()`] or specify keys explicitly with [`.join()`]
> - [`CROSS JOIN`] — use [`.join()`] with empty key lists, or [`ctx.sql("... CROSS JOIN ...")`][`SessionContext::sql()`]
>
> For most workflows, the DataFrame API is fully sufficient. Fall back to SQL for these edge cases.

#### Why Joins Matter

Real-world data rarely lives in a single table. Customers are in one file, orders in another, products in a third. Joins let you:

- **Enrich** records by attaching related data (customer name → their orders)
- **Filter** by relationships (only customers _with_ orders, or _without_)
- **Reconcile** datasets (find what's in A but not B, or in both)
- **Validate** data quality—anti-joins reveal orphaned records (orders referencing non-existent customers), broken foreign keys, or rows dropped during ETL

Without joins, you'd be stuck writing nested loops or manual lookups. DataFusion's join engine handles the matching efficiently—you describe _what_ to combine, not _how_.

#### How Joins Work

Every join has three ingredients:

1. **Two tables** — left (your starting DataFrame) and right (the one you're joining)
2. **Join keys** — which columns to match (`customers.id = orders.customer_id`)
3. **Join type** — what to do with matches and non-matches

A very basic example is shown as the following as common in SQL :

```sql
-- SQL equivalent
SELECT *
FROM customers           -- left table
JOIN orders              -- right table
  ON customers.id = orders.customer_id   -- join keys
```

Since we cannot cover a tutorial for joins, please follow other tutorials as but not only the following resources:

| Resource                        | Focus                                                              |
| :------------------------------ | :----------------------------------------------------------------- |
| [Visual JOIN guide]             | Interactive visualization of all join types with animated examples |
| [Join tutorial]                 | Why Venn diagrams are misleading for understanding joins           |
| [Semi and Anti joins explained] | First-class existence checks that SQL forgot                       |
| [PostgreSQL JOIN docs]          | Authoritative reference—DataFusion follows PostgreSQL semantics    |
| [NULL handling in joins]        | Why `NULL = NULL` is `UNKNOWN`, not `TRUE`                         |

#### DataFrame API vs SQL

**Two paths, same destination.** Both APIs compile to the same internal [`LogicalPlan`] and benefit from identical optimizer passes—the difference is _how_ you construct the query:

| Aspect           | DataFrame API                                                 | SQL API                                           |
| ---------------- | ------------------------------------------------------------- | ------------------------------------------------- |
| **Construction** | Builder pattern—chain methods like [`.join()`], [`.filter()`] | Parser—write a query string, DataFusion parses it |
| **Type safety**  | Compile-time checks; typos caught by `rustc`                  | Runtime errors; typos discovered at execution     |
| **Composition**  | Programmatic; easy to build queries conditionally             | String-based; dynamic SQL requires concatenation  |
| **Result**       | [`LogicalPlan`] → Optimizer → Execution                       | [`LogicalPlan` ]→ Optimizer → Execution           |

**The multiplicity of SQL-Dialects**<br>
DataFusion's SQL parser ([`sqlparser`]) accepts syntax from multiple dialects—PostgreSQL, MySQL, Snowflake, and others. Throughout this documentation, we use **PostgreSQL syntax** as the reference standard: it's widely understood, well-documented, and DataFusion's join semantics (NULL handling, outer join behavior) closely follow PostgreSQL conventions. <br>
For more deeper insights follow [SQL Dialects][Understanding SQL Dialects (medium-article)]

Both SQL and the DataFrame API support the standard join families:

| Family      | SQL syntax                                   | DataFrame [`JoinType`]        | Purpose                                     |
| ----------- | -------------------------------------------- | ----------------------------- | ------------------------------------------- |
| **Inner**   | [`INNER JOIN`]                               | [`Inner`]                     | Only matching rows                          |
| **Outer**   | [`LEFT`] / [`RIGHT`] / [`FULL OUTER JOIN`]   | [`Left`], [`Right`], [`Full`] | Keep non-matches from one or both sides     |
| **Semi**    | [`LEFT / RIGHT SEMI JOIN`][`LEFT SEMI JOIN`] | [`LeftSemi`], [`RightSemi` ]  | Filter by existence (no columns from right) |
| **Anti**    | [`LEFT / RIGHT ANTI JOIN`][`LEFT ANTI JOIN`] | [`LeftAnti`], [`RightAnti` ]  | Filter by non-existence                     |
| **Cross**   | [`CROSS JOIN`]                               | _(none)_                      | Cartesian product (use empty keys)          |
| **Natural** | [`NATURAL JOIN`]                             | _(none)_                      | Auto-match same-named columns               |
| **Mark**    | _(internal)_                                 | [`LeftMark`], [`RightMark`]   | Adds boolean column for `EXISTS` subqueries |

> **SQL-only joins:**<br> > [`NATURAL JOIN`] and [`CROSS JOIN`] have no direct [`JoinType`] variant.
> Use [`ctx.sql()`][`SessionContext::sql()`] for natural joins; for cross joins, call [`.join()`] with empty key lists (see Anti-Pattern section).

The [`.join()`] method signature in the datafusion dataframe-API:

```rust
use datafusion::prelude::*;  // Includes JoinType

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let left_df = dataframe!("id" => [1, 2])?;
    let right_df = dataframe!("customer_id" => [1, 2])?;

    let joined = left_df.join(
        right_df,                    // 1. Right DataFrame
        JoinType::Inner,             // 2. Join type
        &["id"],                     // 3. Left key columns
        &["customer_id"],            // 4. Right key columns
        None,                        // 5. Optional filter expression
    )?;

    joined.show().await?;
    Ok(())
}
```

**Two ways to specify joins:**

- [`.join()`] — Pass column names (`&[&str]`) for each side plus an optional `filter: Option<Expr>`. DataFusion builds equality predicates from the columns.
- [`.join_on()`] — Pass the full join condition as `Expr`s. Internally this wraps [`.join()`] with empty key lists and a combined filter expression (`expr_1 AND expr_2 ...`). Optimizer passes then extract equality predicates and treat them as equi-join keys.

After optimization, both methods produce equivalent plans—**no performance difference** for standard equi-joins. However, [`.join()`] is the "safer" choice: you explicitly declare equi-join keys, guaranteeing hash/sort-merge algorithms. With [`.join_on()`], if the optimizer can't extract equality predicates from your expression, it may fall back to nested loop joins.

Pick whichever reads better for your use case.

> **Gotcha: [`.join_on()`] uses AND, not OR**
>
> Multiple expressions passed to [`.join_on()`] are combined with [`AND`]:
>
> ```rust
> use datafusion::prelude::*;
>
> #[tokio::main]
> async fn main() -> datafusion::error::Result<()> {
>     let left = dataframe!("a" => [1], "b" => [2])?.alias("l")?;
>     let right = dataframe!("a2" => [1], "b2" => [2])?.alias("r")?;
>     // This means: a = a2 AND b = b2 (not OR!)
>     let joined = left.join_on(right, JoinType::Inner, [col("l.a").eq(col("r.a2")), col("l.b").eq(col("r.b2"))])?;
>     joined.show().await?;
>     Ok(())
> }
> ```
>
> For [`OR`] logic, build a single expression:
>
> ```rust
> use datafusion::prelude::*;
>
> #[tokio::main]
> async fn main() -> datafusion::error::Result<()> {
>     let left = dataframe!("a" => [1], "b" => [2])?.alias("l")?;
>     let right = dataframe!("a2" => [1], "b2" => [2])?.alias("r")?;
>     // Match if EITHER a or b matches
>     let joined = left.join_on(right, JoinType::Inner, [col("l.a").eq(col("r.a2")).or(col("l.b").eq(col("r.b2")))])?;
>     joined.show().await?;
>     Ok(())
> }
> ```

> **Trade-off: DataFrame vs SQL**
>
> | DataFrame API Advantages                                                                                                                                                   | SQL Advantages                                                        |
> | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------- |
> | **First-class Semi/Anti joins** — `JoinType::LeftAnti`, `LeftSemi` etc. are explicit; no workarounds needed (unlike PySpark where you'd use `LEFT JOIN` + `WHERE IS NULL`) | **Visual clarity** — Multi-table joins read naturally in SQL syntax   |
> | **Type-safe composition** — Build joins conditionally with `if/else`; compiler catches column typos                                                                        | **Familiar syntax** — Standard `ON` clause understood by any SQL user |
> | **Chained transformations** — `.join().filter().select()` flows naturally                                                                                                  | **Copy-paste ready** — Test queries directly in SQL tools             |
> | **Complex conditions** — [`.join_on()`] accepts any `Expr`, not just column equality                                                                                       | **Self-documenting** — SQL is often readable by non-programmers       |
>
> **DataFusion-specific advantage:** Unlike many DataFrame libraries, DataFusion exposes the _full_ set of join types ([`LeftSemi`], [`RightSemi`], [`LeftAnti`], [`RightAnti`], [`LeftMark`], [`RightMark`]) as first-class operations—no need to emulate anti-joins with outer joins and null checks.

**Performance note:** <br>
For joins via row-based [`TableProviders`], consider whether the join should happen at the source. If both tables are in Postgres with foreign key indexes, the DB's index-backed joins may outperform transferring data to DataFusion. For cross-source joins or large analytical joins without indexes, DataFusion's hash/sort-merge algorithms excel.

#### How Joins Execute

Under the hood, DataFusion selects from [several join algorithms] based on your data:

| Algorithm                  | When Used                                                                                      |
| :------------------------- | :--------------------------------------------------------------------------------------------- |
| [**Hash Join**]            | Default for equi-joins (`=`). Builds a hash table on the smaller side, probes with the larger. |
| [**Sort-Merge Join**]      | Pre-sorted inputs; can spill to disk for huge datasets.                                        |
| [**Symmetric Hash Join**]  | Streaming/unbounded data—both sides build hash tables, rows pruned via sliding windows.        |
| [**Nested Loop Join**]     | General non-equi conditions where hash-based algorithms don't apply.                           |
| [**Piecewise Merge Join**] | Single range filter (`<`, `>`, `<=`, `>=`)—much faster than nested loop for these cases.       |
| [**Cross Join**]           | Cartesian product—used for SQL [`CROSS JOIN`] and [`.join()`] with empty key lists.            |

The optimizer _can_ (based on configuration and statistics):

- **Swap sides** to put the smaller table on the build side
- **Choose partition mode**—broadcast small tables or hash-partition both sides
- **Push dynamic filters**—min/max bounds from the build side skip irrelevant probe data (e.g., Parquet row groups)

These behaviors are tunable via [`datafusion.optimizer`] settings.

All join algorithms leverage [Arrow]'s columnar format: instead of copying rows, DataFusion computes index arrays and uses vectorized [`take()`] operations to assemble results efficiently.

> **Why DataFusion Joins Are Fast**
>
> Unlike traditional row-based databases, DataFusion combines several modern techniques:
>
> | Technique                          | Benefit                                                                                                                                          |
> | ---------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------ |
> | **Columnar format (Arrow)**        | Read only the columns you need; SIMD instructions process thousands of keys in parallel                                                          |
> | **Vectorized execution**           | Joins process batches of rows, not one at a time—simple inner loops let CPUs parallelize at the instruction level                                |
> | **SQL = DataFrame**                | Both compile to the same `LogicalPlan`—identical optimizer benefits regardless of API choice                                                     |
> | **Statistics-driven optimization** | Table metadata (row counts, min/max) guide join order and algorithm selection—[**16x faster** on TPC-H benchmarks][DataFusion Join Optimization] |
> | **Late materialization**           | During joins, only key columns + row indices are processed; other columns are fetched afterward                                                  |
>
> The result: you describe _what_ to join, and the optimizer handles _how_—often matching or exceeding hand-tuned imperative code.

#### Join Types at a Glance

Joins control how rows from two tables are matched and combined. The key decisions are:

1.  what happens to rows that _don't_ match
2.  which columns appear in the result.

Inner joins discard non-matches; outer joins preserve them with NULLs. Semi and Anti joins answer existence questions without adding columns from the right table.

| Join Type            | Returns                   | Use Case                                    |
| :------------------- | :------------------------ | :------------------------------------------ |
| [`Inner`]            | Matches from both sides   | Standard join—only matching rows            |
| [`Left`]             | All left + matching right | Keep all left rows (NULL if no match)       |
| [`Right`]            | All right + matching left | Keep all right rows (NULL if no match)      |
| [`Full`]             | Everything from both      | See all data, matched or not                |
| [`LeftSemi`]         | Left rows WITH matches    | "Which left rows have a match?"             |
| [`LeftAnti`]         | Left rows WITHOUT matches | "Which left rows have NO match?"            |
| ~~Cross~~ (SQL only) | Cartesian product         | All combinations (see Anti-Pattern section) |

> **Note:** The DataFrame API has no `JoinType::Cross`. Cartesian products are represented as `Inner` joins with empty key lists or as [`CROSS JOIN`] in SQL.

> **Learn more:** You may want to check out this source [Join tutorial] or [Semi and Anti joins explained].

#### Basic: The Inner Join

This example establishes `customers_df` and `orders_df`—used throughout this section. Note: Carol has no orders, and order 104 has no matching customer (orphan).

```rust
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    // Sample data: customers_df [id, name] — Carol has no orders
    let customers_df = dataframe!(
        "id" => [1, 2, 3],
        "name" => ["Alice", "Bob", "Carol"]
    )?;

    // Sample data: orders_df [order_id, customer_id, amount] — order 104 is orphaned
    let orders_df = dataframe!(
        "order_id" => [101, 102, 103, 104],
        "customer_id" => [1, 1, 2, 99],
        "amount" => [100, 200, 150, 300]
    )?;

    // Inner join: only matching rows (Carol excluded, order 104 excluded)
    let result = customers_df.clone().join(
        orders_df.clone(),
        JoinType::Inner,
        &["id"],
        &["customer_id"],
        None
    )?;

    result.show().await?;
    // +----+-------+----------+-------------+--------+
    // | id | name  | order_id | customer_id | amount |
    // +----+-------+----------+-------------+--------+
    // | 1  | Alice | 101      | 1           | 100    |
    // | 1  | Alice | 102      | 1           | 200    |
    // | 2  | Bob   | 103      | 2           | 150    |
    // +----+-------+----------+-------------+--------+

    Ok(())
}
```

**When to use Inner Join:**

- **Enrich data** — Attach related information (customer details → their orders)
- **Filter by relationship** — Keep only rows that have a match on the other side
- **Combine normalized tables** — Reassemble data split across multiple tables

**Not for set intersections!** <br>
If you need rows that exist in _both_ DataFrames (identical schemas, all columns compared), use [`.intersect()`] instead—that's a set operation, not a join.
<br> For more see the subsection [Dataframes unique methods](#dataframe-unique-methods)

> **⚠️ The hidden cost: [Survivorship bias][survivorship_bias]**
>
> Many join types silently drop non-matching rows—Inner, Semi, and Anti joins all filter out data. In the example above, Carol and order 104 simply vanish. Chain several such joins together and you may lose 60% of your data without noticing—you only see the "survivors" (rows that matched at every step).
>
> As a sanity check, if you need to see what's _missing_, use [Outer Joins](#intermediate-leftrightfull-joins) (or other oposit joins like left vs. right) instead—`NULL` values reveal exactly where data gaps exist.

#### Intermediate: Multi-Key Joins

Join on multiple columns when a single key isn't enough to uniquely identify matches—common with composite keys or temporal constraints.

```rust
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    // Regional sales: same product_id can exist in different regions
    let inventory_df = dataframe!(
        "product_id" => [1, 1, 2, 2],
        "region" => ["East", "West", "East", "West"],
        "stock" => [100, 50, 200, 75]
    )?;

    // Use different column names to avoid duplicate field error
    let sales_df = dataframe!(
        "sale_product_id" => [1, 1, 2],
        "sale_region" => ["East", "West", "East"],
        "sold" => [30, 20, 80]
    )?;

    // Multi-key join: match on BOTH product_id AND region
    let joined = inventory_df.join(
        sales_df,
        JoinType::Left,  // Keep all inventory, even unsold
        &["product_id", "region"],
        &["sale_product_id", "sale_region"],
        None
    )?;

    joined.show().await?;
    // +------------+--------+-------+-----------------+-------------+------+
    // | product_id | region | stock | sale_product_id | sale_region | sold |
    // +------------+--------+-------+-----------------+-------------+------+
    // | 1          | East   | 100   | 1               | East        | 30   |
    // | 1          | West   | 50    | 1               | West        | 20   |
    // | 2          | East   | 200   | 2               | East        | 80   |
    // | 2          | West   | 75    |                 |             |      |
    // +------------+--------+-------+-----------------+-------------+------+

    Ok(())
}
```

**To avoid duplicate columns**, use different column names on the right side, then select only what you need:

```rust
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let inventory_df = dataframe!(
        "product_id" => [1, 1, 2, 2],
        "region" => ["East", "West", "East", "West"],
        "stock" => [100, 50, 200, 75]
    )?;

    // Use different column names for join keys on right side
    let sales_df = dataframe!(
        "sale_product_id" => [1, 1, 2],
        "sale_region" => ["East", "West", "East"],
        "sold" => [30, 20, 80]
    )?;

    let joined = inventory_df.join(
        sales_df,
        JoinType::Left,
        &["product_id", "region"],
        &["sale_product_id", "sale_region"],
        None
    )?;

    // Select only the columns you need (left-side keys + data)
    let result = joined.select(vec![
        col("product_id"),
        col("region"),
        col("stock"),
        col("sold"),
    ])?;

    result.show().await?;
    // +------------+--------+-------+------+
    // | product_id | region | stock | sold |
    // +------------+--------+-------+------+
    // | 1          | East   | 100   | 30   |
    // | 1          | West   | 50    | 20   |
    // | 2          | East   | 200   | 80   |
    // | 2          | West   | 75    |      |  ← No sales
    // +------------+--------+-------+------+

    Ok(())
}
```

**⚠️ Handling Same-Named Columns**

DataFusion's [`.join()`] preserves columns from both sides. When join keys share names, use one of these patterns:

| Pattern                        | When to use                                                |
| ------------------------------ | ---------------------------------------------------------- |
| **[`.select()`] after join**   | Simple joins—pick the columns you need                     |
| **[`.alias()`] before join**   | Complex multi-way joins—qualify with `col("alias.column")` |
| **[`.with_column_renamed()`]** | Rename conflicting columns before joining                  |

**Tip:** Call [`.schema()`] after joining to see actual column names.

> **Pro tip for time-dependent data:** <br>
> Multi-key joins on temporal columns work well when truncated to appropriate granularity using [`date_trunc()`]. Joining on `DATE` (day) has minimal edge cases (~0.004% at midnight); joining on raw `TIMESTAMP` (milliseconds) risks silent mismatches.

#### Intermediate: Left/Right/Full Joins

Where Inner Join keeps only the intersection (rows matching on both sides), **"partial" outer joins (left, right and full) preserve rows that don't match**—filling missing columns with `NULL`. This makes data gaps visible instead of silently dropping them.

| Join Type | Keeps                                           | Typical Use Case                                        |
| :-------- | :---------------------------------------------- | :------------------------------------------------------ |
| **Left**  | All left rows, matching right data if available | Customer reports—keep all customers, show orders if any |
| **Right** | All right rows, matching left data if available | Orphan detection—find orders without valid customers    |
| **Full**  | Everything from both sides                      | Data reconciliation—find ALL discrepancies              |

Left Join handles ~90% of outer join use cases. Right Join can usually be rewritten as Left Join by swapping tables. Full Join is for reconciliation scenarios.

##### Left Join — Enrich Your Primary Data

Keep **all rows from the left table**, enrich with matching data from the right table. No match? Right-side columns become `NULL`.

```rust
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    // customers_df:
    // +----+-------+
    // | id | name  |
    // +----+-------+
    // | 1  | Alice |
    // | 2  | Bob   |
    // | 3  | Carol |  ← Has no orders
    // +----+-------+
    let customers_df = dataframe!(
        "id" => [1, 2, 3],
        "name" => ["Alice", "Bob", "Carol"]
    )?;

    // orders_df:
    // +----------+-------------+--------+
    // | order_id | customer_id | amount |
    // +----------+-------------+--------+
    // | 101      | 1           | 100    |
    // | 102      | 1           | 200    |
    // | 103      | 2           | 150    |
    // | 104      | 99          | 300    |  ← Orphan
    // +----------+-------------+--------+
    let orders_df = dataframe!(
        "order_id" => [101, 102, 103, 104],
        "customer_id" => [1, 1, 2, 99],
        "amount" => [100, 200, 150, 300]
    )?;

    // "All customers with their orders (if any)"
    let left_result = customers_df.clone().join(
        orders_df.clone(),
        JoinType::Left,
        &["id"],
        &["customer_id"],
        None
    )?;

    left_result.show().await?;
    // +----+-------+----------+-------------+--------+
    // | id | name  | order_id | customer_id | amount |
    // +----+-------+----------+-------------+--------+
    // | 1  | Alice | 101      | 1           | 100    |
    // | 1  | Alice | 102      | 1           | 200    |
    // | 2  | Bob   | 103      | 2           | 150    |
    // | 3  | Carol |          |             |        |  ← Preserved with NULLs
    // +----+-------+----------+-------------+--------+

    Ok(())
}
```

**Use Case: Self-Joins (Customer Referrals)**

A **self-join** joins a table with itself—essential for hierarchical data. Left Join preserves all rows even if they have no match (like Alice, who has no referrer).

```rust
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    // Extended customers: add referred_by (which customer referred them)
    let customers_referrals = dataframe!(
        "id" => [1, 2, 3],
        "name" => ["Alice", "Bob", "Carol"],
        "referred_by" => [None::<i64>, Some(1), Some(1)]  // Alice referred Bob and Carol
    )?;

    // Self-join: alias both sides to disambiguate
    let customer = customers_referrals.clone().alias("customer")?;
    let referrer = customers_referrals.clone().alias("referrer")?;

    let with_referrers = customer.join(
        referrer,
        JoinType::Left,  // Keep customers without referrers (Alice)
        &["referred_by"],
        &["id"],
        None
    )?.select(vec![
        col("customer.name").alias("customer"),
        col("referrer.name").alias("referred_by"),
    ])?;

    with_referrers.show().await?;
    // +----------+-------------+
    // | customer | referred_by |
    // +----------+-------------+
    // | Alice    | NULL        |  ← No referrer
    // | Bob      | Alice       |
    // | Carol    | Alice       |
    // +----------+-------------+

    Ok(())
}
```

**Key pattern:** <br>
Use [`.alias()`] to create two "views" of the same DataFrame, then join with qualified column names (`customer.name`, `referrer.name`).

**Common self-join patterns:**

- **Hierarchy traversal:** employees → managers, categories → parent categories
- **Sequential comparison:** this_year.sales vs last_year.sales (join on product_id)
- **Finding pairs:** "Which products are often bought together?" (order_items self-join)

##### Right Join — Find Orphaned Records

Keep all rows from the right table—useful for finding records that reference non-existent parents (like order 104 referencing customer 99).

```rust
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let customers_df = dataframe!(
        "id" => [1, 2, 3],
        "name" => ["Alice", "Bob", "Carol"]
    )?;

    let orders_df = dataframe!(
        "order_id" => [101, 102, 103, 104],
        "customer_id" => [1, 1, 2, 99],
        "amount" => [100, 200, 150, 300]
    )?;

    // "All orders, showing customer info (if customer exists)"
    let right_result = customers_df.clone().join(
        orders_df.clone(),
        JoinType::Right,
        &["id"],
        &["customer_id"],
        None
    )?;

    right_result.show().await?;
    // +----+-------+----------+-------------+--------+
    // | id | name  | order_id | customer_id | amount |
    // +----+-------+----------+-------------+--------+
    // | 1  | Alice | 101      | 1           | 100    |
    // | 1  | Alice | 102      | 1           | 200    |
    // | 2  | Bob   | 103      | 2           | 150    |
    // |    |       | 104      | 99          | 300    |  ← Orphan! No customer 99
    // +----+-------+----------+-------------+--------+

    Ok(())
}
```

**Tip:** <br>
Right Join is just Left Join with swapped tables. `A.join(B, Right)` = `B.join(A, Left)`. Most teams use Left Join exclusively for consistency—put your "main" table first.

##### Full Join — Complete Reconciliation

Keep **all rows from both tables**. Where there's no match, fill the "other side" with NULLs. This is the only join that guarantees you see _everything_—matched, unmatched left, AND unmatched right.

**When to use Full Join:**

- **Data reconciliation** — Comparing two data sources to find ALL discrepancies
- **Migration validation** — Ensuring old and new systems have the same records
- **Audit trails** — "Show me what's in A but not B, what's in B but not A, and what's in both"

```rust
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let customers_df = dataframe!(
        "id" => [1, 2, 3],
        "name" => ["Alice", "Bob", "Carol"]
    )?;

    let orders_df = dataframe!(
        "order_id" => [101, 102, 103, 104],
        "customer_id" => [1, 1, 2, 99],
        "amount" => [100, 200, 150, 300]
    )?;

    // "Show me everything: matched, unmatched left, AND unmatched right"
    let full_result = customers_df.clone().join(
        orders_df.clone(),
        JoinType::Full,
        &["id"],
        &["customer_id"],
        None
    )?;

    full_result.show().await?;
    // +----+-------+----------+-------------+--------+
    // | id | name  | order_id | customer_id | amount |
    // +----+-------+----------+-------------+--------+
    // | 1  | Alice | 101      | 1           | 100    |  ← Matched
    // | 1  | Alice | 102      | 1           | 200    |  ← Matched
    // | 2  | Bob   | 103      | 2           | 150    |  ← Matched
    // | 3  | Carol |          |             |        |  ← Left only (no orders)
    // |    |       | 104      | 99          | 300    |  ← Right only (orphan)
    // +----+-------+----------+-------------+--------+

    Ok(())
}
```

**Data Quality Pattern:** Full Join + NULL filters = powerful reconciliation tool:

```rust
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let customers_df = dataframe!(
        "id" => [1, 2, 3],
        "name" => ["Alice", "Bob", "Carol"]
    )?;

    let orders_df = dataframe!(
        "order_id" => [101, 102, 103, 104],
        "customer_id" => [1, 1, 2, 99],
        "amount" => [100, 200, 150, 300]
    )?;

    let full_result = customers_df.join(
        orders_df,
        JoinType::Full,
        &["id"],
        &["customer_id"],
        None
    )?;

    // Find customers WITHOUT orders (left-only)
    let inactive = full_result.clone().filter(col("order_id").is_null())?;

    // Find orphaned orders (right-only, invalid customer_id)
    let orphans = full_result.clone().filter(col("id").is_null())?;

    // Find matched records (both sides present)
    let matched = full_result.filter(
        col("id").is_not_null().and(col("order_id").is_not_null())
    )?;

    inactive.show().await?;
    orphans.show().await?;
    matched.show().await?;

    Ok(())
}
```

This pattern is invaluable for ETL pipelines, data migration validation, and debugging referential integrity issues.

#### Intermediate: Semi and Anti Joins

**What makes them special?** <br>
Semi and Anti joins are **filtering joins**—they filter the left table based on existence in the right table, but **never add columns** from the right table. This is fundamentally different from Inner/Left/Right/Full joins which combine data.

| Join Type    | Question                         | Returns                           | SQL Equivalent                               |
| :----------- | :------------------------------- | :-------------------------------- | :------------------------------------------- |
| **LeftSemi** | "Which left rows HAVE a match?"  | Left columns only, matched rows   | `WHERE EXISTS (SELECT 1 FROM right ...)`     |
| **LeftAnti** | "Which left rows have NO match?" | Left columns only, unmatched rows | `WHERE NOT EXISTS (SELECT 1 FROM right ...)` |

**Why use them instead of alternatives?**

| Alternative                | Problem                                                             | Semi/Anti Advantage                                              |
| :------------------------- | :------------------------------------------------------------------ | :--------------------------------------------------------------- |
| Inner Join + Distinct      | Creates duplicates if right has multiple matches, then removes them | Semi join handles this automatically—one output row per left row |
| Left Join + WHERE NULL     | Joins everything first, then filters                                | Anti join filters during join—more efficient                     |
| `IN (SELECT ...)` subquery | Can be slower, harder to optimize                                   | Semi join is the optimized physical plan for `IN`                |

##### LeftSemi — "Which Rows Have Matches?"

Returns left rows that have **at least one match** in the right table. Even if a customer has 10 orders, they appear only once.

```rust
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    // customers_df:                 orders_df:
    // +----+-------+                +----------+-------------+--------+
    // | id | name  |                | order_id | customer_id | amount |
    // +----+-------+                +----------+-------------+--------+
    // | 1  | Alice |                | 101      | 1           | 100    |
    // | 2  | Bob   |                | 102      | 1           | 200    |
    // | 3  | Carol |                | 103      | 2           | 150    |
    // +----+-------+                | 104      | 99          | 300    |
    //                               +----------+-------------+--------+
    let customers_df = dataframe!(
        "id" => [1, 2, 3],
        "name" => ["Alice", "Bob", "Carol"]
    )?;

    let orders_df = dataframe!(
        "order_id" => [101, 102, 103, 104],
        "customer_id" => [1, 1, 2, 99],
        "amount" => [100, 200, 150, 300]
    )?;

    // "Which customers have placed at least one order?"
    let active_customers = customers_df.clone().join(
        orders_df.clone(),
        JoinType::LeftSemi,
        &["id"],
        &["customer_id"],
        None
    )?;

    active_customers.show().await?;
    // +----+-------+
    // | id | name  |
    // +----+-------+
    // | 1  | Alice |  ← Has 2 orders, appears once
    // | 2  | Bob   |  ← Has 1 order
    // +----+-------+
    // Note: Carol (id=3) excluded—no orders
    // Note: No order columns! Just filtered customers.

    Ok(())
}
```

**Use cases for LeftSemi:**

- Find active customers (have placed orders)
- Find products that have been sold (exist in order_items)
- Filter to "things that are referenced somewhere"

##### LeftAnti — "Which Rows Have No Matches?"

Returns left rows that have **zero matches** in the right table. The inverse of Semi join.

```rust
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    // customers_df:                 orders_df:
    // +----+-------+                +----------+-------------+--------+
    // | id | name  |                | order_id | customer_id | amount |
    // +----+-------+                +----------+-------------+--------+
    // | 1  | Alice |                | 101      | 1           | 100    |
    // | 2  | Bob   |                | 102      | 1           | 200    |
    // | 3  | Carol |                | 103      | 2           | 150    |
    // +----+-------+                | 104      | 99          | 300    |
    //                               +----------+-------------+--------+
    let customers_df = dataframe!(
        "id" => [1, 2, 3],
        "name" => ["Alice", "Bob", "Carol"]
    )?;

    let orders_df = dataframe!(
        "order_id" => [101, 102, 103, 104],
        "customer_id" => [1, 1, 2, 99],
        "amount" => [100, 200, 150, 300]
    )?;

    // "Which customers have NEVER placed an order?"
    let inactive_customers = customers_df.clone().join(
        orders_df.clone(),
        JoinType::LeftAnti,
        &["id"],
        &["customer_id"],
        None
    )?;

    inactive_customers.show().await?;
    // +----+-------+
    // | id | name  |
    // +----+-------+
    // | 3  | Carol |  ← No orders found
    // +----+-------+
    // Alice and Bob excluded—they have orders

    Ok(())
}
```

**Use cases for LeftAnti:**

- Find inactive customers (never ordered)
- Find dead inventory (products never sold)
- Data cleanup: "Find records missing required relationships"
- Complement of Semi: `Semi ∪ Anti = Full Left Table`

##### Why Not Just Use Left Join + Filter?

A common question: "Can't I just do Left Join and filter for NULLs?"

```rust
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let customers_df = dataframe!(
        "id" => [1, 2, 3],
        "name" => ["Alice", "Bob", "Carol"]
    )?;

    let orders_df = dataframe!(
        "order_id" => [101, 102, 103, 104],
        "customer_id" => [1, 1, 2, 99],
        "amount" => [100, 200, 150, 300]
    )?;

    // ❌ Less efficient: Join everything, then filter
    let inactive_v1 = customers_df.clone()
        .join(orders_df.clone(), JoinType::Left, &["id"], &["customer_id"], None)?
        .filter(col("order_id").is_null())?;

    // ✅ More efficient: Anti join filters during the join
    let inactive_v2 = customers_df.clone()
        .join(orders_df.clone(), JoinType::LeftAnti, &["id"], &["customer_id"], None)?;

    // Both produce the same result:
    // +----+-------+
    // | id | name  |
    // +----+-------+
    // | 3  | Carol |
    // +----+-------+
    inactive_v1.show().await?;
    inactive_v2.show().await?;

    Ok(())
}
```

Both produce the same result, but Anti join:

- Doesn't create intermediate joined rows
- Doesn't add (and then ignore) right-side columns
- Optimizer can use more efficient algorithms (e.g., hash-based existence check)

> **Learn more:** See [Semi and Anti joins explained] for why these deserve first-class syntax in SQL.

> **Other variants:** <br>
> DataFusion's [`JoinType`] also includes `RightSemi`, `RightAnti`, and mark variants for advanced use cases. For most DataFrame work, stick to left variants and swap input tables if needed.

> **Mark joins:** <br> > [`LeftMark`]/[`RightMark`] are used internally to decorrelate `EXISTS` subqueries. They return all rows from one side plus an extra boolean "mark" column indicating whether any match exists on the other side. Most DataFrame code won't use them directly, but you may see them in `EXPLAIN` plans for complex SQL with `EXISTS` predicates.

#### Advanced: Multi-Way Joins

**Chain [`.join()`] calls to combine 3+ tables—each join produces a new DataFrame that feeds into the next.**

Real-world data is often normalized across multiple tables. A business question like "which customers have paid orders?" requires combining customers → orders → payments. Each chained Inner Join acts as a filter—only rows matching _all_ join conditions survive.

```rust
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    // customers_df:                 orders_df:
    // +----+-------+                +----------+-------------+--------+
    // | id | name  |                | order_id | customer_id | amount |
    // +----+-------+                +----------+-------------+--------+
    // | 1  | Alice |                | 101      | 1           | 100    |
    // | 2  | Bob   |                | 102      | 1           | 200    |
    // | 3  | Carol |                | 103      | 2           | 150    |
    // +----+-------+                | 104      | 99          | 300    |
    //                               +----------+-------------+--------+
    let customers_df = dataframe!(
        "id" => [1, 2, 3],
        "name" => ["Alice", "Bob", "Carol"]
    )?;

    let orders_df = dataframe!(
        "order_id" => [101, 102, 103, 104],
        "customer_id" => [1, 1, 2, 99],
        "amount" => [100, 200, 150, 300]
    )?;

    // Introduce a payments table: only orders 101 and 103 have payment records
    // payments_df:
    // +------------------+---------+
    // | payment_order_id | status  |
    // +------------------+---------+
    // | 101              | paid    |
    // | 103              | pending |
    // +------------------+---------+
    let payments_df = dataframe!(
        "payment_order_id" => [101, 103],
        "status" => ["paid", "pending"]
    )?;

    // 3-way join: customers → orders → payments
    // Use .select() to keep only the columns we need (avoids duplicates)
    let result = customers_df.clone()
        .join(orders_df.clone(), JoinType::Inner, &["id"], &["customer_id"], None)?
        .join(payments_df.clone(), JoinType::Inner, &["order_id"], &["payment_order_id"], None)?
        .select(vec![ // for better visibility of the result df
            col("name"),
            col("order_id"),
            col("amount"),
            col("status"),
        ])?;

    result.show().await?;
    // +-------+----------+--------+---------+
    // | name  | order_id | amount | status  |
    // +-------+----------+--------+---------+
    // | Alice | 101      | 100    | paid    |
    // | Bob   | 103      | 150    | pending |
    // +-------+----------+--------+---------+
    //
    // What got filtered out:
    // - Alice's order 102: no payment record
    // - Carol: no orders at all
    // - Order 104: orphan (customer_id 99 doesn't exist)

    Ok(())
}
```

> **Tip:** <br>
> Use Left Joins at intermediate steps if you need to preserve unmatched rows (e.g., customers without payments).

##### Join Order Matters

The order you chain joins affects both **readability** and **performance**. General principles:

| Principle                                     | Why                                                                        |
| --------------------------------------------- | -------------------------------------------------------------------------- |
| **Start with your "main" table (Left-Table)** | Reads naturally: "customers with their orders and payments"                |
| **Filter early**                              | Reduces intermediate result size before expensive joins                    |
| **Smaller tables on the right**               | Hash joins build from the right side—smaller = faster                      |
| **Let the optimizer help**                    | DataFusion may reorder joins, but good initial order reduces planning work |

```rust
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let customers_df = dataframe!(
        "id" => [1, 2, 3],
        "name" => ["Alice", "Bob", "Carol"]
    )?;

    let orders_df = dataframe!(
        "order_id" => [101, 102, 103, 104],
        "customer_id" => [1, 1, 2, 99],
        "amount" => [100, 200, 150, 300]
    )?;

    let payments_df = dataframe!(
        "payment_order_id" => [101, 103],
        "status" => ["paid", "pending"]
    )?;

    // ✅ Good: Start with the table you're "asking about"
    // "Which customers have payments?"
    let result = customers_df.clone()
        .join(orders_df.clone(), JoinType::Inner, &["id"], &["customer_id"], None)?
        .join(payments_df.clone(), JoinType::Inner, &["order_id"], &["payment_order_id"], None)?;

    result.show().await?;

    // ✅ Also good: Start with filtered data to reduce intermediate size
    let high_value_orders = orders_df.clone().filter(col("amount").gt(lit(100)))?;
    let result = high_value_orders
        .join(customers_df.clone(), JoinType::Inner, &["customer_id"], &["id"], None)?
        .join(payments_df.clone(), JoinType::Inner, &["order_id"], &["payment_order_id"], None)?;

    result.show().await?;

    Ok(())
}
```

> **Performance tip:** <br>
> The optimizer reorders joins when beneficial, but good initial ordering reduces planning overhead. Use [`.explain()`] to see the actual execution plan.

##### Managing Column Proliferation

Multi-way joins accumulate columns from every table. With each join, you get **all columns from both sides**—including duplicate key columns. Chain [`.select()`] at the end to keep only what you need:

```rust
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let customers_df = dataframe!(
        "id" => [1, 2, 3],
        "name" => ["Alice", "Bob", "Carol"]
    )?;

    let orders_df = dataframe!(
        "order_id" => [101, 102, 103, 104],
        "customer_id" => [1, 1, 2, 99],
        "amount" => [100, 200, 150, 300]
    )?;

    let payments_df = dataframe!(
        "payment_order_id" => [101, 103],
        "status" => ["paid", "pending"]
    )?;

    // Without .select(): 2 + 3 + 2 = 7 columns (with redundant id, customer_id, order_id, payment_order_id)
    // With .select(): pick only the 4 columns that matter
    let result = customers_df.clone()
        .join(orders_df.clone(), JoinType::Inner, &["id"], &["customer_id"], None)?
        .join(payments_df.clone(), JoinType::Inner, &["order_id"], &["payment_order_id"], None)?
        .select(vec![     // <--- This select removes the clutter
            col("name").alias("customer"),
            col("order_id"),
            col("amount"),
            col("status").alias("payment_status"),
        ])?;

    result.show().await?;
    // +----------+----------+--------+----------------+
    // | customer | order_id | amount | payment_status |
    // +----------+----------+--------+----------------+
    // | Alice    | 101      | 100    | paid           |
    // | Bob      | 103      | 150    | pending        |
    // +----------+----------+--------+----------------+

    Ok(())
}
```

> **When SQL might be clearer:** <br>
> Multi-way joins with 4+ tables can become hard to read as chained method calls. Consider [`SessionContext::sql()`] for complex [star-schema queries][databricks_star_schema] where SQL's visual structure helps.

#### Advanced: Join with Complex Conditions

Sometimes you need more than simple column equality. Range joins ("orders placed within 7 days of signup"), inequality predicates ("amount > threshold"), or compound logic ("match on id AND status = 'active'") require expressions that [`.join()`] can't express with just column names.

[`.join_on()`] accepts arbitrary boolean expressions as join conditions. Internally it wraps [`.join()`] with empty key lists and passes your expressions as a filter—the optimizer then extracts any equality predicates for efficient hash/sort-merge execution.

| Use Case           | Example Condition                                                       |
| ------------------ | ----------------------------------------------------------------------- |
| **Range join**     | `order_date.between(start_date, end_date)`                              |
| **Inequality**     | `col("amount").gt(col("threshold"))`                                    |
| **Compound logic** | `col("id").eq(col("customer_id")).and(col("status").eq(lit("active")))` |

```rust
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let customers_df = dataframe!(
        "id" => [1, 2, 3],
        "name" => ["Alice", "Bob", "Carol"]
    )?;

    let orders_df = dataframe!(
        "order_id" => [101, 102, 103, 104],
        "customer_id" => [1, 1, 2, 99],
        "amount" => [100, 200, 150, 300]
    )?;

    // Give DataFrames aliases so we can qualify column references
    let customers = customers_df.clone().alias("customers")?;
    let orders = orders_df.clone().alias("orders")?;

    // Join with compound condition: match on id AND filter amount > 100
    let high_value = customers.join_on(
        orders,
        JoinType::Inner,
        [col("customers.id").eq(col("orders.customer_id"))
            .and(col("orders.amount").gt(lit(100)))]
    )?;

    high_value.show().await?;
    // +----+-------+----------+-------------+--------+
    // | id | name  | order_id | customer_id | amount |
    // +----+-------+----------+-------------+--------+
    // | 1  | Alice | 102      | 1           | 200    |
    // | 2  | Bob   | 103      | 2           | 150    |
    // +----+-------+----------+-------------+--------+
    // Alice's order 101 (amount=100) excluded—doesn't meet amount > 100
    // Carol excluded—no orders at all

    Ok(())
}
```

> **Tip:** <br>
> When using [`.join_on()`], column names may clash between tables. Use [`.alias()`] to qualify references: `col("customers.id")` vs `col("orders.id")`.

##### The `filter` Argument on Outer Joins

The [`.join()`] method's fifth parameter is [`filter: Option<Expr>`][join_filter_param]—easy to overlook in the signature but powerful for outer joins. This filter has **subtle but important semantics**: it applies only to _matched_ rows, not to preserved unmatched rows.

This distinction matters because:

- A [`WHERE`] clause **after** the join would filter out unmatched rows (turning your Left Join into an Inner Join)
- The `filter` argument applies **during** the join, controlling which matches are considered valid while still preserving unmatched rows

| Approach                                      | Behavior                         | Result                              |
| --------------------------------------------- | -------------------------------- | ----------------------------------- |
| [`.join(..., Some(filter))`][`.join()`]       | Filter is part of join condition | Unmatched rows preserved with NULLs |
| [`.join(..., None).filter(...)`][`.filter()`] | Filter applied after join        | Unmatched rows may be removed       |

```rust
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let customers_df = dataframe!(
        "id" => [1, 2, 3],
        "name" => ["Alice", "Bob", "Carol"]
    )?;

    let orders_df = dataframe!(
        "order_id" => [101, 102, 103, 104],
        "customer_id" => [1, 1, 2, 99],
        "amount" => [100, 200, 150, 300]
    )?;

    // Left join with filter: Carol still appears, but only orders > 100 attach
    let result = customers_df.clone().join(
        orders_df.clone(),
        JoinType::Left,
        &["id"],                           // left_cols
        &["customer_id"],                  // right_cols
        Some(col("amount").gt(lit(100))),  // filter (5th param) - applied only to matched rows
    )?;

    result.show().await?;
    // +----+-------+----------+-------------+--------+
    // | id | name  | order_id | customer_id | amount |
    // +----+-------+----------+-------------+--------+
    // | 1  | Alice | 102      | 1           | 200    |  ←  > 100 attached
    // | 2  | Bob   | 103      | 2           | 150    |
    // | 3  | Carol |          |             |        |  ← Preserved!
    // +----+-------+----------+-------------+--------+
    // Alice's order 101 (amount=100) excluded by filter
    // Carol preserved because Left Join keeps all left rows

    Ok(())
}
```

> **Mental model:** <br>
> Think of `filter` as part of the _join condition_, not a `WHERE` after the join. It controls which matches are valid during the join itself.

> **Also applies to [`.join_on()`]:** <br>
> Since [`.join_on()`] is implemented as [`.join()`] with empty key lists and combined `on_exprs` as `filter`, the same semantics apply.

#### Anti-Pattern: Accidental Cartesian Product

Empty join keys produce a **Cartesian product**—every left row paired with every right row. This is almost never intentional and can crash your query or exhaust memory.

| Left rows | Right rows | Result rows       | Scale                            |
| --------- | ---------- | ----------------- | -------------------------------- |
| 3         | 4          | 12                | Tiny dataset, still 4× larger    |
| 1,000     | 1,000      | 1,000,000         | 1 million rows                   |
| 1,000,000 | 1,000,000  | 1,000,000,000,000 | **1 trillion rows** — will crash |

```rust
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let customers_df = dataframe!(
        "id" => [1, 2, 3],
        "name" => ["Alice", "Bob", "Carol"]
    )?;

    let orders_df = dataframe!(
        "order_id" => [101, 102, 103, 104],
        "customer_id" => [1, 1, 2, 99],
        "amount" => [100, 200, 150, 300]
    )?;

    // ❌ DANGEROUS: Empty keys = Cartesian product
    let cartesian = customers_df.clone().join(
        orders_df.clone(),
        JoinType::Inner,
        &[],  // No join keys!
        &[],
        None
    )?;

    cartesian.show().await?;
    // +----+-------+----------+-------------+--------+
    // | id | name  | order_id | customer_id | amount |
    // +----+-------+----------+-------------+--------+
    // | 1  | Alice | 101      | 1           | 100    |  ← Alice × order 101
    // | 1  | Alice | 102      | 1           | 200    |  ← Alice × order 102
    // | 1  | Alice | 103      | 2           | 150    |  ← Alice × order 103 (not her order!)
    // | 1  | Alice | 104      | 99          | 300    |  ← Alice × order 104 (not her order!)
    // | 2  | Bob   | 101      | 1           | 100    |  ← Bob × order 101 (not his order!)
    // | ... 7 more rows ... |
    // +----+-------+----------+-------------+--------+
    // Total: 3 × 4 = 12 rows — every combination!

    // ✅ CORRECT: Always specify join keys
    let correct = customers_df.clone().join(
        orders_df.clone(),
        JoinType::Inner,
        &["id"],         // left key
        &["customer_id"], // right key
        None
    )?;

    println!("Row count: {}", correct.clone().count().await?);
    // Row count: 3  ← Only matching rows (Alice×2, Bob×1)

    Ok(())
}
```

**⚠️ Warning:** <br>
If a join returns unexpectedly many rows, check your keys. An empty or mismatched key array silently produces a Cartesian product. Use [`.count()`] before [`.collect()`] to verify.

**If you need a Cartesian product:** <br>
Use SQL via [`ctx.sql("SELECT ... FROM a CROSS JOIN b")`][`SessionContext::sql()`]. The DataFrame API has no `JoinType::Cross`—empty keys with `Inner` produces the same result but reads like a bug.

#### Join Troubleshooting

Joins can silently produce unexpected results. When something looks wrong, check these common issues:

| Symptom             | Common Causes                                                             | Diagnosis                                                                   |
| :------------------ | :------------------------------------------------------------------------ | :-------------------------------------------------------------------------- |
| **Empty result**    | Key values don't match, trailing whitespace, case mismatch, NULLs in keys | Inspect both sides: `.select(vec![col("key")]).distinct().show().await?`    |
| **Too many rows**   | Duplicate keys create row multiplication, accidental Cartesian product    | Check key uniqueness: `.select(vec![col("key")]).distinct().count().await?` |
| **Missing columns** | Wrong column names after join, schema mismatch                            | Inspect schema: [`.schema()`] and use [`.alias()`] to qualify               |
| **Wrong matches**   | Keys have different types (string vs int), encoding issues                | Compare types: `df.schema().field_with_name("key")?.data_type()`            |

##### Sanity Check: Did the Join Drop Too Much Data?

DataFusion doesn't have built-in join validation, but you can build a simple check to catch silent data loss:

```rust
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let customers_df = dataframe!(
        "id" => [1, 2, 3],
        "name" => ["Alice", "Bob", "Carol"]
    )?;

    let orders_df = dataframe!(
        "order_id" => [101, 102, 103, 104],
        "customer_id" => [1, 1, 2, 99],
        "amount" => [100, 200, 150, 300]
    )?;

    // Count rows before join
    let left_count = customers_df.clone().count().await?;

    // Perform the join
    let joined = customers_df.clone().join(
        orders_df.clone(),
        JoinType::Inner,
        &["id"],
        &["customer_id"],
        None
    )?;
    let joined_count = joined.clone().count().await?;

    // Calculate retention rate
    let retention_pct = (joined_count as f64 / left_count as f64) * 100.0;
    println!("Rows: {} → {} ({:.1}% retention)", left_count, joined_count, retention_pct);

    // ⚠️ Alert if too much data was dropped
    if retention_pct < 50.0 {
        eprintln!("WARNING: Join dropped {:.1}% of rows! Check keys, NULLs, types.",
            100.0 - retention_pct);
    }

    Ok(())
}
```

| Join Type    | Expected Retention     | Warning Sign                       |
| ------------ | ---------------------- | ---------------------------------- |
| **Inner**    | Varies by data overlap | < 50% often indicates key mismatch |
| **Left**     | 100% of left rows      | < 100% means something is wrong    |
| **LeftSemi** | ≤ 100% (filtered)      | 0% = no matches at all             |
| **LeftAnti** | Complement of Semi     | 100% = nothing matched             |

##### **Quick Debugging Steps**

**Step 1:** Inspect inputs before joining

Before joining, verify that key values actually overlap. Use [`.distinct()`] to see the unique key values on each side—if they don't match, your join will produce empty or unexpected results.

```rust
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let customers_df = dataframe!(
        "id" => [1, 2, 3],
        "name" => ["Alice", "Bob", "Carol"]
    )?;

    let orders_df = dataframe!(
        "order_id" => [101, 102, 103, 104],
        "customer_id" => [1, 1, 2, 99],
        "amount" => [100, 200, 150, 300]
    )?;

    // Verify key values exist and match on both sides
    println!("Left keys:");
    customers_df.clone()
        .select(vec![col("id")])?
        .distinct()?  // Unique values only
        .show().await?;
    // +----+
    // | id |
    // +----+
    // | 1  |
    // | 2  |
    // | 3  |
    // +----+

    println!("Right keys:");
    orders_df.clone()
        .select(vec![col("customer_id")])?
        .distinct()?
        .show().await?;
    // +-------------+
    // | customer_id |
    // +-------------+
    // | 1           |
    // | 2           |
    // | 99          |  ← No matching customer! Will be dropped in Inner join
    // +-------------+

    Ok(())
}
```

**Step 2:** Check for NULL keys

In SQL semantics, `NULL = NULL` returns `UNKNOWN` (not `TRUE`), so NULL keys **never match**. This silently drops rows.

```rust
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let df = dataframe!(
        "customer_id" => [Some(1), Some(2), None],
        "value" => [100, 200, 300]
    )?;

    // Count NULLs in join key
    let null_count = df.clone()
        .filter(col("customer_id").is_null())?
        .count().await?;
    println!("NULL keys: {}", null_count);

    // Fix: Replace NULLs with sentinel value before joining
    let df = df.with_column("customer_id", coalesce(vec![col("customer_id"), lit(-1)]))?;
    df.show().await?;

    Ok(())
}
```

> **Config option:** <br>
> DataFusion has [`datafusion.optimizer.filter_null_join_keys`][`datafusion.optimizer`] to automatically filter NULL keys.

**Step 3: Examine the execution plan**

DataFusion's [`.explain()`] is your window into how the query optimizer transformed your join. It reveals which algorithm was selected, whether predicates were pushed down, and potential performance issues.

```rust
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let customers_df = dataframe!(
        "id" => [1, 2, 3],
        "name" => ["Alice", "Bob", "Carol"]
    )?;

    let orders_df = dataframe!(
        "order_id" => [101, 102],
        "customer_id" => [1, 2],
        "amount" => [100, 200]
    )?;

    let joined_df = customers_df.join(
        orders_df,
        JoinType::Inner,
        &["id"],
        &["customer_id"],
        None
    )?;

    // explain(verbose, analyze) - verbose=true shows optimized plan
    joined_df.explain(true, false)?.show().await?;

    Ok(())
}
```

**What to look for in the plan:**

| Node                 |                       Meaning                       | Performance                       |
| -------------------- | :-------------------------------------------------: | --------------------------------- |
| `HashJoinExec`       | Hash-based join (builds hash table from right side) | ✅ Fast for equi-joins            |
| `SortMergeJoinExec`  |             Sort both sides, then merge             | ✅ Good for large sorted data     |
| `NestedLoopJoinExec` |               Compares every row pair               | ⚠️ Slow — only for non-equi joins |
| `CrossJoinExec`      |                  Cartesian product                  | ❌ Usually a bug                  |

**Signs of a healthy plan:**

- Predicates pushed into `ParquetExec` or `CsvExec` (filter early)
- `HashJoinExec` or `SortMergeJoinExec` for equi-joins
- Smaller table on the **build side** (right side of hash join)

**⚠️ Warning signs:**

- `NestedLoopJoinExec` when you expected equi-join → check if optimizer couldn't extract equality predicates
- `CrossJoinExec` → accidental Cartesian product
- Filters appearing **after** the join instead of pushed down

```text
Example output (simplified):
HashJoinExec: mode=Partitioned, join_type=Inner
  left: ParquetExec: file=customers.parquet, predicate=id IS NOT NULL
  right: ParquetExec: file=orders.parquet, predicate=customer_id IS NOT NULL
                      ↑ Good! NULL filter pushed down
```

> **Pro tip:** Use `.explain(true, true)?` (analyze=true) to see actual row counts and timing after execution—helps identify which join leg is the bottleneck.

#### **Join Cheat Sheet**

Quick reference for choosing the right join pattern:

| Goal                        | Method                     | JoinType         |
| :-------------------------- | :------------------------- | :--------------- |
| Standard lookup             | [`.join()`]                | `Inner`          |
| Keep all primary records    | [`.join()`]                | `Left`           |
| Filter by existence         | [`.join()`]                | `LeftSemi`       |
| Filter by non-existence     | [`.join()`]                | `LeftAnti`       |
| See all data (reconcile)    | [`.join()`]                | `Full`           |
| Range/inequality conditions | [`.join_on()`]             | `Inner`          |
| Self-join (hierarchies)     | [`.alias()`] + [`.join()`] | `Inner/Left`     |
| Cartesian product           | Prefer SQL `CROSS JOIN`    | Empty keys = bug |

#### **Further Reading**

Joins are fundamental yet often misunderstood. These resources provide deeper understanding:

**DataFrame APIs** — Similar concepts in other libraries:

| Resource                 | Focus                                                        |
| :----------------------- | :----------------------------------------------------------- |
| [Spark Join Guide]       | Conceptually similar API with extensive examples             |
| [Polars Join Operations] | Rust-native DataFrame library, closest to DataFusion's model |
| [DataFusion `.join()`]   | Official Rust API documentation                              |

**Join Algorithms & Optimization** — How joins execute under the hood:

| Resource                           | Focus                                                                                    |
| :--------------------------------- | :--------------------------------------------------------------------------------------- |
| [Optimizing SQL & DataFrames Pt 1] | Andrew Lamb on DataFusion's optimizer—why SQL and DataFrames compile to the same plan    |
| [Optimizing SQL & DataFrames Pt 2] | Deep dive: predicate pushdown, projection pushdown, join ordering in DataFusion          |
| [DataFusion Join Optimization]     | How DataFusion uses table statistics to choose build/probe sides—**16x faster** on TPC-H |
| [CMU Join Algorithms]              | Andy Pavlo's database course—excellent video lectures on hash/sort-merge joins           |
| [Hash Join (Wikipedia)]            | How hash tables enable O(n+m) equi-joins                                                 |
| [Sort-Merge Join]                  | Why pre-sorted data enables efficient streaming joins                                    |
| [Join optimization strategies]     | How databases choose algorithms and what you can control                                 |

**SQL Semantics** — Conceptual foundations:

| Resource                                                                  | Focus                                                              |
| :------------------------------------------------------------------------ | :----------------------------------------------------------------- |
| [Visual JOIN guide]                                                       | Interactive visualization of all join types with animated examples |
| [Join tutorial]                                                           | Why Venn diagrams are misleading for understanding joins           |
| [Semi and Anti joins explained]                                           | First-class existence checks that SQL forgot                       |
| [PostgreSQL JOIN docs]                                                    | Authoritative reference—DataFusion follows PostgreSQL semantics    |
| [NULL handling in joins]                                                  | Why `NULL = NULL` is `UNKNOWN`, not `TRUE`                         |
| [Understanding SQL Dialects][Understanding SQL Dialects (medium-article)] | Medium article about different SQL dialects                        |

---

---

### Sorting and Limiting

**Sorting reorders rows by one or more columns; limiting truncates output to a fixed number of rows.**

Sorting takes a DataFrame and produces a new one where rows appear in a deterministic order based on one or more **sort keys**. Each key specifies a column (or expression) and a direction: ascending or descending. When multiple keys are provided, they form a [lexicographic order]—the first key is primary, the second breaks ties, and so on. Limiting then slices this ordered result to return only the first N rows, optionally skipping some.

DataFusion uses an [external sort] algorithm—in-memory when data fits, spilling to disk otherwise. For `ORDER BY ... LIMIT n` patterns, the optimizer applies a [Top-K] optimization using a heap, avoiding full materialization. If your source is already sorted (e.g., time-series Parquet), DataFusion's [ordering analysis] can eliminate the sort entirely.

Sorting is true to DataFusion's "excellent performance out of the box" philosophy: express _what_ you want — columns, direction, null placement — and DataFusion chooses _how_ to do it efficiently.

> **Trade-off: DataFrame vs SQL**
>
> - **DataFrame:** Programmatic control over null placement; skip and fetch in one call
> - **SQL:** Familiar `ORDER BY ... DESC NULLS LAST` syntax

#### Basic Sorting

**Single-column sorting is the most common case:** <br>
Rank students by score, list products by price, or order events chronologically. The [`.sort()`] method takes a vector of sort expressions built with `col("column").sort(asc, nulls_first)` — two booleans that control direction and null placement.

```rust
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    // employees (original data):
    // +-------------+-------+-------+
    // | department  | name  | score |
    // +-------------+-------+-------+
    // | Sales       | Alice | 85    |
    // | Sales       | Bob   | 85    |  <- duplicate score
    // | Engineering | Carol | 92    |
    // | Engineering | Dave  |       |  <- NULL score
    // |             | Eve   | 78    |  <- NULL department
    // +-------------+-------+-------+
    let employees = dataframe!(
        "department" => [
            Some("Sales"),
            Some("Sales"),
            Some("Engineering"),
            Some("Engineering"),
            None::<&str>,
        ],
        "name" => ["Alice", "Bob", "Carol", "Dave", "Eve"],
        "score" => [Some(85_i64), Some(85_i64), Some(92_i64), None, Some(78_i64)]
    )?;

    // Sort by score ascending, nulls first
    let sorted = employees.sort(vec![col("score").sort(true, true)])?;

    sorted.show().await?;
    // Result:
    // +-------------+-------+-------+
    // | department  | name  | score |
    // +-------------+-------+-------+
    // | Engineering | Dave  |       |  <- NULL first
    // |             | Eve   | 78    |
    // | Sales       | Alice | 85    |
    // | Sales       | Bob   | 85    |
    // | Engineering | Carol | 92    |
    // +-------------+-------+-------+
    Ok(())
}
```

**Quick reference** — `col("x").sort(asc, nulls_first)`:

| Call                  | Direction | Nulls | SQL Equivalent                |
| :-------------------- | :-------- | :---- | :---------------------------- |
| `.sort(true, true)`   | ASC       | first | `ORDER BY x ASC NULLS FIRST`  |
| `.sort(true, false)`  | ASC       | last  | `ORDER BY x ASC NULLS LAST`   |
| `.sort(false, true)`  | DESC      | first | `ORDER BY x DESC NULLS FIRST` |
| `.sort(false, false)` | DESC      | last  | `ORDER BY x DESC NULLS LAST`  |

When in doubt, check your output with [`.show()`] before building further.

#### Basic: Null Handling in Sorts

**NULLs require explicit handling** <br>
the `nulls_first` boolean (second parameter in [`.sort(ascending, nulls_first)`][`.sort()`]) controls whether NULLs appear at the top or bottom of results.
s
| `nulls_first` | NULLs appear | SQL equivalent | Use when |
|:--------------|:-------------|:---------------|:---------|
| `true` | First (top) | `NULLS FIRST` | Surfacing missing data for cleanup |
| `false` | Last (bottom) | `NULLS LAST` | Rankings, leaderboards, "top N" queries |

Our `employees` DataFrame has NULL values in both `department` (Eve) and `score` (Dave):

```rust
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let employees = dataframe!(
        "department" => [Some("Sales"), Some("Sales"), Some("Engineering"), Some("Engineering"), None],
        "name" => ["Alice", "Bob", "Carol", "Dave", "Eve"],
        "score" => [Some(85), Some(85), Some(92), None, Some(78)]
    )?;

    // Nulls LAST — common for rankings ("top scores" shouldn't show blanks first)
    let nulls_last = employees.clone().sort(vec![
        col("score").sort(false, false)  // DESC, nulls last
    ])?;

    nulls_last.show().await?;
    // +-------------+-------+-------+
    // | department  | name  | score |
    // +-------------+-------+-------+
    // | Engineering | Carol | 92    |
    // | Sales       | Alice | 85    |
    // | Sales       | Bob   | 85    |
    // |             | Eve   | 78    |
    // | Engineering | Dave  |       |  ← NULL pushed to bottom
    // +-------------+-------+-------+

    // Nulls FIRST — surface incomplete records for data quality review
    let nulls_first = employees.clone().sort(vec![
        col("department").sort(true, true)  // ASC, nulls first
    ])?;

    nulls_first.show().await?;
    // +-------------+-------+-------+
    // | department  | name  | score |
    // +-------------+-------+-------+
    // |             | Eve   | 78    |  ← NULL surfaced first
    // | Engineering | Carol | 92    |
    // | Engineering | Dave  |       |
    // | Sales       | Alice | 85    |
    // | Sales       | Bob   | 85    |
    // +-------------+-------+-------+

    Ok(())
}
```

> **Tip:**<br>
> When NULLs appear unexpectedly at the top or bottom of results, check the second boolean in `.sort(asc, nulls_first)`. Use `.filter(col("column").is_not_null())` before sorting to exclude them entirely.

##### Quick Preview with [`.show_limit()`]

For debugging, [`.show_limit(n)`][`.show_limit()`] is a shorthand that executes and displays the first `n` rows:

```rust
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let employees = dataframe!(
        "department" => ["Sales", "Sales", "Engineering"],
        "name" => ["Alice", "Bob", "Carol"],
        "score" => [85, 85, 92]
    )?;

    // Quick peek at data (doesn't require .await on the limit, only on show)
    employees.show_limit(3).await?;
    // +-------------+-------+-------+
    // | department  | name  | score |
    // +-------------+-------+-------+
    // | Sales       | Alice | 85    |
    // | Sales       | Bob   | 85    |
    // | Engineering | Carol | 92    |
    // +-------------+-------+-------+
    // Note: No sort applied — shows rows in natural order

    Ok(())
}
```

> **Tip:** Use `.show_limit(5)` liberally during development to inspect intermediate results without materializing entire DataFrames.

#### Basic: Limiting Results with [`.limit()`]

The [`.limit()`] method controls how many rows to return. It takes two arguments that map directly to SQL's [`OFFSET`] and [`LIMIT`]:

**Quick reference** — [`limit(skip, fetch)`][`.limit()`]:

| Call                                | SQL Equivalent         | Effect                      |
| :---------------------------------- | :--------------------- | :-------------------------- |
| [`.limit(0, Some(10))`][`.limit()`] | [`LIMIT 10`][`LIMIT`]  | First 10 rows               |
| [`.limit(5, Some(10))`][`.limit()`] | `OFFSET 5 LIMIT 10`    | Skip 5, take next 10        |
| [`.limit(0, None)`][`.limit()`]     | _(no limit)_           | All rows (default behavior) |
| [`.limit(5, None)` ][`.limit()`]    | [`OFFSET 5`][`OFFSET`] | Skip 5, take rest           |

> **Ergonomic shortcuts** for sorting and limiting:
>
> | Full API                                  | Shortcut                                    | Effect               |
> | :---------------------------------------- | :------------------------------------------ | :------------------- |
> | `.sort(vec![col("x").sort(true, false)])` | [`.sort_by(vec![col("x")])`][`.sort_by()`]  | Sort ASC, nulls last |
> | `.limit(0, Some(n))?.show().await?`       | [`.show_limit(n).await?`][`.show_limit(n)`] | Preview first n rows |

##### Basic: Top N Results

**The "Top N" pattern retrieves only the first N rows after sorting.** <br>
DataFusion recognizes this `.sort().limit(n)` combination and applies a [Top-K] optimization internally: instead of sorting the entire dataset then truncating, it maintains a heap of only N candidates — discarding rows that can't make the cut. This reduces both memory usage and execution time dramatically (benchmarks show [15x speedups][Top-K]).

> **Hint:** <br>
> Don't confuse TOP N Results with the optimization algorithm called [Top-K].

For Implementing the Top N in your code chain [`.sort()`] with [`.limit()`] to express this pattern:

```rust
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let employees = dataframe!(
        "department" => [Some("Sales"), Some("Sales"), Some("Engineering"), Some("Engineering"), None],
        "name" => ["Alice", "Bob", "Carol", "Dave", "Eve"],
        "score" => [Some(85), Some(85), Some(92), None, Some(78)]
    )?;

    // Sort: score DESC (nulls last), name ASC (tie-breaker)
    let sorted = employees.sort(vec![
        col("score").sort(false, false),
        col("name").sort(true, false)
    ])?;

    // Get top 3 performers
    let top_3 = sorted.clone().limit(0, Some(3))?;

    top_3.show().await?;
    // +-------------+-------+-------+
    // | department  | name  | score |
    // +-------------+-------+-------+
    // | Engineering | Carol | 92    |  ← Highest score
    // | Sales       | Alice | 85    |  ← Tied, but "Alice" < "Bob" alphabetically
    // | Sales       | Bob   | 85    |
    // +-------------+-------+-------+

    Ok(())
}
```

#### Intermediate: Multi-Column Sorting

**When rows tie on the primary sort key, their relative order is undefined** — DataFusion may return them in any order, and that order can change between executions. Add a secondary sort key to break ties deterministically.

Multi-column sorting uses [lexicographic order]: compare by the first key; if equal, compare by the second; and so on. The first column is the _primary_ sort, subsequent columns are _tie-breakers_.

```rust
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    // employees (original data):
    // +-------------+-------+-------+
    // | department  | name  | score |
    // +-------------+-------+-------+
    // | Sales       | Alice | 85    |
    // | Sales       | Bob   | 85    |  <- duplicate score
    // | Engineering | Carol | 92    |
    // | Engineering | Dave  |       |  <- NULL score
    // |             | Eve   | 78    |  <- NULL department
    // +-------------+-------+-------+
    let employees = dataframe!(
        "department" => [Some("Sales"), Some("Sales"), Some("Engineering"), Some("Engineering"), None],
        "name" => ["Alice", "Bob", "Carol", "Dave", "Eve"],
        "score" => [Some(85), Some(85), Some(92), None, Some(78)]
    )?;

    let sorted = employees.clone().sort(vec![
        col("score").sort(false, false),  // DESC, nulls last (primary)
        col("name").sort(true, false)     // ASC, nulls last (tie-breaker)
    ])?;

    sorted.show().await?;
    // +-------------+-------+-------+
    // | department  | name  | score |
    // +-------------+-------+-------+
    // | Engineering | Carol | 92    |
    // | Sales       | Alice | 85    |  ← Tied on score, but "Alice" < "Bob" (name ASC)
    // | Sales       | Bob   | 85    |
    // |             | Eve   | 78    |
    // | Engineering | Dave  |       |  ← NULL score pushed to bottom (nulls last)
    // +-------------+-------+-------+

    Ok(())
}
```

##### Intermediate: Pagination

**Pagination splits large result sets into smaller chunks** — essential for web APIs, dashboards, or any UI that can't display thousands of rows at once. Each "page" shows a slice of the sorted data: page 1 shows rows 1–10, page 2 shows rows 11–20, and so on.

The [`.limit(skip, fetch)`][`.limit()`] method handles both parts: `skip` jumps past already-seen rows, `fetch` takes the next chunk.

```rust
use datafusion::prelude::*;
use datafusion::error::Result;

// Helper function: fetch any page by number
fn get_page(df: &DataFrame, page: usize, page_size: usize) -> Result<DataFrame> {
    let skip = (page - 1) * page_size;
    df.clone().limit(skip, Some(page_size))
}

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let employees = dataframe!(
        "department" => [Some("Sales"), Some("Sales"), Some("Engineering"), Some("Engineering"), None],
        "name" => ["Alice", "Bob", "Carol", "Dave", "Eve"],
        "score" => [Some(85), Some(85), Some(92), None, Some(78)]
    )?;

    let sorted = employees.sort(vec![
        col("score").sort(false, false),
        col("name").sort(true, false)
    ])?;

    let page_size = 2;

    // Fetch pages dynamically — no hardcoded page_1, page_2, page_3...
    let page_1 = get_page(&sorted, 1, page_size)?;     // → Carol, Alice
    let page_2 = get_page(&sorted, 2, page_size)?;     // → Bob, Eve
    let page_3 = get_page(&sorted, 3, page_size)?;     // → Dave

    page_1.show().await?;
    page_2.show().await?;
    page_3.show().await?;

    Ok(())
}
```

**General formula:** `.limit((page - 1) * page_size, Some(page_size))`

> **⚠️ Warning:** <br>
> Each [`.limit()`] call re-executes the query from scratch — DataFusion doesn't "remember" where page 1 ended. Always apply the **same sort** before each [`.limit()`] call.

##### Advanced: Cursor-Based Pagination

**Offset-based pagination gets slower for higher page numbers** — to fetch page N, DataFusion must scan through all N×page_size rows, then discard most of them. Page 1 is fast; page 1000 scans 10,000 rows just to return 10.

| Page | Offset-Based             | Work Done        |
| :--- | :----------------------- | :--------------- |
| 1    | `.limit(0, Some(10))`    | Scan 10 rows     |
| 100  | `.limit(990, Some(10))`  | Scan 1,000 rows  |
| 1000 | `.limit(9990, Some(10))` | Scan 10,000 rows |

**Cursor-based pagination** solves this by filtering instead of skipping — jump directly to "rows after the last one I saw":

```rust
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let df = dataframe!(
        "name" => ["Alice", "Bob", "Carol", "Dave"],
        "score" => [42, 42, 85, 92]
    )?;

    let sorted = df.clone().sort(vec![
        col("score").sort(false, false),
        col("name").sort(true, false)
    ])?;

    // ❌ Offset-based: page 1000 must skip 9990 rows
    // let page_1000 = sorted.clone().limit(9990, Some(10))?;

    // ✅ Cursor-based: O(page_size) for ANY page
    // After page 999, you know the last row had score=42, name="Alice"
    let next_page = df
        .filter(
            col("score").lt(lit(42))  // Rows after last-seen score
                .or(col("score").eq(lit(42)).and(col("name").gt(lit("Alice"))))
        )?
        .sort(vec![col("score").sort(false, false), col("name").sort(true, false)])?
        .limit(0, Some(10))?;  // No skip needed!

    next_page.show().await?;

    Ok(())
}
```

| Approach   | Complexity per Page | Trade-off                              |
| :--------- | :------------------ | :------------------------------------- |
| **Offset** | O(page × page_size) | Simple, but slow for deep pages        |
| **Cursor** | O(page_size)        | Fast always, but needs unique sort key |

> **When to use which:**
>
> - **Offset:** Simple UIs, small datasets, or when users rarely go past page 10
> - **Cursor:** APIs, infinite scroll, large datasets, or when page 1000 must be as fast as page 1

**Further reading:**<br>
[Understanding Offset and Cursor Pagination] — in-depth comparison with visual examples.

#### Advanced: Sorting by Expressions

Sort keys aren't limited to column names — you can sort by **any expression**. This is powerful for computed rankings, case-insensitive ordering, or sorting by derived values.

```rust
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    // Sample data: products with price and quantity
    let products = dataframe!(
        "name" => ["Widget", "gadget", "THING", "item"],
        "price" => [10.0, 25.0, 5.0, 15.0],
        "quantity" => [100, 20, 50, 40]
    )?;

    // Sort by computed value: total inventory value (price × quantity)
    let by_value = products.clone().sort(vec![
        (col("price") * col("quantity")).sort(false, false)  // DESC
    ])?;

    by_value.show().await?;
    // +--------+-------+----------+
    // | name   | price | quantity |
    // +--------+-------+----------+
    // | Widget | 10.0  | 100      |  ← 10×100 = 1000 (highest)
    // | item   | 15.0  | 40       |  ← 15×40 = 600
    // | gadget | 25.0  | 20       |  ← 25×20 = 500
    // | THING  | 5.0   | 50       |  ← 5×50 = 250 (lowest)
    // +--------+-------+----------+

    Ok(())
}
```

**Common expression-based sorts:**

**Case-insensitive sort** <br>
Treat "Apple" and "apple" as equal by comparing lowercase versions.

```rust
use datafusion::prelude::*;
use datafusion::functions::string::expr_fn::lower;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let products = dataframe!(
        "name" => ["Widget", "gadget", "THING", "item"],
        "price" => [10.0, 25.0, 5.0, 15.0],
        "quantity" => [100, 20, 50, 40]
    )?;

    let case_insensitive = products.clone().sort(vec![
        lower(col("name")).sort(true, false)
    ])?;

    case_insensitive.show().await?;
    // +--------+-------+----------+
    // | name   | price | quantity |
    // +--------+-------+----------+
    // | gadget | 25.0  | 20       |  ← "gadget" (compared as lowercase)
    // | item   | 15.0  | 40       |
    // | THING  | 5.0   | 50       |  ← "THING" → "thing"
    // | Widget | 10.0  | 100      |  ← "Widget" → "widget"
    // +--------+-------+----------+

    Ok(())
}
```

**Sort by string length** <br>
Rank items by name length (e.g., shortest product names first for compact displays).

```rust
use datafusion::prelude::*;
use datafusion::functions::unicode::expr_fn::character_length;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let products = dataframe!(
        "name" => ["Widget", "gadget", "THING", "item"],
        "price" => [10.0, 25.0, 5.0, 15.0],
        "quantity" => [100, 20, 50, 40]
    )?;

    let by_length = products.clone().sort(vec![
        character_length(col("name")).sort(false, false)  // longest first
    ])?;

    by_length.show().await?;
    // +--------+-------+----------+
    // | name   | price | quantity |
    // +--------+-------+----------+
    // | Widget | 10.0  | 100      |  ← 6 characters
    // | gadget | 25.0  | 20       |  ← 6 characters (tied, undefined order)
    // | THING  | 5.0   | 50       |  ← 5 characters
    // | item   | 15.0  | 40       |  ← 4 characters
    // +--------+-------+----------+

    Ok(())
}
```

**Sort by absolute value** <br>
Rank by magnitude regardless of sign (e.g., largest price changes first, whether up or down).

```rust
use datafusion::prelude::*;
use datafusion::functions::math::expr_fn::abs;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let changes = dataframe!("change" => [-50, 30, -10, 25])?;
    let by_magnitude = changes.sort(vec![
        abs(col("change")).sort(false, false)  // largest magnitude first
    ])?;

    by_magnitude.show().await?;
    // +--------+
    // | change |
    // +--------+
    // | -50    |  ← abs(-50) = 50 (highest)
    // | 30     |  ← abs(30) = 30
    // | 25     |  ← abs(25) = 25
    // | -10    |  ← abs(-10) = 10 (lowest)
    // +--------+

    Ok(())
}
```

> **Tip:** <br>
> When sorting by expressions, the expression is evaluated but **not added as a column**. If you need the computed value visible, use [`.with_column()`] first, then sort by that column.

---

---

### Set Operations and Deduplication

**Set operations combine or compare DataFrames as if they were mathematical sets** <br>

Union merges rows, intersect finds common rows, except finds differences. Deduplication removes repeated rows, either across all columns or specific ones.

These operations are essential for data pipelines: combining partitioned datasets, finding records that exist in one source but not another, or cleaning up duplicate entries before analysis.

> **Trade-off: DataFrame vs SQL**
>
> - **DataFrame shines:** Schema validation at build time; DataFrame-unique methods like [`.union_by_name()`] and [`.distinct_on()`]
> - **SQL shines:** Standard [`UNION`], [`INTERSECT`], [`EXCEPT`] syntax; portable across databases

**Performance note:** <br>
Set operations and DISTINCT are **row-comparison operations** — fundamentally different from column-wise analytics. If your data resides in a row-based database (i.e.Postgres, MySQL, Oracle) via [`TableProvider`], consider pushing these operations to the source: indexed tables often deduplicate faster there than transferring data to DataFusion. For column-wise analytics (aggregations, filters, scans), DataFusion's columnar approach excels.

#### Basic: Union and Distinct

**Union** stacks two DataFrames vertically (row-wise). **Distinct** removes duplicate rows.

```rust
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let df1 = dataframe!("id" => [1, 2, 3])?;
    let df2 = dataframe!("id" => [3, 4, 5])?;
    //                          ↑ overlapping value

    // Union: stack all rows (SQL: UNION ALL)
    let union_all = df1.clone().union(df2.clone())?;

    union_all.show().await?;
    // +----+
    // | id |
    // +----+
    // | 1  |
    // | 2  |
    // | 3  |  ← from df1
    // | 3  |  ← from df2 (duplicate preserved!)
    // | 4  |
    // | 5  |
    // +----+

    // Union + deduplicate (SQL: UNION)
    let union_dedup = df1.clone().union_distinct(df2.clone())?;

    union_dedup.show().await?;
    // +----+
    // | id |
    // +----+
    // | 1  |
    // | 2  |
    // | 3  |  ← appears only once
    // | 4  |
    // | 5  |
    // +----+

    // Distinct: remove duplicates from a single DataFrame
    let with_dupes = dataframe!("id" => [1, 1, 2, 2, 3])?;
    let unique = with_dupes.distinct()?;

    unique.show().await?;
    // +----+
    // | id |
    // +----+
    // | 1  |
    // | 2  |
    // | 3  |
    // +----+

    Ok(())
}
```

#### Basic: Intersect and Except

**Intersect** finds rows that exist in _both_ DataFrames. **Except** finds rows in the first DataFrame that _don't_ exist in the second.

```rust
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let df1 = dataframe!("id" => [1, 2, 3])?;
    let df2 = dataframe!("id" => [3, 4, 5])?;

    // Intersect: rows in BOTH DataFrames (SQL: INTERSECT)
    let common = df1.clone().intersect(df2.clone())?;

    common.show().await?;
    // +----+
    // | id |
    // +----+
    // | 3  |  ← Only value in both df1 AND df2
    // +----+

    // Except: rows in df1 but NOT in df2 (SQL: EXCEPT / MINUS)
    let df1_only = df1.except(df2)?;

    df1_only.show().await?;
    // +----+
    // | id |
    // +----+
    // | 1  |  ← In df1, not in df2
    // | 2  |  ← In df1, not in df2
    // +----+

    Ok(())
}
```

> **Use cases:**
>
> - **Intersect:** Find customers who exist in both systems during a migration
> - **Except:** Find records that failed to sync, or customers who haven't placed orders

> **Troubleshooting:**
>
> - **Union fails with schema error?** DataFrames must have the same schema. Use [`.union_by_name()`] for column order flexibility, or [`.select()`] to align schemas first.
> - **`.distinct()` not working as expected?** It considers _all_ columns. For distinct on specific columns, use [`.distinct_on()`] or select only those columns first.

> **DataFrame-unique features:** DataFusion offers set operations beyond standard SQL:
>
> - [`.union_by_name()`] — Union by column _name_ instead of position (handles different column orders)
> - [`.distinct_on()`] — Keep first/last row per group (PostgreSQL-style `DISTINCT ON`)
>
> See the [DataFrame-Unique Methods](#dataframe-unique-methods) section for details.

---

### Window Functions

Window functions compute analytics (running totals, rankings, moving averages) **per row** without collapsing rows like `GROUP BY` does. Each row "sees" a window of related rows, defined by `PARTITION BY`, `ORDER BY`, and an optional frame.

In the DataFrame API you build window expressions with the [`ExprFunctionExt`] builder:

1. Start with a window or aggregate function (e.g. `row_number()`, `sum(col("sales"))`)
2. Optionally call `.partition_by([...])` to group rows
3. Call `.order_by([...])` to define ordering within each partition
4. Optionally call `.window_frame(...)` to override the default frame
5. Call `.build()?` to get an `Expr` and pass it to `.window([...])`

The following example shows how to build a window expression for the `sales_rank` column:

```rust
use datafusion::prelude::*;
use datafusion::functions_window::expr_fn::row_number;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let df = dataframe!(
        "region" => ["East", "West"],
        "sales" => [100, 200]
    )?;

    let df = df.window(vec![
        row_number()
            .partition_by(vec![col("region")])
            .order_by(vec![col("sales").sort(false, false)])  // DESC
            .build()?
            .alias("sales_rank"),
    ])?;

    df.show().await?;

    Ok(())
}
```

**SQL equivalent:**

```sql
SELECT
    region,
    day,
    sales,
    ROW_NUMBER() OVER (
        PARTITION BY region
        ORDER BY sales DESC
    ) AS sales_rank
FROM sales;
```

> **DataFrame vs. SQL**
>
> **DataFrame API**
>
> - Composable builder (`.partition_by().order_by().build()`) that you can reuse.
> - Column typos caught at compile time (when using `col("...")` centrally).
> - Multiple different window specs in a single `.window([...])` call.
>
> **SQL**
>
> - Compact `OVER (...)` syntax for simple cases.
> - Named windows (`WINDOW w AS (...)`) to reduce repetition.
> - Frame specs (`ROWS BETWEEN ...`) read very naturally.
>
> See [Window Functions](../../user-guide/sql/window_functions.md) for the full list of SQL window functions.

**Performance note:** <br>
Window functions require sorting by [`PARTITION BY`][Window_function] and [`ORDER BY`] columns. If your data resides in a row-based database (PostgreSQL, MySQL) via [`TableProvider`] with indexes on these columns, consider pushing the window operation to the source. However, when combining multiple window functions over the same partition, DataFusion optimizes by sharing the sort.

[Window_function]: ../../user-guide/sql/window_functions.md

#### Basic: Ranking

Ranking functions assign a position to each row based on sort order within a group. Common use cases include leaderboards, top-N queries, and pagination. The builder pattern constructs the window specification:

| DataFrame method  | Purpose                                              | SQL equivalent |
| ----------------- | ---------------------------------------------------- | -------------- |
| `row_number()`    | The window function—assigns unique sequential rank   | `ROW_NUMBER()` |
| `.partition_by()` | Divides rows into groups; ranking restarts per group | `PARTITION BY` |
| `.order_by()`     | Determines sort order (first in order = rank 1)      | `ORDER BY`     |
| `.build()`        | Finalizes the expression into an `Expr`              | —              |

We'll use this sample dataset throughout the window function examples:

```rust
use datafusion::prelude::*;
use datafusion::functions_window::expr_fn::row_number;
use datafusion::assert_batches_sorted_eq;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    // Sample data: daily sales by region (used throughout this section)
    let sales = dataframe!(
        "region" => ["East", "East", "East", "West", "West"],
        "day" => [1, 2, 3, 1, 2],
        "sales" => [100, 200, 150, 300, 250]
    )?;

    // Verify the input data
    let results = sales.clone().collect().await?;
    assert_batches_sorted_eq!(
        &[
            "+--------+-----+-------+",
            "| region | day | sales |",
            "+--------+-----+-------+",
            "| East   | 1   | 100   |",
            "| East   | 2   | 200   |",
            "| East   | 3   | 150   |",
            "| West   | 1   | 300   |",
            "| West   | 2   | 250   |",
            "+--------+-----+-------+",
        ],
        &results
    );

    // Rank all days by sales (highest = rank 1)
    let ranked = sales.clone().window(vec![
        row_number()
            .order_by(vec![col("sales").sort(false, false)])  // DESC
            .build()?
            .alias("sales_rank")
    ])?;

    ranked.show().await?;
    // Output:
    // +--------+-----+-------+------------+
    // | region | day | sales | sales_rank |
    // +--------+-----+-------+------------+
    // | West   | 1   | 300   | 1          |  <- best overall
    // | West   | 2   | 250   | 2          |
    // | East   | 2   | 200   | 3          |
    // | East   | 3   | 150   | 4          |
    // | East   | 1   | 100   | 5          |  <- worst overall
    // +--------+-----+-------+------------+

    Ok(())
}
```

To rank **within groups**, add `.partition_by()`. Each group gets its own ranking:

```rust
use datafusion::prelude::*;
use datafusion::functions_window::expr_fn::row_number;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let sales = dataframe!(
        "region" => ["East", "East", "East", "West", "West"],
        "day" => [1, 2, 3, 1, 2],
        "sales" => [100, 200, 150, 300, 250]
    )?;

    let ranked_by_region = sales.clone().window(vec![
        row_number()
            .partition_by(vec![col("region")])  // restart ranking per region
            .order_by(vec![col("sales").sort(false, false)])
            .build()?
            .alias("region_rank")
    ])?;

    ranked_by_region.show().await?;
    // Output:
    // +--------+-----+-------+-------------+
    // | region | day | sales | region_rank |
    // +--------+-----+-------+-------------+
    // | East   | 2   | 200   | 1           |  <- best in East
    // | East   | 3   | 150   | 2           |
    // | East   | 1   | 100   | 3           |
    // | West   | 1   | 300   | 1           |  <- best in West
    // | West   | 2   | 250   | 2           |
    // +--------+-----+-------+-------------+

    Ok(())
}
```

**Choosing a ranking function:**

Different ranking functions handle ties (equal values) differently. Choose based on whether you need unique positions or want to preserve tie information:

| Function       | Ties behavior                    | Example (values: 10, 20, 20, 30) | Use when                                              |
| -------------- | -------------------------------- | -------------------------------- | ----------------------------------------------------- |
| `row_number()` | Unique ranks, arbitrary for ties | 1, 2, 3, 4                       | You need unique positions (pagination, deduplication) |
| `rank()`       | Same rank, then skip             | 1, 2, 2, 4                       | Ties matter, gaps acceptable (competition rankings)   |
| `dense_rank()` | Same rank, no gaps               | 1, 2, 2, 3                       | Ties matter, no gaps wanted (top-N categories)        |

For further reading, you may want to read [pyspark-rank-function-with-examples].

#### Intermediate: Running Totals and Aggregates

Aggregate functions like [`sum()`] and [`avg()`] become window functions when combined with the builder pattern. Instead of collapsing all rows into one result, they compute a value for each row based on its window frame—the set of rows considered for the calculation.

| Pattern            | What it computes                 | Window frame                            | Method needed      |
| ------------------ | -------------------------------- | --------------------------------------- | ------------------ |
| **Running total**  | Cumulative sum up to current row | Default (unbounded preceding → current) | Just `.order_by()` |
| **Moving average** | Average over sliding window      | Custom (N preceding → current)          | `.window_frame()`  |

```rust
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();

    // Create sales data
    let sales = dataframe!(
        "region" => ["East", "East", "East", "West", "West"],
        "day" => [1, 2, 3, 1, 2],
        "sales" => [100, 200, 150, 300, 250]
    )?;
    ctx.register_table("sales", sales.into_view())?;

    // Running total per region using SQL window function
    let with_running_total = ctx.sql("
        SELECT region, day, sales,
               SUM(sales) OVER (PARTITION BY region ORDER BY day) as running_total
        FROM sales
    ").await?;

    with_running_total.show().await?;
    // Output:
    // +--------+-----+-------+---------------+
    // | region | day | sales | running_total |
    // +--------+-----+-------+---------------+
    // | East   | 1   | 100   | 100           |  <- day 1 only
    // | East   | 2   | 200   | 300           |  <- 100 + 200
    // | East   | 3   | 150   | 450           |  <- 100 + 200 + 150
    // | West   | 1   | 300   | 300           |  <- restarts for West
    // | West   | 2   | 250   | 550           |  <- 300 + 250
    // +--------+-----+-------+---------------+

    Ok(())
}
```

**Custom window frames** control exactly which rows are included. For a moving average over current + 1 preceding row:

```rust
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();

    // Create sales data
    let sales = dataframe!(
        "region" => ["East", "East", "East", "West", "West"],
        "day" => [1, 2, 3, 1, 2],
        "sales" => [100, 200, 150, 300, 250]
    )?;
    ctx.register_table("sales", sales.into_view())?;

    // 2-day moving average per region using SQL window function
    let with_moving_avg = ctx.sql("
        SELECT region, day, sales,
               AVG(sales) OVER (
                   PARTITION BY region
                   ORDER BY day
                   ROWS BETWEEN 1 PRECEDING AND CURRENT ROW
               ) as moving_avg_2
        FROM sales
    ").await?;

    with_moving_avg.show().await?;
    // Output:
    // +--------+-----+-------+--------------+
    // | region | day | sales | moving_avg_2 |
    // +--------+-----+-------+--------------+
    // | East   | 1   | 100   | 100.0        |  <- only day 1 available
    // | East   | 2   | 200   | 150.0        |  <- avg(100, 200)
    // | East   | 3   | 150   | 175.0        |  <- avg(200, 150)
    // | West   | 1   | 300   | 300.0        |  <- restarts for West
    // | West   | 2   | 250   | 275.0        |  <- avg(300, 250)
    // +--------+-----+-------+--------------+

    Ok(())
}
```

> **Window frame default behavior:** <br>
> When you specify `.order_by()` without `.window_frame()`, the default frame is `RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`—which gives you a running total. This matches SQL standard behavior.

#### Advanced: lead() and lag()

Compare each row with its neighbors—useful for calculating day-over-day changes, detecting trends, or finding gaps in sequences.

| Function                      | Direction         | Returns                 | When no neighbor      |
| ----------------------------- | ----------------- | ----------------------- | --------------------- |
| `lag(expr, offset, default)`  | N rows **before** | Value from previous row | `NULL` (or `default`) |
| `lead(expr, offset, default)` | N rows **after**  | Value from next row     | `NULL` (or `default`) |

```rust
use datafusion::prelude::*;
use datafusion::functions_window::expr_fn::{lag, lead};

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let sales = dataframe!(
        "region" => ["East", "East", "East", "West", "West"],
        "day" => [1, 2, 3, 1, 2],
        "sales" => [100, 200, 150, 300, 250]
    )?;

    // Compare today's sales with yesterday's and tomorrow's (per region)
    let with_comparison = sales.clone().window(vec![
        lag(col("sales"), Some(1), None)
            .partition_by(vec![col("region")])
            .order_by(vec![col("day").sort(true, false)])
            .build()?
            .alias("prev_day_sales"),
        lead(col("sales"), Some(1), None)
            .partition_by(vec![col("region")])
            .order_by(vec![col("day").sort(true, false)])
            .build()?
            .alias("next_day_sales")
    ])?;

    with_comparison.show().await?;
    // Output:
    // +--------+-----+-------+----------------+----------------+
    // | region | day | sales | prev_day_sales | next_day_sales |
    // +--------+-----+-------+----------------+----------------+
    // | East   | 1   | 100   |                | 200            |  <- no prev day
    // | East   | 2   | 200   | 100            | 150            |
    // | East   | 3   | 150   | 200            |                |  <- no next day
    // | West   | 1   | 300   |                | 250            |  <- restarts
    // | West   | 2   | 250   | 300            |                |
    // +--------+-----+-------+----------------+----------------+

    Ok(())
}
```

**Calculating day-over-day change:** Combine `lag()` with arithmetic to compute deltas. This pattern is common for trend analysis—showing growth, decline, or anomalies between consecutive periods:

```rust
use datafusion::prelude::*;
use datafusion::functions_window::expr_fn::lag;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let sales = dataframe!(
        "region" => ["East", "East", "East", "West", "West"],
        "day" => [1, 2, 3, 1, 2],
        "sales" => [100, 200, 150, 300, 250]
    )?;

    // Calculate change from previous day
    let with_change = sales.clone().window(vec![
        lag(col("sales"), Some(1), None)
            .partition_by(vec![col("region")])
            .order_by(vec![col("day").sort(true, false)])
            .build()?
            .alias("prev_sales")
    ])?
    .with_column("daily_change", col("sales") - col("prev_sales"))?;

    with_change.show().await?;
    // Output:
    // +--------+-----+-------+------------+--------------+
    // | region | day | sales | prev_sales | daily_change |
    // +--------+-----+-------+------------+--------------+
    // | East   | 1   | 100   |            |              |  <- NULL - NULL = NULL
    // | East   | 2   | 200   | 100        | 100          |  <- +100 growth
    // | East   | 3   | 150   | 200        | -50          |  <- -50 decline
    // | West   | 1   | 300   |            |              |
    // | West   | 2   | 250   | 300        | -50          |
    // +--------+-----+-------+------------+--------------+

    Ok(())
}
```

#### **Troubleshooting Window Functions:**

| Symptom                                    | Likely cause                   | Solution                                             |
| ------------------------------------------ | ------------------------------ | ---------------------------------------------------- |
| Window spans entire DataFrame              | Missing `.partition_by()`      | Add `.partition_by(vec![col("group_col")])`          |
| Wrong row gets rank 1                      | Sort direction incorrect       | Check `.sort(asc, nulls_first)` — `false` = DESC     |
| `lag()`/`lead()` returns unexpected `NULL` | At partition boundary          | Expected behavior; use `default` parameter if needed |
| Running total includes wrong rows          | Default frame vs custom        | Add `.window_frame()` for precise control            |
| `last_value()` returns current row         | Default frame stops at current | Use `UNBOUNDED FOLLOWING` in custom frame            |

> **Tip:** <br>
> When debugging, add `.sort()` after `.window()` to see results in a predictable order—window functions don't guarantee output row order.

---

### Reshaping Data

**Reshaping transforms the structure of your data—changing rows to columns or columns to rows without altering the underlying values.** <br>

Two common reshaping patterns exist in data processing:

| Operation          | What it does                              | DataFrame support        |
| ------------------ | ----------------------------------------- | ------------------------ |
| **Explode/Unnest** | Expands array elements into separate rows | ✅ [`.unnest_columns()`] |
| **Melt/Unpivot**   | Converts columns into rows (wide → long)  | ❌ Not available         |

Unnesting is essential when working with nested JSON data, multi-valued fields, or array columns from Parquet files. For melt/unpivot operations, see the workaround in [DataFrame-Unique Methods](#dataframe-unique-methods).

> **See also:** [pandas.DataFrame.explode], [PySpark explode] — similar operations in other DataFrame libraries.

#### Unnesting / Exploding Arrays

Unnesting expands each element of an array column into a **separate row**, duplicating the other columns. This is essential when working with nested JSON, multi-valued fields, or array columns from Parquet files.

```rust
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();

    // Input: customers with array of order IDs
    // +----------+------------+
    // | customer | orders     |
    // +----------+------------+
    // | Alice    | [1, 2, 3]  |
    // | Bob      | [4, 5]     |
    // +----------+------------+
    let df = ctx.sql("
        SELECT * FROM (VALUES
            ('Alice', ARRAY[1, 2, 3]),
            ('Bob', ARRAY[4, 5])
        ) AS t(customer, orders)
    ").await?;

    // Unnest expands array elements into separate rows
    let expanded = df.unnest_columns(&["orders"])?;

    expanded.show().await?;
    // Output: one row per array element
    // +----------+--------+
    // | customer | orders |
    // +----------+--------+
    // | Alice    | 1      |
    // | Alice    | 2      |
    // | Alice    | 3      |
    // | Bob      | 4      |
    // | Bob      | 5      |
    // +----------+--------+

    Ok(())
}
```

> **See also:** [PySpark explode] — similar operation in Spark DataFrames.

---

### Subqueries

**Subqueries embed one query inside another—enabling comparisons against computed values or filtered datasets.** <br>

Use subqueries when a filter or expression depends on data from another query. In the DataFrame API, you build subqueries by creating a [`LogicalPlan`] and wrapping it with the appropriate function.

| Subquery type | What it returns       | Expression function   | SQL example                                     |
| ------------- | --------------------- | --------------------- | ----------------------------------------------- |
| **Scalar**    | Single value          | [`scalar_subquery()`] | `WHERE amount > (SELECT AVG(amount) FROM t)`    |
| **IN**        | List membership       | [`in_subquery()`]     | `WHERE id IN (SELECT customer_id FROM premium)` |
| **EXISTS**    | Boolean (rows exist?) | [`exists()`]          | `WHERE EXISTS (SELECT 1 FROM orders WHERE ...)` |

> **Trade-off: DataFrame vs SQL**
>
> - **DataFrame shines:** Type-safe subquery construction; reusable subquery plans as variables; subqueries can be built conditionally
> - **SQL shines:** Nested syntax is more readable; familiar to SQL users; less boilerplate for simple cases

#### Scalar Subqueries

A scalar subquery returns **exactly one value** used in comparisons. Common use cases: filtering against an aggregate (average, max, count) or looking up a single reference value.

**Pattern:** Build the subquery as a [`LogicalPlan`] via [`.into_unoptimized_plan()`], then wrap with [`scalar_subquery()`]:

```rust
use datafusion::prelude::*;
use datafusion::functions_aggregate::expr_fn::avg;
use std::sync::Arc;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();

    // Input: orders table
    // +----------+--------+
    // | order_id | amount |
    // +----------+--------+
    // | 1        | 100    |
    // | 2        | 200    |
    // | 3        | 150    |
    // | 4        | 300    |
    // +----------+--------+
    let orders = dataframe!(
        "order_id" => [1, 2, 3, 4],
        "amount" => [100, 200, 150, 300]
    )?;
    ctx.register_table("orders", orders.clone().into_view())?;

    // Build subquery: SELECT AVG(amount) FROM orders → 187.5
    let avg_subquery = ctx.table("orders").await?
        .aggregate(vec![], vec![avg(col("amount"))])?
        .select(vec![avg(col("amount"))])?
        .into_unoptimized_plan();

    // Filter: WHERE amount > (subquery)
    let result = ctx.table("orders").await?
        .filter(col("amount").gt(scalar_subquery(Arc::new(avg_subquery))))?;

    result.show().await?;
    // Output: orders where amount > 187.5
    // +----------+--------+
    // | order_id | amount |
    // +----------+--------+
    // | 2        | 200    |
    // | 4        | 300    |
    // +----------+--------+

    Ok(())
}
```

#### IN Subqueries

An IN subquery checks if a value **exists in a list** returned by another query. Common use cases: filtering by membership in a lookup table, finding related records, or excluding specific IDs.

**Pattern:** Build the subquery returning a single column, wrap with `in_subquery(column, plan)`:

```rust
use datafusion::prelude::*;
use std::sync::Arc;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();

    // Input: customers and premium lookup tables
    // customers:              premium:
    // +----+-------+          +-------------+
    // | id | name  |          | customer_id |
    // +----+-------+          +-------------+
    // | 1  | Alice |          | 1           |
    // | 2  | Bob   |          | 3           |
    // | 3  | Carol |          +-------------+
    // +----+-------+
    let customers = dataframe!(
        "id" => [1, 2, 3],
        "name" => ["Alice", "Bob", "Carol"]
    )?;

    let premium_customers = dataframe!(
        "customer_id" => [1, 3]
    )?;

    ctx.register_table("customers", customers.into_view())?;
    ctx.register_table("premium", premium_customers.into_view())?;

    // Build subquery: SELECT customer_id FROM premium
    let premium_ids = ctx.table("premium").await?
        .select(vec![col("customer_id")])?
        .into_unoptimized_plan();

    // Filter: WHERE id IN (subquery)
    let result = ctx.table("customers").await?
        .filter(in_subquery(col("id"), Arc::new(premium_ids)))?;

    result.show().await?;
    // Output: customers where id IN (1, 3)
    // +----+-------+
    // | id | name  |
    // +----+-------+
    // | 1  | Alice |
    // | 3  | Carol |
    // +----+-------+

    Ok(())
}
```

---

### Summary: Shared Transformations

**Every operation in this chapter has a direct SQL equivalent—the logic is identical, only the syntax differs.** Both APIs compile to the same logical plan, so performance is equivalent.

| Operation        | DataFrame                            | SQL                  |
| ---------------- | ------------------------------------ | -------------------- |
| Select columns   | `.select()`, `.select_columns()`     | `SELECT`             |
| Filter rows      | `.filter()`                          | `WHERE`              |
| Aggregate        | `.aggregate()`                       | `GROUP BY`           |
| Join tables      | `.join()`                            | `JOIN`               |
| Sort results     | `.sort()`                            | `ORDER BY`           |
| Limit rows       | `.limit()`                           | `LIMIT`              |
| Set operations   | `.union()`, `.intersect()`           | `UNION`, `INTERSECT` |
| Window functions | `.window()` + builder                | `OVER (...)`         |
| Unnest arrays    | `.unnest_columns()`                  | `UNNEST`             |
| Subqueries       | `scalar_subquery()`, `in_subquery()` | `(SELECT ...)`       |

> **When to use which?**
>
> - **DataFrame API:** Complex logic with conditionals, reusable pipelines, compile-time checks, or when building queries programmatically.
> - **SQL API:** Ad-hoc exploration, familiar syntax, or when porting existing SQL queries.

**Next:** [DataFrame-Unique Methods](#dataframe-unique-methods) covers operations that have no direct SQL equivalent.

**See also:**

- [SQL SELECT reference](../../user-guide/sql/select.md) — detailed SQL syntax
- [Window Functions](../../user-guide/sql/window_functions.md) — all window functions
- [Subqueries](../../user-guide/sql/subqueries.md) — SQL subquery patterns

---

## DataFrame-Unique Methods

**Some DataFrame methods have no SQL equivalent—these are the programmatic superpowers that justify using the DataFrame API.**

While the previous section covered operations available in _both_ APIs, this section highlights methods unique to DataFrames or methods where they shine due to their ergonomics. These exist because SQL's declarative grammar cannot express certain programmatic patterns that Rust handles naturally. For more see the [builder-methodology](#builder-methodology-architecting-with-dataframe) section.

For a more detailed overview, the following table list all the methods unique to the DataFrame API , a clustering of those in sections and the reasons why they are unique.

| Method                                                 | Purpose                                 | Why SQL Can't Express It                   |
| ------------------------------------------------------ | --------------------------------------- | ------------------------------------------ |
| **Schema Manipulation**                                |                                         |                                            |
| [`.with_column()`](#schema-manipulation)               | Add/replace a column keeping all others | SQL `SELECT` requires listing all columns  |
| [`.with_column_renamed()`](#schema-manipulation)       | Rename without expression               | SQL uses `AS` inside `SELECT`              |
| [`.drop_columns()`](#schema-manipulation)              | Remove columns by name                  | SQL has no direct equivalent               |
| **Set Operations by Name**                             |                                         |                                            |
| [`.union_by_name()`](#set-operations-by-name)          | Union aligned by name, not position     | SQL `UNION` is positional                  |
| [`.union_by_name_distinct()`](#set-operations-by-name) | Same with deduplication                 | SQL `UNION DISTINCT` is positional         |
| **SQL-DataFrame Hybrid**                               |                                         |                                            |
| [`.parse_sql_expr()`](#sql-dataframe-hybrid-methods)   | Parse SQL string into `Expr`            | Bridges SQL syntax into DataFrame code     |
| [`.select_exprs()`](#sql-dataframe-hybrid-methods)     | Select using SQL expression strings     | Combines SQL ergonomics with chaining      |
| [`.with_param_values()`](#parameter-binding)           | Bind parameter values to placeholders   | Plan-level operation, not a SQL clause     |
| **Data Exploration**                                   |                                         |                                            |
| [`.describe()`](#describing-data)                      | Summary statistics for all columns      | No single SQL statement equivalent         |
| **Convenience Methods**                                |                                         |                                            |
| [`.fill_null()`](#convenience-methods)                 | Fill nulls with default value           | Wrapper—SQL requires `COALESCE` per column |
| [`.cache()`](#convenience-methods)                     | Materialize DataFrame in memory         | Execution control—no SQL concept           |
| **Execution Control**                                  |                                         |                                            |
| [`.collect_partitioned()`](#execution-control)         | Collect preserving partitions           | Partition-aware execution                  |
| [`.execute_stream()`](#execution-control)              | Stream results without buffering        | Streaming execution control                |
| [`.execute_stream_partitioned()`](#execution-control)  | Stream per partition                    | Parallel streaming execution               |
| **Creation**                                           |                                         |                                            |
| [`.from_columns()`](#creating-from-columns)            | Create from column arrays               | Programmatic construction                  |
| **Array/Nested Data**                                  |                                         |                                            |
| [`.unnest_columns()`](#unnesting-arrays)               | Explode arrays into rows                | SQL `UNNEST` varies by database            |
| [`.unnest_columns_with_options()`](#unnesting-arrays)  | Unnest with fine-grained control        | Recursive depth, null handling             |
| **Bridging to SQL**                                    |                                         |                                            |
| [`.into_view()`](#bridging-to-sql)                     | Register DataFrame as SQL table         | Enables hybrid SQL/DataFrame workflows     |

> **Methods with SQL equivalents:** Some methods have SQL counterparts but offer ergonomic advantages:
>
> - **`.distinct_on()`** — DataFusion supports `SELECT DISTINCT ON (...)` in SQL (PostgreSQL-style, issues [#7827], [#7981])
> - **`.alias()`** — Equivalent to `SELECT * FROM (...) AS my_alias` subquery aliasing

[#7827]: https://github.com/apache/datafusion/issues/7827
[#7981]: https://github.com/apache/datafusion/issues/7981
[#12907]: https://github.com/apache/datafusion/issues/12907

> **Gap Note:** `unpivot`/`melt` (wide-to-long reshaping) is not yet available in DataFusion See [Issue #12907][#12907]. Workaround: manual `UNION ALL` of columns.

### Schema Manipulation

These methods modify column structure without requiring you to enumerate all columns—a common pain point in SQL. For more see the [Schema Management](../schema-management.md).

#### Adding and Replacing Columns

[`.with_column()`] adds a new column or replaces an existing one _while keeping all other columns intact_. In SQL, you'd need to explicitly list every column in your `SELECT`.

```rust
use datafusion::prelude::*;
use datafusion::error::Result;

#[tokio::main]
async fn main() -> Result<()> {
    let df = dataframe!(
        "product" => ["Laptop", "Mouse"],
        "price" => [1200, 25]
    )?;

    // Add a computed column - all existing columns are preserved
    let df = df.with_column("discounted", col("price") * lit(0.9))?;

    df.clone().show().await?;
    // +---------+-------+------------+
    // | product | price | discounted |
    // +---------+-------+------------+
    // | Laptop  | 1200  | 1080.0     |
    // | Mouse   | 25    | 22.5       |
    // +---------+-------+------------+

    // Replace an existing column (same name overwrites)
    let df = df.with_column("price", col("price") * lit(1.1))?;

    df.show().await?;
    // +---------+--------+------------+
    // | product | price  | discounted |
    // +---------+--------+------------+
    // | Laptop  | 1320.0 | 1080.0     |
    // | Mouse   | 27.5   | 22.5       |
    // +---------+--------+------------+

    Ok(())
}
```

> **Why this matters:**
> With 2 columns the SQL is fine, but imagine a table with 20 columns—you'd have to list all 20 just to add one computed column. `.with_column()` scales effortlessly.

#### Renaming Columns

[`.with_column_renamed()`] renames a column without requiring an expression—just the old name and new name. Like all DataFrame methods, it's **lazy**: no execution happens until you call a terminal action.

```rust
use datafusion::prelude::*;
use datafusion::error::Result;
use datafusion::assert_batches_eq;

#[tokio::main]
async fn main() -> Result<()> {
    let df = dataframe!(
        "price" => [100, 200],
        "qty" => [5, 10]
    )?;

    // Rename is lazy - just updates the logical plan
    let df = df.with_column_renamed("price", "unit_price")?;

    // Chain multiple renames
    let df = df
        .with_column_renamed("qty", "quantity")?
        .with_column_renamed("unit_price", "cost")?;

    // Execute to see results
    let results = df.collect().await?;
    assert_batches_eq!(
        &[
            "+------+----------+",
            "| cost | quantity |",
            "+------+----------+",
            "| 100  | 5        |",
            "| 200  | 10       |",
            "+------+----------+",
        ],
        &results
    );
    Ok(())
}
```

> **SQL equivalent:** `SELECT price AS unit_price, qty AS quantity FROM ...`
>
> The difference: SQL's `AS` is part of the projection—you must list all columns. `.with_column_renamed()` touches only the renamed column, passing others through unchanged.

#### Dropping Columns

[`.drop_columns()`] removes columns by name. SQL has no equivalent—you must list all columns you want to _keep_ instead.

```rust
use datafusion::prelude::*;
use datafusion::error::Result;
use datafusion::assert_batches_eq;

#[tokio::main]
async fn main() -> Result<()> {
    let df = dataframe!(
        "id" => [1, 2],
        "name" => ["Alice", "Bob"],
        "temp_id" => [999, 998],
        "category" => ["A", "B"]
    )?;

    // Remove multiple columns at once
    let df = df.drop_columns(&["category", "temp_id"])?;

    let results = df.collect().await?;
    assert_batches_eq!(
        &[
            "+----+-------+",
            "| id | name  |",
            "+----+-------+",
            "| 1  | Alice |",
            "| 2  | Bob   |",
            "+----+-------+",
        ],
        &results
    );
    Ok(())
}
```

> **SQL workaround:** `SELECT id, name FROM ...` — must explicitly list every column to keep. With 20 columns, dropping 2 means listing 18.

---

### Set Operations by Name

**This is where DataFusion's [Arrow columnar design](../../user-guide/arrow-introduction.md) shines.**

SQL's `UNION` matches columns by _position_, not name. If two tables have the same columns in different orders, SQL silently produces incorrect results—a common source of bugs when combining data from different sources.

Because Arrow schemas carry column names as metadata, DataFusion can align DataFrames by name instead of position. This is impossible in traditional row-based databases where columns are just offsets.

| Method                        | Duplicate Rows | SQL Equivalent                |
| ----------------------------- | -------------- | ----------------------------- |
| [`.union_by_name()`]          | Keeps all      | `UNION ALL` + reorder columns |
| [`.union_by_name_distinct()`] | Removes        | `UNION` + reorder columns     |

#### Union by Column Name

[`.union_by_name()`] aligns DataFrames by column _name_, not position:

```rust
use datafusion::prelude::*;
use datafusion::error::Result;
use datafusion::assert_batches_sorted_eq;

#[tokio::main]
async fn main() -> Result<()> {
    let q1 = dataframe!(
        "product" => ["A", "B"],
        "revenue" => [100, 200]
    )?;

    let q2 = dataframe!(
        "revenue" => [100, 300],  // Different column order! Row (A, 100) duplicates q1
        "product" => ["A", "C"]
    )?;

    // union_by_name keeps ALL rows (including duplicates)
    let combined = q1.union_by_name(q2)?;

    let results = combined.collect().await?;
    assert_batches_sorted_eq!(
        &[
            "+---------+---------+",
            "| product | revenue |",
            "+---------+---------+",
            "| A       | 100     |",
            "| A       | 100     |",  // Duplicate kept!
            "| B       | 200     |",
            "| C       | 300     |",
            "+---------+---------+",
        ],
        &results
    );
    Ok(())
}
```

[`.union_by_name_distinct()`] removes duplicate rows after aligning by name:

```rust
use datafusion::prelude::*;
use datafusion::error::Result;
use datafusion::assert_batches_sorted_eq;

#[tokio::main]
async fn main() -> Result<()> {
    let q1 = dataframe!(
        "product" => ["A", "B"],
        "revenue" => [100, 200]
    )?;

    let q2 = dataframe!(
        "revenue" => [100, 300],  // First row duplicates q1
        "product" => ["A", "C"]
    )?;

    // Combine and deduplicate
    let combined = q1.union_by_name_distinct(q2)?;

    let results = combined.collect().await?;
    assert_batches_sorted_eq!(
        &[
            "+---------+---------+",
            "| product | revenue |",
            "+---------+---------+",
            "| A       | 100     |",
            "| B       | 200     |",
            "| C       | 300     |",
            "+---------+---------+",
        ],
        &results
    );
    Ok(())
}
```

> **When to use:** Data pipelines combining sources with inconsistent column ordering (e.g., different Parquet writers, CSV exports from different tools).
>
> **SQL equivalent:** None—SQL `UNION` is strictly positional. You'd need to manually reorder columns in one of the queries to match.

**Limitations:**

| Requirement       | Description                                                      | Workaround                             |
| ----------------- | ---------------------------------------------------------------- | -------------------------------------- |
| Same column names | Both DataFrames must have identical column names                 | Rename with [`.with_column_renamed()`] |
| Compatible types  | Types must be castable (`Int32` ↔ `Int64` ✓, `Int32` ↔ `Utf8` ✗) | Cast columns first                     |
| No extra columns  | Columns in one but not the other cause errors                    | Use [`.drop_columns()`] to align       |

### SQL-DataFrame Hybrid Methods

These methods let you combine SQL's familiar syntax with DataFrame's programmatic power—the best of both worlds.

| Method                | Input             | Output      | Use Case                                  |
| --------------------- | ----------------- | ----------- | ----------------------------------------- |
| [`.parse_sql_expr()`] | SQL string        | `Expr`      | Single expression from config/user input  |
| [`.select_exprs()`]   | SQL strings array | `DataFrame` | Multiple computed columns with SQL syntax |

#### Parsing SQL Expressions

[`.parse_sql_expr()`] converts a SQL expression string into a DataFusion `Expr`. Useful when you want SQL syntax for complex expressions but DataFrame chaining for the overall pipeline:

```rust
use datafusion::prelude::*;
use datafusion::error::Result;
use datafusion::assert_batches_eq;

#[tokio::main]
async fn main() -> Result<()> {
    let ctx = SessionContext::new();

    let df = ctx.sql("SELECT 1 as a, 2 as b, 3 as c").await?;

    // Parse SQL expression string into an Expr
    let expr = df.parse_sql_expr("a + b * 2")?;

    // Use it in DataFrame operations
    let df = df.select(vec![col("a"), col("b"), expr.alias("computed")])?;

    let results = df.collect().await?;
    assert_batches_eq!(
        &[
            "+---+---+----------+",
            "| a | b | computed |",
            "+---+---+----------+",
            "| 1 | 2 | 5        |",
            "+---+---+----------+",
        ],
        &results
    );
    Ok(())
}
```

> **Use case:** Dynamically building expressions from user input or configuration files while maintaining type safety in the rest of your pipeline.

#### Selecting with SQL Expressions

[`.select_exprs()`] takes an array of SQL expression strings and projects them—combining SQL's concise syntax with DataFrame chaining:

```rust
use datafusion::prelude::*;
use datafusion::error::Result;

#[tokio::main]
async fn main() -> Result<()> {
    let ctx = SessionContext::new();

    let df = ctx.sql("SELECT 'Alice' as name, 100 as price, 0.1 as tax_rate").await?;

    // Use SQL syntax for complex expressions
    let df = df.select_exprs(&[
        "UPPER(name) AS upper_name",
        "price * (1 + tax_rate) AS total",
        "CASE WHEN price > 50 THEN 'expensive' ELSE 'cheap' END AS category"
    ])?;

    df.show().await?;
    // +------------+-------+-----------+
    // | upper_name | total | category  |
    // +------------+-------+-----------+
    // | ALICE      | 110.0 | expensive |
    // +------------+-------+-----------+
    Ok(())
}
```

> **Why use this over pure SQL?** You get SQL's expression syntax while keeping DataFrame's:
>
> - **Chaining:** `.filter()`, `.join()`, `.aggregate()` flow naturally
> - **Composition:** Build pipelines programmatically
> - **Type checking:** Rust compiler catches method name typos

### Parameter Binding

[`.with_param_values()`] binds parameter values to placeholders (`$1`, `$2`, ...) in a plan—useful for prepared statement patterns and preventing SQL injection:

```rust
use datafusion::prelude::*;
use datafusion::error::Result;
use datafusion::common::ScalarValue;

#[tokio::main]
async fn main() -> Result<()> {
    // Create data and register it
    let df = dataframe!(
        "name" => ["Alice", "Bob", "Carol"],
        "age" => [30, 25, 35]
    )?;

    // Filter using a runtime parameter
    let min_age = 28;  // Could come from user input, config, etc.
    let filtered = df.filter(col("age").gt(lit(min_age)))?;

    filtered.show().await?;
    // +-------+-----+
    // | name  | age |
    // +-------+-----+
    // | Alice | 30  |
    // | Carol | 35  |
    // +-------+-----+

    Ok(())
}
```

> **Use cases:**
>
> - **Reusable templates** — Build query once, bind different values
> - **User input** — Safely inject user-supplied values without SQL injection risk
> - **Dynamic filtering** — Change filter values without rebuilding the plan

### Describing Data

[`.describe()`] generates summary statistics for all columns—similar to pandas' `df.describe()`. No single SQL statement can do this:

```rust
use datafusion::prelude::*;
use datafusion::error::Result;

#[tokio::main]
async fn main() -> Result<()> {
    let df = dataframe!(
        "product" => ["A", "B", "C", "D", "E"],
        "price" => [10.0, 25.0, 15.0, 30.0, 20.0],
        "quantity" => [100, 50, 75, 25, 60]
    )?;

    // Get summary statistics
    let stats = df.describe().await?;
    stats.show().await?;

    // Output includes: count, null_count, mean, std, min, max, median
    // +------------+---------+-------+----------+
    // | describe   | product | price | quantity |
    // +------------+---------+-------+----------+
    // | count      | 5.0     | 5.0   | 5.0      |
    // | null_count | 0.0     | 0.0   | 0.0      |
    // | mean       | null    | 20.0  | 62.0     |
    // | std        | null    | 7.9   | 27.4     |
    // | min        | A       | 10.0  | 25       |
    // | max        | E       | 30.0  | 100      |
    // | median     | null    | 20.0  | 60.0     |
    // +------------+---------+-------+----------+
    Ok(())
}
```

> **SQL equivalent:** Would require 7+ separate aggregate queries unioned together—tedious and error-prone.

### Convenience Methods

These methods wrap common patterns into single, ergonomic calls.

| Method           | Purpose                    | SQL Equivalent        |
| ---------------- | -------------------------- | --------------------- |
| [`.fill_null()`] | Replace nulls with default | `COALESCE` per column |
| [`.cache()`]     | Materialize in memory      | None                  |

#### Filling Null Values

[`.fill_null()`] replaces null values with a default—in SQL you'd need `COALESCE` for each column:

```rust
use datafusion::prelude::*;
use datafusion::error::Result;
use datafusion_common::ScalarValue;
use std::sync::Arc;
use arrow::array::{Int32Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

#[tokio::main]
async fn main() -> Result<()> {
    // Create data with nulls
    let schema = Arc::new(Schema::new(vec![
        Field::new("name", DataType::Utf8, true),
        Field::new("score", DataType::Int32, true),
    ]));

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec![Some("Alice"), None, Some("Carol")])),
            Arc::new(Int32Array::from(vec![Some(95), Some(87), None])),
        ],
    )?;

    let ctx = SessionContext::new();
    let df = ctx.read_batch(batch)?;

    // Fill nulls in specific columns
    let df = df.fill_null(ScalarValue::from("Unknown"), vec!["name".to_string()])?;

    // Or fill all compatible columns
    let df = df.fill_null(ScalarValue::from(0i32), vec![])?;

    df.show().await?;
    // +---------+-------+
    // | name    | score |
    // +---------+-------+
    // | Alice   | 95    |
    // | Unknown | 87    |
    // | Carol   | 0     |
    // +---------+-------+
    Ok(())
}
```

> **SQL equivalent:** `SELECT COALESCE(name, 'Unknown'), COALESCE(score, 0) FROM ...`—must list each column explicitly.

#### Caching DataFrames

[`.cache()`] materializes a DataFrame into memory, useful when you need to reuse intermediate results:

```rust
use datafusion::prelude::*;
use datafusion::error::Result;

#[tokio::main]
async fn main() -> Result<()> {
    let ctx = SessionContext::new();

    // Imagine this is an expensive computation
    let df = ctx.sql("SELECT value AS id FROM generate_series(1, 1000)").await?
        .filter(col("id").gt(lit(500)))?;

    // Cache the result in memory
    let cached = df.cache().await?;

    // Now reuse without recomputing
    let count1 = cached.clone().count().await?;
    let count2 = cached.clone().filter(col("id").lt(lit(750)))?.count().await?;

    println!("Total: {}, Filtered: {}", count1, count2);
    Ok(())
}
```

> **When to use:** Iterative algorithms, multiple aggregations over the same filtered data, or when the source is expensive to read (remote storage, complex joins).

### Execution Control

These methods provide fine-grained control over how query results are produced—essential for memory management and parallel processing.

| Method                            | Returns                 | Use Case                         |
| --------------------------------- | ----------------------- | -------------------------------- |
| [`.execute_stream()`]             | Single stream           | Large datasets, memory-efficient |
| [`.collect_partitioned()`]        | `Vec<Vec<RecordBatch>>` | Process partitions independently |
| [`.execute_stream_partitioned()`] | Multiple streams        | Parallel streaming               |

#### Streaming Results

[`.execute_stream()`] returns results as a stream rather than collecting into memory:

```rust
use datafusion::prelude::*;
use datafusion::error::Result;
use futures::StreamExt;

#[tokio::main]
async fn main() -> Result<()> {
    let df = dataframe!(
        "id" => [1, 2, 3, 4, 5],
        "value" => [10, 20, 30, 40, 50]
    )?;

    // Process results as a stream (memory-efficient for large datasets)
    let mut stream = df.execute_stream().await?;

    let mut total_rows = 0;
    while let Some(batch) = stream.next().await {
        let batch = batch?;
        total_rows += batch.num_rows();
        println!("Processed batch with {} rows", batch.num_rows());
    }
    println!("Total: {} rows", total_rows);
    Ok(())
}
```

#### Partition-Aware Execution

[`.collect_partitioned()`] and [`.execute_stream_partitioned()`] preserve the underlying data partitioning:

```rust
use datafusion::prelude::*;
use datafusion::error::Result;

#[tokio::main]
async fn main() -> Result<()> {
    let ctx = SessionContext::new();

    let df = ctx.sql("SELECT value AS id FROM generate_series(1, 100)").await?;

    // Collect preserving partitions (useful for parallel processing)
    let partitioned_batches = df.collect_partitioned().await?;

    println!("Number of partitions: {}", partitioned_batches.len());
    for (i, partition) in partitioned_batches.iter().enumerate() {
        let rows: usize = partition.iter().map(|b| b.num_rows()).sum();
        println!("Partition {}: {} rows", i, rows);
    }
    Ok(())
}
```

> **When to use:**
>
> - `.execute_stream()` — Large datasets that don't fit in memory
> - `.collect_partitioned()` — When you need to process partitions independently
> - `.execute_stream_partitioned()` — Parallel streaming across partitions

### Creating from Columns

[`.from_columns()`] creates a DataFrame directly from column arrays—useful for programmatic data construction:

```rust
use datafusion::prelude::*;
use datafusion::error::Result;
use datafusion::assert_batches_eq;
use std::sync::Arc;
use arrow::array::{ArrayRef, Int32Array, StringArray};

#[tokio::main]
async fn main() -> Result<()> {
    // Build columns programmatically
    let ids: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3]));
    let names: ArrayRef = Arc::new(StringArray::from(vec!["Alice", "Bob", "Carol"]));

    let df = DataFrame::from_columns(vec![
        ("id", ids),
        ("name", names),
    ])?;

    let results = df.collect().await?;
    assert_batches_eq!(
        &[
            "+----+-------+",
            "| id | name  |",
            "+----+-------+",
            "| 1  | Alice |",
            "| 2  | Bob   |",
            "| 3  | Carol |",
            "+----+-------+",
        ],
        &results
    );
    Ok(())
}
```

> **Alternative:** The `dataframe!` macro is more concise for literals, but `from_columns()` is better when building from existing `ArrayRef` data.

### Unnesting Arrays

[`.unnest_columns()`] explodes array (list) columns into multiple rows—one row per array element. SQL's `UNNEST` syntax varies significantly across databases; DataFusion provides a consistent API.

```rust
use datafusion::prelude::*;
use datafusion::error::Result;
use datafusion::assert_batches_eq;
use std::sync::Arc;
use arrow::array::{ArrayRef, StringArray, ListArray, Int32Array};
use arrow::datatypes::{DataType, Field};
use arrow::buffer::OffsetBuffer;

#[tokio::main]
async fn main() -> Result<()> {
    // Create a DataFrame with a list column
    let tags_field = Arc::new(Field::new_list_field(DataType::Utf8, true));
    let tags = ListArray::new(
        tags_field,
        OffsetBuffer::from_lengths([2, 1]),  // Alice has 2 tags, Bob has 1
        Arc::new(StringArray::from(vec!["vip", "early", "new"])),
        None
    );

    let df = DataFrame::from_columns(vec![
        ("customer", Arc::new(StringArray::from(vec!["Alice", "Bob"])) as ArrayRef),
        ("tags", Arc::new(tags) as ArrayRef),
    ])?;

    // Explode the tags array into rows
    let expanded = df.unnest_columns(&["tags"])?;

    let results = expanded.collect().await?;
    assert_batches_eq!(
        &[
            "+----------+-------+",
            "| customer | tags  |",
            "+----------+-------+",
            "| Alice    | vip   |",
            "| Alice    | early |",
            "| Bob      | new   |",
            "+----------+-------+",
        ],
        &results
    );
    Ok(())
}
```

#### Controlling Unnest Behavior with Options

[`.unnest_columns_with_options()`] provides fine-grained control via [`UnnestOptions`]:

```rust
use datafusion::prelude::*;
use datafusion::error::Result;
use datafusion::assert_batches_eq;
use datafusion_common::UnnestOptions;
use std::sync::Arc;
use arrow::array::{ArrayRef, StringArray, ListArray};
use arrow::datatypes::{DataType, Field};
use arrow::buffer::OffsetBuffer;

#[tokio::main]
async fn main() -> Result<()> {
    // Create data with null and empty arrays
    let tags_field = Arc::new(Field::new_list_field(DataType::Utf8, true));

    // Alice: ["vip"], Bob: null, Carol: [] (empty)
    let tags = ListArray::new(
        tags_field,
        OffsetBuffer::from_lengths([1, 0, 0]),
        Arc::new(StringArray::from(vec!["vip"])),
        Some(vec![true, false, true].into())  // Bob's entry is null
    );

    let df = DataFrame::from_columns(vec![
        ("customer", Arc::new(StringArray::from(vec!["Alice", "Bob", "Carol"])) as ArrayRef),
        ("tags", Arc::new(tags) as ArrayRef),
    ])?;

    // Default: preserve_nulls = true (keeps null rows)
    let with_nulls = df.clone()
        .unnest_columns_with_options(&["tags"], UnnestOptions::new())?;

    let results = with_nulls.collect().await?;
    assert_batches_eq!(
        &[
            "+----------+------+",
            "| customer | tags |",
            "+----------+------+",
            "| Alice    | vip  |",
            "| Bob      |      |",
            "+----------+------+",
        ],
        &results
    );

    // Skip nulls and empty arrays
    let without_nulls = df
        .unnest_columns_with_options(
            &["tags"],
            UnnestOptions::new().with_preserve_nulls(false)
        )?;

    let results = without_nulls.collect().await?;
    assert_batches_eq!(
        &[
            "+----------+------+",
            "| customer | tags |",
            "+----------+------+",
            "| Alice    | vip  |",
            "+----------+------+",
        ],
        &results
    );
    Ok(())
}
```

**UnnestOptions fields:**

| Option           | Default | Effect                                                               |
| ---------------- | ------- | -------------------------------------------------------------------- |
| `preserve_nulls` | `true`  | Keep rows where the array is `NULL` (outputs `NULL` for that column) |
| `recursions`     | `[]`    | For nested arrays, specify recursion depth per column                |

> **Nested arrays:** For deeply nested structures (e.g., `List<List<Int>>`), use `RecursionUnnestOption` to control how many levels to flatten.

[`UnnestOptions`]: https://docs.rs/datafusion/latest/datafusion/common/struct.UnnestOptions.html

### Bridging to SQL

[`.into_view()`] converts a DataFrame into a [`TableProvider`] that can be registered as a SQL-queryable table—enabling hybrid workflows where you build with DataFrames and query with SQL.

```rust
use datafusion::prelude::*;
use datafusion::error::Result;
use datafusion::assert_batches_eq;

#[tokio::main]
async fn main() -> Result<()> {
    let ctx = SessionContext::new();

    // Build a pipeline with DataFrame API
    let df = dataframe!(
        "id" => [1, 2, 3, 4],
        "value" => [100, 200, 300, 400]
    )?
    .filter(col("value").gt(lit(150)))?;

    // Register as a SQL-queryable view
    ctx.register_table("filtered_data", df.into_view())?;

    // Query with SQL
    let result = ctx.sql("SELECT * FROM filtered_data WHERE id > 2").await?;

    let batches = result.collect().await?;
    assert_batches_eq!(
        &[
            "+----+-------+",
            "| id | value |",
            "+----+-------+",
            "| 3  | 300   |",
            "| 4  | 400   |",
            "+----+-------+",
        ],
        &batches
    );
    Ok(())
}
```

> **Use case:** Complex pipelines where some transformations are easier in DataFrame (programmatic column manipulation) and others are easier in SQL (complex joins, window functions with familiar syntax).

[`TableProvider`]: https://docs.rs/datafusion/latest/datafusion/catalog/trait.TableProvider.html

### Methods with SQL Equivalents

These methods have SQL counterparts but offer ergonomic advantages for programmatic use.

#### DISTINCT ON (PostgreSQL-Style)

[`.distinct_on()`] keeps the first row for each unique value in specified columns. DataFusion also supports this via SQL (`SELECT DISTINCT ON (...)`), but the DataFrame method integrates naturally into pipelines:

```rust
use datafusion::prelude::*;
use datafusion::error::Result;

#[tokio::main]
async fn main() -> Result<()> {
    let ctx = SessionContext::new();

    // Create and register a table for the SQL DISTINCT ON example
    let df = dataframe!(
        "customer" => ["Alice", "Alice", "Bob", "Bob"],
        "order_date" => ["2024-01-01", "2024-01-15", "2024-01-05", "2024-01-02"],
        "amount" => [100, 200, 150, 75]
    )?;
    ctx.register_table("orders", df.into_view())?;

    // Use SQL DISTINCT ON which DataFusion supports
    let first_orders = ctx.sql("
        SELECT DISTINCT ON (customer) customer, order_date, amount
        FROM orders
        ORDER BY customer, order_date ASC
    ").await?;

    first_orders.show().await?;
    // +----------+------------+--------+
    // | customer | order_date | amount |
    // +----------+------------+--------+
    // | Alice    | 2024-01-01 | 100    |
    // | Bob      | 2024-01-02 | 75     |
    // +----------+------------+--------+
    Ok(())
}
```

**SQL equivalent:**

```sql
SELECT DISTINCT ON (customer) customer, order_date, amount
FROM orders
ORDER BY customer, order_date ASC;
```

#### Aliasing DataFrames

[`.alias()`] applies a table qualifier to all columns—equivalent to SQL subquery aliasing (`SELECT * FROM (...) AS my_alias`), but useful for DataFrame self-joins:

```rust
use datafusion::prelude::*;
use datafusion::error::Result;
use datafusion::assert_batches_sorted_eq;

#[tokio::main]
async fn main() -> Result<()> {
    let employees = dataframe!(
        "id" => [1, 2, 3],
        "name" => ["Alice", "Bob", "Carol"],
        "manager_id" => [0, 1, 1]  // 0 = no manager
    )?;

    // Self-join: find each employee's manager name
    let emp = employees.clone().alias("emp")?;
    let mgr = employees.alias("mgr")?;

    let with_managers = emp.join(
        mgr,
        JoinType::Left,
        &["manager_id"],
        &["id"],
        None
    )?
    .select(vec![
        col("emp.name").alias("employee"),
        col("mgr.name").alias("manager")
    ])?;

    let results = with_managers.collect().await?;
    assert_batches_sorted_eq!(
        &[
            "+----------+---------+",
            "| employee | manager |",
            "+----------+---------+",
            "| Alice    |         |",
            "| Bob      | Alice   |",
            "| Carol    | Alice   |",
            "+----------+---------+",
        ],
        &results
    );
    Ok(())
}
```

### Summary: DataFrame-Unique Methods

**The DataFrame API isn't just SQL with different syntax—it's a fundamentally different paradigm enabled by Arrow's columnar architecture.**

#### Why These Methods Exist

| Category                   | Key Insight                                                       |
| -------------------------- | ----------------------------------------------------------------- |
| **Schema Manipulation**    | SQL requires listing all columns; DataFrames modify surgically    |
| **Set Operations by Name** | Arrow schemas carry names as metadata—impossible in row-based DBs |
| **SQL-DataFrame Hybrid**   | Best of both worlds: SQL syntax + programmatic composition        |
| **Execution Control**      | Fine-grained memory/streaming control SQL can't express           |
| **Data Exploration**       | `.describe()` in one call vs 7+ SQL queries                       |

#### The Power Stack

```text
┌─────────────────────────────────────────────────────────────┐
│  Your Application                                           │
├─────────────────────────────────────────────────────────────┤
│  DataFrame API          │  SQL API                          │
│  • Type-safe            │  • Familiar syntax                │
│  • Composable           │  • Ad-hoc queries                 │
│  • Programmatic         │  • Complex window functions       │
├─────────────────────────────────────────────────────────────┤
│  DataFusion Query Engine (shared optimizer & executor)      │
├─────────────────────────────────────────────────────────────┤
│  Apache Arrow (columnar memory format)                      │
│  • Zero-copy operations  • SIMD acceleration                │
│  • Schema metadata       • Cross-language compatibility     │
└─────────────────────────────────────────────────────────────┘
```

#### Quick Reference

| Method                     | One-Liner                                     |
| -------------------------- | --------------------------------------------- |
| [`.with_column()`]         | Add/replace column without listing all others |
| [`.with_column_renamed()`] | Rename without expression                     |
| [`.drop_columns()`]        | Remove columns by name                        |
| [`.union_by_name()`]       | Union aligned by column name, not position    |
| [`.parse_sql_expr()`]      | SQL expression → `Expr`                       |
| [`.select_exprs()`]        | SQL strings → projection                      |
| [`.with_param_values()`]   | Bind parameters safely                        |
| [`.describe()`]            | Summary statistics in one call                |
| [`.fill_null()`]           | Replace nulls across columns                  |
| [`.cache()`]               | Materialize for reuse                         |
| [`.execute_stream()`]      | Memory-efficient streaming                    |
| [`.collect_partitioned()`] | Partition-aware collection                    |
| [`.from_columns()`]        | Create from Arrow arrays                      |
| [`.unnest_columns()`]      | Explode arrays to rows                        |
| [`.into_view()`]           | Bridge to SQL world                           |

> **The takeaway:** <br>

- Use DataFrame methods when you need:
  - programmatic control
  - schema manipulation
  - execution flexibility.
- Use SQL when you need
  - familiar syntax
  - complex window functions.

Both compile to the same optimized plan—choose based on ergonomics, not performance.

---

<!--TODO Set the builder methodolgy at the very top-->

## Builder Methodology: Architecting with DataFrames

**The DataFrame API isn't just SQL with different syntax—it's a programmatic _builder_ for query plans that integrates with Rust's type system, control flow, and tooling.**

When you write SQL, you write a _string_ that gets parsed, planned, and executed in one shot. When you use the DataFrame API, you're _constructing a query plan_ step by step, storing intermediate stages in Rust variables, branching the plan with `if/else`, and composing reusable transformations as functions. The plan doesn't execute until you explicitly ask for results.

This is the **[Builder Pattern][builder_pattern]**—a design pattern where you construct a complex object (the query plan) through a series of method calls, each returning a modified builder (a new DataFrame). The diagram below illustrates this two-phase architecture:

```text
                         THE BUILDER PATTERN
    ════════════════════════════════════════════════════════════

     LAZY PHASE (builds plan)              EAGER PHASE (runs)
    ┌────────────────────────────┐        ┌───────────────────┐
    │                            │        │                   │
    │  df ──► .filter() ──► step1│        │  .collect() ──► Data
    │          (new DF)   (new DF)        │  .show()    ──► Output
    │                        │   │        │  .count()   ──► Number
    │           ┌────────────┴───┼────────┼─────────────────────┐
    │           │                │        │                     │
    │           ▼                ▼        │                     │
    │     .aggregate()       .select()    │  SAME step1 feeds   │
    │       (new DF)          (new DF)    │  BOTH branches!     │
    │           │                │        │                     │
    │           ▼                ▼        │                     │
    │       summary          details ─────┼──► .show()          │
    │                                     │                     │
    └─────────────────────────────────────┴─────────────────────┘

    Key: Each method returns a NEW DataFrame (immutable).
         Use .clone() to branch: step1.clone().aggregate(...)
```

**What the diagram shows:**

- **Left side (Lazy Phase):** Each transformation method ([`.filter()`], [`.select()`], [`.aggregate()`]) returns a _new_ DataFrame containing an extended logical plan. No data moves yet—you're just building a blueprint.
- **Right side (Eager Phase):** Terminal actions ([`.collect()`], [`.show()`], [`.count()`]) trigger actual execution. Only then does DataFusion optimize the plan and process data.
- **Branching:** The variable `step1` can feed _multiple_ downstream paths. Unlike SQL CTEs (which exist only within a single query), Rust variables persist across your entire program scope.
- **Immutability:** The original `df` is unchanged after calling [`.filter()`]. Each method returns a fresh DataFrame, enabling safe parallel experimentation.

This architecture unlocks patterns impossible in SQL: dynamic query construction with Rust control flow, reusable transformation functions, and compile-time validation of your pipeline structure.

| Pattern                                                         | What It Enables                           | SQL Limitation                       |
| --------------------------------------------------------------- | ----------------------------------------- | ------------------------------------ |
| [Builder Pattern & Laziness](#the-builder-pattern-and-laziness) | Reuse intermediate plans as variables     | CTEs are query-scoped                |
| [Dynamic Construction](#dynamic-pipeline-construction)          | Rust `if/else` modifies the plan          | String concatenation, injection risk |
| [Encapsulation](#encapsulation-and-reusability)                 | Functions returning `Expr` or `DataFrame` | UDFs are hard to deploy/test         |
| [Memory & Streaming](#memory-management--streaming)             | Control collect vs stream execution       | No equivalent control                |
| [Error Handling](#error-handling-and-observability)             | Compile-time + runtime error separation   | All errors at runtime                |

**Running Example:** Throughout this section, we'll use a single `sales` DataFrame to demonstrate all patterns. This reduces cognitive load and shows how each technique applies to the same data:

```rust
use datafusion::prelude::*;
use datafusion::assert_batches_eq;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let sales = dataframe!(
        "order_id" => [1, 2, 3, 4, 5],
        "product" => ["Laptop", "Mouse", "Keyboard", "Monitor", "Laptop"],
        "category" => ["Electronics", "Accessories", "Accessories", "Electronics", "Electronics"],
        "price" => [1200, 25, 75, 350, 1100],
        "quantity" => [1, 5, 2, 1, 2],
        "customer" => ["Alice", "Bob", "Alice", "Carol", "Bob"]
    )?;

    let batches = sales.clone().collect().await?;
    assert_batches_eq!(
        [
            "+----------+----------+-------------+-------+----------+----------+",
            "| order_id | product  | category    | price | quantity | customer |",
            "+----------+----------+-------------+-------+----------+----------+",
            "| 1        | Laptop   | Electronics | 1200  | 1        | Alice    |",
            "| 2        | Mouse    | Accessories | 25    | 5        | Bob      |",
            "| 3        | Keyboard | Accessories | 75    | 2        | Alice    |",
            "| 4        | Monitor  | Electronics | 350   | 1        | Carol    |",
            "| 5        | Laptop   | Electronics | 1100  | 2        | Bob      |",
            "+----------+----------+-------------+-------+----------+----------+",
        ],
        &batches
    );

    Ok(())
}
```

### The Builder Pattern and Laziness

**Every DataFrame method returns a new DataFrame wrapping an extended [`LogicalPlan`]—no data moves until you call an action like [`.collect()`] or [`.show()`].**

Each transformation method (`.filter()`, `.select()`, `.sort()`) returns a _new_ DataFrame containing a logical plan—a description of _what_ to compute, not the computed result. The original DataFrame is immutable - it remains unchanged.

This lazy evaluation enables DataFusion's optimizer to see the entire pipeline before execution. It can push filters down, eliminate unused columns, and choose optimal join strategies—optimizations that would be impossible if execution happened at each step.

> **Projection Pushdown:**<br>
> When you call [`.select()`] to choose specific columns, DataFusion pushes this information down to the data source. For columnar formats like Parquet, this means only the bytes for selected columns are read from disk. This is a major performance advantage of Lazy Evaluation compared to eager systems (like Pandas) which often read the entire file into memory before filtering columns.

You chain method calls, storing intermediate DataFrames in Rust variables. Only when you call a terminal action ([`.collect()`], [`.show()`], [`.count()`]) does DataFusion optimize and execute the plan.

> **Fluent Interface:** <br>
> This chaining style is known as a [Fluent Interface][fluent_interface]—a design pattern where methods return `self` (or a modified copy) to enable readable chains. If you know Spark's DataFrame API, DataFusion's architecture is conceptually similar to [Spark's Catalyst Optimizer][catalyst_optimizer], but implemented in Rust.

> **See also:** [Concepts § Lazy Evaluation](concepts.md#lazy-evaluation) for a deeper dive into how DataFusion builds and optimizes logical plans.

**In rust code:**

```rust
use datafusion::prelude::*;
use datafusion::assert_batches_eq;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let sales = dataframe!(
        "order_id" => [1, 2, 3, 4, 5],
        "product" => ["Laptop", "Mouse", "Keyboard", "Monitor", "Laptop"],
        "category" => ["Electronics", "Accessories", "Accessories", "Electronics", "Electronics"],
        "price" => [1200, 25, 75, 350, 1100],
        "quantity" => [1, 5, 2, 1, 2],
        "customer" => ["Alice", "Bob", "Alice", "Carol", "Bob"]
    )?;

    // Each step builds a plan, doesn't execute
    let step1 = sales.clone().filter(col("price").gt(lit(100)))?;   // Plan: Filter
    let step2 = step1.select(vec![col("product"), col("price")])?;  // Plan: Filter → Project
    let step3 = step2.sort(vec![col("price").sort(false, true)])?;  // Plan: Filter → Project → Sort

    // Inspect the plan without executing
    println!("{}", step3.logical_plan().display_indent());

    // Only NOW does execution happen
    let batches = step3.collect().await?;
    assert_batches_eq!(
        [
            "+---------+-------+",
            "| product | price |",
            "+---------+-------+",
            "| Laptop  | 1200  |",
            "| Laptop  | 1100  |",
            "| Monitor | 350   |",
            "+---------+-------+",
        ],
        &batches
    );

    Ok(())
}
```

**Variables vs CTEs: A Mental Model Shift**

If you're coming from SQL, you might think of intermediate results like CTEs (`WITH step1 AS (...)`). DataFrames work differently: each step lives in a Rust variable that persists across your entire program scope—not just within a single query. This table highlights the key differences:

| Aspect               | DataFrame (Rust)                               | SQL (CTEs)              |
| -------------------- | ---------------------------------------------- | ----------------------- |
| Intermediate storage | Rust variables                                 | `WITH step1 AS (...)`   |
| Reuse across queries | Variable lives in scope                        | CTE is query-scoped     |
| Debugging            | [`.schema()`], [`.explain()`] at any point     | Must execute to inspect |
| Branching            | `step1.filter(...)` and `step1.aggregate(...)` | Duplicate the CTE       |

The practical benefit: you can inspect, branch, or reuse any intermediate DataFrame without re-executing the pipeline.

> **Footgun:** DataFrame is _consumed_ by transformations. To reuse, call [`.clone()`]:
>
> ```rust
> use datafusion::prelude::*;
> use datafusion::functions_aggregate::expr_fn::sum;
>
> #[tokio::main]
> async fn main() -> datafusion::error::Result<()> {
>     let sales = dataframe!("category" => ["A"], "price" => [100])?;
>     let filtered = sales.clone().filter(col("price").gt(lit(100)))?;  // sales still usable
>     let aggregated = sales.aggregate(vec![col("category")], vec![sum(col("price"))])?;  // sales consumed
>     Ok(())
> }
> ```

### Dynamic Pipeline Construction

**Use Rust control flow (`if/else`, `match`, loops) to build query plans dynamically—something SQL strings make dangerous and error-prone.**

With SQL, dynamic queries often lead to string concatenation—a pattern prone to injection attacks and syntax errors. The DataFrame API eliminates both risks: values pass through [`lit()`] which properly escapes and types them, and Rust's type system ensures column references are valid at build time.

**Control Flow vs String Concatenation**

```rust
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let sales = dataframe!("category" => ["Electronics"], "price" => [100])?;
    let filter_category: Option<&str> = Some("Electronics");

    // ❌ SQL: String concatenation (injection risk, runtime errors)
    let mut query = "SELECT * FROM sales WHERE 1=1".to_string();
    if let Some(cat) = filter_category {
        query.push_str(&format!(" AND category = '{}'", cat));  // 💀 Injection!
    }

    // ✅ DataFrame: Type-safe, validated at build time
    let mut result = sales.clone();
    if let Some(cat) = filter_category {
        result = result.filter(col("category").eq(lit(cat)))?;  // Safe: lit() handles escaping
    }

    result.show().await?;
    Ok(())
}
```

Rust's ownership model adds another layer: the `?` operator ensures errors propagate correctly, and the compiler verifies that `result` is properly reassigned in each branch.

#### Schema-Driven Transformations

Sometimes you don't know the column names or types until runtime—perhaps you're building a generic data processing library, or working with user-uploaded files. The DataFrame API lets you introspect the schema and build transformations dynamically.

The pattern: call [`.schema()`] to get the DataFrame's structure, iterate over fields, and construct expressions based on each column's name and type. This is impossible with static SQL where the query text is fixed at write time.

Using our `sales` DataFrame, let's double all numeric columns while keeping string columns unchanged.

```rust
use datafusion::prelude::*;
use datafusion::assert_batches_eq;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let sales = dataframe!(
        "order_id" => [1, 2, 3, 4, 5],
        "product" => ["Laptop", "Mouse", "Keyboard", "Monitor", "Laptop"],
        "category" => ["Electronics", "Accessories", "Accessories", "Electronics", "Electronics"],
        "price" => [1200, 25, 75, 350, 1100],
        "quantity" => [1, 5, 2, 1, 2],
        "customer" => ["Alice", "Bob", "Alice", "Carol", "Bob"]
    )?;

    // Introspect schema at runtime
    let schema = sales.schema();

    // Build expressions dynamically based on column types
    // Note: We match multiple numeric types to handle real-world data
    let transformed_cols: Vec<_> = schema
        .fields()
        .iter()
        .map(|f| {
            if f.data_type().is_numeric() {
                // Double numeric columns (Int32, Int64, Float32, Float64, etc.)
                (col(f.name()) * lit(2)).alias(format!("{}_doubled", f.name()))
            } else {
                // Keep non-numeric columns as-is
                col(f.name())
            }
        })
        .collect();

    let doubled = sales.clone().select(transformed_cols)?;
    doubled.show().await?;

    Ok(())
}
```

Notice how `order_id`, `price`, and `quantity` (all `Int64`) were doubled and renamed, while `product`, `category`, and `customer` (strings) passed through unchanged.

> **Note:** <br>
> Arrow's [`DataType`] provides the helper method [`.is_numeric()`] which returns `true` for `Int8`, `Int16`, `Int32`, `Int64`, `UInt*`, `Float32`, `Float64`, and `Decimal` types—saving you from writing verbose match statements:

See more information at the [Schema Management](schema-management.md) section.

### Encapsulation and Reusability

**Move complex transformation logic into reusable Rust functions—no UDF registration, no deployment headaches, full unit-testability.**

SQL UDFs require registration with the execution context and have limited composability. Rust functions are native citizens: they compose naturally, benefit from IDE tooling, and can be unit-tested in isolation.

DataFusion supports encapsulation at **two levels**:

| Level               | Returns             | Use Case                        | Example                                                      |
| ------------------- | ------------------- | ------------------------------- | ------------------------------------------------------------ |
| **Column-level**    | `Expr`              | Reusable column transformations | `clean_currency("amount")` → use in `.select()`, `.filter()` |
| **DataFrame-level** | `Result<DataFrame>` | Multi-step pipeline stages      | `summarize_sales(df)` → filtering, aggregating, joining      |

Both approaches let you build a library of tested, composable transformations that work across any DataFrame with compatible schemas.

**Native Functions vs SQL UDFs**

| Aspect       | Rust Functions        | SQL UDFs                       |
| ------------ | --------------------- | ------------------------------ |
| Registration | None needed           | `ctx.register_udf(...)`        |
| Testing      | Standard `#[test]`    | Requires execution context     |
| IDE support  | Full autocomplete     | None                           |
| Composition  | Direct function calls | Limited nesting                |
| Distribution | Compiled into binary  | Must be registered per context |

> **Need actual UDFs?** <br>
> For custom scalar functions (UDFs) or aggregate functions (UDAFs) that must be registered with the context, see [Adding User Defined Functions](../../library-user-guide/functions/adding-udfs.md).

#### Functions Returning [`Expr`] (Column-Level)

When you find yourself writing the same column expression repeatedly—parsing dates, cleaning strings, computing derived values—extract it into a function that returns an [`Expr`]. This keeps your pipeline code clean and makes the logic testable in isolation.

The pattern: write a Rust function that takes column names (or other parameters) and returns an [`Expr`]. You can then use this expression anywhere DataFusion expects one: in [`.select()`], [`.with_column()`], [`.filter()`], etc.

```rust
use datafusion::prelude::*;

/// Calculate profit margin as a percentage
fn profit_margin(revenue_col: &str, cost_col: &str) -> Expr {
    ((col(revenue_col) - col(cost_col)) / col(revenue_col) * lit(100))
        .alias("profit_margin_pct")
}

/// Price category based on value
fn price_category(price_col: &str) -> datafusion::error::Result<Expr> {
    Ok(case(col(price_col).gt(lit(100)))
        .when(lit(true), lit("expensive"))
        .otherwise(lit("affordable"))?
        .alias("category"))
}

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let df = dataframe!(
        "product" => ["Laptop", "Mouse"],
        "revenue" => [150.0, 250.0],
        "cost" => [100.0, 150.0],
        "price" => [1200, 25]
    )?;

    // Usage: reusable across any DataFrame
    let df = df
        .with_column("margin", profit_margin("revenue", "cost"))?
        .with_column("price_tier", price_category("price")?)?;

    df.show().await?;
    Ok(())
}
```

These functions compose naturally—you can nest them, combine them with other expressions, or use them in aggregations.

#### Functions Returning `DataFrame` (Table-Level)

While [`Expr`] functions transform individual columns, sometimes you need to encapsulate an entire multi-step pipeline—filtering, joining, aggregating—into a reusable unit. Functions that take a [`DataFrame`] and return a [`Result<DataFrame>`] let you build composable pipeline stages.

This pattern shines when you have standard transformations applied across different datasets: data cleaning pipelines, report generators, or feature engineering steps for ML.

Using our `sales` DataFrame:

```rust
use datafusion::prelude::*;
use datafusion::functions_aggregate::expr_fn::*;
use datafusion::assert_batches_sorted_eq;

/// Calculate order totals and summarize by category
fn summarize_sales(df: DataFrame) -> datafusion::error::Result<DataFrame> {
    df.with_column("total", col("price") * col("quantity"))?
      .aggregate(
          vec![col("category")],
          vec![
              sum(col("total")).alias("revenue"),
              count(lit(1)).alias("order_count"),
          ]
      )
}

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let sales = dataframe!(
        "order_id" => [1, 2, 3, 4, 5],
        "product" => ["Laptop", "Mouse", "Keyboard", "Monitor", "Laptop"],
        "category" => ["Electronics", "Accessories", "Accessories", "Electronics", "Electronics"],
        "price" => [1200, 25, 75, 350, 1100],
        "quantity" => [1, 5, 2, 1, 2],
        "customer" => ["Alice", "Bob", "Alice", "Carol", "Bob"]
    )?;

    // Usage: apply the same summarization to any sales-like DataFrame
    let summary = summarize_sales(sales.clone())?;
    let batches = summary.collect().await?;
    assert_batches_sorted_eq!(
        [
            "+-------------+---------+-------------+",
            "| category    | revenue | order_count |",
            "+-------------+---------+-------------+",
            "| Accessories | 275     | 2           |",
            "| Electronics | 3750    | 3           |",
            "+-------------+---------+-------------+",
        ],
        &batches
    );

    Ok(())
}
```

You can chain these functions together to build complex pipelines from simple, tested building blocks.

#### Conditional Query Building

Real-world applications rarely have fixed queries. Users filter by different criteria, APIs accept optional parameters, and reports need configurable groupings. The DataFrame API lets you build queries conditionally using standard Rust control flow—`if let`, `match`, loops—without the SQL string concatenation anti-pattern.

This is where the builder pattern truly shines: <br>
Each transformation returns a new DataFrame, so you can conditionally apply steps based on runtime parameters while keeping the code readable and type-safe.

```rust
use datafusion::prelude::*;
use datafusion::assert_batches_eq;

/// Build a sales query with optional filters and configurable sorting
fn build_sales_query(
    df: DataFrame,
    min_price: Option<i32>,
    category: Option<String>,
    sort_desc: bool,
) -> datafusion::error::Result<DataFrame> {
    let mut result = df;

    // Apply filters only if parameters are provided
    if let Some(price) = min_price {
        result = result.filter(col("price").gt(lit(price)))?;
    }
    if let Some(cat) = category {
        result = result.filter(col("category").eq(lit(cat)))?;
    }

    // sort(ascending, nulls_first)
    // If sort_desc is true, ascending must be false (!sort_desc)
    result = result.sort(vec![col("price").sort(!sort_desc, true)])?;

    Ok(result)
}

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let sales = dataframe!(
        "order_id" => [1, 2, 3, 4, 5],
        "product" => ["Laptop", "Mouse", "Keyboard", "Monitor", "Laptop"],
        "category" => ["Electronics", "Accessories", "Accessories", "Electronics", "Electronics"],
        "price" => [1200, 25, 75, 350, 1100],
        "quantity" => [1, 5, 2, 1, 2],
        "customer" => ["Alice", "Bob", "Alice", "Carol", "Bob"]
    )?;

    // Example: high-value electronics, sorted by price descending
    let query = build_sales_query(
        sales.clone(),
        Some(100),                      // min_price: only items > $100
        Some("Electronics".into()),     // category: only Electronics
        true                            // sort_desc: highest price first
    )?;

    let batches = query.collect().await?;
    assert_batches_eq!(
        [
            "+----------+---------+-------------+-------+----------+----------+",
            "| order_id | product | category    | price | quantity | customer |",
            "+----------+---------+-------------+-------+----------+----------+",
            "| 1        | Laptop  | Electronics | 1200  | 1        | Alice    |",
            "| 5        | Laptop  | Electronics | 1100  | 2        | Bob      |",
            "| 4        | Monitor | Electronics | 350   | 1        | Carol    |",
            "+----------+---------+-------------+-------+----------+----------+",
        ],
        &batches
    );

    Ok(())
}
```

Compare this to SQL where you'd either write multiple query variants or resort to string concatenation—both error-prone and hard to test. Here, the logic is explicit, the types are checked, and you can unit-test `build_sales_query` with different parameter combinations.

### Memory Management & Streaming

Up to this point, we've built query plans without moving any data. Now we need to actually execute them. DataFusion offers three execution methods, each with different memory characteristics:

| Method                              | Memory | Use Case                                |
| ----------------------------------- | ------ | --------------------------------------- |
| [`.show()`]                         | High   | Quick debugging (loads ALL data!)       |
| [`.show_limit(n)`][`.show_limit()`] | Low    | Safe inspection (loads only n rows)     |
| [`.collect()`]                      | High   | Small datasets, need all data in memory |
| [`.execute_stream()`]               | Low    | Large datasets, incremental processing  |

> **Note:** <br>
> This section covers execution patterns. For production tuning, see [Best Practices](best-practices.md).

#### Execution Methods Compared

Each method triggers plan optimization and execution, but they differ in how results are delivered:

```rust
use datafusion::prelude::*;
use futures::StreamExt;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let sales = dataframe!(
        "order_id" => [1, 2, 3, 4, 5],
        "product" => ["Laptop", "Mouse", "Keyboard", "Monitor", "Laptop"],
        "category" => ["Electronics", "Accessories", "Accessories", "Electronics", "Electronics"],
        "price" => [1200, 25, 75, 350, 1100],
        "quantity" => [1, 5, 2, 1, 2],
        "customer" => ["Alice", "Bob", "Alice", "Carol", "Bob"]
    )?;

    // show() - Collects ALL data, then prints formatted table
    // WARNING: Loads entire dataset into memory (same as collect())
    sales.clone().show().await?;

    // show_limit(n) - Limits to n rows, THEN collects and prints
    // SAFE: Only loads n rows - use this for large/unknown datasets
    sales.clone().show_limit(10).await?;

    // collect() - Returns Vec<RecordBatch> with ALL data in memory
    // Use when you need programmatic access to results
    let batches = sales.clone().collect().await?;
    println!("Got {} batches with {} total rows",
        batches.len(),
        batches.iter().map(|b| b.num_rows()).sum::<usize>()
    );

    // execute_stream() - Returns async Stream of RecordBatch
    // Memory stays constant regardless of data size
    let mut stream = sales.clone().execute_stream().await?;
    let mut row_count = 0;
    while let Some(batch) = stream.next().await {
        let batch = batch?;
        row_count += batch.num_rows();
        // Process incrementally - memory never exceeds one batch
    }
    println!("Processed {} rows via streaming", row_count);

    Ok(())
}
```

#### Streaming for Large Datasets

When data doesn't fit in memory—or you simply don't know how large it is—streaming is essential. Instead of loading everything at once, [`.execute_stream()`] returns an async [`Stream`] of [`RecordBatch`]es. Each batch arrives, gets processed, and can be discarded before the next arrives.

**Key functions for streaming:**

| Function/Trait                           | Purpose                                                          |
| ---------------------------------------- | ---------------------------------------------------------------- |
| [`.execute_stream()`]                    | Returns `SendableRecordBatchStream` - an async stream of batches |
| [`StreamExt::next()`][`StreamExt`]       | From `futures` crate - pulls the next batch from the stream      |
| [`RecordBatch::column()`][`RecordBatch`] | Access a specific column from a batch                            |
| [`.as_primitive::<T>()`]                 | Cast Arrow array to typed access (e.g., `Int64Type`)             |

**Memory model:** <br>
With [`.collect()`], memory usage equals dataset size. With streaming, memory usage equals **one batch** at a time. The batch size is configurable via [`.with_batch_size()`]—default is 8,192 rows. For a 10GB dataset, that's the difference between 10GB and ~1MB.

```rust
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    // Configure smaller batches for lower latency, larger for throughput
    let config = SessionConfig::new().with_batch_size(4096);  // Default: 8192
    let ctx = SessionContext::new_with_config(config);

    // Use the configured context for queries
    let df = ctx.sql("SELECT 1 as x").await?;
    df.show().await?;

    Ok(())
}
```

```rust
use datafusion::prelude::*;
use futures::StreamExt;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let sales = dataframe!(
        "product" => ["Laptop", "Mouse", "Keyboard"],
        "price" => [1200, 25, 75],
        "quantity" => [1, 5, 2]
    )?;

    // Build the query plan (lazy - no data moves yet)
    let projected = sales
        .select(vec![(col("price") * col("quantity")).alias("line_total")])?;

    // Start streaming execution
    let mut stream = projected.execute_stream().await?;

    // Process batches incrementally - memory efficient for large datasets
    let mut batches_processed = 0;
    while let Some(batch_result) = stream.next().await {
        let batch = batch_result?;
        batches_processed += 1;
        println!("Batch {} has {} rows", batches_processed, batch.num_rows());
        // batch is dropped here - memory freed before next batch arrives
    }

    println!("Processed {} batches total", batches_processed);

    Ok(())
}
```

**When to use streaming:**

- Dataset larger than available memory
- Unknown dataset size (user queries, external sources)
- ETL pipelines where you write results incrementally
- When you only need aggregates, not raw data

_Quick Debug_:<br> Running out of memory? Replace [`.collect()`] with [`.execute_stream()`] or add [`.limit()`] before collecting.

> **See also:**
>
> - [`.execute_stream()`] - DataFrame method returning a stream
> - [`RecordBatch`] - Arrow's columnar batch format
> - [`StreamExt`] - Futures crate trait for stream operations
> - [Best Practices](best-practices.md) - Memory configuration and tuning

### Error Handling and Observability

**DataFusion catches plan errors at build time—before any data moves. SQL catches everything at runtime.**

This separation is a key safety advantage of the DataFrame API: invalid column names, type mismatches, and schema errors surface immediately when you call [`.filter()`] or [`.select()`], not minutes later when your query finally executes on production data.

**Build-Time vs Execution-Time Errors**

Understanding when errors occur helps you write robust pipelines:

| Error Type         | When Caught                | DataFrame API                               | SQL                           |
| ------------------ | -------------------------- | ------------------------------------------- | ----------------------------- |
| Unknown column     | **Build time** (sync)      | `Err` at [`.filter()`] call                 | Runtime parse/execution error |
| Type mismatch      | **Build time** (sync)      | `Err` at [`.with_column()`]                 | Runtime execution error       |
| Invalid expression | **Build time** (sync)      | `Err` at method call                        | Runtime parse error           |
| File not found     | **Execution time** (async) | `Err` at [`.collect().await`][`.collect()`] | Runtime (same)                |
| Out of memory      | **Execution time** (async) | `Err` at [`.collect().await`][`.collect()`] | Runtime (same)                |

**The `Result<DataFrame>` Pattern:**

Every DataFrame transformation returns `Result<DataFrame>`, not `DataFrame`. This isn't just Rust ceremony—it's a deliberate design that lets you catch schema errors **immediately** at the call site, before any data processing begins.

The pattern cleanly separates two phases:

1. **Construction phase** (synchronous):
   - Schema validation
   - Column resolution
   - Type checking
2. **Execution phase** (asynchronous):
   - Actual data processing
   - I/O
   - Memory allocation

```rust
use datafusion::prelude::*;

// Construction: synchronous, validates schema immediately
fn build_sales_report(df: DataFrame) -> datafusion::error::Result<DataFrame> {
    df.filter(col("price").gt(lit(0)))?             // ✓ "price" exists → Ok
      .with_column("tax", col("price") * lit(0.1))? // ✓ types compatible → Ok
      .select(vec![col("product"), col("tax")])     // ✓ columns exist → Ok
}

// What if we reference a column that doesn't exist?
fn build_broken_report(df: DataFrame) -> datafusion::error::Result<DataFrame> {
    df.filter(col("revenue").gt(lit(0)))  // ✗ "revenue" doesn't exist → Err immediately!
    // No data was read, no I/O happened—we failed fast
}

// Execution: asynchronous, handles I/O and runtime errors
async fn run_report(df: DataFrame) -> datafusion::error::Result<()> {
    // Phase 1: Build (fast, no I/O)
    let pipeline = build_sales_report(df)?;  // Schema errors caught here

    // Phase 2: Execute (slow, does actual work)
    pipeline.show().await?;  // I/O, memory errors caught here

    Ok(())
}

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let sales = dataframe!(
        "order_id" => [1, 2],
        "product" => ["Laptop", "Mouse"],
        "category" => ["Electronics", "Accessories"],
        "price" => [1200, 25],
        "quantity" => [1, 5],
        "customer" => ["Alice", "Bob"]
    )?;

    // This succeeds
    run_report(sales.clone()).await?;

    // This would fail at build time (uncomment to see):
    // let broken = build_broken_report(sales)?;

    Ok(())
}
```

**Why this matters:** <br>
You can validate your entire pipeline structure before committing to expensive I/O operations. A typo in a column name fails in milliseconds, not after reading 100GB of data.

#### The [`.explain()`] Matrix: Debugging Plans

Use [`df.explain(verbose, analyze)`][`.explain()`] to inspect what DataFusion will do:

| Call                    | Shows                                 | Use When                    |
| ----------------------- | ------------------------------------- | --------------------------- |
| `explain(false, false)` | Logical + Physical plan (basic)       | Quick sanity check          |
| `explain(true, false)`  | Verbose plan with details             | Understanding optimizations |
| `explain(false, true)`  | **EXPLAIN ANALYZE** (execution stats) | Performance debugging       |
| `explain(true, true)`   | Verbose + Analyzed                    | Deep investigation          |

```rust
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let df = dataframe!(
        "product" => ["Laptop", "Mouse"],
        "price" => [1200, 25]
    )?;

    // See what optimizations were applied
    let plan = df.clone().explain(false, false)?;
    plan.show().await?;

    // See actual execution statistics
    let analyzed = df.clone().explain(false, true)?;
    analyzed.show().await?;

    Ok(())
}
```

**What to look for in [EXPLAIN][`.explain()`] output:**

- **Projection pushdown**: Are only needed columns being read?
- **Predicate pushdown**: Is the filter near the data source?
- **Filter merging**: Were multiple [`.filter()`] calls combined?
- **Partition pruning**: Were irrelevant partitions skipped?

#### Graceful Degradation (Schema-Level)

When building reusable transformation functions, you can't always guarantee which columns exist. Graceful degradation handles **schema variations** by checking column availability before transforming, and falling back to sensible defaults.

This pattern is essential when your transformations must work across DataFrames with different schemas—for example, a generic reporting function that handles both complete and partial data:

```rust
use datafusion::prelude::*;

/// Add a computed "total" column if price and quantity exist, otherwise default to 0
fn add_total_with_fallback(df: DataFrame) -> datafusion::error::Result<DataFrame> {
    let schema = df.schema();

    // field_with_unqualified_name() returns Result<&Field, DataFusionError>
    // .is_ok() checks if the column exists without unwrapping
    let has_price = schema.field_with_unqualified_name("price").is_ok();
    let has_quantity = schema.field_with_unqualified_name("quantity").is_ok();

    if has_price && has_quantity {
        // Both columns exist → compute real total
        df.with_column("total", col("price") * col("quantity"))
    } else {
        // Missing columns → use default (don't crash!)
        df.with_column("total", lit(0))
    }
}

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let sales = dataframe!(
        "product" => ["Laptop", "Mouse"],
        "price" => [1200, 25],
        "quantity" => [1, 5]
    )?;

    // With our sales DataFrame: has both columns → real computation
    let result = add_total_with_fallback(sales.clone())?;
    result.show().await?;
    // total = price * quantity (1200, 125)

    // With incomplete data: missing columns → graceful fallback
    let incomplete = dataframe!("product" => ["Widget", "Gadget"])?;
    let fallback_result = add_total_with_fallback(incomplete)?;
    fallback_result.show().await?;
    // total = 0 for all rows (no crash!)

    Ok(())
}
```

#### Soft Failures with [`try_cast()`] (Value-Level)

While graceful degradation handles source-level failures, **soft failures** handle issues at the **individual value level**. When data quality is uncertain—strings that might not be numbers, dates in mixed formats—[`try_cast()`] converts invalid values to `NULL` instead of failing the entire query.

This is crucial when processing messy real-world data where a few bad values shouldn't stop the pipeline:

```rust
use datafusion::prelude::*;
use datafusion::functions::expr_fn::*;
use datafusion::assert_batches_eq;
use arrow::datatypes::DataType;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    // Create a messy dataset for this example using the macro
    let messy_sales = dataframe!(
        "price_text" => ["1200", "$25", "N/A"]
    )?;

    let safe_conversion = messy_sales
        .with_column("price_numeric",
            try_cast(col("price_text"), DataType::Float64)  // "$25" → NULL, "N/A" → NULL
        )?
        .with_column("price_final",
            coalesce(vec![col("price_numeric"), lit(0.0)])  // Replace NULLs with default
        )?;

    let batches = safe_conversion.collect().await?;
    assert_batches_eq!(
        [
            "+------------+---------------+-------------+",
            "| price_text | price_numeric | price_final |",
            "+------------+---------------+-------------+",
            "| 1200       | 1200.0        | 1200.0      |",
            "| $25        |               | 0.0         |",
            "| N/A        |               | 0.0         |",
            "+------------+---------------+-------------+",
        ],
        &batches
    );

    Ok(())
}
```

| Pattern                  | Level  | Handles                              | Example                          |
| ------------------------ | ------ | ------------------------------------ | -------------------------------- |
| **Graceful Degradation** | Schema | Missing columns, schema variations   | Check column exists before using |
| **Soft Failures**        | Value  | Unparseable strings, type mismatches | [`try_cast()`] returns NULL      |

### Data Validation & Quality

**Validation protects your pipeline at multiple levels: schema validation ensures structure, constraint validation ensures values, and quality inspection tracks how transformations affect your data.**

**Why the DataFrame-API excels here:** <br>
Validation is where DataFusion's DataFrame-API truly shines over the SQL-API. With the SQL-API, validation logic lives in query strings—you can't easily parameterize thresholds, compose rules as functions, or integrate with Rust's type system. The DataFrame-API lets you build validation as **reusable Rust functions** with configurable thresholds, return [`Result<DataFrame>`][`Result<DataFrame>`] to fail fast with meaningful errors, and connect validation failures directly to your logging, metrics, and alerting infrastructure. Both APIs produce the same optimized plans—but the DataFrame-API gives you production-grade data quality tooling, not just "check and hope."

This section covers three complementary approaches:

| Approach                  | Focus                                  | When to Use                                     |
| ------------------------- | -------------------------------------- | ----------------------------------------------- |
| **Schema Validation**     | Structure (fields, types, nullability) | First check—ensure DataFrame has expected shape |
| **Constraint Validation** | Business rules (price > 0, not null)   | Every pipeline—reject/flag bad data             |
| **Quality Inspection**    | Distribution tracking, bias detection  | ML pipelines, auditing, compliance              |

> **Schema validation** is covered in detail in [Schema Management § Schema and Data Validation](schema-management.md#schema-and-data-validation). This section focuses on constraint validation and quality inspection.

#### Data Constraint Validation

Once schema validation confirms your DataFrame has the right structure, constraint validation ensures **values** meet business rules: no negative prices, required fields populated, values within expected ranges.

DataFusion's DataFrame-API provides **composable validation primitives**—filter, flag, and aggregate patterns—that integrate directly with your data pipeline. You get Rust's type safety, meaningful error messages via `Result<DataFrame>`, and validation logic that lives alongside your transformations rather than in a separate configuration layer.

> **Coming from other ecosystems?** <br>
> If you've used [Pandera] (Python), [Great Expectations], or [Deequ] (Spark), the patterns here serve a similar purpose: ensuring data meets business rules before processing. The DataFrame-API approach trades declarative schemas for programmatic flexibility—your validation rules are Rust functions you can test, version, and compose.
>
> For declarative validation built on DataFusion, the community is developing [Term](https://github.com/withterm/term)—an emerging project aiming to bring schema-based validation to the Rust/Arrow ecosystem.

**Three constraint validation strategies:**

| Strategy            | Behavior                 | Use When                                     |
| ------------------- | ------------------------ | -------------------------------------------- |
| **Filter-based**    | Removes invalid rows     | Data must be clean for downstream processing |
| **Flag-based**      | Marks rows, keeps all    | Need to report issues but preserve data      |
| **Aggregate-based** | Produces quality summary | Monitoring data health, CI/CD checks         |

##### **Filter-Based Constraints (Reject Invalid Rows)**

Use this when downstream processing requires clean data. Invalid rows are removed before they can cause calculation errors or corrupt aggregations.

**Trade-off:** <br>
Simple and fast, but you lose visibility into what was rejected. Consider logging reject counts.

```rust
use datafusion::prelude::*;

/// Validate and clean sales data, returning only valid rows
async fn validate_sales(sales: DataFrame) -> datafusion::error::Result<DataFrame> {
    // Count rows before validation (for logging)
    let before_count = sales.clone().count().await?;

    // Define validation rules as filters
    // Each .filter() call is ANDed together—row must pass ALL rules
    let validated = sales
        .filter(col("price").gt(lit(0)))?           // Rule 1: price > 0
        .filter(col("quantity").gt(lit(0)))?        // Rule 2: quantity > 0
        .filter(col("customer").is_not_null())?;    // Rule 3: customer required

    // Count rows after validation
    let after_count = validated.clone().count().await?;
    let rejected = before_count - after_count;

    if rejected > 0 {
        eprintln!("Warning: Rejected {} invalid rows out of {}", rejected, before_count);
    }

    Ok(validated)
}

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let sales = dataframe!(
        "order_id" => [1, 99],
        "product" => ["Laptop", "Unknown"],
        "category" => [Some("Electronics"), None],
        "price" => [1200, -50],
        "quantity" => [1, 0],
        "customer" => [Some("Alice"), None]
    )?;

    let validated = validate_sales(sales).await?;
    validated.show().await?;
    // Only valid rows remain

    Ok(())
}
```

##### **Flag-Based Constraints (Mark Issues, Keep All)**

Use this when you need to preserve all data but identify problems. Downstream processes can filter on `is_valid` or handle invalid rows differently.

**Trade-off:** <br>
Keeps all data for analysis, but requires downstream handling of invalid rows.

```rust
use datafusion::prelude::*;

/// Add validation flags and optionally fail if too many invalid rows
async fn validate_with_flags(
    sales: DataFrame,
    max_invalid_percent: f64,  // e.g., 0.1 = fail if >10% invalid
) -> datafusion::error::Result<DataFrame> {
    // Add a boolean column indicating row validity
    // All rules combined with AND—row is valid only if ALL pass
    let with_flags = sales
        .with_column("is_valid",
            col("price").gt(lit(0))
                .and(col("quantity").gt(lit(0)))
                .and(col("customer").is_not_null())
        )?;

    // Check invalid percentage and fail if threshold exceeded
    let total = with_flags.clone().count().await? as f64;
    let invalid = with_flags.clone()
        .filter(col("is_valid").eq(lit(false)))?
        .count().await? as f64;

    let invalid_percent = invalid / total;
    if invalid_percent > max_invalid_percent {
        return Err(datafusion::error::DataFusionError::Execution(
            format!("Validation failed: {:.1}% invalid rows (threshold: {:.1}%)",
                invalid_percent * 100.0, max_invalid_percent * 100.0)
        ));
    }

    Ok(with_flags)
}

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let sales = dataframe!(
        "order_id" => [1, 2, 3],
        "product" => ["Laptop", "Mouse", "Keyboard"],
        "price" => [1200, 25, 75],
        "quantity" => [1, 5, 2],
        "customer" => ["Alice", "Bob", "Alice"]
    )?;

    // Usage: fail if more than 5% of rows are invalid
    let validated = validate_with_flags(sales.clone(), 0.05).await?;

    // Process valid and invalid rows separately
    let valid_rows = validated.clone().filter(col("is_valid").eq(lit(true)))?;
    let invalid_rows = validated.filter(col("is_valid").eq(lit(false)))?;

    valid_rows.show().await?;

    Ok(())
}
```

##### **Aggregate-Based Constraints (Quality Report)**

Use this for monitoring pipelines, CI/CD quality gates, or dashboards. Produces a single-row summary of data health without modifying the data itself.

**Trade-off:** <br>
Great for observability, but doesn't fix or flag individual rows.

```rust
use datafusion::prelude::*;
use datafusion::functions_aggregate::expr_fn::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let sales = dataframe!(
        "order_id" => [1, 2, 3],
        "product" => ["Laptop", "Mouse", "Keyboard"],
        "price" => [1200, 25, 75],
        "quantity" => [1, 5, 2],
        "customer" => ["Alice", "Bob", "Alice"]
    )?;

    // Build a data quality report using aggregations
    let quality_report = sales.aggregate(
        vec![],  // No grouping—single summary row
        vec![
            count(lit(1)).alias("total_rows"),
            sum(case(col("price").is_null())
                .when(lit(true), lit(1))
                .otherwise(lit(0))?
            ).alias("null_prices"),
            sum(case(col("price").lt_eq(lit(0)))
                .when(lit(true), lit(1))
                .otherwise(lit(0))?
            ).alias("invalid_prices"),
            min(col("price")).alias("min_price"),
            max(col("price")).alias("max_price"),
        ]
    )?;

    // Show the quality report
    quality_report.show().await?;
    // +------------+-------------+----------------+-----------+-----------+
    // | total_rows | null_prices | invalid_prices | min_price | max_price |
    // +------------+-------------+----------------+-----------+-----------+
    // | 3          | 0           | 0              | 25        | 1200      |
    // +------------+-------------+----------------+-----------+-----------+

    println!("Data quality check passed!");
    Ok(())
}
```

**Combining strategies:** In production, you often use all three:

1. **Aggregate** first to assess incoming data quality
2. **Flag** rows to preserve audit trail
3. **Filter** before critical calculations

These patterns let you build validation into your pipeline without external dependencies.

> **See also:** <br>
> For dedicated validation frameworks with schema definitions and reporting, the community has developed tools like [Term](https://github.com/withterm/term) that build on DataFusion's query engine.

#### Data Quality & Bias Inspection

For ML pipelines and data-sensitive applications, understanding how transformations affect your data is critical. Did filtering introduce demographic bias? Did a join drop important records? DataFrames enable inspection patterns that answer these questions:

- **Tuple identifiers**: Use [`row_number()`] to assign stable IDs that track individual rows through transformations—essential for debugging "where did this row go?" questions
- **Distribution tracking**: Register pipeline steps as views and compare group counts before/after operations—catches bias introduced by filters or joins
- **Hybrid inspection**: Build pipelines with DataFrames, audit with SQL—leverage each API's strengths

These patterns come from research on [ML pipeline inspection][Blue Elephants Inspecting Pandas], which showed that many ML fairness issues originate in data preparation, not model training.

> **See [Advanced Topics § Data Quality](dataframes-advance.md#data-quality--bias-inspection)** for complete implementations with code examples.

### Summary: Builder Methodology

**The DataFrame API is Rust-first query building—not SQL with different syntax.**

| Pattern                  | Key Benefit                                             |
| ------------------------ | ------------------------------------------------------- |
| **Builder + Laziness**   | Variables hold plans, not data; branch and reuse freely |
| **Dynamic Construction** | Rust `if/else/match` builds safe, validated plans       |
| **Encapsulation**        | Functions returning [`Expr`]/[`DataFrame`] replace UDFs |
| **Memory Control**       | Choose collect vs stream based on data size             |
| **Error Separation**     | Schema errors at build time, I/O errors at execution    |
| **Data Validation**      | Native filter/aggregate patterns for quality checks     |
| **Observability**        | [`.explain()`] shows optimizer decisions                |

> **The takeaway:** Think of DataFrames as _query builders_, not _data containers_. Build the plan with Rust's full power, let DataFusion optimize it, then execute once.

---

---

## Mixing SQL and DataFrames

DataFusion's SQL and DataFrame APIs are two interfaces to the same query engine. Because both compile to identical [`LogicalPlan`] structures, you can mix them freely within a single application—no performance penalty, no translation overhead.

This section covers how to switch between APIs, when mixing makes sense, and how to choose the right data architecture for your workload. For the underlying theory, see the [Concepts][concepts] chapter.

### The Seamless Workflow

Switching between APIs uses two mechanisms. A common pattern is SQL for initial data selection (declarative) and DataFrames for dynamic logic (type-safe, composable).

Moving from **SQL to DataFrames** is direct: [`ctx.sql("SELECT ...")`][`.sql()`] parses the SQL string and returns a [`DataFrame`].

Moving from **DataFrames to SQL** requires registration. To make a programmatic DataFrame accessible to the SQL API, you must explicitly register it in the `SessionContext`. By calling [`df.into_view()`][`.into_view()`] and passing the result to [`ctx.register_table()`][`.register_table()`], you expose the DataFrame as a named view. This allows the SQL parser to reference your Rust-defined logic in `FROM` clauses, effectively bridging the two worlds.

In this example, we create a DataFrame using the [`dataframe!`] macro, register it as a view, join it with a SQL query, and then apply final filtering back in the DataFrame API:

```rust
use datafusion::prelude::*;
use datafusion::error::Result;

#[tokio::main]
async fn main() -> Result<()> {
    let ctx = SessionContext::new();

    // 1. Create a DataFrame programmatically (DataFrame API)
    let prices_df = dataframe!(
        "item" => ["Coffee", "Tea", "Muffin"],
        "price" => [3.50, 2.50, 4.00]
    )?;

    // 2. Register it as a View so SQL can see it
    //    `into_view()` converts the DataFrame's plan into a TableProvider
    ctx.register_table("prices", prices_df.into_view())?;

    // 3. Use SQL to query the View and join with other logic (SQL API)
    //    ctx.sql() returns a DataFrame, continuing the chain
    let df = ctx.sql("
        SELECT item, price, price * 1.1 as with_tax
        FROM prices
        WHERE price < 4.00
    ").await?;

    // 4. Refine the result programmatically (DataFrame API)
    let final_df = df
        .filter(col("with_tax").lt(lit(4.00)))?
        .select_columns(&["item", "with_tax"])?;

    final_df.show().await?;
    Ok(())
}
```

### Best Practices for Mixing

While combining APIs is powerful, consistency within a pipeline improves readability. Treat mixing as an **architectural decision**, not a line-by-line syntax choice.

**When SQL shines:**

- Complex joins with multiple conditions—declarative syntax is often clearer
- Window functions—`OVER (PARTITION BY ... ORDER BY ...)` reads naturally
- CTEs (Common Table Expressions)—layered subqueries are easier to follow
- Ad-hoc exploration—quick iterations without recompilation

**When DataFrames shine:**

- Dynamic filter construction—build predicates from runtime config or user input
- Reusable pipeline components—encapsulate logic in functions that return `DataFrame`
- Compile-time safety—catch typos and schema mismatches before execution
- IDE integration—autocomplete, refactoring, go-to-definition

**Mixing guidelines:**

- **Stay consistent within a stage:** If you start a transformation in DataFrame methods, finish it there before switching.
- **Switch at boundaries:** The natural places to switch are at the **start** (SQL for complex extraction) or **end** (register a view for external tools) of a pipeline.
- **Avoid ping-pong:** A pipeline that alternates every few lines becomes hard to follow.

> **Performance Note:** <br>
> Both APIs compile to identical [`LogicalPlan`] structures—there is no performance difference. Choose based on ergonomics and team familiarity.

### API Ergonomics Comparison

Beyond use cases, the APIs differ in how they integrate with Rust's development workflow:

| Aspect                 | SQL API                                                                    | DataFrame API                                                                                 |
| :--------------------- | :------------------------------------------------------------------------- | :-------------------------------------------------------------------------------------------- |
| **Error Detection**    | **Runtime:** Schema errors surface when the plan is built.                 | **Compile-time:** Invalid method calls fail compilation.                                      |
| **Variable Injection** | **Parameter Binding:** Use `$1` or `$name` syntax to safely inject values. | **Native Rust:** Pass variables directly: `.filter(col("age").gt(lit(min_age)))`.             |
| **Logic Construction** | **Declarative:** Express logic in a single statement (or use CTEs).        | **Imperative:** Use `for` loops and `if` statements to build queries from runtime conditions. |
| **Tooling**            | Syntax highlighting, but limited IDE support for schema validation.        | Full IDE support: autocomplete, refactoring, go-to-definition.                                |

### Choosing the Right Architecture

While SQL vs. DataFrame is purely ergonomic, the choice of **data architecture** impacts performance. DataFusion excels at analytical workloads—but it is not a universal solution. Understanding when to use DataFusion, when to delegate to other systems, and when to combine them is essential for production architectures.

#### DataFusion's Sweet Spot: OLAP Workloads

DataFusion is a **columnar (OLAP) query engine** optimized for:

- **Aggregations over large datasets:** SUM, AVG, COUNT over millions of rows
- **Complex analytical queries:** Multi-table joins, window functions, CTEs
- **Columnar file formats:** Parquet, Arrow IPC, CSV/JSON scanning
- **Data lake/lakehouse patterns:** Query files directly without loading into a database

For a detailed breakdown of columnar vs. row-based trade-offs, see the [When Row-Based TableProviders Outperform Columnar](#when-row-based-tableproviders-outperform-columnar) section earlier in this document.

#### When OLTP Systems Excel

DataFusion is _not_ optimized for **transactional (OLTP)** workloads. Traditional relational databases like PostgreSQL remain the right choice for:

| Workload                     | Why OLTP Wins                                                |
| :--------------------------- | :----------------------------------------------------------- |
| **Point lookups**            | B-tree indexes provide O(log n) access; no full scan needed  |
| **High-concurrency writes**  | ACID transactions, row-level locking, WAL durability         |
| **Frequent updates/deletes** | Row-based storage allows in-place modification               |
| **Referential integrity**    | Foreign keys, constraints, triggers enforce data consistency |

DataFusion's [`TableProvider`] interface bridges these worlds: register PostgreSQL or MySQL as a table, and DataFusion pushes filters to the database while handling complex analytics locally. See [The Federation Pattern](#the-federation-pattern) below for the architectural overview.

#### When NoSQL Databases Shine

Beyond relational OLTP, specialized NoSQL systems solve problems that neither DataFusion nor traditional databases address well:

| Category               | Example Systems           | Sweet Spot                                                                      |
| :--------------------- | :------------------------ | :------------------------------------------------------------------------------ |
| **Document Stores**    | MongoDB, CouchDB          | Flexible schemas, nested JSON structures, rapid iteration on data models        |
| **Search Engines**     | OpenSearch, Elasticsearch | Full-text search, faceted navigation, relevance ranking, log analytics          |
| **Key-Value Stores**   | Redis, DynamoDB           | Sub-millisecond lookups, session storage, caching, high-throughput simple reads |
| **Wide-Column Stores** | Cassandra, ScyllaDB       | Time-series data, write-heavy workloads, horizontal scaling across regions      |

These systems are **not competitors to DataFusion**—they solve different problems. In practice, many architectures combine them:

- **Operational layer:** NoSQL or OLTP for real-time application data
- **Analytical layer:** DataFusion queries exported snapshots, change-data-capture streams, or federated views

> **Note:** <br>
> For relationship-heavy data (social graphs, recommendation engines, fraud detection), consider specialized **graph databases** like [Neo4j] or [Amazon Neptune]. These excel at traversing connections—a workload where both relational joins and columnar scans struggle.

#### The Federation Pattern

DataFusion's [`TableProvider`] trait enables a **federation architecture**: connect diverse data sources and let each system do what it does best.

```text
┌─────────────────────────────────────────────────────────────┐
│                      DataFusion                             │
│              (Analytical Query Engine)                      │
├─────────────┬─────────────┬─────────────┬───────────────────┤
│  Parquet    │  PostgreSQL │   Redis     │   OpenSearch      │
│  (native)   │ (TableProv) │ (TableProv) │   (TableProv)     │
└─────────────┴─────────────┴─────────────┴───────────────────┘
```

In this architecture:

- **Push down what you can:** Filters and projections reach source systems that can execute them efficiently
- **Federate what you must:** Complex joins across sources happen in DataFusion's columnar engine
- **Choose the right home:** Store data where it will be queried most—don't force analytical patterns onto OLTP systems, or vice versa

### Mixing SQL and DataFrames References

For deeper exploration of the topics covered in this section:

**DataFusion Architecture:**

- [DataFusion Architecture Guide](https://datafusion.apache.org/contributor-guide/architecture.html) — How SQL and DataFrame APIs converge to the same `LogicalPlan`
- [Building Logical Plans](https://datafusion.apache.org/library-user-guide/building-logical-plans.html) — The builder pattern underlying the DataFrame API
- [Apache DataFusion: A Fast, Embeddable, Modular Analytic Query Engine][datafusion paper] — The SIGMOD 2024 paper explaining DataFusion's design

**Federation & TableProviders:**

- [datafusion-table-providers](https://github.com/datafusion-contrib/datafusion-table-providers) — Community implementations for PostgreSQL, MySQL, SQLite, and more
- [Querying Postgres from DataFusion](https://datafusion.apache.org/library-user-guide/custom-table-providers.html) — Tutorial on building custom `TableProvider` implementations
- [InfluxDB 3.0 FDAP Architecture](https://www.influxdata.com/glossary/fdap-stack/) — Real-world federation: DataFusion as the query layer for a time-series database

**DataFrame Paradigm Research:**

- [Towards Scalable Dataframe Systems][dataframe algebra] — Academic analysis of DataFrame semantics and optimization opportunities
- [Apache Spark SQL Paper](https://dl.acm.org/doi/10.1145/2723372.2742797) — The foundational work on unifying SQL and DataFrame APIs

**Lakehouse & Specialized Formats:**

- [delta-rs](https://github.com/delta-io/delta-rs) — Delta Lake TableProvider for ACID transactions on data lakes
- [lance-datafusion](https://crates.io/crates/lance-datafusion) — Lance format integration for ML/vector workloads
- [datafusion-iceberg](https://github.com/apache/iceberg-rust) — Apache Iceberg support (in development)

**Understanding Data System Trade-offs:**

- [Designing Data-Intensive Applications](https://dataintensive.net/) — Martin Kleppmann's comprehensive guide to database internals and distributed systems trade-offs (covers SQL vs NoSQL, OLTP vs OLAP, consistency models)
- [MongoDB vs PostgreSQL](https://www.mongodb.com/resources/compare/mongodb-postgresql) — When document stores make sense
- [The Log: What every software engineer should know](https://engineering.linkedin.com/distributed-systems/log-what-every-software-engineer-should-know-about-real-time-datas-unifying) — Jay Kreps on data architecture patterns

---

## Further Reading

This section provides resources for deeper exploration of DataFrame concepts, data transformation patterns, and the broader data engineering ecosystem—organized from DataFusion-specific to general industry knowledge.

### DataFusion: Official Documentation

Core documentation for the DataFrame API and related features:

- [DataFrame API Overview](index.md) — Entry point to this documentation series
- [Creating DataFrames](creating-dataframes.md) — Data ingestion patterns
- [DataFrame Concepts](concepts.md) — `SessionContext`, `LogicalPlan`, lazy evaluation
- [Writing DataFrames](writing-dataframes.md) — Output formats and sinks
- [Best Practices](best-practices.md) — Performance optimization patterns
- [Advanced Topics](dataframes-advance.md) — Custom UDFs, `TableProvider`, ecosystem integrations
- [SQL User Guide](../../user-guide/sql/index.rst) — SQL dialect reference
- [Expression Functions](../../user-guide/sql/scalar_functions.md) — All available scalar, aggregate, and window functions

### DataFusion: Examples & Source

Learn by example—these are tested, working code:

- [dataframe.rs](https://github.com/apache/datafusion/blob/main/datafusion-examples/examples/dataframe.rs) — Basic DataFrame operations
- [dataframe_transformations.rs](https://github.com/apache/datafusion/blob/main/datafusion-examples/examples/dataframe_transformations.rs) — Examples from this guide
- [expr_api.rs][`expr_api`] — Complex expression building patterns
- [custom_datasource.rs](../../library-user-guide/custom-table-providers.md) — Building a `TableProvider`
- [All Examples](../../../../datafusion-examples) — Complete example collection

### DataFusion Ecosystem

Community projects that extend DataFusion's capabilities:

| Project                                                                                        | Purpose                                              |
| :--------------------------------------------------------------------------------------------- | :--------------------------------------------------- |
| [datafusion-table-providers](https://github.com/datafusion-contrib/datafusion-table-providers) | TableProviders for PostgreSQL, MySQL, SQLite, DuckDB |
| [delta-rs](https://github.com/delta-io/delta-rs)                                               | Delta Lake format with ACID transactions             |
| [lance-datafusion](https://github.com/lance-format/lance)                                      | Lance format for ML/vector workloads                 |
| [datafusion-python](https://github.com/apache/datafusion-python)                               | Python bindings for DataFusion                       |
| [datafusion-ballista](https://github.com/apache/datafusion-ballista)                           | Distributed query execution on DataFusion            |
| [datafusion-comet](https://github.com/apache/datafusion-comet)                                 | Apache Spark plugin using DataFusion                 |

### Industry Standard References

DataFusion's DataFrame API draws inspiration from established systems. These references provide complementary perspectives:

**Apache Spark (Distributed DataFrames):**

- [Spark SQL, DataFrames and Datasets Guide](https://spark.apache.org/docs/latest/sql-programming-guide.html) — The canonical DataFrame API reference
- [PySpark DataFrame API](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/dataframe.html) — Python API that influenced many DataFrame implementations
- [Spark SQL Paper (SIGMOD 2015)](https://dl.acm.org/doi/10.1145/2723372.2742797) — Foundational work on unifying SQL and DataFrames

**PostgreSQL (SQL Semantics):**

- [PostgreSQL SELECT Documentation](https://www.postgresql.org/docs/current/sql-select.html) — DataFusion follows PostgreSQL SQL semantics
- [PostgreSQL Window Functions](https://www.postgresql.org/docs/current/tutorial-window.html) — Window function concepts and syntax
- [PostgreSQL DISTINCT ON](https://www.postgresql.org/docs/current/sql-select.html#SQL-DISTINCT) — The `DISTINCT ON` extension DataFusion supports

**pandas (Data Science Origins):**

- [pandas User Guide](https://pandas.pydata.org/docs/user_guide/index.html) — Where the DataFrame concept was popularized for data science
- [pandas Comparison with SQL](https://pandas.pydata.org/docs/getting_started/comparison/comparison_with_sql.html) — Mental model mapping between SQL and DataFrame operations

**Apache Arrow (Columnar Foundation):**

- [Apache Arrow Specification](https://arrow.apache.org/docs/format/Columnar.html) — The in-memory columnar format underlying DataFusion
- [Arrow Rust Implementation](https://docs.rs/arrow/latest/arrow/) — The Rust Arrow crate DataFusion builds on

### Research Papers & Academic Resources

For understanding the theory behind DataFrame systems and query optimization:

- [Apache DataFusion: A Fast, Embeddable, Modular Analytic Query Engine][datafusion paper] — SIGMOD 2024 paper on DataFusion's architecture
- [Towards Scalable Dataframe Systems][dataframe algebra] — Academic analysis of DataFrame algebra and optimization
- [The Cascades Framework for Query Optimization](https://15721.courses.cs.cmu.edu/spring2016/papers/graefe-ieee1995.pdf) — The optimization framework DataFusion's planner uses
- [Volcano: An Extensible and Parallel Query Evaluation System](https://cs-people.bu.edu/mathan/reading-groups/papers-classics/volcano.pdf) — Iterator model for query execution

### Textbooks & Comprehensive Guides

For building deeper expertise in data systems:

- [Designing Data-Intensive Applications](https://dataintensive.net/) — Martin Kleppmann's comprehensive guide to database internals, distributed systems, and data architecture trade-offs
- [Database Internals](https://www.databass.dev/) — Alex Petrov's deep dive into storage engines and distributed database concepts
- [The Data Warehouse Toolkit](https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/books/) — Kimball's guide to dimensional modeling (relevant for analytical query patterns)

### Concepts Quick Reference

Key concepts referenced throughout this documentation:

| Concept                 | What It Means                                                                     |
| :---------------------- | :-------------------------------------------------------------------------------- |
| **Lazy Evaluation**     | Transformations build a plan; execution happens only on actions like `.collect()` |
| **Projection Pushdown** | Reading only the columns needed, pushed down to the data source                   |
| **Predicate Pushdown**  | Applying filters at the data source to reduce data transfer                       |
| **Columnar Processing** | Operating on columns (vectors) rather than rows for CPU efficiency                |
| **Three-Valued Logic**  | SQL's NULL handling: TRUE, FALSE, or UNKNOWN                                      |
| **Builder Pattern**     | Chaining methods to construct complex objects incrementally                       |

---

<!-- ═══════════════════════════════════════════════════════════════════════════
     REFERENCE DEFINITIONS - Single Source of Truth

     Categories:
     1. DataFusion Core Types (structs, enums, traits)
     2. DataFrame Methods (instance methods with `.`)
     3. Expr Methods (instance methods on Expr)
     4. Standalone Functions (aggregate, scalar, window)
     5. SQL Keywords & Internal Documentation
     6. Arrow & Rust Standard Library
     7. External Resources (papers, tutorials, blogs)
     ═══════════════════════════════════════════════════════════════════════════ -->

<!-- 1. DataFusion Core Types ================================================ -->

[`DataFrame`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html
[`dataframe!`]: https://docs.rs/datafusion/latest/datafusion/macro.dataframe.html
[`datafusion.optimizer`]: https://docs.rs/datafusion/latest/datafusion/optimizer/index.html
[`Expr`]: https://docs.rs/datafusion/latest/datafusion/logical_expr/enum.Expr.html
[`Full`]: https://docs.rs/datafusion/latest/datafusion/logical_expr/enum.JoinType.html#variant.Full "All rows from both tables (NULL where no match)"
[`Inner`]: https://docs.rs/datafusion/latest/datafusion/logical_expr/enum.JoinType.html#variant.Inner "Only rows with matches in both tables"
[`JoinType`]: https://docs.rs/datafusion/latest/datafusion/logical_expr/enum.JoinType.html
[`Left`]: https://docs.rs/datafusion/latest/datafusion/logical_expr/enum.JoinType.html#variant.Left "All left rows + matching right rows (NULL if no match)"
[`LeftAnti`]: https://docs.rs/datafusion/latest/datafusion/logical_expr/enum.JoinType.html#variant.LeftAnti "Left rows that have NO match (no right columns)"
[`LeftMark`]: https://docs.rs/datafusion/latest/datafusion/logical_expr/enum.JoinType.html#variant.LeftMark "Mark join for EXISTS subquery decorrelation"
[`LeftSemi`]: https://docs.rs/datafusion/latest/datafusion/logical_expr/enum.JoinType.html#variant.LeftSemi "Left rows that have a match (no right columns)"
[`LogicalPlan`]: https://docs.rs/datafusion-expr/latest/datafusion_expr/logical_plan/enum.LogicalPlan.html
[`prelude`]: https://docs.rs/datafusion/latest/datafusion/prelude/index.html
[`Result<DataFrame>`]: https://docs.rs/datafusion/latest/datafusion/error/type.Result.html
[`Right`]: https://docs.rs/datafusion/latest/datafusion/logical_expr/enum.JoinType.html#variant.Right "All right rows + matching left rows (NULL if no match)"
[`RightAnti`]: https://docs.rs/datafusion/latest/datafusion/logical_expr/enum.JoinType.html#variant.RightAnti
[`RightMark`]: https://docs.rs/datafusion/latest/datafusion/logical_expr/enum.JoinType.html#variant.RightMark "Mark join for EXISTS subquery decorrelation"
[`RightSemi`]: https://docs.rs/datafusion/latest/datafusion/logical_expr/enum.JoinType.html#variant.RightSemi
[`SessionContext`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html
[`SessionContext::sql()`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.sql
[`sqlparser`]: https://docs.rs/datafusion/latest/datafusion/logical_expr/sqlparser/dialect/index.html "DataFusion's SQL parser supports multiple dialects"
[`TableProvider`]: https://docs.rs/datafusion/latest/datafusion/catalog/trait.TableProvider.html

<!-- 2. DataFrame Methods ==================================================== -->

[`.aggregate()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.aggregate
[`.alias()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.alias
[`.cache()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.cache
[`.collect()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.collect
[`.collect_partitioned()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.collect_partitioned
[`.count()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.count
[`.describe()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.describe
[`.distinct()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.distinct
[`.distinct_on()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.distinct_on
[`.drop_columns()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.drop_columns
[`.except()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.except
[`.execute_stream()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.execute_stream
[`.execute_stream_partitioned()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.execute_stream_partitioned
[`.explain()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.explain
[`.fill_null()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.fill_null
[`.filter()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.filter
[`.from_columns()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.from_columns
[`.intersect()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.intersect
[`.into_unoptimized_plan()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.into_unoptimized_plan
[`.into_view()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.into_view
[`.join()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.join
[`.join_on()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.join_on
[`.limit()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.limit
[`.parse_sql_expr()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.parse_sql_expr
[`.register_table()`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.register_table
[`.schema()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.schema
[`.select()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.select
[`.select_columns()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.select_columns
[`.select_exprs()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.select_exprs
[`.show()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.show
[`.show_limit()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.show_limit
[`.sort()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.sort
[`.sort_by()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.sort_by
[`.sql()`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.sql
[`.union()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.union
[`.union_by_name()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.union_by_name
[`.union_by_name_distinct()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.union_by_name_distinct
[`.union_distinct()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.union_distinct
[`.unnest_columns()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.unnest_columns
[`.unnest_columns_with_options()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.unnest_columns_with_options
[`.window()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.window
[`.with_batch_size()`]: https://docs.rs/datafusion/latest/datafusion/prelude/struct.SessionConfig.html#method.with_batch_size
[`.with_column()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.with_column
[`.with_column_renamed()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.with_column_renamed
[`.with_param_values()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.with_param_values
[join_filter_param]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.join "See the 'filter' parameter in the join() signature"

<!-- 3. Expr Methods ========================================================= -->

[`.and()`]: https://docs.rs/datafusion/latest/datafusion/prelude/enum.Expr.html#method.and
[`.between()`]: https://docs.rs/datafusion/latest/datafusion/prelude/enum.Expr.html#method.between
[`.eq()`]: https://docs.rs/datafusion/latest/datafusion/prelude/enum.Expr.html#method.eq
[`.gt()`]: https://docs.rs/datafusion/latest/datafusion/prelude/enum.Expr.html#method.gt
[`.ilike()`]: https://docs.rs/datafusion/latest/datafusion/prelude/enum.Expr.html#method.ilike
[`.is_not_null()`]: https://docs.rs/datafusion/latest/datafusion/prelude/enum.Expr.html#method.is_not_null
[`.is_null()`]: https://docs.rs/datafusion/latest/datafusion/prelude/enum.Expr.html#method.is_null
[`.like()`]: https://docs.rs/datafusion/latest/datafusion/prelude/enum.Expr.html#method.like
[`.lt()`]: https://docs.rs/datafusion/latest/datafusion/prelude/enum.Expr.html#method.lt
[`.not()`]: https://docs.rs/datafusion/latest/datafusion/prelude/enum.Expr.html#method.not
[`.or()`]: https://docs.rs/datafusion/latest/datafusion/prelude/enum.Expr.html#method.or
[`.otherwise()`]: https://docs.rs/datafusion/latest/datafusion/logical_expr/struct.CaseBuilder.html#method.otherwise

<!-- 4. Standalone Functions (Aggregate, Scalar, Window) ===================== -->

[`approx_distinct()`]: https://docs.rs/datafusion/latest/datafusion/functions_aggregate/expr_fn/fn.approx_distinct.html
[`approx_median()`]: https://docs.rs/datafusion/latest/datafusion/functions_aggregate/expr_fn/fn.approx_median.html
[`array_agg()`]: https://docs.rs/datafusion/latest/datafusion/functions_aggregate/expr_fn/fn.array_agg.html
[`avg()`]: https://docs.rs/datafusion/latest/datafusion/functions_aggregate/expr_fn/fn.avg.html
[`coalesce()`]: https://docs.rs/datafusion/latest/datafusion/functions/core/expr_fn/fn.coalesce.html
[`col()`]: https://docs.rs/datafusion/latest/datafusion/prelude/fn.col.html
[`count()`]: https://docs.rs/datafusion/latest/datafusion/functions_aggregate/expr_fn/fn.count.html
[`count_distinct()`]: https://docs.rs/datafusion/latest/datafusion/functions_aggregate/expr_fn/fn.count_distinct.html
[`date_part()`]: https://docs.rs/datafusion/latest/datafusion/functions/datetime/expr_fn/fn.date_part.html
[`date_trunc()`]: https://docs.rs/datafusion/latest/datafusion/functions/datetime/expr_fn/fn.date_trunc.html
[`exists()`]: https://docs.rs/datafusion/latest/datafusion/prelude/fn.exists.html
[`in_list()`]: https://docs.rs/datafusion/latest/datafusion/prelude/fn.in_list.html
[`in_subquery()`]: https://docs.rs/datafusion/latest/datafusion/prelude/fn.in_subquery.html
[`lit()`]: https://docs.rs/datafusion/latest/datafusion/logical_expr/fn.lit.html
[`lower()`]: https://docs.rs/datafusion/latest/datafusion/functions/expr_fn/fn.lower.html
[`max()`]: https://docs.rs/datafusion/latest/datafusion/functions_aggregate/expr_fn/fn.max.html
[`median()`]: https://docs.rs/datafusion/latest/datafusion/functions_aggregate/expr_fn/fn.median.html
[`min()`]: https://docs.rs/datafusion/latest/datafusion/functions_aggregate/expr_fn/fn.min.html
[`nvl()`]: https://docs.rs/datafusion/latest/datafusion/functions/core/expr_fn/fn.nvl.html
[`replace()`]: https://docs.rs/datafusion/latest/datafusion/functions/expr_fn/fn.replace.html
[`row_number()`]: https://docs.rs/datafusion/latest/datafusion/functions_window/row_number/fn.row_number.html
[`scalar_subquery()`]: https://docs.rs/datafusion/latest/datafusion/prelude/fn.scalar_subquery.html
[`stddev()`]: https://docs.rs/datafusion/latest/datafusion/functions_aggregate/expr_fn/fn.stddev.html
[`string_agg()`]: https://docs.rs/datafusion/latest/datafusion/functions_aggregate/expr_fn/fn.string_agg.html
[`substring()`]: https://docs.rs/datafusion/latest/datafusion/functions/expr_fn/fn.substring.html
[`sum()`]: https://docs.rs/datafusion/latest/datafusion/functions_aggregate/expr_fn/fn.sum.html
[`to_date()`]: https://docs.rs/datafusion/latest/datafusion/functions/datetime/expr_fn/fn.to_date.html
[`trim()`]: https://docs.rs/datafusion/latest/datafusion/functions/expr_fn/fn.trim.html
[`try_cast()`]: https://docs.rs/datafusion/latest/datafusion/logical_expr/fn.try_cast.html
[`upper()`]: https://docs.rs/datafusion/latest/datafusion/functions/expr_fn/fn.upper.html
[`var_pop()`]: https://docs.rs/datafusion/latest/datafusion/functions_aggregate/expr_fn/fn.var_pop.html
[`var_sample()`]: https://docs.rs/datafusion/latest/datafusion/functions_aggregate/expr_fn/fn.var_sample.html
[`when()`]: https://docs.rs/datafusion/latest/datafusion/logical_expr/fn.when.html
[expr_fn]: https://docs.rs/datafusion/latest/datafusion/functions_aggregate/expr_fn/index.html
[`scalar_subquery()`]: https://docs.rs/datafusion/latest/datafusion/logical_expr/fn.scalar_subquery.html

<!-- 5. SQL Keywords & Internal Documentation ================================ -->

[`AND`]: ../../user-guide/sql/operators.md#logical-operators
[`AS`]: https://docs.rs/datafusion/latest/datafusion/logical_expr/sqlparser/ast/struct.ExprWithAlias.html
[`CASE`]: https://docs.rs/datafusion/latest/datafusion/prelude/fn.when.html
[`COALESCE`]: ../../user-guide/sql/scalar_functions.md#coalesce
[`CROSS JOIN`]: ../../user-guide/sql/select.md#cross-join
[`DATE_PART`]: ../../user-guide/sql/scalar_functions.md#date_part
[`DISTINCT ON`]: https://github.com/apache/datafusion/issues/7827
[`DISTINCT`]: ../../user-guide/sql/select.md#select-clause
[`EXCEPT`]: ../../user-guide/sql/select.md#except
[`EXTRACT`]: ../../user-guide/sql/scalar_functions.md#date_part
[`FULL OUTER JOIN`]: ../../user-guide/sql/select.md#full-outer-join
[`GROUP BY`]: ../../user-guide/sql/select.md#group-by-clause
[`HAVING`]: ../../user-guide/sql/select.md#having-clause
[`INNER JOIN`]: ../../user-guide/sql/select.md#inner-join
[`INTERSECT`]: ../../user-guide/sql/select.md#intersect
[`JOIN`]: ../../user-guide/sql/select.md#join-clause
[`LEFT ANTI JOIN`]: ../../user-guide/sql/select.md#left-anti-join
[`LEFT SEMI JOIN`]: ../../user-guide/sql/select.md#left-semi-join
[`LEFT`]: ../../user-guide/sql/select.md#left-join
[`LIMIT`]: ../../user-guide/sql/select.md#limit-clause
[`NATURAL JOIN`]: ../../user-guide/sql/select.md#natural-join
[`OFFSET`]: https://docs.rs/datafusion/latest/datafusion/logical_expr/sqlparser/dialect/keywords/constant.OFFSET.html
[`OR`]: ../../user-guide/sql/operators.md#logical-operators
[`ORDER BY`]: ../../user-guide/sql/select.md#order-by-clause
[`SELECT`]: ../../user-guide/sql/select.md
[`TO_DATE`]: ../../user-guide/sql/scalar_functions.md#to_date
[`UNION`]: ../../user-guide/sql/select.md#union-clause
[`WHERE`]: ../../user-guide/sql/select.md#where
[concepts]: ../concepts.md#mixing-sql-and-dataframes
[`expr_api`]: ../../library-user-guide/working-with-exprs.md
[explain usage]: ../../user-guide/explain-usage.md
[predicate pushdown]: https://docs.rs/datafusion/latest/datafusion/physical_plan/filter_pushdown/struct.PushedDownPredicate.html

<!-- 6. Arrow & Rust Standard Library ======================================== -->

[`.as_primitive::<T>()`]: https://docs.rs/arrow/latest/arrow/array/trait.AsArray.html#method.as_primitive
[`.clone()`]: https://doc.rust-lang.org/std/clone/trait.Clone.html
[`.is_numeric()`]: https://docs.rs/arrow/latest/arrow/datatypes/enum.DataType.html#method.is_numeric
[`DataType`]: https://docs.rs/arrow/latest/arrow/datatypes/enum.DataType.html
[`RecordBatch`]: https://docs.rs/arrow/latest/arrow/record_batch/struct.RecordBatch.html
[`reduce()`]: https://doc.rust-lang.org/core/option/enum.Option.html#method.reduce
[`Stream`]: https://docs.rs/futures/latest/futures/stream/trait.Stream.html
[`StreamExt`]: https://docs.rs/futures/latest/futures/stream/trait.StreamExt.html
[`take()`]: https://docs.rs/arrow/latest/arrow/compute/kernels/take/fn.take.html "Arrow kernel: select elements by index"
[`unwrap_or_else`]: https://doc.rust-lang.org/std/option/enum.Option.html#method.unwrap_or_else
[`unwrap()`]: https://doc.rust-lang.org/core/option/enum.Option.html#method.unwrap
[Arrow]: https://arrow.apache.org/ "Apache Arrow: columnar in-memory format"

<!-- 7. External Resources =================================================== -->

<!-- DataFusion Physical Plan Executors -->

[**Cross Join**]: https://docs.rs/datafusion/latest/datafusion/physical_plan/joins/struct.CrossJoinExec.html "Cartesian product of two tables"
[**Hash Join**]: https://docs.rs/datafusion/latest/datafusion/physical_plan/joins/struct.HashJoinExec.html "Equi-join using hash table on build side"
[**Nested Loop Join**]: https://docs.rs/datafusion/latest/datafusion/physical_plan/joins/struct.NestedLoopJoinExec.html "General non-equi join conditions"
[**Piecewise Merge Join**]: https://docs.rs/datafusion/latest/datafusion/physical_plan/joins/struct.PiecewiseMergeJoinExec.html "Optimized for single range conditions"
[**Sort-Merge Join**]: https://docs.rs/datafusion/latest/datafusion/physical_plan/joins/struct.SortMergeJoinExec.html "Join pre-sorted inputs with optional spilling"
[**Symmetric Hash Join**]: https://docs.rs/datafusion/latest/datafusion/physical_plan/joins/struct.SymmetricHashJoinExec.html "Streaming join for unbounded data"
[several join algorithms]: https://docs.rs/datafusion/latest/datafusion/physical_plan/joins/index.html "DataFusion join implementations"

<!-- Papers & Academic Resources -->

[Apache Spark]: https://people.csail.mit.edu/matei/papers/2015/sigmod_spark_sql.pdf
[dataframe algebra]: https://arxiv.org/pdf/2001.00888
[datafusion paper]: https://andrew.nerdnetworks.org/pdf/SIGMOD-2024-lamb.pdf

<!-- External Documentation -->

[Apache Spark DataFrames]: https://spark.apache.org/docs/latest/sql-programming-guide.html#datasets-and-dataframes
[docs.rs]: https://docs.rs/datafusion/latest/datafusion/#architecture
[pandas.DataFrame.explode]: https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.explode.html
[pandas.melt]: https://pandas.pydata.org/docs/reference/api/pandas.melt.html
[pandas]: https://pandas.pydata.org/docs/user_guide/dsintro.html#dataframe
[Polars Join Operations]: https://docs.pola.rs/user-guide/transformations/joins/ "Polars DataFrame join operations"
[postgres docs]: https://www.postgresql.org/docs/
[PostgreSQL JOIN docs]: https://www.postgresql.org/docs/current/queries-table-expressions.html#QUERIES-JOIN "Authoritative reference for join semantics"
[PostgreSQL semantics]: https://www.postgresql.org/docs/current/queries-table-expressions.html#QUERIES-JOIN "DataFusion follows PostgreSQL SQL semantics"
[PostgreSQL]: https://www.postgresql.org/docs/current/queries-table-expressions.html#QUERIES-JOIN "PostgreSQL: The de facto standard for DataFusion SQL behavior"
[PySpark explode]: https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.explode.html
[pyspark-rank-function-with-examples]: https://sparkbyexamples.com/pyspark/pyspark-rank-function-with-examples/
[spark docs]: https://spark.apache.org/docs/latest/
[Spark Join Guide]: https://spark.apache.org/docs/latest/sql-ref-syntax-qry-select-join.html "Apache Spark SQL join syntax and examples"
[Neo4j]: https://neo4j.com/
[Amazon Neptune]: https://aws.amazon.com/de/neptune/

<!-- Tutorials & Blog Posts -->

[anti_semi_joins]: https://blog.jooq.org/semi-join-and-anti-join-should-have-its-own-syntax-in-sql/
[builder_pattern]: https://refactoring.guru/design-patterns/builder
[catalyst_optimizer]: https://www.databricks.com/blog/2015/04/13/deep-dive-into-spark-sqls-catalyst-optimizer.html "Deep Dive into Spark SQL's Catalyst Optimizer"
[CMU Join Algorithms]: https://www.youtube.com/watch?v=YIdIaPopfpk&list=PLSE8ODhjZXjYMAgsGH-GtY5rJYZ6zjsd5&index=12 "CMU 15-445 Lecture 11: Join Algorithms (Andy Pavlo)"
[DataFusion Join Optimization]: https://xebia.com/blog/making-joins-faster-in-datafusion-based-on-table-statistics/ "Making Joins Faster in DataFusion Based on Table Statistics"
[fluent_interface]: https://martinfowler.com/bliki/FluentInterface.html "Martin Fowler's Fluent Interface"
[Join optimization strategies]: https://use-the-index-luke.com/sql/join "How databases optimize joins and what you can control"
[Join tutorial]: https://blog.jooq.org/say-no-to-venn-diagrams-when-explaining-joins/ "Why Venn diagrams mislead when explaining joins"
[lexicographic order]: https://datafusion.apache.org/blog/2025/03/11/ordering-analysis/#appendix "Lexicographic order: comparing sequences element by element, left to right"
[NULL handling in joins]: https://modern-sql.com/concept/null "Why NULL comparisons return UNKNOWN, not TRUE/FALSE"
[Optimizing SQL & DataFrames Pt 1]: https://www.influxdata.com/blog/optimizing-sql-dataframes-part-one/ "Optimizing SQL (and DataFrames) in DataFusion: Part 1"
[Optimizing SQL & DataFrames Pt 2]: https://www.influxdata.com/blog/optimizing-sql-dataframes-part-two/ "Optimizing SQL (and DataFrames) in DataFusion: Part 2"
[ordering analysis]: https://datafusion.apache.org/blog/2025/03/11/ordering-analysis/ "DataFusion blog: Using Ordering for Better Plans"
[Semi and Anti joins explained]: https://blog.jooq.org/semi-join-and-anti-join-should-have-its-own-syntax-in-sql/ "Why Semi/Anti joins deserve first-class syntax"
[SLAP]: https://www.jameshw.dev/blog/2022-02-05/principles-from-clean-code#d69d60c9b3054d47816551afafcfa847 "Functions should SLAP! — from Clean Code principles"
[SQL clause order]: https://www.postgresql.org/docs/current/sql-select.html#SQL-HAVING
[Top-K]: https://xebia.com/blog/optimizing-topk-queries-in-datafusion/ "TopK optimization: 15x faster ORDER BY ... LIMIT queries"
[Understanding Offset and Cursor Pagination]: https://medium.com/better-programming/understanding-the-offset-and-cursor-pagination-8ddc54d10d98 "In-depth comparison of pagination strategies"
[Understanding SQL Dialects (medium-article)]: https://medium.com/@abhapratiti27/understanding-sql-dialects-a-deeper-dive-into-the-linguistic-variations-of-sql-e7e2fdb7509b
[Visual JOIN guide]: https://joins.spathon.com/ "Interactive visual guide to SQL joins"

<!-- Concepts & Wikipedia -->

[databricks_star_schema]: https://www.databricks.com/glossary/star-schema
[external sort]: https://en.wikipedia.org/wiki/External_sorting "External sorting algorithm for data larger than memory"
[Hash Join (Wikipedia)]: https://en.wikipedia.org/wiki/Hash_join "Hash join algorithm explanation"
[HyperLogLog]: https://en.wikipedia.org/wiki/HyperLogLog
[Sort-Merge Join]: https://en.wikipedia.org/wiki/Sort-merge_join "Sort-merge join algorithm"
[survivorship_bias]: https://en.wikipedia.org/wiki/Survivorship_bias
[three-valued logic]: https://modern-sql.com/concept/three-valued-logic

<!-- Data Quality Tools -->

[Deequ]: https://github.com/awslabs/deequ
[Great Expectations]: https://greatexpectations.io/
[Pandera]: https://pandera.readthedocs.io/
