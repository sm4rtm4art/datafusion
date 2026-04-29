<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License
  http://www.apache.org/licenses/LICENSE-2
Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

<!-- TODO: Diskuss Mayor refractoring!

Way to much content!

- every mayor Methods (join, merge, filter..) As individuall document.
- Concept part adatping
-  Futher ideas: ???

>


<!-- TODO:
- Add encoding https://docs.rs/datafusion/latest/datafusion/functions/encoding/index.html
- nested functions https://docs.rs/datafusion/latest/datafusion/functions_nested/index.html ,
-  maybe even datetime (https://docs.rs/datafusion/latest/datafusion/functions/datetime/index.html)  -->


# Transformations with DataFrame API



```{toctree}
:maxdepth: 1
:caption: Transformation methods
:numbered:
:titlesonly:
aggregations
builder-patterns
concepts
data-quality
dataframe-specifics
filtering
hybrid-sql
joins
reshaping
selection
set-operations
sorting-limiting
subqueries
window-functions
```

## Introduction (Placeholder)

**The “life” phase of the DataFrame lifecycle: build and refine a lazy query plan.**

Transformations are where you re-shape and analyze data: once you [create](./creating-dataframes.md) a DataFrame, you can filter, select, join, aggregate, sort, and enrich data by composing methods that build a [`LogicalPlan`]. In the [DataFrame lifecycle metaphor](./index.md#the-dataframe-lifecycle), this is the "life" phase—execution and persistence happen later (see [Writing & Executing](./writing-dataframes.md)).

This guide compares the DataFrame API to DataFusion's [SQL API](../using-the-sql-api.md). Both compile to the same [`LogicalPlan`], so the choice is primarily about ergonomics: DataFrames shine for programmatic composition and IDE tooling; SQL shines for concise, declarative queries and portability. For the conceptual model, see [DataFrame Concepts](./concepts.md#introduction).

:::{admonition} Style Note
:class: note

In this document, all code elements are highlighted with backticks.

- DataFrame methods are written as `.method()` (e.g., `.select()`) to reflect the chaining syntax central to the API.
- standalone functions `method()` (e.g `col()`)

- static constructors `Struckt::method()` (e.g., `SessionContext::new()`).
- Rust types are formatted as `TypeName` (e.g., `SchemaRef`).

:::

```{contents}
:local:
:depth: 2
```

## Introduction

Transformations allow you to shape, filter, enrich, and analyze your data through a series of composable, type-safe operations. Unlike SQL, where queries are often monolithic strings, DataFrames allow you to **build queries programmatically**. This approach shines when you need to:

- **Chain operations** into readable, logical pipelines.
- **Build queries dynamically** based on runtime conditions or configuration.
- **Leverage the Rust type system** to catch errors at compile time.
- **Reuse logic** by encapsulating complex transformations into functions.

The following diagram illustrates the conceptual position in the DataFrame architecture. Both the SQL API and the DataFrame API share the same [`SessionContext`] and converge to the same [`LogicalPlan`] with column-based operations.<br>

## Introduction

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

For a deeper dive, see:

- [DataFrame Concepts](./concepts.md#introduction)
- [SIGMOD 2024 Paper][datafusion paper],
- [architecture overview on docs.rs][docs.rs]

For most data transformations, the choice between SQL-API and DataFrame-APIs is primarily about ergonomics—both produce identical execution plans. However, the DataFrame API is more than just "SQL with different syntax." The DataFrame API is a programmatic **builder** for query plans, whereas the SQL API is a declarative **parser** for query strings. Research on [DataFrame Algebra][dataframe algebra] shows that the DataFrame paradigm offers a distinct way of _expressing_ data transformations. These patterns were established by the [pandas library][pandas] and continuously refined by projects like [Apache Spark] for efficient parallel, multi-node computation.

---

<!--TODO: SHORTEN AND REBASE IN CONCPTS -->

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
| **Set Ops**     |   [`UNION ALL`][`union`]   |               [`.union()`]               | concatenates DataFrames.                                                                    |
| **Set Ops**     |         [`UNION`]          |          [`.union_distinct()`]           | concatenates and removes duplicates.                                                        |
| **Set Ops**     |   (No direct equivalent)   |           [`.union_by_name()`]           | **Unique**: Unions based on column names, forgiving column order mismatches.                |
| **Distinct**    |   [`DISTINCT`][`select`]   |             [`.distinct()`]              | Removes duplicate rows based on all columns.                                                |
| **Distinct**    | [`DISTINCT ON`] (Postgres) |            [`.distinct_on()`]            | **Unique**: Deduplicates based on specific columns, keeping the "first" row per sort order. |

:::{note}
While you can mix SQL and DataFrames (see [Concepts](concepts.md)), mastering these native methods unlocks the full power of programmatic data manipulation.
:::

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
use datafusion::prelude::*;

fn main() {
    let filter_department: Option<&str> = Some("Sales");

    let mut query = "SELECT * FROM employees WHERE 1=1".to_string();
    if let Some(dept) = filter_department {
        query.push_str(&format!(" AND department = '{}'", dept));
    }
}
```

<!--  SPHINX CODE WITH EMPHASIZING THE CODE (DON'T GET TESTED)
```{code-block} rust
:caption: **SQL approach** — string concatenation:
:emphasize-lines: 5
use datafusion::prelude::*;

fn main() {
    let filter_department: Option<&str> = Some("Sales");

    let mut query = "SELECT * FROM employees WHERE 1=1".to_string();
    if let Some(dept) = filter_department {
        query.push_str(&format!(" AND department = '{}'", dept));
    }
}
```
-->

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

<!--  SPHINX CODE WITH EMPHASIZING THE CODE (DON'T GET TESTED)

```{code-block} rust
:caption: **DataFrame approach** — type-safe, composable:
:emphasize-lines: 10
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
-->

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

(when-row-based-tableproviders-outperform-columnar)=

#### When Row-Based [`TableProvider`] Outperform Columnar

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

- **Push down what you can:** DataFusion's [`TableProvider`] interface supports predicate and projection pushdown — filters and column lists reach the source DB
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
