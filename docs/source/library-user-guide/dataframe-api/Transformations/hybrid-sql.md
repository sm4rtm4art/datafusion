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

<!--  TODO: 

<!--TODO

1. ABSTRACT
2. INTRODUCTION
3. merge the 2 bridging sections into one


Restructure `hybrid_sql.md`

**1. Consolidate the "Bridging" Logic**
*   **Delete** the entire section titled `## Bridging to SQL` (and its code example).
    *   *Reason:* This is a near-duplicate of the earlier section `### The Seamless Workflow`. Both explain `into_view()` and `register_table()`.
*   **Keep** `### The Seamless Workflow` as the primary explanation for switching between APIs.

**2. Relocate "Methods with SQL Equivalents"**
*   **Move** the section `### Methods with SQL Equivalents` (containing `DISTINCT ON` and `Aliasing`) up.
    *   *Placement:* Insert it immediately after `### The Seamless Workflow` and before `### Best Practices for Mixing`.
    *   *Reason:* These methods (`distinct_on`, `alias`) are practical examples of hybrid/SQL-like behavior and fit better alongside the workflow examples than at the bottom.

**3. Remove Out-of-Scope Content**
*   **Delete** everything starting from `### Summary: DataFrame-Unique Methods` down to the very end of the file (including "The Power Stack" and "Quick Reference").
    *   *Reason:* This content is a copy-paste artifact from `dataframe_specifics.md`. It lists methods like `.drop_columns()` and `.unnest_columns()` which are not discussed in this file and do not relate to mixing SQL and DataFrames.

**4. Final Verify Structure**
Ensure the final document flows in this order:
1.  **Introduction** (Mixing SQL and DataFrames)
2.  **The Seamless Workflow** (The `into_view` logic)
3.  **Methods with SQL Equivalents** (`distinct_on`, `alias`)
4.  **Best Practices for Mixing** (When to use which)
5.  **API Ergonomics Comparison** (Error detection, tooling)
6.  **Choosing the Right Architecture** (OLAP vs OLTP)
7.  **The Federation Pattern** (TableProviders)
8.  **References**

-->

# Mixing SQL and DataFrames



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

```{contents} Table of Content
:local:
:depth: 2
```

## Introduction (placeholder)

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

<!--Check Reference
[When Row-Based TableProviders Outperform Columnar](#when-row-based-tableproviders-outperform-columnar)
-->

For a detailed breakdown of columnar vs. row-based trade-offs, see the When Row-Based TableProviders Outperform Columnar section earlier in this document.

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

(the-federation-pattern)=

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
- [Querying Postgres from DataFusion](https://datafusion.apache.org/library-user-guide/custom-table-providers.html) — Tutorial on building custom [`TableProvider`] implementations
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

<!-- TODO: MERGE THIS INTO THE FOLLOWING -->

## Bridging to SQL

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

### Methods with SQL Equivalents

These methods have SQL counterparts but offer ergonomic advantages for programmatic use.

### DISTINCT ON (PostgreSQL-Style)

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

### Aliasing DataFrames

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
│  • Zero-copy operations  • SIMD acceleration*               │
│  • Schema metadata       • Cross-language compatibility     │
└─────────────────────────────────────────────────────────────┘

*SIMD requires `RUSTFLAGS='-C target-cpu=native'`. See [Crate Configuration](../../user-guide/crate-configuration.md).
```

### Quick Reference

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
