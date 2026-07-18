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

<!-- TODO:

- DONE (2026-07-16): consolidated the two bridging sections, removed the
  DataFrame-native tail, handed the `.alias()` tutorial to `joins.md`, fixed
  the parameter-binding example, and repaired the architectural-fit link.
- Reference-style link definitions are missing for this page (for example,
  `[`.into_view()`]` and `[`.parse_sql_expr()`]`) — restore in a later cleanup.
- Architecture subsections (Choosing the Right Architecture / OLAP-OLTP /
  Federation) overlap `Concepts/architectural-dataframe.md` — review ownership
  later.
-->

<!--TODO (storyline and ownership review, 2026-07-18) 
1. Reframe this page around one reader question: when should a pipeline use SQL, DataFrame methods, or a deliberate boundary between them?
2. Preserve the fair comparison: SQL and the DataFrame API build LogicalPlans for the same optimizer and execution engine. Compare ergonomics, composition, parameterization, error timing, and application integration — not execution speed.
3. Own the supported API bridges, subject to repository verification: - SessionContext::sql() - DataFrame::into_view() - DataFrame::parse_sql_expr() - DataFrame::select_exprs() - DataFrame::with_param_values() 
4. Remove or relocate material that does not directly serve API mixing, including broad NoSQL comparisons, federation architecture, storage-engine recommendations, and unrelated external-system surveys. 
5. Correct advocacy and overbroad claims: - Rust does not compile-time validate string column names against a runtime schema. - SQL can be safely parameterized. - DataFrame construction is not universally safer or clearer. - Equivalent plans do not gain speed merely from the authoring API. 
6. Keep detailed transformation behavior on its owning method page. selection.md should hand off .select_exprs() here rather than duplicate its tutorial. 
7. End with a transition from choosing or mixing APIs to composing a DataFrame plan through Rust variables, functions, ownership, and control flow in builder-patterns.md. 
8. Replace legacy blockquotes, <br> formatting, .show() proof examples, and unverified output during subtree implementation. -->

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
- Early error detection—method and type errors fail at compile time; column and schema errors surface when the plan is built, before execution
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
| **Error Detection**    | **Plan-build time:** Schema errors surface when the plan is built.         | **Compile-time:** Invalid method calls fail compilation.                                      |
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

For a detailed breakdown of columnar vs. row-based trade-offs, see [Architectural Fit: OLAP vs. OLTP](../Concepts/architectural-dataframe.md#architectural-fit-olap-vs-oltp).

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

---

## SQL-Expression Bridge Methods

These methods move SQL text into a DataFrame pipeline. They preserve SQL expression syntax while keeping the surrounding pipeline in the DataFrame API.

### SQL-DataFrame Hybrid Methods

These methods let you combine SQL's familiar syntax with DataFrame's programmatic power—the best of both worlds.

| Method                | Input             | Output      | Use Case                                  |
| --------------------- | ----------------- | ----------- | ----------------------------------------- |
| [`.parse_sql_expr()`] | SQL string        | `Expr`      | Single expression from config/user input  |
| [`.select_exprs()`]   | SQL strings array | `DataFrame` | Multiple computed columns with SQL syntax |

### Parsing SQL Expressions

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

### Selecting with SQL Expressions

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
use datafusion::assert_batches_sorted_eq;

#[tokio::main]
async fn main() -> Result<()> {
    let ctx = SessionContext::new();

    // Register data so SQL can reference it
    let people = dataframe!(
        "name" => ["Alice", "Bob", "Carol"],
        "age" => [30, 25, 35]
    )?;
    ctx.register_table("people", people.into_view())?;

    // Build a plan that CONTAINS a placeholder ($1), then bind it
    let plan = ctx.sql("SELECT name, age FROM people WHERE age > $1").await?;
    let bound = plan.with_param_values(vec![ScalarValue::from(28i64)])?;

    let batches = bound.collect().await?;
    assert_batches_sorted_eq!(
        &[
            "+-------+-----+",
            "| name  | age |",
            "+-------+-----+",
            "| Alice | 30  |",
            "| Carol | 35  |",
            "+-------+-----+",
        ],
        &batches
    );
    Ok(())
}
```

- **Native Rust values** — Pass variables directly into DataFrame expressions with `lit()`; no binding is needed (for example, `df.filter(col("age").gt(lit(min_age)))?`).
- **SQL-plan placeholders** — When a plan already contains `$1` or `$name` placeholders, typically from a SQL string, bind them with `.with_param_values()`.

> **Use cases:**
>
> - **Reusable templates** — Build query once, bind different values
> - **User input** — Safely inject user-supplied values without SQL injection risk
> - **Dynamic filtering** — Change filter values without rebuilding the plan

---

## Conclusion

The SQL and DataFrame APIs share one `LogicalPlan`; switch at boundaries with `ctx.sql()` and `into_view()`, embed SQL text with `.parse_sql_expr()` and `.select_exprs()`, and bind placeholders with `.with_param_values()`. Switching APIs never requires executing or materializing data.

---

## Further Reading

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

[`.sql()`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.sql
[`.into_view()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.into_view
[`.register_table()`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.register_table
[`.parse_sql_expr()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.parse_sql_expr
[`.select_exprs()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.select_exprs
[`.with_param_values()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.with_param_values
[`dataframe!`]: https://docs.rs/datafusion/latest/datafusion/macro.dataframe.html
[`dataframe`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html
[`logicalplan`]: https://docs.rs/datafusion/latest/datafusion/logical_expr/enum.LogicalPlan.html
[`tableprovider`]: https://docs.rs/datafusion/latest/datafusion/catalog/trait.TableProvider.html
[concepts]: ../Concepts/index.md
[neo4j]: https://neo4j.com/
[amazon neptune]: https://aws.amazon.com/neptune/
[dataframe algebra]: https://arxiv.org/pdf/2001.00888
[datafusion paper]: https://andrew.nerdnetworks.org/pdf/SIGMOD-2024-lamb.pdf
