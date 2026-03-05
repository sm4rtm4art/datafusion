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

# Two Paths to the Same Plan: Parser vs Builder

**SQL and the DataFrame API are two front-ends to the same query engine**<br>

Both compile to identical [`LogicalPlan`] representations, receive the same optimizations, and execute with the same performance. This unified architecture means you can choose whichever API fits your workflow without sacrificing speed, and you can freely combine both in a single application.

:::{admonition} Style Note
:class: note

In this document, all code elements are highlighted with backticks.

- DataFrame methods are written as `.method()` (e.g., `.select()`) to reflect the chaining syntax central to the API.
- standalone functions `method()` (e.g `col()`)
- static constructors `Struckt::method()` (e.g., `SessionContext::new()`).
- Rust types are formatted as `TypeName` (e.g., `SchemaRef`).

:::

```{contents} Table of Contents for Parser vs Builder
:local:
:depth: 2
```

## Introduction to Parser vs Builder

DataFusion inherits its dual-API design from the broader data ecosystem. The SQL API (parser architecture) and the DataFrame API (builder architecture) are two APIs to the same query engine — identical in performance, different in ergonomics and safety.

### SQL API: The Parser Architecture

SQL is a broadly established standard for database queries and a must-have in any query engine. By design, SQL is a declarative language: you describe _what_ you want, not _how_ to compute it. DataFusion parses SQL strings into an Abstract Syntax Tree (AST) and then converts the AST into a `LogicalPlan`. This makes SQL the natural choice when queries originate as text — whether stored in configuration files, typed by users in a query console, sent by external tools over JDBC/ODBC, or migrated from existing SQL-based systems.

### DataFrame API: The Builder Architecture

With the rise of data science and the pandas library, the DataFrame API emerged as a programmatic alternative to SQL, making datahandling more accessible and easier to understand. Instead of writing query strings, you construct the query plan by chaining methods (`.filter()`, `.select()`, `.aggregate()`) — each call appending a node to the `LogicalPlan` directly, without any parsing step. Built on top of the Apache Arrow columnar format, the DataFrame API offers compile-time type safety, composable query fragments, and full IDE support (autocomplete, refactoring, go-to-definition).

Each API shines in different contexts, but under the hood they share the exact same optimizer and execution engine.

The following diagram illustrates how both paths converge into a single [`LogicalPlan`]:

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

::::::{admonition} Reading the diagram
:class: seealso

:::{admonition} SessionState (top container)
:class: note
Both paths operate within the same execution environment. The [`SessionState`] provides the catalog (table definitions), function registry (UDFs, aggregates), configuration, and query planner. This shared context is why both APIs have access to the same tables and functions.
:::

:::{admonition} SQL (Parser) path
:class: note
A query string goes through `sqlparser`'s lexer and parser to produce an Abstract Syntax Tree (AST) — a structured, tree-shaped representation of the SQL syntax that the engine can reason about programmatically. The logical planner then converts the AST into a [`LogicalPlan`]. This extra parsing step enables familiar SQL syntax but means errors (typos, missing columns) surface at runtime rather than compile time.
:::

:::{admonition} DataFrame API (Builder) path
:class: note
Method calls like [`.filter()`] and [`.select()`] construct [`LogicalPlan`] nodes directly—no parsing, no AST. This is why you get IDE autocomplete and why Rust can catch type errors at compile time (though schema errors remain runtime).
:::

:::{admonition} LogicalPlan (convergence point)
:class: note
Both paths produce the _exact same_ [`LogicalPlan`] structure. There's no "SQL flavor" vs "DataFrame flavor"—just one unified representation. This is the key insight that makes mixing APIs free.
:::

:::{admonition} Further Execution
:class: note
From here, the [`LogicalPlan`] flows through optimization, physical planning, and execution—identically regardless of which path created it.
:::
::::::

:::{admonition} Key takeaway
:class: seealso
Parser vs Builder is purely a construction choice — once you have a `LogicalPlan`, DataFusion doesn't know or care how you built it. Performance is identical; the difference lies in ergonomics and compile-time safety.
:::

---

## Choosing the Right API for the Task

**Performance is identical — the choice is purely about ergonomics, team skills, and where your queries originate.**

Since both APIs compile to the same [`LogicalPlan`], the decision is about developer experience, not execution speed. Different contexts demand different APIs: a REST endpoint receiving user-provided queries naturally uses SQL; a Rust application building complex, conditional pipelines benefits from the builder pattern's compile-time safety. Neither API is universally "better" — they are complementary tools, each suited to specific situations within the same application.

| Choose **DataFrame API** when...                         | Choose **SQL** when...                              |
| -------------------------------------------------------- | --------------------------------------------------- |
| Query logic depends on runtime conditions                | Query is static and well-defined                    |
| You want reusable query fragments (extract to functions) | You need complex analytics (window functions, CTEs) |
| Security matters (no SQL injection by construction)      | Query comes from config files or user input         |
| IDE refactoring and autocomplete matter                  | Team is SQL-fluent, Rust is secondary               |

Use [`.into_view()`] to bridge between APIs at well-defined boundaries: register any DataFrame as a table so downstream SQL queries can reference it by name.

---

## In Practice: Two Paths, One Result

**Both APIs are interchangeable — choose either path for any given task and get identical results.**

The following example queries the same table using SQL (parser path) and the DataFrame API (builder path), then verifies that both produce identical results. This is the core promise of DataFusion's unified architecture: the API you use is a construction choice, not a performance choice.

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

:::{caution} Single Abstraction Layer
While both APIs can be combined in the same application, maintain a **single abstraction layer** per component or module. Mixing SQL strings and DataFrame chains within the same function leads to code that is hard to read, test, and maintain. Pick one API as the primary interface for each layer of your application, and use [`.into_view()`] only at well-defined boundaries (e.g., registering a DataFrame result as a table for a downstream SQL query).
:::

---

## Safety and Security: Where the DataFrame API Shines

**The builder pattern eliminates entire classes of bugs that plague SQL-based systems — by construction, not by convention.**

When queries are built from SQL strings, every dynamic parameter is a potential injection vector. Sanitizing user input, escaping special characters, and validating query structure all fall on the developer. The DataFrame API sidesteps these risks entirely:

- **No SQL injection by construction:** Method calls like `.filter(col("amount").gt(lit(threshold)))` produce `LogicalPlan` nodes directly. There is no string to inject into — the query is never represented as text.
- **Compile-time type safety:** Rust's type system ensures that `Expr` arguments are structurally valid. Passing an integer where a column reference is expected fails at compile time, not in production.
- **Schema validation at plan-build time:** When you chain `.select()` or `.filter()`, DataFusion validates column names and types against the `DFSchema` immediately. A misspelled column name surfaces as a `Result::Err` at the point of construction, not deep inside execution.
- **Composable and auditable:** Query logic lives in typed Rust functions that can be unit-tested, reviewed in pull requests, and refactored with IDE support. SQL strings embedded in code resist all three.

:::{admonition} Key takeaway
:class: note
DataFusion's SQL API is safe when queries are static strings or come from trusted sources. The security advantage of the DataFrame API specifically applies when query logic is **dynamic** — constructed from user input, runtime conditions, or external parameters.
:::

---

:::{admonition} Further reading
:class: seealso
This dual-API architecture follows principles established in the broader data science ecosystem. For the theoretical foundation, see [Towards Scalable Dataframe Systems][dataframe-paper]. For the complete method reference, see [Transformations](../Transformations/index.md).
:::

With both APIs converging into the same `LogicalPlan`, the next question is: what exactly lives inside a DataFrame? See [Anatomy of a DataFrame](anatomy-dataframe.md) for a deep dive into the `LogicalPlan` and `SessionState` that every DataFrame carries.

<!-- Reference-style links -->

[`LogicalPlan`]: https://docs.rs/datafusion-expr/latest/datafusion_expr/logical_plan/enum.LogicalPlan.html
[`SessionState`]: https://docs.rs/datafusion/latest/datafusion/execution/session_state/struct.SessionState.html
[`.filter()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.filter
[`.select()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.select
[`.into_view()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.into_view
[dataframe-paper]: https://arxiv.org/abs/2001.00888
