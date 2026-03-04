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

# Concepts of DataFusion DataFrame API

**Understanding the DataFusion ecosystem — architecture, APIs, and the execution model behind every DataFrame.**

Data-driven applications require processing massive, diverse datasets efficiently across a variety of storage systems and formats. Apache Arrow DataFusion provides a blazing-fast, extensible query engine to meet this need, exposing its core engine through both standard SQL and a programmatic DataFrame API. This guide explores the conceptual foundation of the DataFrame API — from architectural fit and API design to execution internals and ecosystem positioning.

```{toctree} Table of Contents for Concepts of DataFrames
:maxdepth: 1
:numbered:
:titlesonly:
:caption: Concepts of DataFrames
architectural-dataframe
sessioncontext
builder-parser
anatomy-dataframe
expressions
execution-lifecycle
bigger-picture
summary
```

## Concept Documentation Overview

Use the following documents to navigate the conceptual architecture of DataFusion DataFrames. They are designed to be read sequentially, but you can jump directly to the topic you need:

| Document                                                 |              Focus               | Description                                                                               |
| :------------------------------------------------------- | :------------------------------: | :---------------------------------------------------------------------------------------- |
| **[Architectural Overview](architectural-dataframe.md)** |       Architecture and Fit       | Design philosophy, data flow, and OLAP vs OLTP — when DataFusion is the right tool.       |
| **[Session Context](sessioncontext.md)**                 |         The Entry point          | Deep dive into the `SessionContext` as the entry point and environment for DataFrames.    |
| **[Builder vs. Parser](builder-parser.md)**              |    Two API's <br> One engine     | Detailed comparison between SQL string parsing and the programmatic DataFrame builder.    |
| **[Anatomy of a DataFrame](anatomy-dataframe.md)**       |     Internals of a DataFrame     | Deep dive into the inner workings, exploring the `LogicalPlan` and `SessionState`.        |
| **[Expressions](expressions.md)**                        |         Row-Level Logic          | Expressions and how they are used to build the `LogicalPlan`.                             |
| **[Execution Lifecycle](execution-lifecycle.md)**        |       Lazy Materialization       | Detailed breakdown of the logical optimizer, physical planner, and async execution.       |
| **[The Bigger Picture](bigger-picture.md)**              | Historical context and evolution | Where DataFusion fits historically (e.g., Volcano model vs. modern vectorized execution). |
| **[Summary](summary.md)**                                |            Conclusion            | A wrap-up of core concepts, next steps, and further reading resources.                    |

## DataFusion as a Query Engine

**DataFusion as a query engine — connecting diverse data sources to a unified execution pipeline.**

DataFusion is an embeddable, modular OLAP query engine built on Apache Arrow. Through the `TableProvider` trait, it connects to files (Parquet, CSV, JSON, Avro), in-memory data, external databases, and lakehouse formats (Iceberg, Delta Lake) — processing everything through its optimized columnar framework. Two equivalent APIs (SQL and DataFrame) converge into the same `LogicalPlan`, optimizer, and parallel execution engine.

For the full data flow diagram, design philosophy, and architectural context, see [Architectural Overview](architectural-dataframe.md).

DataFusion shines as an **OLAP** engine — scanning millions of rows, aggregating across partitions, and powering lakehouse query layers. When your workload is **OLTP** (single-row lookups, sub-millisecond point queries, frequent updates), a traditional database with indexes is the better fit. You can still bring OLTP data _into_ DataFusion through a custom `TableProvider` for analytical queries over that data.

:::{note} Rule of Thumb
"Find one row by ID" → Use a database with indexes.
"Aggregate a billion rows" → Use DataFusion.
For the full OLAP vs OLTP comparison, see [Architectural Fit](architectural-dataframe.md#architectural-fit-olap-vs-oltp).
:::

## SessionContext: The Hub and Entry Point for Datafusion

**SessionContext — the single entry point for both APIs, managing configuration, catalogs, and execution state.**

At the center of this architecture sits the `SessionContext` (commonly abbreviated as `ctx` in code examples). It acts as the central hub and entry point for all queries. The `SessionContext` registers your table providers, manages configurations, and exposes the underlying execution engine to the user through two distinct interfaces. When a DataFrame is created, it captures a `SessionState`—an immutable snapshot of the `SessionContext` at that exact moment. This design guarantees consistency during the entire data manipulation process.

---

## Two APIs, One Query Engine

**Whether you write declarative SQL or chain programmatic builder methods, both produce the exact same DataFrame — same plan, same optimizations, same performance.**

Both APIs compile down to the exact same execution plan, meaning performance is identical. The choice between them is a matter of system architecture and ergonomics:

### The SQL API (Parser Architecture)

By calling `SessionContext::sql()`, you pass a declarative SQL string. DataFusion parses this string using a configurable dialect and plans the query. This is the natural choice for ad-hoc exploration, complex window functions and CTEs, embedding user-written queries, or migrating existing SQL workloads.

### The DataFrame API (Builder Architecture)

By calling methods like `SessionContext::table()` or `SessionContext::read_parquet()`, you construct queries programmatically. This builder pattern enables readable, top-to-bottom pipelines and composable query fragments. Rust's type system catches schema and syntax errors at compile time, making it vastly superior for dynamic query generation.

### Overview to compare the architectures

| Feature / Need       | SQL API (Parser)                            | DataFrame API (Builder)                    |
| :------------------- | :------------------------------------------ | :----------------------------------------- |
| **Paradigm**         | Declarative strings (`"SELECT * FROM..."`)  | Programmatic method chaining (`.select()`) |
| **Error Checking**   | Runtime (during parsing/planning)           | Compile-time (Rust type system)            |
| **Dynamic Queries**  | String concatenation (error-prone)          | Composable Rust functions (safe, reusable) |
| **Primary Use Case** | BI tools, query consoles, legacy migrations | Application logic, complex ETL pipelines   |

:::{note}
Both APIs produce lazy DataFrames for query statements (`SELECT`). The `.sql()` call parses and plans the query but does **not** execute it — you still need `.collect()`, `.show()`, or `.write_*()` to trigger execution. However, DDL/DML statements (`CREATE TABLE`, `INSERT`, `SET`) execute immediately during the `.sql()` call itself.
:::

---

## The DataFrame: A Lazy Data Framework

**The DataFrame is a lazy, in-memory wrapper around the logical plan and the session state, executing only what is needed, when it is needed.**

The DataFrame concept originated in the Python pandas library as an intuitive data-wrangling tool for data scientists and analysts, significantly reducing the cognitive overhead of complex SQL queries. The API's chainable builder pattern allows users to seamlessly apply filters, projections, and aggregations step-by-step.

As data ecosystems grew, libraries like PySpark and Polars matured this concept for big data by introducing **lazy evaluation**. DataFusion adopts this highly efficient design. Because the plan is evaluated lazily, the engine limits memory overhead by processing only the data that is strictly necessary for the final output.

In DataFusion, a Data**Frame** is not your data — it's the _frame_ around your data. Think of it literally: a framework defining where data lives, how it flows through the query engine, and the environment in which transformations execute.

### The Execution Lifecycle

Because DataFrames are lazy, calling transformation methods like `.filter()` or `.aggregate()` does not process any data; it merely appends new instructions to the `LogicalPlan`.

To actually process data, you must trigger an action that materializes the results. When you call an execution method like `.collect()` (to load into memory) or `.write_parquet()` (to persist to disk), the DataFrame hands its logical plan to the engine:

- The **Logical Optimizer** rewrites the plan to make the math more efficient.
- The **Physical Planner** maps the operations to your hardware, partitioning the work across CPU cores.
- The **Execution Engine** asynchronously pulls the data, yielding a stream of columnar `RecordBatch` chunks rather than evaluating row-by-row.

---
