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

<!--
TODO(Docs): Add a section on "Expressions (`Expr`) and Data Flow"

1. EXPRESSIONS (`Expr`)
   - WHAT TO ADD: Introduce the `Expr` concept. If `DataFrame` is the container and `LogicalPlan` contains the relational operators (Filter, Join), then `Expr` represents the row-level logic inside those operators.
   - HOW TO EXPLAIN IT:
     - Show a quick example: `col("a").gt(lit(10))` creates an `Expr` tree.
     - Explain that methods like `.filter()` and `.select()` take `Expr` as arguments.
     - Mention that `Expr`s are dialect-agnostic and evaluated at runtime against the schema.
     - (Optional but helpful: Link to the `Expr` API docs or a dedicated expressions guide).

2. RECORD BATCHES & PARTITIONS
   - WHAT TO ADD: A brief conceptual bridge between the execution plan and the actual data.
   - HOW TO EXPLAIN IT:
     - Clarify that a DataFrame doesn't hold data as one giant table in memory.
     - Explain that under the hood, data is divided into **partitions** (enabling parallel processing via Tokio).
     - Execution streams these partitions as **RecordBatches** (Arrow's chunked, columnar format) rather than row-by-row.
     - Tie this back to why `.execute_stream()` is memory-efficient compared to `.collect()`.

Placement suggestion: Put the `Expr` explanation right after "DataFrame Structure: LogicalPlan + SessionState" (since it naturally flows from explaining the LogicalPlan), and put the Partitions/RecordBatches explanation right after the "Execution Model / Tokio" section.
-->

# Concepts of DataFusion DataFrame API

**What DataFrames are, where they live, and why they matter in the query engine landscape.**

Data-driven applications require processing massive, diverse datasets efficiently across a variety of storage systems and formats. Apache Arrow DataFusion provides a blazing-fast, extensible query engine to meet this need, exposing its core engine through both standard SQL and a programmatic DataFrame API. This guide explores the conceptual foundation of the DataFrame API—what a DataFrame actually is, why a DataFrame is needed, how it translates builder methods into optimized execution plans, where it is located in the DataFusion architecture, and where it fits within your broader system architecture.

:::{admonition} Style Note
:class: note  
:collapsible: open

To help you quickly scan and understand the API, this documentation follows a strict typographical convention for code elements:

- **Instance Methods:** `.method()` (e.g., `.select()`, `.filter()`) indicates a chained call on a DataFrame or builder.
- **Standalone Functions:** `function()` (e.g., `col()`, `lit()`) indicates a function imported from `datafusion::logical_expr`.
- **Constructors:** `Struct::new()` (e.g., `SessionContext::new()`) indicates a static initialization method.
- **Types & Traits:** `PascalCase` (e.g., `LogicalPlan`, `RecordBatch`) indicates a Rust struct, enum, or trait.
  :::

```{toctree} Table of Contents for Concepts of DataFrames
:maxdepth: 1
:numbered:
:titlesonly:
:caption: Concepts of DataFrames
high-level-dataframe
builder-parser
sessioncontext
bigger-picture
anatomy-dataframe
execution-lifecycle
summarty
```

## Concept Documentation Overview

Use the following documents to navigate the conceptual architecture of DataFusion DataFrames. They are designed to be read sequentially, but you can jump directly to the topic you need:

| Document                                           | Focus           | Description                                                                               |
| :------------------------------------------------- | :-------------- | :---------------------------------------------------------------------------------------- |
| **[Session Context](sessioncontext.md)**           | The Hub         | Deep dive into the `SessionContext` as the entry point and environment for DataFrames.    |
| **[Builder vs. Parser](builder-parser.md)**        | API Choice      | Detailed comparison between SQL string parsing and the programmatic DataFrame builder.    |
| **[Anatomy of a DataFrame](anatomy-dataframe.md)** | Internals       | Deep dive into the inner workings, exploring the `LogicalPlan` and `SessionState`.        |
| **[Execution Lifecycle](execution-lifecycle.md)**  | Materialization | Detailed breakdown of the logical optimizer, physical planner, and async execution.       |
| **[The Bigger Picture](bigger-picture.md)**        | Evolution       | Where DataFusion fits historically (e.g., Volcano model vs. modern vectorized execution). |
| **[Summary](summary.md)**                          | Conclusion      | A wrap-up of core concepts, next steps, and further reading resources.                    |

## The DataFrame: A Framework for Your Data

**Two APIs—one query engine with high connectivity, covering the full spectrum of data processing needs.**

In modern data architectures, information resides across multiple systems and formats. DataFusion acts as a central, out-of-the-box query engine designed to process this distributed data efficiently. It natively reads standard file formats (Parquet, CSV, JSON, Avro) and integrates seamlessly with the Apache Arrow in-memory format.

Beyond native files, DataFusion is highly extensible. Through the `TableProvider` trait, developers can connect the engine to external OLTP databases (via JDBC/ODBC extensions), REST APIs, or modern data lakehouse formats (Iceberg, Delta Lake). External systems can even pre-filter data at the source—a concept known as predicate pushdown—before delivering it to DataFusion. Once ingested, all data is processed in DataFusion's highly optimized, columnar Arrow framework.

The following flow diagram shows the integration of data into the datafusion query engine. Thriving towards the Two API's and the resulting in one DataFrame, to be further executed.

```text
[ DATA SOURCES ]
┌──────────────┐ ┌──────────────┐ ┌──────────────┐ ┌─────────────────────┐
│ Files/Stores │ │ In-Memory    │ │ External DBs │ │ Extensions / Formats│
│(Parquet/CSV) │ │ (Batches)    │ │ & APIs       │ │ (Iceberg, Delta,...)│
└──────┬───────┘ └──────┬───────┘ └──────┬───────┘ └──────────┬──────────┘
       ▼                ▼                ▼                    ▼
┌────────────────────────────────────────────────────────────────────────┐
│                            TABLE PROVIDERS                             │
│               (ListingTable, MemTable, Custom Providers)               │
└───────────────────────────────────┬────────────────────────────────────┘
                                    ▼
┌────────────────────────────────────────────────────────────────────────┐
│                            SESSION CONTEXT                             │
│                (The Hub: Catalogs, Configs, Functions)                 │
└──────────────────┬────────────────────────────────┬────────────────────┘
                   ▼                                ▼
           ┌───────────────┐                ┌───────────────┐
           │    SQL API    │                │ DataFrame API │
           │   (Parser)    │                │   (Builder)   │
           └───────┬───────┘                └───────┬───────┘
                   └────────────────┬───────────────┘
                                    ▼
┌────────────────────────────────────────────────────────────────────────┐
│                                DATAFRAME                               │
│                         (Immutable, Lazy Handle)                       │
├────────────────────────────────────────────────────────────────────────┤
│ 1. LogicalPlan: Relational operations (Filter, Join) + DFSchema        │
│ 2. SessionState: Immutable snapshot of context during creation         │
└───────────────────────────────────┬────────────────────────────────────┘
                                    ▼
                            [ EXECUTION PATH ]
                 (Optimizers ➔ Physical Plan ➔ Async Stream)

```

---

## The Hub: SessionContext

At the center of this architecture sits the `SessionContext` (commonly abbreviated as `ctx` in code examples). It acts as the central hub and entry point for all queries. The `SessionContext` registers your table providers, manages configurations, and exposes the underlying execution engine to the user through two distinct interfaces. When a DataFrame is created, it captures a `SessionState`—an immutable snapshot of the `SessionContext` at that exact moment. This design guarantees consistency during the entire data manipulation process.

---

## Two APIs, One Query Engine

**Two distinct APIs converge into a single unified object. Writing declarative SQL strings or chaining programmatic builder methods, the resulting `DataFrame` struct is exactly the same.**

Both APIs compile down to the exact same execution plan, meaning performance is identical. The choice between them is a matter of system architecture and ergonomics:

### The SQL API (Parser Architecture)

By calling `SessionContext::sql()`, you pass a declarative SQL string. DataFusion parses this string using a configurable dialect and plans the query. This is the ideal interface for BI tools, user-facing query consoles, or legacy system migrations.

### The DataFrame API (Builder Architecture)

By calling methods like `SessionContext::table()` or `SessionContext::read_parquet()`, you construct queries programmatically. This builder pattern enables readable, top-to-bottom pipelines and composable query fragments. Rust's type system catches schema and syntax errors at compile time, making it vastly superior for dynamic query generation.

### Overview to compare the architectures

| Feature / Need       | SQL API (Parser)                            | DataFrame API (Builder)                    |
| :------------------- | :------------------------------------------ | :----------------------------------------- |
| **Paradigm**         | Declarative strings (`"SELECT * FROM..."`)  | Programmatic method chaining (`.select()`) |
| **Error Checking**   | Runtime (during parsing/planning)           | Compile-time (Rust type system)            |
| **Dynamic Queries**  | String concatenation (error-prone)          | Composable Rust functions (safe, reusable) |
| **Primary Use Case** | BI tools, query consoles, legacy migrations | Application logic, complex ETL pipelines   |

## The DataFrame: A Lazy Data Framework

**The DataFrame is a lazy, in-memory wrapper around the logical plan and the session state, executing only what is needed, when it is needed.**

The DataFrame concept originated in the Python pandas library as an intuitive data-wrangling tool for data scientists and analysts, significantly reducing the cognitive overhead of complex SQL queries. The API's chainable builder pattern allows users to seamlessly apply filters, projections, and aggregations step-by-step.

As data ecosystems grew, libraries like PySpark and Polars matured this concept for big data by introducing **lazy evaluation**. DataFusion adopts this highly efficient design. Because the plan is evaluated lazily, the engine limits memory overhead by processing only the data that is strictly necessary for the final output.

### The Execution Lifecycle

Because DataFrames are lazy, calling transformation methods like `.filter()` or `.aggregate()` does not process any data; it merely appends new instructions to the `LogicalPlan`.

To actually process data, you must trigger an action that materializes the results. When you call an execution method like `.collect()` (to load into memory) or `.write_parquet()` (to persist to disk), the DataFrame hands its logical plan to the engine:

- The **Logical Optimizer** rewrites the plan to make the math more efficient.
- The **Physical Planner** maps the operations to your hardware, partitioning the work across CPU cores.
- The **Execution Engine** asynchronously pulls the data, yielding a stream of columnar `RecordBatch` chunks rather than evaluating row-by-row.

---

## Architectural Fit: OLAP vs. OLTP

**The right tool for the right job—knowing DataFusion's sweet spot saves you from architectural dead-ends.**

DataFusion is a **query engine foundation** optimized for read-heavy, scan-oriented workloads over columnar data. This includes OLAP analytics, data lakehouse engines, ETL pipelines, and embedded query execution. The key distinction is understanding OLAP versus OLTP:

| Aspect             | OLAP (DataFusion's strength) | OLTP (Consider alternatives) |
| :----------------- | :--------------------------- | :--------------------------- |
| **Pattern**        | Scan many rows, aggregate    | Find/update single rows      |
| **Data model**     | Immutable, append-only       | Mutable, transactional       |
| **Latency target** | Milliseconds to seconds OK   | Sub-millisecond required     |
| **Indexing**       | Column statistics, pruning   | B-tree, hash indexes         |

**DataFusion shines when:**

- Scanning and aggregating millions to billions of rows.
- Building data lakehouse query layers (Parquet, Delta Lake, Iceberg).
- Executing ETL and data transformation pipelines.
- Building domain-specific query engines on reusable infrastructure.

**Consider alternatives when:**

| Use Case                    | Why DataFusion May Not Fit                 | Better Alternatives                |
| :-------------------------- | :----------------------------------------- | :--------------------------------- |
| **Single-row lookups**      | Columnar format overhead; no index support | PostgreSQL, DynamoDB, Redis        |
| **Sub-millisecond latency** | Query planning overhead (~1-10ms minimum)  | Pre-compiled queries, KV stores    |
| **Heavy UPDATE/DELETE**     | Designed for immutable, append-only data   | OLTP database, or lakehouse format |
| **Small datasets (<100K)**  | Works fine, but simpler APIs exist         | pandas, Polars (less setup)        |
| **Real-time streaming**     | Batch-oriented execution model             | Kafka Streams, Flink, RisingWave   |

:::{note} Rule of Thumb
"Find one row by ID" → Use a database with indexes.
"Aggregate a billion rows" → Use DataFusion.
:::

The OLAP sweet spot: DataFusion is optimized for read-heavy analytical queries where you scan large amounts of data, filter aggressively, and aggregate results. If your workload involves frequent small writes, point lookups, or requires sub-millisecond response times, a different architecture is likely a better fit.
