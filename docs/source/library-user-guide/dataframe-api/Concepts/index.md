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

Data-driven projects demand efficient processing of large, diverse datasets. DataFusion addresses this as a query engine—connecting data sources from Parquet files to object stores to custom providers—through two primary APIs: the DataFrame API and SQL. Both compile to the same `LogicalPlan`, so performance is equivalent; the choice is ergonomics.

The DataFrame API's builder architecture, originated in the known python pandas library, makes it uniquely suited for programmatic query construction: readable, maintainable pipelines with dynamic filters, conditional logic, and seamless Rust integration. What the builder pattern can't express elegantly (complex window functions, CTEs), the SQL-API covers. And when Apache Arrows columnar OLAP processing isn't the right fit, DataFusion's `TableProvider` interface lets you integrate row-oriented systems with predicate pushdown.

This documentation explores the conceptual foundation: what a DataFrame _is_ (a `LogicalPlan` paired with a frozen `SessionState`), how it flows through the execution pipeline, and where DataFusion fits in the broader data systems landscape. For hands-on examples, see:

- [Create](creating-dataframes.md)
- [Schema](schema-management.md)
- [Transform](transformations.md)
- [Write](writing-dataframes.md)

:::{admonition} Style Note

---

:class: note  
:collapsible: closed

---

In this document, all code elements are highlighted with backticks.

- DataFrame methods are written as `.method()` (e.g., `.select()`) to reflect the chaining syntax central to the API.
- standalone functions `method()` (e.g `col()`)
- static constructors `Struckt::method()` (e.g., `SessionContext::new()`).
- Rust types are formatted as `TypeName` (e.g., `SchemaRef`).
  :::

```{toctree} Table of Contents for Concepts of DataFrames
:maxdepth: 1
:numbered:
:titlesonly:
:caption: Concepts of DataFrames
high-level-dataframe
builder-parser
session-context
anatomy-dataframe
execution-lifecycle
summarty
```

## Introduction

### Why the DataFrame-API if the SQL-API is available?

DataFusion provides two entry points to the same query engine: the **SQL-API** (SQL strings parsed via [`sqlparser`] with [configurable dialect]) and the **DataFrame-API** (a builder pattern constructing plans programmatically). Both compile to the same [`LogicalPlan`] and execute identically—the choice is about ergonomics, not performance. Throughout this documentation, we use PostgreSQL syntax when comparing the APIs—it's well documented, widely understood, and DataFusion's default semantics (NULL handling, sort order) closely follow PostgreSQL conventions.

#### Datafram builder-API alongside a parser-based SQL-API?

The builder pattern offers ergonomics that parser patterns like the SQL-API cannot match—readable pipelines that flow top-to-bottom, composable query fragments you can extract into functions and reuse, and Rust's type system catching schema errors at compile time. When your query depends on runtime conditions or maintainability matters as much as correctness, the builder pattern shines.

These trade-offs—when to choose SQL, when to choose the DataFrame-API, and how to mix them freely—are explored in [Two Paths to the Same Plan](#two-paths-to-the-same-plan-parser-vs-builder) and [Mixing SQL and DataFrames](#mixing-sql-and-dataframes).

---

## Architectural Fit: When to Use DataFusion

**The right tool for the right job—knowing DataFusion's sweet spot saves you from architectural dead-ends.**

DataFusion is a **query engine foundation** optimized for read-heavy, scan-oriented workloads over columnar data. This includes OLAP analytics, but also data lakehouse engines, ETL pipelines, and embedded query execution. The key distinction is **OLAP vs OLTP**:

| Aspect             | OLAP (DataFusion's strength) | OLTP (Consider alternatives) |
| ------------------ | ---------------------------- | ---------------------------- |
| **Pattern**        | Scan many rows, aggregate    | Find/update single rows      |
| **Data model**     | Immutable, append-only       | Mutable, transactional       |
| **Latency target** | Milliseconds-seconds OK      | Sub-millisecond required     |
| **Indexing**       | Column statistics, pruning   | B-tree, hash indexes         |

**DataFusion shines when:**

- Scanning and aggregating millions to billions of rows
- Building data lakehouse query layers (Parquet, Delta Lake, Iceberg)
- ETL and data transformation pipelines
- You need embeddable query execution (edge analytics, custom databases)
- Building domain-specific query engines on reusable infrastructure

**Consider alternatives when:**

| Use Case                    | Why DataFusion May Not Fit                 | Better Alternatives                                        |
| --------------------------- | ------------------------------------------ | ---------------------------------------------------------- |
| Single-row lookups by key   | Columnar format overhead; no index support | PostgreSQL, DynamoDB, Redis                                |
| Sub-millisecond latency     | Query planning overhead (~1-10ms minimum)  | Pre-compiled queries, KV stores                            |
| Heavy UPDATE/DELETE         | Designed for immutable, append-only data   | OLTP database, or lakehouse format (Delta, Iceberg) on top |
| Small datasets (<100K rows) | Works fine, but simpler APIs exist         | pandas, Polars (less setup)                                |
| Real-time streaming         | Batch-oriented execution model             | Kafka Streams, Flink, RisingWave                           |

> **Rule of thumb:** <br>
> "Find one row by ID" → use a database with indexes. <br>
> "Aggregate a billion rows" → use DataFusion.

**The OLAP sweet spot:** <br>
DataFusion is optimized for read-heavy analytical queries where you scan large amounts of data, filter aggressively, and aggregate results. If your workload involves frequent small writes, point lookups, or requires sub-millisecond response times, a different architecture is likely a better fit.

---
