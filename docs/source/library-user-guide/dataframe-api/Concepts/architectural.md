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

<!--TODO: Write a general overview of the architectur and its fit-->

# Architectural Fit: When to Use DataFusion

**The right tool for the right job — knowing DataFusion's sweet spot saves you from architectural dead-ends.**

Before diving into the DataFrame API, it helps to understand _where_ DataFusion excels and where a different tool is the better choice. DataFusion is a **query engine foundation** optimized for read-heavy, scan-oriented workloads over columnar data — OLAP analytics, data lakehouse engines, ETL pipelines, and embedded query execution. The key distinction is OLAP versus OLTP:

```{contents} Table of Contents for Architectural Fit
:local:
:depth: 2
```

---

## OLAP vs. OLTP

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

---

## Consider Alternatives When

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

---

## DataFrame API vs. SQL API — A Fair Comparison

DataFusion exposes the same query engine through **two equivalent interfaces**: SQL and the programmatic DataFrame API. Both compile to the same `LogicalPlan`, receive identical optimizations, and execute with the same performance. The choice is about ergonomics and use case, not capability:

| Aspect                | SQL API                                      | DataFrame API                                 |
| :-------------------- | :------------------------------------------- | :-------------------------------------------- |
| **Syntax**            | Declarative SQL strings                      | Rust builder pattern (method chaining)        |
| **Validation timing** | Parse-time (string → plan)                   | Compile-time (Rust type system)               |
| **Composability**     | Subqueries, CTEs                             | Function composition, variables, control flow |
| **Dynamic queries**   | String interpolation (SQL injection risk)    | Programmatic `Expr` construction (type-safe)  |
| **Ideal for**         | Ad-hoc analytics, BI tools, SQL-fluent users | Embedded engines, pipelines, Rust-native apps |

Both APIs are first-class citizens in DataFusion. For a detailed side-by-side comparison with code examples, see [Builder vs. Parser](builder-parser.md).

---

## Where DataFusion Fits in Your Stack

DataFusion is not a standalone database — it's an **embeddable query engine** you integrate into your application. Understanding this distinction helps you architect your system correctly:

```text
┌────────────────────────────────────────────────────────┐
│                   YOUR APPLICATION                      │
│  (Rust binary, Python service, gRPC server, CLI tool)  │
├────────────────────────────────────────────────────────┤
│                                                        │
│    ┌──────────────────────────────────────────────┐    │
│    │               DATAFUSION                     │    │
│    │  ┌───────────┐  ┌────────────┐  ┌─────────┐ │    │
│    │  │ SQL API   │  │ DataFrame  │  │ Catalog │ │    │
│    │  │           │  │    API     │  │         │ │    │
│    │  └─────┬─────┘  └─────┬──────┘  └────┬────┘ │    │
│    │        └──────────────┼───────────────┘      │    │
│    │                       ▼                      │    │
│    │              LogicalPlan → Optimizer          │    │
│    │                       ▼                      │    │
│    │              ExecutionPlan → Tokio            │    │
│    │                       ▼                      │    │
│    │              RecordBatches (Arrow)            │    │
│    └──────────────────────────────────────────────┘    │
│                                                        │
├────────────────────────────────────────────────────────┤
│                     DATA LAYER                          │
│   (Parquet, CSV, JSON, Delta Lake, Iceberg, S3, ...)   │
└────────────────────────────────────────────────────────┘
```

> **Next:** Now that you know _when_ DataFusion fits, continue to [SessionContext](sessioncontext.md) to learn how to set up the execution environment.

---
