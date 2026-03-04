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

# Architectural Overview

**Where the DataFrame API lives in DataFusion's architecture — and when it's the right tool for the job.**

DataFusion is an embeddable, modular query engine built on Apache Arrow. It provides a complete analytical query pipeline — from catalog and data source integration through planning, optimization, and parallel execution — that you integrate into your own application rather than deploying as a standalone database. This section orients you within the architecture before diving into the individual components.

```{contents} Table of Contents for Architectural Overview
:local:
:depth: 2
```

---

## Design Philosophy

**DataFusion a modern OLAP query engine with a `Boring` but battle tested architecture.**

DataFusion's architecture follows three guiding principles (see [SIGMOD 2024 Paper] Section 5.1):

1. **Work "out of the box"** — Provide a very fast, world-class query engine with minimal setup or required configuration.
2. **Customizable everything** — All behavior should be customizable by implementing traits (`TableProvider`, `OptimizerRule`, `ExecutionPlan`, etc.).
3. **Architecturally boring** — Follow industrial best practice rather than trying cutting-edge, but unproven, techniques.

With these principles, users start with a basic, high-performance engine and specialize it over time to suit their needs and available engineering capacity.

---

## Data Flow: From Sources to DataFrame

**DataFusion is highly adaptable by connecting multiple sources with default or custom TableProviders.**

In modern data architectures, information resides across multiple systems and formats. DataFusion acts as a central query engine designed to process this distributed data efficiently. It natively reads standard file formats (Parquet, CSV, JSON, Avro) and integrates seamlessly with the Apache Arrow in-memory format.

Beyond native files, DataFusion is highly extensible. Through the `TableProvider` trait, developers can connect the engine to external databases (via JDBC/ODBC extensions), REST APIs, or modern data lakehouse formats (Iceberg, Delta Lake). External systems can even pre-filter data at the source — a concept known as predicate pushdown — before delivering it to DataFusion. Once ingested, all data is processed in DataFusion's highly optimized, columnar Arrow framework.

The following diagram traces how data enters DataFusion — from diverse sources through `TableProvider`s and `SessionContext`, branching into two APIs that converge into one `DataFrame`:

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
[ API's ]          ▼                                ▼
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

DataFusion exposes the same query engine through **two equivalent interfaces**: the SQL API (parser-based) and the DataFrame API (builder-based). Both compile to the same `LogicalPlan`, receive identical optimizations, and execute with the same performance. For a detailed side-by-side comparison, see [Builder vs. Parser](builder-parser.md).

---

## Architectural Fit: OLAP vs. OLTP

**The right tool for the right job — knowing DataFusion's sweet spot saves you from architectural dead-ends.**

DataFusion is designed as an embeddable, modular OLAP query engine — not a standalone database, but the analytical backbone you integrate into your own application. It is optimized for read-heavy, scan-oriented workloads over columnar data: analytics, data lakehouse engines, ETL pipelines, and embedded query execution.

**DataFusion shines when:**

- Scanning and aggregating millions to billions of rows.
- Building data lakehouse query layers (Parquet, Delta Lake, Iceberg).
- Executing ETL and data transformation pipelines.
- Building domain-specific query engines on reusable infrastructure.

**Consider alternatives when:**

| Use Case                    | DataFusion May Not Fit                     | Better Alternatives                |
| :-------------------------- | :----------------------------------------- | :--------------------------------- |
| **Single-row lookups**      | Columnar format overhead; no index support | PostgreSQL, DynamoDB, Redis        |
| **Sub-millisecond latency** | Query planning overhead (~1-10ms minimum)  | Pre-compiled queries, KV stores    |
| **Heavy UPDATE/DELETE**     | Designed for immutable, append-only data   | OLTP database, or lakehouse format |
| **Small datasets (<100K)**  | Works fine, but simpler APIs exist         | pandas, Polars (less setup)        |
| **Real-time streaming**     | Batch-oriented execution model             | Kafka Streams, Flink, RisingWave   |

:::{note} The OLAP sweet spot:
DataFusion is optimized for read-heavy analytical queries where you scan large amounts of data, filter aggressively, and aggregate results. If your workload involves frequent small writes, point lookups, or requires sub-millisecond response times, a different architecture is likely a better fit.
:::

---

Everything is configured and set in place in the `SessionContext` — catalogs, table providers, UDFs, and runtime configuration all live there. Continue to [SessionContext](sessioncontext.md) for the full picture.

---
