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

# How DataFrame Creation Works

**From data source to lazy query plan: how [`SessionContext`],
[`TableProvider`], and [`LogicalPlan`] connect to produce a [`DataFrame`].**

Every DataFrame in DataFusion starts from a data source — a file, an
in-memory batch, a database, or a lakehouse table — and ends as a lazy
[`DataFrame`] backed by a [`LogicalPlan`]. This page explains the
architecture that makes this work: the [`TableProvider`] trait, the two
access patterns (direct read vs. registration), and the [`SessionContext`]
that ties everything together.

```{contents} Table of Contents
:local:
:depth: 2
```

## Table Providers

**All creation paths route through [`SessionContext`], which resolves
sources into a [`LogicalPlan`] and returns a lazy [`DataFrame`].**

Data sources connect to DataFusion through implementations of the
[`TableProvider`] trait—the universal interface for any data source.
DataFusion ships two built-in providers, and you can implement your own:

1. [`ListingTable`] **(built-in)** handles file-based sources (Parquet, CSV,
   JSON, Avro, Arrow IPC). It manages path resolution, schema inference,
   partition discovery, and predicate pushdown against file metadata, so the
   query engine never reads more data than necessary.

2. [`MemTable`] **(built-in)** wraps in-memory Arrow [`RecordBatch`]es with
   zero-copy access and no serialization overhead—ideal for data already in
   the Arrow ecosystem (Flight, IPC, or computed results).

3. **Custom** [`TableProvider`] implementations bridge everything else—OLTP
   databases, lakehouse formats like Iceberg and Delta Lake, REST APIs—
   translating native protocols into Arrow batches with optional pushdown.
   For a step-by-step guide, see
   [Custom Table Providers](../../custom-table-providers.md).

## Access Patterns

Each provider reaches the [`SessionContext`] through one of two access
patterns: **ephemeral direct reads** (`.read_parquet()`, `.read_csv()`) that
return a DataFrame immediately without catalog registration, or **named
registration** (`.register_parquet()`, `.register_table()`) that stores the
provider in the catalog for repeated access by name. Both paths converge in
the [`SessionContext`], from which every DataFrame creation is triggered.

## Architecture

The following diagram visualizes the full creation pathway:

```text
DATAFRAME CREATION PATHWAYS
════════════════════════════════════════════════════════════════════════════

[ 1. DATA SOURCES ]                    (Where the data lives)
┌───────────────────┐ ┌───────────────────┐ ┌─────────────────────────────┐
│   Files / Stores  │ │   In-Memory       │ │  External & Ecosystem       │
│ (Parquet, CSV,    │ │ (Arrow Batches,   │ │ (Databases, Lakehouse,      │
│  JSON, Avro, IPC) │ │  Flight, IPC)     │ │  Delta, Iceberg, APIs, ...) │
└─────────┬─────────┘ └─────────┬─────────┘ └──────────────┬──────────────┘
          │                     │                          │
          ▼                     ▼                          ▼
[ 2. TABLE PROVIDERS ]               (impl TableProvider trait)
┌───────────────────┐ ┌───────────────────┐ ┌─────────────────────────────┐
│   ListingTable    │ │     MemTable      │ │   Custom TableProvider      │
│    (built-in)     │ │    (built-in)     │ │ (user or ecosystem impl)    │
│                   │ │                   │ │                             │
│ Path resolution,  │ │ Zero-copy Arrow   │ │ Any source that implements  │
│ schema inference, │ │ batch access      │ │ the TableProvider trait     │
│ partition disc.   │ │                   │ │                             │
└─────────┬─────────┘ └─────────┬─────────┘ └──────────────┬──────────────┘
          │                     │                          │
          └─────────────────────┼──────────────────────────┘
                                │
[ 3. ACCESS PATTERN ]           │   (How you introduce it to the Session)
            ┌───────────────────▼───────────────────┐
            │                                       │
   ┌────────▼─────────┐                    ┌────────▼─────────┐
   │  A. DIRECT READ  │                    │   B. REGISTER    │
   │  (Ephemeral)     │                    │   (Named)        │
   │                  │                    │                  │
   │ read_parquet()   │                    │ register_parquet │
   │ read_csv()       │                    │ register_csv     │
   │ read_batch()     │                    │ register_table   │
   │ read_json()      │                    │                  │
   └────────┬─────────┘                    └────────┬─────────┘
            │                                       │
            │ (Returns DataFrame)                   │ (Stored in Catalog)
            ▼                                       ▼
[ 4. THE HUB ]                                [ CATALOG ]
┌───────────────────────────────────────────────────▼──────────────────┐
│                           SessionContext                             │
│ ┌──────────────────────────────────────────────────────────────────┐ │
│ │SessionState: Config · RuntimeEnv · Optimizer · Planner · Catalog │ │
│ │                                                                  │ │
│ │ ┌─────────────────┐                 ┌────────────────────────┐   │ │
│ │ │ Ephemeral Plan  │                 │ Registered Providers   │   │ │
│ │ └─────────────────┘                 │ "sales", "metrics"...  │   │ │
│ │                                     └────────────────────────┘   │ │
│ └──────────┬──────────────────────────────────┬────────────────────┘ │
└────────────┼──────────────────────────────────┼──────────────────────┘
             │                                  │
             │ (Direct Return)                  │ (table("sales"))
             │                                  │ (sql("SELECT..."))
             ▼                                  ▼
   ┌─────────────────────────────────────────────────────────────┐
   │                         DATAFRAME                           │
   │                  (Immutable, Lazy Handle)                   │
   ├─────────────────────────────────────────────────────────────┤
   │ 1. LogicalPlan: Relational operations (Filter, Join, ...)   │
   │ 2. SessionState: Immutable snapshot of context at creation  │
   └────────────────────────────┬────────────────────────────────┘
                                │
                                ▼
                        [ EXECUTION PATH ]
              (Optimizers ➔ Physical Plan ➔ Async Stream)
```

**Reading the diagram top-to-bottom:**

::::::{admonition} Reading the diagram top-to-bottom
:class: seealso

:::{admonition} **Layer 1 — Data Sources**
:class: note

**Layer 1 — Data Sources**: Where bytes live — files on disk, on S3,
batches in memory, rows in databases, records in lakehouse tables.
:::

:::{admonition} **Layer 2 — Table Providers**
:class: note
Each source type has a dedicated [`TableProvider`] implementation that translates source-specific formats into Arrow batches. `ListingTable` and `MemTable` ship with DataFusion; custom implementations extend it to any source.
:::

:::{admonition} **Layer 3 — Access Pattern**
:class: note
Providers enter the session either ephemerally (direct read → immediate DataFrame) or by registration (named entry in the catalog for repeated SQL and DataFrame access).
:::

:::{admonition} **Layer 4 — The Hub**
:class: note
The [`SessionContext`] collects all providers, configuration, and runtime into one place.
:::

::::::

## From Plan to Execution

When a DataFrame is created, DataFusion captures a [`SessionState`]
snapshot—an immutable copy of the catalog, configuration, and runtime—and
pairs it with a [`LogicalPlan`] describing the requested operations. The
resulting [`DataFrame`] is lazy and immutable: a framework around your data,
not the data itself.

Two equivalent APIs manipulate the lazy DataFrame: the programmatic
**DataFrame API** (`.filter()`, `.select()`, `.aggregate()`) and **SQL**
(`ctx.sql("SELECT ...")`). Both produce the same optimized plan—the choice
is ergonomics, not performance. For a detailed comparison, see
[Builder vs. Parser](../Concepts/builder-parser.md).

Execution is triggered by actions like [`.collect()`] or [`.show()`].
DataFusion's optimizer rewrites the logical plan, the physical planner maps
it to parallel tasks, and an async stream of Arrow [`RecordBatch`]es flows
through Rust's memory-safe runtime—delivering optimized, concurrent results.

---

<!-- Link references -->
