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

# Creating DataFrames

**The "birth" phase of the DataFrame lifecycle: from data source to lazy query plan.**

Data lives everywhere—files on disk, tables in databases, streams over the
network, batches in memory. This section shows how DataFusion brings these
diverse sources into a single entry point, the [`SessionContext`], and from
there into a _lazy_ [`DataFrame`] that you can transform and execute
regardless of where the data originated.

:::{admonition} Style Note
:class: information
:collapsible: closed

In this document, all code elements are `highlighted`.

- DataFrame methods are written as `.method()` (e.g., `.select()`) to reflect the chaining syntax central to the API.
- standalone functions `method()` (e.g `col()`)
- static constructors `Struct::method()` (e.g., `SessionContext::new()`).
- Rust types are formatted as `TypeName` (e.g., `SchemaRef`).

**About the examples:**
Examples use [`assert_batches_eq!`] to verify outputs—you see both the code and its result. This pattern ensures examples stay correct as the API evolves.
:::

```{toctree}
:maxdepth: 1
:caption: Creating DataFrames Overview
:numbered:

from-files/index
registered-tables
from-sql
from-memory
inline-data
streaming
from-logical-plan
ecosystem-sources
```

## Creation Methods Overview

**Multiple data sources, one entry point: all creation methods flow through
[`SessionContext`] to produce the same lazy [`DataFrame`].**

Each method below produces the same lazy DataFrame. The differences are in
where your data lives and how much catalog integration you need.

**Choose based on where your data lives and how you'll access it:**

| Category               | Method                                    | Best for                                          | Prefer alternatives when                     |
| ---------------------- | ----------------------------------------- | ------------------------------------------------- | -------------------------------------------- |
| **Direct Read**        | [From Files](from-files/index.md)         | Ad-hoc analysis, ETL pipelines, one-off scripts   | You need stable names or multi-query reuse   |
| **DataFusion Catalog** | [Registered Tables](registered-tables.md) | SQL interoperability, shared schemas, multi-query | Simple one-shot queries                      |
| **SQL**                | [SQL Queries](from-sql.md)                | Complex joins, CTEs, window functions             | Dynamic logic, programmatic column selection |
| **Native**             | [RecordBatches](from-memory.md)           | Arrow Flight, IPC, single batch processing        | Multiple batches (use [`MemTable`] instead)  |
| **Testing**            | [Inline Data](inline-data.md)             | Unit tests, small hand-authored examples          | Production ingestion or large datasets       |
| **Streaming**          | [Streaming Sources](streaming.md)         | Unbounded data, real-time pipelines               | Bounded/batch workloads                      |
| **Advanced**           | [LogicalPlan](from-logical-plan.md)       | Custom DSLs, federation, optimizer testing        | Higher-level methods suffice                 |
| **Ecosystem**          | [External Sources](ecosystem-sources.md)  | Delta Lake, Iceberg, Lance, and more              | Core formats (Parquet, CSV, etc.) suffice    |

:::{admonition} Trade-off: Registration vs. Direct Read
:class: tip

- **Registration** caches schema and metadata in the catalog, making the
  source available to both SQL and the DataFrame API. Best for sources
  queried repeatedly or shared across pipelines. Requires a refresh
  strategy if underlying files change.
- **Direct reads** skip the catalog—metadata is derived on each call and the
  source is not discoverable via SQL. Best for one-off exploration or
  rapidly changing file paths.

**Rule of thumb:** Parquet, remote storage, or multi-file datasets →
register. Small, local, one-off analysis → direct read.
:::

## How Creation Works

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

Each provider reaches the [`SessionContext`] through one of two access
patterns: **ephemeral direct reads** (`.read_parquet()`, `.read_csv()`) that
return a DataFrame immediately without catalog registration, or **named
registration** (`.register_parquet()`, `.register_table()`) that stores the
provider in the catalog for repeated access by name. Both paths converge in
the [`SessionContext`], from which every DataFrame creation is triggered.

The following diagram visualizes this architecture:

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

- **Layer 1 — Data Sources**: Where bytes live—files in files, on S3, batches in
  memory, rows in databases, records in lakehouse tables.
- **Layer 2 — Table Providers**: Each source type has a dedicated
  [`TableProvider`] implementation that translates source-specific formats
  into Arrow batches. `ListingTable` and `MemTable` ship with DataFusion;
  custom implementations extend it to any source.
- **Layer 3 — Access Pattern**: Providers enter the session either
  ephemerally (direct read → immediate DataFrame) or by registration
  (named entry in the catalog for repeated SQL and DataFrame access).
- **Layer 4 — The Hub**: The [`SessionContext`] collects all providers,
  configuration, and runtime into one place.

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

## Getting Started

**Every creation method starts with a [`SessionContext`]—the entry point
that owns the catalog, configuration, and runtime for your session.**

The [`SessionContext`] exposes all DataFrame creation methods. These fall
into three categories:

| Category           | Methods                                                                                                             | Effect                         |
| ------------------ | ------------------------------------------------------------------------------------------------------------------- | ------------------------------ |
| **Direct read**    | [`.read_parquet()`], [`.read_csv()`], [`.read_json()`], [`.read_avro()`], [`.read_arrow()`], [`.read_batch()`], [`.read_table()`] | Returns a `DataFrame` immediately |
| **Registration**   | [`.register_parquet()`], [`.register_csv()`], [`.register_json()`], [`.register_table()`], [`.register_batch()`]    | Stores a `TableProvider` in the catalog |
| **Query**          | [`.sql()`], [`.table()`]                                                                                            | Returns a `DataFrame` from catalog or SQL |

The following example shows the minimal pattern—create a context, create a
DataFrame, register it for reuse, and query by name:

```rust
use datafusion::prelude::*;
use datafusion::error::Result;

#[tokio::main]
async fn main() -> Result<()> {
    // 1. Create a SessionContext
    let ctx = SessionContext::new();

    // 2. Create a DataFrame (here via SQL; any creation method works)
    let df = ctx.sql("SELECT 1 AS id, 'hello' AS greeting").await?;

    // 3. Register as a named table for reuse across queries
    ctx.register_table("greetings", df.into_view())?;

    // 4. Query the registered table — via DataFrame API or SQL
    let result = ctx.table("greetings").await?;
    result.show().await?;

    Ok(())
}
```

This pattern—create context, create or register data, query—is the skeleton
for every example in this guide. For SessionContext configuration (batch
size, parallelism, object stores), see
[SessionContext](../Concepts/sessioncontext.md). For how table names resolve
in the catalog, see
[Data Organization](registered-tables.md#understanding-datafusions-data-organization).

---

<!-- Link references -->
