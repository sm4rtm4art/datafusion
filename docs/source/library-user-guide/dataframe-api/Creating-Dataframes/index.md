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
-->s

<!-- TODO: Migration Checklist

- [ ] **Migrate Intro:** Move the "Introduction" and "The "birth" phase..." text here.
- [ ] **Migrate Philosophy:** Move "The Philosophy of Convergence" here.
- [ ] **Migrate Architecture:** Move "Architecture: From data source to a lazy plan" (including the ASCII diagram).
- [ ] **Create Navigation:** Add a list/table of links to the other files in this directory (`from-files.md`, `from-sql.md`, etc.).
- [ ] **Clean up:** Remove specific code examples that belong in the sub-pages (keep high-level concepts only).

-->

# Creation of DataFusion DataFrames

**The "birth" phase of the DataFrame lifecycle: from data source to lazy query plan.**

Every query starts with data. Whether you're reading Parquet from S3, executing SQL, receiving Arrow batches from a Flight stream, or constructing plans programmatically—all paths converge to a lazy [`DataFrame`] backed by a [`LogicalPlan`]. This guide covers the _when_ and _how_ of each creation method.

In the [DataFrame lifecycle](./index.md#the-dataframe-lifecycle), creation is where you bind a data source to a query plan. The DataFrame doesn't execute yet—it's a recipe waiting to run. For the conceptual model, see [Concepts](./concepts.md). For what happens next: [Transform](./transformations.md) → [Write](./writing-dataframes.md).

:::{admonition} Style Note
:class: note

In this document, all code elements are highlighted with backticks.

- DataFrame methods are written as `.method()` (e.g., `.select()`) to reflect the chaining syntax central to the API.
- standalone functions `method()` (e.g `col()`)
- static constructors `Struckt::method()` (e.g., `SessionContext::new()`).
- Rust types are formatted as `TypeName` (e.g., `SchemaRef`).

:::

```{contents}
:local:
:depth: 2
```

```{toctree}
:maxdepth: 2

catalog-and-context
from-files
from-sql
in-memory
session-configuration
streaming
extension-links
```

## Introduction

**DataFusion provides multiple pathways to create a DataFrame—each optimized for different data sources, yet all converging on a single, powerful abstraction.**

To understand _how_ to create a DataFrame, we must first understand _what_ we are creating. DataFusion is not just a library for reading files; it is a high-performance query engine built on the **Apache Arrow** columnar format. It brings the safety and concurrency of **Rust** to analytical workloads.

**This guide covers:**

| Creation Method            | Best For                          | Section                                                     |
| -------------------------- | --------------------------------- | ----------------------------------------------------------- |
| **From Files**             | Production data, cloud storage    | [Section 1](#1-from-files)                                  |
| **From Registered Tables** | Reuse across queries, SQL interop | [Section 2](#2-from-a-registered-table)                     |
| **From SQL**               | Complex relational logic, CTEs    | [Section 3](#3-from-sql-queries)                            |
| **From RecordBatches**     | Arrow ecosystem, zero-copy        | [Section 4](#4-from-arrow-recordbatches-the-native-pathway) |
| **From Inline Data**       | Tests, examples, prototyping      | [Section 5](#5-from-inline-data-using-the-dataframe-macro)  |
| **From Custom Sources**    | External DBs, APIs, streaming     | [Advanced Topics](dataframes-advance.md#tableprovider)      |

> **About the examples** <br>
> Examples use [`assert_batches_eq!`] to verify outputs—you see both the code and its result. This pattern ensures examples are tested and teaches DataFusion's behavior.

### The Philosophy of Convergence

**All creation methods normalize to the same lazy `LogicalPlan`, regardless of whether you start with SQL or the DataFrame API.**

Whether you read a CSV from disk, stream Arrow batches from a network socket, or parse a SQL query, DataFusion normalizes them all into the same structure: a lazy [`DataFrame`] backed by a [`LogicalPlan`].

- **The Universal Adapter**:<br>
  The DataFrame API decouples _storage_ from _compute_. You can join a Parquet file from S3 with an in-memory Arrow batch and a PostgreSQL table (via [`TableProvider`]) in a single query.
- **The "Lazy" Contract**:<br>
  Creating a DataFrame is a **plan-first operation**. When you call [`.read_parquet()`], DataFusion typically reads only metadata (schema/statistics), not record batches. This means you can usually define DataFrames over datasets far larger than memory—_as long as you don’t execute the plan_.
- **Safety by Design**: <br>
  Leveraging Rust’s ownership model, DataFrames are **immutable**. Transformations (like [`.filter()`] or [`.select()`]) return a new [`DataFrame`] that shares the underlying plan structure, ensuring your query definitions are side-effect free and thread-safe.

### Architecture: From data source to a lazy plan

**All DataFrame creation paths route through `SessionContext`, which resolves sources (via `TableProvider`s) into a single `LogicalPlan`.**

Creating a DataFrame is the _binding step_: DataFusion combines a data source reference with the current session environment ([`SessionContext`]) and returns a lazy [`DataFrame`]. Internally, a [`DataFrame`] is two things: a `SessionState` snapshot (catalog + configuration + runtime environment + function registry) and a [`LogicalPlan`] that describes what to compute.

This design keeps the session configurable while making transformations pure plan-building: you can keep registering tables, object stores, and UDFs in `SessionContext`, but once you have a `DataFrame` you only change the plan (via `.filter()`, `.select()`, etc.) until you execute it with an action.

The diagram below shows that no matter which entry point you choose, creation converges through the same abstractions.

```text
DATAFRAME CREATION PATHWAYS
────────────────────────────────────────────────────────────────────────────

[ SOURCES ]                      (All roads lead to TableProvider)
┌──────────────┐ ┌──────────────┐ ┌──────────────┐ ┌─────────────────────┐
│ Files/Stores │ │ In-Memory    │ │ External DBs │ │ Extensions / Formats│
│(Parquet/CSV) │ │ (Batches)    │ │ & Streaming  │ │ (Iceberg, Delta,...)│
└──────┬───────┘ └──────┬───────┘ └──────┬───────┘ └──────────┬──────────┘
       ▼                ▼                ▼                    ▼
┌──────────────┐ ┌───────────────┐┌──────────────┐ ┌─────────────────────┐
│ ListingTable │ │   MemTable    ││ Custom Table │ │ CatalogProvider /   │
│ (File Scan + │ │ (In-Memory    ││ Provider     │ │ SchemaProvider ➔    │
│ Statistics)  │ │  Batches)     ││ (Pushdown*)  │ │ TableProvider(s)    │
└──────┬───────┘ └──────┬────────┘└──────┬───────┘ └──────────┬──────────┘
       └────────────────┴───────┬────────┴────────────────────┘
                                │ (Registered / Resolved)
                                │
[ THE HUB ]                     ▼
┌──────────────────────────────────────────────────────────────────────┐
│                            SessionContext                            │
│ ┌──────────────────────────────────────────────────────────────────┐ │
│ │                           SessionState                           │ │
│ │Catalog/Schema · Config · RuntimeEnv · UDFs · Optimizer · Planner │ │
│ └────────────────────────────┬─────────────────────────────────────┘ │
└──────────────────────────────┼───────────────────────────────────────┘
                                │
                    Resolves Tables/Functions/Types
                                │
                ┌───────────────┴───────────────┐
                ▼                               ▼
        ┌───────────────┐               ┌───────────────┐
        │    SQL API    │               │ DataFrame API │
        ├───────────────┤               ├───────────────┤
        │ ctx.sql("..") │               │ ctx.table(..) │
        └───────┬───────┘               └───────┬───────┘
                │ parse                         │ build plan
                ▼  + plan                       ▼
         ┌════════════════════════════════════════════┐
         ║      LogicalPlan (Same Representation)     ║
         ╘══════════════════════╤═════════════════════┘
                                │
                                ▼
                 ┌─────────────────────────────┐
                 │          DataFrame          │
                 │  (Immutable, Lazy Handle)   │
                 ├─────────────────────────────┤
                 │ 1. LogicalPlan              │
                 │ 2. SessionState (snapshot)  │
                 └──────────────┬──────────────┘
                                │
    ════════════════════════════╪════════════════════════════════
                                │          ACTIONS (.collect/etc.)
                                ▼
                 ┌─────────────────────────────┐
                 │   Planning + Optimization   │
                 │ (Logical ➔ Physical ➔ Exec) │
                 └──────────────┬──────────────┘
                                │
                                ▼
                    [ RecordBatchStream Output ]
```

**How to read this diagram:** <br>

- **Sources → TableProvider**:<br>
  Every entry point becomes a [`TableProvider`] (files via [`ListingTable`], in-memory via [`MemTable`], extensions via their own providers).
- **Mutable session vs. per-query snapshot**:<br>
  [`SessionContext`] is mutable (register tables/UDFs/object stores, change config). When you create a [`DataFrame`], DataFusion captures a [`SessionState`] snapshot for that query; later changes to [`SessionContext`] do not affect that [`DataFrame`].
- **SQL parser vs DataFrame builder**:<br>
  The SQL API ([`ctx.sql(...)`][`.sql()`]) parses text into a [`LogicalPlan`] and returns a [`DataFrame`] (using the catalog to resolve names like `FROM table_name`, which is why registration matters). The DataFrame API builds the same kind of plan programmatically. Both paths converge on the same [`DataFrame`] abstraction.
- **Transformations vs actions**:<br>
  Transformations return a new [`DataFrame`] (updated plan, same [`SessionState`]). Actions such as [`.collect()`] and [`.execute_stream()`] execute the plan and produce results.

**Integration points (what you can affect):** <br>

- **Pushdown is provider-dependent:** <br>
  Some [`TableProvider`] implementations can apply filters/projections at the source; others apply them inside DataFusion.
- **Extension hook:** <br>
  Implement [`TableProvider`] to integrate custom sources (Kafka, Delta Lake, proprietary APIs) and register them in [`SessionContext`].<br>
  See [Advanced Topics](dataframes-advance.md#tableprovider).

## Before Creating a DataFrame

**Before you pick a creation method, understand how `SessionContext` organizes and resolves data sources.**

DataFusion is an "out of the box" query engine, but for a working query engine and optimal results _query engines have rules_: data sources must be registered or scanned, names must be resolved, and schemas must align. This section covers the catalog model that makes these rules work.

[`SessionContext`] is the entry point for creating DataFrames—it owns the catalog (registered tables), configuration, and runtime. When you create a [`DataFrame`], it captures a snapshot of this state as [`SessionState`], which is why SQL and the DataFrame API seamlessly interoperate.

> **Already familiar with DataFusion's catalog?** <br>
> Skip to [How to create a DataFrame](#how-to-create-a-dataframe).<br>
> For architecture, see [Concepts](concepts.md).

### Understanding DataFusion's Data Organization

**`SessionContext` is your session-local catalog: it maps names to `TableProvider`s.**

Tables live under a three-level hierarchy (**Catalog → Schema → Table**), which keeps queries readable and enables SQL interoperability and metadata discovery.

DataFusion always has a default catalog (`datafusion`) and schema (`public`). When you register a table, you choose its name (for example `"sales"` or `"warehouse.analytics.metrics"`), and that name determines where the [`TableProvider`] is stored. The following schema illustrates the default namespace with "sales" as target table.

```text
SessionContext
└── Catalog ("datafusion")
    └── Schema ("public")              ← Namespace schema (organizes tables)
        └── Table ("sales")
            └── Arrow Schema           ← Data schema (columns + types)
```

> **Disambiguation: "Schema" has two meanings in DataFusion**
>
> 1. **Namespace schema** (like PostgreSQL): A container for organizing tables (e.g., `"public"`)
> 2. **Arrow schema**: The columns and types of a table (e.g., `id: Int32, name: Utf8`)
>
> ```text
> ctx.table("public.sales").await?;   // "public" = namespace schema
> let arrow_schema = df.schema();      // Arrow schema: columns + types
> ```

**Adopting this structure offers three key advantages:**

- **Performance**:<br>
  When you register a table (e.g., a Parquet file), DataFusion analyzes its schema and metadata once. Every subsequent query that uses that table is faster because it skips this expensive step.
- **Clarity**:<br>
  Instead of passing file paths around your code, you refer to data with logical names like `"sales"` or `"fact_orders"`. This makes your queries cleaner and easier to maintain.
- **Interoperability**:<br>
  A registered table is available to both the DataFrame API and SQL. You can register a source with one API and immediately query it from the other.

The core pattern is simple: **register once, query many times**.

```text
ctx.register_parquet("sales", "data/sales/", ParquetReadOptions::default()).await?;

let sales_df = ctx.table("sales").await?; // DataFrame API

let sales_sql = ctx.sql("SELECT * FROM sales").await?;  // SQL API
```

#### How Names Resolve

DataFusion resolves table names in both SQL (`FROM ...`) and the DataFrame API (`ctx.table(...)`) using **1-, 2-, or 3-part identifiers**:

| Identifier                    | Resolves to                  | Use case              |
| ----------------------------- | ---------------------------- | --------------------- |
| `"sales"`                     | `datafusion.public.sales`    | Default (most common) |
| `"analytics.sales"`           | `datafusion.analytics.sales` | Custom schema         |
| `"warehouse.analytics.sales"` | Fully qualified              | Multi-catalog setups  |

- **Default namespace:**<br>
  Unqualified names resolve to `datafusion.public` (default catalog + schema). This is a namespace convention, not an access-control boundary.
- **Lifetime:**<br>
  Registrations are in-memory, scoped to the [`SessionContext`]. For persistence, implement a custom [`CatalogProvider`].
- **Case sensitivity:**<br>
  Unquoted identifiers fold to lowercase; quote to preserve case (`"Sales"`).

> **Cloud storage (Rust):** <br>
> To use `s3://`, `gs://`, or `az://` URLs, register an object store in the runtime environment. The CLI guide shows SQL configuration; for the Rust API see [`datafusion::datasource::object_store`](https://docs.rs/datafusion/latest/datafusion/datasource/object_store/index.html). For a complete S3 setup example (credentials + registration + query), see [`datafusion-examples/examples/external_dependency/main.rs`](https://github.com/apache/datafusion/blob/main/datafusion-examples/examples/external_dependency/main.rs).

> **Performance tip:** <br>
> Registering a table caches schema/metadata (especially valuable for Parquet footers), so repeated queries plan faster.

> **Going deeper?** <br>
> This section covers what you need to create DataFrames. For advanced catalog topics (custom `CatalogProvider`, persistent catalogs, dynamic schema discovery), see the [Catalogs Guide](../catalogs.md). For integrating external data sources, see [Custom Table Providers](../custom-table-providers.md).

<details>
<summary><strong>Example: Fully-qualified namespace setup</strong></summary>

Use multi-part names like `warehouse.analytics.metrics` to separate domains or environments:

```rust
use std::sync::Arc;

use datafusion::assert_batches_eq;
use datafusion::catalog::{CatalogProvider, MemoryCatalogProvider, MemorySchemaProvider};
use datafusion::dataframe;
use datafusion::error::Result;
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> Result<()> {
    let ctx = SessionContext::new();

    // 1. Register a catalog + schema: warehouse.analytics
    let warehouse_catalog = Arc::new(MemoryCatalogProvider::new());
    ctx.register_catalog("warehouse", warehouse_catalog.clone());
    warehouse_catalog.register_schema("analytics", Arc::new(MemorySchemaProvider::new()))?;

    // 2. Register a table into that namespace: warehouse.analytics.metrics
    let metrics_df = dataframe!(
        "metric" => ["latency"],
    )?;
    ctx.register_table("warehouse.analytics.metrics", metrics_df.into_view())?;

    // 3. Resolve the fully-qualified name and execute
    let results = ctx.table("warehouse.analytics.metrics").await?.collect().await?;
    assert_batches_eq!(
        &[
            "+---------+",
            "| metric  |",
            "+---------+",
            "| latency |",
            "+---------+",
        ],
        &results
    );

    Ok(())
}
```

</details>

### References

**DataFusion Documentation:**

- [Catalogs Guide](../catalogs.md) — Deep dive into catalog hierarchy and custom providers
- [Custom Table Providers](../custom-table-providers.md) — Implementing your own data sources
- [`SessionContext` API](https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html) — Entry point for all DataFrame operations
- [`TableProvider` trait](https://docs.rs/datafusion/latest/datafusion/datasource/trait.TableProvider.html) — Interface for custom data sources

**Arrow Ecosystem:**

- [Arrow Schema](https://docs.rs/arrow-schema/latest/arrow_schema/struct.Schema.html) — Column names and types definition
- [Apache Arrow Format](https://arrow.apache.org/docs/format/Columnar.html) — Columnar memory layout

**Background Reading:**

- [Apache DataFusion: A Fast, Embeddable, Modular Analytic Query Engine (Section 5.2)][sigmod-paper]
