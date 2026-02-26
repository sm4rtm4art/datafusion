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

# Creation of DataFusion DataFrames

**The "birth" phase of the DataFrame lifecycle: from data source to lazy query plan.**

Every query starts with data. Whether you're reading Parquet from S3, executing SQL, receiving Arrow batches from a Flight stream, or constructing plans programmatically—all paths converge to a lazy [`DataFrame`] backed by a [`LogicalPlan`]. This guide covers the _when_ and _how_ of each creation method.

In the [DataFrame lifecycle](./index.md#the-dataframe-lifecycle), creation is where you bind a data source to a query plan. The DataFrame doesn't execute yet—it's a recipe waiting to run. For the conceptual model, see [Concepts](./concepts.md). For what happens next: [Transform](./transformations.md) → [Write](./writing-dataframes.md).

```{admonition} Style Note
---
:class: note
:collapsible: closed

---

In this document, all code elements are highlighted with backticks.

- DataFrame methods are written as `.method()` (e.g., `.select()`) to reflect the chaining syntax central to the API.
- standalone functions `method()` (e.g `col()`)
- static constructors `Struckt::method()` (e.g., `SessionContext::new()`).
- Rust types are formatted as `TypeName` (e.g., `SchemaRef`).
```

```{contents}
:local:
:depth: 2
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

---

## How to Create a DataFrame

**DataFusion binds to data wherever it lives—multiple entry points, one destination—unifying files, in-memory batches, and custom sources (via [`TableProvider`]) into a single, lazy DataFrame.**

The [previous section](#before-creating-a-dataframe) established how DataFusion organizes data: <br>
[`SessionContext`] owns the catalog, tables are accessed through [`TableProvider`], and names resolve through the catalog hierarchy. Now we put that foundation to work.

Creating a DataFrame constructs a [`LogicalPlan`] describing _what_ to compute. This may involve reading metadata or inferring schema (see [What happens immediately?](#1-from-files)), but actual record batch scanning happens only when you call an execution methode (i.e `.collect()`, `.show()`).

Because every creation method produces a [`DataFrame`] backed by the same internal representation ([`LogicalPlan`] + [`SessionState`]), you can:

- Read files directly for ad-hoc analysis
- Register tables for SQL interoperability
- Mix both approaches in the same pipeline

**Choose based on where your data lives and how you'll access it:**

| Category        | Method                                                             | Best for                                          | Prefer alternatives when                     |
| --------------- | ------------------------------------------------------------------ | ------------------------------------------------- | -------------------------------------------- |
| **Direct Read** | [1. Files](#1-from-files)                                          | Ad-hoc analysis, ETL pipelines, one-off scripts   | You need stable names or multi-query reuse   |
| **Catalog**     | [2. Registered Table](#2-from-a-registered-table)                  | SQL interoperability, shared schemas, multi-query | Simple one-shot queries                      |
| **SQL**         | [3. SQL Queries](#3-from-sql-queries)                              | Complex joins, CTEs, window functions             | Dynamic logic, programmatic column selection |
| **Native**      | [4. RecordBatches](#4-from-arrow-recordbatches-the-native-pathway) | Arrow Flight, IPC, single batch processing        | Multiple batches (use [`MemTable`] instead)  |
| **Testing**     | [5. Inline Data](#5-from-inline-data-using-the-dataframe-macro)    | Unit tests, small hand-authored examples          | Production ingestion or large datasets       |
| **Advanced**    | [6. LogicalPlan](#6-advanced-constructing-from-a-logicalplan)      | Custom DSLs, federation, optimizer testing        | Higher-level methods (1–5) suffice           |

> **Trade-offs:**
>
> - **Registration:** <br> Upfront metadata reads; requires refresh strategy if files change out-of-band.
> - **Direct reads:** <br> No catalog state; metadata re-derived per call; not discoverable via SQL.
>
> **Default rule:** <br> Parquet, remote storage, or multi-file → register. Small, local, one-off → direct read.

## The Big Picture

For a more detailes, visual representation of the DataFrame creation process, see the diagram below:

```text
DATAFRAME CREATION PATHWAYS
════════════════════════════════════════════════════════════════════════════

[ 1. DATA SOURCES ]              (Where the data lives)
┌──────────────┐ ┌──────────────┐ ┌──────────────┐ ┌───────────────────────┐
│ Files/Stores │ │ In-Memory    │ │ External DBs │ │ Iceberg, Delta, etc.  │
│(Parquet/CSV) │ │ (Batches)    │ │ (Custom)     │ │ (via extensions)      │
└──────┬───────┘ └──────┬───────┘ └──────┬───────┘ └──────────┬────────────┘
       │                │                │                    │
       ▼                │                ▼                    ▼
[ 2. TABLE PROVIDERS ]  ▼          (impl TableProvider trait)
┌──────────────┐ ┌──────────────┐ ┌──────────────┐ ┌───────────────────────┐
│ ListingTable │ │   MemTable   │ │ Custom Table │ │ Extension-provided    │
│ (File Scan)  │ │  (Batches)   │ │ Provider     │ │ TableProvider         │
└──────┬───────┘ └──────┬───────┘ └──────┬───────┘ └──────────┬────────────┘
       │                │                │                    │
       └────────────────┴────────┬───────┴────────────────────┘
                                 │
[ 3. ACCESS PATTERN ]            │   (How you introduce it to the Session)
            ┌────────────────────▼────────────────────┐
            │                                         │
   ┌────────▼─────────┐                      ┌────────▼─────────┐
   │  A. DIRECT READ  │                      │   B. REGISTER    │
   │  (Ephemeral)     │                      │   (Named)        │
   │                  │                      │                  │
   │ read_parquet()   │                      │ register_parquet │
   │ read_csv()       │                      │ register_csv     │
   │ read_batch()     │                      │ register_table   │
   │ read_json()      │                      │                  │
   └────────┬─────────┘                      └────────┬─────────┘
            │                                         │
            │ (Returns DataFrame)                     │ (Stored in Catalog)
            ▼                                         ▼
[ 4. THE HUB ]                                  [ CATALOG ]
┌─────────────────────────────────────────────────────▼────────────────────┐
│                             SessionContext                               │
│ ┌──────────────────────────────────────────────────────────────────────┐ │
│ │  SessionState: Config · RuntimeEnv · Optimizer · Planner · Catalog   │ │
│ │                                                                      │ │
│ │  ┌─────────────────┐                     ┌────────────────────────┐  │ │
│ │  │ Ephemeral Plan  │                     │ Registered Providers   │  │ │
│ │  └─────────────────┘                     │ "sales", "metrics"...  │  │ │
│ └───────────┬───────────────────────────────────────┬──────────────────┘ │
└─────────────┼───────────────────────────────────────┼────────────────────┘
              │                                       │
              │ (Direct Return)                       │ (table("sales"))
              │                                       │ (sql("SELECT..."))
              ▼                                       ▼
       ┌─────────────────────────────────────────────────────┐
       │                      DataFrame                      │
       │           (LogicalPlan + SessionState)              │
       └─────────────────────────────────────────────────────┘
```

> **Understanding the layers:**
>
> - **Layer 1 (Data Sources)**: Where bytes live (S3, disk, RAM, external systems).
> - **Layer 2 (Table Providers)**: Implementations of [`TableProvider`] that translate bytes to Arrow batches.
> - **Layer 3 (Access Pattern)**:
>   - **Ephemeral (Direct Read)**: The plan exists only inside the returned DataFrame.
>   - **Named (Registered)**: A [`TableProvider`] is stored in the catalog, accessible by name.
> - **Layer 4 (The Hub)**: [`SessionContext`] holds configuration and catalogs—the factory for all DataFrames.

---

<!-- Link references -->

### 1. From Files

**Read files directly into a lazy `DataFrame`. Format choice determines optimization potential—Parquet enables metadata pruning; text formats generally require reading full files (except for partition pruning).**

Files are a common entry point—data lakes, ETL pipelines, local analysis—but DataFusion's strength is **fusion**: the same query can join a Parquet file with a PostgreSQL table or a streaming source. This section covers file-based access.

File scans are lazy (no record batches are read until an action), but DataFusion may read **metadata** (and for CSV/NDJSON a **sample** for schema inference) when creating the DataFrame.

| Parameter   | Type                  | Description                                                       |
| ----------- | --------------------- | ----------------------------------------------------------------- |
| `path`      | `impl DataFilePaths`  | Single file, `Vec<&str>`, glob pattern, or cloud URL (`s3://...`) |
| `options`   | `<Format>ReadOptions` | Format-specific configuration (schema, compression, etc.)         |
| **Returns** | `Result<DataFrame>`   | Lazy DataFrame                                                    |

> **What happens immediately?** <br>
> Creating the DataFrame is not strictly "lazy" for all I/O:
>
> - **CSV/JSON:** Reads the first 1,000 rows (default) to infer schema.
> - **Parquet/Arrow/Avro:** Reads file footers/headers to fetch schema.
> - **Statistics:** By default ([`datafusion.execution.collect_statistics = true`][`executionoptions::collect_statistics`]), DataFusion may read file sizes and row counts from metadata.
>
> Actual _data_ processing (filtering, joining, aggregating) happens only when you execute an action.

As an example, here's how to read a Parquet file:

```rust
use datafusion::prelude::*;
# use std::path::PathBuf;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();

    // Path to your Parquet file (local or object-store URL like s3://...)
    let path = "data.parquet";
    # // Hidden: use test data for doctests
    # let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
    #     .join("parquet-testing/data/alltypes_plain.parquet")
    #     .to_string_lossy().to_string();

    // Read a Parquet file — lazy scan, nothing loads until an action
    let df = ctx.read_parquet(&path, ParquetReadOptions::default()).await?;

    df.show().await?;
    Ok(())
}
```

**Warning: Cloud Storage (S3, GCS, Azure)** <br>
DataFusion does not bundle cloud connectors by default. To use `s3://`, `gs://`, or `az://` paths, you must first register the corresponding `ObjectStore` with your `SessionContext`.

<details>
<summary><strong>Quick Start: S3 Registration</strong></summary>

```rust,no_run
use std::sync::Arc;

use datafusion::error::Result;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::object_store::ObjectStore;
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> Result<()> {
    // no_run: requires configuring a cloud object store (credentials, network, etc.)
    let ctx = SessionContext::new();

    // Create an object store implementation for your cloud provider.
    // For a complete S3 setup (AmazonS3Builder, credentials), see:
    // https://github.com/apache/datafusion/blob/main/datafusion-examples/examples/external_dependency/query-aws-s3.rs
    let store: Arc<dyn ObjectStore> = todo!();

    // Register `s3://<bucket>` (or `gs://...`, `az://...`) before reading.
    let url = ObjectStoreUrl::parse("s3://my-bucket")?;
    ctx.runtime_env().register_object_store(url.as_ref(), store);

    let df = ctx
        .read_parquet("s3://my-bucket/data.parquet", ParquetReadOptions::default())
        .await?;

    // Execute with an action such as:
    // df.collect().await?;
    Ok(())
}
```

</details>

See [**Advanced: Custom Data Sources**](dataframes-advance.md#custom-data-sources) for setup details.

---

#### Reading Multiple Files

**DataFusion can read multiple files as a single DataFrame using explicit paths or glob patterns—this applies to all file formats.**

```rust
use datafusion::prelude::*;
# use std::path::PathBuf;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();
    # let test_data = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
    #     .join("parquet-testing/data");

    // Pattern 1: Multiple explicit paths (Vec)
    let paths = vec!["data/part-000.parquet", "data/part-001.parquet"];
    # let paths: Vec<String> = vec![
    #     test_data.join("alltypes_plain.parquet").to_string_lossy().to_string(),
    #     test_data.join("alltypes_plain.snappy.parquet").to_string_lossy().to_string(),
    # ];
    let df = ctx.read_parquet(paths, ParquetReadOptions::default()).await?;

    // Pattern 2: Glob patterns
    let glob_pattern = "data/*.parquet";
    # let glob_pattern = test_data.join("alltypes*.parquet").to_string_lossy().to_string();
    let df = ctx.read_parquet(&glob_pattern, ParquetReadOptions::default()).await?;

    // Pattern 3: Cloud storage (after registering object store)
    // let df = ctx.read_parquet("s3://bucket/data/*.parquet", ...).await?;

    Ok(())
}
```

> **Cloud storage**:<br>
> Register an object store before using `s3://`, `gs://`, or `az://` paths. See the [S3 setup example](https://github.com/apache/datafusion/blob/main/datafusion-examples/examples/external_dependency/query-aws-s3.rs) for complete configuration (credentials, registration, query).

All file readers support the same path patterns:

- **Single file:** `"data.parquet"`
- **Multiple files:** `vec!["a.parquet", "b.parquet"]`
- **Glob patterns:** `"data/**/*.parquet"` (recursive), `"data/*.csv"` (single directory)
- **Cloud URLs:** `"s3://bucket/prefix/*.parquet"` (after object store registration)

> **Note: Subdirectories are ignored by default** <br>
> If you provide a directory path like [`ctx.read_parquet("data/")`][`.read_parquet()`], DataFusion scans only that directory level. It does **not** recursively scan subdirectories unless [`datafusion.execution.listing_table_ignore_subdirectory = false`][`executionoptions::listing_table_ignore_subdirectory`] or you use a recursive glob like `data/**/*.parquet`.

<details>
<summary><strong>Advanced: ListingTable + read _table() for more control</strong></summary>

The `read_<format>()` helpers (such as `read_parquet()`, `read_csv()`, `read_json()`) are the simplest way to scan files. If you need more control (custom `ListingOptions`, multi-path tables, schema management, etc.), build a `ListingTable` and then create a `DataFrame` with `SessionContext::read_table()`.

```rust
use datafusion::prelude::*;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::{ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl};
use std::sync::Arc;
# use std::path::PathBuf;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();

    // Directory containing Parquet files
    let path = "data/";
    # let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
    #     .join("parquet-testing/data")
    #     .to_string_lossy().to_string();
    let table_path = ListingTableUrl::parse(&path)?;
    let listing_options = ListingOptions::new(Arc::new(ParquetFormat::default()));

    let config = ListingTableConfig::new(table_path)
        .with_listing_options(listing_options)
        .infer_schema(&ctx.state())
        .await?;

    let provider = Arc::new(ListingTable::try_new(config)?);
    let df = ctx.read_table(provider)?;

    df.show().await?;
    Ok(())
}
```

</details>

---

[`executionoptions::listing_table_ignore_subdirectory`]: https://docs.rs/datafusion/latest/datafusion/config/struct.ExecutionOptions.html#structfield.listing_table_ignore_subdirectory
[`executionoptions::collect_statistics`]: https://docs.rs/datafusion/latest/datafusion/config/struct.ExecutionOptions.html#structfield.collect_statistics

#### Choosing a File Format

DataFusion natively supports five file formats (other formats like ORC or Iceberg are available via [extensions](https://datafusion.apache.org/library-user-guide/extensions.html)). Storage layout (columnar vs row-oriented) strongly affects query performance:

| Format                                                   | Layout   | Schema Source                | Startup Cost                | Pruning Support       | Best For                         |
| -------------------------------------------------------- | -------- | ---------------------------- | --------------------------- | --------------------- | -------------------------------- |
| **[Parquet][parquet-section]** <br>[`.read_parquet()`]   | Columnar | Embedded (footer)            | **Low** (metadata only)     | ✅ Metadata + Columns | Production analytics, large data |
| **[CSV][csv-section]** <br>[`.read_csv()`]               | Row      | ⚠️ **Inferred** (first 1000) | **High** (inference scan)   | ❌ Partition only     | Simple exchange, imports         |
| **[NDJSON][ndjson-section]** <br>[`.read_json()`]        | Row      | ⚠️ **Inferred** (first 1000) | **Medium** (inference scan) | ❌ Partition only     | Semi-structured logs/APIs        |
| **[Avro][avro-section]** <br>[`.read_avro()`]            | Row      | Embedded (header)            | **Low** (header schema)     | ❌ Partition only     | Kafka, schema evolution          |
| **[Arrow IPC][arrow-ipc-section]** <br>[`.read_arrow()`] | Columnar | Embedded (header)            | **Very Low** (zero-copy\*)  | ❌ Partition only     | Arrow ecosystem, zero-copy       |

<!-- Section links -->

[parquet-section]: #parquet--the-analytical-standard
[csv-section]: #csv--tabular-exchange
[ndjson-section]: #ndjson--semi-structured-logs
[avro-section]: #avro--schema-evolution
[arrow-ipc-section]: #arrow-ipc--zero-copy-native

> For analytics, prefer **columnar formats** (Parquet, Arrow IPC). Columnar storage lets DataFusion read only the needed columns, drastically reducing I/O. Row-based formats (Avro, CSV, JSON) must read entire rows even when you need one field.

---

### Parquet — The Analytical Standard

**The default choice for analytical workloads: columnar, compressed, self-describing, and optimized for selective reads.**

[Apache Parquet](https://parquet.apache.org/) stores data **column-by-column** instead of row-by-row. This layout lets DataFusion read only the columns your query needs, skip irrelevant data using embedded statistics, and benefit from excellent compression ratios.

```rust
use datafusion::prelude::*;
# use std::path::PathBuf;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();

    // Path to your Parquet file
    let path = "data.parquet";
    # let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
    #     .join("parquet-testing/data/alltypes_plain.parquet")
    #     .to_string_lossy().to_string();

    let df = ctx.read_parquet(&path, ParquetReadOptions::default()).await?;

    df.show().await?;
    Ok(())
}
```

For typical analytical queries, Parquet is often **significantly faster** than CSV—not because of raw read speed, but because tools like DataFusion can skip reading large portions of data (row group pruning) and avoid reading unneeded columns (column pruning).

#### Parquet Trade-offs

| Parquet shines ✓                                       | Avoid Parquet ✗                                                   |
| ------------------------------------------------------ | ----------------------------------------------------------------- |
| Production analytics, repeated queries, large datasets | Write-heavy append logs → NDJSON / streaming-native systems       |
| Selective reads (filters + column pruning)             | Human-editable debugging → CSV/JSON                               |
| Efficient storage (compression + columnar layout)      | Very small datasets where metadata/compression overhead dominates |

#### ParquetReadOptions

[`ParquetReadOptions`] provides builder methods for customization which means you can chain the desired options:

| Builder Method                                                                   | Default      | Usage                                                                                                                           |
| :------------------------------------------------------------------------------- | :----------- | :------------------------------------------------------------------------------------------------------------------------------ |
| **[`.parquet_pruning(bool)`][`parquetreadoptions::parquet_pruning()`]**          | `true`       | Skips row groups using min/max statistics. Keep enabled for filtered queries (`WHERE id > 100`).                                |
| **[`.table_partition_cols(Vec)`][`parquetreadoptions::table_partition_cols()`]** | `[]`         | Maps Hive-style directory paths to columns (e.g., `year=2025/`). Use when data is organized in folders by date/category.        |
| **[`.file_extension(&str)`][`parquetreadoptions::file_extension()`]**            | `".parquet"` | Filters input files by suffix. Use when folders contain mixed files (`.crc`, `.json`, temp files).                              |
| **[`.schema(&Schema)`][`parquetreadoptions::schema()`]**                         | `None`       | Supplies the Parquet _file_ schema. Use for production to enforce types and avoid schema-merging surprises across many files.   |
| **[`.skip_metadata(bool)`][`parquetreadoptions::skip_metadata()`]**              | `true`       | Ignores embedded schema metadata to avoid conflicts. Keep `true` for mixed producers; set `false` only if you rely on metadata. |
| **[`.file_sort_order(Vec)`][`parquetreadoptions::file_sort_order()`]**           | `[]`         | Tells the optimizer the data is pre-sorted. Use to speed up merge-joins or `ORDER BY` queries without re-sorting.               |

> **Note:** <br> > [`ParquetReadOptions::schema()`] here is a _builder method_ that sets the schema for reading. This differs from [`DataFrame::schema()`], which _returns_ the schema of an existing DataFrame.

> **Key insight:** <br> > **Row group pruning is enabled by default** (`datafusion.execution.parquet.pruning = true`) and can be overridden per read with `.parquet_pruning(true/false)`. With pruning enabled, DataFusion compares your `WHERE` predicates against each row group's min/max statistics—if no rows can possibly match, the entire group is skipped without reading any data.

<details>
<summary><strong>Example: ParquetReadOptions builder pattern</strong></summary>

The following example demonstrates how to configure `ParquetReadOptions` with various builder methods. For Hive-partitioned data (e.g., `year=2024/month=01/`), use `.table_partition_cols()` to map directory structure to columns.

```rust
use datafusion::prelude::*;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
# use std::path::PathBuf;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();

    // 1. Define the Parquet *file* schema (optional, but recommended for production).
    // Partition columns (year/month) would come from directory structure via `table_partition_cols`.
    let file_schema = Schema::new(vec![Field::new("id", DataType::Int32, true)]);

    // 2. Configure the reader with builder methods
    let options = ParquetReadOptions::default()
        .file_extension(".parquet")   // Filter files by extension
        .parquet_pruning(true)        // Enable statistics-based pruning
        .schema(&file_schema);        // Enforce file schema

    // For Hive-partitioned directories (data/sales/year=2024/month=01/*.parquet),
    // add: .table_partition_cols(vec![("year".into(), DataType::Int32), ...])

    // 3. Read Parquet file(s)
    let path = "data/sales/";
    # // Hidden: use test data for doctests
    # let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
    #     .join("parquet-testing/data/alltypes_plain.parquet")
    #     .to_string_lossy().to_string();
    # // Use default options for test data (no partition columns)
    # let options = ParquetReadOptions::default();

    let df = ctx.read_parquet(&path, options).await?;
    df.show().await?;
    Ok(())
}
```

</details>

#### Parquet inner workings

> **Deep Dive: Parquet File Structure** <br>
> For a detailed look at Parquet internals—row groups, column chunks, statistics, and how DataFusion exploits them—see [Advanced Topics: Parquet File Structure](dataframes-advance.md#parquet-file-structure).

DataFusion uses **metadata-first scanning**:

**When you call `read_parquet()`:**

1. **Footer read** <br>
   Reads the Parquet footer (~few KB) to learn schema and row group metadata (or uses an explicit schema if you provide one)
2. **Plan creation** <br>
   Creates a [`ListingTable`] and returns a lazy `DataFrame`
3. **Zero data bytes** <br>
   No column data loaded yet

> **Note (startup cost vs pruning):** <br> > `read_parquet()` may collect per-file statistics during DataFrame creation (`datafusion.execution.collect_statistics = true` by default). This can add noticeable startup time for many files (especially on cloud object stores), but can speed up filtered queries. To prioritize startup time, disable it: `SessionConfig::new().with_collect_statistics(false)`.

**When you call an action** (`.collect()`, `.show()`):

4. **Row group pruning** <br> Skips row groups where statistics prove no match
5. **Column pruning** <br>
   Reads only columns referenced in your query
6. **Streaming decode** <br>
   Decodes in batches, keeping memory bounded

#### Parquet Pushdown — How DataFusion Skips Data

DataFusion's Parquet reader exploits metadata at multiple levels to minimize I/O:

**1. Metadata-based Skipping (ON by default)**

| Mechanism         | What's skipped             | How it works                                                                                            |
| ----------------- | -------------------------- | ------------------------------------------------------------------------------------------------------- |
| **Partition**     | Entire directories         | Hive-style paths (`year=2024/`) matched against `WHERE`. Works for all formats.                         |
| **Row group**     | Groups of rows (~128MB)    | Min/max statistics compared to filter predicates. Controlled by `datafusion.execution.parquet.pruning`. |
| **Page Index**    | Pages within column chunks | Page-level min/max stats (if written by producer).                                                      |
| **Bloom Filters** | Specific row groups        | Probabilistic check for value existence (e.g., `id = 'abc'`).                                           |

**2. Decode-time Optimizations**

| Mechanism           | Optimization         | How it works                                                                                                            |
| ------------------- | -------------------- | ----------------------------------------------------------------------------------------------------------------------- |
| **Projection**      | Unreferenced columns | Only columns in `SELECT` are read from disk.                                                                            |
| **Filter Pushdown** | Late materialization | Applies filters _during_ decoding to skip values. **OFF** by default (`datafusion.execution.parquet.pushdown_filters`). |

For highly selective queries where built-in statistics aren't enough, DataFusion supports advanced indexing:

- **User-defined indexes** — Embed custom indexes directly in Parquet file metadata
- **External indexes** — Store sidecar index files alongside your Parquet data

See the References section for deep dives on these techniques.

#### Parquet References

**DataFusion Blog (Deep Dives):**

- [Parquet Pushdown](https://datafusion.apache.org/blog/2025/03/21/parquet-pushdown/) — How DataFusion exploits Parquet metadata
- [User-Defined Parquet Indexes](https://datafusion.apache.org/blog/2025/07/14/user-defined-parquet-indexes/) — Embedding custom indexes
- [External Parquet Indexes](https://datafusion.apache.org/blog/2025/08/15/external-parquet-indexes/) — Sidecar index files

**Format & API:**

- [Apache Parquet Documentation](https://parquet.apache.org/docs/) — Official specification
- [`ParquetReadOptions` API](https://docs.rs/datafusion/latest/datafusion/datasource/file_format/options/struct.ParquetReadOptions.html) — All configuration options

---

### CSV — Tabular Exchange

**Simple, human-readable, universal—the lowest common denominator for data exchange.**

CSV is row-oriented text with no embedded schema. When you call `read_csv()`, DataFusion must:

1. **Infer schema** <br>
   By scanning the first N rows (default: 1000)—this happens _at DataFrame creation_, not lazily
2. **Parse text → typed Arrow columns** <br>
   Row by row—a row-to-columnar conversion cost

This makes CSV ideal for _ingestion and interchange_—receiving data from upstream systems ( like mainframes, HL7 feeds, legacy batch jobs, vendor exports) or quick spreadsheet imports. For repeated analytical queries, convert CSV to Parquet once and query Parquet thereafter.

```rust
use datafusion::prelude::*;
# use datafusion::assert_batches_sorted_eq;
# use datafusion::error::Result;
# use std::fs::File;
# use std::io::Write;
# use tempfile::tempdir;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();
    # // Create test CSV file
    # let dir = tempdir()?;
    # let csv_path = dir.path().join("example.csv");
    # let mut file = File::create(&csv_path)?;
    # writeln!(file, "id,name")?;
    # writeln!(file, "1,Alice")?;
    # writeln!(file, "2,Bob")?;

    // Read CSV (schema is inferred immediately at DataFrame creation)
    let path = "data.csv";
    # let path = csv_path.to_str().unwrap();
    let df = ctx.read_csv(path, CsvReadOptions::new()).await?;

    // Print the inferred schema
    println!("{}", df.schema());

    df.show().await?;
    // +----+-------+
    // | id | name  |
    // +----+-------+
    // | 1  | Alice |
    // | 2  | Bob   |
    // +----+-------+
    # // Re-read for assertion (show() consumes the DataFrame)
    # let df = ctx.read_csv(path, CsvReadOptions::new()).await?;
    # let results = df.select_columns(&["id", "name"])?.collect().await?;
    # assert_batches_sorted_eq!(
    #     &[
    #         "+----+-------+",
    #         "| id | name  |",
    #         "+----+-------+",
    #         "| 1  | Alice |",
    #         "| 2  | Bob   |",
    #         "+----+-------+",
    #     ],
    #     &results
    # );
    Ok(())
}
```

> **⚠️ Production Warning: Schema Inference**
> CSV files have no embedded schema—DataFusion infers types from the first 1000 rows. This can fail silently if row 1001 has a different type. **Always provide an explicit schema in production.** See [Schema Management](schema-management.md) for guidance.

**Practical considerations:**

- **No embedded schema** <br>
  DataFusion infers types from the first 1000 rows. Use `.schema_infer_max_records(n)` if early rows aren't representative, or provide an explicit schema in production.
- **No predicate pushdown** <br>
  Every byte must be read and parsed, even if filtered later.
- **Projection reduces CPU, not I/O** <br>
  DataFusion skips _materializing_ unused columns, but still scans the full file.
- **Compression** <br>
  Use `.csv.gz` or `.csv.zst` for transfer; DataFusion reads them directly via `.file_compression_type()`.

#### CSV Trade-offs

| CSV Shines ✓                                         | Avoid CSV ✗                                     |
| ---------------------------------------------------- | ----------------------------------------------- |
| Data exchange with spreadsheets, legacy systems      | Analytics on large datasets → Parquet           |
| Human inspection and quick debugging                 | Schema enforcement critical → Parquet/Avro      |
| One-off exports, universal compatibility             | Storage efficiency matters (5–10x larger)       |
| Small datasets where Parquet overhead isn't worth it | You need predicate pushdown (filter before I/O) |

#### CsvReadOptions

[`CsvReadOptions`] provides builder methods for customization. The most important ones for production use are [`CsvReadOptions::schema()`] (for explicit type control) and [`.delimiter()`][`csvreadoptions::delimiter()`] (for non-comma separators like TSV).

| Builder Method                                                                     | Default        | Usage                                                                                                                  |
| :--------------------------------------------------------------------------------- | :------------- | :--------------------------------------------------------------------------------------------------------------------- |
| **[`.has_header(bool)`][`csvreadoptions::has_header()`]**                          | `true`         | Treats first row as column names. Set `false` if file starts immediately with data.                                    |
| **[`.delimiter(u8)`][`csvreadoptions::delimiter()`]**                              | `b','`         | Field separator character. Use `b'\t'` for TSV or `b';'` for European CSV.                                             |
| **[`.schema(&Schema)`][`csvreadoptions::schema()`]**                               | `None`         | Explicit column names and types. **Recommended for production** to enforce strict types and avoid inference surprises. |
| **[`.schema_infer_max_records(n)`][`csvreadoptions::schema_infer_max_records()`]** | `1000`         | Rows to scan for type inference. Increase if first 1000 rows contain nulls in columns that later have data.            |
| **[`.quote(u8)`][`csvreadoptions::quote()`]**                                      | `b'"'`         | Character to quote fields containing delimiters. Use `b'\''` for single-quote dialects.                                |
| **[`.file_compression_type(...)`][`csvreadoptions::file_compression_type()`]**     | `UNCOMPRESSED` | Compression algorithm (GZIP, BZIP2, ZSTD). For reading `.csv.gz` or `.csv.zst` directly.                               |
| **[`.newlines_in_values(bool)`][`csvreadoptions::newlines_in_values()`]**          | `false`        | Allows `\n` inside quoted fields. **Warning**: Disables parallel file scanning (slower).                               |
| **[`.null_regex(str)`][`csvreadoptions::null_regex()`]**                           | `None`         | Treats specific strings (e.g., `"NA"`) as null. Use when data uses non-standard null markers.                          |
| **[`.file_extension(&str)`][`csvreadoptions::file_extension()`]**                  | `".csv"`       | Filters input files by suffix. Use to ignore metadata files in mixed directories.                                      |

> **Note:** [`CsvReadOptions::schema()`] here is a _builder method_ that sets the schema for reading. This differs from [`DataFrame::schema()`], which _returns_ the schema of an existing DataFrame.

<details>
<summary><strong>Example: Reading compressed CSV with explicit schema</strong></summary>

This example shows how to read a GZIP-compressed CSV file with an explicit schema—a common pattern for log ingestion pipelines.

```rust
use datafusion::prelude::*;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::datasource::file_format::file_compression_type::FileCompressionType;
# use std::path::PathBuf;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();

    // Path to GZIP-compressed CSV
    let path = "logs.csv.gz";
    # let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
    #     .join("testing/data/csv/aggregate_test_100.csv.gz")
    #     .to_string_lossy().to_string();

    // Define schema upfront (skips inference, enforces types)
    let schema = Schema::new(vec![
        Field::new("c1", DataType::Utf8, true),
        Field::new("c2", DataType::Int64, true),
        Field::new("c3", DataType::Int64, true),
    ]);

    // Configure reader for GZIP compressed files
    let options = CsvReadOptions::new()
        .schema(&schema)
        .file_extension(".csv.gz")
        .file_compression_type(FileCompressionType::GZIP);

    // Read — DataFusion decompresses on the fly
    let df = ctx.read_csv(&path, options).await?;

    df.show().await?;
    Ok(())
}
```

</details>

#### CSV Production Tips

- **Always provide explicit schema** <br>
  Schema inference is risky: if row 1001 has a different type than rows 1–1000, your query fails at runtime.
- **Use `.csv.gz`** for compressed transfer — DataFusion decompresses on the fly
- **Set `.null_regex("NA|NULL|\\N")`** to handle common null markers
- **Watch for empty strings vs nulls** — They're treated differently

<details>
<summary><strong>Example: Explicit schema for production</strong></summary>

```rust
use datafusion::prelude::*;
use datafusion::arrow::datatypes::{Schema, Field, DataType};
# use std::fs::File;
# use std::io::Write;
# use tempfile::tempdir;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();
    # // Create test CSV file
    # let dir = tempdir()?;
    # let csv_path = dir.path().join("data.csv");
    # let mut file = File::create(&csv_path)?;
    # writeln!(file, "id,name,amount")?;
    # writeln!(file, "1,Alice,150.50")?;
    # writeln!(file, "2,Bob,200.00")?;
    # writeln!(file, "3,Carol,75.25")?;

    let schema = Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("amount", DataType::Float64, true),
    ]);

    // Read CSV with explicit schema
    let path = "data.csv";
    # let path = csv_path.to_str().unwrap();
    let df = ctx.read_csv(path, CsvReadOptions::new().schema(&schema)).await?;

    df.show().await?;
    Ok(())
}
```

</details>

#### CSV References

- [`CsvReadOptions` API](https://docs.rs/datafusion/latest/datafusion/prelude/struct.CsvReadOptions.html) — All configuration options
- [Example Usage (CSV with SQL and DataFrame)](../../user-guide/example-usage.md)

---

### NDJSON — Semi-Structured Logs

**Newline-delimited JSON: one JSON object per line, ideal for logs and streaming data.**

NDJSON (also called JSON Lines, `.jsonl`) is row-oriented text like CSV, but each line is a self-describing JSON object. When you call [`.read_json()`], DataFusion must:

1. **Infer schema** <br>
   By scanning the first N objects (default: 1000)—this happens _at DataFrame creation_, not lazily
2. **Parse JSON → typed Arrow columns** <br>
   Object by object—nested structures flatten to Arrow structs

This makes NDJSON ideal for _data interchange_—log files, NoSQL database exports (MongoDB, Elasticsearch), streaming APIs, and message queue payloads. For repeated analytical queries, convert to Parquet.

```rust
use datafusion::prelude::*;
# use datafusion::assert_batches_sorted_eq;
# use std::fs::File;
# use std::io::Write;
# use tempfile::tempdir;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();
    # // Create test NDJSON file (one JSON object per line)
    # let dir = tempdir()?;
    # let json_path = dir.path().join("logs.ndjson");
    # let mut file = File::create(&json_path)?;
    # writeln!(file, r#"{{"num":5,"str":"test"}}"#)?;
    # writeln!(file, r#"{{"num":2,"str":"hello"}}"#)?;
    # writeln!(file, r#"{{"num":4,"str":"foo"}}"#)?;

    // Read NDJSON (schema is inferred immediately at DataFrame creation)
    let path = "logs.ndjson";
    # let path = json_path.to_str().unwrap();
    # // Use .ndjson extension to match temp file
    let df = ctx.read_json(path, NdJsonReadOptions::default()
        .file_extension(".ndjson")
    ).await?;

    df.show().await?;
    # // Re-read for assertion (show() consumes the DataFrame)
    # let df = ctx.read_json(path, NdJsonReadOptions::default().file_extension(".ndjson")).await?;
    # let results = df.select_columns(&["str", "num"])?.collect().await?;
    # assert_batches_sorted_eq!(
    #     &[
    #         "+-------+-----+",
    #         "| str   | num |",
    #         "+-------+-----+",
    #         "| foo   | 4   |",
    #         "| hello | 2   |",
    #         "| test  | 5   |",
    #         "+-------+-----+",
    #     ],
    #     &results
    # );
    Ok(())
}
```

> **⚠️ Production Warning: Schema Inference**
> NDJSON files have no embedded schema—DataFusion infers types from the first 1000 objects. Deeply nested or sparse fields may not be detected. **Always provide an explicit schema in production.** See [Schema Management](schema-management.md) for guidance.

**Practical considerations:**

- **Schema inference scans first 1000 objects** <br>
  Deeply nested or sparse fields may not be detected. Provide an explicit [`.schema()`][ndjsonreadoptions::schema()] in production.
- **Nested objects flatten to Arrow structs** <br>
  `{"user": {"name": "Alice"}}` becomes a struct column accessible as `user.name`.
- **No predicate pushdown** <br>
  Every line must be parsed, even if filtered later—similar to CSV.
- **File extension matters** <br>
  Use `.file_extension(".jsonl")` or `.file_extension(".ndjson")` if your files don't end in `.json`.

#### Trade-offs

| NDJSON shines ✓                             | Avoid NDJSON ✗                                      |
| ------------------------------------------- | --------------------------------------------------- |
| Data interchange: logs, APIs, NoSQL exports | Production analytics on large datasets → Parquet    |
| Semi-structured / evolving records          | Highly selective queries needing predicate pushdown |
| Append-friendly, easy to generate           | Strict schema contracts → Avro or Parquet           |

#### NdJsonReadOptions

The [`NdJsonReadOptions`] builder configures the parser.

| Builder Method                                                                    | Default        | Usage                                                                                                  |
| :-------------------------------------------------------------------------------- | :------------- | :----------------------------------------------------------------------------------------------------- |
| **[`.schema(&Schema)`][`ndjsonreadoptions::schema()`]**                           | `None`         | Explicit schema. **Recommended for production** to enforce strict types and avoid inference surprises. |
| **[`.file_extension(&str)`][`ndjsonreadoptions::file_extension()`]**              | `".json"`      | Filters input files by suffix. Use `".jsonl"` or `".ndjson"` for non-standard extensions.              |
| **[`.file_compression_type(...)`][`ndjsonreadoptions::file_compression_type()`]** | `UNCOMPRESSED` | Compression algorithm. For reading `.json.gz` or `.json.zst` directly.                                 |
| **[`.table_partition_cols(Vec)`][`ndjsonreadoptions::table_partition_cols()`]**   | `[]`           | Maps Hive-style directory paths to columns (e.g., `year=2024/month=01/`).                              |

> **Note:** To change schema inference depth (default: 1000 objects), set the field directly: <br> >
> `NdJsonReadOptions { schema_infer_max_records: 5000, ..Default::default() }`

> **Note:**<br> > [`NdJsonReadOptions::schema()`] here is a _builder method_ that sets the schema for reading. This differs from [`DataFrame::schema()`], which _returns_ the schema of an existing DataFrame.

<details>
<summary><strong>Example: Reading compressed NDJSON</strong></summary>

```rust
use datafusion::prelude::*;
use datafusion::datasource::file_format::file_compression_type::FileCompressionType;
# use std::io::Write;
# use tempfile::NamedTempFile;
# use flate2::write::GzEncoder;
# use flate2::Compression;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();

    let options = NdJsonReadOptions::default()
        .file_compression_type(FileCompressionType::GZIP)
        .file_extension(".gz"); // Important: match the actual file extension

    // Path to compressed NDJSON file(s)
    let path = "logs/*.json.gz";
    # // Hidden: create compressed test data for doctests
    # let mut temp_file = NamedTempFile::with_suffix(".json.gz").unwrap();
    # {
    #     let mut encoder = GzEncoder::new(&mut temp_file, Compression::default());
    #     writeln!(encoder, r#"{{"id": 1, "name": "Alice"}}"#).unwrap();
    #     writeln!(encoder, r#"{{"id": 2, "name": "Bob"}}"#).unwrap();
    #     encoder.finish().unwrap();
    # }
    # let path = temp_file.path().to_string_lossy().to_string();

    let df = ctx.read_json(&path, options).await?;
    df.show().await?;
    Ok(())
}
```

</details>

---

### Avro — Schema Evolution

**Row-based format with embedded schema, popular in Kafka ecosystems for schema evolution.**

Avro stores its schema in the file header, enabling forward/backward compatibility as schemas evolve. This makes it ideal for event streaming where producers and consumers may run different versions.

> **Feature flag required:** <br> Add `datafusion = { features = ["avro"] }` to your `Cargo.toml`.

```rust
# #[cfg(feature = "avro")]
# {
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();

    // Path to your Avro file
    let path = "events.avro";
    # // Hidden: use test data for doctests
    # let testdata = datafusion::test_util::arrow_test_data();
    # let path = format!("{testdata}/avro/alltypes_plain.avro");

    // Requires: datafusion = { features = ["avro"] }
    let df = ctx.read_avro(&path, AvroReadOptions::default()).await?;

    df.show().await?;
    Ok(())
}
# }
```

**Practical considerations:**

- **Embedded schema (no inference)** <br>
  Avro files carry a writer schema in the header, so readers don't need schema inference.
- **Row-based execution costs** <br>
  Avro is row-oriented: DataFusion must decode full rows and then build Arrow columns. Expect limited pushdown compared to Parquet.
- **Analytics workflow** <br>
  Avro is great for interchange and event streams; for repeated analytical queries, convert to Parquet.

#### Trade-offs

| Advantage                      | Disadvantage                              |
| ------------------------------ | ----------------------------------------- |
| Embedded schema (no inference) | Row-based (reads entire rows)             |
| Schema evolution support       | Less efficient than Parquet for analytics |
| Compact binary format          | Limited predicate pushdown                |
| Kafka ecosystem integration    | Requires `avro` feature flag              |

#### AvroReadOptions

[`AvroReadOptions`] provides builder methods for customization.

| Builder Method                                                                | Default | Usage                                                                          |
| :---------------------------------------------------------------------------- | :------ | :----------------------------------------------------------------------------- |
| **[`.schema(&Schema)`][`avroreadoptions::schema()`]**                         | `None`  | Explicit schema. Use to enforce strict types and avoid schema drift surprises. |
| **[`.table_partition_cols(Vec)`][`avroreadoptions::table_partition_cols()`]** | `[]`    | Maps Hive-style directory paths to columns (e.g., `year=2024/month=01/`).      |

> **Note:** `.schema()` here is a _builder method_ that sets the schema for reading. This differs from [`DataFrame::schema()`], which _returns_ the schema of an existing DataFrame.

If you need to scan a directory that contains mixed file types, Avro files are selected by extension (default: `.avro`). `AvroReadOptions` does not expose a builder for this—set the field directly using struct update syntax:

`AvroReadOptions { file_extension: ".avrodata", ..Default::default() }`

<details>
<summary><strong>Example: AvroReadOptions builder pattern</strong></summary>

The following example demonstrates `AvroReadOptions` configuration. For Hive-partitioned directories (e.g., `year=2024/month=01/`), use `.table_partition_cols()` to map directory structure to columns.

```rust
# #[cfg(feature = "avro")]
# {
use datafusion::arrow::datatypes::DataType;
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();

    // Configure partition columns for Hive-style directories
    // (e.g., events/year=2024/month=01/*.avro)
    let options = AvroReadOptions::default().table_partition_cols(vec![
        ("year".into(), DataType::Int32),
        ("month".into(), DataType::Int32),
    ]);

    // Path to Avro file or directory
    let path = "events/";
    # // Hidden: use test data for doctests (no partition structure)
    # let testdata = datafusion::test_util::arrow_test_data();
    # let path = format!("{testdata}/avro/alltypes_plain.avro");
    # let options = AvroReadOptions::default();

    let df = ctx.read_avro(&path, options).await?;
    df.show().await?;
    Ok(())
}
# }
```

</details>

#### Avro Production Tips

- **Schema evolution is Avro's strength** <br>
  Use it when producers and consumers evolve independently (Kafka, Pulsar, event sourcing)
- **For analytics, convert to Parquet** <br>
  Avro is great for interchange; for repeated analytical queries, convert once and query Parquet
- **Watch for feature flag** <br> Avro support requires `datafusion = { features = ["avro"] }` in your `Cargo.toml`

---

### Arrow IPC — Zero-Copy Native

**Arrow's native serialization format (Feather v2): very low deserialization overhead, fast startup, perfect for inter-process communication.**

Arrow IPC preserves Arrow's in-memory layout on disk. Deserialization is minimal and often zero-copy (depending on alignment and platform). Ideal for passing data between processes or caching intermediate results.

```rust
use datafusion::execution::options::ArrowReadOptions;
use datafusion::prelude::*;
# use std::path::PathBuf;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();

    let path = "data.arrow";
    # let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
    #     .join("datafusion/datasource-arrow/tests/data/example.arrow")
    #     .to_string_lossy().to_string();

    let df = ctx.read_arrow(&path, ArrowReadOptions::default()).await?;

    df.show().await?;
    Ok(())
}
```

**Practical considerations:**

- **Fastest startup** <br>
  Arrow IPC preserves Arrow's in-memory layout on disk, so DataFusion avoids expensive deserialization.
- **No predicate pushdown** <br>
  Arrow IPC does not provide Parquet-style statistics for pruning; DataFusion must read and decode batches to apply filters.
- **File extension matters** <br>
  DataFusion selects Arrow IPC files by extension (default: `.arrow`). If your files use a different extension (e.g., `.feather`), set the `file_extension` field in `ArrowReadOptions`.

#### When to Use Arrow IPC

| Use Case                     | Arrow IPC Fits?                          |
| ---------------------------- | ---------------------------------------- |
| Inter-process communication  | ✅ Excellent                             |
| Caching intermediate results | ✅ Excellent                             |
| Same-machine data sharing    | ✅ Excellent                             |
| Long-term storage            | ⚠️ Consider Parquet (better compression) |
| Cross-language exchange      | ✅ Arrow is language-agnostic            |

#### Trade-offs

| Advantage                     | Disadvantage              |
| ----------------------------- | ------------------------- |
| Zero deserialization cost     | No predicate pushdown     |
| Fastest startup time          | Larger files than Parquet |
| Preserves Arrow types exactly | Less ecosystem tooling    |
| Columnar layout               | No statistics for pruning |

#### ArrowReadOptions

[`ArrowReadOptions`] provides builder methods for customization.

| Builder Method                                                                 | Default | Usage                                                                                     |
| :----------------------------------------------------------------------------- | :------ | :---------------------------------------------------------------------------------------- |
| **[`.schema(&Schema)`][`arrowreadoptions::schema()`]**                         | `None`  | Explicit schema. Normalize schema across multiple files or override/standardize metadata. |
| **[`.table_partition_cols(Vec)`][`arrowreadoptions::table_partition_cols()`]** | `[]`    | Maps Hive-style directory paths to columns (e.g., `year=2024/month=01/`).                 |

> **Note:** [`ArrowReadOptions::schema()`] here is a _builder method_ that sets the schema for reading. This differs from [`DataFrame::schema()`], which _returns_ the schema of an existing DataFrame.

If you need to scan a directory that contains mixed file types, Arrow IPC files are selected by extension (default: `.arrow`). `ArrowReadOptions` does not expose a builder for this—set the field directly using struct update syntax:

`ArrowReadOptions { file_extension: ".feather", ..Default::default() }`

#### Arrow IPC Production Tips

- **Ideal for inter-process data passing** — Use when sharing Arrow data between processes on the same machine
- **Great for caching intermediate results** — Store DataFusion outputs for later reuse without re-computation
- **For long-term storage, prefer Parquet** — Arrow IPC has minimal compression; Parquet offers better storage efficiency
- **File extension flexibility** — Arrow IPC files may use `.arrow`, `.feather`, or `.ipc`; set `file_extension` accordingly

---

### 2. From a Registered Table

**Register data sources once and query them by name—the catalog caches metadata and both SQL and DataFrame APIs see the same logical table.**

Registration creates a logical name for a physical data source. This abstracts away the underlying details so you work with a simple name like "sales" instead of a file path or connection string.

This named table bridges DataFusion's two query interfaces—the SQL interface and the DataFrame API—both orchestrated by [`SessionContext`]. Under the hood, registration stores a [`TableProvider`] in the catalog so both interfaces see the same logical table.

Registration is lazy:<br>
The data itself isn't loaded into memory. DataFusion caches schema/metadata so the source is ready for high‑performance scanning when an action executes.

**Why register?**

- **Performance**:<br>
  Cache schema/metadata once; large/multi‑file and remote sources benefit from fewer round‑trips and better pruning
- **Partition awareness**:<br>
  Point to a directory and DataFusion auto-discovers Hive-style partitions (`/year=2022/month=01/`), enabling partition pruning
- **Interoperability**:<br>
  The same logical name works in both DataFrame and SQL (`ctx.table("sales")` / `FROM sales`)
- **Discoverability**:<br>
  Appears in `SHOW TABLES` and [`information_schema`]
- **Code clarity & portability**:<br>
  Decouple query code from physical locations; swap sources by changing the catalog

**When to skip registration (use direct reads instead):**

| Scenario                 | Why Direct Reads Work Better                                                                             |
| ------------------------ | -------------------------------------------------------------------------------------------------------- |
| One-off exploration      | Registration overhead isn't worth a single query                                                         |
| Dynamic file paths       | Paths that change frequently make registered names stale                                                 |
| Rapidly evolving schemas | Registered tables cache the schema at registration time; if the file structure changes, queries may fail |
| Simple scripts           | [`.read_parquet()`]/[`.read_csv()`] are more concise for quick tasks                                     |
| Ephemeral data           | Temporary data won't be queried again                                                                    |

> **Rule of thumb**:<br>
> If you'll query the same source more than once, or need SQL access, register it. For single-use exploration, direct reads are simpler.

For the catalog hierarchy and ways to inspect registered objects, see [Understanding DataFusion's Data Organization](#understanding-datafusions-data-organization).

#### Common registration methods

| Method                  | Purpose                               | Memory Impact        | Best For                              |
| ----------------------- | ------------------------------------- | -------------------- | ------------------------------------- |
| [`.register_parquet()`] | Register Parquet file(s) or directory | None (lazy scan)     | Production data, partitioned datasets |
| [`.register_csv()`]     | Register CSV file(s) or directory     | None (lazy scan)     | Data imports, simple formats          |
| [`.register_batch()`]   | Register in-memory RecordBatch        | Holds data in memory | Test data, small lookups              |
| [`.register_table()`]   | Register custom TableProvider         | Depends on provider  | Custom sources, advanced use          |

> **Tip:**<br>
> Registration creates a catalog entry with the data source's schema. The actual data is scanned lazily when queries execute, using DataFusion's streaming execution engine.

> **Async note:**<br>
> All examples in this section use `#[tokio::main]` and `async`/`.await`. DataFusion requires an async runtime for I/O and parallel execution. For details, see [The Tokio Async Runtime](concepts.md#the-tokio-async-runtime-understanding-tokio).

#### Performance benefits: Register once, query many times

**Avoid repeated schema inference and file scanning by registering sources that you'll query multiple times.**

When you call [`.read_csv()`] or [`.read_parquet()`] directly, DataFusion must:

1. **Open the file** (or establish the remote connection)
2. **Infer the schema** by sampling rows (for CSV/JSON) or reading metadata (for Parquet)
3. **Build a new scan plan** from scratch

For a single query, this is fine. But if you run multiple queries against the same source, you pay this cost every time. Registration solves this by caching the schema and [`TableProvider`] in the catalog—subsequent queries skip inference entirely.

```rust
# use std::fs::File;
# use std::io::Write;
# use tempfile::tempdir;
use datafusion::prelude::*;
use datafusion::error::Result;

#[tokio::main]
async fn main() -> Result<()> {
    # // Create test CSV file
    # let dir = tempdir()?;
    # let csv_path = dir.path().join("sales.csv");
    # let mut file = File::create(&csv_path)?;
    # writeln!(file, "id,name,amount")?;
    # writeln!(file, "1,Alice,1500")?;
    # writeln!(file, "2,Bob,500")?;
    # writeln!(file, "3,Carol,2000")?;
    # let csv_path_str = csv_path.to_string_lossy().to_string();
    #
    let ctx = SessionContext::new();

    // ❌ WITHOUT registration - inefficient pattern
    let path = "sales.csv";  // large example file
    # let path = &csv_path_str;
    let _count = ctx.read_csv(path, CsvReadOptions::new()).await?.count().await?;
    let _preview = ctx.read_csv(path, CsvReadOptions::new()).await?
        .limit(0, Some(10))?.collect().await?;
    let _filtered = ctx.read_csv(path, CsvReadOptions::new()).await?
        .filter(col("amount").gt(lit(1000)))?.collect().await?;
    // Problems: File opened 3 times, schema inferred 3 times

    // ✅ WITH registration - best practice
    ctx.register_csv("sales", path, CsvReadOptions::new()).await?;

    // Now each query reuses the registered table
    let _count = ctx.table("sales").await?.count().await?;
    let _preview = ctx.table("sales").await?.limit(0, Some(10))?.collect().await?;
    let _filtered = ctx.table("sales").await?
        .filter(col("amount").gt(lit(1000)))?.collect().await?;
    // Benefits: Schema cached, file handle managed efficiently

    Ok(())
}
```

> **Key insight**:<br>
> Each call to [`ctx.table()`][`.table()`] returns a new DataFrame, but they all reference the same registered source. The schema and metadata are cached, avoiding repeated inference overhead.

#### Mixing SQL and DataFrame APIs

**Combine SQL's declarative power with the DataFrame API's programmatic composability—registered tables are visible to both.**

Once a table is registered, you can query it with either API. This isn't just convenience—each API has strengths:

| API           | Strengths                                                              | Use When                                                 |
| ------------- | ---------------------------------------------------------------------- | -------------------------------------------------------- |
| **SQL**       | Complex joins, CTEs, window functions; familiar to analysts            | Query logic is known upfront; porting existing queries   |
| **DataFrame** | Programmatic composition; compile-time type checking; IDE autocomplete | Building queries dynamically; integrating with Rust code |

The result of [`ctx.sql()`][`.sql()`] is itself a DataFrame, so you can seamlessly transition: write complex joins in SQL, then continue with DataFrame transformations.

```rust
use std::sync::Arc;
use datafusion::prelude::*;
use datafusion::arrow::array::{ArrayRef, Int32Array, StringArray};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::Result;

#[tokio::main]
async fn main() -> Result<()> {
    let ctx = SessionContext::new();

    // Register dimension table (in-memory)
    let users = RecordBatch::try_from_iter(vec![
        ("id", Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef),
        ("name", Arc::new(StringArray::from(vec!["Alice", "Bob", "Carol"])) as ArrayRef),
    ])?;
    ctx.register_batch("users", users)?;

    // Register fact table (in-memory for this example)
    let orders = RecordBatch::try_from_iter(vec![
        ("order_id", Arc::new(Int32Array::from(vec![100, 101, 102, 103])) as ArrayRef),
        ("user_id", Arc::new(Int32Array::from(vec![1, 1, 2, 3])) as ArrayRef),
    ])?;
    ctx.register_batch("orders", orders)?;

    // DataFrame API: join tables programmatically
    let users_df = ctx.table("users").await?;
    let orders_df = ctx.table("orders").await?;
    let _joined = users_df.join(orders_df, JoinType::Inner, &["id"], &["user_id"], None)?;

    // SQL API: same tables, declarative syntax
    let result = ctx.sql("
        SELECT u.name, COUNT(*) as order_count
        FROM users u
        JOIN orders o ON u.id = o.user_id
        GROUP BY u.name
    ").await?;

    result.show().await?;
    Ok(())
}
```

> **Best practice**:<br>
> Prefer one API within a pipeline and switch at natural boundaries (e.g., define a view in SQL, then continue with DataFrame transforms), rather than ping‑ponging between APIs step-by-step.

> **No performance penalty**:<br>
> Both APIs compile down to the same [`LogicalPlan`]—choose based on ergonomics, not speed.

For advanced patterns like SQL-first workflows, round-trip transformations, and registering DataFrames as views, see [From SQL Queries](#3-from-sql-queries).

#### Inspecting the catalog

**Query metadata about registered tables—use the programmatic catalog API (DataFrame-style) for application code or SQL `information_schema` for ad-hoc exploration.**

When building data applications, you often need to discover what tables exist, inspect their schemas, or verify registrations succeeded. DataFusion provides two complementary approaches:

| Approach                                                             | API Style      | Best For                                    |
| -------------------------------------------------------------------- | -------------- | ------------------------------------------- |
| **Programmatic** ([`.catalog()`], [`.schema()`], [`.table_names()`]) | DataFrame/Rust | Application logic, dynamic queries, tooling |
| **SQL** (`information_schema.tables`)                                | SQL            | Ad-hoc exploration, debugging, portability  |

The programmatic approach follows the same builder pattern as the DataFrame API—you navigate the catalog hierarchy through method calls. This is the idiomatic Rust/DataFrame way to work with metadata.

See [Understanding DataFusion's Data Organization](#understanding-datafusions-data-organization) for the full hierarchy. Objects are organized as catalog → schema → table, with defaults `datafusion` and `public`.

```rust
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> Result<()> {
    // Enable information_schema for SQL-based catalog inspection
    let ctx = SessionContext::new_with_config(
        SessionConfig::new().with_information_schema(true)
    );

    // Register a table so we have something to inspect
    let table_schema = Arc::new(Schema::new(vec![Field::new(
        "order_id",
        DataType::Int32,
        false,
    )]));
    let batch = RecordBatch::new_empty(Arc::clone(&table_schema));
    let provider = MemTable::try_new(table_schema, vec![vec![batch]])?;
    ctx.register_table("sales", Arc::new(provider))?;

    // ✅ DataFrame-style: Programmatic catalog navigation
    // "datafusion" is the default catalog name
    let catalog = ctx
        .catalog("datafusion")
        .ok_or_else(|| DataFusionError::Plan("missing catalog: datafusion".to_string()))?;
    let schema = catalog
        .schema("public")
        .ok_or_else(|| DataFusionError::Plan("missing schema: public".to_string()))?;
    println!("Registered tables: {:?}", schema.table_names());

    // ✅ SQL-style: information_schema queries
    let tables_df = ctx
        .sql(
            r#"
            SELECT table_catalog, table_schema, table_name, table_type
            FROM information_schema.tables
            WHERE table_schema = 'public'
            ORDER BY table_name
            "#,
        )
        .await?;

    tables_df.show().await?;

    Ok(())
}
```

> **When to use which**:<br>
>
> - **Programmatic API**: Use when your code needs to react to available tables (e.g., building a schema browser, validating configurations, generating queries dynamically)
> - **SQL `information_schema`**: Use for interactive exploration, debugging, or when you need SQL-standard portability

> **Catalog lifetime**:<br>
> Registered tables live in the [`SessionContext`]'s in-memory catalog. When the context is dropped, all registrations are lost—there is no persistent catalog by default. For long-running applications, keep the context alive or re-register on startup. For persistent catalogs, see [Catalogs](../catalogs.md).

#### Advanced: Custom TableProviders

**Integrate any data source into DataFusion by implementing [`TableProvider`]—the trait that bridges external systems to the query engine.**

A [`TableProvider`] defines how DataFusion reads from a data source. Every registered table—whether from Parquet files, CSV, or in-memory batches—is backed by a [`TableProvider`] implementation. When you need to query data from sources DataFusion doesn't support natively, you implement this trait yourself.

**When to implement `TableProvider`:**

- **External databases**: Query PostgreSQL, MySQL, or other databases with predicate pushdown
- **REST APIs**: Treat API endpoints as queryable tables
- **Streaming sources**: Read from Kafka, Kinesis, or message queues
- **Computed/virtual tables**: Generate data on-the-fly (sequences, system info, derived views)
- **Custom file formats**: Support proprietary or domain-specific formats

**How it works:**

The [`TableProvider`] trait requires you to define the schema and implement [`scan()`][tableprovider_scan] to return an [`ExecutionPlan`]. DataFusion calls your provider when queries reference the registered table name.

```rust
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::error::Result;
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> Result<()> {
    let ctx = SessionContext::new();

    // `register_table` accepts any `Arc<dyn TableProvider>`.
    // Here we use MemTable as an example; your custom provider would
    // implement TableProvider to read from your specific data source.
    let schema = Arc::new(Schema::new(vec![Field::new(
        "status",
        DataType::Utf8,
        true,
    )]));
    let batch = RecordBatch::new_empty(Arc::clone(&schema));
    let provider = MemTable::try_new(schema, vec![vec![batch]])?;

    ctx.register_table("custom_source", Arc::new(provider))?;

    // Now queryable via both APIs
    let _df = ctx.table("custom_source").await?;
    let _sql = ctx.sql("SELECT * FROM custom_source").await?;

    Ok(())
}
```

> **Getting started**:<br>
> For a complete implementation guide with predicate pushdown and projection handling, see [Advanced Topics: TableProvider](dataframes-advance.md#tableprovider) and the [Custom Table Provider Guide](../custom-table-providers.md).

#### References

**DataFusion:**

- [Catalogs Guide](../catalogs.md) — Full catalog hierarchy and custom providers
- [`TableProvider` trait](https://docs.rs/datafusion/latest/datafusion/datasource/trait.TableProvider.html) — Interface for custom data sources
- [`CatalogProvider` trait](https://docs.rs/datafusion/latest/datafusion/catalog/trait.CatalogProvider.html) — Custom catalog implementations
- [`information_schema`](../../user-guide/sql/information_schema.md) — SQL inspection of registered objects
- [Using Rust async for Query Execution](https://datafusion.apache.org/blog/2025/01/28/async-dataframes/) — Async patterns in DataFusion

**Other Systems (for comparison):**

- [Polars: Register DataFrames for SQL](https://docs.pola.rs/user-guide/sql/intro/#register-dataframes) — Similar registration pattern
- [DuckDB: Registering Objects as Tables](https://duckdb.org/docs/api/python/overview#registering-python-objects-as-tables)

---

### 3. From SQL Queries

**[`ctx.sql()`][`.sql()`] returns a lazy `DataFrame`—making SQL a first-class DataFrame creation method, not just a query interface.**

[Section 2](#2-from-a-registered-table) showed how to register tables and access them via [`ctx.table()`][`.table()`]. Here, SQL itself becomes the entry point: you write a query, and the result is a `DataFrame` you can transform programmatically.

**The key insight**:<br> Since [`ctx.sql()`][`.sql()`] returns a DataFrame, you can combine SQL's
declarative power (CTEs, window functions, complex joins) with the DataFrame API's
programmatic flexibility (dynamic filters, conditional logic, Rust integration)—all
in a single, optimized pipeline.

The following patterns show two directions for bridging both APIs:

- **Pattern 1 (SQL → DataFrame)**: Start with SQL, refine with DataFrame operations
- **Pattern 2 (DataFrame → SQL → DataFrame)**: Use [`.into_view()`] to expose DataFrames to SQL mid-pipeline

#### Pattern 1: SQL-first workflow

**Start with SQL, finish with DataFrame—ideal when the analytical logic is naturally expressed in SQL.**

SQL handles the core analytical logic; DataFrame operations add the programmatic
finishing touches.

**Use this when** the core logic is best expressed in SQL and you want programmatic refinement afterward.

```rust
# use std::sync::Arc;
use datafusion::prelude::*;
# use datafusion::arrow::array::{ArrayRef, Int32Array, StringArray};
# use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::Result;

#[tokio::main]
async fn main() -> Result<()> {
    let ctx = SessionContext::new();
#
#     // Create in-memory sales data
#     let sales = RecordBatch::try_from_iter(vec![
#         ("region", Arc::new(StringArray::from(vec!["North", "North", "South", "South"])) as ArrayRef),
#         ("product", Arc::new(StringArray::from(vec!["Widget", "Gadget", "Widget", "Gadget"])) as ArrayRef),
#         ("amount", Arc::new(Int32Array::from(vec![8000, 3000, 6000, 4500])) as ArrayRef),
#     ])?;
#     ctx.register_batch("sales", sales)?;
    // Assume "sales" table is registered (Parquet, CSV, or in-memory)

    // Step 1: Execute complex analytical query in SQL
    let df = ctx.sql("
        WITH ranked_sales AS (
            SELECT
                region,
                product,
                amount,
                ROW_NUMBER() OVER (PARTITION BY region ORDER BY amount DESC) as rank
            FROM sales
        )
        SELECT * FROM ranked_sales WHERE rank <= 3
    ").await?;  // Returns a lazy DataFrame

    // Step 2: Continue with DataFrame API for dynamic refinement
    let top_profitable = df.filter(col("amount").gt(lit(5000)))?;

    top_profitable.show().await?;  // Executes the full, optimized pipeline
    Ok(())
}
```

#### Pattern 2: Round-trip workflow

**DataFrame → SQL → DataFrame—use both APIs at their strongest points in a single pipeline.**

This pattern uses [`.into_view()`] to convert a DataFrame into a logical view, which you then register with [`register_table()`][`.register_table()`] so SQL can reference it by name. The view captures the DataFrame's query plan (not materialized data)—each SQL query against it re-executes the underlying plan.

You prepare data programmatically (dynamic filters, computed columns), expose it to SQL for complex analytics, then continue with DataFrame operations for final enrichment.

**Use this when** you need programmatic preparation, SQL-based analysis, and programmatic finishing—all in one pipeline.

```rust
# use std::sync::Arc;
use datafusion::prelude::*;
# use datafusion::arrow::array::{ArrayRef, Int32Array, StringArray};
# use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::Result;

#[tokio::main]
async fn main() -> Result<()> {
    let ctx = SessionContext::new();
#
#     // Create in-memory sales data with varied amounts
#     let sales = RecordBatch::try_from_iter(vec![
#         ("region", Arc::new(StringArray::from(vec![
#             "North", "North", "North", "South", "South", "South"
#         ])) as ArrayRef),
#         ("product", Arc::new(StringArray::from(vec![
#             "Widget", "Gadget", "Gizmo", "Widget", "Gadget", "Gizmo"
#         ])) as ArrayRef),
#         ("amount", Arc::new(Int32Array::from(vec![
#             8000, 3000, 500, 12000, 4500, 200
#         ])) as ArrayRef),
#     ])?;
#     ctx.register_batch("sales", sales)?;
    // Assume "sales" table is registered

    // Step 1 (DataFrame): Programmatically prepare and filter
    let high_value = ctx.table("sales").await?
        .filter(col("amount").gt(lit(1000)))?
        .select(vec![col("region"), col("product"), col("amount")])?;

    // Step 2: Register intermediate DataFrame as temporary view
    ctx.register_table("high_value_sales", high_value.into_view())?;

    // Step 3 (SQL): Run complex aggregation on prepared data
    let summary = ctx.sql("
        SELECT region,
               COUNT(DISTINCT product) as product_count,
               SUM(amount) as total_revenue
        FROM high_value_sales
        GROUP BY region
        HAVING SUM(amount) > 10000
    ").await?;

    // Step 4 (DataFrame): Apply final programmatic enrichment
    let result = summary
        .with_column("revenue_millions", col("total_revenue") / lit(1_000_000))?
        .sort(vec![col("total_revenue").sort(false, true)])?
        .limit(0, Some(5))?;

    result.show().await?;
    Ok(())
}
```

#### Choosing the right tool

Now that you've seen both patterns, here's a quick reference for when each API shines:

| SQL excels at                          | DataFrame excels at                   |
| :------------------------------------- | ------------------------------------- |
| Window functions (`ROW_NUMBER`, `LAG`) | Dynamic filtering based on variables  |
| CTEs for multi-step transformations    | Programmatic column selection         |
| Complex JOINs and set operations       | Iterative/conditional transformations |
| Familiar syntax for SQL developers     | Type-safe Rust integration            |

For deeper guidance on when to choose which API, see [When to Choose Which?](concepts.md#when-to-choose-which) in the Concepts guide.

> **Advanced**: For external data sources (PostgreSQL, etc.) via custom [`TableProvider`]s, filters/projections may push down to the source system; remaining operations execute columnar in DataFusion.

#### Additional References

**Concepts & Guides:**

- [Two Paths to the Same Plan](concepts.md#two-paths-to-the-same-plan-parser-vs-builder) — How SQL and DataFrame APIs converge
- [When to Choose Which?](concepts.md#when-to-choose-which) — Decision guide for API selection
- [SQL Reference](../../user-guide/sql/index.rst) — Full SQL syntax, functions, and data types

**API Documentation:**

- [`SessionContext::sql()`][`.sql()`] — Execute SQL, returns a lazy DataFrame
- [`SessionContext::sql_with_options()`][`.sql_with_options()`] — SQL with safety controls (disable DDL, DML, or statements)
- [`.into_view()`] — Convert DataFrame to a view for SQL access
- [`register_table()`][`.register_table()`] — Register a TableProvider (including views) in the catalog

---

### 4. From Arrow [`RecordBatch`]es: The Native Pathway

**Create DataFrames directly from in-memory Arrow `RecordBatch`es—the engine's native format—often with zero-copy overhead.**

When your data is already in [Arrow format], this is the most direct route into DataFusion. No parsing, no schema inference—the data is already in the engine's native format.

A [`RecordBatch`] commonly arrives from:

- **Network streams**: [Arrow Flight] for high-performance data transfer
- **File readers**: Libraries that deserialize into Arrow (e.g., Parquet → RecordBatch)
- **Your application**: Programmatically constructed data or output from other Arrow-native components

Once you have a RecordBatch, you choose between two creation methods:

#### The Architectural Choice: Read vs. Register

When you have a [`RecordBatch`], you face a fundamental decision:

| Aspect            | **One-Shot Query** ([`.read_batch()`])  | **Reusable Table** ([`.register_batch()`])                  |
| ----------------- | --------------------------------------- | ----------------------------------------------------------- |
| **What it does**  | Creates an ephemeral DataFrame directly | Adds the batch to the catalog under a name                  |
| **When to use**   | Immediate, one-off transformations      | Multiple references or SQL access needed                    |
| **How to access** | Pass the DataFrame object around        | Reference by name: [`ctx.table("name")`][`.table()`] or SQL |
| **Analogy**       | Like a temporary variable               | Like a temporary view in a database                         |

#### Pattern 1: One-Shot Query with [`.read_batch()`]

Use this when you want to process a batch immediately and don't need to reference it again. The DataFrame is created directly—no catalog entry, no name.

- [`.read_batch(batch)`][`.read_batch()`] — single RecordBatch
- [`.read_batches(vec![batch1, batch2, ...])`][`.read_batches()`] — multiple RecordBatches

> **Note:** Multiple batches must have identical schemas. They're treated as partitions of one logical table—not physically concatenated—enabling parallel processing.

**Example:** Processing a batch immediately after receiving it:

```rust
use std::sync::Arc;
use datafusion::prelude::*;
use datafusion::arrow::array::{ArrayRef, Int32Array, Float64Array};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::Result;
use datafusion::assert_batches_eq;

#[tokio::test]
async fn test_read_batch_one_shot() -> Result<()> {
    let ctx = SessionContext::new();

    // Assume this batch came from Arrow Flight or another source
    let batch = RecordBatch::try_from_iter(vec![
        ("product_id", Arc::new(Int32Array::from(vec![1, 2, 3, 4])) as ArrayRef),
        ("revenue", Arc::new(Float64Array::from(vec![1200.0, 450.0, 890.0, 2100.0])) as ArrayRef),
    ])?;

    // Process immediately and discard
    let df = ctx.read_batch(batch)?
        .filter(col("revenue").gt(lit(500.0)))?
        .sort(vec![col("revenue").sort(false, true)])?;

    // Verify the filtered and sorted results
    let batches = df.collect().await?;
    assert_batches_eq!(
        &[
            "+------------+---------+",
            "| product_id | revenue |",
            "+------------+---------+",
            "| 4          | 2100.0  |",
            "| 1          | 1200.0  |",
            "| 3          | 890.0   |",
            "+------------+---------+",
        ],
        &batches
    );

    Ok(())
}
```

#### Pattern 2: Reusable Table with [`.register_batch()`]

When you need the data accessible from multiple places—or want SQL access—register the batch as a named table:

```rust
use std::sync::Arc;
use datafusion::prelude::*;
use datafusion::arrow::array::{ArrayRef, Int32Array, Float64Array};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::Result;
use datafusion::assert_batches_eq;

#[tokio::test]
async fn test_register_batch_reusable() -> Result<()> {
    let ctx = SessionContext::new();

    // Create a batch (imagine this came from Arrow Flight or another source)
    let batch = RecordBatch::try_from_iter(vec![
        ("product_id", Arc::new(Int32Array::from(vec![1, 2, 3, 4])) as ArrayRef),
        ("revenue", Arc::new(Float64Array::from(vec![1200.0, 450.0, 890.0, 2100.0])) as ArrayRef),
    ])?;

    // Register the batch as a named table
    ctx.register_batch("live_sales", batch)?;

    // Now query it multiple times, even from SQL
    let high_revenue = ctx.sql(
        "SELECT product_id, revenue
        FROM live_sales
        WHERE revenue > 1000
        ORDER BY revenue DESC"
    ).await?;

    let batches = high_revenue.collect().await?;
    assert_batches_eq!(
        &[
            "+------------+---------+",
            "| product_id | revenue |",
            "+------------+---------+",
            "| 4          | 2100.0  |",
            "| 1          | 1200.0  |",
            "+------------+---------+",
        ],
        &batches
    );

    // Can also access via DataFrame API
    let all_products = ctx.table("live_sales").await?
        .select_columns(&["product_id"])?
        .collect().await?;
    assert_eq!(all_products.len(), 1);  // One batch returned

    Ok(())
}
```

#### Common Pitfalls

When constructing `RecordBatch`es manually, these invariants must hold:

- **Equal length**: All arrays (columns) in a batch must have exactly the same row count
- **Nullable columns**: Must be built with `Option<T>`; non-nullable columns must not contain `None`
- **Multiple batches**: Schemas must be identical (names, types, order, nullability)

> **Need help debugging?** See the full checklist in [Arrow Introduction](../../user-guide/arrow-introduction.md)

#### Record Batch References

**DataFusion:**

- [Arrow Introduction](../../user-guide/arrow-introduction.md) — RecordBatch fundamentals and debugging
- [`SessionContext::read_batch()`](https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.read_batch) — One-shot DataFrame from RecordBatch
- [`SessionContext::register_batch()`](https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.register_batch) — Register RecordBatch as table

**Arrow Ecosystem:**

- [`RecordBatch`](https://docs.rs/arrow/latest/arrow/record_batch/struct.RecordBatch.html) — Arrow's columnar in-memory format
- [Arrow Flight](https://arrow.apache.org/docs/format/Flight.html) — Network protocol for Arrow data

---

### 5. From Inline Data (using the [`dataframe!`] macro)

**Create DataFrames from Rust literals—perfect for tests, examples, and prototyping without external data dependencies.**

This approach shines when your data is small, temporary, and lives entirely in code.

**Perfect for:**

- **Unit tests**: Verify transformations work correctly without file I/O overhead or test data management
- **Documentation examples**: Create self-contained, runnable code snippets that anyone can execute
- **Prototyping**: Quickly experiment with DataFusion's API and operations in REPL or notebooks
- **Benchmarking**: Generate controlled test data with known characteristics for performance testing

**Not ideal for:**

- Production data pipelines (use file-based or streaming sources instead)
- Large datasets (literals are compiled into your binary and loaded into memory)
- Dynamic data (values must be known at compile time)

> **DataFrame API advantage**:<br>
> SQL has no direct equivalent for inline test data. SQL's `VALUES` clause requires a `SessionContext` and produces a query result—not a reusable DataFrame you can transform programmatically.

#### 1. [`dataframe!`] macro: Basic syntax

The dataframe! macro uses a declarative, column-oriented syntax. It mimics the structure of a hash map, where keys are column names and values are lists of data.

**Syntax Pattern:**

```text
dataframe! (
    "column_name" => [value1, value2, ...],
     ... )
```

- Column Name: A string literal (e.g., "id").

* Operator: The => arrow associates the name with its data.

- Data: A Rust vector or array literal (e.g., [1, 2, 3]).

> Note:<br>
> This macro automatically creates a new default SessionContext to host the DataFrame. If you need to attach the data to an existing context (e.g., to share configuration), use ctx.read_batch() instead.

```rust
use datafusion::prelude::*;
use datafusion::error::Result;
use datafusion::assert_batches_eq;

#[tokio::test]
async fn test_dataframe_macro_basic() -> Result<()> {
    // Create DataFrame from inline data
    let df = dataframe!(
        "id" => [1, 2, 3],
        "name" => ["Alice", "Bob", "Carol"]
    )?;

    // Verify the DataFrame contains expected data
    let batches = df.collect().await?;
    assert_batches_eq!(
        &[
            "+----+-------+",
            "| id | name  |",
            "+----+-------+",
            "| 1  | Alice |",
            "| 2  | Bob   |",
            "| 3  | Carol |",
            "+----+-------+",
        ],
        &batches
    );

    Ok(())
}
```

#### Complete testing workflow

The [`dataframe!`] macro pairs perfectly with [`assert_batches_eq!`] for validating DataFrame transformations.

Here is a complete unit test showing the **Three-Step Pattern**.

> **Sophisticated Usage:**<br>
> Notice step 1. Instead of hardcoding literals, we use a standard Rust loop to generate the data programmatically. This demonstrates how to inject dynamic data (e.g., from a fuzzer or random generator) into the declarative macro.

```rust
use datafusion::prelude::*;
use datafusion::assert_batches_eq;

#[tokio::test]
async fn test_filter_and_aggregate() -> datafusion::error::Result<()> {
    // 1) CREATE: Generate data programmatically
    // We want to simulate:
    // - 2 entries for "Sales" (Base salary 50k)
    // - 2 entries for "Engineering" (Base salary 80k)
    let mut departments = Vec::new();
    let mut salaries = Vec::new();

    for i in 0..4 {
        if i < 2 {
            departments.push("Sales");
            salaries.push(50000 + (i * 5000)); // 50000, 55000
        } else {
            departments.push("Engineering");
            salaries.push(80000 + ((i - 2) * 5000)); // 80000, 85000
        }
    }

    // Inject the generated vectors directly into the macro
    let df = dataframe!(
        "department" => departments,
        "salary" => salaries
    )?;

    // 2) TRANSFORM: Apply the operations you want to test
    let result = df
        .aggregate(vec![col("department")], vec![sum(col("salary")).alias("total")])?
        .filter(col("total").gt(lit(100000)))?
        .sort(vec![col("total").sort(false, true)])?;

    // 3) VERIFY: Assert the exact expected output
    // Sales: 50k + 55k = 105k
    // Eng:   80k + 85k = 165k
    let batches = result.collect().await?;
    assert_batches_eq!(
        &[
            "+-------------+--------+",
            "| department  | total  |",
            "+-------------+--------+",
            "| Engineering | 165000 |",
            "| Sales       | 105000 |",
            "+-------------+--------+",
        ],
        &batches
    );

    Ok(())
}
```

This three-step pattern (**CREATE → TRANSFORM → VERIFY**) is your blueprint for testing DataFrames.

#### Testing macros for asserting results

DataFusion provides specialized macros to verify your results. These handle the complexity of formatting Arrow RecordBatches so you don't have to manually iterate over rows.

| Macro                          | Best Use Case                                                                                                                                              |
| :----------------------------- | :--------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Data Verification**          |                                                                                                                                                            |
| [`assert_batches_eq!`]         | **Strict Check.** Use when the output is deterministic (e.g., after a [`.sort()`]). Checks values _and_ order.                                             |
| [`assert_batches_sorted_eq!`]  | **Loose Check.** Use when parallel execution might scramble row order (e.g., aggregations). It sorts both sides before comparing.                          |
| **String & Plan Verification** |                                                                                                                                                            |
| [`assert_contains!`]           | **Partial Match.** Use to check if an error message contains a specific phrase, or if an `EXPLAIN` plan contains a specific operator (e.g., "FilterExec"). |
| [`assert_not_contains!`]       | **Negative Check.** Use to ensure a specific operator was optimized away (e.g., ensuring a "Filter" is no longer present after optimization).              |

> **Pro Tip: The Copy-Paste Workflow**<br>
> When [`assert_batches_eq!`] fails, it prints the actual output in the exact ASCII format expected by the macro. You can simply copy this output from your terminal and paste it into your test code to update the expected result.

#### Special cases

The basic [`dataframe!`] syntax handles most scenarios, but two situations require additional techniques:

**Null values** — Use Rust's `Option<T>` type to represent missing data:

```rust
use datafusion::prelude::*;
# use datafusion::error::Result;
# #[tokio::main]
# async fn main() -> Result<()> {
let df = dataframe!(
    "id" => [1, 2, 3],
    "value" => [Some("foo"), None, Some("bar")],  // Option<T> for nulls
    "score" => [Some(100), Some(200), None]
)?;
# df.show().await?;
# Ok(())
# }
```

> For a deeper understanding of Null handling, see: <br> [Understanding Null Values](../../user-guide/dataframe.md#understanding-null-values-none-null-and-nan) for distinctions between `None`, SQL `NULL`, and `NaN`.

> **Explicit Arrow types** <br>
> The [`dataframe!`] macro infers Arrow types from Rust literals (e.g., `i32` → `Int32`, `&str` → `Utf8`). Use [`DataFrame::from_columns()`][`.from_columns()`] when you need direct control:

| Use [`.from_columns()`] when... | Example                                                                     |
| :------------------------------ | :-------------------------------------------------------------------------- |
| You already have Arrow arrays   | Output from another Arrow-native library or computation                     |
| You need a specific Arrow type  | `Int64` instead of inferred `Int32`, or `Timestamp` with specific precision |
| You're bridging systems         | Receiving arrays from Arrow IPC, Flight, or custom TableProviders           |

```rust
use std::sync::Arc;
use datafusion::prelude::*;
use datafusion::arrow::array::{ArrayRef, Int64Array, StringArray};
use datafusion::error::Result;

#[tokio::main]
async fn main() -> Result<()> {
    // Explicit Int64 (dataframe! would infer Int32 from literals)
    let df = DataFrame::from_columns(vec![
        ("id", Arc::new(Int64Array::from(vec![1_i64, 2, 3])) as ArrayRef),
        ("name", Arc::new(StringArray::from(vec!["Alice", "Bob", "Carol"])) as ArrayRef),
    ])?;
    df.show().await?;
    Ok(())
}
```

> **Decision rule**:<br>
> Start with `dataframe!`—it's readable and sufficient for most tests. Switch to `from_columns()` only when you already have Arrow arrays or need explicit type control that the macro can't infer.

### 2. Generative Data (Calculations & Placeholders)

Sometimes you need a DataFrame purely to evaluate expressions, or you need a schema-compliant "empty" table to handle edge cases in pipelines.

#### 1. The "Calculation Root" ([`ctx.read_empty()`][`.read_empty()`])\*\*

This creates a DataFrame with **one row and zero columns**. It acts like a "blank sheet" (similar to `DUAL` in Oracle or a `SELECT` without `FROM` in Postgres) that allows you to execute scalar expressions.

```rust
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();

    // Create a single-row, column-less DataFrame
    let df = ctx.read_empty()?;

    // Use it to evaluate scalar expressions
    let result = df.select(vec![
        lit(5).mul(lit(5)).alias("result"), // 5 * 5
        now().alias("execution_time")       // Current time
    ])?;

    // FIX: Clone the DataFrame to count it, so we don't consume 'result'
    assert_eq!(result.clone().count().await?, 1);

    result.show().await?;
    Ok(())
}
```

#### 2. The Empty Placeholder (Safe Unions)\*\*

If you need a DataFrame with **zero rows** but a specific schema (e.g., to handle "no data found" cases while keeping a `UNION` valid), do **not** use `read_empty()`. Instead, use `read_batch` with an empty `RecordBatch`.

> **Use Case:**<br> > _The "Structural Placeholder."_<br>
> This creates a valid DataFrame object that contains no data. It acts like an empty container that satisfies function signatures and pipeline requirements (like UNION schemas or Parquet writers) when the actual data is missing or filtered out.
>
> **Testing Tip:** <br>
> Use this to verify that your functions handle "no results" scenarios gracefully without crashing (e.g., avoiding division-by-zero errors in aggregations).

```rust
use datafusion::prelude::*;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::assert_batches_eq;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();

    // 1. Simulate an existing DataFrame
    let df_real = dataframe!("id" => [1, 2, 3])?;

    // 2. Get the schema
    // FIX: Use .inner().clone() to get the Arc<Schema>
    let schema = df_real.schema().inner().clone();

    // 3. Create a truly empty DataFrame (0 rows) with that exact schema
    let empty_batch = RecordBatch::new_empty(schema);
    let df_empty = ctx.read_batch(empty_batch)?;

    // VERIFICATION 1: Prove it is actually empty
    // (We clone here just to be safe, though count() is the last usage of df_empty)
    assert_eq!(df_empty.clone().count().await?, 0);

    // 4. Safe Union: This works because both have column "id"
    let combined = df_real.union(df_empty)?;

    // VERIFICATION 2: Prove the union worked (3 rows + 0 rows = 3 rows)
    let result = combined.collect().await?;
    assert_batches_eq!(
        &[
            "+----+",
            "| id |",
            "+----+",
            "| 1  |",
            "| 2  |",
            "| 3  |",
            "+----+",
        ],
        &result
    );

    Ok(())
}
```

> **Key Distinction:**
>
> - **`read_empty()`**: 1 Row, 0 Columns. (Used for logic/math).
> - **`RecordBatch::new_empty()`**: 0 Rows, N Columns. (Used for data pipelines).

#### Inline data References

**DataFusion:**

- [`dataframe!` macro](https://docs.rs/datafusion/latest/datafusion/macro.dataframe.html) — Create DataFrames from literals
- [`assert_batches_eq!`](https://docs.rs/datafusion/latest/datafusion/macro.assert_batches_eq.html) — Test DataFrame outputs
- [`assert_batches_sorted_eq!`](https://docs.rs/datafusion/latest/datafusion/macro.assert_batches_sorted_eq.html) — Order-insensitive test comparison
- [`DataFrame::from_columns()`](https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.from_columns) — Create from Arrow arrays
- [`ctx.read_empty()`](https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.read_empty) — Create empty DataFrame

---

### 6. Advanced: Constructing from a LogicalPlan

**Access the query tree directly—for plan manipulation, federation, or building your own query DSL.**

A DataFrame wraps a [`LogicalPlan`] and [`SessionState`]—the query tree and execution context. This section shows how to extract, manipulate, and reconstruct DataFrames at the plan level—giving you full control over query structure.

#### When you need this

DataFusion's optimizer typically produces efficient plans automatically. In edge cases, advanced users may need direct plan access:

- **Plan manipulation**: Inject filters, rewrite nodes, or transform queries programmatically
- **Query federation**: Exchange plans with other engines (Substrait, Calcite)
- **Domain-Specific Language (DSL) builders**: Construct queries from your own language or configuration
- **Policy injection**: Add row-level security or tenant isolation transparently

> **For deeper coverage**: See [Building Logical Plans](../../library-user-guide/building-logical-plans.md) for comprehensive plan construction techniques.

#### The core pattern

The following example demonstrates the mechanics of extracting, modifying, and reconstructing a DataFrame at the plan level.

**The steps:**

1.  **Create a DataFrame** — using any creation method.
2.  **Extract the plan** — [`.into_parts()`] returns `(SessionState, LogicalPlan)`.
3.  **Modify the plan** — Wrap it in [`LogicalPlanBuilder`] to apply transformations fluently.
4.  **Reconstruct** — [`DataFrame::new(state, plan)`][`dataframe::new()`] creates a new DataFrame.
5.  **Execute** — proceed with `.collect()`.

```rust
use datafusion::prelude::*;
use datafusion::logical_expr::LogicalPlanBuilder; // Essential for plan modification
use datafusion::error::Result;
use datafusion::assert_batches_eq;

#[tokio::test]
async fn test_plan_manipulation() -> Result<()> {
    // 1. Create a DataFrame
    let df = dataframe!(
        "id" => [1, 2, 3],
        "value" => [10, 20, 30]
    )?;

    // 2. Extract the LogicalPlan and SessionState
    let (state, plan) = df.into_parts();

    // 3. Modify the plan
    // We use LogicalPlanBuilder to easily chain operations on the plan
    let modified_plan = LogicalPlanBuilder::from(plan)
        .filter(col("value").gt(lit(15)))?
        .build()?;

    // 4. Construct a new DataFrame from the modified plan
    let new_df = DataFrame::new(state, modified_plan);

    // 5. Execute and verify
    let batches = new_df.collect().await?;
    assert_batches_eq!(
        &[
            "+----+-------+",
            "| id | value |",
            "+----+-------+",
            "| 2  | 20    |",
            "| 3  | 30    |",
            "+----+-------+",
        ],
        &batches
    );

    Ok(())
}
```

> **Why not just use `df.filter()`?**<br>
> This simple filter _could_ be done with the DataFrame API. The power of plan-level access becomes apparent when you need to:
>
> - Transform nodes **throughout** the tree (not just add to the top)
> - Combine or inspect plans from different sources
> - Inject cross-cutting logic at specific node types (e.g., every `TableScan`)
>
> See the [TreeNodeRewriter pattern](#advanced-plan-rewriting-with-treenoderewriter) below for tree-walking transformations.

**Key methods:**

- [`df.into_parts()`][`.into_parts()`] — Extract `(SessionState, LogicalPlan)` from a DataFrame
- [`DataFrame::new(state, plan)`][`dataframe::new()`] — Construct a DataFrame from state and plan
- [`df.logical_plan()`][`.logical_plan()`] — Get a reference to the plan without consuming the DataFrame
- [`ctx.execute_logical_plan(plan)`][`execute_logical_plan()`] — Execute a LogicalPlan, handling DDL statements specially

> **`DataFrame::new()` vs `execute_logical_plan()`**: Use `DataFrame::new()` for pure plan wrapping. Use `execute_logical_plan()` when your plan might contain DDL (CREATE TABLE, DROP, etc.)—it handles those statements before returning a DataFrame.

(advanced-plan-rewriting-with-treenoderewriter)=

#### Advanced: Plan rewriting with TreeNodeRewriter

The [`TreeNodeRewriter`] trait lets you walk and transform every node in a plan tree—not just append to the top like `df.filter()`. Your rewriter visits each node (bottom-up by default), and you decide whether to transform it, replace it, or leave it unchanged. This is the mechanism behind multi-tenant isolation, audit logging, and query policy injection.

For a complete understanding of tree traversal patterns, see the [`TreeNode`][`treenode`] trait documentation.

**Example skeleton** — This shows the structure; real implementations add domain-specific logic:

```rust,no_run
use datafusion::common::tree_node::{TreeNodeRewriter, Transformed, TreeNode};
use datafusion::logical_expr::LogicalPlan;
use datafusion::common::Result;

struct TenantIsolationRewriter {
    tenant_id: String,
}

impl TreeNodeRewriter for TenantIsolationRewriter {
    type Node = LogicalPlan;

    // Visit nodes bottom-up (f_up)
    fn f_up(&mut self, node: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
        match node {
            LogicalPlan::TableScan(scan) => {
                // REAL IMPLEMENTATION:
                // 1. Check if scan.table_name requires isolation
                // 2. Add a filter: WHERE tenant_id = self.tenant_id
                // 3. Return Transformed::yes(LogicalPlan::TableScan(new_scan))

                // For this skeleton, we return unchanged
                Ok(Transformed::no(LogicalPlan::TableScan(scan)))
            }
            // Pass through all other nodes
            _ => Ok(Transformed::no(node)),
        }
    }
}

// Usage:
// let mut rewriter = TenantIsolationRewriter { tenant_id: "tenant_123".into() };
// let rewritten_plan = plan.rewrite(&mut rewriter)?.data;
```

> **Note**:<br>
> This skeleton shows the pattern. For complete implementations, see [Building Logical Plans](../../library-user-guide/building-logical-plans.md) which covers plan construction in depth.

#### Best practices for logical plan rewriting

- **Re-optimize after rewriting**:<br>
  Your modifications may create new optimization opportunities (predicate pushdown, projection pruning). Call `state.optimize(&plan)` after rewriting to let DataFusion's optimizer work on your transformed plan.

- **Keep rewrites idempotent**:<br>
  If your rewriter runs twice on the same plan, it shouldn't double-add filters or create duplicate nodes. Check for existing modifications before applying new ones.

- **Test plan shapes, not just results**:<br>
  Correct output doesn't guarantee an efficient plan. Use `df.explain(true, false)` to inspect the plan structure and assert that filters pushed down, joins optimized, etc.

#### References

**Guides:**

- [Building Logical Plans](../../library-user-guide/building-logical-plans.md) — Comprehensive plan construction techniques
- [Query Planning Architecture](../../contributor-guide/architecture.md) — How plans flow through the engine

**Deep dives:**

- [Optimizing SQL & DataFrames (Part 1)](https://datafusion.apache.org/blog/2025/06/15/optimizing-sql-dataframes-part-one/) — Understanding plan optimization
- [Optimizing SQL & DataFrames (Part 2)](https://datafusion.apache.org/blog/2025/06/15/optimizing-sql-dataframes-part-two/) — Pushdown limits and edge cases

**API:**

- [`DataFrame::new()`](https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.new) — Construct from SessionState and LogicalPlan
- [`DataFrame::into_parts()`](https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.into_parts) — Extract state and plan
- [`execute_logical_plan()`](https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.execute_logical_plan) — Execute plan with DDL handling
- [`LogicalPlan`](https://docs.rs/datafusion/latest/datafusion/logical_expr/enum.LogicalPlan.html) — The query tree structure
- [`TreeNodeRewriter`](https://docs.rs/datafusion/latest/datafusion/common/tree_node/trait.TreeNodeRewriter.html) — Plan transformation trait
- [`TreeNode`](https://docs.rs/datafusion/latest/datafusion/common/tree_node/trait.TreeNode.html) — Base trait for tree traversal

---

## Conclusion: All Creation Methods

Every method above converges to the same result: a lazy `DataFrame` backed by a `LogicalPlan`. Choose based on your data source and whether you need reuse.

| When you need to...       | Method                                                                   | Notes                                    |
| ------------------------- | ------------------------------------------------------------------------ | ---------------------------------------- |
| Scan files once           | [`.read_parquet()`], [`.read_csv()`], [`.read_json()`], [`.read_avro()`] | Simplest path for file data              |
| Reuse data across queries | `register_*()` then `table()`                                            | Schema cached, SQL + DataFrame access    |
| Run SQL queries           | [`.sql()`]                                                               | Returns DataFrame for further transforms |
| Process Arrow data        | [`.read_batch()`], [`.read_batches()`]                                   | Zero-copy from Arrow ecosystem           |
| Test or prototype         | [`dataframe!`] macro                                                     | No files, inline Rust literals           |
| Control Arrow types       | [`.from_columns()`]                                                      | When macro inference isn't enough        |
| Custom data sources       | [`.read_table()`]                                                        | Any `TableProvider` implementation       |
| Build plans directly      | [`.new()`]                                                               | Advanced: full plan control              |

> **Start simple**: For most use cases, [`.read_parquet()`] or [`.dataframe!`] gets you started. Add registration ([`.register_*()`]) when you need SQL access or query reuse. Drop to [`.DataFrame::new(state, plan)`] only for advanced plan manipulation.

---

## Further Reading

### Internal Guides

| Resource                                                     | Description                                                           |
| ------------------------------------------------------------ | --------------------------------------------------------------------- |
| [Concepts](concepts.md)                                      | Two paths to the same plan, lazy execution, architecture              |
| [Transformations](transformations.md)                        | Add filters, projections, joins, and aggregates (build the lazy plan) |
| [Writing DataFrames](writing-dataframes.md)                  | Execute (`.collect()`, `.execute_stream()`) and write results         |
| [Best Practices](best-practices.md)                          | Performance tuning and correctness tips                               |
| [Building Logical Plans](../building-logical-plans.md)       | Work directly with `LogicalPlan` / `LogicalPlanBuilder`               |
| [Arrow Introduction](../../user-guide/arrow-introduction.md) | Arrow basics: `RecordBatch`, schemas, and columnar memory             |
| [SQL Reference](../../user-guide/sql/index.rst)              | Full SQL syntax, functions, and data types                            |

### API Documentation (docs.rs)

| Type / Trait                                                                                                    | Description                                                    |
| --------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------- |
| [`SessionContext`](https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html)   | Entry point: register data sources, create DataFrames, run SQL |
| [`DataFrame`](https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html)                     | Lazy plan builder; actions trigger execution                   |
| [`LogicalPlan`](https://docs.rs/datafusion-expr/latest/datafusion_expr/logical_plan/enum.LogicalPlan.html)      | Logical representation produced by SQL and DataFrames          |
| [`TableProvider`](https://docs.rs/datafusion/latest/datafusion/datasource/trait.TableProvider.html)             | Data source abstraction used by `SessionContext`               |
| [`RecordBatch`](https://docs.rs/arrow/latest/arrow/record_batch/struct.RecordBatch.html)                        | Arrow's columnar in-memory format                              |
| [`TreeNodeRewriter`](https://docs.rs/datafusion/latest/datafusion/common/tree_node/trait.TreeNodeRewriter.html) | Plan transformation trait for advanced rewrites                |

### External Resources

| Resource                                                                                                                  | Description                               |
| ------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------- |
| [Apache Arrow DataFusion: SIGMOD 2024 Paper](https://dl.acm.org/doi/10.1145/3626246.3653368)                              | Academic paper on DataFusion architecture |
| [Introducing Arrow Flight](https://arrow.apache.org/blog/2019/10/13/introducing-arrow-flight/)                            | High-performance Arrow data transport     |
| [Optimizing SQL & DataFrames (Part 1)](https://datafusion.apache.org/blog/2025/06/15/optimizing-sql-dataframes-part-one/) | Understanding plan optimization           |
| [Optimizing SQL & DataFrames (Part 2)](https://datafusion.apache.org/blog/2025/06/15/optimizing-sql-dataframes-part-two/) | Pushdown limits and edge cases            |
| [Using Rust async for Query Execution](https://datafusion.apache.org/blog/2025/06/30/cancellation/)                       | Async execution and query cancellation    |

---

<!-- ==========================================================================
     REFERENCE-STYLE LINKS
     Keep alphabetized within each section for maintainability.

     Organization:
     1. Internal documentation links
     2. Core types (DataFrame, SessionContext, etc.)
     3. SessionContext methods (alphabetized)
     4. DataFrame methods (alphabetized)
     5. Read options by format (Arrow, Avro, CSV, JSON, Parquet)
     6. External resources and blogs
     ========================================================================== -->

<!-- Internal documentation links -->

[arrow flight]: https://arrow.apache.org/blog/2019/10/13/introducing-arrow-flight/
[arrow format]: ../../user-guide/arrow-introduction.md
[catalog schema]: https://datafusion.apache.org/library-user-guide/catalogs.html
[information_schema]: ../../user-guide/sql/information_schema.md

<!-- Core types (alphabetized) -->

[`listingtable`]: https://docs.rs/datafusion/latest/datafusion/datasource/listing/struct.ListingTable.html
[`catalogprovider`]: https://docs.rs/datafusion/latest/datafusion/catalog/trait.CatalogProvider.html
[`dataframe`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html
[`executionplan`]: https://docs.rs/datafusion/latest/datafusion/physical_plan/trait.ExecutionPlan.html
[`logicalplan`]: https://docs.rs/datafusion-expr/latest/datafusion_expr/logical_plan/enum.LogicalPlan.html
[`memtable`]: https://docs.rs/datafusion/latest/datafusion/datasource/struct.MemTable.html
[`object_store`]: https://docs.rs/object_store/latest/object_store/
[`recordbatch`]: https://docs.rs/arrow/latest/arrow/record_batch/struct.RecordBatch.html
[`runtimeenv`]: https://docs.rs/datafusion/latest/datafusion/catalog/trait.Session.html#tymethod.runtime_env
[`sessioncontext`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html
[`sessionstate`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionState.html
[`tableprovider`]: https://docs.rs/datafusion/latest/datafusion/datasource/trait.TableProvider.html
[`tablescan`]: https://docs.rs/datafusion/latest/datafusion/logical_expr/logical_plan/struct.TableScan.html
[tableprovider_scan]: https://docs.rs/datafusion/latest/datafusion/datasource/trait.TableProvider.html#tymethod.scan
[`treenode`]: https://docs.rs/datafusion/latest/datafusion/common/tree_node/trait.TreeNode.html
[`treenoderewriter`]: https://docs.rs/datafusion/latest/datafusion/common/tree_node/trait.TreeNodeRewriter.html

<!-- SessionContext methods (alphabetized) -->

[`.catalog()`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.catalog
[`.catalog_names()`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.catalog_names
[`catalog_names()`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.catalog_names
[`execute_logical_plan()`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.execute_logical_plan
[`.new()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.new
[`.read_arrow()`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.read_arrow
[`.read_avro()`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.read_avro
[`.read_batch()`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.read_batch
[`.read_batches()`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.read_batches
[`.read_csv()`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.read_csv
[`.read_empty()`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.read_empty
[`.read_json()`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.read_json
[`.read_parquet()`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.read_parquet
[`.read_table()`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.read_table
[`.register_batch()`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.register_batch
[`.register_csv()`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.register_csv
[`.register_listing_table()`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.register_listing_table
[`.register_parquet()`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.register_parquet
[`.register_table()`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.register_table
[`.sql()`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.sql
[`.sql_with_options()`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.sql_with_options
[`.table()`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.table

<!-- DataFrame methods (alphabetized) -->

[`.collect()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.collect
[`.create_physical_plan()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.create_physical_plan
[`.execute_stream()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.execute_stream
[`.explain()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.explain
[`.filter()`]: https://docs.rs/datafusion/latest/datafusion/logical_expr/struct.Filter.html
[`.from_columns()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.from_columns
[`.into_parts()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.into_parts
[`.into_view()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.into_view
[`.logical_plan()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.logical_plan
[`.select()`]: https://docs.rs/datafusion/latest/datafusion/logical_expr/struct.Select.html
[`.show()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.show
[`.sort()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.sort
[`dataframe!`]: https://docs.rs/datafusion/latest/datafusion/macro.dataframe.html
[`dataframe::new`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.new
[`dataframe::new()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.new
[`dataframe::schema()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.schema

<!-- Catalog/Schema methods (alphabetized) -->

[`.schema()`]: https://docs.rs/datafusion/latest/datafusion/catalog/trait.CatalogProvider.html#tymethod.schema
[`.schema_names()`]: https://docs.rs/datafusion/latest/datafusion/catalog/trait.CatalogProvider.html#tymethod.schema_names
[`.table_names()`]: https://docs.rs/datafusion/latest/datafusion/catalog/trait.SchemaProvider.html#tymethod.table_names
[`schema()`]: https://docs.rs/datafusion/latest/datafusion/catalog/trait.CatalogProvider.html#tymethod.schema

<!-- Arrow read options (alphabetized) -->

[`arrowreadoptions`]: https://docs.rs/datafusion/latest/datafusion/datasource/file_format/options/struct.ArrowReadOptions.html
[`arrowreadoptions::schema()`]: https://docs.rs/datafusion/latest/datafusion/datasource/file_format/options/struct.ArrowReadOptions.html#method.schema
[`arrowreadoptions::table_partition_cols()`]: https://docs.rs/datafusion/latest/datafusion/datasource/file_format/options/struct.ArrowReadOptions.html#method.table_partition_cols

<!-- Avro read options (alphabetized) -->

[`avroreadoptions`]: https://docs.rs/datafusion/latest/datafusion/datasource/file_format/options/struct.AvroReadOptions.html
[`avroreadoptions::schema()`]: https://docs.rs/datafusion/latest/datafusion/datasource/file_format/options/struct.AvroReadOptions.html#method.schema
[`avroreadoptions::table_partition_cols()`]: https://docs.rs/datafusion/latest/datafusion/datasource/file_format/options/struct.AvroReadOptions.html#method.table_partition_cols

<!-- CSV read options (alphabetized) -->

[csvreadoptions]: https://docs.rs/datafusion/latest/datafusion/datasource/file_format/options/struct.CsvReadOptions.html#method.new
[`csvreadoptions`]: https://docs.rs/datafusion/latest/datafusion/datasource/file_format/options/struct.CsvReadOptions.html
[`csvreadoptions::delimiter()`]: https://docs.rs/datafusion/latest/datafusion/datasource/file_format/options/struct.CsvReadOptions.html#method.delimiter
[`csvreadoptions::file_compression_type()`]: https://docs.rs/datafusion/latest/datafusion/datasource/file_format/options/struct.CsvReadOptions.html#method.file_compression_type
[`csvreadoptions::file_extension()`]: https://docs.rs/datafusion/latest/datafusion/datasource/file_format/options/struct.CsvReadOptions.html#method.file_extension
[`csvreadoptions::has_header()`]: https://docs.rs/datafusion/latest/datafusion/datasource/file_format/options/struct.CsvReadOptions.html#method.has_header
[`csvreadoptions::newlines_in_values()`]: https://docs.rs/datafusion/latest/datafusion/datasource/file_format/options/struct.CsvReadOptions.html#method.newlines_in_values
[`csvreadoptions::null_regex()`]: https://docs.rs/datafusion/latest/datafusion/datasource/file_format/options/struct.CsvReadOptions.html#method.null_regex
[`csvreadoptions::quote()`]: https://docs.rs/datafusion/latest/datafusion/datasource/file_format/options/struct.CsvReadOptions.html#method.quote
[`csvreadoptions::schema()`]: https://docs.rs/datafusion/latest/datafusion/datasource/file_format/options/struct.CsvReadOptions.html#method.schema
[`csvreadoptions::schema_infer_max_records()`]: https://docs.rs/datafusion/latest/datafusion/datasource/file_format/options/struct.CsvReadOptions.html#method.schema_infer_max_records

<!-- JSON read options (alphabetized) -->

[`ndjsonreadoptions`]: https://docs.rs/datafusion/latest/datafusion/datasource/file_format/options/struct.NdJsonReadOptions.html
[`ndjsonreadoptions::file_compression_type()`]: https://docs.rs/datafusion/latest/datafusion/datasource/file_format/options/struct.NdJsonReadOptions.html#method.file_compression_type
[`ndjsonreadoptions::file_extension()`]: https://docs.rs/datafusion/latest/datafusion/datasource/file_format/options/struct.NdJsonReadOptions.html#method.file_extension
[`ndjsonreadoptions::schema()`]: https://docs.rs/datafusion/latest/datafusion/datasource/file_format/options/struct.NdJsonReadOptions.html#method.schema
[`ndjsonreadoptions::table_partition_cols()`]: https://docs.rs/datafusion/latest/datafusion/datasource/file_format/options/struct.NdJsonReadOptions.html#method.table_partition_cols

<!-- Parquet read options (alphabetized) -->

[`parquetreadoptions`]: https://docs.rs/datafusion/latest/datafusion/datasource/file_format/options/struct.ParquetReadOptions.html
[`parquetreadoptions::file_extension()`]: https://docs.rs/datafusion/latest/datafusion/datasource/file_format/options/struct.ParquetReadOptions.html#method.file_extension
[`parquetreadoptions::file_sort_order()`]: https://docs.rs/datafusion/latest/datafusion/datasource/file_format/options/struct.ParquetReadOptions.html#method.file_sort_order
[`parquetreadoptions::parquet_pruning()`]: https://docs.rs/datafusion/latest/datafusion/datasource/file_format/options/struct.ParquetReadOptions.html#method.parquet_pruning
[`parquetreadoptions::schema()`]: https://docs.rs/datafusion/latest/datafusion/datasource/file_format/options/struct.ParquetReadOptions.html#method.schema
[`parquetreadoptions::skip_metadata()`]: https://docs.rs/datafusion/latest/datafusion/datasource/file_format/options/struct.ParquetReadOptions.html#method.skip_metadata
[`parquetreadoptions::table_partition_cols()`]: https://docs.rs/datafusion/latest/datafusion/datasource/file_format/options/struct.ParquetReadOptions.html#method.table_partition_cols

<!-- Listing options -->

[`listingoptions::infer_schema()`]: https://docs.rs/datafusion-catalog-listing/latest/datafusion_catalog_listing/options/struct.ListingOptions.html#method.infer_schema

<!-- Testing macros (alphabetized) -->

[`assert_batches_eq!`]: https://docs.rs/datafusion/latest/datafusion/macro.assert_batches_eq.html
[`assert_batches_sorted_eq!`]: https://docs.rs/datafusion/latest/datafusion/macro.assert_batches_sorted_eq.html
[`assert_contains!`]: https://docs.rs/datafusion/latest/datafusion/macro.assert_contains.html
[`assert_not_contains!`]: https://docs.rs/datafusion/latest/datafusion/macro.assert_not_contains.html

<!-- External resources and blogs (alphabetized) -->

[comet]: https://datafusion.apache.org/comet/
[custom_table_providers example]: https://github.com/apache/datafusion/blob/main/datafusion-examples/examples/external_dependency/custom_datasource.rs
[datafusion planning]: https://docs.rs/datafusion/latest/datafusion/#planning
[datafusion tokio]: https://docs.rs/datafusion/latest/datafusion/#thread-scheduling-cpu--io-thread-pools-and-tokio-runtimes
[`information_schema`]: https://datafusion.apache.org/user-guide/sql/information_schema.html
[medium article]: https://medium.com/@anowerhossain97/register-the-dataframe-as-a-sql-table-in-pyspark-92cc1387ca02
[parquet_crate]: https://docs.rs/parquet/latest/parquet/
[parquet_docs]: https://parquet.apache.org/docs/file-format/
[parquet_viewer]: https://github.com/XiangpengHao/parquet-viewer
[ruminations on multi-tenant databases]: https://www.db.in.tum.de/research/publications/conferences/BTW2007-mtd.pdf
[running-sql-queries-programmatically]: https://spark.apache.org/docs/latest/sql-getting-started.html#running-sql-queries-programmatically
[schema_infer_max_records_csv]: https://docs.rs/datafusion/latest/datafusion/datasource/file_format/csv/struct.CsvFormat.html#method.with_schema_infer_max_rec
[schema_infer_max_records_json]: https://docs.rs/datafusion/latest/datafusion/datasource/file_format/json/struct.JsonFormat.html#method.with_schema_infer_max_rec
[sigmod-paper]: https://andrew.nerdnetworks.org/pdf/SIGMOD-2024-lamb.pdf
[spark guide]: https://spark.apache.org/docs/latest/sql-programming-guide.html
[tokio_blogpost]: https://datafusion.apache.org/blog/2025/06/30/cancellation/
[tokio_tutorial]: https://tokio.rs/tokio/tutorial

<!-- Rust standard library -->

[`.unwrap()`]: https://doc.rust-lang.org/std/option/enum.Option.html#method.unwrap
