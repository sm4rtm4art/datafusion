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

Every query starts with data. Whether you're reading Parquet from S3, executing SQL, receiving Arrow batches from a Flight stream, or constructing plans programmatically—all paths converge to a lazy [`DataFrame`][DataFrame] backed by a [`LogicalPlan`][LogicalPlan]. This guide covers the _when_ and _how_ of each creation method.

In the [DataFrame lifecycle](./index.md#the-dataframe-lifecycle), creation is where you bind a data source to a query plan. The DataFrame doesn't execute yet—it's a recipe waiting to run. For the conceptual model, see [Concepts](./concepts.md). For what happens next: [Transform](./transformations.md) → [Write](./writing-dataframes.md).

> **Style Note:** <br>
> In this guide, all code elements are highlighted with backticks. DataFrame methods are written as `.method()` (e.g., `.select()`) to reflect the chaining syntax central to the API. This distinguishes them from standalone functions (e.g., `col()`) and static constructors (e.g., `SessionContext::new()`). Rust types are formatted as `TypeName` (e.g., `SchemaRef`).

```{contents}
:local:
:depth: 2
```

## Introduction

**DataFusion provides multiple pathways to create a DataFrame—each optimized for different data sources, yet all converging on a single, powerful abstraction.**

To understand _how_ to create a DataFrame, we must first understand _what_ we are creating. DataFusion is not just a library for reading files; it is a high-performance query engine built on the **Apache Arrow** columnar format. It brings the safety and concurrency of **Rust** to analytical workloads.

### The Philosophy of Convergence

**All creation methods normalize to the same lazy `LogicalPlan`, regardless of whether you start with SQL or the DataFrame API.**

Whether you read a CSV from disk, stream Arrow batches from a network socket, or parse a SQL query, DataFusion normalizes them all into the same structure: a lazy [`DataFrame`][DataFrame] backed by a [`LogicalPlan`][LogicalPlan].

- **The Universal Adapter**:<br>
  The DataFrame API decouples _storage_ from _compute_. You can join a Parquet file from S3 with an in-memory Arrow batch and a PostgreSQL table (via [`TableProvider`]) in a single query.
- **The "Lazy" Contract**:<br>
  Creating a DataFrame is a **metadata-only operation**. When you call `read_parquet`, DataFusion reads only the file footer (schema/statistics), not the data. This means you can safely define DataFrames over petabytes of data on a laptop.
- **Safety by Design**: <br>
  Leveraging Rust’s ownership model, DataFrames are **immutable**. Every transformation (like `.filter()` or `.select()`) consumes the old handle and produces a new one, ensuring that your query plans are side-effect free and thread-safe.

### Architecture: Bringing Compute to Data

**All DataFrame creation paths route through `SessionContext`, which resolves sources (via `TableProvider`s) into a single `LogicalPlan`.**

**How to read this diagram:** <br>
Start at **Sources**, follow the arrows to the corresponding **`TableProvider`**, then into **`SessionContext`** (the catalog + configuration hub). Both **SQL** and **DataFrame** APIs build the same **`LogicalPlan`**. **Actions consume the `DataFrame` handle** and either stream ([`.execute_stream()`]), buffer ([`.collect()`]), or persist (`.write_*()`) results.

```text
DATAFRAME CREATION PATHWAYS
────────────────────────────────────────────────────────────────────────────

[ SOURCES ]                      (All roads lead to TableProvider)
┌──────────────┐ ┌──────────────┐ ┌──────────────┐ ┌─────────────────────┐
│ Files/Stores │ │ In-Memory    │ │ External DBs │ │ Catalogs / Formats  │
│(Parquet/CSV) │ │ (Batches)    │ │ & Streaming  │ │ (Iceberg, Delta,...)│
└──────┬───────┘ └──────┬───────┘ └──────┬───────┘ └──────────┬──────────┘
       ▼                ▼                ▼                    ▼
┌──────────────┐ ┌───────────────┐┌──────────────┐ ┌─────────────────────┐
│ ListingTable │ │   MemTable    ││ Custom Table │ │ CatalogProvider /   │
│ (File Scan + │ │ (In-Memory    ││ Provider     │ │ SchemaProvider ➔    │
│ Statistics)  │ │  Batches)     ││ (Pushdown?)  │ │ TableProvider(s)    │
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

**Key Architectural features enabling this:**

- **Async Native**:<br>
  DataFusion runs CPU-bound operators as async `RecordBatch` streams scheduled on Tokio runtimes (work-stealing thread pools). See [Thread scheduling in DataFusion][tokio_runtimes].
- **Vectorized Execution**:<br>
  All data flows as Arrow arrays, enabling SIMD optimizations and zero-copy integration with the broader Arrow ecosystem.
- **Extensibility**:<br>
  Through the [`TableProvider`] trait, you can teach DataFusion to read from _any_ custom source (Kafka, Delta Lake, or proprietary APIs). This extensibility even opens the door for hardware acceleration (like GPUs via [Comet]).

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

## Before Creating a DataFrame

**Before you pick a creation method, understand how `SessionContext` organizes and resolves data sources.**

DataFusion is an "out of the box" query engine, but for a working query engine and optimal results _query engines have rules_: data sources must be registered or scanned, names must be resolved, and schemas must align. This section covers the catalog model that makes these rules work.

[`SessionContext`][SessionContext] is the entry point for creating DataFrames—it owns the catalog (registered tables), configuration, and runtime. When you create a [`DataFrame`], it captures a snapshot of this state, which is why SQL and the DataFrame API seamlessly interoperate.

> **Already familiar with DataFusion's catalog?** <br>
> Skip to [How to create a DataFrame](#how-to-create-a-dataframe).<br>
> For architecture, see [Concepts](concepts.md).

### Understanding DataFusion's Data Organization

**`SessionContext` is your session-local catalog: it maps names to `TableProvider`s.**

Tables live under a three-level hierarchy (**Catalog → Schema → Table**), which keeps queries readable and enables SQL interoperability and metadata discovery.

DataFusion always has a default catalog (`datafusion`) and schema (`public`). When you register a table, you choose its name (for example `"sales"` or `"warehouse.analytics.metrics"`), and that name determines where the `TableProvider` is stored. The following schema illustrates the default namespace with "sales" as target table.

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
let sales_df = ctx.table("sales").await?;           // DataFrame API
let sales_sql = ctx.sql("SELECT * FROM sales").await?;  // SQL API
```

> **Choosing a Creation Method**
>
> | When to use                    | Method                                 | Learn more                                                                 |
> | ------------------------------ | -------------------------------------- | -------------------------------------------------------------------------- |
> | Use files once                 | [`.read_parquet()`], [`.read_csv()`]   | [From Files](#1-from-files)                                                |
> | Reuse across queries or in SQL | [`.register_parquet()`] → [`.table()`] | [From a Registered Table](#2-from-a-registered-table)                      |
> | Arrow data already in memory   | [`.read_batch()`]                      | [From Arrow RecordBatches](#4-from-arrow-recordbatches-the-native-pathway) |
> | Inline data or tests           | [`dataframe!`][`dataframe!`]           | [From Inline Data](#5-from-inline-data-using-the-dataframe-macro)          |
> | Explore what's registered      | [`.catalog_names()`], `SHOW TABLES`    | [Catalogs Guide](../catalogs.md)                                           |
>
> **Trade-offs:**
>
> - **Registration:** Upfront metadata reads; requires refresh strategy if files change out-of-band.
> - **Direct reads:** No catalog state; metadata re-derived per call; not discoverable via SQL.
>
> **Default rule:** Parquet, remote storage, or multi-file → register. Small, local, one-off → direct read.

#### How Names Resolve

DataFusion resolves table names using **1-, 2-, or 3-part identifiers**:

| Identifier                    | Resolves to                  | Use case              |
| ----------------------------- | ---------------------------- | --------------------- |
| `"sales"`                     | `datafusion.public.sales`    | Default (most common) |
| `"analytics.sales"`           | `datafusion.analytics.sales` | Custom schema         |
| `"warehouse.analytics.sales"` | Fully qualified              | Multi-catalog setups  |

- **Default namespace:**<br>
  Unqualified names land in `datafusion.public`. The `"public"` schema is just a convention—no security implications.
- **Lifetime:**<br>
  Registrations are in-memory, scoped to the [`SessionContext`][SessionContext]. For persistence, implement a custom [`CatalogProvider`].
- **Case sensitivity:**<br>
  Unquoted identifiers fold to lowercase; quote to preserve case (`"Sales"`).

> **Working with cloud storage?** <br>
> Register an object store before using `s3://`, `gs://`, or `az://` paths. See the [CLI datasources guide](../../user-guide/cli/datasources.md) for configuration examples and [`datafusion::datasource::object_store`](https://docs.rs/datafusion/latest/datafusion/datasource/object_store/index.html) for the API.
>
> <!-- TODO: Create dedicated library-user-guide/object-stores.md covering programmatic ObjectStore registration (RuntimeEnv::register_object_store), credential handling, and S3/GCS/Azure setup. The CLI guide covers SQL; library users might need Rust examples. -->

> **Performance note:**<br>
> Registration caches schema and metadata (especially valuable for Parquet footers), so repeated queries plan faster.
>
> **See also:**
>
> - [Catalogs Guide](../catalogs.md)
> - [Custom Table Providers](../custom-table-providers.md)

<details>
<summary><strong>Example: Fully-qualified namespace setup</strong></summary>

Use multi-part names like `warehouse.analytics.metrics` to separate domains or environments:

```rust
use std::sync::Arc;

use datafusion::assert_batches_eq;
use datafusion::catalog::{CatalogProvider,
                          MemoryCatalogProvider,
                          MemorySchemaProvider};
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

- [Apache DataFusion: A Fast, Embeddable, Modular Analytic Query Engine (Section 5.2)][sigmod-paper][— SIGMOD 2024 paper on DataFusion's architecture]

---

## How to Create a DataFrame

**DataFusion binds to data wherever it lives—multiple entry points, one destination—unifying files, streams, and catalogs into a single, lazy DataFrame.**

The [previous section](#before-creating-a-dataframe) established how DataFusion organizes data: `SessionContext` owns the catalog, tables are accessed through [`TableProvider`], and names resolve through the catalog hierarchy. Now we put that foundation to work.

Creating a DataFrame does not load data—it constructs a [`LogicalPlan`][LogicalPlan] describing _what_ to compute. The actual bytes flow only when you call an action (`.collect()`, `.show()`). This lazy model lets DataFusion optimize your entire query before touching any data.

Because every creation method compiles to the same `LogicalPlan`, you can:

- Read files directly for ad-hoc analysis
- Register tables for SQL interoperability
- Mix both approaches in the same pipeline

**Choose based on where your data lives and how you'll access it:**

| Category        | Method                                                             | Best for                                          | Avoid if                                  |
| --------------- | ------------------------------------------------------------------ | ------------------------------------------------- | ----------------------------------------- |
| **Direct Read** | [1. Files](#1-from-files)                                          | Ad-hoc analysis, ETL pipelines, one-off scripts   | You need SQL access to the table          |
| **Catalog**     | [2. Registered Table](#2-from-a-registered-table)                  | SQL interoperability, shared schemas, multi-query | One-shot queries (overhead of naming)     |
| **Hybrid**      | [3. SQL Queries](#3-from-sql-queries)                              | Complex joins, CTEs, window functions             | Dynamic logic, strong typing requirements |
| **Native**      | [4. RecordBatches](#4-from-arrow-recordbatches-the-native-pathway) | Arrow Flight, IPC, inter-process communication    | Starting from scratch                     |
| **Testing**     | [5. Inline Data](#5-from-inline-data-using-the-dataframe-macro)    | Unit tests, reproducible bug reports              | Data > 10K rows                           |
| **Advanced**    | [6. LogicalPlan](#6-advanced-constructing-from-a-logicalplan)      | Custom DSLs, federation, optimizer testing        | Higher-level methods (1–5) suffice        |

#### The Big Picture

DataFusion operates as a hub connecting physical data sources to logical query plans:

```text
DATAFRAME CREATION PATHWAYS
════════════════════════════════════════════════════════════════════════════

[ 1. DATA SOURCES ]              (Where the data lives)
┌──────────────┐ ┌──────────────┐ ┌──────────────┐ ┌───────────────────────┐
│ Files/Stores │ │ In-Memory    │ │ External DBs │ │ Catalogs / Formats    │
│(Parquet/CSV) │ │ (Batches)    │ │ & Streaming  │ │ (Iceberg, Delta, ...) │
└──────┬───────┘ └──────┬───────┘ └──────┬───────┘ └──────────┬────────────┘
       │                │                │                    │
       ▼                ▼                ▼                    ▼
[ 2. ADAPTERS ]                  (The "TableProvider" implementations)
┌──────────────┐ ┌──────────────┐ ┌──────────────┐ ┌───────────────────────┐
│ ListingTable │ │   MemTable   │ │ Custom Table │ │ CatalogProvider /     │
│ (File Scan)  │ │  (Batches)   │ │ Provider     │ │ SchemaProvider        │
└──────┬───────┘ └──────┬───────┘ └──────┬───────┘ └──────────┬────────────┘
       │                │                │                    │
       └────────────────┴────────┬───────┴────────────────────┘
                                 ▼
[ 3. ACCESS PATTERN ]     (How you introduce it to the Session)
            ┌────────────────────┴────────────────────┐
            │                                         │
   ┌────────▼─────────┐                      ┌────────▼─────────┐
   │  A. DIRECT READ  │                      │   B. REGISTER    │
   │  (Anonymous)     │                      │   (Named)        │
   │                  │                      │                  │
   │ ctx.read_parquet │                      │ ctx.register_*   │
   │ ctx.read_csv     │                      │ ctx.register_udf │
   │ ctx.read_batch   │                      │                  │
   └────────┬─────────┘                      └────────┬─────────┘
            │                                         │
            │ (Ephemeral Plan)                        │ (Stored in Catalog)
            ▼                                         ▼
[ 4. THE HUB ]                                  [ CATALOG ]
┌─────────────────────────────────────────────────────▼────────────────────┐
│                             SessionContext                               │
│ ┌──────────────────────────────────────────────────────────────────────┐ │
│ │  SessionState: Config · RuntimeEnv · Optimizer · Planner · Catalog   │ │
│ │                                                                      │ │
│ │  ┌─────────────────┐                     ┌────────────────────────┐  │ │
│ │  │ Anonymous Table │ <──(Resides in)───> │ Registered Tables      │  │ │
│ │  └─────────────────┘      Memory         │ "sales", "metrics"...  │  │ │
│ └───────────┬───────────────────────────────────────┬──────────────────┘ │
└─────────────┼───────────────────────────────────────┼────────────────────┘
              │                                       │
              │ (Direct Return)                       │ (ctx.table("sales"))
              │                                       │ (ctx.sql("..."))
              ▼                                       ▼
       ┌─────────────────────────────────────────────────────┐
       │                      DataFrame                      │
       │           (LogicalPlan + State Snapshot)            │
       └─────────────────────────────────────────────────────┘
```

> **Understanding the layers:**
>
> - **Layer 1 (Data Sources)**: Where bytes live (S3, disk, RAM).
> - **Layer 2 (Adapters)**: [`TableProvider`] traits that translate bytes to Arrow.
> - **Layer 3 (Access Pattern)**:
>   - **Anonymous (Direct Read)**: The table exists only inside the returned DataFrame.
>   - **Named (Registered)**: The table is stored in `SessionContext`, accessible via SQL.
> - **Layer 4 (The Hub)**: `SessionContext` holds configuration and catalogs—the factory for all DataFrames.

---

<!-- Link references -->

### 1. From Files

**Read files directly into a lazy `DataFrame`. Format choice determines optimization potential—Parquet enables predicate pushdown; other formats (CSV and JSON) require full scans.**

Files are a common entry point—data lakes, ETL pipelines, local analysis—but DataFusion's strength is **fusion**: the same query can join a Parquet file with a PostgreSQL table or a streaming source. This section covers file-based access.

All file reads are **lazy**: DataFusion builds a query plan without loading data until you call an action ([`.collect()`], [`.show()`]).

**Basic pattern:** `ctx.read_<format>(path, options).await?`

| Parameter   | Type                  | Description                                                       |
| ----------- | --------------------- | ----------------------------------------------------------------- |
| `path`      | `impl DataFilePaths`  | Single file, `Vec<&str>`, glob pattern, or cloud URL (`s3://...`) |
| `options`   | `<Format>ReadOptions` | Format-specific configuration (schema, compression, etc.)         |
| **Returns** | `Result<DataFrame>`   | Lazy DataFrame                                                    |

> **Warning: Cloud Storage (S3, GCS, Azure)** <br>
> DataFusion does not bundle cloud connectors by default. To use `s3://`, `gs://`, or `az://` paths, you must first register the corresponding `ObjectStore` with your `SessionContext`.
> See [**Advanced: Object Store Configuration**](dataframes-advance.md#object-store-configuration) for setup details.

As an example, here's how to read a Parquet file:

```rust
use datafusion::prelude::*;
use datafusion::test_util::parquet_test_data;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();

    // Use DataFusion's test data (parquet-testing submodule)
    let testdata = parquet_test_data();

    // Read a Parquet file — lazy scan, nothing loads until an action
    let df = ctx.read_parquet(
        &format!("{testdata}/alltypes_plain.parquet"),
        ParquetReadOptions::default()
    ).await?;

    df.show().await?;
    Ok(())
}
```

#### Choosing a File Format

DataFusion supports five file formats. Storage layout (columnar vs row-oriented) strongly affects query performance:

| Format                              | Layout   | Startup Cost                | Predicate Pushdown  | Best For                         |
| ----------------------------------- | -------- | --------------------------- | ------------------- | -------------------------------- |
| **Parquet** <br>[`.read_parquet()`] | Columnar | **Low** (metadata only)     | ✅ Yes (statistics) | Production analytics, large data |
| **Arrow IPC** <br>[`.read_arrow()`] | Columnar | **Instant** (zero-copy)     | ❌ No               | Arrow ecosystem, zero-copy       |
| **Avro**<br> [`.read_avro()`]       | Row      | **Low** (header schema)     | ⚠️ Limited          | Kafka, schema evolution          |
| **CSV**<br> [`.read_csv()`]         | Row      | **High** (inference scan)   | ❌ No               | Simple exchange, imports         |
| **NDJSON** <br> [`.read_json()`]    | Row      | **Medium** (inference scan) | ❌ No               | Semi-structured logs/APIs        |

> For analytics, prefer **columnar formats** (Parquet, Arrow IPC). Columnar storage lets DataFusion read only the needed columns, drastically reducing I/O. Row-based formats (Avro, CSV, JSON) must read entire rows even when you need one field.

Each format has a dedicated section below with options, gotchas, and examples.

---

### Parquet — The Analytical Standard

**The default choice for analytical workloads: columnar, compressed, self-describing, and optimized for selective reads.**

[Apache Parquet](https://parquet.apache.org/) stores data **column-by-column** instead of row-by-row. This layout lets DataFusion read only the columns your query needs, skip irrelevant data using embedded statistics, and benefit from excellent compression ratios.

```rust
use datafusion::prelude::*;
use datafusion::test_util::parquet_test_data;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();

    // Use DataFusion's test data (parquet-testing submodule)
    let testdata = parquet_test_data();
    let df = ctx.read_parquet(
        &format!("{testdata}/alltypes_plain.parquet"),
        ParquetReadOptions::default()
    ).await?;

    df.show().await?;
    Ok(())
}
```

For typical analytical queries, Parquet is often **significantly faster** than CSV—not because of raw read speed, but because tools like DataFusion can skip reading large portions of data (row group pruning) and avoid reading unneeded columns (column pruning).

#### Parquet File Structure

```text
PARQUET FILE STRUCTURE & SCANNING LOGIC
════════════════════════════════════════════════════════════════════════════

  Query: SELECT "price" FROM table WHERE "date" = '2026-01-02'

         (Physically stored on disk)        (DataFusion Reader Logic)
┌──────────────────────────────────────────┐
│             PARQUET FILE                 │  1. READ FOOTER FIRST
│                                          │  (Load Schema & Stats)
│  ┌────────────────────────────────────┐  │            │
│  │           FILE FOOTER              │◄──────────────┘
│  │ (Schema, Row Group Metadata, Stats)│  │
│  └────────────────────────────────────┘  │
│                                          │
│  ┌────────────────────────────────────┐  │  2. ROW GROUP PRUNING
│  │           ROW GROUP 1              │  │  Check Stats:
│  │     (Rows 0 - 10,000)              │  │  "date" min: '2026-01-01'
│  │                                    │  │  "date" max: '2026-01-01'
│  │ ┌───────────┐  ┌───────────┐       │  │
│  │ │ Col: date │  │ Col: price│       │  │  Result: SKIP ENTIRE GROUP
│  │ └───────────┘  └───────────┘       │  │  (No IO for these columns)
│  └────────────────────────────────────┘  │
│                                          │
│  ┌────────────────────────────────────┐  │  3. COLUMN PRUNING
│  │           ROW GROUP 2              │  │  Check Stats:
│  │     (Rows 10,001 - 20,000)         │  │  "date" min: '2026-01-02'
│  │                                    │  │  "date" max: '2026-01-02'
│  │ ┌───────────┐  ┌───────────┐       │  │  Result: MATCH!
│  │ │ Col: date │  │ Col: price│◄──────┼─────Read only "price" column
│  │ └───────────┘  └───────────┘       │  │  (Skip "date" data after
│  └────────────────────────────────────┘  │   verifying stats)
└──────────────────────────────────────────┘
```

> **Why is metadata a footer at the bottom?** <br>
> Parquet is a "write-once" format: statistics aren't known until all data is written, so the footer goes last. Parquet readers seek to the end of the file first—then they know where everything else is.

Parquet files organize data into **row groups** (horizontal partitions, typically 128MB) containing **column chunks**:

| Component         | Description                                                                                                            |
| ----------------- | ---------------------------------------------------------------------------------------------------------------------- |
| **Row groups**    | Independent horizontal slices (~128MB each). Each can be read/skipped separately.                                      |
| **Column chunks** | One per column per row group. **Stored contiguously on disk**—enables efficient seeks to read only the needed columns. |
| **Statistics**    | Min/max values, null count per column chunk—enables row group pruning.                                                 |
| **Footer**        | Schema + row group metadata. Read first to plan the query (~few KB).                                                   |

For more details, see:

- [Parquet File Format][parquet_docs] — Official documentation.
- [Parquet rust crate][parquet_crate] documentation.
- [Parquet Viewer][parquet_viewer] — Explore schema, row groups, and statistics visually.

**Practical considerations:**

- **Row group size** <br>
  Commonly ~128MB (writer-dependent). Larger (512MB–1GB) improves sequential I/O; smaller improves pruning granularity.
- **Schema evolution** <br>
  Columns can be added but renaming/reordering is limited. Plan schema upfront.
- **Compression** <br>
  Set at write time (Snappy, Zstd, Gzip). Reader handles any compression automatically. See [Writing Parquet](writing-dataframes.md#writing-to-parquet) for compression options.

#### Trade-offs

| Parquet shines ✓                                       | Avoid Parquet ✗                                                   |
| ------------------------------------------------------ | ----------------------------------------------------------------- |
| Production analytics, repeated queries, large datasets | Write-heavy append logs → NDJSON / streaming-native systems       |
| Selective reads (filters + column pruning)             | Human-editable debugging → CSV/JSON                               |
| Efficient storage (compression + columnar layout)      | Very small datasets where metadata/compression overhead dominates |

#### How Parquet Reads Work

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

#### ParquetReadOptions

[`ParquetReadOptions`] provides builder methods for customization which means you can chain the desired options:

| Option                                                                           | Default      | Description                                           | When to use                                                                             |
| :------------------------------------------------------------------------------- | :----------- | :---------------------------------------------------- | :-------------------------------------------------------------------------------------- |
| **[`.parquet_pruning(bool)`][`ParquetReadOptions::parquet_pruning()`]**          | `true`       | Skips row groups using min/max statistics.            | **Always**, especially for filtered queries (`WHERE id > 100`).                         |
| **[`.table_partition_cols(Vec)`][`ParquetReadOptions::table_partition_cols()`]** | `[]`         | Maps directory paths to columns (e.g., `year=2025`).  | **Hive Partitioning**: When data is organized in folders by date/category.              |
| **[`.file_extension(&str)`][`ParquetReadOptions::file_extension()`]**            | `".parquet"` | Filters input files by suffix.                        | **Mixed Directories**: If your folder contains `.crc`, `.json`, or temp files.          |
| **[`.schema(&Schema)`][`ParquetReadOptions::schema()`]**                         | `None`       | Supplies the Parquet _file_ schema (skips inference). | **Production**: Avoid inference on large/multi-file datasets; enforce specific types.   |
| **[`.skip_metadata(bool)`][`ParquetReadOptions::skip_metadata()`]**              | `true`       | Ignores embedded schema metadata to avoid conflicts.  | **Default**: Keep `true` for mixed producers; set `false` only if you rely on metadata. |
| **[`.file_sort_order(Vec)`][`ParquetReadOptions::file_sort_order()`]**           | `[]`         | Tells the optimizer the data is pre-sorted.           | **Sorting**: To speed up merge-joins or `ORDER BY` queries without re-sorting.          |

> **Note:** <br> > [`ParquetReadOptions::schema()`] here is a _builder method_ that sets the schema for reading. This differs from [`DataFrame::schema()`], which _returns_ the schema of an existing DataFrame.

> **Key insight:** <br> > **Row group pruning is enabled by default** (`datafusion.execution.parquet.pruning = true`) and can be overridden per read with `.parquet_pruning(true/false)`. With pruning enabled, DataFusion compares your `WHERE` predicates against each row group's min/max statistics—if no rows can possibly match, the entire group is skipped without reading any data.

<details>
<summary><strong>Example: Partition-pruned read with custom extension</strong></summary>

In the following example, it is shown how to apply custom options to the `read_parquet` method for Hive-partitioned data.

```rust,no_run
use datafusion::prelude::*;
use datafusion::arrow::datatypes::{DataType, Field, Schema};

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();

    // 1. Define the Parquet *file* schema (optional, but recommended for production).
    // Partition columns (year/month) come from the directory structure via `table_partition_cols`.
    let file_schema = Schema::new(vec![Field::new("product_id", DataType::Utf8, false)]);

    // 2. Configure the reader
    let options = ParquetReadOptions::default()
        .file_extension(".parquet")             // Filter files
        .table_partition_cols(vec![             // Define partition columns
            ("year".into(), DataType::Int32),
            ("month".into(), DataType::Int32)
        ])
        .parquet_pruning(true)   // Enable statistics pruning
        .schema(&file_schema);   // Enforce file schema (excluding partitions)

    // 3. Read Hive-partitioned directory (e.g., data/sales/year=2024/month=01/*.parquet)
    let df = ctx.read_parquet("data/sales/", options).await?;
    df.show().await?;
    Ok(())
}
```

</details>

#### Parquet Pushdown — How DataFusion Skips Data

DataFusion's Parquet reader exploits metadata at multiple levels to minimize I/O:

| Level         | What's skipped             | How it works                                                   |
| ------------- | -------------------------- | -------------------------------------------------------------- |
| **Partition** | Entire directories         | Hive-style paths (`year=2024/`) matched against `WHERE` clause |
| **Row group** | Groups of rows (~128MB)    | Min/max statistics compared to filter predicates               |
| **Page**      | Pages within column chunks | Page-level indexes (if written by producer)                    |
| **Column**    | Unreferenced columns       | Only columns in `SELECT` are read from disk                    |

**Bloom filters** add another layer: if the file producer wrote Bloom filters, DataFusion can skip row groups that _definitely_ don't contain a value (useful for `WHERE id = 'abc123'`).

For highly selective queries where built-in statistics aren't enough, DataFusion supports advanced indexing:

- **User-defined indexes** — Embed custom indexes directly in Parquet file metadata
- **External indexes** — Store sidecar index files alongside your Parquet data

See the References section for deep dives on these techniques.

#### References

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
use datafusion::test_util::arrow_test_data;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();

    // Use Arrow's test data (arrow-testing submodule)
    let testdata = arrow_test_data();
    let df = ctx.read_csv(
        &format!("{testdata}/csv/aggregate_test_100.csv"),
        CsvReadOptions::new()
    ).await?;

    df.show().await?;
    Ok(())
}
```

**Practical considerations:**

- **No embedded schema** <br>
  DataFusion infers types from the first 1000 rows. Use `.schema_infer_max_records(n)` if early rows aren't representative, or provide an explicit schema in production.
- **No predicate pushdown** <br>
  Every byte must be read and parsed, even if filtered later.
- **Projection reduces CPU, not I/O** <br>
  DataFusion skips _materializing_ unused columns, but still scans the full file.
- **Compression** <br>
  Use `.csv.gz` or `.csv.zst` for transfer; DataFusion reads them directly via `.file_compression_type()`.

#### Trade-offs

| CSV Shines ✓                                         | Avoid CSV ✗                                     |
| ---------------------------------------------------- | ----------------------------------------------- |
| Data exchange with spreadsheets, legacy systems      | Analytics on large datasets → Parquet           |
| Human inspection and quick debugging                 | Schema enforcement critical → Parquet/Avro      |
| One-off exports, universal compatibility             | Storage efficiency matters (5–10x larger)       |
| Small datasets where Parquet overhead isn't worth it | You need predicate pushdown (filter before I/O) |

#### CsvReadOptions

[`CsvReadOptions`] provides builder methods for customization. The most important ones for production use are [`CsvReadOptions::schema()`] (for explicit type control) and [`.delimiter()`][`CsvReadOptions::delimiter()`] (for non-comma separators like TSV).

| Option                                                                             | Default        | Description                                           | When to use                                                                                            |
| :--------------------------------------------------------------------------------- | :------------- | :---------------------------------------------------- | :----------------------------------------------------------------------------------------------------- |
| **[`.has_header(bool)`][`CsvReadOptions::has_header()`]**                          | `true`         | Treats the first row as column names.                 | **Standard CSVs**: Set to `false` if the file starts immediately with data.                            |
| **[`.delimiter(u8)`][`CsvReadOptions::delimiter()`]**                              | `b','`         | Sets the field separator character.                   | **Non-Standard**: Use `b'\t'` for TSV or `b';'` for European CSV.                                      |
| **[`.schema(&Schema)`][`CsvReadOptions::schema()`]**                               | `None`         | Provides explicit column names and types.             | **Production**: Enforces strict types and avoids inference surprises.                                  |
| **[`.schema_infer_max_records(n)`][`CsvReadOptions::schema_infer_max_records()`]** | `1000`         | Number of rows to scan to guess types.                | **Sparse Data**: Increase this if the first 1000 rows contain `nulls` in a column that later has data. |
| **[`.quote(u8)`][`CsvReadOptions::quote()`]**                                      | `b'"'`         | Character used to quote fields containing delimiters. | **Custom Dialects**: If your file uses single quotes (`'`) or other wrappers.                          |
| **[`.file_compression_type(...)`][`CsvReadOptions::file_compression_type()`]**     | `UNCOMPRESSED` | Sets the compression algorithm (GZIP, BZIP2, ZSTD).   | **Compressed Files**: Reading `.csv.gz` or `.csv.zst` directly.                                        |
| **[`.newlines_in_values(bool)`][`CsvReadOptions::newlines_in_values()`]**          | `false`        | Allows newlines `\n` inside quoted fields.            | **Multi-line Text**: **Warning**: This disables parallel file scanning (slower).                       |
| **[`.null_regex(str)`][`CsvReadOptions::null_regex()`]**                           | `None`         | Treats specific strings (e.g., `"NA"`) as null.       | **Data Cleaning**: When data uses non-standard null markers.                                           |
| **[`.file_extension(&str)`][`CsvReadOptions::file_extension()`]**                  | `".csv"`       | Filters input files by extension.                     | **Mixed Directories**: To ignore metadata files in the same folder.                                    |

> **Note:** [`CsvReadOptions::schema()`] here is a _builder method_ that sets the schema for reading. This differs from [`DataFrame::schema()`], which _returns_ the schema of an existing DataFrame.

<details>
<summary><strong>Example: Reading compressed CSV with explicit schema</strong></summary>

This example shows how to read a GZIP-compressed CSV file with an explicit schema—a common pattern for log ingestion pipelines.

```rust,no_run
use datafusion::prelude::*;
use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use datafusion::datasource::file_format::file_compression_type::FileCompressionType;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();

    // 1. Define schema upfront (skips inference, enforces types)
    let schema = Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("timestamp", DataType::Timestamp(TimeUnit::Microsecond, None), false),
    ]);

    // 2. Configure reader for GZIP compressed files
    let options = CsvReadOptions::new()
        .schema(&schema)
        .file_compression_type(FileCompressionType::GZIP);

    // 3. Read — DataFusion decompresses on the fly
    let df = ctx.read_csv("logs.csv.gz", options).await?;

    df.show().await?;
    Ok(())
}
```

</details>

#### Production Tips

- **Always provide explicit schema** <br>
  Schema inference is risky: if row 1001 has a different type than rows 1–1000, your query fails at runtime.
- **Use `.csv.gz`** for compressed transfer — DataFusion decompresses on the fly
- **Set `.null_regex("NA|NULL|\\N")`** to handle common null markers
- **Watch for empty strings vs nulls** — They're treated differently

<details>
<summary><strong>Example: Explicit schema for production</strong></summary>

```rust,no_run
use datafusion::prelude::*;
use datafusion::arrow::datatypes::{Schema, Field, DataType};

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();

    let schema = Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("amount", DataType::Float64, true),
    ]);

    let df = ctx.read_csv(
        "data.csv",
        CsvReadOptions::new().schema(&schema)
    ).await?;

    df.show().await?;
    Ok(())
}
```

</details>

#### References

- [`CsvReadOptions` API](https://docs.rs/datafusion/latest/datafusion/prelude/struct.CsvReadOptions.html) — All configuration options
- [Example Usage (CSV with SQL and DataFrame)](../../user-guide/example-usage.md)

---

### NDJSON — Semi-Structured Logs

**Newline-delimited JSON: one JSON object per line, ideal for logs and streaming data.**

NDJSON (also called JSON Lines, `.jsonl`) is row-oriented text like CSV, but each line is a self-describing JSON object. When you call `read_json()`, DataFusion must:

1. **Infer schema** <br>
   By scanning the first N objects (default: 1000)—this happens _at DataFrame creation_, not lazily
2. **Parse JSON → typed Arrow columns** <br>
   Object by object—nested structures flatten to Arrow structs

This makes NDJSON ideal for _data interchange_—log files, NoSQL database exports (MongoDB, Elasticsearch), streaming APIs, and message queue payloads. For repeated analytical queries, convert to Parquet.

```rust,no_run
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();

    // Each line is a complete JSON object
    let df = ctx.read_json(
        "logs.ndjson",
        NdJsonReadOptions::default()
    ).await?;

    df.show().await?;
    Ok(())
}
```

**Practical considerations:**

- **Schema inference scans first 1000 objects** <br>
  Deeply nested or sparse fields may not be detected. Provide an explicit `.schema()` in production.
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

| Option                                                                            | Default        | Description                       | When to use                                                                                      |
| :-------------------------------------------------------------------------------- | :------------- | :-------------------------------- | :----------------------------------------------------------------------------------------------- |
| **[`.schema(&Schema)`][`NdJsonReadOptions::schema()`]**                           | `None`         | Provides explicit schema.         | **Production**: Enforces strict types and avoids inference surprises.                            |
| **[`.file_extension(&str)`][`NdJsonReadOptions::file_extension()`]**              | `".json"`      | Filters input files by extension. | **Mixed Directories**: Use `".jsonl"` or `".ndjson"` if you have other file types in the folder. |
| **[`.file_compression_type(...)`][`NdJsonReadOptions::file_compression_type()`]** | `UNCOMPRESSED` | Sets the compression algorithm.   | **Compressed Logs**: Reading `.json.gz` or `.json.zst` directly.                                 |
| **[`.table_partition_cols(Vec)`][`NdJsonReadOptions::table_partition_cols()`]**   | `[]`           | Maps directory paths to columns.  | **Hive Partitioning**: When data is stored in `year=2024/month=01/` folders.                     |

> **Note:** To change schema inference depth (default: 1000 objects), set the field directly: <br> >
> `NdJsonReadOptions { schema_infer_max_records: 5000, ..Default::default() }`

> **Note:**<br> > [`NdJsonReadOptions::schema()`] here is a _builder method_ that sets the schema for reading. This differs from [`DataFrame::schema()`], which _returns_ the schema of an existing DataFrame.

<details>
<summary><strong>Example: Reading compressed NDJSON</strong></summary>

```rust,no_run
use datafusion::prelude::*;
use datafusion::datasource::file_format::file_compression_type::FileCompressionType;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();

    let options = NdJsonReadOptions::default()
        .file_compression_type(FileCompressionType::GZIP)
        .file_extension(".gz"); // Important: match the actual file extension

    let df = ctx.read_json("logs/*.json.gz", options).await?;
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

```rust,no_run
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();

    // Requires: datafusion = { features = ["avro"] }
    let df = ctx.read_avro("events.avro", AvroReadOptions::default()).await?;

    df.show().await?;
    Ok(())
}
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

| Option                                                                        | Default | Description                      | When to use                                                                  |
| :---------------------------------------------------------------------------- | :------ | :------------------------------- | :--------------------------------------------------------------------------- |
| **[`.schema(&Schema)`][`AvroReadOptions::schema()`]**                         | `None`  | Provides explicit schema.        | **Production**: Enforces strict types and avoids schema drift surprises.     |
| **[`.table_partition_cols(Vec)`][`AvroReadOptions::table_partition_cols()`]** | `[]`    | Maps directory paths to columns. | **Hive Partitioning**: When data is stored in `year=2024/month=01/` folders. |

> **Note:** `.schema()` here is a _builder method_ that sets the schema for reading. This differs from [`DataFrame::schema()`], which _returns_ the schema of an existing DataFrame.

If you need to scan a directory that contains mixed file types, Avro files are selected by extension (default: `.avro`). `AvroReadOptions` does not expose a builder for this—set the field directly using struct update syntax:

`AvroReadOptions { file_extension: ".avrodata", ..Default::default() }`

<details>
<summary><strong>Example: Hive-style partitioning for Avro files</strong></summary>

```rust,no_run
use datafusion::prelude::*;
use datafusion::arrow::datatypes::DataType;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();

    let options = AvroReadOptions::default().table_partition_cols(vec![
        ("year".into(), DataType::Int32),
        ("month".into(), DataType::Int32),
    ]);

    // Example layout: events/year=2024/month=01/*.avro
    let df = ctx.read_avro("events/", options).await?;
    df.show().await?;
    Ok(())
}
```

</details>

---

### Arrow IPC — Zero-Copy Native

**Arrow's native serialization format (Feather v2): zero-copy reads, fastest startup, perfect for inter-process communication.**

Arrow IPC preserves Arrow's in-memory layout on disk. No deserialization needed—data maps directly into memory. Ideal for passing data between processes or caching intermediate results.

```rust,no_run
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();

    let df = ctx.read_arrow("data.arrow", ArrowReadOptions::default()).await?;

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

| Option                                                                         | Default | Description                      | When to use                                                                                  |
| :----------------------------------------------------------------------------- | :------ | :------------------------------- | :------------------------------------------------------------------------------------------- |
| **[`.schema(&Schema)`][`ArrowReadOptions::schema()`]**                         | `None`  | Provides explicit schema.        | **Schema control**: Normalize schema across multiple files or override/standardize metadata. |
| **[`.table_partition_cols(Vec)`][`ArrowReadOptions::table_partition_cols()`]** | `[]`    | Maps directory paths to columns. | **Hive Partitioning**: When data is stored in `year=2024/month=01/` folders.                 |

> **Note:** `.schema()` here is a _builder method_ that sets the schema for reading. This differs from [`DataFrame::schema()`], which _returns_ the schema of an existing DataFrame.

If you need to scan a directory that contains mixed file types, Arrow IPC files are selected by extension (default: `.arrow`). `ArrowReadOptions` does not expose a builder for this—set the field directly using struct update syntax:

`ArrowReadOptions { file_extension: ".feather", ..Default::default() }`

---

### Reading Multiple Files

DataFusion can read multiple files as a single `DataFrame` using explicit paths or glob patterns:

```rust,no_run
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();

    // Multiple explicit paths
    let df = ctx.read_parquet(
        vec!["data/part-000.parquet", "data/part-001.parquet"],
        ParquetReadOptions::default(),
    ).await?;

    // Glob patterns — works with local paths and cloud storage
    let df = ctx.read_parquet("data/**/*.parquet", ParquetReadOptions::default()).await?;

    // Cloud storage (after registering object store)
    let df = ctx.read_parquet("s3://bucket/data/*.parquet", ParquetReadOptions::default()).await?;

    Ok(())
}
```

> **Cloud storage**: Register an object store before using `s3://`, `gs://`, or `az://` paths. See [Object Store Configuration](../../user-guide/cli/datasources.md) for setup.

All file readers support the same path patterns:

- **Single file:** `"data.parquet"`
- **Multiple files:** `vec!["a.parquet", "b.parquet"]`
- **Glob patterns:** `"data/**/*.parquet"` (recursive), `"data/*.csv"` (single directory)
- **Cloud URLs:** `"s3://bucket/prefix/*.parquet"` (after object store registration)

<details>
<summary><strong>Advanced: ListingTable + read_table() for more control</strong></summary>

The `read_<format>()` helpers (such as `read_parquet()`, `read_csv()`, `read_json()`) are the simplest way to scan files. If you need more control (custom `ListingOptions`, multi-path tables, schema management, etc.), build a `ListingTable` and then create a `DataFrame` with `SessionContext::read_table()`.

```rust,no_run
use datafusion::prelude::*;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::{ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl};
use std::sync::Arc;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();

    let table_path = ListingTableUrl::parse("data/")?;
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

<!-- Other references -->

[parquet_docs]: https://parquet.apache.org/docs/file-format/
[parquet_crate]: https://docs.rs/parquet/latest/parquet/
[parquet_viewer]: https://github.com/XiangpengHao/parquet-viewer
[`DataFrame::schema()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.schema

<!-- json read options -->

[`NdJsonReadOptions`]: https://docs.rs/datafusion/latest/datafusion/datasource/file_format/options/struct.NdJsonReadOptions.html
[`NdJsonReadOptions::schema()`]: https://docs.rs/datafusion/latest/datafusion/datasource/file_format/options/struct.NdJsonReadOptions.html#method.schema
[`NdJsonReadOptions::file_extension()`]: https://docs.rs/datafusion/latest/datafusion/datasource/file_format/options/struct.NdJsonReadOptions.html#method.file_extension
[`NdJsonReadOptions::file_compression_type()`]: https://docs.rs/datafusion/latest/datafusion/datasource/file_format/options/struct.NdJsonReadOptions.html#method.file_compression_type
[`NdJsonReadOptions::table_partition_cols()`]: https://docs.rs/datafusion/latest/datafusion/datasource/file_format/options/struct.NdJsonReadOptions.html#method.table_partition_cols

<!-- avro read options -->

[`AvroReadOptions`]: https://docs.rs/datafusion/latest/datafusion/datasource/file_format/options/struct.AvroReadOptions.html
[`AvroReadOptions::schema()`]: https://docs.rs/datafusion/latest/datafusion/datasource/file_format/options/struct.AvroReadOptions.html#method.schema
[`AvroReadOptions::table_partition_cols()`]: https://docs.rs/datafusion/latest/datafusion/datasource/file_format/options/struct.AvroReadOptions.html#method.table_partition_cols

<!-- arrow read options -->

[`ArrowReadOptions`]: https://docs.rs/datafusion/latest/datafusion/datasource/file_format/options/struct.ArrowReadOptions.html
[`ArrowReadOptions::schema()`]: https://docs.rs/datafusion/latest/datafusion/datasource/file_format/options/struct.ArrowReadOptions.html#method.schema
[`ArrowReadOptions::table_partition_cols()`]: https://docs.rs/datafusion/latest/datafusion/datasource/file_format/options/struct.ArrowReadOptions.html#method.table_partition_cols

<!-- parquet read options -->

[`ParquetReadOptions`]: https://docs.rs/datafusion/latest/datafusion/datasource/file_format/options/struct.ParquetReadOptions.html
[`ParquetReadOptions::parquet_pruning()`]: https://docs.rs/datafusion/latest/datafusion/datasource/file_format/options/struct.ParquetReadOptions.html#method.parquet_pruning
[`ParquetReadOptions::table_partition_cols()`]: https://docs.rs/datafusion/latest/datafusion/datasource/file_format/options/struct.ParquetReadOptions.html#method.table_partition_cols
[`ParquetReadOptions::file_extension()`]: https://docs.rs/datafusion/latest/datafusion/datasource/file_format/options/struct.ParquetReadOptions.html#method.file_extension
[`ParquetReadOptions::schema()`]: https://docs.rs/datafusion/latest/datafusion/datasource/file_format/options/struct.ParquetReadOptions.html#method.schema
[`ParquetReadOptions::skip_metadata()`]: https://docs.rs/datafusion/latest/datafusion/datasource/file_format/options/struct.ParquetReadOptions.html#method.skip_metadata
[`ParquetReadOptions::file_sort_order()`]: https://docs.rs/datafusion/latest/datafusion/datasource/file_format/options/struct.ParquetReadOptions.html#method.file_sort_order

<!-- csv read options -->

[`CsvReadOptions`]: https://docs.rs/datafusion/latest/datafusion/datasource/file_format/options/struct.CsvReadOptions.html
[`CsvReadOptions::has_header()`]: https://docs.rs/datafusion/latest/datafusion/datasource/file_format/options/struct.CsvReadOptions.html#method.has_header
[`CsvReadOptions::delimiter()`]: https://docs.rs/datafusion/latest/datafusion/datasource/file_format/options/struct.CsvReadOptions.html#method.delimiter
[`CsvReadOptions::schema()`]: https://docs.rs/datafusion/latest/datafusion/datasource/file_format/options/struct.CsvReadOptions.html#method.schema
[`CsvReadOptions::schema_infer_max_records()`]: https://docs.rs/datafusion/latest/datafusion/datasource/file_format/options/struct.CsvReadOptions.html#method.schema_infer_max_records
[`CsvReadOptions::quote()`]: https://docs.rs/datafusion/latest/datafusion/datasource/file_format/options/struct.CsvReadOptions.html#method.quote
[`CsvReadOptions::file_compression_type()`]: https://docs.rs/datafusion/latest/datafusion/datasource/file_format/options/struct.CsvReadOptions.html#method.file_compression_type
[`CsvReadOptions::newlines_in_values()`]: https://docs.rs/datafusion/latest/datafusion/datasource/file_format/options/struct.CsvReadOptions.html#method.newlines_in_values
[`CsvReadOptions::null_regex()`]: https://docs.rs/datafusion/latest/datafusion/datasource/file_format/options/struct.CsvReadOptions.html#method.null_regex
[`CsvReadOptions::file_extension()`]: https://docs.rs/datafusion/latest/datafusion/datasource/file_format/options/struct.CsvReadOptions.html#method.file_extension

### Schema Handling — Inference vs Explicit

Different formats handle schemas differently:

| Format        | Schema Source                 | Inference Needed?           |
| ------------- | ----------------------------- | --------------------------- |
| **Parquet**   | Embedded in file footer       | No                          |
| **Arrow IPC** | Embedded in file header       | No                          |
| **Avro**      | Embedded in file header       | No                          |
| **CSV**       | Inferred from first N rows    | Yes (default: 1000 rows)    |
| **NDJSON**    | Inferred from first N objects | Yes (default: 1000 objects) |

#### When to Provide Explicit Schemas

For CSV and NDJSON, **explicit schemas are strongly recommended in production**:

```rust,no_run
use datafusion::prelude::*;
use datafusion::arrow::datatypes::{Schema, Field, DataType};
use std::sync::Arc;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("amount", DataType::Float64, true),
    ]));

    // CSV with explicit schema
    let df = ctx.read_csv("data.csv", CsvReadOptions::new().schema(&schema)).await?;

    // NDJSON with explicit schema
    let df = ctx.read_json("logs.ndjson", NdJsonReadOptions::default().schema(&schema)).await?;

    Ok(())
}
```

**Use explicit schemas when:**

- Running in production (avoid inference surprises)
- Early rows/objects aren't representative of the full dataset
- You need specific types (e.g., force `Utf8` instead of inferred `Int64`)
- Reading very large files (skip inference overhead)

<!-- TODO: Cross-link to a central "Schema Inference: behavior and limits" section in schema-management.md once finalized. -->

---

### From Files — References

**API Documentation:**

- [`ParquetReadOptions`](https://docs.rs/datafusion/latest/datafusion/prelude/struct.ParquetReadOptions.html) — Parquet configuration
- [`CsvReadOptions`](https://docs.rs/datafusion/latest/datafusion/prelude/struct.CsvReadOptions.html) — CSV configuration
- [`NdJsonReadOptions`](https://docs.rs/datafusion/latest/datafusion/prelude/struct.NdJsonReadOptions.html) — NDJSON configuration
- [`AvroReadOptions`](https://docs.rs/datafusion/latest/datafusion/prelude/struct.AvroReadOptions.html) — Avro configuration
- [`ArrowReadOptions`](https://docs.rs/datafusion/latest/datafusion/prelude/struct.ArrowReadOptions.html) — Arrow IPC configuration

**Parquet Deep Dives:**

- [Embedding User-Defined Parquet Indexes](https://datafusion.apache.org/blog/2025/07/14/user-defined-parquet-indexes/) — Custom indexes within Parquet files
- [`parquet_index.rs` example](https://github.com/apache/datafusion/blob/main/datafusion-examples/examples/parquet_index.rs) — External index files
- [`advanced_parquet_index.rs`](https://github.com/apache/datafusion/blob/main/datafusion-examples/examples/advanced_parquet_index.rs) — Advanced pruning

**Examples:**

- [Example Usage (CSV with SQL and DataFrame)](../../user-guide/example-usage.md)
- [`datafusion-examples/examples/dataframe.rs`](https://github.com/apache/datafusion/blob/main/datafusion-examples/examples/dataframe.rs) — DataFrame basics
- [`datafusion-examples/examples/parquet_sql_multiple_files.rs`](https://github.com/apache/datafusion/blob/main/datafusion-examples/examples/parquet_sql_multiple_files.rs) — Multi-file Parquet

---

### 2. From a Registered Table

**Register data sources once and query them by name—the catalog caches metadata and both SQL and DataFrame APIs see the same logical table.**

Registration creates a logical name for a physical data source. This abstracts away the underlying details so you work with a simple name like "sales" instead of a file path or connection string.

This named table bridges DataFusion's two query interfaces—the SQL interface and the DataFrame API—both orchestrated by `SessionContext`. Under the hood, registration stores a [`TableProvider`] in the catalog so both interfaces see the same logical table.

Registration is lazy: the data itself isn't loaded into memory. DataFusion caches schema/metadata so the source is ready for high‑performance scanning when an action executes.

**Why register?**

- **Performance**: Cache schema/metadata once; large/multi‑file and remote sources benefit from fewer round‑trips and better pruning
- **Interoperability**: The same logical name works in both DataFrame and SQL (`ctx.table("sales")` / `FROM sales`)
- **Discoverability**: Appears in `SHOW TABLES` and [`information_schema`]
- **Code clarity & portability**: Decouple query code from physical locations; swap sources by changing the catalog

For the catalog hierarchy and ways to inspect registered objects, see [Understanding DataFusion's Data Organization](#understanding-datafusions-data-organization).

#### Common registration methods

| Method                  | Purpose                          | Memory Impact        | Best For                     |
| ----------------------- | -------------------------------- | -------------------- | ---------------------------- |
| [`.register_parquet()`] | Register Parquet file(s) by name | None (lazy scan)     | Production data, analytics   |
| [`.register_csv()`]     | Register CSV file(s) by name     | None (lazy scan)     | Data imports, simple formats |
| [`.register_batch()`]   | Register in-memory RecordBatch   | Holds data in memory | Test data, small lookups     |
| [`.register_table()`]   | Register custom TableProvider    | Depends on provider  | Custom sources, advanced use |

> **Tip:** Registration creates a catalog entry with the data source's schema. The actual data is scanned lazily when queries execute, using DataFusion's streaming execution engine.

#### Performance benefits: Register once, query many times

Without registration, every query must re-scan files and re-infer schemas. Registration eliminates this overhead:

```rust
use datafusion::prelude::*;
use datafusion::error::Result;

#[tokio::main]
async fn main() -> Result<()> {
    let ctx = SessionContext::new();

    // ❌ WITHOUT registration - inefficient pattern
    let count1 = ctx.read_csv("large.csv", CsvReadOptions::new()).await?.count().await?;
    let preview = ctx.read_csv("large.csv", CsvReadOptions::new()).await?
        .limit(0, Some(10))?.collect().await?;
    let filtered = ctx.read_csv("large.csv", CsvReadOptions::new()).await?
        .filter(col("amount").gt(lit(1000)))?.collect().await?;
    // Problems: File opened 3 times, schema inferred 3 times,
    // no sharing between queries

    // ✅ WITH registration - best practice
    ctx.register_csv("sales", "large.csv", CsvReadOptions::new()).await?;

    // Now each query reuses the registered table
    let count2 = ctx.table("sales").await?.count().await?;
    let preview2 = ctx.table("sales").await?.limit(0, Some(10))?.collect().await?;
    let filtered2 = ctx.table("sales").await?
        .filter(col("amount").gt(lit(1000)))?.collect().await?;
    // Benefits: Schema inferred once, file handle managed efficiently,
    // optimizer can share work between queries

    Ok(())
}
```

> **Key insight**: Each call to `ctx.table()` returns a new DataFrame, but they all reference the same registered source. The optimizer can even share scans between concurrent queries.

#### Mixing SQL and DataFrame APIs

As described in above [From a Registered Table](#2-from-a-registered-table), registration stores tables in the session catalog. Both the SQL engine and the DataFrame API resolve names from the same catalog, so you can mix them when it improves clarity. The result of [`ctx.sql()`][`.sql()`] is itself a DataFrame, so you can keep chaining DataFrame transformations.

```rust
use std::sync::Arc;
use datafusion::prelude::*;
use datafusion::arrow::array::{ArrayRef, Int32Array, StringArray};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::Result;

#[tokio::main]
async fn main() -> Result<()> {
    let ctx = SessionContext::new();

    // Register in-memory data (great for dimension tables)
    let users = RecordBatch::try_from_iter(vec![
        ("id", Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef),
        ("name", Arc::new(StringArray::from(vec!["Alice", "Bob", "Carol"])) as ArrayRef),
    ])?;
    ctx.register_batch("users", users)?;

    // Register file data (lazy - no loading until execution)
    ctx.register_parquet("orders", "orders.parquet", ParquetReadOptions::default()).await?;

    // Use either API — both see the same tables

    // DataFrame API
    let users_df = ctx.table("users").await?;
    let orders_df = ctx.table("orders").await?;
    let joined = users_df.join(orders_df, JoinType::Inner, &["id"], &["user_id"], None)?;

    // SQL API
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

> **Best practice**: Prefer one API within a pipeline and switch at natural boundaries (e.g., define a view in SQL, then continue with DataFrame transforms), rather than ping‑ponging between APIs step-by-step.  
> **Note**: There is no inherent performance penalty to mixing—both compile down to the same [`LogicalPlan`][Logicalplan].

> **Async runtime**: You may have noticed `#[tokio::main]` and `async` in the examples. DataFusion requires an async runtime like Tokio because operations return futures that execute when awaited. This enables efficient I/O and concurrency. For async patterns and cancellation, see [Understanding Async in DataFusion][tokio_blogpost].

#### Inspecting the catalog

See [Understanding DataFusion's Data Organization](#understanding-datafusions-data-organization) for the full hierarchy and inspection tools. Quick recap from [inspect the catalog](#Inspecting the Catalog)
: objects are organized as catalog → schema → table. Defaults are catalog `datafusion` and schema `public` (plus optional `information_schema`).

```rust
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> Result<()> {
    let ctx = SessionContext::new();

    // Register a table so we have something to inspect
    let table_schema = Arc::new(Schema::new(vec![Field::new(
        "order_id",
        DataType::Int32,
        false,
    )]));
    let batch = RecordBatch::new_empty(Arc::clone(&table_schema));
    let provider = MemTable::try_new(table_schema, vec![vec![batch]])?;
    ctx.register_table("sales", Arc::new(provider))?;

    // Programmatic inspection: catalog → schema → tables
    let catalog = ctx
        .catalog("datafusion")
        .ok_or_else(|| DataFusionError::Plan("missing catalog: datafusion".to_string()))?;
    let schema = catalog
        .schema("public")
        .ok_or_else(|| DataFusionError::Plan("missing schema: public".to_string()))?;
    println!("Registered tables: {:?}", schema.table_names());

    // SQL inspection via information_schema
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

> **Best practice**: Use SQL [`information_schema`] for ad-hoc exploration and programmatic catalog APIs for dynamic applications.

#### Advanced: Custom TableProviders

For custom sources (APIs, databases, computed tables), implement the [`TableProvider`] trait and register it with the session. See the advanced guide for details.

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
    // Here we use a built-in provider (`MemTable`) as a stand-in for a custom provider.
    let schema = Arc::new(Schema::new(vec![Field::new(
        "status",
        DataType::Utf8,
        true,
    )]));
    let batch = RecordBatch::new_empty(Arc::clone(&schema));
    let provider = MemTable::try_new(schema, vec![vec![batch]])?;

    ctx.register_table("custom_source", Arc::new(provider))?;

    Ok(())
}
```

See: [Advanced Topics: TableProvider](dataframes-advance.md#tableprovider)

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

**Execute SQL and get a lazy `DataFrame`—both APIs compile to the same optimized plan, so you can mix them freely.**

This is DataFusion's hybrid strength: leverage SQL for complex relational operations (CTEs, window functions, complex joins), then seamlessly continue with DataFrames for programmatic logic.

#### Pattern 1: SQL-first workflow

**Use this when** the core logic is best expressed in SQL (CTEs, window functions, complex joins) and you want programmatic finishing touches.

```rust
use datafusion::prelude::*;
use datafusion::error::Result;

#[tokio::main]
async fn main() -> Result<()> {
    let ctx = SessionContext::new();
    ctx.register_parquet("sales", "sales.parquet", ParquetReadOptions::default()).await?;

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

**Use this when** you need to programmatically prepare data, analyze it with SQL, then apply final programmatic transformations.

```rust
use datafusion::prelude::*;
use datafusion::error::Result;

#[tokio::main]
async fn main() -> Result<()> {
    let ctx = SessionContext::new();
    ctx.register_parquet("sales", "sales.parquet", ParquetReadOptions::default()).await?;

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

Now that you've seen the patterns:

| SQL excels at                          | DataFrame excels at                   |
| :------------------------------------- | ------------------------------------- |
| Window functions (`ROW_NUMBER`, `LAG`) | Dynamic filtering based on variables  |
| CTEs for multi-step transformations    | Programmatic column selection         |
| Complex JOINs and set operations       | Iterative/conditional transformations |
| Familiar syntax for SQL developers     | Type-safe Rust integration            |

> **Key insight**: [`ctx.sql()`][`.sql()`] returns a lazy DataFrame. Nothing executes until `.show()`, `.collect()`, or similar actions. DataFusion's optimizer sees the entire plan—from both SQL and DataFrame steps—and optimizes it as a single unit.

> **Advanced**: For external data sources (PostgreSQL, etc.) via custom [`TableProvider`]s, filters/projections may push down to the source system; remaining operations execute columnar in DataFusion.

#### References

**DataFusion:**

- [Concepts: Mixing SQL and DataFrames](concepts.md#mixing-sql-and-dataframes) — Deeper dive into the hybrid execution model
- [SQL Reference](../../user-guide/sql/index.md) — Full SQL syntax, functions, and data types
- [`SessionContext::sql()`](https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.sql) — API documentation

**Other Systems (for comparison):**

- [Spark: Catalyst Optimizer](https://databricks.com/glossary/catalyst-optimizer) — Similar unified DataFrame/SQL execution plans

---

### 4. From Arrow [`RecordBatch`]es: The Native Pathway

**Create DataFrames directly from in-memory Arrow `RecordBatch`es—the engine's native format—often with zero-copy overhead.**

This is the most efficient way to get data into DataFusion when you already have Arrow data.

> New to Arrow? See [What is a RecordBatch?](../../user-guide/arrow-introduction.md#what-is-a-recordbatch-and-why-batch) for a quick primer.

A [`RecordBatch`] is the standard in-memory format for columnar data, commonly produced by:

- Network streams like [Arrow Flight]
- File readers that deserialize into Arrow (e.g., Parquet → Arrow)
- Other Rust components in your application that operate on Arrow data

#### The Architectural Choice: Read vs. Register

When you have a [`RecordBatch`], you face a fundamental decision: treat it as ephemeral data for immediate processing, or register it as a reusable table in your session.

| Aspect            | **One-Shot Query** ([`.read_batch()`])  | **Reusable Table** ([`.register_batch()`])                  |
| ----------------- | --------------------------------------- | ----------------------------------------------------------- |
| **What it does**  | Creates an ephemeral DataFrame directly | Adds the batch to the catalog under a name                  |
| **When to use**   | Immediate, one-off transformations      | Multiple references or SQL access needed                    |
| **How to access** | Pass the DataFrame object around        | Reference by name: [`ctx.table("name")`][`.table()`] or SQL |
| **Analogy**       | Like a temporary variable               | Like a temporary view in a database                         |

#### Pattern 1: One-Shot Query with [`.read_batch()`]

Perfect for processing a batch immediately after receiving it:

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

> **Why this pattern?** Instead of using `.show()` which just prints output, [`assert_batches_eq!`] lets you verify the transformation worked correctly. This is essential for testing but also makes examples more educational—you see both the operation AND its expected outcome.

#### Pattern 2: Reusable Table with [`.register_batch()`]

When you need the data accessible from multiple places:

```rust
// Register the batch as a named table
ctx.register_batch("live_sales", batch)?;

// Now query it multiple times, even from SQL
let high_revenue_count = ctx.sql(
    "SELECT COUNT(*) FROM live_sales WHERE revenue > 1000"
).await?.collect().await?;

let all_products = ctx.table("live_sales").await?
    .select_columns(&["product_id"])?
    .collect().await?;
```

> **Tip**: Processing multiple batches? Use [`.read_batches()`] for one-shot processing of a `Vec<RecordBatch>`.

#### Common Pitfalls

When constructing `RecordBatch`es manually, these invariants must hold:

- **Equal length**: All arrays (columns) in a batch must have exactly the same row count
- **Nullable columns**: Must be built with `Option<T>`; non-nullable columns must not contain `None`
- **Multiple batches**: Schemas must be identical (names, types, order, nullability)

> **Need help debugging?** See the full checklist in [What is a RecordBatch?](../../user-guide/arrow-introduction.md#what-is-a-recordbatch-and-why-batch)

#### References

**DataFusion:**

- [What is a RecordBatch?](../../user-guide/arrow-introduction.md#what-is-a-recordbatch-and-why-batch) — RecordBatch fundamentals and debugging
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

> **Default choice**: Use [`dataframe!`] macro for 99% of inline data cases. It's concise, readable, and type-safe.

#### Basic syntax with [`dataframe!`]

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

#### Handling null values <!-- TODO: More input -->

Use Rust's `Option` type for nullable columns:

```rust
let df = dataframe!(
    "id" => [1, 2, 3],
    "value" => [Some("foo"), None, Some("bar")],  // Option<T> for nulls
    "score" => [Some(100), Some(200), None]
)?;
```

> See distinctions between `None`, SQL `NULL`, and `NaN`: [Understanding Null Values](../../user-guide/dataframe.md#understanding-null-values-none-null-and-nan).

#### Complete testing workflow

The [`dataframe!`] macro pairs perfectly with [`assert_batches_eq!`] for validating DataFrame transformations. Here's a complete unit test showing the three-step pattern:

```rust
use datafusion::prelude::*;
use datafusion::assert_batches_eq;

#[tokio::test]
async fn test_filter_and_aggregate() -> datafusion::error::Result<()> {
    // 1) CREATE: Set up test data inline (no files needed!)
    let df = dataframe!(
        "department" => ["Sales", "Sales", "Engineering", "Engineering"],
        "salary" => [50000, 55000, 80000, 85000]
    )?;

    // 2) TRANSFORM: Apply the operations you want to test
    let result = df
        .aggregate(vec![col("department")], vec![sum(col("salary")).alias("total")])?
        .filter(col("total").gt(lit(100000)))?
        .sort(vec![col("total").sort(false, true)])?;

    // 3) VERIFY: Assert the exact expected output
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

> **About [`assert_batches_eq!`]**: Compares pretty-formatted output of [`RecordBatch`]es. **Failure output is copy-pasteable**—when tests fail, you can paste the actual output directly into your expected results. Works with any DataFrame source (files, SQL, in-memory, etc.), not just [`dataframe!`].
> **Conclusion**: This three-step pattern (CREATE → TRANSFORM → VERIFY) is your blueprint for testing DataFrames:
>
> - `dataframe!` creates test data without files
> - Standard DataFrame operations apply your logic
> - [`assert_batches_eq!`]verifies the exact output
>
> **Testing variants:**

- [`assert_batches_sorted_eq!`]: For order-insensitive comparisons

#### When to use [`DataFrame::from_columns()`]

Use [`DataFrame::from_columns()`] when you already have Arrow arrays (e.g., from another component) or need explicit control over array types:

```rust
use std::sync::Arc;
use datafusion::prelude::*;
use datafusion::arrow::array::{ArrayRef, Int32Array, StringArray};
use datafusion::error::Result;

#[tokio::main]
async fn main() -> Result<()> {
    let df = DataFrame::from_columns(vec![
        ("id", Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef),
        ("name", Arc::new(StringArray::from(vec!["Alice", "Bob", "Carol"])) as ArrayRef),
    ])?;
    df.show().await?;
    Ok(())
}
```

#### References

**DataFusion:**

- [`dataframe!` macro](https://docs.rs/datafusion/latest/datafusion/macro.dataframe.html) — Create DataFrames from literals
- [`assert_batches_eq!`](https://docs.rs/datafusion/latest/datafusion/macro.assert_batches_eq.html) — Test DataFrame outputs
- [`assert_batches_sorted_eq!`](https://docs.rs/datafusion/latest/datafusion/macro.assert_batches_sorted_eq.html) — Order-insensitive test comparison
- [`DataFrame::from_columns()`](https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.from_columns) — Create from Arrow arrays

---

### 6. Advanced: Constructing from a LogicalPlan

**Construct or rewrite the query tree directly—full engine-room access for federation, policy injection, and DSL builders.**

You are no longer just using the query engine—you are programming it. Constructing a [`LogicalPlan`][LogicalPlan] and handing it to [`DataFrame::new`] lets you inject, rewrite, and compose queries at the AST level, then pass them through DataFusion's optimizer and executor.

#### The fundamental shift

- SQL: declarative language for queries
- DataFrame API: programmatic builder for queries
- LogicalPlan: the query tree itself (full control, engine-room access, "Danger zone")

#### When this power matters

Use this approach when you need capabilities beyond the surface APIs:

- Cross-engine query federation or plan interchange (Substrait, Calcite, Spark)
- Organization-wide query rewriting (e.g., multi-tenant isolation, row-level security)
- Time-travel/versioned plans, injected policies, or adaptive plan surgery
- Building your own query language or DSL on top of DataFusion

Avoid this for normal data processing (use SQL/DataFrame), or for new sources (use Custom TableProviders in the advanced guide).

#### Relationship: how everything fits

```text
+----------+       +-----------+
|  SQL     |       | DataFrame |
| ctx.sql  |       |   API     |
+----------+       +-----------+
      \               /
       \             /
        v           v
    +---------------------+
    |     Logical Plan    |
    +---------------------+
              |
              | optimize (rules, pushdown, projection)
              v
    +---------------------+
    |  Optimized Logical  |
    |        Plan         |
    +---------------------+
              |
              | plan (physical planner)
              v
    +---------------------+
    |    Physical Plan    |
    +---------------------+
              |
              | execute (Tokio + CPU runtimes)
              v
    +---------------------+
    |      Results        |
    |   (RecordBatches)   |
    +---------------------+
```

See also: DataFusion planning overview [docs planing]

Both SQL and DataFrame compile to a [`LogicalPlan`][LogicalPlan]. Working at the plan level lets you transform or construct the tree directly before optimization.

#### Example: automatic multi-tenant isolation (plan rewrite)

This example transparently injects a `tenant_id = <id>` filter into every table scan, so users don't have to remember tenant predicates and cannot accidentally query across tenants. You're transforming the LogicalPlan before optimization, so predicate pushdown still applies.

> **Problem:**
>
> - In multi-tenant systems, a missing `tenant_id` predicate can leak data across customers.
> - Duplicating predicates in every query is error‑prone and hard to audit across SQL and DataFrame APIs. (See: [Deep dive in to multi-tenant systems][Ruminations on Multi-Tenant Databases])
>
> **Approach:**
>
> - Rewrite the logical plan to inject a tenant predicate at every [`TableScan`] before optimization so filters can still push down.
>
> **Outcome:**
>
> - Centralized, consistent, and auditable isolation without changing user queries.

- What it does:

  - Walks the LogicalPlan and augments each [`TableScan`] with a tenant filter
  - Re-optimizes the plan so the filter can push down to sources
  - Executes the updated plan and verifies with `explain(true, true)`

- When to use:

  - Global policy enforcement (RLS), multi-tenant scoping, environment-wide rewrites

- Alternatives:
  - If you only need source-specific scoping, consider `TableProvider`-level filtering

```rust
use datafusion::prelude::*;
use datafusion::logical_expr::{LogicalPlan, col, lit};
use datafusion::common::tree_node::{TreeNode, TreeNodeRewriter, Transformed};

/// Rewriter that adds `tenant_id = <id>` to every TableScan.
///
/// Why rewrite at plan level?
/// - Centralizes policy: users write normal queries; isolation is automatic
/// - Runs before optimization: filters can still be pushed down
/// - Works across both SQL and DataFrame pipelines
struct TenantIsolationRewriter {
    tenant_id: i32,
}

impl TreeNodeRewriter for TenantIsolationRewriter {
    type Node = LogicalPlan;

    /// Use `f_up` (bottom-up) so that child plans are already normalized before we inspect them.
    fn f_up(&mut self, node: LogicalPlan) -> datafusion::common::Result<Transformed<LogicalPlan>> {
        match node {
            LogicalPlan::TableScan(mut scan) => {
                // NOTE: In production, consider avoiding duplicate filters by
                // checking scan.filters for an existing tenant predicate.
                // This example keeps it simple for clarity.
                let tenant_filter = col("tenant_id").eq(lit(self.tenant_id));
                scan.filters.push(tenant_filter);

                // Mark as transformed to continue traversal with the updated node.
                Ok(Transformed::yes(LogicalPlan::TableScan(scan)))
            }
            other => Ok(Transformed::no(other)),
        }
    }
}

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();

    // User query WITHOUT any tenant predicate; the rewriter enforces it for all scans.
    let df = ctx.sql("SELECT * FROM users u JOIN orders o ON u.id = o.user_id").await?;

    // Extract the plan and session state for transformation.
    let (state, plan) = df.into_parts();

    // Rewrite: inject tenant filters into all TableScan nodes.
    let mut rewriter = TenantIsolationRewriter { tenant_id: 42 };
    let rewritten = plan.rewrite(&mut rewriter)?.data;

    // Best practice: re-optimize after rewriting so pushdown/projection rules still apply.
    let optimized = state.optimize(&rewritten)?;

    // Reconstruct a DataFrame from the optimized plan and original state.
    let safe_df = DataFrame::new(state, optimized);

    // Verify and run: explain (logical + physical) to confirm pushdowns, then execute.
    safe_df.explain(true, true).await?;
    safe_df.show().await?;
    Ok(())
}
```

**Discussion and best practices:**

- Prefer idempotent rewrites: avoid adding duplicate tenant predicates
- Qualify columns if needed (e.g., `u.tenant_id`) and ensure all tables have the column
- Keep rewrites narrowly scoped; don't rewrite nodes you don't intend to alter
- Always re-optimize after modifications; the optimizer will push filters and prune early
- Verify with `explain(true, true)` to confirm expected plan shape (filter placement, pushdowns)
- For source-specific scoping, you may get cleaner results in a custom `TableProvider`

See the planning overview for where this fits in the pipeline: [Datafusion planning][datafusion planning]

> Verify and debug
>
> - Use [`df.explain(true, true)`][`.explain()`] to confirm filters/pushdowns are present
> - Prefer small golden tests that assert plan shape (not just results)

#### Safety checklist

- Qualify column names if needed; don't break [`catalog.schema.table`][catalog schema] resolution
- Keep rewrites idempotent; avoid duplicating filters on subsequent rewrites
- Preserve determinism; avoid depending on mutable runtime state
- Re-optimize after rewrites; let pushdown/projection rules fire
- Treat this API as less stable than SQL/DataFrame; pin versions and test plan shapes

#### References: <!-- TODO; Do references -->

- See also :
  - Custom sources: [Advanced Topics: TableProvider](dataframes-advance.md#tableprovider)
  - Performance and plan behavior: [Best Practices: Performance Quick Checklist](best-practices.md#performance-quick-checklist)
  - Debugging and best practices: [Best Practices: Debugging Techniques](best-practices.md#debugging-techniques)
  - [Ruminations on Multi-Tenant Databases]

---

[sigmod-paper]: https://andrew.nerdnetworks.org/pdf/SIGMOD-2024-lamb.pdf

<!-- Link references -->

[`object_store`]: https://docs.rs/object_store/latest/object_store/
[`DataFrame`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html
[`SessionState`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionState.html

<!-- datafram methods  -->

[`.explain()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.explain
[`.register_parquet()`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.register_parquet
[`.register_csv()`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.register_csv
[`.register_batch()`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.register_batch
[`.register_table()`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.register_table
[`.read_parquet()`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.read_parquet
[`.read_csv()`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.read_csv
[`.read_json()`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.read_json
[`.read_avro()`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.read_avro
[`.read_arrow()`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.read_arrow
[`.collect()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.collect
[`.show()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.show
[`.execute_stream()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.execute_stream
[`CatalogProvider`]: https://docs.rs/datafusion/latest/datafusion/catalog/trait.CatalogProvider.html
[`RuntimeEnv`]: https://docs.rs/datafusion/latest/datafusion/catalog/trait.Session.html#tymethod.runtime_env
[`.read_batch()`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.read_batch
[`.read_batches()`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.read_batches
[`dataframe!`]: https://docs.rs/datafusion/latest/datafusion/macro.dataframe.html
[`ListingOptions::infer_schema()`]: https://docs.rs/datafusion-catalog-listing/latest/datafusion_catalog_listing/options/struct.ListingOptions.html#method.infer_schema
[`.register_listing_table()`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.register_listing_table
[`.catalog()`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.catalog
[`.catalog_names()`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.catalog_names
[`.schema_names()`]: https://docs.rs/datafusion/latest/datafusion/catalog/trait.CatalogProvider.html#tymethod.schema_names
[`schema()`]: https://docs.rs/datafusion/latest/datafusion/catalog/trait.CatalogProvider.html#tymethod.schema
[`.table_names()`]: https://docs.rs/datafusion/latest/datafusion/catalog/trait.SchemaProvider.html#tymethod.table_names
[`.table()`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.table
[`.sql()`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.sql
[`information_schema`]: https://datafusion.apache.org/user-guide/sql/information_schema.html
[`.unwrap()`]: https://doc.rust-lang.org/std/option/enum.Option.html#method.unwrap
[`TableProvider`]: https://docs.rs/datafusion/latest/datafusion/datasource/trait.TableProvider.html
[CsvReadOptions]: https://docs.rs/datafusion/latest/datafusion/datasource/file_format/options/struct.CsvReadOptions.html#method.new
[`DataFrame::new`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.new
[`DataFrame::schema()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.schema
[`.create_physical_plan()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.create_physical_plan

<!-- orther -->

[catalog schema]: https://datafusion.apache.org/library-user-guide/catalogs.html
[Ruminations on Multi-Tenant Databases]: https://www.db.in.tum.de/research/publications/conferences/BTW2007-mtd.pdf
[`TableScan`]: https://docs.rs/datafusion/latest/datafusion/logical_expr/logical_plan/struct.TableScan.html
[datafusion planning]: https://docs.rs/datafusion/latest/datafusion/#planning
[`RecordBatch`]: https://docs.rs/arrow/latest/arrow/record_batch/struct.RecordBatch.html
[Arror Flight]: https://arrow.apache.org/blog/2019/10/13/introducing-arrow-flight/
[datafusion tokio]: https://docs.rs/datafusion/latest/datafusion/#thread-scheduling-cpu--io-thread-pools-and-tokio-runtimes
[Comet]: https://datafusion.apache.org/comet/
[spark guide]: https://spark.apache.org/docs/latest/sql-programming-guide.html
[running-sql-queries-programmatically]: https://spark.apache.org/docs/latest/sql-getting-started.html#running-sql-queries-programmatically
[tokio_tutorial]: https://tokio.rs/tokio/tutorial
[tokio_blogpost]: https://datafusion.apache.org/blog/2025/06/30/cancellation/
[medium article]: https://medium.com/@anowerhossain97/register-the-dataframe-as-a-sql-table-in-pyspark-92cc1387ca02
[schema_infer_max_records_csv]: https://docs.rs/datafusion/latest/datafusion/datasource/file_format/csv/struct.CsvFormat.html#method.with_schema_infer_max_rec
[schema_infer_max_records_json]: https://docs.rs/datafusion/latest/datafusion/datasource/file_format/json/struct.JsonFormat.html#method.with_schema_infer_max_rec
[`TableProvider`]: https://docs.rs/datafusion/latest/datafusion/datasource/trait.TableProvider.html
[custom_table_providers example]: https://github.com/apache/datafusion/blob/main/datafusion-examples/examples/custom_table_providers.rs
[SessionContext]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html
[LogicalPlan]: https://docs.rs/datafusion-expr/latest/datafusion_expr/logical_plan/enum.LogicalPlan.html
[`ListingOptions::infer_schema()`]: https://docs.rs/datafusion-catalog-listing/latest/datafusion_catalog_listing/options/struct.ListingOptions.html#method.infer_schema
[`.register_listing_table()`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.register_listing_table
[`.catalog()`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.catalog
[`catalog_names()`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.catalog_names
[information_schema]: ../../user-guide/sql/information_schema.md
[`assert_batches_eq!`]: https://docs.rs/datafusion/latest/datafusion/macro.assert_batches_eq.html

## DataFrame Execution

After creating a DataFrame and applying transformations, you need to execute it to get results. DataFusion uses **lazy evaluation**: transformations build a query plan without processing data until you call an action method.

> **For complete documentation on DataFrame execution and writing**, see [Writing and Executing DataFrames](writing-dataframes.md), which covers:
>
> - Execution actions ([`.collect()`], [`.show()`], [`.execute_stream()`])
> - Writing to files ([`.write_parquet()`], [`.write_csv()`], [`.write_json()`])
> - Writing to tables ([`.write_table()`])
> - Performance considerations and best practices

**Quick example:**

```rust
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();

    // Create and transform a DataFrame (lazy - no execution yet)
    let df = ctx.read_csv("data.csv", CsvReadOptions::new()).await?
        .filter(col("amount").gt(lit(100)))?
        .select(vec![col("id"), col("amount")])?;

    // Execute and display results
    df.show().await?;  // Action: triggers execution

    // Or write results to a file
    // df.write_parquet("output.parquet", DataFrameWriteOptions::new(), None).await?;

    Ok(())
}
```

> **Key insight**: Nothing runs until you call an action like `.show()`, `.collect()`, or `.write_*()`. The optimizer sees the full pipeline and applies optimizations before execution.

---

## Error Handling & Recovery <!-- TODO: More input -->

Production systems need robust error handling when creating DataFrames. Understanding common failure modes helps you build resilient applications.

### Common Creation Errors

```rust
use datafusion::prelude::*;
use datafusion::error::{DataFusionError, Result};

async fn robust_csv_read(path: &str) -> Result<DataFrame> {
    let ctx = SessionContext::new();

    match ctx.read_csv(path, CsvReadOptions::new()).await {
        Ok(df) => Ok(df),
        Err(e) => match e {
            // File not found
            DataFusionError::IoError(io_err) if io_err.kind() == std::io::ErrorKind::NotFound => {
                eprintln!("File not found: {}", path);
                Err(e)
            },
            // Schema inference failed (empty file, malformed data)
            DataFusionError::ArrowError(arrow_err) => {
                eprintln!("Schema error: {}. Trying with explicit schema...", arrow_err);
                // Retry with explicit schema
                let schema = Arc::new(Schema::new(vec![
                    Field::new("col1", DataType::Utf8, true),
                    Field::new("col2", DataType::Int64, true),
                ]));
                ctx.read_csv(path, CsvReadOptions::new().schema(&schema)).await
            },
            // Other errors
            _ => {
                eprintln!("Unexpected error: {}", e);
                Err(e)
            }
        }
    }
}
```

### Schema Validation and Recovery

```rust
use datafusion::prelude::*;
use datafusion::arrow::datatypes::{DataType, Schema, Field};
use datafusion::error::Result;
use std::sync::Arc;

async fn read_with_schema_validation(
    ctx: &SessionContext,
    path: &str,
    expected_schema: &Schema
) -> Result<DataFrame> {
    // Read with inferred schema
    let df = ctx.read_parquet(path, ParquetReadOptions::default()).await?;
    let actual_schema = df.schema();

    // Validate schema
    for expected_field in expected_schema.fields() {
        match actual_schema.field_with_name(expected_field.name()) {
            Ok(actual_field) => {
                if actual_field.data_type() != expected_field.data_type() {
                    eprintln!(
                        "Warning: Column '{}' has type {:?}, expected {:?}",
                        expected_field.name(),
                        actual_field.data_type(),
                        expected_field.data_type()
                    );
                    // Could apply cast here if needed
                }
            },
            Err(_) => {
                eprintln!("Warning: Expected column '{}' not found", expected_field.name());
            }
        }
    }

    Ok(df)
}
```

### Handling Partial File Read Failures

When reading multiple files, some might fail while others succeed:

```rust
use datafusion::prelude::*;
use datafusion::error::Result;

async fn read_files_with_fallback(
    ctx: &SessionContext,
    paths: Vec<&str>
) -> Result<Vec<DataFrame>> {
    let mut successful_dfs = Vec::new();

    for path in paths {
        match ctx.read_parquet(path, ParquetReadOptions::default()).await {
            Ok(df) => {
                println!("✓ Successfully read: {}", path);
                successful_dfs.push(df);
            },
            Err(e) => {
                eprintln!("✗ Failed to read {}: {}", path, e);
                // Continue with other files
            }
        }
    }

    if successful_dfs.is_empty() {
        return Err(datafusion::error::DataFusionError::Plan(
            "All file reads failed".to_string()
        ));
    }

    Ok(successful_dfs)
}
```

### Timeout Handling for Remote Sources

```rust
use datafusion::prelude::*;
use datafusion::error::{DataFusionError, Result};
use tokio::time::{timeout, Duration};

async fn read_with_timeout(
    ctx: &SessionContext,
    path: &str,
    timeout_secs: u64
) -> Result<DataFrame> {
    match timeout(
        Duration::from_secs(timeout_secs),
        ctx.read_parquet(path, ParquetReadOptions::default())
    ).await {
        Ok(Ok(df)) => Ok(df),
        Ok(Err(e)) => Err(e),
        Err(_) => Err(DataFusionError::Execution(
            format!("Read timed out after {} seconds", timeout_secs)
        ))
    }
}

// Usage
#[tokio::main]
async fn main() -> Result<()> {
    let ctx = SessionContext::new();

    match read_with_timeout(&ctx, "s3://bucket/large.parquet", 30).await {
        Ok(df) => println!("Read succeeded: {} rows", df.count().await?),
        Err(e) => eprintln!("Read failed: {}", e),
    }

    Ok(())
}
```

> **Best practice**: Always handle I/O errors gracefully in production. Log failures, implement retries with exponential backoff for transient errors, and provide fallback data sources when possible.

---

## Real-World Creation Patterns <!-- TODO: More input -->

### Reading from Cloud Storage (S3)

```rust
use datafusion::prelude::*;
use datafusion::error::Result;

#[tokio::main]
async fn main() -> Result<()> {
    let ctx = SessionContext::new();

    // Direct S3 paths work if AWS credentials are configured
    let df = ctx.read_parquet(
        "s3://my-bucket/data/*.parquet",
        ParquetReadOptions::default()
    ).await?;

    // Read from specific partition
    let df_2024 = ctx.read_parquet(
        "s3://my-bucket/data/year=2024/*.parquet",
        ParquetReadOptions::default()
    ).await?;

    df.show_limit(10).await?;
    Ok(())
}
```

> **Configuration**: Cloud storage access requires appropriate credentials (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY) and the `object_store` feature enabled.

### Incremental/Partitioned Data Loading

```rust
use datafusion::prelude::*;
use datafusion::error::Result;

async fn load_daily_partitions(
    ctx: &SessionContext,
    base_path: &str,
    dates: &[&str]
) -> Result<DataFrame> {
    let mut paths = Vec::new();
    for date in dates {
        paths.push(format!("{}/date={}/data.parquet", base_path, date));
    }

    // Read all partitions as a single DataFrame
    let df = ctx.read_parquet(
        paths.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
        ParquetReadOptions::default()
    ).await?;

    Ok(df)
}

#[tokio::main]
async fn main() -> Result<()> {
    let ctx = SessionContext::new();

    // Load last 7 days
    let dates = ["2024-01-15", "2024-01-16", "2024-01-17", "2024-01-18",
                 "2024-01-19", "2024-01-20", "2024-01-21"];
    let df = load_daily_partitions(&ctx, "s3://data/sales", &dates).await?;

    println!("Loaded {} rows from {} partitions", df.count().await?, dates.len());
    Ok(())
}
```

### Reading Compressed Files

DataFusion automatically detects and decompresses common formats:

```rust
use datafusion::prelude::*;
use datafusion::error::Result;

#[tokio::main]
async fn main() -> Result<()> {
    let ctx = SessionContext::new();

    // Gzip compressed CSV - automatically detected by extension
    let df_gz = ctx.read_csv("data.csv.gz", CsvReadOptions::new()).await?;

    // Zstandard compressed Parquet
    let df_zst = ctx.read_parquet("data.parquet.zst", ParquetReadOptions::default()).await?;

    // Bzip2 compressed JSON
    let df_bz2 = ctx.read_json("data.json.bz2", NdJsonReadOptions::default()).await?;

    // Parquet with internal compression (Snappy, ZSTD, etc.) is transparent
    let df_parquet = ctx.read_parquet("data.parquet", ParquetReadOptions::default()).await?;

    Ok(())
}
```

> **Supported compression**: GZIP (.gz), Bzip2 (.bz2), XZ (.xz), Zstandard (.zst). Parquet files handle internal compression automatically.

### Advanced Glob Patterns

```rust
use datafusion::prelude::*;
use datafusion::error::Result;

#[tokio::main]
async fn main() -> Result<()> {
    let ctx = SessionContext::new();

    // All Parquet files in directory
    let df1 = ctx.read_parquet("data/*.parquet", ParquetReadOptions::default()).await?;

    // Recursive glob (all subdirectories)
    let df2 = ctx.read_parquet("data/**/*.parquet", ParquetReadOptions::default()).await?;

    // Multiple patterns with registration
    ctx.register_parquet(
        "all_sales",
        "sales/{2023,2024}/**/part-*.parquet",
        ParquetReadOptions::default()
    ).await?;

    let df3 = ctx.table("all_sales").await?;
    println!("Loaded {} files", df3.count().await?);

    Ok(())
}
```

## Creation-Time Optimizations <!-- TODO: More input -->

DataFusion applies several optimizations during DataFrame creation that significantly improve performance.

### Column Projection (Reading Only Needed Columns)

Reading only necessary columns reduces I/O and memory usage:

```rust
use datafusion::prelude::*;
use datafusion::error::Result;

#[tokio::main]
async fn main() -> Result<()> {
    let ctx = SessionContext::new();

    // ❌ Inefficient: reads all columns then projects
    let df_all = ctx.read_parquet("wide_table.parquet", ParquetReadOptions::default()).await?;
    let df_projected = df_all.select(vec![col("id"), col("name")])?;

    // ✅ Efficient: projection pushdown reads only id and name from Parquet
    let df_efficient = ctx.read_parquet("wide_table.parquet", ParquetReadOptions::default()).await?
        .select(vec![col("id"), col("name")])?;
    // DataFusion optimizer pushes the select down to the Parquet reader

    // Verify with explain
    df_efficient.clone().explain(false, false)?.show().await?;
    // Look for "projection=[id, name]" in the scan node

    Ok(())
}
```

### Predicate Pushdown (Filtering at Source)

Filters are pushed to file readers when possible:

```rust
use datafusion::prelude::*;
use datafusion::error::Result;

#[tokio::main]
async fn main() -> Result<()> {
    let ctx = SessionContext::new();

    // Filter is pushed down to Parquet reader
    let df = ctx.read_parquet("sales.parquet", ParquetReadOptions::default()).await?
        .filter(col("region").eq(lit("EMEA")))?
        .filter(col("amount").gt(lit(1000)))?;

    // For Parquet: uses row group statistics to skip entire row groups
    // For partitioned data: skips entire partitions

    df.clone().explain(false, false)?.show().await?;
    // Look for predicate pushdown in the plan

    Ok(())
}
```

### Partition Pruning

When reading partitioned data, DataFusion skips irrelevant partitions:

```rust
use datafusion::prelude::*;
use datafusion::error::Result;

#[tokio::main]
async fn main() -> Result<()> {
    let ctx = SessionContext::new();

    // Directory structure: data/year=2023/month=01/data.parquet
    //                      data/year=2023/month=02/data.parquet
    //                      data/year=2024/month=01/data.parquet
    let df = ctx.read_parquet("data/**/*.parquet", ParquetReadOptions::default()).await?
        .filter(col("year").eq(lit(2024)))?
        .filter(col("month").eq(lit(1)))?;

    // Only reads data/year=2024/month=01/*.parquet
    // Skips 2023 data entirely

    println!("Partition pruning in action:");
    df.clone().explain(false, false)?.show().await?;

    Ok(())
}
```

### Statistics-Based Optimization for Parquet

Parquet files store statistics (min, max, null count) that enable aggressive optimization:

```rust
use datafusion::prelude::*;
use datafusion::error::Result;

#[tokio::main]
async fn main() -> Result<()> {
    let ctx = SessionContext::new();

    // Query that can use statistics
    let df = ctx.read_parquet("large_table.parquet", ParquetReadOptions::default()).await?
        .filter(col("timestamp").gt(lit("2024-01-01 00:00:00")))?;

    // Parquet reader checks row group statistics:
    // - If max(timestamp) < 2024-01-01, skip entire row group
    // - If min(timestamp) > 2024-01-01, read entire row group
    // - Otherwise, read and filter

    // This can skip reading millions of rows without scanning data

    // Aggregate queries benefit too
    let count = ctx.read_parquet("table.parquet", ParquetReadOptions::default()).await?
        .count().await?;
    // May use Parquet metadata instead of scanning all rows

    println!("Row count: {}", count);
    Ok(())
}
```

> **Performance tip**: Always filter and project as early as possible in your DataFrame pipeline. DataFusion's optimizer will push these operations to the file readers, dramatically reducing I/O.

For more optimization strategies, see [Best Practices](best-practices.md).

## Configuration Impact on DataFrame Creation <!-- TODO: More input -->

The [`SessionContext`][SessionContext] configuration significantly affects how DataFrames are created and executed. Understanding these settings helps you tune performance for your workload.

### Key Configuration Options for Creation

```rust
use datafusion::prelude::*;
use datafusion::execution::config::SessionConfig;

let config = SessionConfig::new()
    // Batch size affects memory usage during reading
    .with_batch_size(8192)  // Default: 8192 rows per batch

    // Target partitions for parallel processing
    .with_target_partitions(8)  // Default: num_cpus

    // Repartition large files for parallelism
    .with_repartition_file_scans(true)
    .with_repartition_file_min_size(64 * 1024 * 1024)  // 64MB minimum

    // Schema inference limits for CSV/JSON
    .with_information_schema(true);

let ctx = SessionContext::with_config(config);
```

### Batch Size Impact

The batch size controls how many rows are read at once:

```rust
use datafusion::prelude::*;
use datafusion::execution::config::SessionConfig;
use datafusion::error::Result;

#[tokio::main]
async fn main() -> Result<()> {
    // Small batch size - lower memory, more overhead
    let config_small = SessionConfig::new().with_batch_size(1024);
    let ctx_small = SessionContext::new_with_config(config_small);

    // Large batch size - higher memory, better throughput
    let config_large = SessionConfig::new().with_batch_size(65536);
    let ctx_large = SessionContext::new_with_config(config_large);

    // Compare performance
    let df_small = ctx_small.read_parquet("large.parquet", ParquetReadOptions::default()).await?;
    let df_large = ctx_large.read_parquet("large.parquet", ParquetReadOptions::default()).await?;

    // Smaller batches → more batches → more overhead
    // Larger batches → fewer batches → higher memory per operation

    Ok(())
}
```

**When to adjust batch size:**

- **Increase** (16K-64K+): High-throughput analytics, large memory available
- **Decrease** (2K-4K): Memory-constrained environments, many concurrent queries
- **Default** (8K): Good balance for most workloads

### Target Partitions and Parallelism

```rust
use datafusion::prelude::*;
use datafusion::execution::config::SessionConfig;

// Control parallelism during creation
let config = SessionConfig::new()
    .with_target_partitions(16);  // 16-way parallelism

let ctx = SessionContext::with_config(config);

// Affects:
// - How files are split for parallel reading
// - Number of concurrent tasks during execution
// - Memory usage (more partitions = more concurrent batches)

let df = ctx.read_parquet("large_file.parquet", ParquetReadOptions::default()).await?;
// File will be split into ~16 partitions if large enough
```

### Format-Specific Configuration <!-- TODO: More input -->

#### CSV Reading Options

```rust
use datafusion::prelude::*;

let df = ctx.read_csv("data.csv", CsvReadOptions::new()
    .has_header(true)
    .delimiter(b',')
    .quote(b'"')
    .escape(Some(b'\\'))
    .schema_infer_max_records(100)  // Scan first 100 rows for schema
    .file_compression_type(datafusion::datasource::file_format::file_compression_type::FileCompressionType::GZIP)
).await?;
```

#### Parquet Reading Options <!-- TODO: More input -->

```rust
use datafusion::prelude::*;

let df = ctx.read_parquet("data.parquet", ParquetReadOptions::new()
    .parquet_pruning(true)       // Enable predicate pushdown (default: true)
    .skip_metadata(false)        // Read metadata (default: false)
).await?;
```

### Memory Limits and Spilling <!-- TODO: More input -->

```rust
use datafusion::prelude::*;
use datafusion::execution::config::SessionConfig;
use datafusion::execution::runtime_env::{RuntimeEnv, RuntimeConfig};
use std::sync::Arc;

// Configure memory limits
let runtime_config = RuntimeConfig::new()
    .with_memory_limit(2 * 1024 * 1024 * 1024, 1.0);  // 2GB limit

let runtime = Arc::new(RuntimeEnv::new(runtime_config)?);
let config = SessionConfig::new();
let ctx = SessionContext::new_with_config_rt(config, runtime);

// Operations will spill to disk if memory limit is exceeded
let df = ctx.read_csv("huge.csv", CsvReadOptions::new()).await?;
```

> **Note**: Memory limits apply during execution, not file scanning. File reading is streaming by default.

### Connection Pooling for Remote Sources <!-- TODO: More input -->

For S3 and other remote sources, configure connection pooling:

```rust
use datafusion::prelude::*;
use datafusion::execution::runtime_env::{RuntimeEnv, RuntimeConfig};
use object_store::aws::AmazonS3Builder;
use std::sync::Arc;
use url::Url;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    // Configure S3 with connection pooling
    let s3 = AmazonS3Builder::new()
        .with_region("us-east-1")
        .with_access_key_id("...")
        .with_secret_access_key("...")
        .with_allow_http(true)  // For testing
        .build()?;

    let runtime_config = RuntimeConfig::new();
    let runtime = Arc::new(RuntimeEnv::try_new(runtime_config)?);

    // Register S3 store
    runtime.register_object_store(
        &Url::parse("s3://my-bucket")?,
        Arc::new(s3)
    );

    let ctx = SessionContext::new_with_config_rt(SessionConfig::new(), runtime);

    let df = ctx.read_parquet("s3://my-bucket/data/*.parquet", ParquetReadOptions::default()).await?;
    df.show_limit(10).await?;

    Ok(())
}
```

> **Configuration summary**: Tuning these settings can dramatically improve performance. Start with defaults and adjust based on profiling. See [Best Practices](best-practices.md) for more tuning guidance.

## Interoperability <!-- TODO: More input -->

DataFusion integrates seamlessly with the Arrow ecosystem and can exchange data with other systems.

### Creating DataFrames from Arrow Flight

[Arrow Flight] is a high-performance framework for transferring Arrow data over the network:

[Arrow Flight]: https://arrow.apache.org/blog/2019/10/13/introducing-arrow-flight/

```rust
use datafusion::prelude::*;
use datafusion::error::Result;
use arrow_flight::{FlightClient, Ticket};
use futures::StreamExt;
use std::sync::Arc;

async fn from_arrow_flight(endpoint: &str, ticket: Ticket) -> Result<DataFrame> {
    let ctx = SessionContext::new();

    // Connect to Flight server
    let mut client = FlightClient::new(endpoint).await?;

    // Fetch data as a stream of RecordBatches
    let mut stream = client.do_get(ticket).await?;

    let mut batches = Vec::new();
    while let Some(batch) = stream.next().await {
        batches.push(batch?);
    }

    // Create DataFrame from batches
    let df = ctx.read_batches(batches)?;

    Ok(df)
}
```

### Creating from Serde Structs

Generate DataFrames from Rust structs:

```rust
use datafusion::prelude::*;
use datafusion::arrow::array::*;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::Result;
use std::sync::Arc;

#[derive(Debug)]
struct Sale {
    id: i32,
    product: String,
    amount: f64,
}

fn from_structs(sales: Vec<Sale>) -> Result<DataFrame> {
    let ctx = SessionContext::new();

    // Extract fields into Arrow arrays
    let ids: Int32Array = sales.iter().map(|s| s.id).collect();
    let products: StringArray = sales.iter().map(|s| s.product.as_str()).collect();
    let amounts: Float64Array = sales.iter().map(|s| s.amount).collect();

    // Build RecordBatch
    let batch = RecordBatch::try_from_iter(vec![
        ("id", Arc::new(ids) as ArrayRef),
        ("product", Arc::new(products) as ArrayRef),
        ("amount", Arc::new(amounts) as ArrayRef),
    ])?;

    ctx.read_batch(batch)
}

// Usage
#[tokio::main]
async fn main() -> Result<()> {
    let sales = vec![
        Sale { id: 1, product: "Widget".to_string(), amount: 99.99 },
        Sale { id: 2, product: "Gadget".to_string(), amount: 149.99 },
        Sale { id: 3, product: "Doohickey".to_string(), amount: 79.99 },
    ];

    let df = from_structs(sales)?;
    df.show().await?;

    Ok(())
}
```

### Streaming Sources (Conceptual Example)

DataFusion is designed as a **batch-oriented** query engine. While it doesn't natively support continuous streaming sources like Kafka, you can integrate them using a **micro-batching pattern** (similar to Spark Streaming) where messages are collected into time windows and processed as batches.

```rust
use datafusion::prelude::*;
use datafusion::error::Result;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::array::{ArrayRef, Int64Array, StringArray};
use std::sync::Arc;

// Micro-batching pattern: collect Kafka messages, process as DataFrames
async fn from_kafka_microbatch(
    topic: &str,
    batch_size: usize
) -> Result<DataFrame> {
    let ctx = SessionContext::new();

    // Example integration using rdkafka with micro-batching:
    //
    // use rdkafka::consumer::{Consumer, StreamConsumer};
    // use rdkafka::config::ClientConfig;
    //
    // // 1. Setup Kafka consumer
    // let consumer: StreamConsumer = ClientConfig::new()
    //     .set("bootstrap.servers", "localhost:9092")
    //     .set("group.id", "datafusion-consumer")
    //     .set("enable.auto.commit", "false")  // Manual commit for exactly-once
    //     .create()?;
    //
    // consumer.subscribe(&[topic])?;
    //
    // // 2. Collect messages for a batch window
    // let mut ids = Vec::new();
    // let mut values = Vec::new();
    //
    // for _ in 0..batch_size {
    //     match consumer.recv().await {
    //         Ok(message) => {
    //             let parsed = parse_json(message.payload())?;
    //             ids.push(parsed.id);
    //             values.push(parsed.value);
    //         },
    //         Err(e) => eprintln!("Kafka error: {}", e),
    //     }
    // }
    //
    // // 3. Build RecordBatch from collected messages
    // let batch = RecordBatch::try_from_iter(vec![
    //     ("id", Arc::new(Int64Array::from(ids)) as ArrayRef),
    //     ("value", Arc::new(StringArray::from(values)) as ArrayRef),
    // ])?;
    //
    // // 4. Process batch as DataFrame
    // let df = ctx.read_batch(batch)?
    //     .filter(col("value").is_not_null())?
    //     .aggregate(vec![col("id")], vec![count(col("value"))])?;
    //
    // // 5. Commit offsets after successful processing
    // // consumer.commit_consumer_state(CommitMode::Async)?;
    //
    // Ok(df)

    unimplemented!("Micro-batching integration - see https://github.com/apache/datafusion/issues/4285")
}
```

> **Batch vs Streaming Processing**:
>
> DataFusion is designed for **batch analytics**, not continuous stream processing. For streaming workloads:
>
> - **Use micro-batching** (shown above): Collect messages into windows, process as DataFrames
> - **For true streaming**: Apache Flink, Kafka Streams provide stateful stream processing (windows, watermarks, late data handling)
> - **Hybrid pattern**: Streaming engine for real-time → DataFusion for analytical queries on stored data
>
> **Future streaming support**: The DataFusion community is actively developing native streaming execution capabilities:
>
> - [Streaming Execution EPIC](https://synnada.notion.site/EPIC-Long-running-stateful-execution-support-for-unbounded-data-with-mini-batches-a416b29ae9a5438492663723dbeca805) - **Detailed design proposal** with architecture, task breakdown, and implementation roadmap
> - [DataFusion #4285](https://github.com/apache/datafusion/issues/4285) - GitHub issue tracking the streaming roadmap
> - [DataFusion #1544](https://github.com/apache/datafusion/issues/1544) - Original streaming support discussion
>
> Until native streaming support is implemented, micro-batching (as shown above) remains the recommended pattern for integrating DataFusion with streaming sources.

> **Ecosystem note**: DataFusion's Arrow-native design makes integration straightforward. Any system that produces or consumes Arrow data can interface with DataFusion with minimal overhead.

For more integration examples, see the [DataFusion examples](https://github.com/apache/datafusion/tree/main/datafusion-examples/examples).

## Best Practices for DataFrame Creation <!-- TODO: More input -->

### Creation Anti-patterns

Avoid these common mistakes when creating DataFrames:

#### ❌ Anti-pattern 1: Creating DataFrames in Loops Without Registration

```rust
use datafusion::prelude::*;

// BAD: Re-reads and re-infers schema on each iteration
for query_id in query_ids {
    let df = ctx.read_csv("large.csv", CsvReadOptions::new()).await?;
    let result = df.filter(col("id").eq(lit(query_id)))?
        .collect().await?;
    process(result);
}

// GOOD: Register once, query many times
ctx.register_csv("data", "large.csv", CsvReadOptions::new()).await?;
for query_id in query_ids {
    let df = ctx.table("data").await?;
    let result = df.filter(col("id").eq(lit(query_id)))?
        .collect().await?;
    process(result);
}
```

#### ❌ Anti-pattern 2: Reading Entire File When Streaming Would Work

```rust
use datafusion::prelude::*;
use futures::StreamExt;

// BAD: Loads all data into memory
let df = ctx.read_parquet("huge.parquet", ParquetReadOptions::default()).await?;
let batches = df.collect().await?;
for batch in batches {
    process_batch(batch)?; // Process one at a time anyway!
}

// GOOD: Stream batches incrementally
let df = ctx.read_parquet("huge.parquet", ParquetReadOptions::default()).await?;
let mut stream = df.execute_stream().await?;
while let Some(batch) = stream.next().await {
    process_batch(batch?)?;
}
```

#### ❌ Anti-pattern 3: Not Leveraging Lazy Evaluation

```rust
use datafusion::prelude::*;

// BAD: Applies transformations after collecting
let df = ctx.read_parquet("data.parquet", ParquetReadOptions::default()).await?;
let all_data = df.collect().await?; // Loads everything
// Now have to filter in application code

// GOOD: Push filters into the DataFrame (executed lazily)
let df = ctx.read_parquet("data.parquet", ParquetReadOptions::default()).await?
    .filter(col("region").eq(lit("EMEA")))?
    .select(vec![col("id"), col("name"), col("amount")])?;
let filtered_data = df.collect().await?; // Only reads what's needed
```

#### ❌ Anti-pattern 4: Ignoring Predicate and Projection Pushdown

```rust
use datafusion::prelude::*;

// BAD: Reads all columns then filters
let df = ctx.read_parquet("wide_table.parquet", ParquetReadOptions::default()).await?;
let all_cols = df.collect().await?;
// Filter in application code

// GOOD: Filter and project early
let df = ctx.read_parquet("wide_table.parquet", ParquetReadOptions::default()).await?
    .filter(col("year").eq(lit(2024)))?           // Pushed to Parquet reader
    .select(vec![col("id"), col("name")])?;        // Only reads 2 columns
let result = df.collect().await?;
```

#### ❌ Anti-pattern 5: Memory Management Pitfalls

```rust
use datafusion::prelude::*;

// BAD: Creating massive DataFrames in memory
let huge_data = create_huge_recordbatch()?; // 10GB in memory
let df = ctx.read_batch(huge_data)?;

// GOOD: Write to file, then read lazily
let huge_data = create_huge_recordbatch()?;
write_to_parquet(huge_data, "temp.parquet")?;
let df = ctx.read_parquet("temp.parquet", ParquetReadOptions::default()).await?;
// Now can stream/process incrementally
```

#### ❌ Anti-pattern 6: Not Handling Schema Evolution

```rust
use datafusion::prelude::*;

// BAD: Assumes all files have identical schemas
let df = ctx.read_parquet(
    vec!["old.parquet", "new.parquet"],
    ParquetReadOptions::default()
).await?;
// Fails if schemas don't match exactly

// GOOD: Handle schema evolution explicitly
match ctx.read_parquet(
    vec!["old.parquet", "new.parquet"],
    ParquetReadOptions::default()
).await {
    Ok(df) => {
        // Verify schema if needed
        verify_schema(df.schema())?;
        Ok(df)
    },
    Err(e) => {
        // Fall back to reading separately and unifying
        let df1 = ctx.read_parquet("old.parquet", ParquetReadOptions::default()).await?;
        let df2 = ctx.read_parquet("new.parquet", ParquetReadOptions::default()).await?;
        let unified = unify_schemas(df1, df2)?;
        Ok(unified)
    }
}
```

### Performance Tips Summary

| **DO**                                 | **DON'T**                                  |
| -------------------------------------- | ------------------------------------------ |
| ✅ Register tables for reuse           | ❌ Re-read files multiple times            |
| ✅ Filter and project early            | ❌ Load everything then filter in app code |
| ✅ Stream large results                | ❌ Collect everything into memory          |
| ✅ Use explicit schemas when available | ❌ Rely on inference for production data   |
| ✅ Leverage Parquet for analytics      | ❌ Use CSV for large-scale processing      |
| ✅ Check explain() plans               | ❌ Assume operations are efficient         |

### Testing Best Practices

Throughout this guide, you've seen the [`assert_batches_eq!`] pattern in action. Here's why it's a best practice:

**The Pattern:**

```rust
let batches = df.collect().await?;
assert_batches_eq!(
    &[
        "+-------+-----+",
        "| name  | age |",
        "+-------+-----+",
        "| Alice | 30  |",
        "+-------+-----+",
    ],
    &batches
);
```

**Why use this pattern?**

- ✅ **Self-documenting**: Shows both the code AND expected output
- ✅ **Verifiable**: Tests actually run and catch regressions
- ✅ **Copy-pasteable**: When tests fail, output can be directly pasted back
- ✅ **Universal**: Works with any DataFrame source (files, SQL, in-memory)

**When to use:**

- Unit tests for DataFrame transformations
- Documentation examples (like this guide!)
- Regression tests for bug fixes
- Learning materials where showing output helps understanding

**Alternatives:**

- `.show().await?` - Good for development/debugging, but not verifiable
- Manual assertions on column values - More code, less readable
- Comparing serialized formats - Fragile to formatting changes

> **Conclusion\*: By now you've seen this pattern multiple times. It's not just about testing—it's about **understanding\*\*. Each [`assert_batches_eq!`] tells you "this is what happens when you run this code." That's powerful for learning, debugging, and maintaining code.

### Debugging DataFrame Creation

#### Inspecting Query Plans

Use `explain()` to see what DataFusion will actually execute:

```rust
use datafusion::prelude::*;

let df = ctx.read_parquet("data.parquet", ParquetReadOptions::default()).await?
    .filter(col("amount").gt(lit(1000)))?
    .select(vec![col("id"), col("amount")])?;

// Logical plan
println!("Logical Plan:");
df.clone().explain(false, false)?.show().await?;

// Physical plan with more details
println!("\nPhysical Plan:");
df.clone().explain(false, true)?.show().await?;

// Look for:
// - "projection=[...]" - which columns are actually read
// - "predicate=..." - filters pushed to source
// - "partitions=..." - parallelism level
```

#### Sampling Large Files

Test on a sample before processing the whole file:

```rust
use datafusion::prelude::*;

// Sample first N rows
let sample = ctx.read_parquet("huge.parquet", ParquetReadOptions::default()).await?
    .limit(0, Some(1000))?;

sample.show().await?;

// Inspect schema
println!("Schema: {}", sample.schema());

// Test transformations on sample
let transformed = sample
    .filter(col("status").eq(lit("active")))?
    .aggregate(vec![col("category")], vec![count(col("*"))])?;

transformed.show().await?;

// Once validated, run on full dataset
```

#### Validating File Contents

```rust
use datafusion::prelude::*;
use datafusion::error::Result;

async fn validate_parquet_file(ctx: &SessionContext, path: &str) -> Result<()> {
    println!("Validating: {}", path);

    let df = ctx.read_parquet(path, ParquetReadOptions::default()).await?;

    // 1. Check schema
    println!("Schema:");
    for field in df.schema().fields() {
        println!("  {}: {:?} (nullable: {})",
            field.name(), field.data_type(), field.is_nullable());
    }

    // 2. Check row count
    let count = df.clone().count().await?;
    println!("Row count: {}", count);

    // 3. Check for nulls in key columns
    let null_check = df.clone()
        .aggregate(
            vec![],
            vec![
                count(col("id")).alias("id_count"),
                count(lit(1)).alias("total_rows"),
            ]
        )?
        .collect().await?;

    println!("Null check: {:?}", null_check);

    // 4. Sample data
    println!("\nSample rows:");
    df.limit(0, Some(5))?.show().await?;

    Ok(())
}
```

#### Monitoring Progress

For long-running creation operations:

```rust
use datafusion::prelude::*;
use std::time::Instant;

async fn monitored_read(ctx: &SessionContext, paths: Vec<&str>) -> datafusion::error::Result<DataFrame> {
    let start = Instant::now();

    println!("Starting read of {} files...", paths.len());

    let df = ctx.read_parquet(paths, ParquetReadOptions::default()).await?;

    println!("Schema inferred in {:?}", start.elapsed());

    // Count rows to force scan
    let count_start = Instant::now();
    let count = df.clone().count().await?;
    println!("Counted {} rows in {:?}", count, count_start.elapsed());

    println!("Total time: {:?}", start.elapsed());

    Ok(df)
}
```

> **Debugging tip**: Always use `explain()` to understand what DataFusion is actually doing. The physical plan shows the exact operations and their order, which is essential for performance tuning.

For more debugging and profiling techniques, see [Best Practices § Debugging Techniques](best-practices.md#debugging-techniques).
