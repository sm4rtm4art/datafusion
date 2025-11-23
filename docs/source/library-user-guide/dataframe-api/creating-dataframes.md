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

# Creating and Executing DataFrames

Creating a DataFrame is the foundational first step for building any query pipeline in DataFusion. It's the entry point to the query engine. True to the flexible, interoperable spirit of Apache Arrow, DataFusion offers a rich set of methods for ingesting data, which is the focus of this guide.

This document is designed for developers and data engineers of all levels. It covers every method for creating DataFrames—from reading a simple CSV file for an ad-hoc query to configuring high-performance connectors for production data pipelines in cloud storage. You will learn not just how to create DataFrames, but also why to choose one method over another, ensuring your application is both correct and efficient.

> **For Advanced Topics**: This guide covers the foundational API calls for creating DataFrames. For a deeper dive into production-level concerns like schema management, error handling, connecting to S3, Arrow Flight, Kafka, custom providers, and performance tuning, please see our dedicated guide to [Advanced Creation Topics](dataframe-advance.md) .

> **Style Note:** In this guide, all code elements are highlighted with backticks. DataFrame methods are written as `.method()` (e.g., `.select()`) to reflect the chaining syntax central to the API. This distinguishes them from standalone functions (e.g., `col()`) and static constructors (e.g., `SessionContext::new()`). Rust types are formatted as `TypeName` (e.g., `SchemaRef`).

```{contents}
:local:
:depth: 2
```

To situate DataFrames in the overall architecture, here is a brief recap from [Concepts](concepts.md): a high‑level diagram showing where DataFusion DataFrames sit in the query pipeline.

```
                +------------------+
                |  SessionContext  |
                +------------------+
                         |
                         | creates
                         v
     +-----------+                 +-------------+
     |   SQL     |                 | DataFrame   |
     |  ctx.sql  |                 |   API (lazy)|
     +-----------+                 +-------------+
             \                         /
              \                       /
               v                     v
             +-----------------------------+
             |         LogicalPlan         |
             +-----------------------------+
                         |
                         | optimize (rules: projection/predicate pushdown, etc.)
                         v
             +-----------------------------+
             |     Optimized LogicalPlan   |
             +-----------------------------+
                         |
                         | plan (physical planner)
                         v
             +-----------------------------+
             |     ExecutionPlan (Physical)|
             +-----------------------------+
                         |
                         | optimize (physical optimizer)
                         v
             +-----------------------------+
             |   Optimized ExecutionPlan   |
             +-----------------------------+
                         |
                         | execute (Tokio + CPU runtimes)
                         v
             +-----------------------------+
             |     RecordBatch streams     |
             +-----------------------------+
```

> **How to read this**
>
> - [`SessionContext`][SessionContext] holds catalogs, configuration, and runtime; both SQL and the DataFrame API resolve names from the same session catalog
> - [`ctx.sql("...")`][`.sql()`] parses SQL, resolves tables/views in the same catalog, and returns a lazy DataFrame
> - [`ctx.table("name")`][`.table()`] references a registered table by logical name for programmatic DataFrame building
> - Both APIs compile to the same [`LogicalPlan`][LogicalPlan]; the optimizer sees the entire pipeline (SQL + DataFrame) and applies pushdown/projection rules
> - Nothing executes until an action (`.collect()`, `.show()`, `.execute_stream()`) is called (see: [Excecution DataFrame]("../creating-dataframes.md#Executing DataFrames") )
>   ; results are Arrow [`RecordBatch`] streams
>
> SQL specifics
>
> - Name resolution uses the session catalog (default `datafusion.public`); you can use fully qualified `catalog.schema.table` names when needed
> - You can mix APIs via temporary views, for example: `ctx.register_table("prep", df.into_view())?` then `ctx.sql("SELECT ... FROM prep")`
> - Prefer one API per stage for readability: express complex set logic in SQL; use DataFrame API for dynamic, type-safe Rust logic
>
> **See also**
>
> - Concepts: [`concepts.md`](./concepts.md)
> - Transformations: [`transformations.md`](transformations.md)
> - DataFrame Execution (later in this guide)

> **📚 About the examples in this guide**
>
> Throughout this document, you'll see examples using [`assert_batches_eq!`] to verify DataFrame outputs. This isn't just for testing—it's a learning tool! When you see:
>
> ```rust
> let batches = df.collect().await?;
> assert_batches_eq!(&[...], &batches);
> ```
>
> You're seeing both **what the code does** and **what result it produces**. This pattern will help you understand DataFusion's behavior as you read through the examples.

<!-- TODO: add reference-->

## Before Creating a DataFrame

DataFusion provides a comprehensive set of methods for creating DataFrames, each designed to fit a specific use case. This flexibility is intentional: as a query engine, DataFusion integrates into diverse environments—from large-scale data pipelines to interactive SQL queries and embedded applications.

The [`SessionContext`][SessionContext] is the main entry point for creating DataFrames. It maintains session state (catalogs, tables, configuration) and is where every DataFrame journey begins. Both the DataFrame API and SQL API compile to the same [`LogicalPlan`][LogicalPlan], which DataFusion then optimizes and executes identically. For architectural details, see [Concepts](concepts.md) or [Mixing SQL and DataFrames](transformations.md#mixing-sql-and-dataframes).

### Understanding DataFusion's Data Organization

Think of DataFusion's `SessionContext` as a workspace. Inside this workspace, you organize your data sources into a clean, three-level hierarchy, just like a traditional database:

```
SessionContext
└── Catalog ("datafusion")
    └── Schema ("public")
        └── Table ("sales")
```

Adopting this structure offers three key advantages:

- **Performance**: When you register a table (e.g., a Parquet file), DataFusion analyzes its schema and metadata once. Every subsequent query that uses that table is faster because it skips this expensive step.
- **Clarity**: Instead of passing file paths around your code, you refer to data with logical names like `"sales"` or `"fact_orders"`. This makes your queries cleaner and easier to maintain.
- **Interoperability**: A registered table is available to both the DataFrame API and SQL. You can register a source with one API and immediately query it from the other.

The core pattern is simple: **register once, query many times**.

> **Choosing a Creation Method**
>
> | When to use                    | Method                                            | Learn more                                            |
> | ------------------------------ | ------------------------------------------------- | ----------------------------------------------------- |
> | Use files once                 | [`.read_parquet()`], [`.read_csv()`]              | [From Files](#1-from-files)                           |
> | Reuse across queries or in SQL | [`.register_parquet()`] → [`.table()`]            | [From a Registered Table](#2-from-a-registered-table) |
> | Inline data or tests           | [`.read_batch()`], [`dataframe!()`][`dataframe!`] | [From In-Memory Data](#4-from-in-memory-data)         |
> | Explore what's registered      | [`.catalog_names()`], `SHOW TABLES`               | [Catalogs Guide](../catalogs.md)                      |

#### Key Concepts

**Default namespace**: For simple workflows, just use a table name like `"sales"`. DataFusion places it in the default location: `datafusion.public.sales`. The `"public"` schema is just a default namespace—it has no security implications.

**Custom catalogs & schemas**: You can create your own catalogs for organization, multi-tenancy, or separating environments (prod vs staging):

```rust
use datafusion::catalog::MemoryCatalogProvider;
use datafusion::catalog::MemorySchemaProvider;
use std::sync::Arc;

let catalog = MemoryCatalogProvider::new();
let schema = MemorySchemaProvider::new();
catalog.register_schema("analytics", Arc::new(schema))?;
ctx.register_catalog("warehouse", Arc::new(catalog));

// Now register tables in custom locations
ctx.register_table("warehouse.analytics.metrics", provider)?;
```

> See also:

- [Catalogs, Schemas, and Tables](../catalogs.md#general-concepts)
- [Implementing `MemoryCatalogProvider`](../catalogs.md#implementing-memorycatalogprovider)
- For table scan internals, see [Custom Table Providers](../custom-table-providers.md).

**Name resolution**: Identifiers can be 1-, 2-, or 3-part:

- `"sales"` → `datafusion.public.sales` (default catalog + schema)
- `"analytics.sales"` → `datafusion.analytics.sales` (default catalog)
- `"warehouse.analytics.sales"` → explicit catalog + schema + table

Quote to preserve case: `"SELECT * FROM \"Sales\""`.

**Object stores**: Files can live on local disk or any object store (S3, GCS, Azure) configured in the [`RuntimeEnv`]. See [Object Store Configuration](../../user-guide/configs.md#object-store).

**Lifetime & persistence**: Registrations are in-memory and scoped to the [`SessionContext`][sessionContext] lifetime. For persistent catalogs, plug in a custom [`CatalogProvider`]. See the [Catalogs Guide](../catalogs.md) for designs and examples.

**Case sensitivity**: Table and column identifiers are folded to lowercase unless quoted:

```rust
ctx.table("Sales").await?;   // Resolves to "sales"
ctx.table("SALES").await?;   // Also resolves to "sales"

// SQL with quotes preserves case:
ctx.sql("SELECT * FROM \"Sales\"").await?;  // Looks for "Sales" (case-sensitive)
```

> **Performance note**: Registration caches schema and file metadata so subsequent DataFrames avoid re-inferring schemas and re-reading file footers. This is especially valuable for Parquet files where metadata parsing can be expensive. For advanced performance techniques, see [Advanced Topics](dataframe-advance.md#performance-optimizations).
>
> **For advanced topics** (information_schema, dynamic file catalogs, custom providers), see the [Catalogs Guide](../catalogs.md).

### Creating DataFrames: Direct Reads vs. Registered Tables

When you create a DataFrame, you're either:

1. **Reading data directly** from files (one-time use, no catalog involved)
2. **Accessing a registered table** from the catalog (reusable across queries)

The second approach uses DataFusion's catalog system—a three-level hierarchy that stores [`TableProvider`]s (objects that know how to scan your data sources). For a deeper look at how catalogs, schemas, and tables relate, see the [Catalogs Guide](../catalogs.md#general-concepts):

```
SessionContext (your workspace)
└── Catalog ("datafusion" by default)
    └── Schema ("public" by default)  <-- NOT the same as table schema!
        └── Table ("sales")
            └── Table Schema (columns: id, amount, date...)  <-- The actual structure
```

#### The Two Meanings of "Schema"

> ⚠️ **Confusing terminology alert**: The word "schema" has two completely different meanings in DataFusion:
>
> 1. **Schema as namespace** (like PostgreSQL schemas): A container for organizing tables
>
>    - Example: `"public"` schema contains your tables
>    - This is just for organization, like folders for files
>
> 2. **Schema as structure** (Arrow schema): The columns and types of a table
>    - Example: `(id: Int32, name: Utf8, amount: Float64)`
>    - This defines what data the table actually contains
>
> ```rust
> // "public" here is a namespace schema (meaning #1)
> ctx.table("public.sales").await?;
>
> // This returns the table's structure schema (meaning #2)
> let arrow_schema = df.schema();  // Fields: [(id, Int32), (name, Utf8), ...]
> ```

#### Why This Matters for DataFrame Creation

The catalog system impacts creation and access patterns: direct reads are ideal for one-off scans; registered tables shine when you need reuse, SQL interop, discovery, or multi-file/remote performance. Use the decision guide below to choose.

**Without registration (direct read):**

```rust
// Creates a new DataFrame each time, re-analyzes the file
let df1 = ctx.read_parquet("sales.parquet", options).await?;
let df2 = ctx.read_parquet("sales.parquet", options).await?; // Reads metadata again!
```

**With registration (catalog approach):**

```rust
// Register once - analyzes file metadata and stores it
ctx.register_parquet("sales", "sales.parquet", options).await?;

// Create DataFrames many times - uses cached metadata
let df1 = ctx.table("sales").await?;  // Fast!
let df2 = ctx.table("sales").await?;  // Fast!
```

##### Decision Guide: Register vs. Direct Read

| Scenario                                          | Prefer      | Rationale                                                                                                            | References                                                                                                                                                                     |
| ------------------------------------------------- | ----------- | -------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Multiple queries or SQL/discovery are anticipated | Register    | Reuses cached metadata; enables SQL ([`ctx.sql()`][`.sql()`]), `SHOW TABLES`, and `information_schema` introspection | [Catalogs Guide](../catalogs.md#general-concepts), [information_schema](../../user-guide/sql/information_schema.md)                                                            |
| Remote object stores or many files/partitions     | Register    | Amortizes metadata I/O and improves pruning across files                                                             | [Parquet pruning](https://datafusion.apache.org/blog/2025/03/20/parquet-pruning/), [External indexes](https://datafusion.apache.org/blog/2025/08/15/external-parquet-indexes/) |
| One-off, small, local scans (e.g., CSV/NDJSON)    | Direct read | No catalog state or refresh policy required; minimal startup cost                                                    | —                                                                                                                                                                              |

**Trade-offs**

- Registration: upfront metadata reads and minimal catalog memory; requires a refresh strategy if files change out-of-band.
- Direct reads: no catalog state; metadata/schema re-derived per call; not discoverable via SQL or catalogs.

> **Default rule (when uncertain)**
>
> - Parquet, remote storage, or multi-file inputs → prefer registering.
> - Small, local, one-off reads → prefer direct reads.

#### Working with the Catalog System

For most use cases, you can ignore the catalog hierarchy and just use table names:

```rust
use datafusion::prelude::*;
use datafusion::error::Result;

#[tokio::main]
async fn main() -> Result<()> {
    let ctx = SessionContext::new();

    // Simple: Just use a table name (goes into datafusion.public.sales)
    ctx.register_csv("sales", "sales.csv", CsvReadOptions::new()).await?;
    let df = ctx.table("sales").await?;

    // Advanced: Use custom catalog/schema for organization (covered in advanced guide)
    // ctx.register_csv("warehouse.analytics.sales", "sales.csv", ...).await?;

    Ok(())
}
```

> **Best practice**: Use SQL for complex relational logic (joins/aggregations), then switch to the DataFrame API for programmatic steps. Avoid alternating APIs step-by-step; both compile to the same optimized plan and execute columnar in DataFusion, with supported pushdown delegated to connectors when available.

#### The Performance Advantage of Registration

Here's why registration matters in practice:

```rust
use datafusion::prelude::*;
use datafusion::error::Result;

#[tokio::main]
async fn main() -> Result<()> {
    let ctx = SessionContext::new();

    // ❌ Inefficient: Reading the same file multiple times
    let df1 = ctx.read_parquet("sales.parquet", ParquetReadOptions::default()).await?;
    // ... later in your code ...
    let df2 = ctx.read_parquet("sales.parquet", ParquetReadOptions::default()).await?;
    // Each read_parquet() call:
    // - Opens the file
    // - Reads Parquet metadata (can be expensive for large files)
    // - Infers the schema
    // - Creates a new logical plan

    // ✅ Efficient: Register once, use many times
    ctx.register_parquet(
        "sales",                          // Table name (you choose this)
        "sales.parquet",                  // File path
        ParquetReadOptions::default()
    ).await?;

    let df3 = ctx.table("sales").await?;  // Access by the registered name
    // ... later in your code ...
    let df4 = ctx.table("sales").await?;  // Same name, reuses cached metadata
    // Each table() call:
    // - Looks up cached metadata (microseconds)
    // - Reuses the same schema
    // - Creates a new logical plan with cached info

    Ok(())
}
```

**Additional benefits of registration:**

- **SQL interoperability**: Once registered, you can query the table with SQL
- **Discoverability**: Tables show up in [`information_schema`]
- **Consistency**: Everyone uses the same table with the same schema

#### Inspecting the Catalog

Once you register tables, they become part of the `SessionContext`'s catalog. This enables discovery and inspection, which is essential for interactive exploration and debugging. DataFusion organizes this information in a hierarchy:

```
SessionContext
└── Catalog: datafusion (default)
    ├── Schema: public (default)
    │   ├── sales      (table)
    │   └── users      (table)
    └── Schema: information_schema
        ├── tables     (view)
        └── columns    (view)
```

You can explore this catalog either programmatically using Rust or by executing SQL queries.

##### Programmatic Inspection

List schemas and tables with the API:

```rust
use datafusion::prelude::*;
use datafusion::error::Result;

#[tokio::main]
async fn main() -> Result<()> {
    let ctx = SessionContext::new();

    ctx.register_csv("sales", "sales.csv", CsvReadOptions::new()).await?;
    ctx.register_parquet("users", "users.parquet", ParquetReadOptions::default()).await?;

    // Default catalog ("datafusion") and schema ("public")
    let catalog = ctx.catalog("datafusion").unwrap();
    let schema = catalog.schema("public").unwrap();

    // List registered tables
    let table_names = schema.table_names();
    println!("Tables: {:?}", table_names); // Tables: ["sales", "users"]
    Ok(())
}
```

##### SQL Inspection

For quick exploration, SQL is often more convenient.

```rust
use datafusion::prelude::*;
use datafusion::error::Result;
use datafusion::assert_batches_eq;

#[tokio::test]
async fn test_show_tables() -> Result<()> {
    let ctx = SessionContext::new();

    ctx.register_csv("sales", "sales.csv", CsvReadOptions::new()).await?;
    ctx.register_parquet("users", "users.parquet", ParquetReadOptions::default()).await?;

    // SHOW TABLES for a quick overview
    let df = ctx.sql("SHOW TABLES").await?;

    // Verify the registered tables appear in the catalog
    let batches = df.collect().await?;
    assert_batches_eq!(
        &[
            "+------------+",
            "| table_name |",
            "+------------+",
            "| sales      |",
            "| users      |",
            "+------------+",
        ],
        &batches
    );

    Ok(())
}
```

> **Testing pattern**: Using [`assert_batches_eq!`] verifies the exact output. This is more robust than visual inspection with `.show()` and is the recommended pattern for examples that demonstrate expected results.

For detailed metadata (columns and types), enable [`information_schema`] and query it:

```rust
use datafusion::prelude::*;
use datafusion::execution::config::SessionConfig;
use datafusion::error::Result;

#[tokio::main]
async fn main() -> Result<()> {
    let ctx = SessionContext::with_config(SessionConfig::new().with_information_schema(true));
    let df = ctx.sql(
        "SELECT table_name, column_name, data_type\n\
         FROM information_schema.columns\n\
         WHERE table_schema = 'public'"
    ).await?;
    df.show().await?;
    Ok(())
}
```

Commonly used methods in this section: [`ctx.catalog_names()`][`.catalog_names()`], [`ctx.catalog()`][`.catalog()`], [`catalog.schema_names()`][`.schema_names()`], [`catalog.schema()`][`schema()`], [`schema.table_names()`][`.table_names()`]:, [`ctx.table()`][`.table()`], [`ctx.sql()`][`.sql()`]. Examples use [`.unwrap()`] for brevity; handle errors as appropriate.

> For advanced catalog operations, see the [Catalogs Guide](../catalogs.md).

**See also:**

- DataFusion
  - [Catalogs, Schemas, and Tables](../catalogs.md)
  - [information_schema](../../user-guide/sql/information_schema.md)
  - [Transformations: Mixing SQL and DataFrames](transformations.md#mixing-sql-and-dataframes)
  - [Advanced: Performance Optimizations](dataframe-advance.md#performance-optimizations)
- Spark (query engine + DataFrame API + discovery)
  - [Spark SQL and DataFrames Guide][spark guide]
  - [SHOW TABLES](https://spark.apache.org/docs/latest/sql-ref-syntax-aux-show-tables.html)
  - [SparkSession Catalog API (PySpark)](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.Catalog.html)
- Flink (Table API + SQL catalogs)
  - [Flink Table/SQL: Catalogs](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/table/catalogs/)
- Trino (SQL query engine with catalogs)
  - [Concepts: Catalogs and Schemas](https://trino.io/docs/current/overview/concepts.html#catalogs-and-schemas)
- DuckDB (discovery & information_schema)
  - [SHOW statements](https://duckdb.org/docs/sql/statements/show)
  - [information_schema](https://duckdb.org/docs/stable/sql/meta/information_schema)
- ANSI-style metadata
  - [PostgreSQL information_schema](https://www.postgresql.org/docs/current/information-schema.html)
  - [MySQL information_schema](https://dev.mysql.com/doc/mysql-infoschema-excerpt/8.0/en/information-schema-introduction.html)

---

## How to create a Dataframe

This section shows the practical ways to construct a `DataFrame` in DataFusion: from files, registered tables, SQL, in‑memory data, inline data, and from a [`LogicalPlan`][LogicalPlan]. All approaches compile to the same [`LogicalPlan`][LogicalPlan], so you can freely mix SQL and the DataFrame API. By this you achive the best of both worlds. Choose based on where the data lives and whether you'll reuse it (see the decision guide above).

### DataFrame Creation Methods

Now that you understand how DataFusion organizes data, let's look at the different ways to create DataFrames. Choose your method based on where your data lives and how you'll use it:

1. **From files**: Read Parquet, CSV, JSON, Avro, or Arrow files — [From Files](#1-from-files)

   - **Best for**: Production data pipelines, large datasets, cloud storage
   - **Avoid if**: Data is already in memory or < 1000 rows like test cases (use inline data instead)

2. **From a registered table**: Access tables by name — [From a Registered Table](#2-from-a-registered-table)

   - **Best for**: Reusing data across multiple queries, mixing SQL and DataFrame operations
   - **Avoid if**: You're only using the data once (just read directly)

3. **From SQL queries**: Execute SQL and get a DataFrame — [From SQL Queries](#3-from-sql-queries)

   - **Best for**: Complex joins/aggregations, leveraging SQL expertise, migrating SQL codebases
   - **Avoid if**: You need programmatic column names or dynamic transformations

4. **From in-memory data**: Create from Arrow RecordBatches — [From In-Memory Data](#4-from-in-memory-data)

   - **Best for**: Integrating with Arrow ecosystem (Flight, other systems), processing existing batches
   - **Avoid if**: Starting from scratch (use inline data or files)

5. **From inline data**: Quick examples and tests with the macro — [From Inline Data](#5-from-inline-data-using-the-dataframe-macro)

   - **Best for**: Unit tests, prototypes, examples, learning
   - **Avoid if**: Data exceeds ~10K rows or comes from external sources

6. **Advanced**: Construct directly from a LogicalPlan — [Constructing from a LogicalPlan](#6-advanced-constructing-from-a-logicalplan)
   - **Best for**: Custom query builders, Substrait integration, optimizer testing
   - **Avoid if**: Higher-level methods (1-5) meet your needs

---

### 1. From Files

Reading from files is the most common and powerful way to load data into DataFusion DataFrames. It is the primary entry point for production workloads—whether you're querying a partitioned data lake, processing ETL batches from cloud storage (S3/GCS/Azure), or analyzing a local file.

DataFusion is designed for efficient file scans:

- **Lazy & streaming** – reads data in batches and only when an action runs, keeping memory usage low and handling datasets larger than RAM.
- **Optimized by default** – applies predicate and projection pushdown, reading only the rows and columns you need.

Basic pattern: `ctx.read_<format>(path, options).await?`

- Note: For options use the representative builders
- **Exception** : CSV options differes! Use the builder: [`CsvReadOptions::new()`][CsvReadOptions] (not `default()`, `new()` is for common cases, default() pro minimal, base configuration)

> **Scan, not read**: The `read_*` methods build a lazy file scan—no bytes are loaded until an action runs ([`.collect()`], [`.show()`], [`.execute_stream()`]). Data is streamed in batches during execution.

```rust
use datafusion::prelude::*;
use datafusion::error::Result;

#[tokio::main]
async fn main() -> Result<()> {
    let ctx = SessionContext::new();

    // Read a Parquet file into a DataFrame
    // Parquet with custom options
let df_parquet = ctx.read_parquet(
    "data.parquet",
    ParquetReadOptions::new()
        // .schema(&schema)                 // override embedded schema
        // .parquet_pruning(true)           // enable row group pruning (default: true)
        // .skip_metadata(true)             // skip file metadata to avoid conflicts
        // .file_extension(".parquet")      // custom file extension filter
        // .table_partition_cols(vec![      // specify partition columns for pruning
        //     ("year".to_string(), DataType::Int32),
        //     ("month".to_string(), DataType::Int32),
        // ])
        // .file_sort_order(vec![...])      // indicate known sort order
).await?;

    // Data is streamed; nothing loads until an action
    df.show().await?;
    Ok(())
}
```

#### Choosing a File Format

DataFusion supports several common formats. Storage layout (columnar vs row-oriented) strongly affects performance.

| Format                            | Layout   | Memory | Startup  | Predicate Pushdown | Best For                         |
| --------------------------------- | -------- | ------ | -------- | ------------------ | -------------------------------- |
| **Parquet** ([`.read_parquet()`]) | Columnar | Low    | Low      | Yes (statistics)   | Production analytics, large data |
| **Arrow IPC** ([`.read_arrow()`]) | Columnar | Low    | Very Low | No                 | Arrow ecosystem, zero-copy       |
| **Avro** ([`.read_avro()`])       | Row      | Medium | Low      | Limited            | Kafka, schema evolution          |
| **CSV** ([`.read_csv()`])         | Row      | Medium | High     | No                 | Simple exchange, imports         |
| **NDJSON** ([`.read_json()`])     | Row      | Medium | Medium   | No                 | Semi-structured logs/APIs        |

> For analytics, prefer columnar formats like **Parquet** or **Arrow IPC**. Columnar storage lets DataFusion read only the needed columns, drastically reducing I/O. Row-based formats (Avro, CSV, JSON) must read entire rows even when you need one field.

#### Common Reading Patterns and Schema Handling

Beyond reading a single file, two key areas to understand are how to read multiple files and how to manage schemas.

#### 1) Reading multiple files and customizing options

DataFusion can read files with custom options and multiple files as a single `DataFrame` (via a list of paths or glob patterns) and lets you tune format-specific options.

```rust
use datafusion::prelude::*;
use datafusion::datasource::arrow::ArrowReadOptions;

// CSV with custom options
let df = ctx.read_csv(
    "data.csv",
    CsvReadOptions::new()
        .schema(&schema)       // enforce canonical types
        .has_header(true)      // file has a header row
        .delimiter(b',')       // single-byte delimiter (e.g., b';' for semicolon)
        // .quote(b'"')        // optional: quote character (default b'"')
        // .comment(b'#')      // optional: ignore lines starting with '#'
        // .truncated_rows(true) // optional: allow fewer columns and fill NULLs for nullable fields
).await?;

// NDJSON with custom options (newline-delimited JSON)
let df_json = ctx.read_json(
    "data.ndjson",
    NdJsonReadOptions::default()
        // .schema(&schema)                  // enforce schema (skips inference)
        // .schema_infer_max_records(1000)   // rows to scan for inference (default: 1000)
        // .file_extension(".jsonl")         // custom extension (default: ".json")
        // .file_compression_type(           // handle compressed files
        //     FileCompressionType::GZIP
        // )
        // .table_partition_cols(vec![...])  // partition columns for pruning
        // .mark_infinite(true)              // for unbounded streams (FIFO files)
        // .file_sort_order(vec![...])       // indicate known sort order
).await?;

// Multiple files (works for all formats)
let df_multi = ctx.read_parquet(
    vec!["data1.parquet", "data2.parquet"],
    ParquetReadOptions::default()
).await?;

// Glob patterns to discover files (local or object stores)
let df_glob = ctx.read_parquet(
    "s3://bucket/data/year=2024/**/*.parquet",
    ParquetReadOptions::default()
).await?;
```

> **NDJSON**: Each line is a separate JSON object (efficient for streaming). Common extensions: `.ndjson`, `.jsonl` (sometimes `.json`).

> **Arrow IPC**: Arrow's native serialization format (a.k.a. Feather v2). Zero-copy in Arrow ecosystem and very fast to read.

#### 2) Schema handling: inference vs explicit schema

> **Schema inference**: When no schema is provided, DataFusion inspects the first `schema_infer_max_records` rows/objects (default: 1000) to guess column names and data types. Parquet/Arrow/Avro embed schemas (no inference needed).

By default, DataFusion infers schemas:

- **Parquet/Avro/Arrow**: Schema embedded in file metadata (instant, accurate)
- **CSV**: Scans the first [`schema_infer_max_records`][schema_infer_max_records_csv] rows (default: 1000)
- **NDJSON**: Scans the first [`schema_infer_max_records`][schema_infer_max_records_json] objects (default: 1000)

For CSV/NDJSON in production, providing an **explicit schema is strongly recommended** to avoid inference overhead and mis-typed columns. Adding an expicit schema is shwon in the following:

<!-- TODO: Cross-link to a central "Schema Inference: behavior and limits" section in schema-management.md once finalized. -->

```rust
use datafusion::arrow::datatypes::{Schema, Field, DataType};
use std::sync::Arc;

// Define explicit schema
let schema = Arc::new(Schema::new(vec![
    Field::new("id", DataType::Int64, false),
    Field::new("name", DataType::Utf8, true),
    Field::new("amount", DataType::Float64, true),
]));

// Use explicit schema (skips inference)
let df = ctx.read_csv(
    "data.csv",
    CsvReadOptions::new()
        .schema(&schema)
        .has_header(true)
).await?;
```

**When to use explicit schemas**:

- CSV/NDJSON in production for reliability
- Enforcing specific types (e.g., force `Utf8` vs inferred integer)
- Skipping inference on very large files for performance
- When early rows/objects aren't representative (avoid mis-inference)

#### References:

See also (DataFusion):

> - Example Usage (SQL and DataFrame on CSV): [example-usage](../../user-guide/example-usage.md)
> - Using the SQL API: ../using-the-sql-api.md
> - SQL Format Options (CSV/JSON/Parquet): ../../user-guide/sql/format_options.md
> - Examples (repo): datafusion-examples/examples/dataframe.rs, datafusion-examples/examples/dataframe_transformations.rs

See also (other DataFrame libraries)

> - Polars (Rust): Lazy vs eager (scan vs read) — https://docs.pola.rs/user-guide/lazy/
> - Polars CSV — https://docs.pola.rs/user-guide/io/csv/
> - Polars Parquet — https://docs.pola.rs/user-guide/io/parquet/
> - Polars JSON — https://docs.pola.rs/user-guide/io/json/
> - Spark SQL: CSV — https://spark.apache.org/docs/latest/sql-data-sources-csv.html
> - Spark SQL: JSON — https://spark.apache.org/docs/latest/sql-data-sources-json.html
> - Spark SQL: Parquet — https://spark.apache.org/docs/latest/sql-data-sources-parquet.html

<!-- TODO: Cleaning references -->

### 2. From a Registered Table

Registration is a powerful pattern in DataFusion that creates a logical name for a physical data source. This abstracts away the underlying details so you work with a simple name like "sales" instead of a file path or connection string.

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

> **How it works**: Registration creates a catalog entry with the data source's schema. The actual data is scanned lazily when queries execute, using DataFusion's streaming execution engine.

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
// Programmatic inspection (after registering tables)
let catalog = ctx.catalog("datafusion").unwrap();
let schema = catalog.schema("public").unwrap();
let tables = schema.table_names();
println!("Registered tables: {:?}", tables);

// SQL inspection
let tables_df = ctx.sql(
    r#"
    SELECT table_catalog, table_schema, table_name, table_type
    FROM information_schema.tables
    WHERE table_schema != 'information_schema'
    "#
).await?;
tables_df.show().await?;
```

> **Best practice**: Use SQL [`information_schema`] for ad-hoc exploration and programmatic catalog APIs for dynamic applications.

#### Advanced: Custom TableProviders

For custom sources (APIs, databases, computed tables), implement the [`TableProvider`] trait and register it with the session. See the advanced guide for details.

```rust
// Register a custom data source
ctx.register_table("custom_source", Arc::new(MyCustomProvider::new()))?;
```

See: [Custom TableProviders](dataframe-advance.md#custom-tableproviders)

#### References

- **Internal (DataFusion API — registration, catalog, mixing)**

  - `TableProvider` — https://docs.rs/datafusion/latest/datafusion/catalog/trait.TableProvider.html
  - `CatalogProvider` / `SchemaProvider` — https://docs.rs/datafusion/latest/datafusion/catalog/trait.CatalogProvider.html · https://docs.rs/datafusion/latest/datafusion/catalog/trait.SchemaProvider.html
  - `MemoryCatalogProvider` / `MemorySchemaProvider` — https://docs.rs/datafusion/latest/datafusion/catalog/memory/struct.MemoryCatalogProvider.html · https://docs.rs/datafusion/latest/datafusion/catalog/memory/struct.MemorySchemaProvider.html
  - [`information_schema`](docs/source/user-guide/sql/information_schema.md)
  - [`ViewTable` (SQL views)](https://docs.rs/datafusion/latest/datafusion/catalog/view/struct.ViewTable.html)
  - Tokio in datafusion docs: [datafusion tokio]
  - Datafusion blogpost: [Using Rust async for Query Execution][tokio_blogpost]

- **External (analogs in other systems)**
  - Spark: [spark guide]
  - Spark SQL programmically: [running-sql-queries-programmatically]
  - Polars: [register DataFrames for SQL](https://docs.pola.rs/user-guide/sql/intro/#register-dataframes)
  - DuckDB: [register objects as tables](https://duckdb.org/docs/api/python/overview#registering-python-objects-as-tables)
  - Medium article : [register dataframes as sql in pyspark][medium article]
  - Tokio deep dive: [Tokio tutorial][tokio_tutorial]

---

### 3. From SQL Queries

Execute SQL queries and get the result as a `DataFrame` using [`.sql()`]. This is DataFusion's hybrid strength: leverage SQL for complex relational operations, then seamlessly continue with DataFrames for programmatic logic—both compile to the same optimized columnar execution plan.

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

- **Internal (DataFusion)**

  - [Concepts: Mixing SQL and DataFrames](concepts.md#mixing-sql-and-dataframes) — Deeper dive into the hybrid execution model
  - [SQL Reference](../../user-guide/sql/index.md) — Full SQL syntax, functions, and data types
  - [`TableProvider` API](https://docs.rs/datafusion/latest/datafusion/catalog/trait.TableProvider.html) — Advanced: connecting to external sources with pushdown

- **External (related concepts)**
  - Spark guid: [spark guide]
  - Spark: [Catalyst Optimizer](https://databricks.com/glossary/catalyst-optimizer) — Similar unified DataFrame/SQL execution plans

---

### 4. From Arrow [`RecordBatch`]es: The Native Pathway

Create DataFrames directly from in-memory Arrow [`RecordBatch`]es. This is the most native and efficient way to get data into DataFusion because **you're speaking the engine's native language**, often with zero-copy overhead.

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

#### References <!-- TODO: References -->

---

### 5. From Inline Data (using the [`dataframe!`] macro)

Create DataFrames directly from Rust literals—no files, no external data sources. This approach shines when your data is small, temporary, and lives entirely in code.

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

#### References <!-- TODO: here references -->

---

### 6. Advanced: Constructing from a LogicalPlan

You are no longer just using the query engine — you are programming it. Constructing a [`LogicalPlan`][LogicalPlan] and handing it to [`DataFrame::new`] lets you inject, rewrite, and compose queries at the AST level, then pass them through DataFusion's optimizer and executor.

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

```
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
  - Custom sources: [Custom TableProviders](dataframe-advance.md#custom-tableproviders)
  - Performance and plan behavior: [Performance Optimizations](dataframe-advance.md#performance-optimizations)
  - Debugging and best practices: [Debugging & Best Practices](dataframe-advance.md#debugging--best-practices)
  - [Ruminations on Multi-Tenant Databases]

---

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
    let ctx_small = SessionContext::with_config(config_small);

    // Large batch size - higher memory, better throughput
    let config_large = SessionConfig::new().with_batch_size(65536);
    let ctx_large = SessionContext::with_config(config_large);

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
    let runtime = Arc::new(RuntimeEnv::new(runtime_config)?);

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
[`.create_physical_plan()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.create_physical_plan

<!-- orther -->

[catalog schema]: https://datafusion.apache.org/library-user-guide/catalogs.html
[Ruminations on Multi-Tenant Databases]: https://www.db.in.tum.de/research/publications/conferences/BTW2007-mtd.pdf
[`TableScan`]: https://docs.rs/datafusion/latest/datafusion/logical_expr/logical_plan/struct.TableScan.html
[datafusion planning]: https://docs.rs/datafusion/latest/datafusion/#planning
[`RecordBatch`]: https://docs.rs/arrow/latest/arrow/record_batch/struct.RecordBatch.html
[Arror Flight]: https://arrow.apache.org/blog/2019/10/13/introducing-arrow-flight/
[datafusion tokio]: https://docs.rs/datafusion/latest/datafusion/#thread-scheduling-cpu--io-thread-pools-and-tokio-runtimes
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
