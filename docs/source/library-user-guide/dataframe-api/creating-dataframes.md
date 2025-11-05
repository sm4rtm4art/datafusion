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

> **For Advanced Topics**: This guide covers the foundational API calls for creating DataFrames. For a deeper dive into production-level concerns like schema management, error handling, connecting to S3, Arrow Flight, Kafka, custom providers, and performance tuning, please see our dedicated guide to [Advanced Creation Topics](creating-dataframes-advanced.md) .

```{contents}
:local:
:depth: 2
```

To situate DataFrames in the overall architecture, here is a brief recap from [Concepts](concepts.md): a high‑level diagram showing where DataFusion DataFrames sit in the query pipeline.

```
SessionContext
  ↓ creates
DataFrame (lazy)
  ↓ wraps
LogicalPlan
  ↓ optimizes
Optimized LogicalPlan
  ↓ plans into
ExecutionPlan (Physical Plan)
  ↓ optimizes
Optimized ExecutionPlan
  ↓ executes
RecordBatch streams
```

> **How to read this**
>
> - [[`SessionContext`][SessionContext] holds catalogs, config, and runtime
> - DataFrame creation is lazy: it builds a [`LogicalPlan`][LogicalPlan] (no data is read yet)
> - Add transforms (filter/join/aggregate) to the plan
> - An action (`.collect()`, `.show()`) triggers optimization and execution
> - Results are Arrow `RecordBatches`
>
> **See also**
>
> - Concepts: [`concepts.md`](./concepts.md)
> - Transformations: [`transformations.md`](transformations.md)
> - DataFrame Execution (later in this guide)

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

> **Performance note**: Registration caches schema and file metadata so subsequent DataFrames avoid re-inferring schemas and re-reading file footers. This is especially valuable for Parquet files where metadata parsing can be expensive. For advanced performance techniques, see [Advanced Topics](creating-dataframes-advanced.md#performance-optimizations).
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

| Scenario                                          | Prefer      | Rationale                                                                                              | References                                                                                                                                                                     |
| ------------------------------------------------- | ----------- | ------------------------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Multiple queries or SQL/discovery are anticipated | Register    | Reuses cached metadata; enables SQL (`ctx.sql`), `SHOW TABLES`, and `information_schema` introspection | [Catalogs Guide](../catalogs.md#general-concepts), [information_schema](../../user-guide/sql/information_schema.md)                                                            |
| Remote object stores or many files/partitions     | Register    | Amortizes metadata I/O and improves pruning across files                                               | [Parquet pruning](https://datafusion.apache.org/blog/2025/03/20/parquet-pruning/), [External indexes](https://datafusion.apache.org/blog/2025/08/15/external-parquet-indexes/) |
| One-off, small, local scans (e.g., CSV/NDJSON)    | Direct read | No catalog state or refresh policy required; minimal startup cost                                      | —                                                                                                                                                                              |

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

#[tokio::main]
async fn main() -> Result<()> {
    let ctx = SessionContext::new();

    ctx.register_csv("sales", "sales.csv", CsvReadOptions::new()).await?;
    ctx.register_parquet("users", "users.parquet", ParquetReadOptions::default()).await?;

    // SHOW TABLES for a quick overview
    let df = ctx.sql("SHOW TABLES").await?;
    df.show().await?;

    Ok(())
}
```

Output (simplified):

```
+------------+
| table_name |
+------------+
| sales      |
| users      |
+------------+
```

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

> See also
>
> - DataFusion
>   - [Catalogs, Schemas, and Tables](../catalogs.md)
>   - [information_schema](../../user-guide/sql/information_schema.md)
>   - [Transformations: Mixing SQL and DataFrames](transformations.md#mixing-sql-and-dataframes)
>   - [Advanced: Performance Optimizations](creating-dataframes-advanced.md#performance-optimizations)
> - Spark (query engine + DataFrame API + discovery)
>   - [Spark SQL and DataFrames Guide](https://spark.apache.org/docs/latest/sql-programming-guide.html)
>   - [SHOW TABLES](https://spark.apache.org/docs/latest/sql-ref-syntax-aux-show-tables.html)
>   - [SparkSession Catalog API (PySpark)](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.Catalog.html)
> - Flink (Table API + SQL catalogs)
>   - [Flink Table/SQL: Catalogs](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/table/catalogs/)
> - Trino (SQL query engine with catalogs)
>   - [Concepts: Catalogs and Schemas](https://trino.io/docs/current/overview/concepts.html#catalogs-and-schemas)
> - DuckDB (discovery & information_schema)
>   - [SHOW statements](https://duckdb.org/docs/sql/statements/show)
>   - [information_schema](https://duckdb.org/docs/stable/sql/meta/information_schema)
> - ANSI-style metadata
>   - [PostgreSQL information_schema](https://www.postgresql.org/docs/current/information-schema.html)
>   - [MySQL information_schema](https://dev.mysql.com/doc/mysql-infoschema-excerpt/8.0/en/information-schema-introduction.html)

---

## How to create a Dataframe

This section shows the practical ways to construct a `DataFrame` in DataFusion: from files, registered tables, SQL, in‑memory data, inline data, and from a `LogicalPlan`. All approaches compile to the same `LogicalPlan`, so you can freely mix SQL and the DataFrame API. By this you achive the best of both worlds. Choose based on where the data lives and whether you’ll reuse it (see the decision guide above).

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

> **Scan, not read**: The `read_*` methods build a lazy file scan—no bytes are loaded until an action runs ([`.collect()`], [`.show()`], [`.execute_stream()`]). Data is streamed in batches during execution.

```rust
use datafusion::prelude::*;
use datafusion::error::Result;

#[tokio::main]
async fn main() -> Result<()> {
    let ctx = SessionContext::new();

    // Read a Parquet file into a DataFrame
    let df = ctx
        .read_parquet("path/to/data.parquet", ParquetReadOptions::default())
        .await?;

    // Data is streamed; nothing loads until an action
    df.show().await?;
    Ok(())
}
```

#### Choosing a File Format

DataFusion supports several common formats. Storage layout (columnar vs row-oriented) strongly affects analytics performance.

| Format        | Layout   | Memory | Startup  | Streaming | Predicate Pushdown | Best For                         |
| ------------- | -------- | ------ | -------- | --------- | ------------------ | -------------------------------- |
| **Parquet**   | Columnar | Low    | Low      | Yes       | Yes (statistics)   | Production analytics, large data |
| **Arrow IPC** | Columnar | Low    | Very Low | Yes       | No                 | Arrow ecosystem, zero-copy       |
| **Avro**      | Row      | Medium | Low      | Yes       | Limited            | Kafka, schema evolution          |
| **CSV**       | Row      | Medium | High     | Yes       | No                 | Simple exchange, imports         |
| **NDJSON**    | Row      | Medium | Medium   | Yes       | No                 | Semi-structured logs/APIs        |

> For analytics, prefer columnar formats like **Parquet** or **Arrow IPC**. Columnar storage lets DataFusion read only the needed columns, drastically reducing I/O. Row-based formats (Avro, CSV, JSON) must read entire rows even when you need one field.

#### Common Reading Patterns and Schema Handling

Beyond reading a single file, two key areas to understand are how to read multiple files and how to manage schemas.

#### 1) Reading multiple files and customizing options

DataFusion can read multiple files as a single `DataFrame` (via a list of paths or glob patterns) and lets you tune format-specific options.

```rust
use datafusion::prelude::*;
use datafusion::datasource::arrow::ArrowReadOptions;

// CSV with custom options
let df_csv = ctx.read_csv(
    "data.csv",
    CsvReadOptions::new()
        .has_header(true)
        .delimiter(b';')
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

> **Schema inference**: When no schema is provided, DataFusion inspects the first `schema_infer_max_records` rows/objects (default: 1000) to guess column names and data types. Parquet/Arrow/Avro embed schemas (no inference needed). Providing an explicit schema skips inference and is recommended for CSV/NDJSON in production.

By default, DataFusion infers schemas:

- **Parquet/Avro/Arrow**: Schema embedded in file metadata (instant, accurate)
- **CSV**: Scans the first [`schema_infer_max_records`][schema_infer_max_records_csv] rows (default: 1000)
- **NDJSON**: Scans the first [`schema_infer_max_records`][schema_infer_max_records_json] objects (default: 1000)

For CSV/NDJSON in production, providing an explicit schema is strongly recommended to avoid inference overhead and mis-typed columns:

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

> **When to use explicit schemas**:
>
> - CSV/NDJSON in production for reliability
> - Enforcing specific types (e.g., force `Utf8` vs inferred integer)
> - Skipping inference on very large files for performance
> - When early rows/objects aren't representative (avoid mis-inference)

---

### 2. From a Registered Table

Registration is a powerful pattern in DataFusion that lets you name data sources once and reuse them efficiently across queries. When you register a table, DataFusion creates a catalog entry that points to your data source—the data itself isn't loaded into memory, but the schema is cached and the source is ready for lazy scanning.

For the catalog hierarchy and ways to inspect registered objects, see [Understanding DataFusion's Data Organization](#understanding-datafusions-data-organization).

This pattern is essential for:

- **Performance**: Schema inference happens once at registration, not on every query
- **Interoperability**: Registered tables work seamlessly in both DataFrame API and SQL
- **Code clarity**: Named tables make complex pipelines more readable and maintainable

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

Registration enables seamless interoperability between SQL and DataFrame APIs—a one of the strength of DataFusion:

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

    // Register file data (lazy - no loading)
    ctx.register_parquet("orders", "orders.parquet", ParquetReadOptions::default()).await?;

    // Now use either API - they see the same tables

    // DataFrame API
    let users_df = ctx.table("users").await?;
    let orders_df = ctx.table("orders").await?;
    let joined = users_df.join(orders_df, JoinType::Inner, &["id"], &["user_id"], None)?;

    // SQL API - same tables available
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

#### Inspecting the catalog

See [Understanding DataFusion's Data Organization](#understanding-datafusions-data-organization) for the full hierarchy and inspection tools. Quick recap: objects are organized as catalog → schema → table. Defaults are catalog `datafusion` and schema `public` (plus optional `information_schema`).

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

> **Best practice**: Use SQL `information_schema` for ad-hoc exploration and programmatic catalog APIs for dynamic applications.

#### Advanced: Custom TableProviders

For data sources beyond files, implement the [`TableProvider`] trait:

```rust
// Custom sources like APIs, databases, or computed tables
ctx.register_table("custom_source", Arc::new(MyCustomProvider::new()))?;
```

See the [custom_table_providers example] for implementation details.

---

<!-- datafram methods  -->

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

<!-- orther -->

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

### 3. From SQL Queries

Execute SQL queries and get the result as a `DataFrame` using [`sql()`]. This is powerful for:

- **Mixing SQL and DataFrame APIs**: Use SQL's declarative syntax for complex joins/aggregations, then DataFrame methods for programmatic transformations
- **Migrating from SQL**: Gradually transition SQL-heavy codebases to DataFrames
- **Leveraging SQL expertise**: Write familiar SQL while gaining DataFrame flexibility

**Key method:**

| Method    | Purpose                                  |
| --------- | ---------------------------------------- |
| [`sql()`] | Execute SQL query and return a DataFrame |

#### Example: Mixing SQL and DataFrame Operations

```rust
use std::sync::Arc;
use datafusion::prelude::*;
use datafusion::arrow::array::{ArrayRef, Int32Array, StringArray};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::Result;

#[tokio::main]
async fn main() -> Result<()> {
    let ctx = SessionContext::new();

    // Register sample data
    let users = RecordBatch::try_from_iter(vec![
        ("id", Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef),
        ("name", Arc::new(StringArray::from(vec!["Alice", "Bob", "Carol"])) as ArrayRef),
        ("department", Arc::new(StringArray::from(vec!["Engineering", "Sales", "Engineering"])) as ArrayRef),
    ])?;
    ctx.register_batch("users", users)?;

    // Start with SQL for complex logic
    let df = ctx.sql("
        SELECT
            department,
            COUNT(*) as employee_count
        FROM users
        WHERE name != 'Bob'
        GROUP BY department
    ").await?;

    // Continue with DataFrame API for programmatic filtering
    let df = df.filter(col("employee_count").gt(lit(1)))?
              .sort(vec![col("employee_count").sort(false, true)])?;

    df.show().await?;
    // Outputs:
    // +-------------+----------------+
    // | department  | employee_count |
    // +-------------+----------------+
    // | Engineering | 2              |
    // +-------------+----------------+

    Ok(())
}
```

#### Example: Round-Trip Between DataFrame and SQL

You can build DataFrames programmatically, register them as views, query with SQL, and continue with DataFrame operations:

```rust
use datafusion::prelude::*;
use datafusion::error::Result;

#[tokio::main]
async fn main() -> Result<()> {
    let ctx = SessionContext::new();
    ctx.register_parquet("sales", "sales.parquet", ParquetReadOptions::default()).await?;

    // 1. Start with DataFrame API
    let high_value = ctx.table("sales").await?
        .filter(col("amount").gt(lit(1000)))?
        .select(vec![col("region"), col("product"), col("amount")])?;

    // 2. Register as a view
    ctx.register_table("high_value_sales", high_value.into_view())?;

    // 3. Query with SQL (complex aggregation)
    let summary = ctx.sql("
        SELECT
            region,
            COUNT(DISTINCT product) as product_count,
            SUM(amount) as total_revenue,
            AVG(amount) as avg_sale
        FROM high_value_sales
        GROUP BY region
        HAVING SUM(amount) > 10000
        ORDER BY total_revenue DESC
    ").await?;

    // 4. Back to DataFrame API for final transformation
    let top_regions = summary
        .limit(0, Some(5))?
        .with_column("revenue_millions", col("total_revenue") / lit(1_000_000))?;

    top_regions.show().await?;

    Ok(())
}
```

> **Tip**: SQL queries in DataFusion can reference any registered table, view, or file. For more patterns, see [Mixing SQL and DataFrames](transformations.md#mixing-sql-and-dataframes). For SQL syntax details, see the [SQL Reference](../../user-guide/sql/index.md).

### 4. From In-Memory Data

Create DataFrames from Arrow [`RecordBatch`]es using [`read_batch()`] or [`read_batches()`]. This is useful for:

- **Testing and prototyping**: Quickly create sample data for development
- **Integrating with Arrow ecosystem**: Process data from Arrow Flight, Parquet readers, or other Arrow-based systems
- **In-process analytics**: Analyze data already in Arrow format without serialization overhead
- **Joining external data**: Combine programmatically-generated data with existing tables

**Key methods:**

| Method             | Purpose                                      |
| ------------------ | -------------------------------------------- |
| [`read_batch()`]   | Create DataFrame from a single RecordBatch   |
| [`read_batches()`] | Create DataFrame from multiple RecordBatches |

**Example - Creating DataFrame from RecordBatch:**

```rust
use std::sync::Arc;
use datafusion::prelude::*;
use datafusion::arrow::array::{ArrayRef, Int32Array, Float64Array};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::Result;

#[tokio::main]
async fn main() -> Result<()> {
    let ctx = SessionContext::new();

    // Create a RecordBatch (e.g., from Arrow Flight, Parquet, or generated)
    let batch = RecordBatch::try_from_iter(vec![
        ("product_id", Arc::new(Int32Array::from(vec![1, 2, 3, 4])) as ArrayRef),
        ("revenue", Arc::new(Float64Array::from(vec![1200.0, 450.0, 890.0, 2100.0])) as ArrayRef),
    ])?;

    // Create DataFrame and apply transformations
    let df = ctx.read_batch(batch)?
        .filter(col("revenue").gt(lit(500.0)))?
        .sort(vec![col("revenue").sort(false, true)])?;

    df.show().await?;
    // Outputs:
    // +------------+---------+
    // | product_id | revenue |
    // +------------+---------+
    // | 4          | 2100.0  |
    // | 1          | 1200.0  |
    // | 3          | 890.0   |
    // +------------+---------+

    Ok(())
}
```

**Example - Multiple batches:**

```rust
// Process multiple RecordBatches as a single DataFrame
let batches = vec![batch1, batch2, batch3];
let df = ctx.read_batches(batches)?;
```

> **Tip**: If you need to reuse the same RecordBatch data multiple times, consider registering it as a table with [`register_batch()`] instead. See [From a Registered Table](#2-from-a-registered-table).

## Schema Management

When creating DataFrames from multiple sources or evolving data over time, understanding schema management is essential for avoiding errors and ensuring data consistency.

### Schema Evolution Across Files

When reading multiple files with slightly different schemas, DataFusion can handle schema evolution:

```rust
use datafusion::prelude::*;
use datafusion::error::Result;

#[tokio::main]
async fn main() -> Result<()> {
    let ctx = SessionContext::new();

    // Files with evolving schemas:
    // - data_v1.parquet: (id: Int32, name: Utf8)
    // - data_v2.parquet: (id: Int32, name: Utf8, email: Utf8)
    // - data_v3.parquet: (id: Int64, name: Utf8, email: Utf8, age: Int32)

    // DataFusion will:
    // 1. Union all columns (id, name, email, age)
    // 2. Fill missing columns with NULLs
    // 3. Apply type coercion where possible
    let df = ctx.read_parquet(
        vec!["data_v1.parquet", "data_v2.parquet", "data_v3.parquet"],
        ParquetReadOptions::default()
    ).await?;

    // Result schema: (id: Int64, name: Utf8, email: Utf8 (nullable), age: Int32 (nullable))
    println!("Unified schema: {}", df.schema());

    Ok(())
}
```

> **Schema unification rules**:
>
> - Columns present in some files but not others become nullable
> - Numeric types are coerced to the widest type (Int32 → Int64 → Float64)
> - String types (Utf8, LargeUtf8) are compatible
> - Incompatible types (e.g., Int32 vs Utf8) cause errors

### Type Coercion Rules

DataFusion automatically coerces types in expressions when safe and meaningful:

```rust
use datafusion::prelude::*;

// Int32 + Int64 → Int64
let expr = col("int32_col") + col("int64_col");

// Int32 > Float64 → both coerced to Float64
let filter = col("age").gt(lit(25.5));

// Utf8 concatenation
let concat = col("first_name").concat(lit(" ")).concat(col("last_name"));
```

**Coercion hierarchy** (lower → higher):

```
Int8 → Int16 → Int32 → Int64 → Float32 → Float64
UInt8 → UInt16 → UInt32 → UInt64
Utf8 ↔ LargeUtf8 (interchangeable)
Date32 → Date64 → Timestamp
```

> **Reference**: For complete type coercion rules, see [SQL Data Types](../../user-guide/sql/data_types.md).

### Handling Schema Conflicts

When combining DataFrames with incompatible schemas, you have several strategies:

#### 1. Explicit Column Selection

```rust
use datafusion::prelude::*;
use datafusion::arrow::datatypes::{DataType, TimeUnit};

// Two DataFrames with different schemas
let df1 = ctx.table("sales_2023").await?; // (date, amount, region)
let df2 = ctx.table("sales_2024").await?; // (timestamp, revenue, geo, category)

// Select common columns with explicit casting
let unified1 = df1.select(vec![
    col("date").cast_to(&DataType::Timestamp(TimeUnit::Second, None), df1.schema())?.alias("timestamp"),
    col("amount").alias("revenue"),
    col("region").alias("geo"),
    lit("Unknown").alias("category"),
])?;

let unified2 = df2.select(vec![
    col("timestamp"),
    col("revenue"),
    col("geo"),
    col("category"),
])?;

// Now schemas match - can union
let combined = unified1.union(unified2)?;
```

#### 2. Union by Name (Schema Flexibility)

```rust
use datafusion::prelude::*;

// union_by_name handles different column orders and missing columns
let df1 = dataframe!(
    "a" => [1, 2, 3],
    "b" => [4, 5, 6]
)?;

let df2 = dataframe!(
    "b" => [7, 8],      // Different order
    "a" => [9, 10],
    "c" => [11, 12]     // Extra column
)?;

// Union by name - missing columns filled with NULL
let result = df1.union_by_name(df2)?;
// Schema: (a: Int32, b: Int32, c: Int32 nullable)
```

#### 3. Schema Validation Before Operations

```rust
use datafusion::prelude::*;
use datafusion::arrow::datatypes::{DataType, Schema};

fn schemas_compatible(schema1: &Schema, schema2: &Schema) -> bool {
    if schema1.fields().len() != schema2.fields().len() {
        return false;
    }
    schema1.fields().iter().zip(schema2.fields()).all(|(f1, f2)| {
        f1.name() == f2.name() && f1.data_type() == f2.data_type()
    })
}

// Validate before union
let df1 = ctx.table("table1").await?;
let df2 = ctx.table("table2").await?;

if schemas_compatible(df1.schema().as_ref(), df2.schema().as_ref()) {
    let combined = df1.union(df2)?;
    // ...
} else {
    eprintln!("Schema mismatch - apply transformations first");
}
```

### Schema Inspection and Introspection

```rust
use datafusion::prelude::*;
use datafusion::arrow::datatypes::DataType;

let df = ctx.read_parquet("data.parquet", ParquetReadOptions::default()).await?;
let schema = df.schema();

// Inspect all fields
for field in schema.fields() {
    println!("Column: {}", field.name());
    println!("  Type: {:?}", field.data_type());
    println!("  Nullable: {}", field.is_nullable());

    // Check specific types
    match field.data_type() {
        DataType::Int32 | DataType::Int64 => println!("  → Integer column"),
        DataType::Utf8 | DataType::LargeUtf8 => println!("  → String column"),
        DataType::List(_) | DataType::LargeList(_) => println!("  → Array column"),
        DataType::Struct(_) => println!("  → Nested struct"),
        _ => {}
    }
}

// Check for specific column
if schema.column_with_name("user_id").is_some() {
    println!("Found user_id column");
}

// Get column index
if let Some((idx, _field)) = schema.column_with_name("email") {
    println!("Email is at index {}", idx);
}
```

> **See also**: [Arrow Introduction](../../user-guide/arrow-introduction.md) for a deep dive into Arrow schemas and data types.

### 5. From Inline Data (using the `dataframe!` macro)

Create DataFrames from inline literals. This is ideal for quick examples, unit tests, prototyping, and learning without external files.

#### Method 1: `DataFrame::from_columns()` - Pandas-like approach\*\*

If you're coming from pandas, [`DataFrame::from_columns()`] provides a familiar dictionary-like pattern:

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

#### Method 2: `dataframe!` macro - Simplified syntax\*\*

The [`dataframe!`] macro provides the same functionality with much less boilerplate:

```rust
use datafusion::prelude::*;
use datafusion::error::Result;

#[tokio::main]
async fn main() -> Result<()> {
    let df = dataframe!(
        "id" => [1, 2, 3],
        "name" => ["Alice", "Bob", "Carol"]
    )?;
    df.show().await?;
    Ok(())
}
```

**Handling null values:**

Both approaches use Rust's `Option` type for nullable values:

```rust
// With dataframe! macro
let df = dataframe!(
    "id" => [1, 2, 3],
    "value" => [Some("foo"), None, Some("bar")],  // Nullable string column
    "score" => [Some(100), Some(200), None]       // Nullable int column
)?;
```

> **Note on null values**: In Rust, `Some(value)` represents a present value and `None` represents a null (SQL `NULL`). For important distinctions between `None`, `Null`, and `NaN`, see [Understanding Null Values](../../user-guide/dataframe.md#understanding-null-values-none-null-and-nan) in the Users Guide.

**Key differences:**

| Aspect                | `from_columns()`                           | `dataframe!` macro         |
| --------------------- | ------------------------------------------ | -------------------------- |
| **Syntax**            | Requires `Arc`, `ArrayRef`, type imports   | Minimal, infers everything |
| **Use case**          | When you have existing Arrow arrays        | Quick inline data creation |
| **Type inference**    | Manual (`Int32Array`, `StringArray`, etc.) | Automatic from literals    |
| **Verbosity**         | ~10 lines for simple example               | ~3 lines for same result   |
| **Nullability**       | Use `Some(value)` / `None`                 | Same - `Some()` / `None`   |
| **Pandas similarity** | Dictionary-like `vec![("col", array)]`     | Cleaner column syntax      |

#### Testing with `assert_batches_eq!`

When writing tests, the `dataframe!` macro pairs perfectly with [`assert_batches_eq!`] for validating results. While `dataframe!` creates test data, `assert_batches_eq!` validates that your DataFrame operations produce the expected output.

The [`assert_batches_eq!`] macro compares the pretty-formatted output of RecordBatches with an expected vector of strings. It's designed so that **failure output can be directly copy/pasted** into your test code as expected results—making test maintenance simple. This works with **any** DataFrame (from files, SQL, in-memory, etc.), not just those created with `dataframe!`.

**How it works:**

- **On success**: Returns `()` silently (test passes)
- **On mismatch**: Panics with a readable diff showing expected vs actual tables
- **Comparison**: Exact string match of the pretty-printed table (order-sensitive)
- **Error location**: Being a macro, errors appear on the correct line in your test

**Signature:** `assert_batches_eq!(expected_lines: &[&str], batches: &[RecordBatch])`

**Example - Testing DataFrame transformations:**

```rust
use datafusion::prelude::*;
use datafusion::assert_batches_eq;

#[tokio::test]
async fn test_filter_and_aggregate() -> datafusion::error::Result<()> {
    // Create test data
    let df = dataframe!(
        "department" => ["Sales", "Sales", "Engineering", "Engineering"],
        "salary" => [50000, 55000, 80000, 85000]
    )?;

    // Apply transformations
    let result = df
        .aggregate(vec![col("department")], vec![sum(col("salary")).alias("total")])?
        .filter(col("total").gt(lit(100000)))?
        .sort(vec![col("total").sort(false, true)])?;

    // Collect and validate
    let batches = result.collect().await?;
    datafusion::assert_batches_eq!(
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

**Key features:**

- **Failure output is copy-pasteable**: When tests fail, you can copy the actual output directly into your expected result
- **Handles NULL and NaN distinctly**: Nulls display as empty cells or `NULL`, NaN displays as `NaN`
- **Works with any data source**: Files, SQL, in-memory, etc.—not just `dataframe!` macro

**Variants:**

- [`assert_batches_sorted_eq!`]: For order-insensitive comparisons

### 6. Advanced: Constructing from a LogicalPlan

For advanced integrations, you can construct a `DataFrame` directly from a pre-built [`LogicalPlan`] using [`DataFrame::new`]. This low-level approach is useful for:

- **Building custom query builders**: Create your own DSL or API that compiles to DataFusion plans
- **Integrating with other systems**: Convert plans from Substrait, Spark, or other query engines
- **Programmatic plan construction**: Use [`LogicalPlanBuilder`] for complex plan manipulations
- **Query rewriting**: Intercept and modify plans before execution
- **Testing optimizer rules**: Create specific plan structures for testing

**Example - Basic construction:**

```rust
use datafusion::prelude::*;
use datafusion::logical_expr::LogicalPlanBuilder;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();

    // Build a plan using LogicalPlanBuilder
    let plan = LogicalPlanBuilder::empty(true).build()?;

    // Construct DataFrame from plan and session state
    let df = DataFrame::new(ctx.state(), plan);

    df.show().await?;
    Ok(())
}
```

**Example - Building a plan programmatically:**

```rust
use datafusion::logical_expr::LogicalPlanBuilder;

// Load a table and build a complex plan
let table = ctx.table("users").await?;
let (_state, base_plan) = table.into_parts();

let plan = LogicalPlanBuilder::from(base_plan)
    .filter(col("age").gt(lit(21)))?
    .project(vec![col("name"), col("age")])?
    .sort(vec![col("age").sort(false, true)])?
    .limit(0, Some(10))?
    .build()?;

let df = DataFrame::new(ctx.state(), plan);
```

**Going deeper:**

- **[`DataFrame::new`]**: Low-level constructor documentation
- **[`LogicalPlanBuilder`]**: Builder API for constructing plans
- **[`LogicalPlan`]**: Plan node types and structure
- **[Query Optimizer guide](query-optimizer.md)**: Understanding how plans are optimized
- **Relationship between `LogicalPlan`s and `DataFrame`s**: See [section below](#relationship-between-logicalplans-and-dataframes) for converting between them

Most users won't need this level of control—the higher-level creation methods (1-5 above) cover typical use cases. Use this approach when you need to work directly with DataFusion's query plan representation.

## Error Handling & Recovery

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

## Real-World Creation Patterns

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

## Creation-Time Optimizations

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

## Configuration Impact on DataFrame Creation

The `SessionContext` configuration significantly affects how DataFrames are created and executed. Understanding these settings helps you tune performance for your workload.

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

### Format-Specific Configuration

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

#### Parquet Reading Options

```rust
use datafusion::prelude::*;

let df = ctx.read_parquet("data.parquet", ParquetReadOptions::new()
    .parquet_pruning(true)       // Enable predicate pushdown (default: true)
    .skip_metadata(false)        // Read metadata (default: false)
).await?;
```

### Memory Limits and Spilling

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

### Connection Pooling for Remote Sources

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

## Interoperability

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

## Best Practices for DataFrame Creation

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

## DataFrame Execution

DataFusion [`DataFrame`]s use **lazy evaluation**: transformations build a query plan without processing data. Execution only happens when you call an action method like [`collect()`] or [`show()`].

> **For complete execution documentation**, see [Concepts § Execution Model](concepts.md#execution-model-actions-vs-transformations), which covers:
>
> - Transformations vs Actions
> - Result-producing methods ([`collect()`], [`show()`], [`execute_stream()`])
> - Sink actions ([`write_parquet()`], [`write_csv()`])
> - Execution trade-offs (collect vs streaming, caching, partitioning)
> - Performance tuning and debugging

**Quick execution reference:**

| Method                         | Use Case                                          |
| ------------------------------ | ------------------------------------------------- |
| [`collect()`]                  | Buffer entire result in memory                    |
| [`execute_stream()`]           | Stream large results incrementally                |
| [`show()`] / [`show_limit(n)`] | Display preview (use `show_limit` for large data) |
| [`count()`]                    | Get row count (optimized for Parquet)             |
| [`cache()`]                    | Materialize for reuse across multiple queries     |

For writing results to files, see [Writing DataFrames](writing-dataframes.md).
