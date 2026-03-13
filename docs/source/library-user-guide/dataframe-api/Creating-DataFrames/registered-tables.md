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

# Creating DataFrames From a Registered Table

<!--TODO

1. ABSTRACT
2. INTRODUCTION
3. DEDUP: "Understanding DataFusion's Data Organization" overlaps with
   creating-concepts.md "The Catalog Model". Per agreement: 60-70% depth
   lives in creating-concepts.md (cognitive authority), 30-40% here
   (action-oriented, focused on registration mechanics). Trim the
   catalog hierarchy explanation here to a brief summary + pointer.
4. DEDUP: "Performance benefits" and "Mixing SQL and DataFrame APIs"
   are solid action content — keep here. But verify no overlap with
   index.md's "How Creation Works" section.
5. REVIEW: Async note on line 118 references "concepts.md" which is
   the legacy monolith — update link to point to the correct target.
-->

```{contents} Table of Contents
:local:
:depth: 2
```

## Introduction (placeholder)

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

For the catalog hierarchy and name resolution rules, see [Understanding DataFusion's Data Organization](#understanding-datafusions-data-organization) below.

### Understanding DataFusion's Data Organization

<!--TODO:

Implement a overview schema of the name resolution in the catalog model

```text
SessionContext
└── CatalogProvider ("datafusion")   ← default catalog
    └── SchemaProvider ("public")    ← default schema
        └── Table ("sales")
            └── Arrow Schema         ← columns + types
```
-->

**Registered tables live in a three-level catalog hierarchy — Catalog →
Schema → Table — with defaults `datafusion` and `public`.**

Unqualified names like `"sales"` resolve to `datafusion.public.sales`.
Two-part names select a schema (`"analytics.sales"`), and fully-qualified
names address a specific catalog (`"warehouse.analytics.sales"`).

For the complete catalog model — including the trait hierarchy, schema
disambiguation, name resolution rules, case sensitivity, and extensibility
— see [The Catalog Model](creating-concepts.md#the-catalog-model).

> **Cloud storage (Rust):** <br>
> To use `s3://`, `gs://`, or `az://` URLs, register an object store in the
> runtime environment. See
> [`datafusion::datasource::object_store`](https://docs.rs/datafusion/latest/datafusion/datasource/object_store/index.html)
> and the
> [S3 setup example](https://github.com/apache/datafusion/blob/main/datafusion-examples/examples/external_dependency/main.rs).

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
