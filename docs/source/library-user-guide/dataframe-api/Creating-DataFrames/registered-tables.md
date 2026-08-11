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

# Creating DataFrames From Registered Tables

**Registering a data source with the [`SessionContext`] makes it
available across all DataFusion APIs — by name, for repeated queries,
without re-deriving metadata.**

DataFusion's DataFrame API and SQL interface both operate on the same
underlying [`LogicalPlan`]. Registration bridges these two worlds:
store a [`TableProvider`] under a logical name in the session catalog,
and every subsequent call to [`.table()`] or `FROM table_name` returns
a fresh, lazy [`DataFrame`] backed by cached schema and metadata. This
page covers how to register, deregister, inspect, and query registered
tables.

**Key methods:**
| Method | Purpose |
| ----------------------- | ------------------------------------ |
| [`.register_parquet()`] | Register Parquet file(s) / directory |
| [`.register_csv()`] | Register CSV file(s) / directory |
| [`.register_json()`] | Register JSON file(s) / directory |
| [`.register_avro()`] | Register Avro file(s) / directory |
| [`.register_arrow()`] | Register Arrow IPC file(s) |
| [`.register_batch()`] | Register in-memory `RecordBatch` |
| [`.register_table()`] | Register any [`TableProvider`] |
| [`.deregister_table()`] | Remove a registered table |
| [`.table()`] | DataFrame from a registered name |
| [`.sql()`] | Query registered tables via SQL |

:::{admonition} Style Note
:class: note
:collapsible: closed

In this document, code elements follow a consistent pattern:

- **DataFrame methods:** `.method()` (e.g., `.select()`, `.filter()`)
- **Standalone functions:** `function()` (e.g., `col()`, `lit()`)
- **Constructors:** `Type::new()` (e.g., `SessionContext::new()`)
- **Types:** `TypeName` (e.g., `SchemaRef`, `RecordBatch`)
- **Lazy transformations:** return a `DataFrame` and build the `LogicalPlan`
- **Actions:** (`.collect()`, `.show()`) trigger execution

:::

```{contents} Table of Contents
:local:
:depth: 2
```

## Registering Data Sources

**Register data sources once and query them by name — the catalog
caches metadata and both SQL and DataFrame APIs see the same logical
table.**

Registration creates a logical name for a physical data source. This
abstracts away the underlying details so you work with a simple name
like `"sales"` instead of a file path or connection string.

This named table bridges DataFusion's two query interfaces — the SQL
interface and the DataFrame API — both orchestrated by
[`SessionContext`]. Under the hood, registration stores a
[`TableProvider`] in the catalog so both interfaces see the same
logical table.

Registration is lazy: the data itself isn't loaded into memory.
DataFusion caches schema and metadata so the source is ready for
high-performance scanning when an action triggers execution.

All registration and query methods are `async`. DataFusion uses Tokio
for both I/O and parallel execution — see
[Execution Lifecycle][execution-lifecycle]
for details.

:::{admonition} Why register?
:class: note

- **Performance**: Cache schema/metadata once; large, multi-file, and
  remote sources benefit from fewer round-trips and better pruning
- **Partition awareness**: Point to a directory and DataFusion
  auto-discovers Hive-style partitions (`/year=2022/month=01/`),
  enabling partition pruning
- **Interoperability**: The same logical name works in both DataFrame
  and SQL (`ctx.table("sales")` / `FROM sales`)
- **Discoverability**: Appears in `SHOW TABLES` and
  [`information_schema`]
- **Code clarity & portability**: Decouple query code from physical
  locations; swap sources by changing the catalog

:::

### When Not to Register

**Direct read methods (`.read_*()`) skip the catalog entirely — use
them when you don't need a named, reusable table.**

Registration is not always the right choice. Direct read methods
create an ephemeral [`TableProvider`] and return a [`DataFrame`]
immediately without touching the catalog. This is faster for one-off
work and avoids stale metadata when files change frequently.

| Scenario                 | Why Direct Reads Work Better                                                                             |
| ------------------------ | -------------------------------------------------------------------------------------------------------- |
| One-off exploration      | Registration overhead isn't worth a single query                                                         |
| Dynamic file paths       | Paths that change frequently make registered names stale                                                 |
| Rapidly evolving schemas | Registered tables cache the schema at registration time; if the file structure changes, queries may fail |
| Simple scripts           | [`.read_parquet()`]/[`.read_csv()`] are more concise for quick tasks                                     |
| Ephemeral data           | Temporary data won't be queried again                                                                    |

:::{admonition} Rule of thumb
:class: tip
If you'll query the same source more than once, or need SQL access,
register it. For single-use exploration, direct reads are simpler.
:::

---

## The Catalog at a Glance

**Registered tables live in a three-level catalog hierarchy — Catalog
→ Schema → Table — with defaults `datafusion` and `public`.**

Before registering, it helps to understand where registrations are
stored. The [`SessionContext`] organizes registered [`TableProvider`]s
in a Catalog → Schema → Table hierarchy. Most users never leave the
defaults, but the structure supports multi-catalog setups for
federation and multi-tenancy.

```text
SessionContext
└── CatalogProvider ("datafusion")   ← default catalog
    └── SchemaProvider ("public")    ← default schema
        └── Table ("sales")
            └── Arrow Schema         ← columns + types
```

Unqualified names like `"sales"` resolve to `datafusion.public.sales`.
Two-part names select a schema (`"analytics.sales"`), and fully-qualified
names address a specific catalog (`"warehouse.analytics.sales"`).

For the complete catalog model — including the trait hierarchy, schema
disambiguation, name resolution rules, case sensitivity, and extensibility
— see [Creating Concepts][creating-concepts].

:::{admonition} Cloud storage (Rust)
:class: note
To use `s3://`, `gs://`, or `az://` URLs, register an object store in
the runtime environment. See

- [`datafusion::datasource::object_store`]

- [S3 setup example][s3-example-main]

:::

### Inspecting the Catalog

**Query metadata about registered tables — use the programmatic
catalog API for application code or SQL `information_schema` for
ad-hoc exploration.**

After registering tables, you often need to verify what exists, check
schemas, or build dynamic queries based on available tables. DataFusion
provides two complementary inspection approaches — a programmatic Rust
API that navigates the catalog hierarchy through method calls, and a
SQL-standard `information_schema` for interactive exploration.

| Approach                                                             | API Style      | Best For                                    |
| -------------------------------------------------------------------- | -------------- | ------------------------------------------- |
| **Programmatic** ([`.catalog()`], [`.schema()`], [`.table_names()`]) | DataFrame/Rust | Application logic, dynamic queries, tooling |
| **SQL** (`information_schema.tables`)                                | SQL            | Ad-hoc exploration, debugging, portability  |

For a quick existence check before querying, use [`.table_exist()`].

```rust
use std::sync::Arc;
use datafusion::prelude::*;
use datafusion::assert_batches_eq;
use datafusion::arrow::array::{ArrayRef, Int32Array};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::{DataFusionError, Result};

#[tokio::main]
async fn main() -> Result<()> {
    let ctx = SessionContext::new_with_config(
        SessionConfig::new().with_information_schema(true)
    );

    // Register a table so we have something to inspect
    let sales = RecordBatch::try_from_iter(vec![
        ("order_id", Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef),
    ])?;
    ctx.register_batch("sales", sales)?;

    // Quick existence check
    assert!(ctx.table_exist("sales")?);

    // Programmatic: navigate the catalog hierarchy
    let catalog = ctx
        .catalog("datafusion")
        .ok_or_else(|| DataFusionError::Plan("missing catalog".into()))?;
    let schema = catalog
        .schema("public")
        .ok_or_else(|| DataFusionError::Plan("missing schema".into()))?;
    assert!(schema.table_names().contains(&"sales".to_string()));

    // SQL: information_schema queries
    let result = ctx
        .sql(
            "SELECT table_name, table_type \
             FROM information_schema.tables \
             WHERE table_schema = 'public' \
             ORDER BY table_name",
        )
        .await?
        .collect()
        .await?;

    assert_batches_eq!(
        &[
            "+------------+------------+",
            "| table_name | table_type |",
            "+------------+------------+",
            "| sales      | BASE TABLE |",
            "+------------+------------+",
        ],
        &result
    );

    Ok(())
}
```

:::{admonition} When to use which
:class: tip

- **Programmatic API**: Use when your code needs to react to available
  tables (e.g., building a schema browser, validating configurations,
  generating queries dynamically)
- **SQL `information_schema`**: Use for interactive exploration,
  debugging, or when you need SQL-standard portability

:::

:::{admonition} Catalog lifetime
:class: warning
Registered tables live in the [`SessionContext`]'s in-memory catalog.
When the context is dropped, all registrations are lost — there is no
persistent catalog by default. For long-running applications, keep the
context alive or re-register on startup. For persistent catalogs, see
[Catalogs](../../catalogs.md).
:::

---

## Registration Methods

**DataFusion provides format-specific convenience methods because
each data source needs different configuration — schema inference
strategy, file options, memory semantics — but they all store a
[`TableProvider`] in the catalog under a logical name.**

File-based methods like [`.register_parquet()`] and [`.register_csv()`]
wrap a [`ListingTable`] internally, handling path resolution, schema
inference, and partition discovery. In-memory methods like
[`.register_batch()`] wrap a [`MemTable`] with zero I/O. For anything
else — databases, REST APIs, custom formats — [`.register_table()`]
accepts any [`TableProvider`] implementation directly.

| Method                  | Purpose                               | Memory Impact        | Best For                              |
| ----------------------- | ------------------------------------- | -------------------- | ------------------------------------- |
| [`.register_parquet()`] | Register Parquet file(s) or directory | None (lazy scan)     | Production data, partitioned datasets |
| [`.register_csv()`]     | Register CSV file(s) or directory     | None (lazy scan)     | Data imports, simple formats          |
| [`.register_json()`]    | Register JSON file(s) or directory    | None (lazy scan)     | Semi-structured data                  |
| [`.register_avro()`]    | Register Avro file(s) or directory    | None (lazy scan)     | Schema-embedded formats               |
| [`.register_arrow()`]   | Register Arrow IPC file(s)            | None (lazy scan)     | Arrow-native interchange              |
| [`.register_batch()`]   | Register in-memory RecordBatch        | Holds data in memory | Test data, small lookups              |
| [`.register_table()`]   | Register custom TableProvider         | Depends on provider  | Custom sources, advanced use          |

:::{admonition} Lazy registration
:class: tip
Registration creates a catalog entry with the data source's schema.
The actual data is scanned lazily when queries execute, using
DataFusion's streaming execution engine.
:::

### Deregistration and Schema Refresh

**Remove a registered table with [`.deregister_table()`] — use this
to clean up or refresh a registration when the underlying schema
changes.**

Registration caches the Arrow schema at the moment you register. If
the source schema evolves — new columns appear, types change, files
are reorganized — queries against the stale registration may fail or
return incorrect results. The fix is a deregister-then-re-register
cycle. Use [`.table_exist()`] to guard against deregistering a name
that was never registered.

[`.deregister_table()`] returns the removed [`TableProvider`] wrapped
in an `Option`, so you can inspect or reuse the old provider if needed.

```rust
use std::sync::Arc;
use datafusion::prelude::*;
use datafusion::assert_batches_eq;
use datafusion::arrow::array::{ArrayRef, Int32Array, StringArray};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::Result;

#[tokio::main]
async fn main() -> Result<()> {
    let ctx = SessionContext::new();

    // Register initial table with one column
    let sales_v1 = RecordBatch::try_from_iter(vec![
        ("id", Arc::new(Int32Array::from(vec![1, 2])) as ArrayRef),
    ])?;
    ctx.register_batch("sales", sales_v1)?;
    assert!(ctx.table_exist("sales")?);

    // Schema changed upstream — deregister the stale registration
    ctx.deregister_table("sales")?;
    assert!(!ctx.table_exist("sales")?);

    // Re-register with updated schema (added "region" column)
    let sales_v2 = RecordBatch::try_from_iter(vec![
        ("id", Arc::new(Int32Array::from(vec![1, 2])) as ArrayRef),
        ("region", Arc::new(StringArray::from(vec!["US", "EU"])) as ArrayRef),
    ])?;
    ctx.register_batch("sales", sales_v2)?;

    // Verify the updated registration reflects the new schema
    let result = ctx.table("sales").await?.collect().await?;
    assert_batches_eq!(
        &[
            "+----+--------+",
            "| id | region |",
            "+----+--------+",
            "| 1  | US     |",
            "| 2  | EU     |",
            "+----+--------+",
        ],
        &result
    );

    Ok(())
}
```

### Registering DataFrames as Views

**Register a DataFrame pipeline as a named view using [`.into_view()`]
and [`.register_table()`] — making composed transformations available
to SQL and other DataFrames.**

Any [`DataFrame`] can be registered as a logical view. The underlying
[`LogicalPlan`] is stored (not the data), so subsequent queries compose
on top of the view lazily. This is DataFusion's equivalent of SQL's
`CREATE VIEW`: define a reusable query once, then reference it by name
from both the DataFrame API and SQL.

Use views to share intermediate pipeline stages across queries, expose
DataFrame transformations to SQL users, or break complex logic into
named, testable pieces.

```rust
use datafusion::prelude::*;
use datafusion::assert_batches_eq;
use datafusion::error::Result;

#[tokio::main]
async fn main() -> Result<()> {
    let ctx = SessionContext::new();

    // Build a lazy DataFrame pipeline
    let df = ctx.sql("SELECT 1 AS id, 'hello' AS greeting").await?;

    // Register as a named view — stores the plan, not data
    ctx.register_table("greetings", df.into_view())?;

    // Query by name via SQL
    let result = ctx
        .sql("SELECT * FROM greetings")
        .await?
        .collect()
        .await?;

    assert_batches_eq!(
        &[
            "+----+----------+",
            "| id | greeting |",
            "+----+----------+",
            "| 1  | hello    |",
            "+----+----------+",
        ],
        &result
    );

    Ok(())
}
```

### Custom TableProviders

**When built-in convenience methods don't cover your source, implement
[`TableProvider`] directly and register it with [`.register_table()`]
— the general-purpose registration method that all others build on.**

Every convenience method (i.e. [`.register_parquet()`], [`.register_csv()`], [`.register_batch()`], ...) constructs a [`TableProvider`] internally
and passes it to [`.register_table()`]. When you need to integrate a
source DataFusion doesn't support natively — databases, REST APIs,
streaming systems, custom file formats — you implement the
[`TableProvider`] trait yourself and register it the same way. The
trait requires an Arrow [`Schema`] via `schema()` and an
[`ExecutionPlan`] via [`scan()`][tableprovider_scan].

```rust
use std::sync::Arc;
use datafusion::prelude::*;
use datafusion::assert_batches_eq;
use datafusion::arrow::array::StringArray;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::error::Result;

#[tokio::main]
async fn main() -> Result<()> {
    let ctx = SessionContext::new();

    // register_table() accepts any Arc<dyn TableProvider>.
    // Here MemTable stands in for your custom provider.
    let schema = Arc::new(Schema::new(vec![Field::new(
        "status",
        DataType::Utf8,
        true,
    )]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(StringArray::from(vec!["active", "inactive"]))],
    )?;
    let provider = MemTable::try_new(schema, vec![vec![batch]])?;

    ctx.register_table("custom_source", Arc::new(provider))?;
    assert!(ctx.table_exist("custom_source")?);

    let result = ctx.table("custom_source").await?.collect().await?;
    assert_batches_eq!(
        &[
            "+----------+",
            "| status   |",
            "+----------+",
            "| active   |",
            "| inactive |",
            "+----------+",
        ],
        &result
    );

    Ok(())
}
```

:::{admonition} Getting started
:class: seealso
For a complete implementation guide with predicate pushdown and
projection handling, see the
[Custom Table Provider Guide](../../custom-table-providers.md).
:::

---

## Working with Registered Tables

**Once a data source is registered, the real power emerges — cached
metadata, dual-API access, and composable query patterns that scale
from quick scripts to production pipelines.**

Registration is the setup; the patterns below show the payoff.
Querying by name avoids repeated schema inference, and both the
DataFrame API and SQL can operate on the same logical table. Each
call to [`.table()`] or `FROM table_name` returns a fresh, lazy
[`DataFrame`] backed by the cached [`TableProvider`].

### Cached Queries: Register Once, Query Many

**Avoid repeated schema inference and file scanning by registering
sources that you'll query multiple times.**

When you call [`.read_csv()`] or [`.read_parquet()`] directly,
DataFusion must open the file, infer the schema, and build a new scan
plan from scratch — every time. For a single query, this is fine. But
if you run multiple queries against the same source, you pay this cost
on every call. Registration solves this by caching the schema and
[`TableProvider`] in the catalog — subsequent queries skip inference
entirely.

```rust
# use std::fs::File;
# use std::io::Write;
# use tempfile::tempdir;
use datafusion::prelude::*;
use datafusion::assert_batches_sorted_eq;
use datafusion::error::Result;

#[tokio::main]
async fn main() -> Result<()> {
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

    // ❌ WITHOUT registration — schema inferred on every call
    let path = "sales.csv";
    # let path = &csv_path_str;
    let _count = ctx.read_csv(path, CsvReadOptions::new()).await?.count().await?;
    let _preview = ctx.read_csv(path, CsvReadOptions::new()).await?
        .limit(0, Some(10))?.collect().await?;
    // Problem: file opened twice, schema inferred twice

    // ✅ WITH registration — schema cached, reused across queries
    ctx.register_csv("sales", path, CsvReadOptions::new()).await?;

    let filtered = ctx
        .table("sales")
        .await?
        .filter(col("amount").gt(lit(1000)))?
        .collect()
        .await?;

    assert_batches_sorted_eq!(
        &[
            "+----+-------+--------+",
            "| id | name  | amount |",
            "+----+-------+--------+",
            "| 1  | Alice | 1500   |",
            "| 3  | Carol | 2000   |",
            "+----+-------+--------+",
        ],
        &filtered
    );

    Ok(())
}
```

:::{admonition} Key insight
:class: tip
Each call to [`.table()`] returns a new [`DataFrame`], but all of
them reference the same registered source. The schema and metadata
are cached — no repeated inference, no repeated I/O.
:::

### Mixing SQL and DataFrame APIs

**Combine SQL's declarative power with the DataFrame API's
programmatic composability — registered tables are visible to both,
and the result of [`.sql()`] is itself a [`DataFrame`].**

This is the core payoff of registration. Both APIs compile down to
the same [`LogicalPlan`], so there is no performance penalty for
choosing one over the other. The key insight is the _transition
point_: [`ctx.sql()`][`.sql()`] returns a [`DataFrame`], so you can
write complex joins or CTEs in SQL and then continue with DataFrame
transformations — filters, projections, aggregations — in Rust. The
reverse is also possible: register a [`DataFrame`] as a view (see
[Registering DataFrames as Views](#registering-dataframes-as-views))
and query it from SQL.

| API           | Strengths                                                              | Use When                                                 |
| ------------- | ---------------------------------------------------------------------- | -------------------------------------------------------- |
| **SQL**       | Complex joins, CTEs, window functions; familiar to analysts            | Query logic is known upfront; porting existing queries   |
| **DataFrame** | Programmatic composition; compile-time type checking; IDE autocomplete | Building queries dynamically; integrating with Rust code |

```rust
# use std::sync::Arc;
# use datafusion::arrow::array::{ArrayRef, Int32Array, StringArray};
# use datafusion::arrow::record_batch::RecordBatch;
use datafusion::prelude::*;
use datafusion::assert_batches_sorted_eq;
use datafusion::error::Result;

#[tokio::main]
async fn main() -> Result<()> {
    let ctx = SessionContext::new();

    // Register "users" (id: Int32, name: Utf8)
    // and "orders" (order_id: Int32, user_id: Int32)
    # let users = RecordBatch::try_from_iter(vec![
    #     ("id", Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef),
    #     ("name", Arc::new(StringArray::from(vec!["Alice", "Bob", "Carol"])) as ArrayRef),
    # ])?;
    # ctx.register_batch("users", users)?;
    # let orders = RecordBatch::try_from_iter(vec![
    #     ("order_id", Arc::new(Int32Array::from(vec![100, 101, 102, 103])) as ArrayRef),
    #     ("user_id", Arc::new(Int32Array::from(vec![1, 1, 2, 3])) as ArrayRef),
    # ])?;
    # ctx.register_batch("orders", orders)?;

    // SQL for the join, then collect as DataFrame
    let result = ctx
        .sql(
            "SELECT u.name, COUNT(*) as order_count \
             FROM users u JOIN orders o ON u.id = o.user_id \
             GROUP BY u.name",
        )
        .await?
        .collect()
        .await?;

    assert_batches_sorted_eq!(
        &[
            "+-------+-------------+",
            "| name  | order_count |",
            "+-------+-------------+",
            "| Alice | 2           |",
            "| Bob   | 1           |",
            "| Carol | 1           |",
            "+-------+-------------+",
        ],
        &result
    );

    Ok(())
}
```

:::{admonition} No performance penalty
:class: attention
Both APIs compile down to the same [`LogicalPlan`] — choose based on
ergonomics, not speed.
:::

:::{admonition} Best practice
:class: caution
Prefer one API within a pipeline and switch at natural boundaries
(e.g., define a view in SQL, then continue with DataFrame transforms),
rather than ping-ponging between APIs step-by-step.
:::

For SQL-first workflows, round-trip transformations, and advanced
patterns, see [From SQL Queries][from-sql].

## Further Reading

**DataFusion:**

- [Catalogs Guide][catalogs] — Full catalog hierarchy and custom providers
- [`TableProvider` trait][`tableprovider`] — Interface for custom data sources
- [`CatalogProvider` trait][`catalogprovider`] — Custom catalog implementations
- [`information_schema`][information-schema] — SQL inspection of registered objects
- [Using Rust async for Query Execution][async-cancellation-blog] — Async patterns in DataFusion

**Other Systems (for comparison):**

- [Polars: Register DataFrames for SQL][polars-register] — Similar registration pattern
- [DuckDB: Registering Objects as Tables][duckdb-register]

---

<!-- References -->

<!-- Internal documentation -->

[catalogs]: ../../catalogs.md
[creating-concepts]: creating-concepts.md
[execution-lifecycle]: ../Concepts/execution-lifecycle.md
[from-sql]: from-sql.md
[information-schema]: ../../../user-guide/sql/information_schema.md

<!-- Core types -->

[`catalogprovider`]: https://docs.rs/datafusion/latest/datafusion/catalog/trait.CatalogProvider.html
[`dataframe`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html
[`datafusion::datasource::object_store`]: https://docs.rs/datafusion/latest/datafusion/datasource/object_store/index.html
[`executionplan`]: https://docs.rs/datafusion/latest/datafusion/physical_plan/trait.ExecutionPlan.html
[`information_schema`]: ../../../user-guide/sql/information_schema.md
[`listingtable`]: https://docs.rs/datafusion/latest/datafusion/datasource/listing/struct.ListingTable.html
[`logicalplan`]: https://docs.rs/datafusion-expr/latest/datafusion_expr/logical_plan/enum.LogicalPlan.html
[`memtable`]: https://docs.rs/datafusion/latest/datafusion/datasource/memory/struct.MemTable.html
[`schema`]: https://docs.rs/arrow/latest/arrow/datatypes/struct.Schema.html
[`sessioncontext`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html
[`tableprovider`]: https://docs.rs/datafusion/latest/datafusion/catalog/trait.TableProvider.html

<!-- Methods and functions -->

[`.catalog()`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.catalog
[`.deregister_table()`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.deregister_table
[`.into_view()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.into_view
[`.read_csv()`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.read_csv
[`.read_parquet()`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.read_parquet
[`.register_arrow()`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.register_arrow
[`.register_avro()`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.register_avro
[`.register_batch()`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.register_batch
[`.register_csv()`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.register_csv
[`.register_json()`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.register_json
[`.register_parquet()`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.register_parquet
[`.register_table()`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.register_table
[`.schema()`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.schema
[`.sql()`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.sql
[`.table()`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.table
[`.table_exist()`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.table_exist
[`.table_names()`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.table_names
[tableprovider_scan]: https://docs.rs/datafusion/latest/datafusion/catalog/trait.TableProvider.html#tymethod.scan

<!-- External resources -->

[async-cancellation-blog]: https://datafusion.apache.org/blog/2025/06/30/cancellation/
[duckdb-register]: https://duckdb.org/docs/api/python/overview#registering-python-objects-as-tables
[polars-register]: https://docs.pola.rs/user-guide/sql/intro/#register-dataframes
[s3-example-main]: https://github.com/apache/datafusion/blob/main/datafusion-examples/examples/external_dependency/main.rs
