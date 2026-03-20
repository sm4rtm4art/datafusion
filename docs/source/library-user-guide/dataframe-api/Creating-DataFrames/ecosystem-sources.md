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

# Creating DataFrames from Ecosystem Sources

**Extend DataFusion beyond built-in formats — connect lakehouse tables,
databases, and distributed engines through community `TableProvider`
crates.**

DataFusion natively supports Parquet, CSV, JSON, Avro, and Arrow IPC.
For everything else — Delta Lake, Iceberg, PostgreSQL, DuckDB, and
more — the community provides external crates that implement the
[`TableProvider`] trait. Once registered, these sources are
indistinguishable from built-in ones: the same `.filter()`, `.select()`,
`.collect()` DataFrame operations and SQL queries work unchanged. This
page maps what is available and shows the universal integration pattern.

**Key concepts:**

| Concept                  | Role                                                          |
| ------------------------ | ------------------------------------------------------------- |
| [`TableProvider`]        | Trait that bridges any external source to DataFusion          |
| [`.register_table()`]    | Registers a `TableProvider` in the session catalog            |
| [`TableProviderFactory`] | Enables `CREATE EXTERNAL TABLE` SQL syntax for custom formats |

:::{admonition} Style Note
:class: note
:collapsible: closed

In this document, all code elements are highlighted with backticks.

- DataFrame methods are written as `.method()` (e.g., `.select()`) to reflect the chaining syntax central to the API.
- standalone functions `method()` (e.g `col()`)

- - static constructors `Struckt::method()` (e.g., `SessionContext::new()`).
- Rust types are formatted as `TypeName` (e.g., `SchemaRef`).

:::

```{contents} Table of Contents
:local:
:depth: 2
```

## The Universal Pattern

**Every ecosystem source follows the same three-step path: obtain a
`TableProvider` from an external crate, register it, and query — your
downstream DataFrame code stays the same.**

Regardless of whether the source is a Delta Lake table, a PostgreSQL
database, or a custom file format, the integration path is identical.
The external crate handles format-specific logic (parsing, metadata,
pushdown); DataFusion handles optimization and execution. This
separation means you learn the pattern once and apply it everywhere.

```text
┌─────────────────────┐
│  Ecosystem Crate    │  delta-rs, iceberg-rust, lance, ...
└─────────┬───────────┘
          ▼
┌─────────────────────┐
│  TableProvider      │  Crate implements this trait
│  (Arc<dyn ...>)     │
└─────────┬───────────┘
          ▼
┌─────────────────────┐
│  .register_table()  │  Stores provider in the catalog
└─────────┬───────────┘
          ▼
┌─────────────────────┐
│  DataFrame / SQL    │  .table("events") or FROM events
└─────────────────────┘
```

```rust,no_run
// no_run: ecosystem crates are external dependencies
use std::sync::Arc;
use datafusion::catalog::TableProvider;
use datafusion::prelude::*;
use datafusion::error::Result;

#[tokio::main]
async fn main() -> Result<()> {
    let ctx = SessionContext::new();

    // 1. Obtain a TableProvider from the ecosystem crate
    //    (e.g., delta_rs::open_table("s3://bucket/events").await?)
    let provider: Arc<dyn TableProvider> = todo!();

    // 2. Register under a logical name
    ctx.register_table("events", provider)?;

    // 3. Query via DataFrame API — unchanged from built-in sources
    ctx.table("events")
        .await?
        .filter(col("year").eq(lit(2025)))?
        .select(vec![col("event_id"), col("payload")])?
        .show()
        .await?;

    Ok(())
}
```

:::{admonition} SQL pathway: `TableProviderFactory`
:class: tip

Some ecosystem crates also implement [`TableProviderFactory`], enabling
registration via SQL DDL:

```sql
CREATE EXTERNAL TABLE events
STORED AS DELTA
LOCATION 's3://bucket/events/';
```

Check each crate's documentation for DDL support.
:::

For a complete guide on building your own `TableProvider`, see the
[Custom Table Provider Guide]. For the registration pattern in detail,
see [Custom TableProviders](registered-tables.md#custom-tableproviders).

## Available Sources

**DataFusion's ecosystem spans lakehouse formats, database connectors,
and distributed execution — all integrated through `TableProvider`.**

### Lakehouse Formats

Open table formats add transactional guarantees — ACID commits, time
travel, schema evolution — on top of object storage. Each crate
implements [`TableProvider`] for both reads and writes, so the same
registration pattern produces a fully queryable [`DataFrame`].

| Format         | Crate                                                  | Highlights                                   | Status           |
| -------------- | ------------------------------------------------------ | -------------------------------------------- | ---------------- |
| Delta Lake     | [delta-rs](https://github.com/delta-io/delta-rs)       | ACID transactions, time travel, Z-ordering   | Production-ready |
| Apache Iceberg | [iceberg-rust](https://github.com/apache/iceberg-rust) | Hidden partitioning, partition evolution     | Maturing         |
| Lance          | [lance](https://github.com/lancedb/lance)              | ML-optimized columnar with vector search     | Production-ready |
| Apache Hudi    | [hudi-rs](https://github.com/apache/hudi-rs)           | Incremental processing, record-level upserts | Incubating       |

For write-side integration (`.write_table()` with lakehouse formats),
see [When to Consider Lakehouse Table Formats].

### Database and Flight Connectors

The [DataFusion Table Providers] crate bridges relational databases and
Arrow Flight sources into DataFusion. Each connector supports predicate
pushdown where the source database can handle it, minimizing data
transfer.

| Source     | Protocol     | Notes                   |
| ---------- | ------------ | ----------------------- |
| PostgreSQL | Native       | Predicate pushdown      |
| MySQL      | Native       | Predicate pushdown      |
| SQLite     | Embedded     | Local analytical access |
| DuckDB     | Embedded     | In-process analytics    |
| Flight SQL | Arrow Flight | Zero-copy transfer      |

### Distributed Execution

For workloads that exceed a single node, DataFusion serves as the
execution kernel inside distributed frameworks:

| Project             | What it does                                   | Link                                                    |
| ------------------- | ---------------------------------------------- | ------------------------------------------------------- |
| DataFusion Ballista | Distributed query execution using DataFusion   | [GitHub](https://github.com/apache/datafusion-ballista) |
| DataFusion Comet    | Spark and Iceberg accelerator using DataFusion | [Docs](https://datafusion.apache.org/comet/)            |

### More Community Extensions

The [`datafusion-contrib`] organization hosts additional extensions.
See the full [Extensions List] for an up-to-date catalog.

| Extension                   | Type            | Description                                |
| --------------------------- | --------------- | ------------------------------------------ |
| [DataFusion Federation]     | Framework       | Execute (parts of) plans on remote engines |
| [DataFusion ORC]            | `TableProvider` | Apache ORC file format                     |
| [DataFusion JSON Functions] | Functions       | Scalar functions for querying JSON strings |

:::{admonition} Cloud Object Stores
:class: seealso

Cloud storage (S3, GCS, Azure) is a transport layer, not a data source
format. DataFusion reads cloud-hosted files through object store URLs
after registering an `ObjectStore` implementation. See
[Cloud Storage](from-files/index.md#cloud-storage) for setup details.
:::

:::{admonition} Community crates evolve independently
:class: warning

The projects listed here are maintained outside Apache DataFusion core
and follow their own release cycles. **Always check each crate's
documentation** for the latest API compatibility, supported DataFusion
versions, and feature status.
:::

## References

**Guides:**

- [Custom Table Provider Guide] — build your own `TableProvider`
- [Registered Tables](registered-tables.md) — registration patterns and catalog integration
- [Extensions List] — full community extensions catalog
- [When to Consider Lakehouse Table Formats] — write-side lakehouse integration

**API:**

- [`TableProvider`] — the trait that bridges external sources
- [`.register_table()`] — register any provider in the catalog
- [`TableProviderFactory`] — enable DDL-based registration

<!-- Link references -->

[Custom Table Provider Guide]: ../../custom-table-providers.md
[Extensions List]: ../../extensions.md
[When to Consider Lakehouse Table Formats]: ../Writing-DataFrames/writing-dataframes.md#when-to-consider-lakehouse-table-formats
[DataFusion Table Providers]: https://github.com/datafusion-contrib/datafusion-table-providers
[DataFusion Federation]: https://github.com/datafusion-contrib/datafusion-federation
[DataFusion ORC]: https://github.com/datafusion-contrib/datafusion-orc
[DataFusion JSON Functions]: https://github.com/datafusion-contrib/datafusion-functions-json
[`datafusion-contrib`]: https://github.com/datafusion-contrib
[`TableProvider`]: https://docs.rs/datafusion/latest/datafusion/catalog/trait.TableProvider.html
[`.register_table()`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.register_table
[`TableProviderFactory`]: https://docs.rs/datafusion/latest/datafusion/catalog/trait.TableProviderFactory.html
[`DataFrame`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html
