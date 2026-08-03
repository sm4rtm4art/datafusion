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
:class: note
:collapsible: closed

In this document, all code elements are `highlighted`.

- DataFrame methods are written as `.method()` (e.g., `.select()`) to reflect the chaining syntax central to the API.
- standalone functions `method()` (e.g., `col()`)
- static constructors `Struct::method()` (e.g., `SessionContext::new()`).
- Rust types are formatted as `TypeName` (e.g., `SchemaRef`).

**About the examples:**
Examples use [`assert_batches_eq!`] to verify outputs—you see both the code and its result. This pattern ensures examples stay correct as the API evolves.
:::

```{toctree}
:maxdepth: 1
:caption: Creating DataFrames Overview
:numbered:

creating-concepts
from-files/index.md
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

A gerneral [Concepts](creating-concepts.md) part is given additionaly the following topics are based on where your data lives.

**Choose based on where your data lives and how you'll access it:**

| Category               | Method                                    | Best for                                          | Prefer alternatives when                     |
| ---------------------- | ----------------------------------------- | ------------------------------------------------- | -------------------------------------------- |
| **Direct Read**        | [From Files](from-files/index.md)         | Ad-hoc analysis, ETL pipelines, one-off scripts   | You need stable names or multi-query reuse   |
| **DataFusion Catalog** | [Registered Tables](registered-tables.md) | SQL interoperability, shared schemas, multi-query | Simple one-shot queries                      |
| **SQL**                | [SQL Queries](from-sql.md)                | Complex joins, CTEs, window functions             | Dynamic logic, programmatic column selection |
| **Native**             | [RecordBatches](from-memory.md)           | Arrow ecosystem, single or multi-batch processing | Large file-based datasets                    |
| **Testing**            | [Inline Data](inline-data.md)             | Unit tests, small hand-authored examples          | Production ingestion or large datasets       |
| **Streaming**          | [Streaming Sources](streaming.md)         | Unbounded data, real-time pipelines               | Bounded/batch workloads                      |
| **Advanced**           | [LogicalPlan](from-logical-plan.md)       | Custom DSLs, federation, optimizer testing        | Higher-level methods suffice                 |
| **Ecosystem**          | [External Sources](ecosystem-sources.md)  | Lakehouse formats, databases, distributed engines | Core formats (Parquet, CSV, etc.) suffice    |

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

Data sources connect to DataFusion through [`TableProvider`] implementations
— [`ListingTable`] for files, [`MemTable`] for in-memory batches, and
custom providers for everything else. Each provider enters the session via
a direct read (ephemeral) or catalog registration (named), and the result
is always a lazy [`DataFrame`] paired with a [`SessionState`] snapshot.

For the full architecture — including the creation pathway diagram, table
provider details, and how the plan reaches execution — see
[How DataFrame Creation Works](creating-concepts.md).

## Getting Started

**Every creation method starts with a [`SessionContext`]—the entry point
that owns the catalog, configuration, and runtime for your session.**

The [`SessionContext`] exposes all DataFrame creation methods. These fall
into three categories:

| Category         | Methods                                                                                                                           | Effect                                    |
| ---------------- | --------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------- |
| **Direct read**  | [`.read_parquet()`], [`.read_csv()`], [`.read_json()`], [`.read_avro()`], [`.read_arrow()`], [`.read_batch()`], [`.read_table()`] | Returns a `DataFrame` immediately         |
| **Registration** | [`.register_parquet()`], [`.register_csv()`], [`.register_json()`], [`.register_table()`], [`.register_batch()`]                  | Stores a `TableProvider` in the catalog   |
| **Query**        | [`.sql()`], [`.table()`]                                                                                                          | Returns a `DataFrame` from catalog or SQL |

The following example shows the minimal pattern—create a context, create a
DataFrame, register it for reuse, and query by name:

```rust
use datafusion::prelude::*;
use datafusion::assert_batches_eq;
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
    let batches = ctx.table("greetings").await?.collect().await?;
    assert_batches_eq!(
        &[
            "+----+----------+",
            "| id | greeting |",
            "+----+----------+",
            "| 1  | hello    |",
            "+----+----------+",
        ],
        &batches
    );

    Ok(())
}
```

This pattern—create context, create or register data, query—is the skeleton
for every example in this guide. For SessionContext configuration (batch
size, parallelism, object stores), see
[SessionContext](../Concepts/sessioncontext.md). For how table names resolve
in the catalog, see
[The Catalog at a Glance](registered-tables.md#the-catalog-at-a-glance).

---

<!-- Link references -->

[`sessioncontext`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html
[`dataframe`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html
[`logicalplan`]: https://docs.rs/datafusion/latest/datafusion/logical_expr/enum.LogicalPlan.html
[`tableprovider`]: https://docs.rs/datafusion/latest/datafusion/catalog/trait.TableProvider.html
[`listingtable`]: https://docs.rs/datafusion/latest/datafusion/datasource/listing/struct.ListingTable.html
[`memtable`]: https://docs.rs/datafusion/latest/datafusion/datasource/struct.MemTable.html
[`sessionstate`]: https://docs.rs/datafusion/latest/datafusion/execution/session_state/struct.SessionState.html
[`assert_batches_eq!`]: https://docs.rs/datafusion/latest/datafusion/macro.assert_batches_eq.html
[`.read_parquet()`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.read_parquet
[`.read_csv()`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.read_csv
[`.read_json()`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.read_json
[`.read_avro()`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.read_avro
[`.read_arrow()`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.read_arrow
[`.read_batch()`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.read_batch
[`.read_table()`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.read_table
[`.register_parquet()`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.register_parquet
[`.register_csv()`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.register_csv
[`.register_json()`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.register_json
[`.register_table()`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.register_table
[`.register_batch()`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.register_batch
[`.sql()`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.sql
[`.table()`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.table
