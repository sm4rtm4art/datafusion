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

# DataFrame Creation Architecture

**The [`DataFrame`] is the central structure for data manipulation in
DataFusion — a lazy handle that standardizes any data source into
Arrow's columnar ecosystem for reliable, parallel execution.**

Before a [`DataFrame`] can exist, raw data must be standardized into
Arrow's columnar format. A four-layer pipeline — data source,
[`TableProvider`], access pattern, and [`SessionContext`] — turns files,
databases, streams, and in-memory batches into a unified [`LogicalPlan`]
ready for optimization and execution. Understanding this creation path
helps you choose the right provider, optimize at the source, and
integrate diverse systems into a single query.

**Concepts covered on this page:**

| Concept                                                                                    | What it covers                                                                |
| ------------------------------------------------------------------------------------------ | ----------------------------------------------------------------------------- |
| [From DataSource to DataFrame](#from-datasource-to-dataframe)                              | The standardization pipeline from raw data to a lazy DataFrame                |
| [Table Providers](#table-providers)                                                        | Built-in and custom `TableProvider` implementations bridging sources to Arrow |
| [Access Patterns](#access-patterns)                                                        | Ephemeral direct reads vs. named registration and when to use each            |
| [The SessionContext: Hub for Data Processing](#the-sessioncontext-hub-for-data-processing) | Catalog, configuration, runtime, and the `SessionState` clone                 |
| [Creation of a DataFrame](#creation-of-a-dataframe)                                        | How `SessionState` + `LogicalPlan` converge into a lazy `DataFrame`           |

## From DataSource to DataFrame

**A four-layer pipeline standardizes data from any source into Arrow's
columnar ecosystem, producing a lazy [`DataFrame`] as the single point
of data manipulation.**

Data lives in countless formats — plain CSV, columnar Parquet, streaming
Avro, OLTP databases like PostgreSQL. Even Parquet, though designed for
efficient Arrow conversion, stores data in its own compressed encoding
on disk. For reliable, parallel execution every source must be
standardized into Arrow's in-memory columnar format.

DataFusion standardizes these sources through a four-layer pipeline
designed to connect any format to the Arrow ecosystem. A
[`TableProvider`] adapts the source, two access patterns handle the
difference between ephemeral file reads and named catalog tables, and
the [`SessionContext`] orchestrates configuration, runtime, and catalog
into a single hub.

At the end of the pipeline, the [`SessionContext`] clones its
[`SessionState`] and pairs it with a [`LogicalPlan`] — producing the
[`DataFrame`]: a lazy, composable handle where every transformation
returns a new plan and Rust's ownership model guarantees thread safety.

The following diagram shows the dataflow through each layer:

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
[ 2. TABLE PROVIDERS ]                        (impl TableProvider trait)
┌───────────────────┐ ┌───────────────────┐ ┌─────────────────────────────┐
│   ListingTable    │ │     MemTable      │ │   Custom TableProvider      │
│    (built-in)     │ │    (built-in)     │ │ (user or ecosystem impl)    │
│                   │ │                   │ │                             │
│ Path resolution,  │ │  Zero-copy Arrow  │ │ Any source that implements  │
│ schema inference, │ │   batch access    │ │  the TableProvider trait    │
│ partition disc.   │ │                   │ │                             │
└─────────┬─────────┘ └─────────┬─────────┘ └──────────────┬──────────────┘
          │                     │                          │
          └─────────────────────┼──────────────────────────┘
                                │
[ 3. ACCESS PATTERN ]           │   (How you introduce it to the Session)
                ┌───────────────▼───────────────────┐
                │                                   │
       ┌────────▼─────────┐                ┌────────▼─────────┐
       │  A. DIRECT READ  │                │   B. REGISTER    │
       │   (Ephemeral)    │                │     (Named)      │
       │                  │                │                  │
       │   read_parquet() │                │ register_parquet │
       │   read_csv()     │                │ register_csv     │
       │   read_batch()   │                │ register_table   │
       │   read_json()    │                │                  │
       └────────┬─────────┘                └────────┬─────────┘
                │                                   │
                │ (Returns DataFrame)               │ (Stored in Catalog)
                │                                   ▼
[ 4. THE HUB ]  │                              [ CATALOG ]
┌───────────────▼───────────────────────────────────▼──────────────────┐
│                           SESSIONCONTEXT                             │
│ ┌──────────────────────────────────────────────────────────────────┐ │
│ │                          SESSIONSTATE                            │ │
│ │       Config · RuntimeEnv · Optimizer · Planner · Catalog        │ │
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
             └──────────────┬───────────────────┘
                            │
               DataFrame::new(state, plan)
                            │
                            ▼
   ┌────────────────────────┬───────────────────────────────────┐
   │                    DATAFRAME (Lazy, Composable)            │
   ├────────────────────────┼───────────────────────────────────┤
   │  LogicalPlan           │  SessionState clone               │
   │  · DFSchema            │  · Config, functions → own copy   │
   │  · Relational ops      │  · Catalog, runtime  → shared Arc │
   │    (frozen at creation)│                                   │
   └────────────────────────┴──────────────┬────────────────────┘
                                           │
                                           ▼
                                   [ EXECUTION PATH ]
                       (via SessionState: Optimize ➔ Plan ➔ Execute)
```

| Layer              | Input                                   | Output                               | Mechanism                         |
| ------------------ | --------------------------------------- | ------------------------------------ | --------------------------------- |
| **Data Source**    | Raw bytes (files, streams, memory, DBs) | Source-specific data                 | —                                 |
| **TableProvider**  | Source-specific data                    | Arrow [`Schema`] + scan plan         | `impl TableProvider`              |
| **Access Pattern** | TableProvider                           | Ephemeral plan or catalog entry      | [`.read_*()`] / [`.register_*()`] |
| **SessionContext** | Plans + catalog + config + runtime      | SessionState clone + [`LogicalPlan`] | State clone                       |
| **DataFrame**      | SessionState clone + [`LogicalPlan`]    | Lazy, composable query handle        | `DataFrame::new(state, plan)`     |

For hands-on guides to each creation method (Parquet, CSV, SQL,
RecordBatches, and more), see the [Creating DataFrames](index.md)
overview.

## Table Providers

**The [`TableProvider`] trait defines the contract between a data source
and DataFusion — each source needs its own implementation, but three
built-in providers cover most cases.**

A [`TableProvider`] exposes an Arrow [`Schema`] via `schema()` and
produces an [`ExecutionPlan`] via `scan()`. You configure a provider once
and query it many times — each execution calls `scan()` to produce a
fresh plan while the provider itself holds cached metadata (schema,
partition layout). DataFusion ships three built-in providers:
[`ListingTable`], [`MemTable`], and [`StreamingTable`]. Anything beyond
these requires a custom implementation.

### ListingTable (Files)

[`ListingTable`] is the default [`TableProvider`] for reading files.
When you call [`.read_parquet()`], [`.register_csv()`], or any other
file-oriented convenience method, DataFusion creates a [`ListingTable`]
under the hood. It bridges the gap between file-level concerns — path
resolution (local and remote storage), schema inference, and Hive-style
partition discovery (`/year=2024/month=03/`) — and the query engine's
table abstraction.

The built-in formats and their schema inference costs:

| Format    | Description                              | Schema inference at creation                    |
| --------- | ---------------------------------------- | ----------------------------------------------- |
| Parquet   | Columnar, compressed, designed for Arrow | Reads file footer only — lightweight            |
| CSV       | Plain text, no embedded metadata         | Samples up to 1,000 rows for type inference     |
| JSON      | Semi-structured, no embedded metadata    | Samples rows (similar to CSV)                   |
| Avro      | Schema embedded in file header           | Reads header — lightweight                      |
| Arrow IPC | Native Arrow serialization               | Full Arrow schema in file — no inference needed |
| Any       | With explicit schema provided            | **Zero I/O** — no file access at all            |

Other file formats can be supported by implementing the [`TableProvider`]
trait directly with a custom file reader. For when and how statistics
(row counts, min/max per column) are collected, see
[Execution Lifecycle](../Concepts/execution-lifecycle.md).

### MemTable (In-Memory)

[`MemTable`] is the adapter for data already inside the Arrow ecosystem.
It wraps Arrow [`RecordBatch`]es with no I/O, no serialization, and no
schema inference — the Arrow schema is known at construction. This makes
[`MemTable`] the zero-conversion path for data arriving through Arrow
Flight, IPC deserialization, or computed intermediate results.

### StreamingTable (Unbounded)

[`StreamingTable`] handles unbounded, continuously arriving data through
[`PartitionStream`] sources. It signals to the planner that input is
unbounded, causing it to select streaming-compatible operators (symmetric
hash joins, windowed aggregations) instead of operators that require
seeing all data.

For architecture and usage patterns, see
[Streaming Sources](streaming.md).

### Custom TableProvider

Any source that does not fit the built-in providers can implement the
[`TableProvider`] trait directly. This covers OLTP databases (PostgreSQL,
MySQL), lakehouse formats (Delta Lake, Iceberg, Lance), and REST APIs.
DataFusion includes no built-in database connectors — custom providers
use native Rust client libraries (e.g., `tokio-postgres`, `sqlx`) to
talk to their source and return Arrow [`RecordBatch`]es via `scan()`.

Custom providers can implement optional trait methods to enable
predicate pushdown and projection pushdown, letting the source filter
data before it reaches DataFusion. Internally, DataFusion also uses
[`TableProvider`] for SQL views (`ViewTable`) and recursive CTEs.

:::{admonition} Ecosystem providers
:class: seealso

Community-maintained providers extend DataFusion to Delta Lake
(`delta-rs`), Apache Iceberg (`iceberg-rust`), Lance, and more. See
[Ecosystem Data Sources](ecosystem-sources.md) for an overview. For
implementing your own, see the
[Custom Table Providers](../../custom-table-providers.md) guide.
:::

### Provider-Dependent Capabilities

**The provider you choose at creation time determines what the optimizer
can do at execution time — not all sources are created equal.**

A [`TableProvider`] can optionally expose capabilities that let the
optimizer reduce I/O before data ever reaches the query engine. The more
a provider exposes, the less work DataFusion has to do:

| Capability          | Effect                                                  | Example                            |
| ------------------- | ------------------------------------------------------- | ---------------------------------- |
| Predicate pushdown  | Source filters rows before DataFusion sees them         | Parquet row-group pruning          |
| Projection pushdown | Source reads only requested columns                     | Parquet column selection           |
| Partition pruning   | Planner skips entire file partitions                    | Hive-style `/year=2024/` filtering |
| Statistics          | Optimizer estimates cardinality for join/agg strategies | Parquet footer statistics          |

Providers that expose more capabilities give the optimizer more room to
reduce I/O and memory usage — a key reason to prefer Parquet and explicit
schemas in production workloads.

How these capabilities compare across the built-in providers:

| Feature                 | Parquet             | CSV                      | MemTable                |
| ----------------------- | ------------------- | ------------------------ | ----------------------- |
| **Predicate pushdown**  | Row-group pruning   | Full scan required       | N/A (already in memory) |
| **Projection pushdown** | Column skipping     | Parses column subset     | Pointer offsets         |
| **Statistics**          | Exact (from footer) | Inferred / estimated     | Exact                   |
| **Schema source**       | File metadata       | Inference (row sampling) | Explicit (zero I/O)     |

## Access Patterns

**Two patterns determine how a [`TableProvider`] enters the
[`SessionContext`] — the choice shapes catalog visibility, metadata
caching, and SQL interoperability.**

DataFusion serves two query interfaces: the programmatic DataFrame API
and SQL. These interfaces have fundamentally different needs — SQL
requires named tables (`FROM sales`), while the DataFrame API can work
directly with file paths. Access patterns exist to bridge this gap: a
**direct read** feeds the DataFrame API without touching the catalog,
while **registration** stores the provider by name so both interfaces
can reach it. Both patterns produce a [`DataFrame`] in the end, but the
path through the [`SessionContext`] differs.

### Direct Read (Ephemeral)

Methods like [`.read_parquet()`] or [`.read_csv()`] create a
[`TableProvider`] internally, build an ephemeral [`LogicalPlan`], and
return a [`DataFrame`] immediately. The provider is not stored in the
catalog — no other query can reference it, `SHOW TABLES` will not list
it, and metadata (schema, partition layout) is re-derived on every call.
DataFusion's file-based SQL syntax (`SELECT * FROM 'data.parquet'`) is
functionally equivalent.

**Best for:** one-off exploration, ad-hoc scripts, or rapidly changing
file paths.

:::{admonition} Creation reads metadata, not data
:class: warning

[`.read_parquet()`] does not read your data. It creates a
[`ListingTable`], infers the schema from file metadata (Parquet footer,
CSV sample rows), and returns a lazy [`DataFrame`]. No rows are scanned.
Actual data reading only happens when an action like [`.collect()`] or
[`.show()`] triggers execution. The same applies to all `read_*` and
`register_*` methods.
:::

### Registration (Named)

Methods like [`.register_parquet()`] or [`.register_table()`] store the
[`TableProvider`] in the session catalog under a logical name (e.g.,
`"sales"`), caching metadata inside the [`SessionContext`] for reuse by
both the DataFrame and SQL API. Subsequent queries reference the name,
not the physical location.

**Best for:** data queried more than once, SQL interoperability, or
shared pipelines where a stable name decouples query logic from physical
paths.

**Trade-off:** the Arrow schema is fixed at registration time. File
discovery happens lazily at execution (new files are picked up), but if
the upstream schema changes (new columns, type changes), you must
re-register to pick up the new schema. For schema evolution strategies,
see [Schema Management](../Schema-Management/index.md).

### Choosing Between Ephemeral and Named Path

:::{admonition} Rule of thumb
:class: tip

Parquet, remote storage, or multi-file datasets → **register**. Small,
local, or one-off analysis → **direct read**.
:::

| Concern              | Direct Read                          | Registration                      |
| -------------------- | ------------------------------------ | --------------------------------- |
| Catalog visibility   | Not visible                          | Visible via SQL and `SHOW TABLES` |
| Metadata caching     | Re-derived per call                  | Cached at registration time       |
| SQL interoperability | File paths only (`FROM 'f.parquet'`) | Full (`FROM sales`)               |
| Setup cost           | None                                 | One registration call             |
| Best for             | Exploration, one-off ETL             | Production, multi-query, shared   |

## The SessionContext: Hub for Data Processing

**[`SessionContext`] is the single source of truth for everything a
query needs — catalog, configuration, runtime resources, and UDFs —
orchestrating the pipeline from data source to [`DataFrame`].**

The [`SessionContext`] plays three roles in the creation pipeline.
First, it is the **API surface**: every creation method —
[`.read_parquet()`], [`.table()`], [`.sql()`] — is a method on
[`SessionContext`]. Second, it **stores and finds data sources**: the
catalog is where registration puts [`TableProvider`]s and where query
creation retrieves them by name. Third, it **assembles the DataFrame**:
it clones its [`SessionState`], pairs it with a [`LogicalPlan`], and
returns the result. For the full API surface and configuration guide,
see [SessionContext](../Concepts/sessioncontext.md).

### From Method Call to DataFrame

Every creation method follows the same internal sequence, regardless of
whether you call [`.read_parquet()`], [`.table()`], or [`.sql()`]:

1. **Resolve the source** — create or look up a [`TableProvider`]
   (build a [`ListingTable`] from a file path, or find a registered
   provider in the catalog by name).
2. **Build the plan** — wrap the provider in a scan node to produce a
   [`LogicalPlan`].
3. **Clone the state** — call `self.state()` to get a structural clone
   of the current [`SessionState`].
4. **Return the DataFrame** — `DataFrame::new(state, plan)`.

The difference between methods is only step 1: direct reads build a
provider on the fly, `ctx.table()` looks one up in the catalog, and
`ctx.sql()` parses SQL into a plan that may reference multiple catalog
entries.

### What It Owns

Everything a query needs to go from plan to results lives inside the
[`SessionContext`]. These four categories define the execution
environment that every [`DataFrame`] ultimately draws from:

| Component     | What it holds                                                                  |
| ------------- | ------------------------------------------------------------------------------ |
| **Catalog**   | Registered [`TableProvider`]s in a three-level hierarchy                       |
| **Config**    | Batch size, target partitions, optimizer rules, timezone                       |
| **Runtime**   | [`MemoryPool`], [`DiskManager`], [`ObjectStoreRegistry`] (`s3://`, `gs://`, …) |
| **Functions** | UDFs, UDAFs, UDWFs                                                             |

### The Catalog: How Registration Connects to Creation

**Step 1 of the creation sequence — "resolve the source" — depends
entirely on the catalog. Registration puts a [`TableProvider`] in;
`ctx.table()` and SQL get it back out by name.**

When you call [`.register_parquet()`] or [`.register_table()`], the
[`SessionContext`] stores the provider in a three-level hierarchy
inside its catalog: Catalog → Schema → Table. When you later call
`ctx.table("sales")` or write `FROM sales` in SQL, the
[`SessionContext`] walks this hierarchy to find the provider — and
from there, step 2 (build the plan) proceeds as usual.

```text
SessionContext
└── CatalogProvider ("datafusion")   ← default catalog
    └── SchemaProvider ("public")    ← default schema
        └── Table ("sales")         ← your registered TableProvider
            └── Arrow Schema        ← columns + types
```

DataFusion provides a default catalog (`datafusion`) and schema
(`public`). Each level is backed by a trait — [`CatalogProvider`],
[`SchemaProvider`], and [`TableProvider`] — so you can replace any
level with a custom implementation (e.g., AWS Glue, Databricks Unity
Catalog).

#### How Names Resolve

Name resolution determines what `ctx.table("sales")` and `FROM sales`
actually find — get it wrong and queries fail with "table not found" or
silently hit the wrong source. DataFusion resolves table names using
**1-, 2-, or 3-part identifiers**:

| Identifier                    | Resolves to                  | Use case              |
| ----------------------------- | ---------------------------- | --------------------- |
| `"sales"`                     | `datafusion.public.sales`    | Default (most common) |
| `"analytics.sales"`           | `datafusion.analytics.sales` | Custom schema         |
| `"warehouse.analytics.sales"` | Fully qualified              | Multi-catalog setups  |

- **Default namespace:** Unqualified names resolve to `datafusion.public`
  (default catalog + schema). This is a naming convention, not an
  access-control boundary.
- **Lifetime:** Registrations are in-memory, scoped to the
  [`SessionContext`]. For persistence, implement a custom
  [`CatalogProvider`].
- **Case sensitivity:** Unquoted identifiers fold to lowercase; quote to
  preserve case (`"Sales"`).

:::{admonition} "Schema" means different things at different layers
:class: note

In the creation path, "schema" carries three meanings: a catalog
namespace ([`SchemaProvider`]), Arrow column definitions ([`Schema`]),
and query-qualified columns ([`DFSchema`]). For the full taxonomy —
including `SchemaRef` and ownership flow — see
[Schema Management](../Schema-Management/index.md).
:::

For hands-on registration examples, multi-catalog setups, and cloud
storage configuration, see [Registered Tables](registered-tables.md).

### The SessionState Clone

Understanding what the [`DataFrame`] receives from the
[`SessionContext`] matters: it explains why catalog changes after
creation are still visible, and why config changes are not.

When a [`DataFrame`] is created, [`SessionContext`] clones its
[`SessionState`] (`self.state.read().clone()`). The clone is
**structural, not deep** — the isolation depends on how each field is
stored:

| Field category  | Examples                                                                                      | Clone behavior                                                | After-creation mutations visible?                                                                                                     |
| --------------- | --------------------------------------------------------------------------------------------- | ------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------- |
| **Value types** | `SessionConfig`, `ExecutionProps`, function registries (`HashMap<String, Arc<ScalarUDF>>`, …) | Independent copy (HashMap cloned, individual UDFs Arc-shared) | **No** — changes to config or new UDF registrations do not affect existing DataFrames                                                 |
| **Arc-wrapped** | `catalog_list` (`Arc<dyn CatalogProviderList>`), `runtime_env` (`Arc<RuntimeEnv>`)            | Shared reference (Arc clone)                                  | **Yes** — the default catalog uses interior mutability, so tables registered after DataFrame creation _are_ visible to that DataFrame |

The [`LogicalPlan`], by contrast, is fully owned by the [`DataFrame`]
and never mutated — it is the truly frozen part of the pair.

:::{admonition} What is shared, what is not
:class: note

[`SessionContext`] is mutable — you can register tables, add UDFs, and
configure object stores at any time. Each [`DataFrame`] holds its own
[`SessionState`] clone, which gives it an independent copy of
configuration and function registries. However, the **catalog** and
**runtime** are shared via `Arc` — new table registrations on the
context _are_ visible to previously created DataFrames. The
[`LogicalPlan`] itself is what makes a query deterministic: it captures
the relational operations as they existed at creation time.
:::

## Creation of a DataFrame

**Everything the previous sections describe converges here — the
[`SessionContext`] pairs a [`SessionState`] clone with a
[`LogicalPlan`], and a [`DataFrame`] is born.**

Two components snap together. The [`LogicalPlan`] captures the
relational operations — scans, filters, projections — as a tree of
plan nodes. Each node carries a [`DFSchema`] (an Arrow schema enriched
with table qualifiers) that describes its output columns and types.
The [`SessionState`] clone provides the execution environment: config,
functions, and shared references to the catalog and runtime.

The constructor is a single line:

```rust,ignore
DataFrame::new(session_state, plan)
```

The result is a lazy handle that carries no data. Every transformation
— `.filter()`, `.select()`, `.aggregate()` — returns a _new_
[`DataFrame`] with an extended [`LogicalPlan`] and the same
[`SessionState`] clone. Nothing executes until an action —
[`.collect()`], [`.show()`], [`.execute_stream()`] — triggers the
optimizer, physical planner, and async execution stream. For the full
execution story, see
[Execution Lifecycle](../Concepts/execution-lifecycle.md). For a
comparison of the two APIs that build plans (DataFrame API vs SQL), see
[Builder vs. Parser](../Concepts/builder-parser.md).

### Creation Choices Ripple Forward

The [`TableProvider`] and access pattern you chose upstream constrain
what the optimizer can do downstream. Parquet exposes column statistics
and partition layout, enabling predicate pushdown and partition
pruning — CSV and JSON do not. Registered tables cache metadata once,
giving the optimizer cardinality estimates for join ordering and
aggregation strategies. Direct reads re-derive this metadata on every
call. These decisions are baked into the [`LogicalPlan`] at creation
time; the optimizer can only work with what the plan provides.

**And this is how a DataFrame is born.**

<!-- Link references -->

[`DataFrame`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html
[`LogicalPlan`]: https://docs.rs/datafusion/latest/datafusion/logical_expr/enum.LogicalPlan.html
[`DFSchema`]: https://docs.rs/datafusion/latest/datafusion/common/struct.DFSchema.html
[`Schema`]: https://docs.rs/arrow/latest/arrow/datatypes/struct.Schema.html
[`SessionContext`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html
[`SessionState`]: https://docs.rs/datafusion/latest/datafusion/execution/session_state/struct.SessionState.html
[`TableProvider`]: https://docs.rs/datafusion/latest/datafusion/catalog/trait.TableProvider.html
[`ListingTable`]: https://docs.rs/datafusion/latest/datafusion/datasource/listing/struct.ListingTable.html
[`MemTable`]: https://docs.rs/datafusion/latest/datafusion/datasource/memory/struct.MemTable.html
[`StreamingTable`]: https://docs.rs/datafusion/latest/datafusion/catalog/struct.StreamingTable.html
[`PartitionStream`]: https://docs.rs/datafusion/latest/datafusion/physical_plan/streaming/trait.PartitionStream.html
[`ExecutionPlan`]: https://docs.rs/datafusion/latest/datafusion/physical_plan/trait.ExecutionPlan.html
[`RecordBatch`]: https://docs.rs/arrow/latest/arrow/record_batch/struct.RecordBatch.html
[`.collect()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.collect
[`.show()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.show
[`.execute_stream()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.execute_stream
[`.read_parquet()`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.read_parquet
[`.read_csv()`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.read_csv
[`.register_parquet()`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.register_parquet
[`.register_csv()`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.register_csv
[`.register_table()`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.register_table
[`MemoryPool`]: https://docs.rs/datafusion/latest/datafusion/execution/memory_pool/trait.MemoryPool.html
[`DiskManager`]: https://docs.rs/datafusion/latest/datafusion/execution/disk_manager/struct.DiskManager.html
[`ObjectStoreRegistry`]: https://docs.rs/datafusion/latest/datafusion/execution/object_store/trait.ObjectStoreRegistry.html
[`CatalogProvider`]: https://docs.rs/datafusion/latest/datafusion/catalog/trait.CatalogProvider.html
[`SchemaProvider`]: https://docs.rs/datafusion/latest/datafusion/catalog/trait.SchemaProvider.html
