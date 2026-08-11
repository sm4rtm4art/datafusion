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

# Creating DataFrames from RecordBatches

**Arrow [`RecordBatch`]es are DataFusion's native in-memory format —
wrapping them in a [`DataFrame`] requires no parsing, no inference,
and no conversion.**

When data already lives in Arrow's columnar format — from Arrow
Flight streams, IPC deserialization, Parquet readers, or your own
application logic — this is the most direct creation path into
DataFusion. Every method on this page wraps batches in a [`MemTable`]
(DataFusion's in-memory [`TableProvider`]) and returns a single lazy
[`DataFrame`], ready for the full builder API. This page covers
one-shot reads for ephemeral processing, catalog registration for
multi-query reuse, and explicit [`MemTable`] construction for
partitioned parallelism.

**Key methods:**
| Method | Purpose |
| --------------------- | -------------------------------------------------- |
| [`.read_batch()`] | Ephemeral DataFrame from a single [`RecordBatch`] |
| [`.read_batches()`] | Ephemeral DataFrame from multiple batches |
| [`.register_batch()`] | Register a batch as a named table in the catalog |
| [`MemTable::try_new()`] | Full control over partitions for parallel execution |

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

## From RecordBatch to DataFrame

**Every method on this page wraps [`RecordBatch`]es in a [`MemTable`]
and returns one lazy [`DataFrame`] — no conversion overhead.**

DataFusion is built on [Apache Arrow](../../../user-guide/arrow-introduction.md),
and [`RecordBatch`] is Arrow's standard unit for in-memory columnar
data. A [`RecordBatch`] commonly arrives from Arrow Flight streams,
IPC deserialization, Parquet readers, or your own application logic.
To bring these batches into DataFusion, every creation method on
this page wraps them in a [`MemTable`] — DataFusion's in-memory
[`TableProvider`] — and produces a single lazy [`DataFrame`].

Regardless of how many batches you pass, each method call produces
**one** [`DataFrame`] representing one logical table. Multiple
batches are not physically concatenated; they are stored inside the
[`MemTable`] and streamed batch-by-batch during execution. For the
architectural role of [`MemTable`] in DataFusion's provider model,
see [MemTable (In-Memory)][creating-concepts].

### Single Batch with `.read_batch()`

**Wrap a single [`RecordBatch`] in an ephemeral [`DataFrame`] — no
catalog entry, no name, immediate access.**

[`.read_batch()`] is the simplest entry point: pass one
[`RecordBatch`], get one [`DataFrame`]. The DataFrame exists only
as long as you hold the variable — nothing is registered in the
catalog.

```rust
use std::sync::Arc;
use datafusion::prelude::*;
use datafusion::arrow::array::{ArrayRef, Int32Array, Float64Array};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::Result;
use datafusion::assert_batches_eq;

#[tokio::main]
async fn main() -> Result<()> {
    let ctx = SessionContext::new();

    let batch = RecordBatch::try_from_iter(vec![
        ("product_id", Arc::new(Int32Array::from(vec![1, 2, 3, 4])) as ArrayRef),
        ("revenue", Arc::new(Float64Array::from(vec![1200.0, 450.0, 890.0, 2100.0])) as ArrayRef),
    ])?;

    // Wrap in a DataFrame — no catalog entry
    let result = ctx.read_batch(batch)?
        .filter(col("revenue").gt(lit(500.0)))?
        .sort(vec![col("revenue").sort(false, true)])?
        .collect()
        .await?;

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
        &result
    );

    Ok(())
}
```

### Multiple Batches with `.read_batches()`

**Combine multiple [`RecordBatch`]es with the same schema into a
single ephemeral [`DataFrame`] — batches are streamed, not
concatenated.**

[`.read_batches()`] is useful when data arrives in chunks — for
example, from multiple Arrow Flight streams or a chunked file
reader. All batches must share the exact same schema. They are
stored within one [`MemTable`] partition and streamed sequentially
during execution; DataFusion does not physically merge them into
a single batch.

```rust
use std::sync::Arc;
use datafusion::prelude::*;
use datafusion::arrow::array::{ArrayRef, Int32Array, StringArray};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::Result;
use datafusion::assert_batches_sorted_eq;

#[tokio::main]
async fn main() -> Result<()> {
    let ctx = SessionContext::new();

    // Two batches with identical schemas — e.g., from two Arrow Flight streams
    let batch_a = RecordBatch::try_from_iter(vec![
        ("sensor_id", Arc::new(Int32Array::from(vec![1, 2])) as ArrayRef),
        ("location", Arc::new(StringArray::from(vec!["North", "South"])) as ArrayRef),
    ])?;
    let batch_b = RecordBatch::try_from_iter(vec![
        ("sensor_id", Arc::new(Int32Array::from(vec![3, 4])) as ArrayRef),
        ("location", Arc::new(StringArray::from(vec!["East", "West"])) as ArrayRef),
    ])?;

    // One DataFrame from both batches
    let result = ctx.read_batches(vec![batch_a, batch_b])?
        .sort(vec![col("sensor_id").sort(true, true)])?
        .collect()
        .await?;

    assert_batches_sorted_eq!(
        &[
            "+-----------+----------+",
            "| sensor_id | location |",
            "+-----------+----------+",
            "| 1         | North    |",
            "| 2         | South    |",
            "| 3         | East     |",
            "| 4         | West     |",
            "+-----------+----------+",
        ],
        &result
    );

    Ok(())
}
```

:::{admonition} Schema consistency
:class: warning
All batches passed to [`.read_batches()`] must share the exact same
schema (column names, types, order, nullability). A mismatch
produces an error at creation time. For details on constructing
[`RecordBatch`]es and common pitfalls, see the
[Arrow Introduction][arrow-introduction].
:::

### Registering for Reuse

**Register a [`RecordBatch`] as a named table in the catalog —
making it accessible to both the DataFrame API and SQL across
multiple queries.**

[`.register_batch()`] wraps the batch in a [`MemTable`] and places
it in the catalog under the given name. From that point, you can
query the table by name via [`.table()`] or [`.sql()`], and
reference it in any number of downstream queries.

```rust
use std::sync::Arc;
use datafusion::prelude::*;
use datafusion::arrow::array::{ArrayRef, Int32Array, Float64Array};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::Result;
use datafusion::assert_batches_eq;

#[tokio::main]
async fn main() -> Result<()> {
    let ctx = SessionContext::new();

    let batch = RecordBatch::try_from_iter(vec![
        ("product_id", Arc::new(Int32Array::from(vec![1, 2, 3, 4])) as ArrayRef),
        ("revenue", Arc::new(Float64Array::from(vec![1200.0, 450.0, 890.0, 2100.0])) as ArrayRef),
    ])?;

    // Register as a named table
    ctx.register_batch("live_sales", batch)?;

    // Query via SQL
    let high_revenue = ctx.sql(
        "SELECT product_id, revenue \
         FROM live_sales \
         WHERE revenue > 1000 \
         ORDER BY revenue DESC"
    ).await?;

    let result = high_revenue.collect().await?;
    assert_batches_eq!(
        &[
            "+------------+---------+",
            "| product_id | revenue |",
            "+------------+---------+",
            "| 4          | 2100.0  |",
            "| 1          | 1200.0  |",
            "+------------+---------+",
        ],
        &result
    );

    // Also accessible via the DataFrame API
    let count = ctx.table("live_sales").await?
        .count()
        .await?;
    assert_eq!(count, 4);

    Ok(())
}
```

For details on catalog registration, deregistration, and how
registered tables interact with SQL, see
[Registered Tables][registered-tables].

### Explicit MemTable for Partitioned Data

**Create a [`MemTable`] directly for full control over partitioning
— enabling parallel execution across multiple batch groups.**

The convenience methods ([`.read_batch()`], [`.read_batches()`],
[`.register_batch()`]) all create a [`MemTable`] internally with a
single partition. When you need multiple partitions — for example,
to let DataFusion process batch groups in parallel across CPU
cores — construct the [`MemTable`] explicitly with
[`MemTable::try_new()`].

```rust
use std::sync::Arc;
use datafusion::prelude::*;
use datafusion::arrow::array::{ArrayRef, Int32Array, StringArray};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::error::Result;
use datafusion::assert_batches_sorted_eq;

#[tokio::main]
async fn main() -> Result<()> {
    let ctx = SessionContext::new();

    // Partition 1: sensors from region North
    let north = RecordBatch::try_from_iter(vec![
        ("sensor_id", Arc::new(Int32Array::from(vec![1, 2])) as ArrayRef),
        ("region", Arc::new(StringArray::from(vec!["North", "North"])) as ArrayRef),
    ])?;

    // Partition 2: sensors from region South
    let south = RecordBatch::try_from_iter(vec![
        ("sensor_id", Arc::new(Int32Array::from(vec![3, 4])) as ArrayRef),
        ("region", Arc::new(StringArray::from(vec!["South", "South"])) as ArrayRef),
    ])?;

    // Two partitions — DataFusion can process them in parallel
    let schema = north.schema();
    let provider = MemTable::try_new(schema, vec![vec![north], vec![south]])?;

    ctx.register_table("sensors", Arc::new(provider))?;

    let result = ctx.table("sensors").await?
        .sort(vec![col("sensor_id").sort(true, true)])?
        .collect()
        .await?;

    assert_batches_sorted_eq!(
        &[
            "+-----------+--------+",
            "| sensor_id | region |",
            "+-----------+--------+",
            "| 1         | North  |",
            "| 2         | North  |",
            "| 3         | South  |",
            "| 4         | South  |",
            "+-----------+--------+",
        ],
        &result
    );

    Ok(())
}
```

The `partitions` argument to [`MemTable::try_new()`] is a
`Vec<Vec<RecordBatch>>` — each inner `Vec` is one partition. During
execution, DataFusion can assign different partitions to different
threads. Within a partition, batches are streamed sequentially.

:::{admonition} When to use explicit MemTable
:class: tip
Use [`MemTable::try_new()`] when you have naturally partitioned data
(e.g., batches from different sources or regions) and want DataFusion
to parallelize across them. For single-batch or single-partition
scenarios, the convenience methods are simpler.
:::

---

## Choosing the Right Method

**All paths produce the same lazy [`DataFrame`] — choose based on
batch count, catalog needs, and parallelism.**

The four methods differ only in their input shape and catalog
behavior — the resulting [`DataFrame`] is identical regardless of
which method created it. The table below summarizes the trade-offs.

| Method                  | Input                   | Catalog entry | Partitions | Best for                               |
| ----------------------- | ----------------------- | ------------- | ---------- | -------------------------------------- |
| [`.read_batch()`]       | Single `RecordBatch`    | No            | 1          | One-off processing of a single batch   |
| [`.read_batches()`]     | `Vec<RecordBatch>`      | No            | 1          | Combining multiple same-schema batches |
| [`.register_batch()`]   | Single `RecordBatch`    | Yes (named)   | 1          | SQL access, multi-query reuse          |
| [`MemTable::try_new()`] | `Vec<Vec<RecordBatch>>` | Manual        | N          | Parallel execution across partitions   |

:::{admonition} Multiple batches via `.register_batch()`
:class: note
[`.register_batch()`] accepts only a single [`RecordBatch`]. To
register multiple batches under one name, construct a [`MemTable`]
explicitly and register it with [`.register_table()`].
:::

---

## Bringing It Together

Every creation method on this page wraps Arrow [`RecordBatch`]es in
a [`MemTable`] and produces a single lazy [`DataFrame`]. Use
[`.read_batch()`] or [`.read_batches()`] for ephemeral, one-off
processing. Use [`.register_batch()`] when you need a named table
accessible to SQL and multiple queries. And when you need
partitioned parallelism, construct a [`MemTable`] directly with
[`MemTable::try_new()`]. In all cases, the data stays in Arrow's
native columnar format — no conversion, no serialization overhead.

---

## Further Reading

**Concepts & Guides:**

- [Arrow Introduction][arrow-introduction] — RecordBatch fundamentals, construction, and common pitfalls
- [MemTable (In-Memory)][creating-concepts] — Architectural role of MemTable in DataFusion's provider model
- [Registered Tables][registered-tables] — Catalog registration, deregistration, and SQL access

**API Documentation:**

- [`SessionContext::read_batch()`] — One-shot DataFrame from a single RecordBatch
- [`SessionContext::read_batches()`] — One-shot DataFrame from multiple RecordBatches
- [`SessionContext::register_batch()`] — Register a RecordBatch as a named table
- [`MemTable`] — In-memory TableProvider for RecordBatches
- [`RecordBatch`] — Arrow's columnar in-memory data format

**Arrow Ecosystem:**

- [Arrow Flight][arrow-flight] — Network protocol for high-performance Arrow data transfer

---

<!-- References -->

<!-- Internal documentation -->

[arrow-introduction]: ../../../user-guide/arrow-introduction.md
[creating-concepts]: creating-concepts.md
[registered-tables]: registered-tables.md

<!-- Core types -->

[`dataframe`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html
[`memtable`]: https://docs.rs/datafusion/latest/datafusion/datasource/memory/struct.MemTable.html
[`recordbatch`]: https://docs.rs/arrow/latest/arrow/record_batch/struct.RecordBatch.html
[`tableprovider`]: https://docs.rs/datafusion/latest/datafusion/catalog/trait.TableProvider.html

<!-- Methods and functions -->

[`.read_batch()`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.read_batch
[`.read_batches()`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.read_batches
[`.register_batch()`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.register_batch
[`.register_table()`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.register_table
[`.sql()`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.sql
[`.table()`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.table
[`memtable::try_new()`]: https://docs.rs/datafusion/latest/datafusion/datasource/memory/struct.MemTable.html#method.try_new
[`sessioncontext::read_batch()`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.read_batch
[`sessioncontext::read_batches()`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.read_batches
[`sessioncontext::register_batch()`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.register_batch

<!-- External resources -->

[arrow-flight]: https://arrow.apache.org/docs/format/Flight.html
