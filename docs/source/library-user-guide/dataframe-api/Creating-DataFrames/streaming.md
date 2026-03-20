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

# Creating DataFrames from Streaming Sources

**Connect DataFusion to data that never ends — FIFO pipes, custom
network sources, or any continuously arriving stream — through streaming
[`TableProvider`] implementations and the planner's built-in bounded vs.
unbounded awareness.**

Most DataFusion workflows operate on bounded data: files with a known
size, `RecordBatch`es already in memory. This page covers the other
side — unbounded sources where new data can arrive at any moment. It
explains how DataFusion distinguishes bounded from unbounded input, how
to create streaming DataFrames through the built-in [`StreamTable`] and
[`StreamingTable`] providers (or via SQL), and what the planner does
differently when it knows the input has no end.

**Key types and traits:**

| Type / Trait           | Purpose                                                    |
| ---------------------- | ---------------------------------------------------------- |
| [`StreamTable`]        | File-based streaming provider (FIFO, tailed files)         |
| [`StreamingTable`]     | General-purpose streaming provider via [`PartitionStream`] |
| [`PartitionStream`]    | Trait: implement for custom streaming sources              |
| [`FileStreamProvider`] | Built-in file reader/writer for CSV and JSON streams       |
| [`StreamConfig`]       | Configuration wrapper (encoding, ordering, constraints)    |
| [`StreamingTableExec`] | Physical plan node executing streaming partitions          |

:::{admonition} Style Note
:class: note
:collapsible: closed

In this document, all code elements are highlighted with backticks.

- DataFrame methods are written as `.method()` (e.g., `.select()`) to reflect the chaining syntax central to the API.
- standalone functions `method()` (e.g `col()`)

- - static constructors `Struckt::method()` (e.g., `SessionContext::new()`).
- Rust types are formatted as `TypeName` (e.g., `SchemaRef`).

:::

```{contents} Table of Content
:local:
:depth: 2
```

## Streaming and Unbounded Data

**Streaming sources produce _unbounded_ DataFrames — data with no known
end — and DataFusion's planner adapts its operator selection
accordingly.**

:::{admonition} Two meanings of "streaming"
:class: note
This page covers **unbounded data sources** — data that never ends.
DataFusion also uses "streaming" to describe its internal execution
model, where _any_ DataFrame (even from a bounded Parquet file) delivers
results as an incremental `RecordBatch` stream. For that execution-side
topic, see
[Streaming Execution](../Writing-DataFrames/streaming-execution.md).
:::

Streaming data in the DataFusion context means _unbounded data_ —
datasets with no natural end. Where bounded sources (files,
`RecordBatch`es, inline data) eventually deliver their last row and
signal completion, an unbounded source like a First-In-First-Out (FIFO)
pipe, a Kafka topic, or a websocket feed can keep producing data
indefinitely. DataFusion models this distinction through the
`Boundedness` enum at the physical-plan level:

```text
                    Boundedness
                        │
            ┌───────────┴────────────┐
            │                        │
        Bounded                  Unbounded
(files, RecordBatches)   { requires_infinite_memory }
                                     │
                         ┌───────────┴───────────┐
                         │                       │
                       false                   true
                  bounded-memory          needs all data
                  operators work       (Sort, full Agg...)
```

When a [`TableProvider`] signals unbounded input, the physical optimizer
selects streaming-compatible operators (e.g. symmetric hash joins,
incremental aggregations) and **rejects** plans that would require seeing
all data before producing output (e.g. a full `ORDER BY` without a
`LIMIT`). This rejection happens at plan time, not at runtime.

:::{admonition} Always use `.execute_stream()` for unbounded sources
:class: warning
Use [`.execute_stream()`] to consume unbounded DataFrames incrementally,
one batch at a time. Calling [`.collect()`] on an unbounded DataFrame
buffers all arriving batches into memory — memory that grows without
bound.
:::

## Creation Methods for Streaming Sources

**DataFusion provides two built-in [`TableProvider`] implementations for
streaming data and one SQL path — all producing the same lazy
[`DataFrame`] that you transform and execute like any other.**

DataFusion does not ship dedicated methods for streaming providers like
`.read_kafka()` or `.read_websocket()` on [`SessionContext`]. Instead,
streaming sources enter the system as
[`TableProvider`] implementations, registered through the same
[`.register_table()`] or [`.read_table()`] methods used for bounded
sources. The difference is purely in the provider's behavior: it yields
batches indefinitely and signals `Unbounded` to the planner.

| Type / Path                       | Built-in? | Best for                                      |
| --------------------------------- | --------- | --------------------------------------------- |
| [`StreamTable`]                   | Yes       | File-based streams (FIFO pipes, tailed files) |
| `CREATE UNBOUNDED EXTERNAL TABLE` | Yes (SQL) | Declarative registration of unbounded sources |
| [`StreamingTable`]                | Yes       | Custom sources via [`PartitionStream`] trait  |
| Custom [`TableProvider`]          | No        | Full control (Kafka, gRPC, websockets)        |

### File-Based Streams with `StreamTable`

**[`StreamTable`] reads from a single file path as a continuous stream —
the built-in provider for FIFO pipes and tailed log files, with CSV and
JSON encodings out of the box.**

The creation pipeline has three steps: wrap a file path in a
[`FileStreamProvider`], configure it through [`StreamConfig`], and
register the resulting [`StreamTable`] with the session. [`StreamTable`]
always signals `Unbounded` to the planner, regardless of whether the
underlying file is finite or an actual FIFO.

```text
┌─────────────────────────────────────┐
│  FileStreamProvider::new_file(      │
│      schema, path                   │
│  )                                  │
└──────────────┬──────────────────────┘
               │
               ▼
┌─────────────────────────────────────┐
│  StreamConfig::new(source)          │
│    .with_order(...)                 │
│    .with_constraints(...)           │
└──────────────┬──────────────────────┘
               │
               ▼
┌─────────────────────────────────────┐
│  StreamTable::new(config)           │
└──────────────┬──────────────────────┘
               │
               ▼
┌─────────────────────────────────────┐
│  ctx.register_table("name", table)  │
└─────────────────────────────────────┘
```

The following example demonstrates the API using a CSV file created
at runtime. In production, the path would typically point to a Unix
FIFO (`mkfifo`) or a continuously appended log file:

```rust
use std::sync::Arc;
# use std::io::Write;
use datafusion::prelude::*;
# use datafusion::error::Result;
# use datafusion::assert_batches_eq;
use datafusion::datasource::stream::{
    FileStreamProvider, StreamConfig, StreamTable,
};
use arrow::datatypes::{DataType, Field, Schema};

#[tokio::main]
async fn main() -> Result<()> {
    let ctx = SessionContext::new();

    // In production this would be a FIFO pipe; here we create a temp CSV
    # let tmp_dir = tempfile::tempdir()?;
    # let csv_path = tmp_dir.path().join("sensor.csv");
    # let mut f = std::fs::File::create(&csv_path)?;
    # writeln!(f, "device,reading,ts")?;
    # writeln!(f, "sensor_a,23.5,1")?;
    # writeln!(f, "sensor_b,18.2,2")?;
    # writeln!(f, "sensor_a,24.1,3")?;
    # drop(f);
    let path = "/tmp/sensor_feed.csv";
    # let path = &csv_path;

    // Define the schema explicitly — streaming sources cannot infer schema
    let schema = Arc::new(Schema::new(vec![
        Field::new("device", DataType::Utf8, false),
        Field::new("reading", DataType::Float64, false),
        Field::new("ts", DataType::Int64, false),
    ]));

    // Build the streaming provider pipeline
    let source = FileStreamProvider::new_file(
        Arc::clone(&schema),
        path.into(),
    )
    .with_header(true)
    .with_batch_size(10);

    let config = StreamConfig::new(Arc::new(source));
    let table = StreamTable::new(Arc::new(config));

    // Register and query like any other table
    ctx.register_table("sensor_feed", Arc::new(table))?;
    let df = ctx
        .sql("SELECT device, reading FROM sensor_feed")
        .await?;

    let batches = df.collect().await?;
    assert_batches_eq!(
        &[
            "+----------+---------+",
            "| device   | reading |",
            "+----------+---------+",
            "| sensor_a | 23.5    |",
            "| sensor_b | 18.2    |",
            "| sensor_a | 24.1    |",
            "+----------+---------+",
        ],
        &batches
    );

    Ok(())
}
```

The result is a lazy [`DataFrame`] — the same type returned by every
other creation method. You can chain `.filter()`, `.select()`,
`.aggregate()` and all other DataFrame operations before executing.

:::{admonition} FIFO on Unix
:class: tip
On Unix systems, create a named pipe with `mkfifo /tmp/sensor.pipe`,
then point `FileStreamProvider` at the pipe path. A separate process
writes CSV or JSON lines into the pipe, and DataFusion reads them as an
unbounded stream. See the
[FIFO integration test](https://github.com/apache/datafusion/blob/main/datafusion/core/tests/fifo/mod.rs)
for a complete working example.
:::

:::{admonition} Supported encodings
:class: note
[`FileStreamProvider`] supports `StreamEncoding::Csv` (default) and
`StreamEncoding::Json` (newline-delimited JSON). Other formats require a
custom [`StreamProvider`] implementation.
:::

### SQL: `CREATE UNBOUNDED EXTERNAL TABLE`

**The SQL path to streaming: adding the `UNBOUNDED` keyword to
`CREATE EXTERNAL TABLE` tells the planner the source never ends.**

When DataFusion's SQL engine sees `UNBOUNDED`, it delegates to
[`StreamTableFactory`] and marks the resulting table as unbounded. The
planner then applies the same streaming-aware operator selection as
with the Rust API. If the query requires an operator that cannot run on
unbounded input, plan generation fails with a clear error.

```sql
CREATE UNBOUNDED EXTERNAL TABLE sensor_feed (
    ts      TIMESTAMP NOT NULL,
    device  VARCHAR   NOT NULL,
    reading DOUBLE    NOT NULL
)
STORED AS CSV
LOCATION '/tmp/sensor.pipe'
WITH ORDER (ts ASC);
```

The `WITH ORDER` clause is particularly valuable for streaming sources:
it declares the pre-existing sort order of arriving data, enabling the
planner to use incremental operators (e.g. streaming aggregation on
ordered keys) without inserting a full sort.

For the complete SQL syntax, see
[DDL: CREATE EXTERNAL TABLE](../../../../user-guide/sql/ddl.md).

### Custom Sources with `StreamingTable`

**[`StreamingTable`] wraps any [`PartitionStream`] implementation into a
streaming [`TableProvider`] — the general-purpose path for custom
unbounded sources.**

While [`StreamTable`] is limited to file-based sources, [`StreamingTable`]
accepts any implementation of the [`PartitionStream`] trait. This is the
path for Kafka consumers, gRPC streams, websocket feeds, or any custom
data source that produces [`RecordBatch`]es incrementally.

The critical API call is `.with_infinite_table(true)` on
[`StreamingTable`] — without it, the provider defaults to bounded and
the planner will not apply streaming operators. You can also declare
pre-existing sort order via `.with_sort_order()`.

The following conceptual example shows how a Tokio channel-based source
implements [`PartitionStream`] and is registered as an unbounded table:

```rust,no_run
use std::sync::Arc;
# use std::fmt::Debug;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::catalog::streaming::StreamingTable;
use datafusion::physical_plan::streaming::PartitionStream;
use datafusion::physical_plan::stream::RecordBatchReceiverStreamBuilder;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::execution::TaskContext;
use datafusion::prelude::*;
# use datafusion::error::Result;

#[derive(Debug)]
struct ChannelSource {
    schema: SchemaRef,
    receiver: std::sync::Mutex<
        Option<tokio::sync::mpsc::Receiver<arrow::array::RecordBatch>>,
    >,
}

impl PartitionStream for ChannelSource {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(&self, _ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
        let schema = Arc::clone(&self.schema);
        let mut rx = self.receiver.lock().unwrap().take()
            .expect("partition can only be read once");
        let mut builder = RecordBatchReceiverStreamBuilder::new(schema, 2);
        let tx = builder.tx();
        builder.spawn(async move {
            while let Some(batch) = rx.recv().await {
                if tx.send(Ok(batch)).await.is_err() {
                    break;
                }
            }
            Ok(())
        });
        builder.build()
    }
}

 #[tokio::main]
 async fn main() -> Result<()> {
     let ctx = SessionContext::new();

    let schema = Arc::new(Schema::new(vec![
        Field::new("device", DataType::Utf8, false),
        Field::new("reading", DataType::Float64, false),
    ]));

    let (_tx, rx) = tokio::sync::mpsc::channel(100);
    let source = ChannelSource {
        schema: Arc::clone(&schema),
        receiver: std::sync::Mutex::new(Some(rx)),
    };

    let provider = StreamingTable::try_new(schema, vec![Arc::new(source)])?
        .with_infinite_table(true);

    ctx.register_table("live_readings", Arc::new(provider))?;

    // Query the unbounded source — use execute_stream(), never collect()
    let df = ctx.sql("SELECT * FROM live_readings").await?;
    let _stream = df.execute_stream().await?;
    // Process batches incrementally via stream.next().await

     Ok(())
 }
```

From this point, `df` is an ordinary [`DataFrame`]. The only difference
from bounded DataFrames is in _consumption_: use [`.execute_stream()`]
instead of [`.collect()`] to process batches incrementally.

:::{admonition} Conceptual example
:class: note
This example shows the API pattern for a channel-based streaming source.
In production, replace the Tokio channel with your actual data source
(Kafka consumer, gRPC stream, websocket, etc.).
:::

## How the Planner Responds

**When unbounded input enters the plan, DataFusion's physical optimizer
selects streaming-compatible operators and rejects plans that would
require infinite memory.**

Once you have an unbounded [`DataFrame`], any transformation you chain
(`.filter()`, `.aggregate()`, `.join()`) goes through the same lazy
planning as bounded DataFrames — but the physical optimizer applies
additional constraints. The [`SanityCheckPlan`] optimizer rule inspects
the `Boundedness` of every node in the physical plan. If an operator would require
`requires_infinite_memory: true` on an unbounded input — or has
`EmissionType::Final` (must see all data before producing output) — the
plan is rejected with a clear error _before_ any execution begins.

The following table summarizes how common operator categories behave with
unbounded input:

| Operator category         | Unbounded support | Notes                                                       |
| ------------------------- | ----------------- | ----------------------------------------------------------- |
| Filter, Projection        | Yes               | Stateless — process row-by-row with bounded memory          |
| LIMIT (with fetch)        | Yes               | Converts unbounded to bounded by stopping after N rows      |
| Symmetric Hash Join       | Conditional       | Requires prunable join conditions and ordered input         |
| Incremental Aggregation   | Yes               | Streaming aggregation on ordered keys (e.g. `MIN`, `MAX`)   |
| Sort-Preserving Merge     | Yes               | Merges pre-sorted partitions with bounded memory            |
| Full Sort                 | No                | Requires seeing all data — `requires_infinite_memory: true` |
| Full Aggregation          | No                | Operators like `MEDIAN` need all values                     |
| Hash Join (non-streaming) | No                | Builds a full hash table from one side                      |

:::{admonition} Ordered streams unlock more operators
:class: tip
Declaring sort order on your streaming source (via `StreamConfig::with_order()`
or the SQL `WITH ORDER` clause) gives the planner more options. For
example, `MIN` and `MAX` on an ordered unbounded stream can run with
bounded memory, while on an unordered stream they require infinite memory.
:::

For the full optimization pipeline and how `Boundedness` propagates
through plan nodes, see
[Execution Lifecycle](../Concepts/execution-lifecycle.md).

## What DataFusion Does Not Provide

**DataFusion provides the building blocks for streaming — `TableProvider`
traits, `Boundedness`-aware planning, and incremental execution — but is
not a turnkey streaming framework.**

DataFusion is a query engine, not a streaming platform. The following
capabilities are outside its current scope:

| Capability               | Status in DataFusion                                   |
| ------------------------ | ------------------------------------------------------ |
| Kafka / Pulsar connector | Not built-in — implement via custom [`TableProvider`]  |
| Event-time watermarks    | Not supported — no built-in watermark tracking         |
| Checkpointing            | Not supported — no Chandy-Lamport or similar mechanism |
| Distributed shuffle      | Not built-in — single-node execution by default        |
| Windowed aggregations    | Limited — basic streaming aggregation only             |

This is by design: DataFusion focuses on being an excellent, extensible
query engine that streaming frameworks can build _on top of_. While it
does not provide turnkey watermarking, its sort-order tracking and
interval-based pruning in streaming joins provide the primitives that
downstream frameworks build upon. Several projects do exactly that:

- [**Arroyo**](https://github.com/ArroyoSystems/arroyo) — distributed
  stream processing engine using DataFusion for SQL parsing and logical
  plan generation, with custom streaming operators, Kafka connectors,
  and Chandy-Lamport checkpointing.
- [**Denormalized**](https://github.com/probably-nothing-labs/denormalized) —
  a "DuckDB for streaming" focused on single-node windowed
  aggregations on top of DataFusion.
- [**Synnada**](https://synnada.ai/) — building a unified batch and
  streaming engine on DataFusion, contributing streaming operator
  improvements upstream.

:::{admonition} Community discussion
:class: seealso
For the ongoing conversation about streaming support in DataFusion,
see [GitHub Discussion #11404](https://github.com/apache/datafusion/issues/11404).
:::

## Choosing the Right Approach

**Three paths to unbounded DataFrames — pick based on your source type
and how much control you need.**

| Approach                          | Source type           | Control level | Setup effort | Best for                                          |
| --------------------------------- | --------------------- | ------------- | ------------ | ------------------------------------------------- |
| [`StreamTable`]                   | File-based (FIFO)     | Low           | Minimal      | FIFO pipes, tailed CSV/JSON files                 |
| `CREATE UNBOUNDED EXTERNAL TABLE` | File-based (SQL)      | Low           | Minimal      | SQL-first workflows, declarative registration     |
| [`StreamingTable`]                | Any `PartitionStream` | Medium        | Moderate     | Channel-based sources, custom integrations        |
| Custom [`TableProvider`]          | Anything              | Full          | Significant  | Kafka, gRPC, websockets, sources needing pushdown |

For simple file-based streaming, `StreamTable` or the SQL `UNBOUNDED`
path suffices. For custom sources (message queues, network streams),
implement `PartitionStream` and use `StreamingTable`. For sources that
need predicate pushdown, custom partitioning, or sink capabilities,
implement the full `TableProvider` trait directly.

## References

**Concepts and Guides:**

- [Streaming Execution](../Writing-DataFrames/streaming-execution.md) —
  consuming DataFrames incrementally via `.execute_stream()`
- [Execution Lifecycle](../Concepts/execution-lifecycle.md) —
  how plans move from lazy construction through optimization to execution
- [How DataFrame Creation Works](creating-concepts.md) —
  the `TableProvider` architecture and built-in providers
- [Registered Tables](registered-tables.md) —
  registering and managing tables in the catalog
- [DDL: CREATE EXTERNAL TABLE](../../../../user-guide/sql/ddl.md) —
  full SQL syntax including `UNBOUNDED` and `WITH ORDER`

**API Documentation:**

- [`StreamTable`] — file-based streaming `TableProvider`
- [`StreamingTable`] — generic streaming `TableProvider`
- [`PartitionStream`] — trait for custom streaming partitions
- [`StreamingTableExec`] — physical execution plan for streaming sources
- [`FileStreamProvider`] — built-in file stream reader/writer
- [`StreamConfig`] — configuration for `StreamTable`
- [`StreamProvider`] — trait for custom file-like stream sources
- [`TableProvider`] — the core trait for all data sources

<!-- Link references -->

[`DataFrame`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html
[`SessionContext`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html
[`TableProvider`]: https://docs.rs/datafusion/latest/datafusion/catalog/trait.TableProvider.html
[`.register_table()`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.register_table
[`.read_table()`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.read_table
[`.collect()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.collect
[`.execute_stream()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.execute_stream
[`StreamTable`]: https://docs.rs/datafusion/latest/datafusion/datasource/stream/struct.StreamTable.html
[`StreamConfig`]: https://docs.rs/datafusion/latest/datafusion/datasource/stream/struct.StreamConfig.html
[`StreamProvider`]: https://docs.rs/datafusion/latest/datafusion/datasource/stream/trait.StreamProvider.html
[`FileStreamProvider`]: https://docs.rs/datafusion/latest/datafusion/datasource/stream/struct.FileStreamProvider.html
[`StreamTableFactory`]: https://docs.rs/datafusion/latest/datafusion/datasource/stream/struct.StreamTableFactory.html
[`StreamingTable`]: https://docs.rs/datafusion/latest/datafusion/catalog/struct.StreamingTable.html
[`PartitionStream`]: https://docs.rs/datafusion/latest/datafusion/physical_plan/streaming/trait.PartitionStream.html
[`StreamingTableExec`]: https://docs.rs/datafusion/latest/datafusion/physical_plan/streaming/struct.StreamingTableExec.html
[`RecordBatch`]: https://docs.rs/arrow/latest/arrow/record_batch/struct.RecordBatch.html
[`SanityCheckPlan`]: https://docs.rs/datafusion/latest/datafusion/physical_optimizer/sanity_checker/struct.SanityCheckPlan.html
