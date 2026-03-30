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

# Arrow IPC — Zero-Copy Native Format

**Arrow IPC is Arrow's native serialization
format: near-zero deserialization overhead, columnar projection, and
exact type fidelity across languages.**

Arrow IPC — also known as Feather v2 — persists Arrow's in-memory columnar
layout directly to disk. DataFusion is built on Arrow, so IPC files are
its most natural data source: the on-disk bytes already match the in-memory
representation, so reading requires minimal conversion. DataFusion reads IPC
**files** from disk or object stores via `ctx.read_arrow()` — it does not
natively connect to live message brokers.


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


```{contents}
:local:
:depth: 2
```

## Reading Arrow IPC Files

**A single call to `ctx.read_arrow()` returns a lazy DataFrame — the
embedded schema is read from the file header with near-zero
deserialization cost.**

The name "Arrow IPC" stands for Inter-Process Communication — a serialization protocol
that stores Arrow record batches as a flat sequence of binary messages.
DataFusion reads the schema from the file header (or footer, for the File
format), then returns a lazy `DataFrame`. Unlike CSV or JSON, no schema
inference is needed — types are preserved exactly as they were written.

Arrow IPC is columnar, so DataFusion performs true I/O-level column projection:
when reading the File format, only the requested columns are decoded from disk.
This is stronger than the in-memory-only projection available for
row-based formats like CSV, JSON, or Avro.

The Arrow IPC specification defines two sub-formats. DataFusion auto-detects
which one a file uses:

1. **File format** (`.arrow`, `.feather`) — includes a footer with schema and
   batch offsets; supports range-based **parallel reading**
2. **Stream format** — no footer; must be read **sequentially** from start to
   end (not to be confused with live event streaming — see
   [Streaming Sources](../streaming.md))

:::{seealso}

Arrow IPC serializes record batches as self-contained binary messages — a schema
message followed by one or more data messages.
The File format adds a footer for random access;
the Stream format is append-only. Both preserve Arrow's columnar layout byte-for-byte,
enabling near-zero deserialization.

For more details, See:

- [Arrow Columnar Format — Serialization and IPC](https://arrow.apache.org/docs/format/Columnar.html#serialization-and-interprocess-communication-ipc)
- [Arrow Flight RPC](https://arrow.apache.org/docs/format/Flight.html)
  :::

```rust
use datafusion::execution::options::ArrowReadOptions;
use datafusion::prelude::*;
use datafusion::assert_batches_eq;
# use std::path::PathBuf;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();

    let path = "data.arrow";
    # let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
    #     .join("../datasource-arrow/tests/data/example.arrow")
    #     .to_string_lossy().to_string();

    let df = ctx.read_arrow(&path, ArrowReadOptions::default()).await?;

    // Schema is available immediately — no inference needed
    println!("{}", df.schema());

    let results = df.select_columns(&["f0", "f1"])?.collect().await?;
    assert_batches_eq!(
        &[
            "+----+-----+",
            "| f0 | f1  |",
            "+----+-----+",
            "| 1  | foo |",
            "| 2  | bar |",
            "| 3  | baz |",
            "| 4  |     |",
            "+----+-----+",
        ],
        &results
    );

    Ok(())
}
```

## ArrowReadOptions

**[`ArrowReadOptions`] requires minimal configuration — the embedded
schema and columnar layout handle most concerns automatically.**

Arrow IPC files are self-describing: the schema is embedded in the header
(Stream format) or footer (File format). The main decisions are whether to
override the embedded schema, which file extension to match, and how to
handle Hive-style partitioned directories.

| Option                                                                         | Default    | Usage                                                                                    |
| :----------------------------------------------------------------------------- | :--------- | :--------------------------------------------------------------------------------------- |
| **[`.schema(&Schema)`][`arrowreadoptions::schema()`]**                         | `None`     | Override the embedded schema. Normalize types across files or standardize metadata.      |
| **`.file_extension`**                                                          | `".arrow"` | Filters input files by suffix. No builder method — use struct update syntax (see below). |
| **[`.table_partition_cols(Vec)`][`arrowreadoptions::table_partition_cols()`]** | `[]`       | Maps Hive-style directory paths to columns (e.g., `year=2024/month=01/`).                |

[`file_extension`] has no builder method. If your files use a different
suffix (`.feather`, `.ipc`), set the field directly via struct update syntax:

```rust,ignore
let options = ArrowReadOptions {
    file_extension: ".feather",
    ..Default::default()
};
```

:::{admonition} schema() is a builder method
:class: note

[`ArrowReadOptions::schema()`] _sets_ the expected schema for the data
reader before the file is processed. This defines the contract for how
DataFusion should interpret the incoming bytes.

This differs from [`DataFrame::schema()`], which _returns_ the resolved
`DFSchema` of an already-created DataFrame.
:::

---

## When to Use Arrow IPC

**Arrow IPC is the fastest path from disk to query — use it for
inter-process data exchange, caching, and cross-language sharing where
query startup time matters more than storage efficiency.**

Arrow IPC's strength is near-zero deserialization: the on-disk layout
matches Arrow's in-memory format, so DataFusion can start querying
almost immediately. It also preserves Arrow types exactly, avoiding the
lossy round-trips that can occur with CSV or JSON.

- **No predicate pushdown** — Arrow IPC carries no min/max statistics;
  DataFusion must read and decode batches before applying filters
- **Column projection supported** — for the File format, DataFusion reads
  only the requested columns from disk (true I/O-level pruning)
- **No file-level compression** — Arrow IPC handles buffer-level
  compression internally (LZ4, ZSTD) and transparently; there is no
  `.gz` wrapper to configure
- **No statistics for the optimizer** — Arrow IPC provides no row counts
  or value ranges for cost-based optimization

| Arrow IPC Shines ✓                                   | Avoid Arrow IPC ✗                                   |
| ---------------------------------------------------- | --------------------------------------------------- |
| Inter-process communication (shared memory, pipes)   | Production analytics on large datasets → Parquet    |
| Caching intermediate DataFusion results              | Highly selective queries needing predicate pushdown |
| Cross-language exchange (Arrow is language-agnostic) | Long-term storage (larger files than Parquet)       |
| Same-machine data sharing between processes          | Need min/max statistics for optimizer → Parquet     |
| Wire format for Arrow Flight RPC                     | Less ecosystem tooling than Parquet or CSV          |

:::{admonition} Register for repeated queries and SQL access
:class: tip

Use `ctx.register_arrow("table_name", "path.arrow", options)` to register
the IPC file as a named table in the `SessionContext` catalog. This enables:

- **SQL access** — query the table via `ctx.sql("SELECT * FROM table_name")`
- **Cross-query reuse** — multiple DataFrame operations and SQL queries
  can reference the same table name without re-reading options or paths

For datasets that will be queried analytically over time, consider
converting to Parquet — its embedded statistics and predicate pushdown
provide significantly better query performance.
:::

---

## Production Tips

**Arrow IPC files are best suited for ephemeral, high-throughput data
exchange — for long-lived analytical data, convert to Parquet.**

The format prioritizes read speed over storage efficiency. The following
guidelines help avoid common pitfalls.

| Concern                      | Guidance                                                                                                                               |
| :--------------------------- | :------------------------------------------------------------------------------------------------------------------------------------- |
| Caching intermediate results | Write DataFusion outputs as IPC to avoid re-computation. LZ4 buffer compression is applied by default — no configuration needed.       |
| Cross-process sharing        | Standard format for passing record batches between processes on the same machine. Columnar alignment enables memory-mapped reads.      |
| File extensions              | Files may use `.arrow`, `.feather`, or `.ipc`. Set `file_extension` in `ArrowReadOptions` to match your naming convention.             |
| Long-term storage            | Prefer Parquet — smaller files, embedded statistics, and predicate pushdown provide significantly better analytical query performance. |

## Arrow IPC References

- [`ArrowReadOptions` API](https://docs.rs/datafusion/latest/datafusion/execution/options/struct.ArrowReadOptions.html) — All configuration options
- [Arrow Columnar Format — IPC](https://arrow.apache.org/docs/format/Columnar.html#serialization-and-interprocess-communication-ipc) — Format specification for the IPC protocol
- [Arrow Flight RPC](https://arrow.apache.org/docs/format/Flight.html) — High-performance network data transfer built on Arrow IPC
