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

# Avro — Schema-Embedded Event Streaming

**Avro — Row-oriented binary format with an embedded schema, designed for
schema evolution in event streaming and cross-system interchange.**

Avro is the standard format for event streaming systems (i.e. Kafka, Pulsar,...) and
schema registries (i.e. Confluent, Apicurio,...). Every Avro file carries its writer
schema in the header, making it self-describing without the inference
overhead of CSV or JSON. DataFusion reads Avro **files** through
`ctx.read_avro()` — event data archived to disk, S3, or any object store.
It does not natively connect to live Kafka or Pulsar brokers; consuming
streams requires an external connector that writes Avro data to files or
batches first. DataFusion extracts the schema directly from the file header
and decodes row-by-row into Arrow columns. As a row-based format, Avro
does not support predicate pushdown — for analytical workloads, Parquet is
the better choice.

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

## Reading Avro Files

:::{admonition} Feature flag required
:class: warning

Avro support is a compile-time opt-in. Without the `avro` feature flag,
`ctx.read_avro()` and `ctx.register_avro()` do not exist — your code will
not compile. This gate exists because Avro pulls in the `apache-avro` crate
and its transitive dependencies, adding to compile time and binary size.
Enable it in your `Cargo.toml`:

```toml
datafusion = { version = "...", features = ["avro"] }
```

:::

**A single call to `ctx.read_avro()` reads the schema from the file
header and returns a lazy DataFrame — no inference, no sampling.**

Avro files carry their schema in the file header.
DataFusion reads this embedded schema directly — there is no sampling step
and no risk of inference errors, unlike CSV or JSON. When reading multiple files, DataFusion
merges the schemas across all files via `Schema::try_merge()`, supporting
files written at different schema versions.

Nested Avro records map to Arrow struct columns, and Avro enums map to
Arrow dictionary types. The actual data processing waits until you trigger
an action like `.collect()`.

```rust
# #[cfg(feature = "avro")]
# {
use datafusion::prelude::*;
use datafusion::assert_batches_sorted_eq;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();

    // Path to your Avro file
    let path = "events.avro";
    # let testdata = datafusion::test_util::arrow_test_data();
    # let path = format!("{testdata}/avro/alltypes_plain.avro");

    let df = ctx.read_avro(&path, AvroReadOptions::default()).await?;

    // The embedded schema is available immediately
    println!("{}", df.schema());

    // Select and verify results
    let results = df.select_columns(&["id", "bool_col"])?.collect().await?;
    assert_batches_sorted_eq!(
        &[
            "+----+----------+",
            "| id | bool_col |",
            "+----+----------+",
            "| 0  | true     |",
            "| 1  | false    |",
            "| 2  | true     |",
            "| 3  | false    |",
            "| 4  | true     |",
            "| 5  | false    |",
            "| 6  | true     |",
            "| 7  | false    |",
            "+----+----------+",
        ],
        &results
    );

    Ok(())
}
# }
```

## AvroReadOptions

**[`AvroReadOptions`] exposes only a few settings — Avro's embedded schema
and internal compression handle most concerns automatically.**

There are no delimiters, quoting rules, compression settings, or inference
parameters to configure. The schema is read from the file header
automatically. The main decisions are whether to override the embedded
schema, which file extension to match, and how to handle Hive-style
partitioned directories.

| Option                                                                        | Default   | Usage                                                                                         |
| :---------------------------------------------------------------------------- | :-------- | :-------------------------------------------------------------------------------------------- |
| **[`.schema(&Schema)`][`avroreadoptions::schema()`]**                         | `None`    | Override the embedded schema. Use to enforce strict types or resolve cross-file schema drift. |
| **`.file_extension`**                                                         | `".avro"` | Filters input files by suffix. No builder method — use struct update syntax (see below).      |
| **[`.table_partition_cols(Vec)`][`avroreadoptions::table_partition_cols()`]** | `[]`      | Maps Hive-style directory paths to columns (e.g., `year=2024/month=01/`).                     |

[`file_extension`] has no builder method. If your files use a different
suffix, set the field directly via struct update syntax:

```rust,ignore
let options = AvroReadOptions {
    file_extension: ".avrodata",
    ..Default::default()
};
```

:::{admonition} Example: Hive-partitioned Avro directory
:class: seealso
:collapsible: open

For Hive-partitioned directories (e.g., `events/year=2024/month=01/*.avro`),
use `.table_partition_cols()` to map directory structure to columns.

```rust
# #[cfg(feature = "avro")]
# {
use datafusion::arrow::datatypes::DataType;
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();

    // Configure partition columns for Hive-style directories
    let options = AvroReadOptions::default().table_partition_cols(vec![
        ("year".into(), DataType::Int32),
        ("month".into(), DataType::Int32),
    ]);

    // Path to partitioned Avro directory
    let path = "events/";
    # let testdata = datafusion::test_util::arrow_test_data();
    # let path = format!("{testdata}/avro/alltypes_plain.avro");
    # let options = AvroReadOptions::default();

    let df = ctx.read_avro(&path, options).await?;
    df.show().await?;
    Ok(())
}
# }
```

:::

## When to Use Avro

**Avro is streaming-native: schema metadata and compatibility matter more
than query performance.**

Avro's strength is the embedded schema — producers and consumers can evolve
independently, and every file is self-describing. As a row-based format in
DataFusion, it trades analytical performance for interchange reliability.

In short: embedded schema (no inference risk), row-based decoding (no
pushdown), transparent block-level compression — Avro optimizes for
interchange, not analytics.

- **No predicate pushdown** — DataFusion decodes full rows; it cannot skip
  data based on filter predicates
- **In-memory projection only** — the I/O layer reads entire Avro rows
  from disk, but DataFusion's decoder only allocates Arrow arrays for the
  selected columns, saving significant memory on wide schemas
- **No statistics for the optimizer** — Avro provides no row counts or
  min/max values
- **Block-level compression is transparent** — Avro handles compression
  internally (Snappy, Deflate, Bzip2, XZ, Zstandard). The codec is
  embedded in the file header and decompression happens automatically —
  there is nothing to configure. See the
  [Avro specification](https://avro.apache.org/docs/current/specification/#required-codecs)
  for supported codecs

| Avro Shines ✓                                     | Avoid Avro ✗                                        |
| ------------------------------------------------- | --------------------------------------------------- |
| Event streaming (Kafka, Pulsar, Kinesis)          | Production analytics on large datasets → Parquet    |
| Schema registry integration (Confluent, Apicurio) | Highly selective queries needing predicate pushdown |
| Cross-system interchange with schema contracts    | Storage efficiency for wide tables → Parquet        |
| Schema evolution (add/remove fields safely)       | Columnar analytics (aggregations, joins) → Parquet  |
| Compact binary format (smaller than JSON)         | Need min/max statistics for optimizer → Parquet     |

:::{admonition} Register for repeated queries and SQL access
:class: tip

Use `ctx.register_avro("table_name", "path.avro", options)` to register the
Avro file as a named table in the `SessionContext` catalog. This enables:

- **SQL access** — query the table via `ctx.sql("SELECT * FROM table_name")`
- **Cross-query reuse** — multiple DataFrame operations and SQL queries
  can reference the same table name without re-reading options or paths
- **Header caching** — the embedded schema is resolved once at registration
  time, avoiding repeated header reads

For datasets that will be queried analytically over time, consider
converting to Parquet — its columnar layout, embedded statistics, and
predicate pushdown provide significantly better query performance.
:::

## Production Tips

**Schema drift across files is Avro's primary production risk — use an
explicit schema override when reading from multiple producers.**

Avro eliminates the schema inference problems that plague CSV and JSON.
However, it introduces a different class of concerns: files written by
different producers or at different schema versions may not merge cleanly,
and Avro's schema resolution logic lives outside DataFusion.

- **Schema drift across files** — When reading a directory of Avro files
  from multiple producers, schemas may diverge. DataFusion merges them via
  `Schema::try_merge()`, but incompatible changes (e.g., a field changing
  from `Int` to `String`) produce an error. Use
  [`AvroReadOptions::schema()`] to enforce a canonical schema when drift
  is a risk.
- **Schema evolution is Avro's, not DataFusion's** — Avro's
  [schema resolution rules](https://avro.apache.org/docs/current/specification/#schema-resolution)
  (defaults for missing fields, field aliasing, type promotion) are
  handled by the underlying `apache-avro` crate. DataFusion reads what the
  Avro decoder produces. If you rely on reader/writer schema resolution,
  test the behavior with your specific schema versions.
- **Feature flag across environments** — Avro support requires
  `datafusion = { features = ["avro"] }`. Without it, `read_avro()` and
  `register_avro()` do not compile. This is easy to miss when moving
  between development, CI, and production configurations.

## Avro References

- [`AvroReadOptions` API](https://docs.rs/datafusion/latest/datafusion/prelude/struct.AvroReadOptions.html) — All configuration options
- [Apache Avro Specification](https://avro.apache.org/docs/current/specification/) — Format specification, schema resolution, codecs
