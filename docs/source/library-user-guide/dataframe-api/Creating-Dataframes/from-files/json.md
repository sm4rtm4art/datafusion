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

# JSON — Semi-Structured and Streaming Data

**JSON — Row-oriented and self-describing per record, ubiquitous in web
APIs, logging pipelines, and NoSQL systems.**

JSON is the default interchange format for web APIs, logging pipelines, message queues, and NoSQL databases (i.e.MongoDB, Elasticsearch, CouchDB...). DataFusion reads JSON in NDJSON format — also known as JSON Lines (.jsonl) or Newline-Delimited JSON (.ndjson) — where each line contains one complete JSON object.

```{contents} Table of Contents
:local:
:depth: 2
```

## Reading JSON Files

**A single call to `ctx.read_json()` returns a lazy DataFrame — schema
inference happens at creation time, data processing waits for an action.**

When you call `ctx.read_json()`, DataFusion reads the beginning of the file
to infer column types, then returns a lazy `DataFrame`. Nested JSON objects
flatten to Arrow struct columns — `{"user": {"name": "Alice"}}` becomes a
struct column accessible as `user.name`. The actual parsing of all rows
waits until you trigger an action like `.collect()`.

```rust
use datafusion::prelude::*;
use datafusion::assert_batches_sorted_eq;
# use std::fs::File;
# use std::io::Write;
# use tempfile::tempdir;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();
    # let dir = tempdir()?;
    # let json_path = dir.path().join("events.json");
    # let mut file = File::create(&json_path)?;
    # writeln!(file, r#"{{"id":1,"event":"login"}}"#)?;
    # writeln!(file, r#"{{"id":2,"event":"click"}}"#)?;
    # writeln!(file, r#"{{"id":3,"event":"logout"}}"#)?;

    // Read NDJSON — schema is inferred at creation time
    let path = "events.json";
    # let path = json_path.to_str().unwrap();
    let df = ctx.read_json(path, NdJsonReadOptions::default()).await?;

    // The inferred schema is available immediately
    println!("{}", df.schema());

    // Project columns explicitly (JSON field order is not guaranteed)
    let results = df.select_columns(&["id", "event"])?.collect().await?;
    assert_batches_sorted_eq!(
        &[
            "+----+--------+",
            "| id | event  |",
            "+----+--------+",
            "| 1  | login  |",
            "| 2  | click  |",
            "| 3  | logout |",
            "+----+--------+",
        ],
        &results
    );

    Ok(())
}
```

:::{admonition} NDJSON only — no JSON arrays or pretty-printed files
:class: warning

DataFusion expects **Newline-Delimited JSON** (one complete JSON object per
line). Standard JSON arrays (`[{"a": 1}, {"b": 2}]`) and pretty-printed
multi-line JSON objects will produce parsing errors. If your data is in
array format, convert it to NDJSON first (one object per line, no wrapping
array brackets).
:::

:::{admonition} Schema inference is limited
:class: warning

NDJSON files carry no file-level schema — DataFusion infers types from
the first 1,000 objects (configurable via `.schema_infer_max_records()`).
Deeply nested, sparse, or late-appearing fields may not be detected.
**Always provide an explicit schema in production.** See
[Production Best Practices](#production-best-practices) below.
:::

## NdJsonReadOptions

**[`NdJsonReadOptions`] configures how DataFusion parses NDJSON files —
schema, file extensions, compression, and streaming behavior.**

JSON's simplicity means fewer variables than Parquet or CSV. There are no delimiter or quoting rules to configure. The main decisions are whether to provide an explicit schema, which file extensions to match, and whether the data is compressed. The following tables lists the available options.

| Builder Method                                                                            | Default   | Usage                                                                                                  |
| :---------------------------------------------------------------------------------------- | :-------- | :----------------------------------------------------------------------------------------------------- |
| **[`.schema(&Schema)`][`ndjsonreadoptions::schema()`]**                                   | `None`    | Explicit schema. **Recommended for production** to enforce strict types and avoid inference surprises. |
| **[`.schema_infer_max_records(usize)`][`ndjsonreadoptions::schema_infer_max_records()`]** | `1000`    | Number of objects sampled for schema inference. Increase for heterogeneous data; set `0` to disable.   |
| **[`.file_extension(&str)`][`ndjsonreadoptions::file_extension()`]**                      | `".json"` | Filters input files by suffix. Use `".jsonl"` or `".ndjson"` for non-standard extensions.              |
| **[`.table_partition_cols(Vec)`][`ndjsonreadoptions::table_partition_cols()`]**           | `[]`      | Maps Hive-style directory paths to columns (e.g., `year=2024/month=01/`).                              |
| **[`.file_sort_order(Vec)`][`ndjsonreadoptions::file_sort_order()`]**                     | `[]`      | Tells the optimizer the data is pre-sorted. Use to speed up merge-joins or `ORDER BY` queries.         |
| **[`.mark_infinite(bool)`][`ndjsonreadoptions::mark_infinite()`]**                        | `false`   | Marks this source as unbounded (never reaches EOF). Use for Unix named pipes or streaming inputs.      |

### Compression

**DataFusion reads compressed NDJSON files directly — no manual
decompression step required.**

Log pipelines and data exports commonly compress NDJSON before writing to
storage. JSON's repetitive structure (field names, timestamps, similar
values) compresses well, often achieving 5-10x size reduction. DataFusion
supports `GZIP`, `BZIP2`, `XZ`, and `ZSTD`, decompressing on the fly
during the read process.

:::{admonition} Compressed NDJSON disables parallel reading
:class: warning

DataFusion reads uncompressed NDJSON files in parallel by splitting the
file into byte ranges across multiple CPU cores. Compressed formats (GZIP,
BZIP2, XZ, ZSTD) are **non-splittable** — the decompression stream has no
random-access entry points, so the entire file must be read sequentially on
a single thread. For large files, this creates a significant bottleneck.
:::

| Builder Method                                                                    | Default        | Usage                                                                                          |
| :-------------------------------------------------------------------------------- | :------------- | :--------------------------------------------------------------------------------------------- |
| **[`.file_compression_type(...)`][`ndjsonreadoptions::file_compression_type()`]** | `UNCOMPRESSED` | Compression algorithm (GZIP, BZIP2, XZ, ZSTD). For reading `.json.gz` or `.json.zst` directly. |

:::{admonition} Example: Reading compressed NDJSON
:class: seealso
:collapsible: open

This example reads a GZIP-compressed NDJSON file — a common pattern for
log ingestion pipelines.

```rust
use datafusion::prelude::*;
use datafusion::assert_batches_sorted_eq;
use datafusion::datasource::file_format::file_compression_type::FileCompressionType;
# use std::io::Write;
# use tempfile::NamedTempFile;
# use flate2::write::GzEncoder;
# use flate2::Compression;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();

    let options = NdJsonReadOptions::default()
        .file_compression_type(FileCompressionType::GZIP)
        .file_extension(".gz");

    // Path to compressed NDJSON file(s)
    let path = "logs/*.json.gz";
    # let mut temp_file = NamedTempFile::with_suffix(".json.gz").unwrap();
    # {
    #     let mut encoder = GzEncoder::new(&mut temp_file, Compression::default());
    #     writeln!(encoder, r#"{{"id": 1, "name": "Alice"}}"#).unwrap();
    #     writeln!(encoder, r#"{{"id": 2, "name": "Bob"}}"#).unwrap();
    #     encoder.finish().unwrap();
    # }
    # let path = temp_file.path().to_string_lossy().to_string();

    let df = ctx.read_json(&path, options).await?;

    let results = df.select_columns(&["id", "name"])?.collect().await?;
    assert_batches_sorted_eq!(
        &[
            "+----+-------+",
            "| id | name  |",
            "+----+-------+",
            "| 1  | Alice |",
            "| 2  | Bob   |",
            "+----+-------+",
        ],
        &results
    );

    Ok(())
}
```

:::

## When to Use JSON

**Use JSON for ingestion and interchange of semi-structured data. For analytical workloads or anything queried repeatedly, convert to Parquet.**

JSON files carry no column statistics and no internal structure beyond
individual objects. DataFusion cannot perform predicate pushdown (skipping
irrelevant data based on statistics). Every line must be read from disk
because the parser needs to see the full JSON object to find field
boundaries.

However, DataFusion **does** perform projection pushdown in memory: it
tells the JSON parser which fields to materialize, so only the selected
columns are allocated as Arrow arrays. On objects with many fields, this
saves significant RAM even though all bytes are still read from disk.

- **No predicate pushdown** — every line is parsed, even if filtered later
- **In-memory projection only** — all bytes are read from disk, but only
  selected columns are materialized as Arrow arrays

| JSON Shines ✓                               | Avoid JSON ✗                                        |
| ------------------------------------------- | --------------------------------------------------- |
| Data interchange: APIs, logs, NoSQL exports | Production analytics on large datasets → Parquet    |
| Semi-structured / evolving schemas          | Highly selective queries needing predicate pushdown |
| Streaming pipelines, message queues         | Storage efficiency matters (verbose text format)    |
| Healthcare data exchange (HL7 FHIR)         | Strict schema contracts → Avro or Parquet           |
| Append-friendly, human-readable             | Deeply nested data queried analytically → Parquet   |

:::{admonition} Register for repeated queries and SQL access
:class: tip

Use `ctx.register_json("table_name", "path.json", options)` to register the
JSON file as a named table in the `SessionContext` catalog. This enables:

- **SQL access** — query the table via `ctx.sql("SELECT * FROM table_name")`
- **Cross-query reuse** — multiple DataFrame operations and SQL queries
  can reference the same table name without re-reading options or paths
- **Schema caching** — the inferred (or explicit) schema is resolved once
  at registration time, avoiding repeated inference scans

For datasets that will be queried analytically over time, consider
converting to Parquet — its columnar layout and embedded statistics provide
significantly better query performance.
:::

## Production Best Practices

**Always provide an explicit schema in production to avoid inference
failures with sparse or heterogeneous data.**

Schema inference from a finite sample is inherently unreliable for
heterogeneous data. DataFusion samples a fixed window of objects (default:
1,000) and locks column types based solely on what it observes. Any field
that first appears beyond the sample boundary, or any type variation
(e.g., a field that is sometimes a string and sometimes an integer),
produces a `DataFusionError` at execution time. Within the sample,
DataFusion handles some coercion (Int64 + Float64 widens to Float64), but
once the schema is locked, it is fixed — the same mechanics as
[CSV schema inference](csv.md#production-best-practices).

To guarantee safety, use [`NdJsonReadOptions::schema()`] to provide an
explicit schema:

```rust
use datafusion::prelude::*;
use datafusion::assert_batches_sorted_eq;
use datafusion::arrow::datatypes::{Schema, Field, DataType};
# use std::fs::File;
# use std::io::Write;
# use tempfile::tempdir;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();
    # let dir = tempdir()?;
    # let json_path = dir.path().join("events.json");
    # let mut file = File::create(&json_path)?;
    # writeln!(file, r#"{{"id":1,"event":"login","value":42.5}}"#)?;
    # writeln!(file, r#"{{"id":2,"event":"click","value":10.0}}"#)?;

    // Define the explicit schema
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("event", DataType::Utf8, true),
        Field::new("value", DataType::Float64, true),
    ]);

    let path = "events.json";
    # let path = json_path.to_str().unwrap();
    let df = ctx.read_json(path, NdJsonReadOptions::default()
        .schema(&schema)
    ).await?;

    let results = df.collect().await?;
    assert_batches_sorted_eq!(
        &[
            "+----+-------+-------+",
            "| id | event | value |",
            "+----+-------+-------+",
            "| 1  | login | 42.5  |",
            "| 2  | click | 10.0  |",
            "+----+-------+-------+",
        ],
        &results
    );

    Ok(())
}
```

:::{admonition} schema() is a builder method
:class: note

[`NdJsonReadOptions::schema()`] _sets_ the expected schema for the data
reader before the file is processed. This defines the contract for how
DataFusion should parse the incoming bytes.

This differs from [`DataFrame::schema()`], which _returns_ the resolved
`DFSchema` of an already-created DataFrame.
:::

## JSON References

- [`NdJsonReadOptions` API](https://docs.rs/datafusion/latest/datafusion/prelude/struct.NdJsonReadOptions.html) — All configuration options
- [JSON Format Options (SQL)](../../../../../user-guide/sql/format_options.md#json-format-options) — SQL-level options for `CREATE EXTERNAL TABLE` and `COPY`
