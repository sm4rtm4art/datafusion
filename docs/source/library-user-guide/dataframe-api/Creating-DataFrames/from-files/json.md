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

JSON is the default interchange format for web APIs, logging pipelines,
message queues, and NoSQL databases (i.e. MongoDB, Elasticsearch, CouchDB...).
DataFusion reads JSON in **NDJSON format** by default — also known as JSON
Lines (`.jsonl`) or Newline-Delimited JSON (`.ndjson`) — where each line
contains one complete JSON object. Standard **JSON arrays**
(`[{...}, {...}]`) are also supported via [`.newline_delimited()`][`jsonreadoptions::newline_delimited()`]
option.

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

## Reading JSON Files

**A single call to [`.read_json()`] returns a lazy DataFrame — schema
inference happens at creation time, data processing waits for an action.**

When you call [`.read_json()`], DataFusion reads the beginning of the file
to infer column types, then returns a lazy [`DataFrame`]. Nested JSON objects
flatten to Arrow struct columns — `{"user": {"name": "Alice"}}` becomes a
struct column accessible as `user.name`. The actual parsing of all rows
waits until you trigger an action like [`.collect()`].

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
    let df = ctx.read_json(path, JsonReadOptions::default()).await?;

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

:::{admonition} NDJSON is the default — JSON arrays require explicit opt-in
:class: note

DataFusion defaults to **Newline-Delimited JSON** (one complete JSON object
per line). Standard JSON arrays (`[{"a": 1}, {"b": 2}]`) and
pretty-printed multi-line objects will produce parsing errors unless you
explicitly set [`.newline_delimited()`][`jsonreadoptions::newline_delimited()`].

```rust,ignore
// Read a JSON array file ([{...}, {...}])
let options = JsonReadOptions::default().newline_delimited(false);
let df = ctx.read_json("data.json", options).await?;
```

```sql
-- SQL equivalent
CREATE EXTERNAL TABLE my_table
STORED AS JSON
OPTIONS ('format.newline_delimited' 'false')
LOCATION 'path/to/array.json';
```

**Limitations:** JSON array format reads the entire file sequentially — it
cannot split the file into byte ranges for parallel scanning
([`datafusion.optimizer.repartition_file_scans`][repartition-file-scans]). For large datasets, NDJSON remains the
performant choice.

:::

:::{admonition} Schema inference is limited
:class: warning

JSON files carry no file-level schema — DataFusion infers types from
the first 1,000 objects (configurable via [`.schema_infer_max_records()`][`jsonreadoptions::schema_infer_max_records()`]).
Deeply nested, sparse, or late-appearing fields may not be detected.
**Always provide an explicit schema in production.** See
[Production Best Practices](#production-best-practices) below.
:::

## JsonReadOptions

**[`JsonReadOptions`] configures how DataFusion parses JSON files —
schema, file extensions, compression, format, and streaming behavior.**

JSON's simplicity means fewer variables than Parquet or CSV. There are no
delimiter or quoting rules to configure. The main decisions are whether to
provide an explicit schema, which file extensions to match, and whether the
data is compressed or in array format. The following table lists the
available options.

| Builder Method                                                                          | Default   | Usage                                                                                                  |
| :-------------------------------------------------------------------------------------- | :-------- | :----------------------------------------------------------------------------------------------------- |
| **[`.schema(&Schema)`][`jsonreadoptions::schema()`]**                                   | `None`    | Explicit schema. **Recommended for production** to enforce strict types and avoid inference surprises. |
| **[`.schema_infer_max_records(usize)`][`jsonreadoptions::schema_infer_max_records()`]** | `1000`    | Number of objects sampled for schema inference. Increase for heterogeneous data; set `0` to disable.   |
| **[`.newline_delimited(bool)`][`jsonreadoptions::newline_delimited()`]**                | `true`    | Set `false` to read standard JSON arrays (`[{...}, {...}]`). Disables parallel file scanning.          |
| **[`.file_extension(&str)`][`jsonreadoptions::file_extension()`]**                      | `".json"` | Filters input files by suffix. Use `".jsonl"` or `".ndjson"` for non-standard extensions.              |
| **[`.table_partition_cols(Vec)`][`jsonreadoptions::table_partition_cols()`]**           | `[]`      | Maps Hive-style directory paths to columns (e.g., `year=2024/month=01/`).                              |
| **[`.file_sort_order(Vec)`][`jsonreadoptions::file_sort_order()`]**                     | `[]`      | Tells the optimizer the data is pre-sorted. Use to speed up merge-joins or `ORDER BY` queries.         |
| **[`.mark_infinite(bool)`][`jsonreadoptions::mark_infinite()`]**                        | `false`   | Marks this source as unbounded (never reaches EOF). Use for Unix named pipes or streaming inputs.      |

:::{admonition} JSON array support
:class: tip

[`JsonReadOptions`] supports reading standard JSON arrays
(`[{...}, {...}]`) via the [`.newline_delimited()`][`jsonreadoptions::newline_delimited()`] builder method.
When set, DataFusion streams the array into NDJSON internally — no manual
conversion needed. See [the admonition above](#reading-json-files) for a
code example.
:::

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

| Builder Method                                                                  | Default        | Usage                                                                                          |
| :------------------------------------------------------------------------------ | :------------- | :--------------------------------------------------------------------------------------------- |
| **[`.file_compression_type(...)`][`jsonreadoptions::file_compression_type()`]** | `UNCOMPRESSED` | Compression algorithm (GZIP, BZIP2, XZ, ZSTD). For reading `.json.gz` or `.json.zst` directly. |

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

    let options = JsonReadOptions::default()
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

---

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

Use [`.register_json()`] to register the
JSON file as a named table in the [`SessionContext`] catalog. This enables:

- **SQL access** — query the table via [`.sql()`]
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
produces a [`DataFusionError`] at execution time. Within the sample,
DataFusion handles some coercion (Int64 + Float64 widens to Float64), but
once the schema is locked, it is fixed — the same mechanics as
[CSV schema inference][csv].

JSON null handling follows the explicit Arrow schema. Missing keys and
explicit JSON `null` values become NULL when the corresponding `Field` is
nullable. If a required field is missing or null, DataFusion returns an
error instead of silently widening the schema contract.

To guarantee safety, use [`JsonReadOptions::schema()`] to provide an
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
    # writeln!(file, r#"{{"id":2,"event":"click"}}"#)?;
    # writeln!(file, r#"{{"id":3,"event":null,"value":10.0}}"#)?;

    // Define the explicit schema
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("event", DataType::Utf8, true),
        Field::new("value", DataType::Float64, true),
    ]);

    let path = "events.json";
    # let path = json_path.to_str().unwrap();
    let df = ctx.read_json(path, JsonReadOptions::default()
        .schema(&schema)
    ).await?;

    let results = df.collect().await?;
    assert_batches_sorted_eq!(
        &[
            "+----+-------+-------+",
            "| id | event | value |",
            "+----+-------+-------+",
            "| 1  | login | 42.5  |",
            "| 2  | click |       |",
            "| 3  |       | 10.0  |",
            "+----+-------+-------+",
        ],
        &results
    );

    Ok(())
}
```

:::{admonition} schema() is a builder method
:class: note

[`JsonReadOptions::schema()`] _sets_ the expected schema for the data
reader before the file is processed. This defines the contract for how
DataFusion should parse the incoming bytes.

This differs from [`DataFrame::schema()`], which _returns_ the resolved
[`DFSchema`] of an already-created DataFrame.
:::

## JSON References

- [`JsonReadOptions` API][`jsonreadoptions`] — All configuration options
- [JSON Format Options (SQL)][format-options] — SQL-level options for `CREATE EXTERNAL TABLE` and `COPY`
- [datafusion-functions-json][datafusion-functions-json] — Community-maintained scalar functions for querying JSON strings (`json_get`, `json_contains`, `json_length`)

---

<!-- References -->

<!-- Internal documentation -->

[csv]: csv.md
[format-options]: ../../../../user-guide/sql/format_options.md
[repartition-file-scans]: ../../../../user-guide/configs.md

<!-- Core types -->

[`dataframe`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html
[`datafusionerror`]: https://docs.rs/datafusion/latest/datafusion/error/enum.DataFusionError.html
[`dfschema`]: https://docs.rs/datafusion/latest/datafusion/common/struct.DFSchema.html
[`jsonreadoptions`]: https://docs.rs/datafusion/latest/datafusion/datasource/file_format/options/struct.JsonReadOptions.html
[`sessioncontext`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html

<!-- Methods and functions -->

[`.collect()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.collect
[`.read_json()`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.read_json
[`.register_json()`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.register_json
[`.sql()`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.sql
[`dataframe::schema()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.schema
[`jsonreadoptions::file_compression_type()`]: https://docs.rs/datafusion/latest/datafusion/datasource/file_format/options/struct.JsonReadOptions.html#method.file_compression_type
[`jsonreadoptions::file_extension()`]: https://docs.rs/datafusion/latest/datafusion/datasource/file_format/options/struct.JsonReadOptions.html#method.file_extension
[`jsonreadoptions::file_sort_order()`]: https://docs.rs/datafusion/latest/datafusion/datasource/file_format/options/struct.JsonReadOptions.html#method.file_sort_order
[`jsonreadoptions::mark_infinite()`]: https://docs.rs/datafusion/latest/datafusion/datasource/file_format/options/struct.JsonReadOptions.html#method.mark_infinite
[`jsonreadoptions::newline_delimited()`]: https://docs.rs/datafusion/latest/datafusion/datasource/file_format/options/struct.JsonReadOptions.html#method.newline_delimited
[`jsonreadoptions::schema()`]: https://docs.rs/datafusion/latest/datafusion/datasource/file_format/options/struct.JsonReadOptions.html#method.schema
[`jsonreadoptions::schema_infer_max_records()`]: https://docs.rs/datafusion/latest/datafusion/datasource/file_format/options/struct.JsonReadOptions.html#method.schema_infer_max_records
[`jsonreadoptions::table_partition_cols()`]: https://docs.rs/datafusion/latest/datafusion/datasource/file_format/options/struct.JsonReadOptions.html#method.table_partition_cols

<!-- External resources -->

[datafusion-functions-json]: https://github.com/datafusion-contrib/datafusion-functions-json
