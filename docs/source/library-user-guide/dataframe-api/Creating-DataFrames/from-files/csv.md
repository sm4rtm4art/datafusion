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

# CSV — The Universal Interchange Format

**CSV — Row-based, simple, human-readable, and universal. It is the lowest common denominator for data exchange, but comes with schema inference and parsing overhead.**

CSV is the most widely used format for data exchange, accepted across virtually all domains and tools. However, it is row-oriented text with no embedded schema. DataFusion must parse text row-by-row and infer data types by scanning the file, making it more expensive to read than columnar formats like Parquet.

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

## Reading CSV Files

**A single call to `ctx.read_csv()` infers the schema from the first 1,000 rows and returns a lazy DataFrame ready for querying.**

When you call `ctx.read_csv()`, DataFusion immediately reads the beginning of the file to infer column types from the data it samples. The actual data processing—filtering, joining, and full parsing—waits until you trigger an action like `.collect()`.

```rust
use datafusion::prelude::*;
use datafusion::assert_batches_sorted_eq;
# use datafusion::error::Result;
# use std::fs::File;
# use std::io::Write;
# use tempfile::tempdir;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();
    # // Create test CSV file
    # let dir = tempdir()?;
    # let csv_path = dir.path().join("example.csv");
    # let mut file = File::create(&csv_path)?;
    # writeln!(file, "id,name")?;
    # writeln!(file, "1,Alice")?;
    # writeln!(file, "2,Bob")?;

    // Read CSV (schema is inferred immediately at DataFrame creation)
    let path = "data.csv";
    # let path = csv_path.to_str().unwrap();
    let df = ctx.read_csv(path, CsvReadOptions::new()).await?;

    // The inferred schema is available immediately
    println!("{}", df.schema());
    // Output:
    // Schema { fields: [
    //     Field { name: "id", data_type: Int64, nullable: true, ... },
    //     Field { name: "name", data_type: Utf8, nullable: true, ... }
    // ], ... }

    // Execute eagerly and verify results
    let results = df.collect().await?;
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

## CsvReadOptions: Handling the Messy Reality

**[`CsvReadOptions`] provides builder methods to handle the structural
variance of CSV files — from delimiters and quoting rules to compression
and null representation.**

Unlike Parquet, CSV has no formal specification. Delimiter choice (`,`, `;`,
`\t`), quoting rules, escape characters, null markers, line endings, and
header conventions all vary between producers. A file exported from Excel
looks different from a PostgreSQL `COPY` dump or an R `write.csv()` output.
[`CsvReadOptions`] lets you configure each of these axes so DataFusion can
parse your specific files correctly.

### Formatting and Structure

The following options configure how DataFusion parses the structural
elements of a CSV file — delimiters, quoting, escaping, and line handling:

| Builder Method                                                                         | Default  | Usage                                                                                                  |
| :------------------------------------------------------------------------------------- | :------- | :----------------------------------------------------------------------------------------------------- |
| **[`.has_header(bool)`][`csvreadoptions::has_header()`]**                              | `true`   | Treats first row as column names. Set `false` if file starts immediately with data.                    |
| **[`.delimiter(u8)`][`csvreadoptions::delimiter()`]**                                  | `b','`   | Field separator character. Use `b'\t'` for TSV or `b';'` for European CSV.                             |
| **[`.quote(u8)`][`csvreadoptions::quote()`]**                                          | `b'"'`   | Character to quote fields containing delimiters. Use `b'\''` for single-quote dialects.                |
| **[`.terminator(Option<u8>)`][`csvreadoptions::terminator()`]**                        | `None`   | Line terminator character. Defaults to `None` (CRLF). Override for non-standard line endings.          |
| **[`.escape(u8)`][`csvreadoptions::escape()`]**                                        | `None`   | Character used to escape quotes inside quoted fields.                                                  |
| **[`.comment(u8)`][`csvreadoptions::comment()`]**                                      | `None`   | Character marking the start of a comment line (e.g., `b'#'`). Lines starting with this are ignored.    |
| **[`.newlines_in_values(bool)`][`csvreadoptions::newlines_in_values()`]**              | `false`  | Allows `\n` inside quoted fields. **Warning**: Disables parallel file scanning (slower).               |
| **[`.null_regex(Option<String>)`][`csvreadoptions::null_regex()`]**                    | `None`   | Treats specific strings (e.g., `"NA"`, `"\\N"`) as null. Use when data uses non-standard null markers. |
| **[`.truncated_rows(bool)`][`csvreadoptions::truncated_rows()`]**                      | `false`  | If `true`, pads rows with missing columns using nulls instead of returning an error.                   |
| **[`.schema_infer_max_records(usize)`][`csvreadoptions::schema_infer_max_records()`]** | `1000`   | Number of rows sampled for schema inference. Increase for heterogeneous data; set `0` to disable.      |
| **[`.file_extension(&str)`][`csvreadoptions::file_extension()`]**                      | `".csv"` | Filters input files by suffix. Use to ignore metadata files in mixed directories.                      |
| **[`.table_partition_cols(Vec)`][`csvreadoptions::table_partition_cols()`]**           | `[]`     | Maps Hive-style directory paths to columns (e.g., `year=2025/`).                                       |
| **[`.file_sort_order(Vec)`][`csvreadoptions::file_sort_order()`]**                     | `[]`     | Tells the optimizer the data is pre-sorted. Use to speed up merge-joins or `ORDER BY` queries.         |

### Compression

**DataFusion supports reading compressed CSV files directly, which drastically reduces I/O bottlenecks and storage costs.**

CSV files are uncompressed plain text — they consume significant disk space
and I/O bandwidth. Compressing them before storage is standard practice.

DataFusion can decompress these files on the fly during the read process. It
supports `GZIP`, `BZIP2`, `XZ`, and `ZSTD`. To enable this, configure the
`.file_compression_type()`:

:::{admonition} Compressed CSVs disable parallel reading
:class: warning

DataFusion reads uncompressed CSV files in parallel by splitting the file
into byte ranges across multiple CPU cores. Compressed formats (GZIP,
BZIP2, XZ, ZSTD) are **non-splittable** — the decompression stream has no
random-access entry points, so the entire file must be read sequentially on
a single thread. For large files, this creates a significant bottleneck.

The same constraint applies when `.newlines_in_values(true)` is set,
because row boundaries can no longer be determined by byte offset alone.
:::

| Builder Method                                                                 | Default        | Usage                                                                                        |
| :----------------------------------------------------------------------------- | :------------- | :------------------------------------------------------------------------------------------- |
| **[`.file_compression_type(...)`][`csvreadoptions::file_compression_type()`]** | `UNCOMPRESSED` | Compression algorithm (GZIP, BZIP2, XZ, ZSTD). For reading `.csv.gz` or `.csv.zst` directly. |

:::{admonition} Example: Reading compressed CSV
:class: seealso
:collapsible: open

This example shows how to read a GZIP-compressed CSV file—a common pattern for log ingestion pipelines.

```rust
use datafusion::prelude::*;
use datafusion::assert_batches_eq;
use datafusion::datasource::file_format::file_compression_type::FileCompressionType;
# use std::path::PathBuf;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();

    // Path to GZIP-compressed CSV
    let path = "logs.csv.gz";
    # let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
    #     .join("../../testing/data/csv/aggregate_test_100.csv.gz")
    #     .to_string_lossy().to_string();

    // Configure reader for GZIP compressed files
    let options = CsvReadOptions::new()
        .file_extension(".csv.gz")
        .file_compression_type(FileCompressionType::GZIP);

    // Read and decompress on the fly
    let df = ctx.read_csv(&path, options).await?;

    // Select a few columns and verify decompressed data
    let df = df.select_columns(&["c1", "c2", "c3"])?;
    # let df = df.limit(0, Some(3))?;
    let results = df.collect().await?;
    assert_batches_eq!(
        &[
            "+----+----+-----+",
            "| c1 | c2 | c3  |",
            "+----+----+-----+",
            "| c  | 2  | 1   |",
            "| d  | 5  | -40 |",
            "| b  | 1  | 29  |",
            "+----+----+-----+",
        ],
        &results
    );

    Ok(())
}
```

:::

## When to use CSVs

**CSV shines for human-readability, ingestion and interchange, but its lack of metadata makes it a poor choice for repeated analytical queries.**

CSV files carry no embedded schema, no column statistics, and no internal
structure beyond rows and delimiters. DataFusion cannot perform predicate
pushdown (skipping irrelevant row groups based on statistics). Every byte
of the file must be read from disk because row boundaries require parsing
all delimiters.

However, DataFusion **does** perform projection pushdown in memory: it
tells the CSV parser which columns to materialize, so only the selected
columns are allocated as Arrow arrays. On wide tables with many columns,
this saves significant RAM even though all bytes are still read from disk.

| CSV Shines ✓                                         | Avoid CSV ✗                                     |
| ---------------------------------------------------- | ----------------------------------------------- |
| Data exchange with spreadsheets, legacy systems      | Analytics on large datasets → Parquet           |
| Human inspection and quick debugging                 | Schema enforcement critical → Parquet/Avro      |
| One-off exports, universal compatibility             | Storage efficiency matters (5–10x larger)       |
| Small datasets where Parquet overhead isn't worth it | You need predicate pushdown (filter before I/O) |

:::{admonition} Register for repeated queries and SQL access
:class: tip

Use `ctx.register_csv("table_name", "path.csv", options)` to register the
CSV file as a named table in the `SessionContext` catalog. This enables:

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

**Always provide an explicit schema in production to avoid schema inconsistency and data drift failures.**

CSV files carry no embedded schema. DataFusion infers types by sampling a
limited number of rows at the beginning of the file (configurable via
`.schema_infer_max_records()`, default 1,000) and locks the column types
based solely on what it observes in that window.

:::{admonition} Schema Inference Problem
:class: caution

Schema inference from a finite sample is inherently
unreliable for heterogeneous data. Any type variation that first appears
_beyond_ the sample boundary (default: first 1,000 rows) produces a
`DataFusionError` at execution time (when you call `.collect()`).
Increasing the sample size only shifts the boundary — it never eliminates
the risk. The parser cannot coerce values that contradict the inferred
types.

**For example:** <br>
if the sampled rows for a column contain only integers,
DataFusion infers `Int64`. A later row containing `150.5` (a float),
`"N/A"` (an unrecognized null marker), or an empty field will fail to
parse. Within the sample, DataFusion _does_ handle some type coercion
(Int64 + Float64 widens to Float64), but once the schema is locked, it is
fixed.
:::

::::{admonition} Identifier casing pitfall
:class: warning

DataFusion follows the PostgreSQL convention: **unquoted identifiers are
folded to lowercase**. CSV headers preserve the original casing from the
source file (e.g., `Amount`, `firstName`). If you reference these columns
with `col("amount")` or in SQL as `SELECT amount`, DataFusion looks for a
lowercase `amount` — which won't match the header `Amount`.

**Workarounds:**

- **Double-quote** the identifier: `col("\"Amount\"")` or in SQL
  `SELECT "Amount"`
- **Provide an explicit schema** with lowercase field names — this
  normalizes casing at read time and eliminates the mismatch entirely

For more details, see [Schema Management](../Schema-Management/index.md).
::::

To guarantee safety, use [`CsvReadOptions::schema()`] to explicitly define the schema. This skips the inference scan (improving startup time) and enforces strict types. You can also use `.null_regex()` to define how missing values are represented in your specific dataset.

For more details on managing schemas across different sources, see [Schema Management](../Schema-Management/index.md).

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
    # // Create test CSV file
    # let dir = tempdir()?;
    # let csv_path = dir.path().join("data.csv");
    # let mut file = File::create(&csv_path)?;
    # writeln!(file, "id,name,amount")?;
    # writeln!(file, "1,Alice,150.50")?;
    # writeln!(file, "2,Bob,200.00")?;
    # writeln!(file, "3,Carol,75.25")?;

    // 1. Define the explicit schema
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("amount", DataType::Float64, true),
    ]);

    // 2. Configure options with the schema
    let options = CsvReadOptions::new().schema(&schema);

    // 3. Read CSV (skips inference scan)
    let path = "data.csv";
    # let path = csv_path.to_str().unwrap();
    let df = ctx.read_csv(path, options).await?;

    // Execute eagerly and verify results
    let results = df.collect().await?;
    assert_batches_sorted_eq!(
        &[
            "+----+-------+--------+",
            "| id | name  | amount |",
            "+----+-------+--------+",
            "| 1  | Alice | 150.5  |",
            "| 2  | Bob   | 200.0  |",
            "| 3  | Carol | 75.25  |",
            "+----+-------+--------+",
        ],
        &results
    );

    Ok(())
}
```

:::{admonition} schema() is a builder method
:class: note

[`CsvReadOptions::schema()`] _sets_ the expected schema for the data reader before the file is processed. This defines the contract for how DataFusion should parse the incoming bytes.

This differs from [`DataFrame::schema()`], which _returns_ the resolved `DFSchema` of an already-created DataFrame. The `DFSchema` contains the final types and column names after all inference, explicit definitions, and DataFrame transformations have been applied.
:::

## CSV References

- [`CsvReadOptions` API](https://docs.rs/datafusion/latest/datafusion/prelude/struct.CsvReadOptions.html) — All configuration options
- [CSV Format Options (SQL)](../../../../../user-guide/sql/format_options.md#csv-format-options) — SQL-level options for `CREATE EXTERNAL TABLE` and `COPY`
- [Example Usage (CSV with SQL and DataFrame)](../../user-guide/example-usage.md)
