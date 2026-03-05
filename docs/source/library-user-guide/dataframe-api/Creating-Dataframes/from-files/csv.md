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

### CSV — Tabular Exchange

<!--TODO

1. ABSTRACT
2. INTRODUCTION
-->

```{contents}
:local:
:depth: 2
```

**Simple, human-readable, universal—the lowest common denominator for data exchange.**

CSV is row-oriented text with no embedded schema. When you call `read_csv()`, DataFusion must:

1. **Infer schema** <br>
   By scanning the first N rows (default: 1000)—this happens _at DataFrame creation_, not lazily
2. **Parse text → typed Arrow columns** <br>
   Row by row—a row-to-columnar conversion cost

This makes CSV ideal for _ingestion and interchange_—receiving data from upstream systems ( like mainframes, HL7 feeds, legacy batch jobs, vendor exports) or quick spreadsheet imports. For repeated analytical queries, convert CSV to Parquet once and query Parquet thereafter.

```rust
use datafusion::prelude::*;
# use datafusion::assert_batches_sorted_eq;
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

    // Print the inferred schema
    println!("{}", df.schema());

    df.show().await?;
    // +----+-------+
    // | id | name  |
    // +----+-------+
    // | 1  | Alice |
    // | 2  | Bob   |
    // +----+-------+
    # // Re-read for assertion (show() consumes the DataFrame)
    # let df = ctx.read_csv(path, CsvReadOptions::new()).await?;
    # let results = df.select_columns(&["id", "name"])?.collect().await?;
    # assert_batches_sorted_eq!(
    #     &[
    #         "+----+-------+",
    #         "| id | name  |",
    #         "+----+-------+",
    #         "| 1  | Alice |",
    #         "| 2  | Bob   |",
    #         "+----+-------+",
    #     ],
    #     &results
    # );
    Ok(())
}
```

> **⚠️ Production Warning: Schema Inference**
> CSV files have no embedded schema—DataFusion infers types from the first 1000 rows. This can fail silently if row 1001 has a different type. **Always provide an explicit schema in production.** See [Schema Management](schema-management.md) for guidance.

**Practical considerations:**

- **No embedded schema** <br>
  DataFusion infers types from the first 1000 rows. Use `.schema_infer_max_records(n)` if early rows aren't representative, or provide an explicit schema in production.
- **No predicate pushdown** <br>
  Every byte must be read and parsed, even if filtered later.
- **Projection reduces CPU, not I/O** <br>
  DataFusion skips _materializing_ unused columns, but still scans the full file.
- **Compression** <br>
  Use `.csv.gz` or `.csv.zst` for transfer; DataFusion reads them directly via `.file_compression_type()`.

#### CSV Trade-offs

| CSV Shines ✓                                         | Avoid CSV ✗                                     |
| ---------------------------------------------------- | ----------------------------------------------- |
| Data exchange with spreadsheets, legacy systems      | Analytics on large datasets → Parquet           |
| Human inspection and quick debugging                 | Schema enforcement critical → Parquet/Avro      |
| One-off exports, universal compatibility             | Storage efficiency matters (5–10x larger)       |
| Small datasets where Parquet overhead isn't worth it | You need predicate pushdown (filter before I/O) |

#### CsvReadOptions

[`CsvReadOptions`] provides builder methods for customization. The most important ones for production use are [`CsvReadOptions::schema()`] (for explicit type control) and [`.delimiter()`][`csvreadoptions::delimiter()`] (for non-comma separators like TSV).

| Builder Method                                                                     | Default        | Usage                                                                                                                  |
| :--------------------------------------------------------------------------------- | :------------- | :--------------------------------------------------------------------------------------------------------------------- |
| **[`.has_header(bool)`][`csvreadoptions::has_header()`]**                          | `true`         | Treats first row as column names. Set `false` if file starts immediately with data.                                    |
| **[`.delimiter(u8)`][`csvreadoptions::delimiter()`]**                              | `b','`         | Field separator character. Use `b'\t'` for TSV or `b';'` for European CSV.                                             |
| **[`.schema(&Schema)`][`csvreadoptions::schema()`]**                               | `None`         | Explicit column names and types. **Recommended for production** to enforce strict types and avoid inference surprises. |
| **[`.schema_infer_max_records(n)`][`csvreadoptions::schema_infer_max_records()`]** | `1000`         | Rows to scan for type inference. Increase if first 1000 rows contain nulls in columns that later have data.            |
| **[`.quote(u8)`][`csvreadoptions::quote()`]**                                      | `b'"'`         | Character to quote fields containing delimiters. Use `b'\''` for single-quote dialects.                                |
| **[`.file_compression_type(...)`][`csvreadoptions::file_compression_type()`]**     | `UNCOMPRESSED` | Compression algorithm (GZIP, BZIP2, ZSTD). For reading `.csv.gz` or `.csv.zst` directly.                               |
| **[`.newlines_in_values(bool)`][`csvreadoptions::newlines_in_values()`]**          | `false`        | Allows `\n` inside quoted fields. **Warning**: Disables parallel file scanning (slower).                               |
| **[`.null_regex(str)`][`csvreadoptions::null_regex()`]**                           | `None`         | Treats specific strings (e.g., `"NA"`) as null. Use when data uses non-standard null markers.                          |
| **[`.file_extension(&str)`][`csvreadoptions::file_extension()`]**                  | `".csv"`       | Filters input files by suffix. Use to ignore metadata files in mixed directories.                                      |

> **Note:** [`CsvReadOptions::schema()`] here is a _builder method_ that sets the schema for reading. This differs from [`DataFrame::schema()`], which _returns_ the schema of an existing DataFrame.

<details>
<summary><strong>Example: Reading compressed CSV with explicit schema</strong></summary>

This example shows how to read a GZIP-compressed CSV file with an explicit schema—a common pattern for log ingestion pipelines.

```rust
use datafusion::prelude::*;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::datasource::file_format::file_compression_type::FileCompressionType;
# use std::path::PathBuf;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();

    // Path to GZIP-compressed CSV
    let path = "logs.csv.gz";
    # let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
    #     .join("testing/data/csv/aggregate_test_100.csv.gz")
    #     .to_string_lossy().to_string();

    // Define schema upfront (skips inference, enforces types)
    let schema = Schema::new(vec![
        Field::new("c1", DataType::Utf8, true),
        Field::new("c2", DataType::Int64, true),
        Field::new("c3", DataType::Int64, true),
    ]);

    // Configure reader for GZIP compressed files
    let options = CsvReadOptions::new()
        .schema(&schema)
        .file_extension(".csv.gz")
        .file_compression_type(FileCompressionType::GZIP);

    // Read — DataFusion decompresses on the fly
    let df = ctx.read_csv(&path, options).await?;

    df.show().await?;
    Ok(())
}
```

</details>

#### CSV Production Tips

- **Always provide explicit schema** <br>
  Schema inference is risky: if row 1001 has a different type than rows 1–1000, your query fails at runtime.
- **Use `.csv.gz`** for compressed transfer — DataFusion decompresses on the fly
- **Set `.null_regex("NA|NULL|\\N")`** to handle common null markers
- **Watch for empty strings vs nulls** — They're treated differently

<details>
<summary><strong>Example: Explicit schema for production</strong></summary>

```rust
use datafusion::prelude::*;
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

    let schema = Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("amount", DataType::Float64, true),
    ]);

    // Read CSV with explicit schema
    let path = "data.csv";
    # let path = csv_path.to_str().unwrap();
    let df = ctx.read_csv(path, CsvReadOptions::new().schema(&schema)).await?;

    df.show().await?;
    Ok(())
}
```

</details>

#### CSV References

- [`CsvReadOptions` API](https://docs.rs/datafusion/latest/datafusion/prelude/struct.CsvReadOptions.html) — All configuration options
- [Example Usage (CSV with SQL and DataFrame)](../../user-guide/example-usage.md)
