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

### NDJSON — Semi-Structured Logs

<!--TODO

1. ABSTRACT
2. INTRODUCTION
-->

```{contents}
:local:
:depth: 2
```

**Newline-delimited JSON: one JSON object per line, ideal for logs and streaming data.**

NDJSON (also called JSON Lines, `.jsonl`) is row-oriented text like CSV, but each line is a self-describing JSON object. When you call [`.read_json()`], DataFusion must:

1. **Infer schema** <br>
   By scanning the first N objects (default: 1000)—this happens _at DataFrame creation_, not lazily
2. **Parse JSON → typed Arrow columns** <br>
   Object by object—nested structures flatten to Arrow structs

This makes NDJSON ideal for _data interchange_—log files, NoSQL database exports (MongoDB, Elasticsearch), streaming APIs, and message queue payloads. For repeated analytical queries, convert to Parquet.

```rust
use datafusion::prelude::*;
# use datafusion::assert_batches_sorted_eq;
# use std::fs::File;
# use std::io::Write;
# use tempfile::tempdir;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();
    # // Create test NDJSON file (one JSON object per line)
    # let dir = tempdir()?;
    # let json_path = dir.path().join("logs.ndjson");
    # let mut file = File::create(&json_path)?;
    # writeln!(file, r#"{{"num":5,"str":"test"}}"#)?;
    # writeln!(file, r#"{{"num":2,"str":"hello"}}"#)?;
    # writeln!(file, r#"{{"num":4,"str":"foo"}}"#)?;

    // Read NDJSON (schema is inferred immediately at DataFrame creation)
    let path = "logs.ndjson";
    # let path = json_path.to_str().unwrap();
    # // Use .ndjson extension to match temp file
    let df = ctx.read_json(path, NdJsonReadOptions::default()
        .file_extension(".ndjson")
    ).await?;

    df.show().await?;
    # // Re-read for assertion (show() consumes the DataFrame)
    # let df = ctx.read_json(path, NdJsonReadOptions::default().file_extension(".ndjson")).await?;
    # let results = df.select_columns(&["str", "num"])?.collect().await?;
    # assert_batches_sorted_eq!(
    #     &[
    #         "+-------+-----+",
    #         "| str   | num |",
    #         "+-------+-----+",
    #         "| foo   | 4   |",
    #         "| hello | 2   |",
    #         "| test  | 5   |",
    #         "+-------+-----+",
    #     ],
    #     &results
    # );
    Ok(())
}
```

> **⚠️ Production Warning: Schema Inference**
> NDJSON files have no embedded schema—DataFusion infers types from the first 1000 objects. Deeply nested or sparse fields may not be detected. **Always provide an explicit schema in production.** See [Schema Management](schema-management.md) for guidance.

**Practical considerations:**

- **Schema inference scans first 1000 objects** <br>
  Deeply nested or sparse fields may not be detected. Provide an explicit [`.schema()`][ndjsonreadoptions::schema()] in production.
- **Nested objects flatten to Arrow structs** <br>
  `{"user": {"name": "Alice"}}` becomes a struct column accessible as `user.name`.
- **No predicate pushdown** <br>
  Every line must be parsed, even if filtered later—similar to CSV.
- **File extension matters** <br>
  Use `.file_extension(".jsonl")` or `.file_extension(".ndjson")` if your files don't end in `.json`.

#### Trade-offs

| NDJSON shines ✓                             | Avoid NDJSON ✗                                      |
| ------------------------------------------- | --------------------------------------------------- |
| Data interchange: logs, APIs, NoSQL exports | Production analytics on large datasets → Parquet    |
| Semi-structured / evolving records          | Highly selective queries needing predicate pushdown |
| Append-friendly, easy to generate           | Strict schema contracts → Avro or Parquet           |

#### NdJsonReadOptions

The [`NdJsonReadOptions`] builder configures the parser.

| Builder Method                                                                    | Default        | Usage                                                                                                  |
| :-------------------------------------------------------------------------------- | :------------- | :----------------------------------------------------------------------------------------------------- |
| **[`.schema(&Schema)`][`ndjsonreadoptions::schema()`]**                           | `None`         | Explicit schema. **Recommended for production** to enforce strict types and avoid inference surprises. |
| **[`.file_extension(&str)`][`ndjsonreadoptions::file_extension()`]**              | `".json"`      | Filters input files by suffix. Use `".jsonl"` or `".ndjson"` for non-standard extensions.              |
| **[`.file_compression_type(...)`][`ndjsonreadoptions::file_compression_type()`]** | `UNCOMPRESSED` | Compression algorithm. For reading `.json.gz` or `.json.zst` directly.                                 |
| **[`.table_partition_cols(Vec)`][`ndjsonreadoptions::table_partition_cols()`]**   | `[]`           | Maps Hive-style directory paths to columns (e.g., `year=2024/month=01/`).                              |

> **Note:** To change schema inference depth (default: 1000 objects), set the field directly: <br> >
> `NdJsonReadOptions { schema_infer_max_records: 5000, ..Default::default() }`

> **Note:**<br> > [`NdJsonReadOptions::schema()`] here is a _builder method_ that sets the schema for reading. This differs from [`DataFrame::schema()`], which _returns_ the schema of an existing DataFrame.

<details>
<summary><strong>Example: Reading compressed NDJSON</strong></summary>

```rust
use datafusion::prelude::*;
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
        .file_extension(".gz"); // Important: match the actual file extension

    // Path to compressed NDJSON file(s)
    let path = "logs/*.json.gz";
    # // Hidden: create compressed test data for doctests
    # let mut temp_file = NamedTempFile::with_suffix(".json.gz").unwrap();
    # {
    #     let mut encoder = GzEncoder::new(&mut temp_file, Compression::default());
    #     writeln!(encoder, r#"{{"id": 1, "name": "Alice"}}"#).unwrap();
    #     writeln!(encoder, r#"{{"id": 2, "name": "Bob"}}"#).unwrap();
    #     encoder.finish().unwrap();
    # }
    # let path = temp_file.path().to_string_lossy().to_string();

    let df = ctx.read_json(&path, options).await?;
    df.show().await?;
    Ok(())
}
```

</details>

---
