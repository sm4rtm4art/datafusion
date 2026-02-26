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

# Creating DataFrames from Files

<!--TODO

1. ABSTRACT
2. INTRODUCTION
3. OVERVIEW TABLE
4. GENERAL REFERENCES
-->

```{toctree}
:maxdepth: 1
:caption: Supported Formats

Arrow
Avro
CSV
JSON
Parquet
```

## Introduction (Placeholder)

**Read files directly into a lazy `DataFrame`. Format choice determines optimization potential—Parquet enables metadata pruning; text formats generally require reading full files (except for partition pruning).**

Files are a common entry point—data lakes, ETL pipelines, local analysis—but DataFusion's strength is **fusion**: the same query can join a Parquet file with a PostgreSQL table or a streaming source. This section covers file-based access.

File scans are lazy (no record batches are read until an action), but DataFusion may read **metadata** (and for CSV/NDJSON a **sample** for schema inference) when creating the DataFrame.

| Parameter   | Type                  | Description                                                       |
| ----------- | --------------------- | ----------------------------------------------------------------- |
| `path`      | `impl DataFilePaths`  | Single file, `Vec<&str>`, glob pattern, or cloud URL (`s3://...`) |
| `options`   | `<Format>ReadOptions` | Format-specific configuration (schema, compression, etc.)         |
| **Returns** | `Result<DataFrame>`   | Lazy DataFrame                                                    |

> **What happens immediately?** <br>
> Creating the DataFrame is not strictly "lazy" for all I/O:
>
> - **CSV/JSON:** Reads the first 1,000 rows (default) to infer schema.
> - **Parquet/Arrow/Avro:** Reads file footers/headers to fetch schema.
> - **Statistics:** By default ([`datafusion.execution.collect_statistics = true`][`executionoptions::collect_statistics`]), DataFusion may read file sizes and row counts from metadata.
>
> Actual _data_ processing (filtering, joining, aggregating) happens only when you execute an action.

As an example, here's how to read a Parquet file:

```rust
use datafusion::prelude::*;
# use std::path::PathBuf;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();

    // Path to your Parquet file (local or object-store URL like s3://...)
    let path = "data.parquet";
    # // Hidden: use test data for doctests
    # let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
    #     .join("parquet-testing/data/alltypes_plain.parquet")
    #     .to_string_lossy().to_string();

    // Read a Parquet file — lazy scan, nothing loads until an action
    let df = ctx.read_parquet(&path, ParquetReadOptions::default()).await?;

    df.show().await?;
    Ok(())
}
```

**Warning: Cloud Storage (S3, GCS, Azure)** <br>
DataFusion does not bundle cloud connectors by default. To use `s3://`, `gs://`, or `az://` paths, you must first register the corresponding `ObjectStore` with your `SessionContext`.

<details>
<summary><strong>Quick Start: S3 Registration</strong></summary>

```rust,no_run
use std::sync::Arc;

use datafusion::error::Result;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::object_store::ObjectStore;
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> Result<()> {
    // no_run: requires configuring a cloud object store (credentials, network, etc.)
    let ctx = SessionContext::new();

    // Create an object store implementation for your cloud provider.
    // For a complete S3 setup (AmazonS3Builder, credentials), see:
    // https://github.com/apache/datafusion/blob/main/datafusion-examples/examples/external_dependency/query-aws-s3.rs
    let store: Arc<dyn ObjectStore> = todo!();

    // Register `s3://<bucket>` (or `gs://...`, `az://...`) before reading.
    let url = ObjectStoreUrl::parse("s3://my-bucket")?;
    ctx.runtime_env().register_object_store(url.as_ref(), store);

    let df = ctx
        .read_parquet("s3://my-bucket/data.parquet", ParquetReadOptions::default())
        .await?;

    // Execute with an action such as:
    // df.collect().await?;
    Ok(())
}
```

</details>

See [**Advanced: Custom Data Sources**](dataframes-advance.md#custom-data-sources) for setup details.

---

## Reading Multiple Files

**DataFusion can read multiple files as a single DataFrame using explicit paths or glob patterns—this applies to all file formats.**

```rust
use datafusion::prelude::*;
# use std::path::PathBuf;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();
    # let test_data = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
    #     .join("parquet-testing/data");

    // Pattern 1: Multiple explicit paths (Vec)
    let paths = vec!["data/part-000.parquet", "data/part-001.parquet"];
    # let paths: Vec<String> = vec![
    #     test_data.join("alltypes_plain.parquet").to_string_lossy().to_string(),
    #     test_data.join("alltypes_plain.snappy.parquet").to_string_lossy().to_string(),
    # ];
    let df = ctx.read_parquet(paths, ParquetReadOptions::default()).await?;

    // Pattern 2: Glob patterns
    let glob_pattern = "data/*.parquet";
    # let glob_pattern = test_data.join("alltypes*.parquet").to_string_lossy().to_string();
    let df = ctx.read_parquet(&glob_pattern, ParquetReadOptions::default()).await?;

    // Pattern 3: Cloud storage (after registering object store)
    // let df = ctx.read_parquet("s3://bucket/data/*.parquet", ...).await?;

    Ok(())
}
```

> **Cloud storage**:<br>
> Register an object store before using `s3://`, `gs://`, or `az://` paths. See the [S3 setup example](https://github.com/apache/datafusion/blob/main/datafusion-examples/examples/external_dependency/query-aws-s3.rs) for complete configuration (credentials, registration, query).

All file readers support the same path patterns:

- **Single file:** `"data.parquet"`
- **Multiple files:** `vec!["a.parquet", "b.parquet"]`
- **Glob patterns:** `"data/**/*.parquet"` (recursive), `"data/*.csv"` (single directory)
- **Cloud URLs:** `"s3://bucket/prefix/*.parquet"` (after object store registration)

> **Note: Subdirectories are ignored by default** <br>
> If you provide a directory path like [`ctx.read_parquet("data/")`][`.read_parquet()`], DataFusion scans only that directory level. It does **not** recursively scan subdirectories unless [`datafusion.execution.listing_table_ignore_subdirectory = false`][`executionoptions::listing_table_ignore_subdirectory`] or you use a recursive glob like `data/**/*.parquet`.

<details>
<summary><strong>Advanced: ListingTable + read _table() for more control</strong></summary>

The `read_<format>()` helpers (such as `read_parquet()`, `read_csv()`, `read_json()`) are the simplest way to scan files. If you need more control (custom `ListingOptions`, multi-path tables, schema management, etc.), build a `ListingTable` and then create a `DataFrame` with `SessionContext::read_table()`.

```rust
use datafusion::prelude::*;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::{ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl};
use std::sync::Arc;
# use std::path::PathBuf;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();

    // Directory containing Parquet files
    let path = "data/";
    # let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
    #     .join("parquet-testing/data")
    #     .to_string_lossy().to_string();
    let table_path = ListingTableUrl::parse(&path)?;
    let listing_options = ListingOptions::new(Arc::new(ParquetFormat::default()));

    let config = ListingTableConfig::new(table_path)
        .with_listing_options(listing_options)
        .infer_schema(&ctx.state())
        .await?;

    let provider = Arc::new(ListingTable::try_new(config)?);
    let df = ctx.read_table(provider)?;

    df.show().await?;
    Ok(())
}
```

</details>

---

[`executionoptions::listing_table_ignore_subdirectory`]: https://docs.rs/datafusion/latest/datafusion/config/struct.ExecutionOptions.html#structfield.listing_table_ignore_subdirectory
[`executionoptions::collect_statistics`]: https://docs.rs/datafusion/latest/datafusion/config/struct.ExecutionOptions.html#structfield.collect_statistics

### Choosing a File Format

DataFusion natively supports five file formats (other formats like ORC or Iceberg are available via [extensions](https://datafusion.apache.org/library-user-guide/extensions.html)). Storage layout (columnar vs row-oriented) strongly affects query performance:

| Format                                                   | Layout   | Schema Source                | Startup Cost                | Pruning Support       | Best For                         |
| -------------------------------------------------------- | -------- | ---------------------------- | --------------------------- | --------------------- | -------------------------------- |
| **[Parquet][parquet-section]** <br>[`.read_parquet()`]   | Columnar | Embedded (footer)            | **Low** (metadata only)     | ✅ Metadata + Columns | Production analytics, large data |
| **[CSV][csv-section]** <br>[`.read_csv()`]               | Row      | ⚠️ **Inferred** (first 1000) | **High** (inference scan)   | ❌ Partition only     | Simple exchange, imports         |
| **[NDJSON][ndjson-section]** <br>[`.read_json()`]        | Row      | ⚠️ **Inferred** (first 1000) | **Medium** (inference scan) | ❌ Partition only     | Semi-structured logs/APIs        |
| **[Avro][avro-section]** <br>[`.read_avro()`]            | Row      | Embedded (header)            | **Low** (header schema)     | ❌ Partition only     | Kafka, schema evolution          |
| **[Arrow IPC][arrow-ipc-section]** <br>[`.read_arrow()`] | Columnar | Embedded (header)            | **Very Low** (zero-copy\*)  | ❌ Partition only     | Arrow ecosystem, zero-copy       |

<!-- Section links -->

[parquet-section]: #parquet--the-analytical-standard
[csv-section]: #csv--tabular-exchange
[ndjson-section]: #ndjson--semi-structured-logs
[avro-section]: #avro--schema-evolution
[arrow-ipc-section]: #arrow-ipc--zero-copy-native

> For analytics, prefer **columnar formats** (Parquet, Arrow IPC). Columnar storage lets DataFusion read only the needed columns, drastically reducing I/O. Row-based formats (Avro, CSV, JSON) must read entire rows even when you need one field.
