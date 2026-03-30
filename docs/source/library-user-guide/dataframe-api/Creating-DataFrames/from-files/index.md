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

**File-based DataFrames: from disk or cloud storage to lazy query plan
through [`ListingTable`].**

Files are the most common entry point for data analysis—data lakes, ETL
exports, local CSV dumps, cloud-hosted Parquet datasets. DataFusion reads
all supported file formats through [`ListingTable`], a built-in
[`TableProvider`] that handles path resolution, schema discovery, and
metadata-level optimizations behind a simple `read_<format>()` API. The
result is always a lazy [`DataFrame`]: metadata is read at creation time,
but actual data processing waits until you trigger an action.

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


```{toctree}
:maxdepth: 1
:caption: DataFusion supported file formats

parquet.md
csv.md
json.md
avro.md
arrow-ipc.md
```

## Choosing a File Format

**Storage layout—columnar vs. row-oriented—determines how much data
DataFusion must read, which directly affects query performance.**

Not all file formats are equal in a query engine. Columnar formats like
Parquet embed metadata (schemas, statistics, row counts) that enable
pushdown—DataFusion reads only the columns and row groups a query actually
needs. Row-based formats (CSV, JSON) typically carry no metadata, so
DataFusion must infer the schema by sampling rows at startup, and pushdown
beyond partition pruning is not possible. This adds startup cost and can be
a source of type-inference errors.

DataFusion natively supports five file formats. Other formats (ORC, Iceberg,
Delta Lake, ...) are available via [ecosystem extensions](../ecosystem-sources.md).

| Format                                              | Layout   | Schema Source                | Startup Cost                | Pruning Support       | Best For                         |
| --------------------------------------------------- | -------- | ---------------------------- | --------------------------- | --------------------- | -------------------------------- |
| **[Parquet](parquet.md)** <br>[`.read_parquet()`]   | Columnar | Embedded (footer)            | **Low** (metadata only)     | ✅ Metadata + Columns | Production analytics, large data |
| **[CSV](csv.md)** <br>[`.read_csv()`]               | Row      | ⚠️ **Inferred** (first 1000) | **High** (inference scan)   | ❌ Partition only     | Simple exchange, imports         |
| **[NDJSON](json.md)** <br>[`.read_json()`]          | Row      | ⚠️ **Inferred** (first 1000) | **Medium** (inference scan) | ❌ Partition only     | Semi-structured logs/APIs        |
| **[Avro](avro.md)** <br>[`.read_avro()`]            | Row      | Embedded (header)            | **Low** (header schema)     | ❌ Partition only     | Kafka, schema evolution          |
| **[Arrow IPC](arrow-ipc.md)** <br>[`.read_arrow()`] | Columnar | Embedded (header)            | **Very Low** (zero-copy\*)  | ❌ Partition only     | Arrow ecosystem, zero-copy       |

\* Arrow IPC files already use Arrow's in-memory columnar layout, so
DataFusion can map the data directly without deserialization — hence
"zero-copy" for local, uncompressed files. Compressed or remote IPC still
requires a decompression or network copy.

:::{admonition} Prefer columnar formats for analytics
:class: tip

Columnar storage (Parquet, Arrow IPC) lets DataFusion read only the columns
your query needs, drastically reducing I/O. Row-based formats (CSV, JSON,
Avro) must read entire rows even when you need a single field. When you
control the storage format, **Parquet is the default recommendation**.
:::

## How File Reading Works

**Every `read_<format>()` call internally creates a [`ListingTable`]—the
built-in [`TableProvider`] that bridges files to DataFusion's query engine.**

When you call a convenience method like `.read_parquet()` or `.read_csv()`,
DataFusion constructs a [`ListingTable`] configured for that format. The
[`ListingTable`] handles format-specific concerns—path resolution, schema
inference, partition discovery, and predicate pushdown against file
metadata—so the query engine never reads more data than necessary. The
result is a lazy [`DataFrame`]: a [`LogicalPlan`] paired with a
[`SessionState`] clone, ready for transformations.

File-based DataFrames reach the [`SessionContext`] through one of two access
patterns:

- **Ephemeral (direct read):** Methods like `.read_parquet()` and
  `.read_csv()` return a DataFrame immediately. The [`ListingTable`] exists
  only inside that DataFrame's plan—it is not stored in the catalog and
  cannot be referenced by SQL or other queries.
- **Named (registration):** Methods like `.register_parquet()` and
  `.register_csv()` store the [`ListingTable`] in the catalog under a name
  you choose. The source becomes queryable by both the DataFrame API
  (`ctx.table("sales")`) and SQL (`SELECT * FROM sales`).

**For more details:** See the [Creating DataFrames](../index.md) section.

:::{admonition} Rule of thumb
:class: tip

Use direct reads for ad-hoc exploration; register for production pipelines
or multi-query workloads.
:::

### General pattern for reading files

All `read_<format>()` methods share the same API shape — a path and
format-specific options:

| Parameter   | Type                  | Description                                                                   |
| ----------- | --------------------- | ----------------------------------------------------------------------------- |
| `path`      | `impl DataFilePaths`  | Single file, `Vec<&str>`, glob pattern, or cloud URL (`s3://...`)             |
| `options`   | `<Format>ReadOptions` | Format-specific configuration (schema override, compression, delimiter, etc.) |
| **Returns** | `Result<DataFrame>`   | Lazy DataFrame backed by a [`ListingTable`]                                   |

As an example, here's how to read a Parquet file—the simplest case because
Parquet embeds its schema in the file footer (no inference needed):

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

### What happens at creation time

**File scans are lazy—no record batches are read until an action—but
DataFusion does perform some I/O when creating the DataFrame.**

- **Parquet / Arrow IPC / Avro:** Reads file footers or headers to fetch the
  embedded schema. This is fast and typically a single small read per file.
- **CSV / NDJSON:** Reads the first 1,000 rows (configurable) to **infer**
  the schema, since these formats carry **no embedded type information**.
- **Statistics:** When
  [`datafusion.execution.collect_statistics = true`][`executionoptions::collect_statistics`]
  (the default), DataFusion may also read file sizes and row counts from
  metadata.

Actual data processing—filtering, joining, aggregating—happens only when
you execute an action like [`.collect()`] or [`.show()`].

---

## Reading Multiple Files

**DataFusion reads multiple files as a single logical dataset—schemas are
unified and the query engine parallelizes across files automatically.**

Real-world datasets are rarely a single file. Data lakes partition by date
or region, ETL pipelines produce sharded outputs, and time-series data
accumulates daily files. All file readers accept multiple paths, glob
patterns, or cloud URL prefixes and merge matching files into one
[`DataFrame`].

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

All file readers support the same path patterns:

| Pattern        | Syntax                                                                         | Example                                    |
| -------------- | ------------------------------------------------------------------------------ | ------------------------------------------ |
| Single file    | `"path/to/file"`                                                               | `"data.parquet"`                           |
| Multiple files | `vec!["a", "b"]`                                                               | `vec!["part-0.parquet", "part-1.parquet"]` |
| Glob           | `"pattern"` with `*` or `**`                                                   | `"data/**/*.parquet"`                      |
| Cloud URL      | `"scheme://bucket/prefix"` (after [object store registration](#cloud-storage)) | `"s3://bucket/data/*.parquet"`             |

:::{admonition} Subdirectories are ignored by default
:class: note

If you provide a directory path like `ctx.read_parquet("data/", ...)`,
DataFusion scans only that directory level. It does **not** recursively
scan subdirectories unless
[`datafusion.execution.listing_table_ignore_subdirectory = false`][`executionoptions::listing_table_ignore_subdirectory`]
or you use a recursive glob like `data/**/*.parquet`.
:::

## Cloud Storage

**DataFusion reads cloud-hosted files through object store URLs, but cloud connectors are not bundled by default.**

(Cloud) object storage—Amazon S3, Google Cloud Storage, Azure Blob Storage,
or S3-compatible stores like MinIO—is a common home for analytical data.
DataFusion supports reading from these stores using the same
`read_<format>()` methods, but you must first register an [`ObjectStore`]
implementation that handles authentication and network access for your
provider.

:::{admonition} Quick Start: S3 Registration
:class: seealso
:collapsible: closed

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
    // https://github.com/apache/datafusion/blob/main/datafusion-examples/examples/external_dependency/query_aws_s3.rs
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

:::

For a complete S3 setup (credentials, builder configuration, query), see
the [S3 example in datafusion-examples](https://github.com/apache/datafusion/blob/main/datafusion-examples/examples/external_dependency/query_aws_s3.rs).

## Advanced: ListingTable and read_table()

**Build a [`ListingTable`] directly when the `read_<format>()` convenience
methods don't offer enough control.**

The convenience methods (`.read_parquet()`, `.read_csv()`, etc.) cover the
common case, but they hide the underlying [`ListingTable`] configuration.
For scenarios that require more control, construct a [`ListingTable`]
yourself and pass it to [`SessionContext::read_table()`]:

- **Schema override:** Provide an explicit schema instead of relying on
  inference (important for CSV/JSON in production).
- **Custom file extensions:** Scan files with non-standard extensions
  (e.g., `.dat` files that are actually Parquet).
- **Partition columns:** Define Hive-style partition columns manually.
- **Multiple table paths:** Combine files from different directories into
  one logical table.

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

---

[`executionoptions::listing_table_ignore_subdirectory`]: https://docs.rs/datafusion/latest/datafusion/config/struct.ExecutionOptions.html#structfield.listing_table_ignore_subdirectory
[`executionoptions::collect_statistics`]: https://docs.rs/datafusion/latest/datafusion/config/struct.ExecutionOptions.html#structfield.collect_statistics

<!-- Link references -->
