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

# Parquet — The Analytical Standard

**Parquet — the default choice for analytical workloads: columnar, compressed,
self-describing, and optimized for selective reads.**

[Apache Parquet][apache-parquet] stores data
**column-by-column** instead of row-by-row. This layout lets DataFusion
read only the columns your query needs, skip irrelevant row groups using
embedded statistics, and benefit from excellent compression ratios. If you
control the storage format and are unsure which to choose, **start with
Parquet** — its metadata-driven optimizations compound as data volume grows.

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

## Reading Parquet Files

**A single call to [`.read_parquet()`] creates a lazy [`DataFrame`] backed by
Parquet's embedded metadata — no data is loaded until you execute an
action.**

Reading a Parquet file is the simplest file-based entry point in DataFusion.
Because Parquet embeds its schema in the file footer, DataFusion needs no
inference step — it reads a few kilobytes of metadata and immediately returns
a lazy [`DataFrame`] ready for transformations. Pass a local path, a glob
pattern, or a cloud URL (after [object store registration][from-files-index])
along with [`ParquetReadOptions`] to configure the read.

```rust
use datafusion::prelude::*;
use datafusion::assert_batches_eq;
# use std::path::PathBuf;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();

    // Path to your Parquet file
    let path = "data.parquet";
    # let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
    #     .join("../../parquet-testing/data/alltypes_plain.parquet")
    #     .to_string_lossy().to_string();

    // Build lazy plan from Parquet metadata
    let df = ctx.read_parquet(&path, ParquetReadOptions::default()).await?;

    // Select specific columns and execute eagerly
    let df = df.select_columns(&["id", "bool_col", "tinyint_col"])?;
    # let df = df.limit(0, Some(3))?;
    let results = df.collect().await?;
    assert_batches_eq!(
        &[
            "+----+----------+-------------+",
            "| id | bool_col | tinyint_col |",
            "+----+----------+-------------+",
            "| 4  | true     | 0           |",
            "| 5  | false    | 1           |",
            "| 6  | true     | 0           |",
            "+----+----------+-------------+",
        ],
        &results
    );

    Ok(())
}
```

For typical analytical queries, Parquet is often **significantly faster**
than CSV — not because of raw read speed, but because DataFusion can skip
entire row groups that don't match your filters and avoid reading columns
your query doesn't reference. These optimizations are automatic and require
no user configuration.

## ParquetReadOptions

**[`ParquetReadOptions`] configures how DataFusion reads Parquet files —
from pruning behavior to schema enforcement — using a builder pattern.**

As a self-describing format with schema and statistics embedded in the
footer, most Parquet defaults work out of the box. The builder methods
below let you tune file discovery, enforce schemas across multi-file
datasets, and control pruning behavior. For basic reads,
[`ParquetReadOptions::default()`][`parquetreadoptions`] is sufficient.

| Builder Method                                                                         | Default                        | Usage                                                                                                                           |
| :------------------------------------------------------------------------------------- | :----------------------------- | :------------------------------------------------------------------------------------------------------------------------------ |
| **[`.file_extension(&str)`][`parquetreadoptions::file_extension()`]**                  | `".parquet"`                   | Filters input files by suffix. Use when folders contain mixed files (`.crc`, `.json`, temp files).                              |
| **[`.table_partition_cols(Vec)`][`parquetreadoptions::table_partition_cols()`]**       | `[]`                           | Maps Hive-style directory paths to columns (e.g., `year=2025/`). Use when data is organized in folders by date/category.        |
| **[`.parquet_pruning(bool)`][`parquetreadoptions::parquet_pruning()`]**                | `true`                         | Skips row groups using min/max statistics. Keep enabled for filtered queries (`WHERE id > 100`).                                |
| **[`.skip_metadata(bool)`][`parquetreadoptions::skip_metadata()`]**                    | `true`                         | Ignores embedded schema metadata to avoid conflicts. Keep `true` for mixed producers; set `false` only if you rely on metadata. |
| **[`.schema(&Schema)`][`parquetreadoptions::schema()`]**                               | `None`                         | Supplies the Parquet _file_ schema. Use for production to enforce types and avoid schema-merging surprises across many files.   |
| **[`.file_sort_order(Vec)`][`parquetreadoptions::file_sort_order()`]**                 | `[]`                           | Tells the optimizer the data is pre-sorted. Use to speed up merge-joins or `ORDER BY` queries without re-sorting.               |
| **`.file_decryption_properties(Option)`**                                              | `None`                         | Decryption configuration for Parquet [modular encryption][parquet-encryption]. Advanced.                                        |
| **[`.metadata_size_hint(Option<usize>)`][`parquetreadoptions::metadata_size_hint()`]** | `None` (session: 512&nbsp;KiB) | Size hint for the initial footer I/O. See note below.                                                                           |

:::{admonition} metadata_size_hint and footer fetching
:class: note
:collapsible: closed

The per-read default for [`.metadata_size_hint()`][`parquetreadoptions::metadata_size_hint()`] is `None`, which defers to
the session setting [`datafusion.execution.parquet.metadata_size_hint`][parquet-metadata-size-hint]
(default: 512 KiB). This tail read is large enough to capture the complete
footer of most Parquet files in a single I/O request — eliminating one
network round trip on cloud object stores. Override only if your files have
unusually large footers (e.g., tables with hundreds of columns).
:::

:::{admonition} schema() is a builder method
:class: note

[`ParquetReadOptions::schema()`] _sets_ the schema for reading. This differs
from [`DataFrame::schema()`], which _returns_ the schema of an existing
DataFrame.
:::

:::{admonition} Example: ParquetReadOptions builder pattern
:class: seealso
:collapsible: open

The following example demonstrates how to configure [`ParquetReadOptions`]
with various builder methods. For Hive-partitioned data
(e.g., `year=2024/month=01/`), use [`.table_partition_cols()`][`parquetreadoptions::table_partition_cols()`] to map
directory structure to columns.

```rust
use datafusion::prelude::*;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
# use std::path::PathBuf;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();

    // 1. Define the Parquet *file* schema (optional, but recommended for production).
    let file_schema = Schema::new(vec![Field::new("id", DataType::Int32, true)]);

    // 2. Configure the reader with builder methods
    let options = ParquetReadOptions::default()
        .file_extension(".parquet")   // Filter files by extension
        .parquet_pruning(true)        // Enable statistics-based pruning
        .schema(&file_schema);        // Enforce file schema

    // For Hive-partitioned directories (data/sales/year=2024/month=01/*.parquet),
    // add: .table_partition_cols(vec![("year".into(), DataType::Int32), ...])

    // 3. Read Parquet file(s)
    let path = "data/sales/";
    # // Hidden: use test data for doctests
    # let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
    #     .join("../../parquet-testing/data/alltypes_plain.parquet")
    #     .to_string_lossy().to_string();
    # // Use default options for test data (no partition columns)
    # let options = ParquetReadOptions::default();

    let df = ctx.read_parquet(&path, options).await?;
    df.show().await?;
    Ok(())
}
```

:::

## When to Use Parquet

**Parquet excels at read-heavy analytical workloads where selective access
and compression matter most.**

Parquet's columnar layout and embedded statistics make it the natural choice
when queries touch a subset of columns or filter large datasets. However,
the format is write-once and not [human-readable][parquet-viewer], so it's
not ideal for every use case.

| Parquet shines                                         | Consider alternatives                                             |
| ------------------------------------------------------ | ----------------------------------------------------------------- |
| Production analytics, repeated queries, large datasets | Write-heavy append logs → NDJSON / streaming-native systems       |
| Selective reads (filters + column pruning)             | Human-editable debugging → CSV/JSON                               |
| Efficient storage (compression + columnar layout)      | Very small datasets where metadata/compression overhead dominates |

:::{admonition} Register for repeated queries and SQL access
:class: tip

Use [`.register_parquet()`] to
register the Parquet file as a named table in the [`SessionContext`] catalog.
This enables:

- **SQL access** — query the table via [`.sql()`]
- **Cross-query reuse** — multiple DataFrame operations and SQL queries
  can reference the same table name without re-reading options or paths
  :::

## Production Best Practices

**Parquet embeds its schema in the file footer — no inference scan, no
sampling surprises. Production tuning focuses on metadata handling and
multi-file consistency, not type guessing.**

Parquet files are self-describing: they embed an Arrow-compatible schema
and statistical metadata (min/max values, null counts) in the file footer.
DataFusion reads both during [`.read_parquet()`], making query planning
predictable and performant — no inference scan, no type guessing,
and immediate access to row group statistics for pruning.

For background on the Parquet file structure (footer, row groups, column
chunks, page layout), see the
[Apache Parquet File Format][parquet-file-format]
documentation.

### DataFrame creation with `.read_parquet()`

The structure of Parquet files enables efficient processing through
embedded schema and metadata; the following happens when [`.read_parquet()`]
is called:

1. **Footer read** — DataFusion reads the Parquet footer (~few KB) to
   learn the schema and row group metadata. By default, the engine fetches
   the last 512 KiB of the file in a single I/O request, which is enough
   to capture the complete footer for most files.
2. **Plan creation** — Creates a [`ListingTable`] and returns a lazy
   [`DataFrame`]. No column data is loaded yet.

:::{admonition} Startup cost for many files on cloud object stores
:class: note

For directories with many files on cloud object stores, DataFusion
collects per-file statistics at planning time
([`datafusion.execution.collect_statistics`][collect-statistics] = true). For local files or
small datasets this is negligible. For hundreds+ of remote files, disable
it to reduce startup latency:
[`SessionConfig::with_collect_statistics()`] (`SessionConfig::new().with_collect_statistics(false)`).
:::

### Explicit schemas for multi-file datasets

When reading directories of Parquet files from different producers or
evolving pipelines, schema mismatches across files can cause failures.
Provide an explicit schema via [`.schema()`][`parquetreadoptions::schema()`] to enforce consistent types and
avoid schema-merging surprises:

```rust
use datafusion::prelude::*;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
# use std::path::PathBuf;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();

    // Lock the schema across all files in the directory
    let schema = Schema::new(vec![Field::new("id", DataType::Int32, true)]);
    let options = ParquetReadOptions::default().schema(&schema);

    let path = "data/events/";
    # let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
    #     .join("../../parquet-testing/data/alltypes_plain.parquet")
    #     .to_string_lossy().to_string();
    # let options = ParquetReadOptions::default();

    let df = ctx.read_parquet(&path, options).await?;
    df.show().await?;
    Ok(())
}
```

## Why Parquet Performs

**DataFusion exploits Parquet's columnar layout and embedded metadata at
every stage of query execution — from skipping entire directories down to
avoiding individual row decoding.**

This section highlights the key optimizations that make Parquet the
highest-performing format in DataFusion:

- **Projection pushdown** — read only the columns your query needs (always on)
- **Metadata-based pruning** — skip row groups and pages using statistics (on by default)
- **Filter pushdown** — evaluate filters during decoding to skip rows (opt-in)
- **StringView** — zero-copy string loading from Parquet pages (always on)
- **Advanced indexing** — custom indexes for domain-specific pruning (programmatic API)

Each optimization is automatic or controlled by a single configuration
flag. For deep dives, follow the blog post links.

### Projection pushdown

DataFusion reads only the columns your query references. For a query like
`SELECT name, amount FROM orders`, only the `name` and `amount` column
chunks are read from disk — all other columns are skipped entirely. This is
always enabled, requires no configuration, and is the single biggest
performance advantage of columnar formats over row-based formats like
CSV and JSON. (This is the columnar advantage that makes OLAP engines fast.)

### Metadata-based pruning

Before reading any column data, DataFusion uses Parquet metadata to skip
large sections of the file that cannot match your query predicates:

| Layer               | What's skipped             | How it works                                                                                |
| ------------------- | -------------------------- | ------------------------------------------------------------------------------------------- |
| 1. **Partition**    | Entire directories         | Hive-style paths (`year=2024/`) matched against `WHERE` predicates.                         |
| 2. **Row group**    | Groups of rows (~128 MB)   | Min/max statistics compared to filter predicates (`datafusion.execution.parquet.pruning`).  |
| 3. **Page index**   | Pages within column chunks | Page-level min/max stats (if written by the file producer).                                 |
| 4. **Bloom filter** | Row groups by value        | Probabilistic check for value existence — effective for equality predicates (`id = 'abc'`). |

Row group pruning is **ON by default**. It can be overridden per read via
[`ParquetReadOptions::parquet_pruning()`][`parquetreadoptions::parquet_pruning()`] or globally via the session
setting [`datafusion.execution.parquet.pruning`][parquet-pruning-config].

For a detailed walkthrough of the full pruning pipeline, see
[Parquet Pruning in DataFusion: Read Only What Matters][parquet-pruning].

### Filter pushdown (late materialization)

Beyond metadata pruning, DataFusion can apply filters _during_ Parquet
decoding — decoding only the filter columns first, building a row mask,
then selectively decoding remaining columns for matching rows. This is
especially effective for highly selective queries where only a small
fraction of rows match.

:::{admonition} Filter pushdown is OFF by default
:class: important

Filter pushdown can introduce overhead when filter columns overlap with
projected columns. DataFusion mitigates this with an interleaved decoding
pipeline that caches recently decompressed pages, yielding up to 2.2x
speedup on selective queries.
:::

Enable filter pushdown via session-level settings (not [`ParquetReadOptions`]):

| Setting                                                                     | Default | Effect                                                                    |
| :-------------------------------------------------------------------------- | :------ | :------------------------------------------------------------------------ |
| [`datafusion.execution.parquet.pushdown_filters`][parquet-pushdown-filters] | `false` | Enables filter evaluation during Parquet decoding (late materialization). |
| [`datafusion.execution.parquet.reorder_filters`][parquet-reorder-filters]   | `false` | Heuristically reorders filter expressions to minimize evaluation cost.    |

```rust
use datafusion::prelude::*;

# #[tokio::main]
# async fn main() -> datafusion::error::Result<()> {
let ctx = SessionContext::new_with_config(
    SessionConfig::new()
        .set_bool("datafusion.execution.parquet.pushdown_filters", true)
        .set_bool("datafusion.execution.parquet.reorder_filters", true),
);
# Ok(())
# }
```

For the full analysis — including the interleaved pipeline, caching
strategy, and ClickBench benchmarks — see
[Efficient Filter Pushdown in Parquet][parquet-pushdown].

### StringView: zero-copy string loading

DataFusion uses Arrow's StringView representation (also known as
"German-style strings") to load string columns from Parquet without
copying the underlying bytes. Traditional string arrays require copying
and consolidating string data into a contiguous buffer; StringView
reuses the decoded Parquet pages directly, reducing memory allocations
and yielding **20–200% speedups** on string-intensive queries.

For implementation details and benchmarks, see
[Using StringView to Make Queries Faster][stringview-blog].

### Advanced indexing

For highly selective queries where built-in statistics are not enough,
DataFusion supports custom indexing via [`ParquetAccessPlan`]. This is a
programmatic API — you implement a custom [`ParquetFileReaderFactory`] that
provides DataFusion with a pre-built access plan to skip specific row
groups or pages based on external logic:

- **User-defined indexes** — embed custom indexes directly in Parquet
  file metadata
- **External indexes** — store sidecar index files (e.g., full-text or
  geospatial) alongside your Parquet data

See the blog posts in [References](#parquet-references) below for
implementation guides on both approaches.

## Parquet References

**DataFusion Blog (Deep Dives):**

- [Parquet Pruning in DataFusion: Read Only What Matters][parquet-pruning] — The multi-layer pruning pipeline
- [Efficient Filter Pushdown in Parquet][parquet-pushdown] — Late materialization and interleaved decoding
- [Using StringView to Make Queries Faster][stringview-blog] — Zero-copy string loading from Parquet
- [User-Defined Parquet Indexes][parquet-user-indexes] — Embedding custom indexes
- [External Parquet Indexes][parquet-external-indexes] — Sidecar index files

**Format & API:**

- [Apache Parquet File Format][parquet-file-format] — Official specification
- [`ParquetReadOptions` API][`parquetreadoptions`] — All configuration options
- [Parquet Format Options (SQL)][format-options] — SQL-level options for `CREATE EXTERNAL TABLE` and `COPY`

---

<!-- References -->

<!-- Internal documentation -->

[collect-statistics]: ../../../../user-guide/configs.md
[format-options]: ../../../../user-guide/sql/format_options.md
[from-files-index]: index.md
[parquet-metadata-size-hint]: ../../../../user-guide/configs.md
[parquet-pruning-config]: ../../../../user-guide/configs.md
[parquet-pushdown-filters]: ../../../../user-guide/configs.md
[parquet-reorder-filters]: ../../../../user-guide/configs.md

<!-- Core types -->

[`dataframe`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html
[`listingtable`]: https://docs.rs/datafusion/latest/datafusion/datasource/listing/struct.ListingTable.html
[`parquetaccessplan`]: https://docs.rs/datafusion/latest/datafusion/datasource/physical_plan/parquet/struct.ParquetAccessPlan.html
[`parquetfilereaderfactory`]: https://docs.rs/datafusion/latest/datafusion/datasource/physical_plan/parquet/trait.ParquetFileReaderFactory.html
[`parquetreadoptions`]: https://docs.rs/datafusion/latest/datafusion/datasource/file_format/options/struct.ParquetReadOptions.html
[`sessioncontext`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html

<!-- Methods and functions -->

[`.read_parquet()`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.read_parquet
[`.register_parquet()`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.register_parquet
[`.sql()`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.sql
[`dataframe::schema()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.schema
[`parquetreadoptions::file_extension()`]: https://docs.rs/datafusion/latest/datafusion/datasource/file_format/options/struct.ParquetReadOptions.html#method.file_extension
[`parquetreadoptions::file_sort_order()`]: https://docs.rs/datafusion/latest/datafusion/datasource/file_format/options/struct.ParquetReadOptions.html#method.file_sort_order
[`parquetreadoptions::metadata_size_hint()`]: https://docs.rs/datafusion/latest/datafusion/datasource/file_format/options/struct.ParquetReadOptions.html#method.metadata_size_hint
[`parquetreadoptions::parquet_pruning()`]: https://docs.rs/datafusion/latest/datafusion/datasource/file_format/options/struct.ParquetReadOptions.html#method.parquet_pruning
[`parquetreadoptions::schema()`]: https://docs.rs/datafusion/latest/datafusion/datasource/file_format/options/struct.ParquetReadOptions.html#method.schema
[`parquetreadoptions::skip_metadata()`]: https://docs.rs/datafusion/latest/datafusion/datasource/file_format/options/struct.ParquetReadOptions.html#method.skip_metadata
[`parquetreadoptions::table_partition_cols()`]: https://docs.rs/datafusion/latest/datafusion/datasource/file_format/options/struct.ParquetReadOptions.html#method.table_partition_cols
[`sessionconfig::with_collect_statistics()`]: https://docs.rs/datafusion/latest/datafusion/execution/config/struct.SessionConfig.html#method.with_collect_statistics

<!-- External resources -->

[apache-parquet]: https://parquet.apache.org/
[parquet-encryption]: https://parquet.apache.org/docs/file-format/data-pages/encryption/
[parquet-external-indexes]: https://datafusion.apache.org/blog/2025/08/15/external-parquet-indexes/
[parquet-file-format]: https://parquet.apache.org/docs/file-format/
[parquet-pruning]: https://datafusion.apache.org/blog/2025/03/20/parquet-pruning/
[parquet-pushdown]: https://datafusion.apache.org/blog/2025/03/21/parquet-pushdown/
[parquet-user-indexes]: https://datafusion.apache.org/blog/2025/07/14/user-defined-parquet-indexes/
[parquet-viewer]: https://parquet-viewer.xiangpeng.systems/
[stringview-blog]: https://datafusion.apache.org/blog/2024/09/13/string-view-german-style-strings-part-1/
