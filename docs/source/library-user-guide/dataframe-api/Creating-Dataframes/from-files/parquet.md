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

**Parquet - the default choice for analytical workloads: columnar, compressed,
self-describing, and optimized for selective reads.**

[Apache Parquet](https://parquet.apache.org/) stores data
**column-by-column** instead of row-by-row. This layout lets DataFusion
read only the columns your query needs, skip irrelevant row groups using
embedded statistics, and benefit from excellent compression ratios. If you
control the storage format and are unsure which to choose, **start with
Parquet** — its metadata-driven optimizations compound as data volume grows.

```{contents} Table of Contents
:local:
:depth: 2
```

## Reading Parquet Files

**A single call to `.read_parquet()` creates a lazy DataFrame backed by
Parquet's embedded metadata — no data is loaded until you execute an
action.**

Reading a Parquet file is the simplest file-based entry point in DataFusion.
Because Parquet embeds its schema in the file footer, DataFusion needs no
inference step — it reads a few kilobytes of metadata and immediately returns
a lazy [`DataFrame`] ready for transformations. Pass a local path, a glob
pattern, or a cloud URL (after [object store registration](index.md#cloud-storage))
along with [`ParquetReadOptions`] to configure the read.

```rust
use datafusion::prelude::*;
# use std::path::PathBuf;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();

    // Path to your Parquet file
    let path = "data.parquet";
    # let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
    #     .join("parquet-testing/data/alltypes_plain.parquet")
    #     .to_string_lossy().to_string();

    let df = ctx.read_parquet(&path, ParquetReadOptions::default()).await?;

    df.show().await?;
    Ok(())
}
```

For typical analytical queries, Parquet is often **significantly faster**
than CSV — not because of raw read speed, but because DataFusion can skip
entire row groups that don't match your filters and avoid reading columns
your query doesn't reference. These optimizations are automatic and require
no user configuration.

## When to Use Parquet

**Parquet excels at read-heavy analytical workloads where selective access
and compression matter most.**

Parquet's columnar layout and embedded statistics make it the natural choice
when queries touch a subset of columns or filter large datasets. However,
the format is write-once and not human-readable, so it's not ideal for every
use case.

| Parquet shines                                         | Consider alternatives                                             |
| ------------------------------------------------------ | ----------------------------------------------------------------- |
| Production analytics, repeated queries, large datasets | Write-heavy append logs → NDJSON / streaming-native systems       |
| Selective reads (filters + column pruning)             | Human-editable debugging → CSV/JSON                               |
| Efficient storage (compression + columnar layout)      | Very small datasets where metadata/compression overhead dominates |

## ParquetReadOptions

**[`ParquetReadOptions`] configures how DataFusion reads Parquet files —
from pruning behavior to schema enforcement — using a builder pattern.**

| Builder Method                                                                   | Default      | Usage                                                                                                                           |
| :------------------------------------------------------------------------------- | :----------- | :------------------------------------------------------------------------------------------------------------------------------ |
| **[`.file_extension(&str)`][`parquetreadoptions::file_extension()`]**            | `".parquet"` | Filters input files by suffix. Use when folders contain mixed files (`.crc`, `.json`, temp files).                              |
| **[`.table_partition_cols(Vec)`][`parquetreadoptions::table_partition_cols()`]** | `[]`         | Maps Hive-style directory paths to columns (e.g., `year=2025/`). Use when data is organized in folders by date/category.        |
| **[`.parquet_pruning(bool)`][`parquetreadoptions::parquet_pruning()`]**          | `true`       | Skips row groups using min/max statistics. Keep enabled for filtered queries (`WHERE id > 100`).                                |
| **[`.skip_metadata(bool)`][`parquetreadoptions::skip_metadata()`]**              | `true`       | Ignores embedded schema metadata to avoid conflicts. Keep `true` for mixed producers; set `false` only if you rely on metadata. |
| **[`.schema(&Schema)`][`parquetreadoptions::schema()`]**                         | `None`       | Supplies the Parquet _file_ schema. Use for production to enforce types and avoid schema-merging surprises across many files.   |
| **[`.file_sort_order(Vec)`][`parquetreadoptions::file_sort_order()`]**           | `[]`         | Tells the optimizer the data is pre-sorted. Use to speed up merge-joins or `ORDER BY` queries without re-sorting.               |
| **`.file_decryption_properties(Option)`**                                        | `None`       | Decryption configuration for Parquet [modular encryption](https://parquet.apache.org/docs/file-format/encryption/). Advanced.   |
| **`.metadata_size_hint(Option<usize>)`**                                         | `None`       | Hint (in bytes) for the footer fetch size. Useful for cloud object stores where a larger initial read avoids extra round trips. |

:::{admonition} schema() is a builder method
:class: note

[`ParquetReadOptions::schema()`] _sets_ the schema for reading. This differs
from [`DataFrame::schema()`], which _returns_ the schema of an existing
DataFrame.
:::

:::{admonition} Example: ParquetReadOptions builder pattern
:class: seealso
:collapsible: open

The following example demonstrates how to configure `ParquetReadOptions`
with various builder methods. For Hive-partitioned data
(e.g., `year=2024/month=01/`), use `.table_partition_cols()` to map
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
    #     .join("parquet-testing/data/alltypes_plain.parquet")
    #     .to_string_lossy().to_string();
    # // Use default options for test data (no partition columns)
    # let options = ParquetReadOptions::default();

    let df = ctx.read_parquet(&path, options).await?;
    df.show().await?;
    Ok(())
}
```

:::

---

## How Parquet Reading Works

**DataFusion uses metadata-first scanning — reading the Parquet footer at
creation time and deferring all data I/O until execution, where multiple
pruning layers minimize what actually gets read.**

### At creation time

When you call `.read_parquet()`:

1. **Footer read** — Reads the Parquet footer (~few KB) to learn the schema
   and row group metadata, or uses an explicit schema if you provided one
   via `ParquetReadOptions::schema()`.
2. **Plan creation** — Creates a [`ListingTable`] and returns a lazy
   `DataFrame`. No column data is loaded yet.

:::{admonition} Startup cost vs. pruning
:class: note

`.read_parquet()` may collect per-file statistics during DataFrame creation
(`datafusion.execution.collect_statistics = true` by default). This can add
noticeable startup time for many files (especially on cloud object stores),
but speeds up filtered queries. To prioritize startup time, disable it:
`SessionConfig::new().with_collect_statistics(false)`.
:::

### At execution time

When you call an action (`.collect()`, `.show()`), DataFusion exploits
Parquet metadata at multiple levels to minimize I/O:

**Metadata-based skipping (ON by default):**

| Mechanism         | What's skipped             | How it works                                                                                            |
| ----------------- | -------------------------- | ------------------------------------------------------------------------------------------------------- |
| **Partition**     | Entire directories         | Hive-style paths (`year=2024/`) matched against `WHERE`. Works for all formats.                         |
| **Row group**     | Groups of rows (~128MB)    | Min/max statistics compared to filter predicates. Controlled by `datafusion.execution.parquet.pruning`. |
| **Page Index**    | Pages within column chunks | Page-level min/max stats (if written by producer).                                                      |
| **Bloom Filters** | Specific row groups        | Probabilistic check for value existence (e.g., `id = 'abc'`).                                           |

**Decode-time optimizations:**

| Mechanism           | Optimization         | How it works                                                                                                            |
| ------------------- | -------------------- | ----------------------------------------------------------------------------------------------------------------------- |
| **Projection**      | Unreferenced columns | Only columns in `SELECT` are read from disk.                                                                            |
| **Filter Pushdown** | Late materialization | Applies filters _during_ decoding to skip values. **OFF** by default (`datafusion.execution.parquet.pushdown_filters`). |

:::{admonition} Row group pruning is enabled by default
:class: tip

Row group pruning (`datafusion.execution.parquet.pruning = true`) can be
overridden per read with `.parquet_pruning(true/false)`. With pruning
enabled, DataFusion compares your `WHERE` predicates against each row
group's min/max statistics — if no rows can possibly match, the entire group
is skipped without reading any data.
:::

For highly selective queries where built-in statistics aren't enough,
DataFusion supports advanced indexing:

- **User-defined indexes** — Embed custom indexes directly in Parquet file
  metadata
- **External indexes** — Store sidecar index files alongside your Parquet
  data

See the [References](#parquet-references) below for deep dives on these
techniques.

## Filter Pushdown (Late Materialization)

**Beyond metadata pruning, DataFusion can apply filters _during_ Parquet
decoding — reading only filter columns first and selectively decoding
remaining columns for matching rows. This is OFF by default.**

The [metadata-based skipping](#at-execution-time) described above (row group
pruning, page index, bloom filters) operates at the _metadata level_ — it
decides which chunks of data to skip before reading any column values. Filter
pushdown goes one step further: it operates at the _row level_ during
decoding itself.

### How it works

With filter pushdown enabled, DataFusion uses **late materialization**:

1. **Decode filter columns only** — For a query like
   `SELECT val FROM sensor_data WHERE location = 'office'`, DataFusion first
   decodes only the `location` column.
2. **Build a row mask** — Evaluates the filter predicate to produce a
   boolean mask identifying which rows match.
3. **Selectively decode remaining columns** — Only rows that passed the
   filter are decoded from the remaining projected columns (`val`), skipping
   all non-matching rows entirely.

This is especially effective for **highly selective filters** — if only 1% of
rows match, 99% of the projected column data is never decoded.

### Configuration

Filter pushdown is controlled by session-level settings, not by
`ParquetReadOptions`:

| Setting                                         | Default | Effect                                                                    |
| :---------------------------------------------- | :------ | :------------------------------------------------------------------------ |
| `datafusion.execution.parquet.pushdown_filters` | `false` | Enables filter evaluation during Parquet decoding (late materialization). |
| `datafusion.execution.parquet.reorder_filters`  | `false` | Heuristically reorders filter expressions to minimize evaluation cost.    |

Enable both for filtered analytical queries on large Parquet datasets:

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

### Performance trade-offs

Filter pushdown is not unconditionally faster. Columns that appear in
**both** the filter and the output projection must be decoded twice — once
for filter evaluation and once for the output. For non-selective filters
(where most rows match), this double-decode overhead can outweigh the
savings.

DataFusion's Parquet reader mitigates this with an **interleaved decoding
pipeline** that caches recently decompressed pages, limiting the overhead
to at most two pages (~2 MB) per shared column. On the
[ClickBench](https://benchmark.clickhouse.com/) benchmark suite, this
approach reduced total query time by ~15%, with up to 2.2x speedup on
highly selective queries and no significant regression elsewhere.

:::{admonition} When to enable filter pushdown
:class: tip

Enable `pushdown_filters` when your queries filter on columns that
are selective (few matching rows relative to total). Keep it disabled for
full-table scans or queries with non-selective predicates where the
double-decode overhead provides no benefit.
:::

For a detailed analysis of the interleaved pipeline, caching strategy, and
benchmark results, see the
[Efficient Filter Pushdown in Parquet](https://datafusion.apache.org/blog/2025/03/21/parquet-pushdown/)
blog post.

## Parquet References

**DataFusion Blog (Deep Dives):**

- [Parquet Pushdown](https://datafusion.apache.org/blog/2025/03/21/parquet-pushdown/) — How DataFusion exploits Parquet metadata
- [User-Defined Parquet Indexes](https://datafusion.apache.org/blog/2025/07/14/user-defined-parquet-indexes/) — Embedding custom indexes
- [External Parquet Indexes](https://datafusion.apache.org/blog/2025/08/15/external-parquet-indexes/) — Sidecar index files

**Format & API:**

- [Apache Parquet Documentation](https://parquet.apache.org/docs/) — Official specification
- [`ParquetReadOptions` API](https://docs.rs/datafusion/latest/datafusion/datasource/file_format/options/struct.ParquetReadOptions.html) — All configuration options
- [Parquet Format Options (SQL)](../../../../../user-guide/sql/format_options.md#parquet-format-options) — SQL-level options for `CREATE EXTERNAL TABLE` and `COPY`

---
