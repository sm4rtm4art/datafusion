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

<!--
TODO(Docs): writing-dataframes.md - Persisting Data to Storage

1. MOVE CONTENT HERE:
   - "Writing DataFrames (Persistent Storage)" intro.
   - The Format Comparison Table (Parquet vs CSV vs JSON).
   - `DataFrameWriteOptions` configuration.
   - The individual sections for `.write_parquet()`, `.write_csv()`, `.write_json()`, and `.write_table()`.
   - The section on Lakehouse Table Formats (Iceberg, Delta, Lance).

2. ADD THE MISSING BLIND SPOTS:
   - [ ] Add the "Single File Bottleneck" warning: In the `DataFrameWriteOptions` section, explicitly warn that `.with_single_file_output(true)` forces all parallel partitions to merge into a single thread. Advise using directory outputs for large datasets.
   - [ ] Add the "Cloud Object Stores" hint: Below the `memory://` example, add a short paragraph explaining that writing to `s3://` or `gs://` requires registering the store (e.g., `AmazonS3Builder` from the `object_store` crate) so users know how to handle credentials.

3. STRUCTURE:
   - Keep all formats in this ONE file to serve as a unified reference. Do not split Parquet/CSV/JSON into separate files.

4. COMPRESSION:
  _ Add a small overview on the compression used in datafusion. Show differences and when which shines, if possible.
-->

# Writing Concepts

<!--TODO

1. ABSTRACT
2. INTRODUCTION
-->

```{contents} Table of Contents for Writing Concepts
:local:
:depth: 2
:caption: Writing Concepts
```

## Writing DataFrames (Persistent Storage)

**Writing moves DataFrame results from transient memory (RAM) to durable storage (disk), converting in-flight `RecordBatch`es into persistent files or table records.**

Up to this point in the DataFrame lifecycle, data exists only in memory: <br>
Execution methods like [`.collect()`] materialize query results as Arrow `RecordBatch`es in RAM—fast to access, but lost when the process ends. Write methods bridge the gap between ephemeral compute and durable storage, serializing those in-memory batches to files or tables that survive restarts and can be shared across systems.

```text
┌─────────────────────────────────────────────────────────────────────────────┐
│                        DataFrame Lifecycle                                  │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌─────────────┐      ┌─────────────┐      ┌─────────────────────────────┐  │
│  │   Define    │ ──▶  │   Execute   │ ──▶  │          Write              │  │
│  │  (lazy)     │      │   (RAM)     │      │         (DISK)              │  │
│  └─────────────┘      └─────────────┘      └─────────────────────────────┘  │
│                                                                             │
│  .filter()            .collect()            .write_parquet()                │
│  .select()            .show()               .write_csv()                    │
│  .aggregate()         .cache()              .write_json()                   │
│                                             .write_table()                  │
│                                                                             │
│  LogicalPlan          RecordBatch           Files / TableProvider           │
│  (no I/O)             (in-memory)           (persistent storage)            │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

> **Performance reality: RAM is fast, disk is slow** <br>
> In-memory execution operates at nanosecond to microsecond latencies—CPU and memory bandwidth are the limits. Writing to disk introduces millisecond latencies (local SSD) to hundreds of milliseconds (network object stores like S3). A query that executes in seconds may take minutes to write if output is large or the target is remote. Plan write operations accordingly: batch outputs, choose efficient formats (Parquet over CSV), and consider whether you truly need to persist results or if in-memory caching suffices.

Writing matters when results must outlive the current session—whether exporting analytics to a data lake, building ETL pipelines, or populating downstream tables for other consumers.

### General Considerations for Writing

**Writing to disk introduces challenges that don't exist in memory.**

Understanding these concerns upfront helps you design robust pipelines and avoid surprises in production.

In-memory operations benefit from the controlled environment of your process: <br>
Memory is fast, failures are immediate, and there's no format negotiation. Once you cross the boundary to persistent storage, you inherit the complexities of the outside world—network failures, storage quotas, file system semantics, and format compatibility with downstream consumers.

The table below summarizes the key concerns. Think of these as a mental checklist before any write operation:

| Concern              | What it means                                                                                                                                                      |
| -------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| **Durability**       | Write operations are I/O-bound. Local SSDs add milliseconds; object stores (S3, GCS, Azure) add network round-trips. Budget time accordingly.                      |
| **Atomicity**        | File writes are not transactional. A crash mid-write leaves partial files. Defensive pattern: write to a temp path, then rename on success.                        |
| **Storage capacity** | Disk quotas are larger than RAM but still finite. Partitioned writes can create many small files—the "small files problem" that hurts downstream read performance. |
| **Format lock-in**   | Your format choice affects every downstream consumer. Parquet preserves schema and compresses well; CSV loses types. Choose based on who reads the data next.      |
| **Permissions**      | File system permissions and object store IAM policies must allow writes. These errors often surface late, during execution rather than planning.                   |
| **Path handling**    | Write methods accept `&str` paths. Use `path.display().to_string()` (not `.to_str().unwrap()`) to convert `PathBuf`—it avoids panics on non-UTF-8 paths.           |

> **Tip:** <br>
> For production pipelines, validate write targets early (check permissions, available space) and implement retry logic for transient failures. See [Best Practices](best-practices.md) for patterns.

### Write Methods Overview

**DataFusion provides four write methods that persist DataFrame results to files or tables.** <br>
All write methods are **actions**—they execute eagerly, consume the DataFrame, and return a one-row `RecordBatch` with a `count` column indicating how many rows were written.

Each DataFrame write method has a SQL equivalent that produces the same [`LogicalPlan`]. Choose the API that fits your workflow—DataFrame for programmatic control, SQL for ad-hoc exports—without sacrificing functionality or performance.

| DataFrame API        | Destination                               | SQL Equivalent                                  | Under the hood                                    |
| -------------------- | ----------------------------------------- | ----------------------------------------------- | ------------------------------------------------- |
| [`.write_parquet()`] | Parquet files (filesystem/object store)   | `COPY (SELECT ...) TO 'path' STORED AS PARQUET` | `LogicalPlan::Copy` → file sink                   |
| [`.write_csv()`]     | CSV files (filesystem/object store)       | `COPY (SELECT ...) TO 'path' STORED AS CSV`     | `LogicalPlan::Copy` → file sink                   |
| [`.write_json()`]    | NDJSON files (filesystem/object store)    | `COPY (SELECT ...) TO 'path' STORED AS JSON`    | `LogicalPlan::Copy` → file sink                   |
| _(none)_             | Arrow IPC files (filesystem/object store) | `COPY (SELECT ...) TO 'path' STORED AS ARROW`   | SQL-only; no DataFrame method                     |
| [`.write_table()`]   | Registered table ([`TableProvider`])      | `INSERT INTO target_table SELECT ...`           | `LogicalPlan::Dml` → `TableProvider::insert_into` |

For SQL syntax details, see:

- [DML: `COPY` and `INSERT`](../../user-guide/sql/dml.md).

**File writes:** ([`.write_parquet()`], [`.write_csv()`], [`.write_json()`]) stream `RecordBatch`es directly to storage using format-specific serializers. <br>
**Table writes;** ([`.write_table()`]) delegate to the target provider's `insert_into` implementation, which may write files, send data over a network, or perform custom logic.

> **Note: Format gaps** <br>
>
> - **Arrow (IPC)** is SQL-only—use `ctx.sql("COPY ... STORED AS ARROW")` for Arrow output.\
> - **Avro** is read-only in DataFusion; writing Avro is not supported.

> **Trade-off: DataFrame vs SQL for writes** <br>
>
> - **SQL shines:** Ad-hoc exports and one-off pipelines—concise and familiar.\
> - **DataFrame shines:** Paths and options are dynamic Rust values, enabling programmatic branching and integration with application logic.

> **Tip:** <br>
> Write methods consume the DataFrame. <br>
> Use [`df.clone()`][`.clone()`] if you need to write to multiple destinations or run another action like [`.collect()`].

### Choosing the destination: filesystem vs object stores

**Write destinations are URLs resolved through an object store registry.**

The same write code can target:

- local disk (`/tmp/...`)
- in-memory storage (`memory:///...` for tests)
- or cloud stores (`s3://...`, `gs://...`, `az://...`)

Once configured, you pass a path or URL to the write method; DataFusion selects the appropriate `ObjectStore` implementation based on the URL scheme.

Filesystem paths (no scheme) use the local filesystem object store.

**SQL equivalent:** <br>

```sql
COPY (SELECT ...)
TO 'memory:///sales.parquet' STORED
AS PARQUET
```

The example below writes to an in-memory object store and reads the file back.

> **Note:** <br>
> We use `ctx.sql()` here instead of `dataframe!` because the DataFrame must share the same `SessionContext` where the object store is registered.

```rust
use std::sync::Arc;

use datafusion::assert_batches_eq;
use datafusion::dataframe::DataFrameWriteOptions;
use datafusion::error::Result;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::object_store::{memory::InMemory, ObjectStore};
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> Result<()> {
    let ctx = SessionContext::new();

    // Register an in-memory object store for the `memory://` scheme.
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    ctx.runtime_env()
        .register_object_store(ObjectStoreUrl::parse("memory://")?.as_ref(), store);

    let sales_df = ctx
        .sql(
            "SELECT * FROM (VALUES
                (1, 'East', 100),
                (2, 'West', 200)
            ) AS sales(id, region, amount)",
        )
        .await?;

    // Writing executes eagerly and returns a single `count` batch.
    let write_result = sales_df
        .write_parquet("memory:///sales.parquet",
                         DataFrameWriteOptions::new(), None)
        .await?;

    assert_batches_eq!(
        &[
            "+-------+",
            "| count |",
            "+-------+",
            "| 2     |",
            "+-------+",
        ],
        &write_result
    );

    // Read it back from the same object store.
    let row_count = ctx
        .read_parquet("memory:///sales.parquet",
                     ParquetReadOptions::default())
        .await?
        .count()
        .await?;
    assert_eq!(row_count, 2);

    Ok(())
}
```

> **Warning:** <br>
> The `memory://` object store is process-local and not durable. <br>
> Use `memory://` for tests and examples, **not for persistent storage.**

> **Tip:** <br>
> For S3 / Azure / GCS configuration, see [Advanced Topics](dataframes-advance.md).
> <br>
> DataFusion SQL-API users can use `COPY ... TO 's3://...'` with the same object store configuration.

### Choosing a File Format

**Choose the output format based on downstream consumers and schema needs.**

Parquet is the default choice for analytics. Other formats like CSV and JSON are useful for interoperability (for example, log pipelines or NoSQL ingestion) and debugging. Arrow IPC is ideal for zero-copy interchange with other Arrow-based tools.

| Format          | Type     | Schema Preservation | Compression | DataFrame API        | Best For                         |
| --------------- | -------- | ------------------- | ----------- | -------------------- | -------------------------------- |
| **Parquet**     | Columnar | Strong (embedded)   | Excellent   | [`.write_parquet()`] | Analytics, data lakes, archives  |
| **CSV**         | Row      | Weak (no types)     | Optional    | [`.write_csv()`]     | Interchange, spreadsheets        |
| **JSON**        | Row      | Moderate            | Optional    | [`.write_json()`]    | APIs, logs, human-readable       |
| **Arrow (IPC)** | Columnar | Strong (embedded)   | Optional    | SQL only             | Zero-copy Arrow tool interchange |

#### Format Comparison

Here a small overview of the different behaviour of the common files should be given:

|                        | Parquet | CSV | JSON |
| ---------------------- | :-----: | :-: | :--: |
| Type fidelity          |   ✓✓    |  ✗  |  ○   |
| Columnar pruning       |   ✓✓    |  ✗  |  ✗   |
| Compression            |   ✓✓    |  ○  |  ○   |
| Schema in metadata     |   ✓✓    |  ✗  |  ✗   |
| Human readable         |    ✗    | ✓✓  |  ✓✓  |
| Universal tool support |    ○    | ✓✓  |  ✓✓  |

<small>✓✓ excellent · ✓ supported · ○ partial · ✗ not supported</small>

> **Note:** <br>
> Arrow (IPC) writing is available via SQL `COPY ... STORED AS ARROW` but has no DataFrame method.\
> If you start from a `DataFrame`, register the DataFrame as a SQL view using [`SessionContext::register_table`] and [`df.into_view()`][`.into_view()`], then execute a SQL `COPY` statement via `ctx.sql("COPY ...")`.\
> This is different from [`.registry()`], which is for function (UDF) lookup when building expressions.

For format-specific writer options (compression codecs, delimiters, etc.), see:

- [Format Options](../../user-guide/sql/format_options.md) — SQL-level options for `COPY` statements
- Format-specific sections below for DataFrame API options

### Configuring Writes

**Configuring writes means choosing a destination, common write behavior, and format-specific writer settings.** This matters because most production writes need at least one policy decision (partitioning, sorting, overwrite semantics, compression) beyond “write to this path”. DataFusion expresses these choices directly in the DataFrame API via [`DataFrameWriteOptions`] plus optional writer options.

To apply the configuration, call a write action (for example, [`.write_parquet()`]) with a `path`, `options`, and optional format-specific `writer_options`.

**SQL equivalent:** <br>
Use a `COPY ... STORED AS ...` statement (and `INSERT INTO ...` for table writes). See [DML: `COPY` and `INSERT`](../../user-guide/sql/dml.md).

| Parameter        | Type                      | Required | Purpose                                        |
| ---------------- | ------------------------- | -------- | ---------------------------------------------- |
| `path`           | `&str`                    | Yes      | Destination path or URL (file or object store) |
| `options`        | `DataFrameWriteOptions`   | Yes      | Partitioning, sorting, insert operation        |
| `writer_options` | `Option<Format-specific>` | No       | Compression, delimiters, row group size, etc.  |

**Call shape (method signatures):** <br>
These examples show the parameter structure; they omit setup like creating `df` and choosing real paths.

```text
// Parquet — None uses internal defaults, or pass explicit options
df.write_parquet("out.parquet", DataFrameWriteOptions::new(), None).await?
df.write_parquet("out.parquet", DataFrameWriteOptions::new(), Some(TableParquetOptions::default())).await?

// CSV — uses CsvOptions::default()
df.write_csv("out.csv", DataFrameWriteOptions::new(), Some(CsvOptions::default())).await?

// JSON — uses JsonOptions::default()
df.write_json("out.json", DataFrameWriteOptions::new(), Some(JsonOptions::default())).await?

// Table — no writer_options, format determined by TableProvider
df.write_table("my_table", DataFrameWriteOptions::new()).await?
```

> **Note:**<br>
>
> - `TableParquetOptions` provides both `::new()` and `::default()` (equivalent).
> - `CsvOptions` and `JsonOptions` only provide `::default()`.

#### DataFrameWriteOptions

[`DataFrameWriteOptions`] controls partitioning, sorting, and table insert behavior. Not every option applies to every write target:

- **File writes** ([`.write_parquet()`], [`.write_csv()`], [`.write_json()`]): use `.with_partition_by(...)` and `.with_sort_by(...)`
- **Table writes** ([`.write_table()`]): use `.with_insert_operation(...)` (and optionally `.with_sort_by(...)`)

An example of how to configure write options is shown below.

```rust
use datafusion::prelude::*;
use datafusion::dataframe::DataFrameWriteOptions;
use datafusion::error::Result;

#[tokio::main]
async fn main() -> Result<()> {
    // Configure write options (can be reused across writes)
    let _options = DataFrameWriteOptions::new()
        .with_partition_by(vec!["year".to_string(), "month".to_string()]) // Hive-style partitioning
        .with_sort_by(vec![col("timestamp").sort(true, false)]); // Sort rows before writing

    Ok(())
}
```

| Option                         | Used by                                                                       | Effect                                                                        | Default |
| ------------------------------ | ----------------------------------------------------------------------------- | ----------------------------------------------------------------------------- | ------- |
| [`.with_partition_by()`]       | [`.write_parquet()`], [`.write_csv()`], [`.write_json()`]                     | Create hive-style output directories by column values                         | `[]`    |
| [`.with_sort_by()`]            | [`.write_parquet()`], [`.write_csv()`], [`.write_json()`], [`.write_table()`] | Sort rows before writing (may add a global sort, which can be expensive)      | `[]`    |
| [`.with_insert_operation()`]   | [`.write_table()`]                                                            | Control insert behavior (Append/Overwrite/Replace), if the target supports it | Append  |
| [`.with_single_file_output()`] | [`.write_parquet()`], [`.write_csv()`], [`.write_json()`]                     | Request coalescing output into a single file                                  | `false` |

> **Note: <br>
> Output layout (files and partitions)** <br>
>
> - **Single file vs dataset directory:** <br>
>   For file writes, `path` determines the default layout. A non-collection path with a file extension (for example `output.parquet`) writes a single file; a directory/collection path (for example `output_dir/`) writes multiple files.\
> - **Partitioned writes:** <br>
>   [`.with_partition_by()`] creates hive-style directories such as `region=East/`. By default, DataFusion drops partition columns from the file payload and recovers them from directory names on read. Set `execution.keep_partition_by_columns = true` to keep partition columns in the written files.\

> **Warning:** <br>
> [`.write_parquet()`], [`.write_csv()`], and [`.write_json()`] currently support `InsertOp::Append` only. <br>
> Use [`.with_insert_operation()`] with [`.write_table()`] when writing to a table sink.

### Writing to Parquet

**Parquet is the go-to file format for analytical workloads in DataFusion—columnar, compressed, and self-describing.**

Parquet's deep Apache Arrow integration enables columnar access, embedded schema/statistics, and compression—unlocking projection pushdown, predicate pruning, and partition elimination on read. Use [`.write_parquet()`] with [`DataFrameWriteOptions`]; for Parquet-specific settings (compression, row-group size), pass [`TableParquetOptions`].

**SQL equivalent (DataFusion SQL API):**

```sql
COPY (SELECT ...) TO 'path' STORED AS PARQUET
```

#### When Parquet Shines

| Use Case                 | Why Parquet Works                                 |
| ------------------------ | ------------------------------------------------- |
| Data lake storage        | Columnar pruning skips irrelevant data on read    |
| Analytics pipelines      | Schema + statistics enable query optimization     |
| Large dataset archival   | Excellent compression ratios (zstd, snappy, gzip) |
| Cross-system interchange | Arrow-native format—zero-copy reads in many tools |

> **Trade-off:** <br>
> Parquet is not human-readable and requires tooling to inspect. For quick debugging or spreadsheet exports, use CSV.

#### Configuration Options

| Option      | Method / Path                     | Example                           | Parallel I/O  |
| ----------- | --------------------------------- | --------------------------------- | :-----------: |
| Single file | File path with extension          | `sales.parquet`                   |       ✗       |
| Multi-file  | Directory path                    | `sales_dataset/`                  |       ✓       |
| Partitioned | [`.with_partition_by()`]          | `["region"]` → `region=East/...`  |       ✓       |
| Sorted      | [`.with_sort_by()`]               | `[col("date").sort(true, false)]` | ✓ (expensive) |
| Compression | `TableParquetOptions.compression` | `"zstd(3)"`, `"snappy"`, `"gzip"` |       —       |

> **File naming:** <br>
> Each write generates a unique random ID → `{write_id}_{part}.parquet`. Writing to the same directory **accumulates files**. To replace data, delete the directory first or use [`.write_table()`] with `InsertOp::Overwrite`.

> **Tip:** <br>
> Prefer directory output for large results—multiple files enable parallel reads.

```rust
use datafusion::prelude::*;
use datafusion::dataframe::DataFrameWriteOptions;
use datafusion::config::TableParquetOptions;
use datafusion::error::Result;

#[tokio::main]
async fn main() -> Result<()> {
    let ctx = SessionContext::new();
    let output = tempfile::tempdir()?;

    // Sample dataset: sales transactions
    let sales_df = ctx.sql("
        SELECT * FROM (VALUES
            (1, 'East',  100, '2024-01-15'),
            (2, 'West',  200, '2024-01-16'),
            (3, 'East',  150, '2024-01-17'),
            (4, 'West',  300, '2024-02-01')
        ) AS t(id, region, amount, sale_date)
    ").await?;

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // 1) Single file — simple export
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    let path = output.path().join("sales.parquet").display().to_string();
    sales_df.clone().write_parquet(
        &path,
        DataFrameWriteOptions::new(),
        None,
    ).await?;

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // 2) Directory output — enables parallel I/O on read
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    let dir_buf = output.path().join("sales_multi");
    std::fs::create_dir_all(&dir_buf)?;
    let dir = format!("{}/", dir_buf.display());
    sales_df.clone().write_parquet(
        &dir,
        DataFrameWriteOptions::new(),
        None,
    ).await?;
    // Result: sales_multi/{write_id}_0.parquet, {write_id}_1.parquet, ...

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // 3) Partitioned dataset — hive-style directories for partition pruning
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    let partitioned_buf = output.path().join("sales_by_region");
    std::fs::create_dir_all(&partitioned_buf)?;
    let partitioned = format!("{}/", partitioned_buf.display());
    sales_df.clone().write_parquet(
        &partitioned,
        DataFrameWriteOptions::new()
            .with_partition_by(vec!["region".into()]),
        None,
    ).await?;
    // Result: sales_by_region/region=East/{id}.parquet
    //         sales_by_region/region=West/{id}.parquet

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // 4) Sorted + compressed — optimized for range scans
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    let optimized = output.path().join("sales_optimized.parquet").display().to_string();
    let mut parquet_opts = TableParquetOptions::default();
    parquet_opts.global.compression = Some("zstd(3)".into());

    sales_df.write_parquet(
        &optimized,
        DataFrameWriteOptions::new()
            .with_sort_by(vec![col("sale_date").sort(true, false)]),
        Some(parquet_opts),
    ).await?;

    Ok(())
}
```

#### Why This Matters: The Read-Time Payoff

Writing Parquet is an investment that pays dividends every time you query the data:

| What you wrote          | What DataFusion does on read                                            |
| ----------------------- | ----------------------------------------------------------------------- |
| Columnar layout         | Reads only the columns your query needs (projection pushdown)           |
| Embedded statistics     | Skips entire row-groups where min/max don't match your filter           |
| Partitioned directories | Skips entire partition folders that don't match `WHERE region = 'East'` |
| Sorted output           | Enables efficient range scans and early termination                     |

This is why Parquet is the default choice for analytical workloads: the write-time overhead is small, but the read-time savings compound with every query.

> **Deep dive in the benefits of parquet files:**

- [Predicate Pushdown]
- [Partitioned Datasets]
- [Parquet Pruning (blog)][parquet-pruning]
- [Parquet Pushdown (blog)][parquet-pushdown]

### Writing to CSV

**CSV is the universal interchange format—human-readable, supported everywhere, but schema-less and row based.**

CSV trades type fidelity for simplicity: every value becomes a string, there's no embedded schema, and consumers must infer or know the structure. This makes CSV ideal for quick exports, debugging, and integration with tools like Excel, but less suitable for analytics pipelines where Parquet preserves types and enables query optimizations.

Use [`.write_csv()`] with [`DataFrameWriteOptions`] to control layout; pass [`CsvOptions`] to customize delimiters, headers, quoting, and compression.

**SQL equivalent (DataFusion SQL API):**

```sql
COPY (SELECT ...) TO 'path' STORED AS CSV
```

#### When CSV Shines

| Use Case                  | Why CSV Works                          |
| ------------------------- | -------------------------------------- |
| Spreadsheet export        | Excel, Google Sheets open CSV natively |
| Debugging                 | Human-readable—open in any text editor |
| Legacy system integration | Decades of tooling support             |
| Simple ETL handoffs       | No special libraries needed to parse   |

> **Trade-off:** <br>
> CSV loses type information. Integers, floats, dates, and booleans all become strings. On re-read, DataFusion must infer or be told the schema. For type-preserving round-trips, use Parquet.

#### Configuration Options

| Option           | Method                     | Example                                  |
| ---------------- | -------------------------- | ---------------------------------------- |
| Delimiter        | [`.with_delimiter()`]      | `b'\t'` for TSV, `b';'` for European CSV |
| Header row       | [`.with_has_header()`]     | `true` (default) or `false`              |
| Quote character  | [`.with_quote()`]          | `b'"'` (default)                         |
| Escape character | [`.with_escape()`]         | `Some(b'\\')` for backslash escaping     |
| Double quote     | [`.with_double_quote()`]   | `true` (default) — escapes `"` as `""`   |
| Terminator       | [`.with_terminator()`]     | `Some(b'\n')` for Unix line endings      |
| Truncated rows   | [`.with_truncated_rows()`] | `true` to pad short rows with nulls      |
| Compression      | [`.with_compression()`]    | `GZIP`, `ZSTD`, `BZIP2`, `XZ`            |

> **How it works:** <br>
> [`CsvOptions`] wraps Arrow's [`WriterBuilder`] for CSV serialization. Most options above have builder methods; fields like i.e. [`date_format`], [`timestamp_format`], [`null`], and [`quote`] must be set directly on the struct. [Compression][`.with_compression()`] is applied by DataFusion as a wrapper around the serialized output.

```rust
use datafusion::prelude::*;
use datafusion::dataframe::DataFrameWriteOptions;
use datafusion::config::CsvOptions;
use datafusion::common::parsers::CompressionTypeVariant;
use datafusion::error::Result;

#[tokio::main]
async fn main() -> Result<()> {
    let ctx = SessionContext::new();
    let output = tempfile::tempdir()?;

    let sales_df = ctx.sql("
        SELECT * FROM (VALUES
            (1, 'East',  100.50, '2024-01-15'),
            (2, 'West',  200.75, '2024-01-16'),
            (3, 'East',  150.00, '2024-01-17')
        ) AS t(id, region, amount, sale_date)
    ").await?;

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // 1) Standard CSV — comma-delimited with header
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    let path = output.path().join("sales.csv").display().to_string();
    sales_df.clone().write_csv(
        &path,
        DataFrameWriteOptions::new(),
        None,  // defaults: comma, header, no compression
    ).await?;

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // 2) TSV with compression — for log ingestion pipelines
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    let path = output.path().join("sales.tsv.gz").display().to_string();
    sales_df.clone().write_csv(
        &path,
        DataFrameWriteOptions::new(),
        Some(CsvOptions::default()
            .with_delimiter(b'\t')
            .with_compression(CompressionTypeVariant::GZIP)),
    ).await?;

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // 3) European-style CSV — semicolon delimiter, no header
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    let path = output.path().join("sales_eu.csv").display().to_string();
    sales_df.write_csv(
        &path,
        DataFrameWriteOptions::new(),
        Some(CsvOptions::default()
            .with_delimiter(b';')
            .with_has_header(false)),
    ).await?;

    Ok(())
}
```

### Writing to JSON (NDJSON)

**JSON is the lingua franca of web APIs and data pipelines—human-readable, self-describing, but row-oriented.**

DataFusion writes newline-delimited JSON (NDJSON): one JSON object per line, not a JSON array. This format streams naturally into log aggregators, message queues, and REST APIs. Unlike Parquet, JSON preserves nested structures as-is but lacks columnar optimizations—every read scans the full file. Use [`.write_json()`] with [`DataFrameWriteOptions`]; pass [`JsonOptions`] for compression settings.

**SQL equivalent (DataFusion SQL API):**

```sql
COPY (SELECT ...) TO 'path' STORED AS JSON
```

#### When JSON Shines

| Use Case           | Why JSON Works                                  |
| ------------------ | ----------------------------------------------- |
| API responses      | Native format for REST/GraphQL endpoints        |
| NoSQL databases    | MongoDB, CouchDB, DynamoDB speak JSON natively  |
| Log ingestion      | NDJSON streams into Elasticsearch, Splunk, Loki |
| Message queues     | Kafka, RabbitMQ consumers expect JSON payloads  |
| Quick debugging    | Human-readable in any text editor               |
| Nested data export | Preserves complex structures without flattening |

> **Trade-off:** <br>
> JSON is row-oriented with no embedded schema or statistics. On re-read, DataFusion must scan the entire file—no column pruning, no predicate pushdown, no row-group skipping. For analytical queries over large datasets, use Parquet; for interchange and streaming, JSON excels.

#### Configuration Options

| Option      | Method / Field              | Example                   |
| ----------- | --------------------------- | ------------------------- |
| Compression | [`JsonOptions.compression`] | `GZIP`, `ZSTD`, `BZIP2`   |
| Partitioned | [`.with_partition_by()`]    | `["region"]` → hive-style |

> **Note:** <br>
> [`JsonOptions`] is minimal—just :

- [`compression`][`jsonoptions.compression`]
- [`schema_infer_max_rec`]

No builder methods; set fields directly.

```rust
use datafusion::prelude::*;
use datafusion::dataframe::DataFrameWriteOptions;
use datafusion::config::JsonOptions;
use datafusion::common::parsers::CompressionTypeVariant;
use datafusion::error::Result;

#[tokio::main]
async fn main() -> Result<()> {
    let ctx = SessionContext::new();
    let output = tempfile::tempdir()?;

    let events_df = ctx.sql("
        SELECT * FROM (VALUES
            (1, 'click',  '2024-01-15T10:30:00'),
            (2, 'view',   '2024-01-15T10:31:00'),
            (3, 'purchase','2024-01-15T10:32:00')
        ) AS t(id, event_type, timestamp)
    ").await?;

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // 1) Standard NDJSON — one JSON object per line
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    let path = output.path().join("events.ndjson").display().to_string();
    events_df.clone().write_json(
        &path,
        DataFrameWriteOptions::new(),
        None,
    ).await?;
    // Output:
    // {"id":1,"event_type":"click","timestamp":"2024-01-15T10:30:00"}
    // {"id":2,"event_type":"view","timestamp":"2024-01-15T10:31:00"}
    // ...

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // 2) Compressed NDJSON — for log shipping
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    let path = output.path().join("events.ndjson.gz").display().to_string();
    let mut json_opts = JsonOptions::default();
    json_opts.compression = CompressionTypeVariant::GZIP;

    events_df.write_json(
        &path,
        DataFrameWriteOptions::new(),
        Some(json_opts),
    ).await?;

    Ok(())
}
```

### Writing to Registered Tables

**[`.write_table()`] is the universal sink—write to any storage backend that implements [`TableProvider`].**

Unlike file-specific methods (`.write_parquet()`, `.write_csv()`), `.write_table()` abstracts the destination entirely. The target [`TableProvider`] controls format, partitioning, transactions, and error handling. This enables writing to databases, cloud warehouses, custom connectors, or in-memory tables with the same API.

Under the hood, DataFusion calls [`TableProvider::insert_into`], which returns an [`ExecutionPlan`] streaming `RecordBatch`es into the target—the provider converts them to its native format.

**SQL equivalent (DataFusion SQL API):**

```sql
INSERT INTO sales SELECT * FROM new_data
```

#### When to Use [`.write_table()`]

| Use Case                | Why [`.write_table()`] Works                      |
| ----------------------- | ------------------------------------------------- |
| Database connectors     | Provider handles connection, batching, retries    |
| Cloud data warehouses   | Snowflake, BigQuery, Redshift via custom provider |
| Multi-format pipelines  | Same code writes to Parquet, Delta, Iceberg       |
| In-memory staging       | [`MemTable`] for intermediate results             |
| Transactional semantics | Provider controls commit/rollback behavior        |

> **Key difference from file methods:** <br>
> `.write_parquet()` / `.write_csv()` write directly to object storage. [`.write_table()` ]delegates to a registered table—the provider decides how and where data lands.

#### Insert Operations

Use [`DataFrameWriteOptions::with_insert_operation(...)`][`with_insert_operation()`] to control insert semantics:

| Operation                            | SQL Equivalent     | Behavior                                    | Provider Support               |
| ------------------------------------ | ------------------ | ------------------------------------------- | ------------------------------ |
| [`InsertOp::Append`][`append`]       | `INSERT INTO`      | Add new rows to existing data               | Most providers                 |
| [`InsertOp::Overwrite`][`overwrite`] | `INSERT OVERWRITE` | Replace all existing rows                   | Some providers (e.g., Parquet) |
| [`InsertOp::Replace`][`replace`]     | `REPLACE INTO`     | Replace conflicting rows (upsert semantics) | Few providers                  |

> **Warning:** <br>
> Not all providers support all operations. <br>
> [`MemTable`] currently supports [`Append`] only. Check your provider's documentation for supported operations. See [docs.rs][`memtable::insert_into`]

#### Schema Compatibility

The DataFrame schema must be **logically equivalent** to the target table's schema—column names (case-sensitive), data types (exact or implicitly castable), and column order must align.

If schemas don't match, use [`.select()`] with [`.alias()`] to reorder/rename, or [`.cast_to()`] to align types before writing.

> **See also:** <br>
> [Schema Management](./schema-management.md) covers type coercion rules, validation patterns, and debugging mismatches in depth.

#### Built-in TableProvider Implementations

| Provider                    | Supports [`insert_into`] | Supported Operations | Notes                                           |
| --------------------------- | ------------------------ | -------------------- | ----------------------------------------------- |
| [`MemTable`]                | Yes                      | Append               | In-memory; data lost on session end             |
| [`ListingTable`] (Parquet)  | Yes                      | Append               | Writes new Parquet files to the table directory |
| [`ListingTable`] (CSV/JSON) | Yes                      | Append               | Writes new files to the table directory         |
| Custom [`TableProvider`]    | Implementation-defined   | Varies               | Database connectors, object stores, etc.        |

> **Tip:** <br>
> For custom database connectors, implement [`TableProvider::insert_into`] to handle data ingestion. The input [`ExecutionPlan`] provides a stream of [`RecordBatch`]es that your implementation converts to the target format.

```rust
use datafusion::prelude::*;
use datafusion::dataframe::DataFrameWriteOptions;
use datafusion::assert_batches_eq;
use datafusion::error::Result;
use datafusion::logical_expr::dml::InsertOp;

#[tokio::main]
async fn main() -> Result<()> {
    let ctx = SessionContext::new();
    let temp_dir = tempfile::tempdir()?;
    let table_path = temp_dir.path().join("sales");
    std::fs::create_dir_all(&table_path)?;  // Must exist as directory

    // Create target table (external Parquet table pointing to a directory)
    ctx.sql(&format!(
        "CREATE EXTERNAL TABLE sales (id INT, region TEXT, amount INT)
         STORED AS PARQUET LOCATION '{}'",
        table_path.display()
    )).await?.collect().await?;

    // Prepare new data to insert (cast to match table schema)
    let new_sales = ctx.sql("
        SELECT
            CAST(id AS INT) AS id,
            region,
            CAST(amount AS INT) AS amount
        FROM (VALUES
            (1, 'East', 100),
            (2, 'West', 200),
            (3, 'East', 150)
        ) AS sales(id, region, amount)
    ").await?;

    // Append to table (default behavior)
    let write_options = DataFrameWriteOptions::new().with_insert_operation(InsertOp::Append);
    let write_result = new_sales.write_table(
        "sales",
        write_options,
    ).await?;

    assert_batches_eq!(
        &[
            "+-------+",
            "| count |",
            "+-------+",
            "| 3     |",
            "+-------+",
        ],
        &write_result
    );

    // Verify the data was written (read from the table)
    let batches = ctx
        .sql("SELECT COUNT(*) AS count FROM sales")
        .await?
        .collect()
        .await?;
    assert_batches_eq!(
        &[
            "+-------+",
            "| count |",
            "+-------+",
            "| 3     |",
            "+-------+",
        ],
        &batches
    );

    Ok(())
}
```

#### Prerequisites

The target table must be registered before calling `.write_table()`:

| Method               | Example                                                |
| -------------------- | ------------------------------------------------------ |
| SQL DDL              | `CREATE EXTERNAL TABLE sales ... LOCATION '...'`       |
| [`register_table()`] | `ctx.register_table("sales", Arc::new(my_provider))?;` |

#### Performance Tip

For row-oriented sinks (database connectors), **Arrow-to-row conversion and network I/O dominate cost**—not DataFrame vs. SQL syntax. Push filters and projections into the [`TableProvider`] where possible.

### When to Consider Lakehouse Table Formats

**Raw file writes are simple but brittle—lakehouse formats add transactional guarantees.**

For one-off exports, raw Parquet/CSV/JSON is fine. But as pipelines grow—concurrent writers, schema changes, failure recovery—the cracks show. [Apache Iceberg], [Delta Lake], and [Apache Hudi] add a metadata layer that provides database-like semantics on top of object storage.

#### Raw Files vs. Table Formats

| Capability              | Raw Files                   | Lakehouse Formats                |
| ----------------------- | --------------------------- | -------------------------------- |
| **Atomic commits**      | ✗ Partial writes on failure | ✓ All-or-nothing                 |
| **Concurrent writers**  | ✗ Risk of corruption        | ✓ Conflict detection             |
| **Schema evolution**    | ✗ Manual coordination       | ✓ Add/rename/drop columns safely |
| **Time travel**         | ✗ Manual snapshots only     | ✓ Query any historical version   |
| **Compaction**          | ✗ Small files accumulate    | ✓ Automatic or on-demand         |
| **Partition evolution** | ✗ Full rewrite required     | ✓ Change without rewriting       |

#### Decision Guide

| Scenario                                  | Recommendation |
| ----------------------------------------- | -------------- |
| One-off exports, ad-hoc analysis          | Raw files ✓    |
| Single-writer, append-only logs           | Raw files ✓    |
| Systems that only read raw Parquet/CSV    | Raw files ✓    |
| Multiple concurrent ETL jobs              | Table format ✓ |
| Need failure recovery without cleanup     | Table format ✓ |
| Schema evolution over time                | Table format ✓ |
| Audit/compliance (lineage, point-in-time) | Table format ✓ |
| Large lakes with frequent small updates   | Table format ✓ |

#### DataFusion Integration

DataFusion provides the [`TableProvider`] and [`TableProviderFactory`] traits—external crates implement them for specific formats. See [Catalogs, Schemas, and Tables] for the full architecture.

| Format         | Rust Crate                                             | Status           |
| -------------- | ------------------------------------------------------ | ---------------- |
| Delta Lake     | [delta-rs](https://github.com/delta-io/delta-rs)       | Production-ready |
| Apache Iceberg | [iceberg-rust](https://github.com/apache/iceberg-rust) | Maturing         |
| Lance          | [lance](https://github.com/lancedb/lance)              | Production-ready |

Once registered, [`.write_table()`] works unchanged—your DataFrame code stays the same, only storage semantics differ.

> **Build your own:** <br>
> See [Custom Table Provider] for implementing data sources/sinks, or [datafusion-contrib](https://github.com/datafusion-contrib) for community integrations.

[catalogs, schemas, and tables]: https://datafusion.apache.org/library-user-guide/catalogs.html
[custom table provider]: https://datafusion.apache.org/library-user-guide/custom-table-providers.html
[apache iceberg]: https://iceberg.apache.org/
[delta lake]: https://delta.io/
[apache hudi]: https://hudi.apache.org/
[`tableproviderfactory`]: https://docs.rs/datafusion/latest/datafusion/catalog/trait.TableProviderFactory.html

## Quick Reference

| Practice                        | Why                                                    |
| ------------------------------- | ------------------------------------------------------ |
| Prefer Parquet                  | Schema + compression + columnar pruning on read        |
| Use directory outputs           | Enables parallel reads; avoids single-file bottlenecks |
| Partition by filter columns     | `with_partition_by(["region"])` → partition pruning    |
| Register object stores first    | Required for `s3://`, `gcs://`, `memory:///` URLs      |
| Use `.with_sort_by()` sparingly | Enables range scans but adds expensive global sort     |
