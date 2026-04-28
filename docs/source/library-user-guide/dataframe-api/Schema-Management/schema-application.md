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

<!-- TODO: Write the abstract last (Stage 5). -->

# Applying Schemas

:::{admonition} Style Note
:class: note
:collapsible: closed

In this document, code elements follow a consistent pattern:

- **DataFrame methods:** `df.method()` (e.g., `df.select(...)`, `df.filter(...)`)
- **DFSchema instance methods:** `df.schema().method()` (e.g., `df.schema().fields()`)
- **DFSchema associated functions:** `DFSchema::function()` (e.g., `DFSchema::try_from(...)`)
- **Standalone functions:** `function()` (e.g., `col(...)`, `lit(...)`)
- **Constructors:** `Type::new()` (e.g., `SessionContext::new()`)
- **Types:** `TypeName` (e.g., `SchemaRef`, `RecordBatch`)
- **Lazy transformations:** return a `DataFrame` and build the `LogicalPlan`
- **Actions:** (`.collect()`, `.show()`) trigger execution

:::

```{contents} Table of Contents for Applying Schemas
:local:
:depth: 2
```

## Introduction 

**Applying a schema is the bridge between definition and execution — the mechanism differs by format, but the pattern is consistent: pass the schema to the read options and let DataFusion enforce it.**

Text formats (CSV, NDJSON) benefit most from explicit schemas because they carry no type information. Self-describing formats (Parquet, Avro, Arrow IPC) embed their own schemas but require normalization when files evolve independently. Partitioned datasets add a structural dimension where partition columns live outside the file schema. For building the schemas themselves — fields, parameterized types, metadata, `DFSchema` — see [Creating Schemas](schema-creation.md).



## Text Formats: CSV and NDJSON

**Text formats carry no type information — provide an explicit schema for production workloads to prevent inference drift.**

Without a schema, DataFusion infers types from a sample of rows ([`schema_infer_max_records`], default 1,000). Providing a schema via the read options enforces a declared contract on the raw data, overriding inference entirely. For why inference falls short and when explicit schemas are worth defining, see [The Case for Explicit Definition](schema-creation.md#the-case-for-explicit-definition).

| Format     |   Alignment    | Key Behaviors                                                                                  |
| :--------- | :------------: | :--------------------------------------------------------------------------------------------- |
| **CSV**    | **Positional** | Fields map to schema columns by order. Header names are read but position determines mapping   |
| **NDJSON** | **Name-based** | JSON keys match schema field names. Order doesn't matter. Missing keys → NULL, extra → ignored |

### CSV — Positional Alignment

CSV uses **positional** mapping: the first column maps to the first field in your schema, the second to the second, and so on. Header names (if present with [`has_header(true)`][`has_header`]) are read but field order determines the mapping.

Key behaviors:

- **Types**: values are parsed into the declared Arrow types (e.g., `Decimal128(19,2)` for currency).
- **Missing/extra columns**: row-length mismatches error by default; use [`truncated_rows(true)`][`truncated_rows`] to fill missing nullable columns with NULLs.
- **Format details**: single-byte `delimiter`, `quote`, optional `escape`, `comment`, and `terminator`.

```rust
use datafusion::prelude::*;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::assert_batches_eq;
# use std::fs::File;
# use std::io::Write;
# use tempfile::tempdir;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();
    # let dir = tempdir()?;
    # let csv_path = dir.path().join("sales.csv");
    # let mut file = File::create(&csv_path)?;
    # writeln!(file, "order_id,customer_id,amount")?;
    # writeln!(file, "1001,CUST-001,99.99")?;
    # writeln!(file, "1002,CUST-002,150.50")?;

    // Define the canonical schema — explicit types prevent inference drift
    let sales_schema = Schema::new(vec![
        Field::new("order_id", DataType::Int64, false),
        Field::new("customer_id", DataType::Utf8, false),
        Field::new("amount", DataType::Decimal128(19, 2), true),
    ]);

    // Apply schema to CSV reader — overrides inference
    let path = "sales.csv";
    # let path = csv_path.to_str().unwrap();
    let df = ctx.read_csv(path, CsvReadOptions::new()
        .schema(&sales_schema)
        .has_header(true)
    ).await?;

    assert_batches_eq!(
        &[
            "+----------+-------------+--------+",
            "| order_id | customer_id | amount |",
            "+----------+-------------+--------+",
            "| 1001     | CUST-001    | 99.99  |",
            "| 1002     | CUST-002    | 150.50 |",
            "+----------+-------------+--------+",
        ],
        &df.collect().await?
    );

    Ok(())
}
```

:::{admonition} Inference pitfalls in CSV
:class: warning
Schema inference samples only the first 1,000 rows by default ([`schema_infer_max_records`]). Common pitfalls: IDs inferred as `Int32` then overflow, currency inferred as `Float64` (rounding errors), sparse columns inferred as `Utf8`. Always provide explicit schemas for CSV in production. For the full inference mechanism and its failure modes, see [Schema Inference](schema-inference.md).
:::

### NDJSON — Name-Based Alignment

NDJSON uses **name-based** mapping: JSON keys match schema field names, so field order does not matter. Missing keys become NULL; extra keys are silently ignored.

Key behaviors:

- **Missing keys**: with an explicit schema, missing fields become NULL.
- **Extra keys**: keys not in the schema are ignored (no error, no column).
- **Types**: JSON values are cast to declared Arrow types; invalid casts raise errors.
- **Nested data**: supports `Struct`, `List`, `Map` (CSV cannot express nested types). For defining nested types in a schema, see [Nested Types: Struct, List, Map](schema-creation.md#nested-types-struct-list-map).

```rust
use datafusion::prelude::*;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::assert_batches_eq;
# use std::fs::File;
# use std::io::Write;
# use tempfile::tempdir;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();
    # let dir = tempdir()?;
    # let json_path = dir.path().join("data.json");
    # let mut file = File::create(&json_path)?;
    # writeln!(file, r#"{{"id": 1, "name": "Alice"}}"#)?;
    # writeln!(file, r#"{{"id": 2}}"#)?;

    // Explicit schema: "name" is nullable to handle missing keys
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
    ]);

    let path = "data.json";
    # let path = json_path.to_str().unwrap();
    let df = ctx.read_json(path, NdJsonReadOptions::default()
        .schema(&schema)
    ).await?;

    // Row 2 has no "name" key — becomes NULL
    assert_batches_eq!(
        &[
            "+----+-------+",
            "| id | name  |",
            "+----+-------+",
            "| 1  | Alice |",
            "| 2  |       |",
            "+----+-------+",
        ],
        &df.collect().await?
    );

    Ok(())
}
```

Text formats are the most common case for explicit schemas. Self-describing formats carry their own schema, but evolution across files introduces a different challenge.

---

## Self-Describing Formats: Parquet, Avro, Arrow IPC

**Self-describing formats embed schemas in file metadata — but schemas evolve across files, and DataFusion auto-merges them.**

When reading multiple files with evolved schemas, DataFusion merges them automatically:

```text
v1_sales.parquet:  id (Int32),  amount (Decimal128(19,2))
v2_sales.parquet:  id (Int64),  amount (Decimal128(38,9)),  region (Utf8)

Merged result:     id (Int64),  amount (Decimal128(38,9)),  region (Utf8, nullable)
```

- Types are **widened**: `Int32` + `Int64` → `Int64`
- Columns are **added**: missing columns appear as nullable
- Use [`ctx.read_parquet()`] with multiple file paths to trigger auto-merge

### Normalizing to a Canonical Schema

**After reading, normalize to your canonical schema using [`.select()`] and [`.cast_to()`] — decouple your pipeline from upstream schema changes.**

```rust
use datafusion::prelude::*;
use datafusion::arrow::datatypes::DataType;
use datafusion::assert_batches_eq;
use datafusion::logical_expr::ExprSchemable;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    // Simulate data that arrived as Int32 (might be widened Int32 from Parquet)
    let df = dataframe!(
        "id" => [1_i32, 2_i32, 3_i32, 4_i32],
        "amount" => [99.99, 150.50, 75.25, 200.00]
    )?;

    // Normalize to canonical types: Int32 → Int64, Float64 stays Float64
    let canonical_df = df.clone().select(vec![
        col("id").cast_to(&DataType::Int64, df.schema())?.alias("id"),
        col("amount").cast_to(&DataType::Float64, df.schema())?.alias("amount"),
    ])?;

    // Verify the canonical schema has the expected types
    let schema = canonical_df.schema();
    assert_eq!(
        schema.field_with_unqualified_name("id")?.data_type(),
        &DataType::Int64
    );

    assert_batches_eq!(
        &[
            "+----+--------+",
            "| id | amount |",
            "+----+--------+",
            "| 1  | 99.99  |",
            "| 2  | 150.5  |",
            "| 3  | 75.25  |",
            "| 4  | 200.0  |",
            "+----+--------+",
        ],
        &canonical_df.collect().await?
    );

    Ok(())
}
```

:::{admonition} Parquet metadata and schema skipping
:class: tip
DataFusion skips file-level schema metadata by default when reading Parquet. If your pipeline relies on metadata (PII flags, lineage annotations), set `.skip_metadata(false)` on [`ParquetReadOptions`]. For how metadata is attached during schema creation, see [Attaching Metadata](schema-creation.md#attaching-metadata).
:::

Auto-merge handles schema evolution across files. For partitioned datasets, schema application involves an additional dimension: partition columns that live outside the file schema.

---

## Partitioned Datasets with ListingTable

**Hive-style partitioning lets DataFusion skip entire directories based on query filters — but partition columns must be declared separately from the file schema.**

[`ListingTable`] reads directories like `/data/events/year=2024/month=01/...` and uses query filters to skip non-matching partitions entirely. The file schema describes the columns *inside* each file; partition columns are declared via [`with_table_partition_cols()`] and must match the directory nesting order.

```rust,no_run
// no_run: requires a filesystem with Hive-style partition layout
use std::sync::Arc;
use datafusion::prelude::*;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();

    // 1. Schema of the DATA INSIDE the Parquet files
    //    Do NOT include partition columns here
    let file_schema = Arc::new(Schema::new(vec![
        Field::new("event_id", DataType::Utf8, false),
        Field::new("payload", DataType::Utf8, true),
    ]));

    // 2. Describe directory structure — partition column order must match nesting
    let listing_options = ListingOptions::new(Arc::new(ParquetFormat::default()))
        .with_file_extension("parquet")
        .with_table_partition_cols(vec![
            ("year".into(),  DataType::Int32),
            ("month".into(), DataType::Int8),
        ]);

    // 3. Build the ListingTable and register it
    let config = ListingTableConfig::new(ListingTableUrl::parse("/data/events")?)
        .with_listing_options(listing_options)
        .with_schema(file_schema);
    let table = Arc::new(ListingTable::try_new(config)?);
    ctx.register_table("events", table)?;

    // 4. Query with a filter — DataFusion prunes partitions automatically
    let df = ctx.sql("SELECT * FROM events WHERE year = 2024").await?;

    // Verify pruning with EXPLAIN
    df.explain(false, false)?.show().await?;

    Ok(())
}
```

:::{admonition} Verify partition pruning
:class: tip
Use [`.explain()`] to confirm that partition filters appear in the plan. If they don't, check that the filter column matches a declared partition column name and type exactly.
:::

---

## Conclusion

**Applying a schema is the bridge between definition and execution — the mechanism differs by format, but the pattern is consistent: pass the schema to the read options and let DataFusion enforce it.**

Text formats (CSV, NDJSON) benefit most from explicit schemas because they carry no type information. Self-describing formats (Parquet, Avro, Arrow IPC) embed their own schemas but require normalization when files evolve independently. Partitioned datasets add a structural dimension where partition columns live outside the file schema. For building the schemas themselves — fields, parameterized types, metadata, `DFSchema` — see [Creating Schemas](schema-creation.md).

### Further Reading

- [Creating Schemas](schema-creation.md) — constructing Arrow schemas from primitives through `DFSchema`
- [Schema Inference](schema-inference.md) — the inference path and its failure modes
- [Inspecting and Validating Schemas](schema-inspection.md) — checking a schema before execution
- [Schema Concepts](schema-concepts.md) — the schema contract, primary vs. secondary metadata
- [Anatomy of a Schema](schema-anatomy.md) — field-level reference for [`DataType`], nullability, metadata

<!-- Link references -->

[`schema_infer_max_records`]: https://docs.rs/datafusion/latest/datafusion/datasource/file_format/options/struct.CsvReadOptions.html#method.schema_infer_max_records
[`has_header`]: https://docs.rs/datafusion/latest/datafusion/datasource/file_format/options/struct.CsvReadOptions.html#method.has_header
[`truncated_rows`]: https://docs.rs/datafusion/latest/datafusion/datasource/file_format/options/struct.CsvReadOptions.html#method.truncated_rows
[`ctx.read_parquet()`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.read_parquet
[`ParquetReadOptions`]: https://docs.rs/datafusion/latest/datafusion/datasource/file_format/options/struct.ParquetReadOptions.html
[`ListingTable`]: https://docs.rs/datafusion/latest/datafusion/datasource/listing/struct.ListingTable.html
[`DataType`]: https://docs.rs/arrow-schema/latest/arrow_schema/enum.DataType.html
