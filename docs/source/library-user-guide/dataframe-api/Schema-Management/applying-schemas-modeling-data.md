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

# Applying Schemas and Modeling Data

<!--TODO

1. ABSTRACT
2. INTRODUCTION
-->

```{contents} Applying Schemas and Modeling Data
:local:
:depth: 2
```

## Introduction (placeholder)

## Applying Schemas and Modeling Data

A schema defines the structure of your data—column names, types, nullability, and nested structures. Applying schemas when reading files enables planning-time validation, improves query performance, and ensures data quality. This section covers schema strategies for different file formats, handling schema evolution, partition pruning, and modeling nested data. <br> **See also:**

- [Data Model & Schema](./concepts.md#data-model--schema) for fundamentals and
- [Creating DataFrames](./creating-dataframes.md) for file reading basics.

**Jump to:**

- [CSV](#strategy-text-formats)
- [NDJSON](#strategy-text-formats)
- [Parquet](#strategy-self-describing-formats)
- [Partitions](#strategy-partitioned-datasets)
- [Nested Data](#strategy-nested-data)

(strategy-text-formats)=

### Strategy 1: Text Formats (CSV & NDJSON) — Enforce Schemas

**Text formats don't embed type information—provide an explicit schema for production workloads to prevent inference drift.**

Without a schema, DataFusion infers types from a sample of rows ([`schema_infer_max_records`], default 1,000). Providing a schema enforces a contract on the raw data.

| Format     |   Alignment    | Key Behaviors                                                                                  |
| :--------- | :------------: | :--------------------------------------------------------------------------------------------- |
| **CSV**    | **Positional** | Fields map to schema columns by order. Header names are read but position determines mapping   |
| **NDJSON** | **Name-based** | JSON keys match schema field names. Order doesn't matter. Missing keys → NULL, extra → ignored |

#### CSV — Positional Alignment

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

> **Warning:** <br>
> Schema inference samples only the first 1,000 rows by default ([`schema_infer_max_records`]). Common pitfalls: IDs inferred as `Int32` then overflow, currency inferred as `Float64` (rounding errors), sparse columns inferred as `Utf8`. Always provide explicit schemas for CSV in production.

#### NDJSON — Name-Based Alignment with Flexible Structure

NDJSON uses **name-based** mapping: JSON keys match schema field names, so field order does not matter. Missing keys become NULL; extra keys are silently ignored.

Key behaviors:

- **Missing keys**: with an explicit schema, missing fields become NULL.
- **Extra keys**: keys not in the schema are ignored (no error, no column).
- **Types**: JSON values are cast to declared Arrow types; invalid casts raise errors.
- **Nested data**: supports `Struct`, `List`, `Map` (CSV cannot express nested types).

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

(strategy-self-describing-formats)=

### Strategy 2: Self-Describing Formats (Parquet/Avro/Arrow) — Merge & Normalize

**Self-describing formats embed schemas, but schemas evolve—DataFusion auto-merges them and you should normalize to canonical types.**

When reading multiple files with evolved schemas, DataFusion merges them automatically:

```text
v1_sales.parquet:  id (Int32),  amount (Decimal128(19,2))
v2_sales.parquet:  id (Int64),  amount (Decimal128(38,9)),  region (Utf8)

Merged result:     id (Int64),  amount (Decimal128(38,9)),  region (Utf8, nullable)
```

- Types are **widened**: `Int32` + `Int64` → `Int64`
- Columns are **added**: missing columns appear as nullable
- Use [`.read_parquet()`] with multiple file paths to trigger auto-merge

#### Defensive pattern: enforce a canonical schema

**After reading, normalize to your canonical schema using [`.select()`] and [`.cast_to()`]—decouple your pipeline from upstream schema changes.**

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

(strategy-partitioned-datasets)=

### Strategy 3: Partitioned Datasets — Pruning with ListingTable

**Hive-style partitioning lets DataFusion skip entire directories based on query filters.**

[`ListingTable`] reads directories like `/data/events/year=2024/month=01/...` and uses query filters to skip non-matching partitions entirely. Use [`.explain()`] to verify pruning in your plan. (See also: [Reading Explain Plans](../../user-guide/explain-usage.md))

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

(strategy-nested-data)=

### Strategy 4: Nested Data — Struct/List/Map Modeling

**Use Arrow's `Struct`, `List`, and `Map` types to model hierarchical data—avoid lossy flattening of JSON or Parquet sources.**

Define the schema with nested types and query nested fields using `get_field()` (structs) or `array_element()` (lists):

```rust
use std::sync::Arc;
use datafusion::prelude::*;
use datafusion::arrow::datatypes::{DataType, Field, Schema, Fields};

fn main() {
    // Struct: a nested object
    let metadata_type = DataType::Struct(Fields::from(vec![
        Field::new("source", DataType::Utf8, true),
        Field::new("version", DataType::Int32, true),
    ]));

    // List: variable-length array
    let tags_type = DataType::List(
        Arc::new(Field::new("tag", DataType::Utf8, true))
    );

    // Map: key-value pairs (List<Struct<key, value>> internally)
    let attributes_type = DataType::Map(
        Arc::new(Field::new("entries",
            DataType::Struct(Fields::from(vec![
                Field::new("key", DataType::Utf8, false),
                Field::new("value", DataType::Int64, true),
            ])),
            false
        )),
        false,
    );

    let schema = Schema::new(vec![
        Field::new("metadata", metadata_type, true),
        Field::new("tags", tags_type, true),
        Field::new("attributes", attributes_type, true),
    ]);
    assert_eq!(schema.fields().len(), 3);

    // Querying nested fields:
    let _source = get_field(col("metadata"), "source");          // Struct access
    let _filter = get_field(col("metadata"), "version").gt_eq(lit(2));
    let _first  = array_element(col("tags"), lit(1));            // List: 1-based
}
```

> **Tip:** <br>
> Use `LargeUtf8` or `LargeList` only when a single value might exceed 2 GB. DataFusion does not enforce key uniqueness in maps—handle duplicate keys in query logic if needed.

---

```

```
