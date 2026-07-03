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

# Applying Explicit Schemas at Read Time

**Applying schemas at read time turns raw input into `DataFrame`s with declared contracts instead of inferred guesses.**

Schemas give data its structure, turning raw values into typed, queryable columns. Explicit schemas make data flows reliable, inspectable, and auditable by declaring column names, Arrow types, nullability, and metadata before execution begins. [Creating Schemas](schema-creation.md) shows how to declare those contracts; this page shows how to apply them when a reader or table provider creates a lazy `DataFrame`.

The following sections cover formats without embedded schemas (CSV and JSON), self-describing formats with canonical schemas (Parquet, Avro, and Arrow IPC), schema metadata, and partitioned reads. The examples show how DataFusion attaches declared schemas at the read boundary, how the resulting `DFSchema` behaves, and where mismatches fail before downstream transformations rely on the wrong contract.

**Key methods:**

| Method                                                         | Purpose                                                    | Section                                                                                         |
| :------------------------------------------------------------- | :--------------------------------------------------------- | :---------------------------------------------------------------------------------------------- |
| [`CsvReadOptions::schema()`]                                   | Attach an explicit schema to CSV reads                     | [CSV Positional Alignment](#csv-positional-alignment)                                           |
| [`NdJsonReadOptions::schema()`]                                | Attach an explicit schema to JSON reads                    | [JSON Name-Based Alignment](#json-name-based-alignment)                                         |
| [`ParquetReadOptions::schema()`]                               | Read Parquet with a canonical schema                       | [Applying a Canonical Schema to Parquet](#applying-a-canonical-schema-to-parquet)               |
| [`AvroReadOptions::schema()`] / [`ArrowReadOptions::schema()`] | Attach canonical schemas to self-describing formats        | [Self-Describing Formats and Canonical Schemas](#self-describing-formats-and-canonical-schemas) |
| [`ParquetReadOptions::skip_metadata()`]                        | Preserve Parquet schema metadata during reads              | [Applying Schemas with Metadata](#applying-schemas-with-metadata)                               |
| [`ListingTableConfig::with_schema()`]                          | Set the file schema for a directory-backed table           | [Partitioned Datasets with ListingTable](#partitioned-datasets-with-listingtable)               |
| [`ListingOptions::with_table_partition_cols()`]                | Add path-derived partition columns outside the file schema | [Partitioned Datasets with ListingTable](#partitioned-datasets-with-listingtable)               |

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

```{contents} Table of Contents for Applying Explicit Schemas at Read Time
:local:
:depth: 2
```

## How Explicit Schemas Enter DataFrame Plans

**Readers create `DataFrame`s with resolved schemas; transformations create new `DataFrame`s with new `LogicalPlan`s.**

The schemas built in [Creating Schemas](schema-creation.md) become useful when a reader or table provider uses them to create a `DataFrame`. DataFusion accepts an Arrow [`Schema`] through format-specific read options, resolves that source schema, builds a [`LogicalPlan`], and exposes the resulting [`DFSchema`] through `df.schema()`.

If a source value cannot be parsed into the declared type, DataFusion fails instead of silently changing the contract. Once a `DataFrame` exists, later schema changes come from new lazy plans: `.select()` builds a plan with a different column order or aliases, `.cast_to()` adds type conversions, and methods such as `.with_column()` add projected fields. Those transformation patterns belong in [Transforming Schemas](schema-transformation.md) and [Schema Management with DataFrame methods](schema-dataframe-methods.md). This document stays at the boundary where a schema is attached to input data and then normalized for downstream work.

:::{admonition} Normalization after read is lazy
:class: note
If the attached source schema is not the final shape your application needs, use `.select()`, aliases, and casts to build a new `DataFrame` plan. The original `DataFrame` is not mutated, and no data is processed until an action such as `.collect()` runs.
:::

### When Source Values Do Not Match the Schema

An explicit schema is a contract, not a suggestion. DataFusion can create the lazy `DataFrame` with the declared schema, but source values are still parsed when an action reads the data. If a value cannot be converted to the declared Arrow type, the action returns an error instead of widening the schema or changing the column type.

The example declares `reading` as `Int64`, then writes a CSV row where that field contains `not_an_int`. The call to `ctx.read_csv()` attaches the schema and creates the lazy `DataFrame`; the call to `.collect()` performs the read and returns the parsing error. The assertion checks the contract boundary without depending on a specific error message.

```rust
use datafusion::prelude::*;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
# use std::fs::File;
# use std::io::Write;
# use tempfile::tempdir;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();
    # let dir = tempdir()?;
    # let csv_path = dir.path().join("readings.csv");
    # let mut file = File::create(&csv_path)?;
    # writeln!(file, "sensor_id,reading")?;
    # writeln!(file, "A-1,not_an_int")?;

    // Declare the contract the DataFrame must expose.
    let readings_schema = Schema::new(vec![
        Field::new("sensor_id", DataType::Utf8, false),
        Field::new("reading", DataType::Int64, false),
    ]);

    let path = "readings.csv";
    # let path = csv_path.to_string_lossy().to_string();
    // Build the lazy DataFrame with the declared schema.
    let readings_df = ctx.read_csv(
        path,
        CsvReadOptions::new()
            .schema(&readings_schema)
            .has_header(true),
    ).await?;

    // Execute the read. The invalid Int64 value fails here.
    let result = readings_df.collect().await;
    assert!(result.is_err());

    Ok(())
}
```

---

## Text Formats Without Embedded Schemas: CSV and JSON

**Explicit schemas matter most when the file format provides no schema of its own; for CSV and JSON, inference is useful for exploration but fragile for production contracts.**

DataFusion creates source schemas in two broad ways. Self-describing formats such as Parquet, Avro, and Arrow IPC provide embedded metadata that DataFusion reads directly. CSV and JSON provide rows of values without a reliable column contract, so DataFusion samples rows to infer names and types unless you provide a schema. CSV and JSON both use a default sample size of 1,000 records, configurable through [`CsvReadOptions::schema_infer_max_records()`] and [`NdJsonReadOptions::schema_infer_max_records()`]. For the failure modes behind inference, see [Schema Inference](schema-inference.md).

Attaching a schema to CSV or JSON defines the `DataFrame` schema for that read; it does not modify the source file or persist field names, data types, nullability, or metadata back into the data. Reattach the schema on each read, keep the contract in code or a schema registry, or store the data in a self-describing format when the schema must travel with the file.

| Format   | Alignment  | Main schema behavior                                                                          |
| :------- | :--------- | :-------------------------------------------------------------------------------------------- |
| **CSV**  | Positional | Schema fields map to columns by order; headers can be skipped but do not control field names. |
| **JSON** | Name-based | JSON object keys map to schema fields by name; field order does not matter.                   |

DataFusion's JSON reader consumes newline-delimited JSON objects through [`ctx.read_json()`] and [`NdJsonReadOptions`]. The documentation uses "JSON" for the user-facing format name and `NdJsonReadOptions` for the Rust API type.

### CSV Positional Alignment

CSV is a plain-text row format, often with the first row used as a header. When you provide an explicit schema, DataFusion maps CSV data columns to schema fields by position: the first data column maps to the first schema field, the second data column maps to the second schema field, and so on. [`CsvReadOptions::has_header()`] tells DataFusion whether the first row is a header row instead of data. During inference, that header can provide column names; with an explicit schema, the schema controls field names and types.

| Behavior          | What happens                                                                                                                                                                             |
| :---------------- | :--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Type parsing**  | Raw text is parsed into declared Arrow types such as `Int64`, `Utf8`, or `Decimal128(19, 2)`.                                                                                            |
| **Null markers**  | Empty CSV fields are parsed as NULL by default; use [`CsvReadOptions::null_regex()`] with anchored regex patterns such as `^NA$`, or a raw Rust string literal like `r"^\\N$"` for `\N`. |
| **Short rows**    | Missing trailing fields fail by default; [`CsvReadOptions::truncated_rows(true)`] fills nullable fields with NULL values.                                                                |
| **Extra columns** | Extra fields remain a row-length mismatch; fix the input, adjust the schema, or read with the correct delimiter and quoting options.                                                     |

For delimiter, quoting, null-marker, and row-shape options, see [Reading CSV Files](../Creating-DataFrames/from-files/csv.md#formatting-and-structure).

The schema attached to the CSV reader defines how raw text becomes typed columns. If the source stores Unix timestamps as integers, read them as integers first, then build a new `DataFrame` plan that casts the column to a timestamp.

```rust
use datafusion::prelude::*;
use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
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
    # writeln!(file, "order_id,customer_id,created_at_epoch")?;
    # writeln!(file, "1001,CUST-001,1735689600")?;
    # writeln!(file, "1002,CUST-002,1735821000")?;

    // Attach a canonical schema so Unix seconds are read as Int64.
    let sales_schema = Schema::new(vec![
        Field::new("order_id", DataType::Int64, false),
        Field::new("customer_id", DataType::Utf8, false),
        Field::new("created_at_epoch", DataType::Int64, false),
    ]);

    let path = "sales.csv";
    # let path = csv_path.to_string_lossy().to_string();
    let sales_df = ctx.read_csv(path, CsvReadOptions::new()
        .schema(&sales_schema)
        .has_header(true)
    ).await?;
    // The schema is attached to the DataFrame and can be accessed through `.schema()`.
    assert_eq!(
        sales_df.schema().field_with_unqualified_name("created_at_epoch")?.data_type(),
        &DataType::Int64
    );

    assert_batches_eq!(
        &[
            "+----------+-------------+------------------+",
            "| order_id | customer_id | created_at_epoch |",
            "+----------+-------------+------------------+",
            "| 1001     | CUST-001    | 1735689600       |",
            "| 1002     | CUST-002    | 1735821000       |",
            "+----------+-------------+------------------+",
        ],
        &sales_df.clone().collect().await?
    );

    let normalized_df = sales_df.select(vec![
        col("order_id"),
        col("customer_id"),
        cast(
            col("created_at_epoch"),
            DataType::Timestamp(TimeUnit::Second, None),
        ).alias("created_at"),
    ])?;

    assert_eq!(
        normalized_df.schema().field_with_unqualified_name("created_at")?.data_type(),
        &DataType::Timestamp(TimeUnit::Second, None)
    );

    assert_batches_eq!(
        &[
            "+----------+-------------+---------------------+",
            "| order_id | customer_id | created_at          |",
            "+----------+-------------+---------------------+",
            "| 1001     | CUST-001    | 2025-01-01T00:00:00 |",
            "| 1002     | CUST-002    | 2025-01-02T12:30:00 |",
            "+----------+-------------+---------------------+",
        ],
        &normalized_df.collect().await?
    );

    Ok(())
}
```

:::{admonition} Inference pitfalls in CSV
:class: warning
Schema inference samples only the first 1,000 rows by default. Common pitfalls include identifiers inferred as `Int32` before later values overflow, currency inferred as `Float64`, and sparse columns missing from the inferred schema. Attach explicit schemas to CSV reads when downstream code depends on stable types.
:::

### JSON Name-Based Alignment

JSON uses **name-based** mapping: object keys match schema field names, so field order does not matter. That makes JSON more flexible than CSV when producers reorder fields or omit optional attributes, but it also means the schema must decide which keys become columns and which values are valid.

Attach an explicit schema when the JSON source has a stable contract. The schema defines the output columns, their Arrow types, and their nullability. Missing keys become NULL when the declared field is nullable, extra keys outside the schema are not included in the resulting `DataFrame`, and values that cannot be parsed into the declared type fail the read.

| Behavior                  | What happens                                                                        |
| :------------------------ | :---------------------------------------------------------------------------------- |
| **Field alignment**       | JSON object keys match schema fields by name; field order does not matter.          |
| **Declared output**       | Schema fields determine the columns and Arrow types in the resulting `DataFrame`.   |
| **Missing nullable keys** | Missing keys produce NULL values when the corresponding schema field is nullable.   |
| **Extra keys**            | Keys outside the explicit schema are ignored and do not become `DataFrame` columns. |
| **Invalid values**        | Values fail when they cannot be parsed into the declared Arrow type.                |

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

    let user_schema = Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
    ]);

    let path = "data.json";
    # let path = json_path.to_string_lossy().to_string();
    let df = ctx.read_json(path, NdJsonReadOptions::default()
        .schema(&user_schema)
    ).await?;

    // Row 2 has no "name" key, so the nullable field becomes NULL.
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

#### JSON Nested Fields

JSON sources can carry nested objects and arrays that CSV cannot represent directly. Those shapes are useful for event metadata, tags, properties, and source-specific payloads, but they need an explicit Arrow shape when downstream code expects stable nested access.

Use `DataType::Struct` for nested JSON objects and `DataType::List` for JSON arrays. The schema keeps the nested values in a single column, and a later `DataFrame` projection can extract the fields that a query needs. This separates the read contract from the projection contract: the reader preserves the nested structure, while `get_field()` and `array_element()` decide which nested values become top-level columns.

| Behavior                | What happens                                                               |
| :---------------------- | :------------------------------------------------------------------------- |
| **Nested object**       | Declare the JSON object as `DataType::Struct(...)`.                        |
| **Array**               | Declare repeated JSON values as `DataType::List(...)`.                     |
| **Struct access**       | Use `get_field(col("metadata"), "source")` to read a named nested field.   |
| **List access**         | Use `array_element(col("tags"), lit(1_i64))` to read a list element.       |
| **Nullable structures** | Missing nullable nested fields or missing nullable structures become NULL. |

```rust
use std::sync::Arc;
use datafusion::prelude::*;
use datafusion::arrow::datatypes::{DataType, Field, Fields, Schema};
use datafusion::assert_batches_eq;
# use std::fs::File;
# use std::io::Write;
# use tempfile::tempdir;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();
    # let dir = tempdir()?;
    # let json_path = dir.path().join("events.json");
    # let mut file = File::create(&json_path)?;
    # writeln!(file, r#"{{"event_id":"evt-1","metadata":{{"source":"api","version":2}},"tags":["paid","mobile"]}}"#)?;
    # writeln!(file, r#"{{"event_id":"evt-2","metadata":{{"source":"batch","version":1}},"tags":["trial"]}}"#)?;

    // Declare nested Arrow types that match the JSON object and array shape.
    let metadata_type = DataType::Struct(Fields::from(vec![
        Field::new("source", DataType::Utf8, true),
        Field::new("version", DataType::Int32, true),
    ]));
    let tags_type = DataType::List(Arc::new(Field::new("item", DataType::Utf8, true)));

    let event_schema = Schema::new(vec![
        Field::new("event_id", DataType::Utf8, false),
        Field::new("metadata", metadata_type, true),
        Field::new("tags", tags_type, true),
    ]);

    let path = "events.json";
    # let path = json_path.to_string_lossy().to_string();
    // Attach the nested schema at the JSON read boundary.
    let events_df = ctx.read_json(path, NdJsonReadOptions::default()
        .schema(&event_schema)
    ).await?;

    // Build a lazy projection that extracts values from Struct and List columns.
    let projected_df = events_df.select(vec![
        col("event_id"),
        get_field(col("metadata"), "source").alias("source"),
        array_element(col("tags"), lit(1_i64)).alias("first_tag"),
    ])?;

    assert_batches_eq!(
        &[
            "+----------+--------+-----------+",
            "| event_id | source | first_tag |",
            "+----------+--------+-----------+",
            "| evt-1    | api    | paid      |",
            "| evt-2    | batch  | trial     |",
            "+----------+--------+-----------+",
        ],
        &projected_df.collect().await?
    );

    Ok(())
}
```

:::{admonition} Nested JSON edge cases
:class: warning
`array_element()` uses 1-based indexes, so `lit(1_i64)` reads the first list element. Out-of-range list access returns NULL, and JSON keys that are not declared in the schema are not projected into the `DataFrame`. If nested data is required, mark the parent structure and required child fields as non-nullable so invalid or incomplete records fail early.
:::

Text formats are the most common case for explicit schemas. Self-describing formats carry their own schema, but canonical schemas still matter when a pipeline must pin the expected contract.

---

## Self-Describing Formats and Canonical Schemas

**Self-describing formats remove row-sampling inference, but canonical schemas still pin the table contract when files evolve, metadata matters, or multiple files disagree.**

Self-describing formats store schema metadata in the file, so DataFusion does not need to sample rows to discover column names and types. The risk moves from inference to compatibility: directories can contain files with different schemas, metadata can be skipped or preserved differently, and embedded schemas may not match the contract a pipeline wants to expose. Supplying a canonical schema tells the reader which table schema to use and can attach metadata that downstream code expects.

| Format        | Default schema source            | Attach schema with               | Notes                                                                  |
| :------------ | :------------------------------- | :------------------------------- | :--------------------------------------------------------------------- |
| **Parquet**   | File footer metadata             | [`ParquetReadOptions::schema()`] | Most common analytical format; can skip or preserve schema metadata.   |
| **Avro**\*    | Avro schema embedded in the file | [`AvroReadOptions::schema()`]    | Useful for row-oriented interchange with an explicit writer schema.    |
| **Arrow IPC** | Arrow schema in the stream/file  | [`ArrowReadOptions::schema()`]   | Best for Arrow-native interchange and metadata-preserving round trips. |

\* Requires DataFusion's `avro` feature.

:::{admonition} Schema merging is not type coercion
:class: caution
When DataFusion reads multiple self-describing files, the file format merges compatible schemas with Arrow [`Schema::try_merge()`]. The merge can add fields and relax nullability, but it does **not** widen primitive type conflicts such as `Int32` and `Int64` for the same field. DataFusion's type coercion rules apply later inside the [`LogicalPlan`]; see [Type Coercion](type-coercion.md).
:::

### Applying a Canonical Schema to Parquet

Parquet is the most common case for canonical schemas because production datasets often span many files and versions. Use [`ParquetReadOptions::schema()`] when the file metadata is valid but your pipeline needs to pin the table contract used by the reader.

The example below writes an older Parquet file without a `region` column, then reads it with a canonical schema that includes `region` as nullable. The result demonstrates contract pinning: the reader exposes the expected column even when older files do not contain it. If the canonical schema marks a missing column as non-nullable, DataFusion returns an error instead of filling the column with NULL values.

```rust
use datafusion::prelude::*;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::assert_batches_eq;
# use tempfile::TempDir;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    // Older files may predate a nullable column that the pipeline now expects.
    let old_orders_df = dataframe!(
        "order_id" => [1001_i64, 1002_i64],
        "amount" => [99.99_f64, 150.50_f64],
    )?;

    # let temp_dir = TempDir::new()?;
    # let parquet_path = temp_dir.path().join("orders.parquet");
    let path = "orders.parquet";
    # let path = parquet_path.to_str().unwrap();
    # // Default write options are sufficient for this temporary fixture.
    old_orders_df.write_parquet(path, Default::default(), None).await?;

    // Pin the reader contract expected by downstream code.
    let canonical_schema = Schema::new(vec![
        Field::new("order_id", DataType::Int64, false),
        Field::new("region", DataType::Utf8, true),
        Field::new("amount", DataType::Float64, false),
    ]);

    let ctx = SessionContext::new();
    let orders_df = ctx.read_parquet(
        path,
        ParquetReadOptions::default().schema(&canonical_schema),
    ).await?;

    assert_batches_eq!(
        &[
            "+----------+--------+--------+",
            "| order_id | region | amount |",
            "+----------+--------+--------+",
            "| 1001     |        | 99.99  |",
            "| 1002     |        | 150.5  |",
            "+----------+--------+--------+",
        ],
        &orders_df.collect().await?
    );

    Ok(())
}
```

:::{admonition} Default Parquet read options
:class: note
`ParquetReadOptions::new()` and `ParquetReadOptions::default()` both create default read options. This page uses `default()` because it matches the surrounding DataFrame documentation, but either form is valid.
:::

Canonical schemas pin the field contract used for the read. The next section shows how the same attached schema can also carry metadata that downstream tools preserve or inspect.

---

## Applying Schemas with Metadata

**Schema metadata attached at read time is preserved on the resulting `DataFrame`; whether it survives file round trips depends on the storage format.**

Metadata is semantic context attached to a field or schema: units, source systems, schema versions, PII labels, or business definitions. Arrow stores metadata as free-form `HashMap<String, String>` values on individual fields and on the schema as a whole. DataFusion preserves those keys on the resulting `DataFrame` schema, but it does not interpret or validate them, so misspelled keys and inconsistent vocabularies remain application-level risks.

Metadata is created in the schema itself, not in the read options. When you attach a [`Schema`] with field-level or schema-level metadata to a reader, the resulting `DataFrame` carries that metadata just like it carries field names, data types, and nullability. For constructing schemas with metadata, see [Attaching Metadata](schema-creation.md#attaching-metadata).

```rust
use std::collections::HashMap;
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
    # let csv_path = dir.path().join("users.csv");
    # let mut file = File::create(&csv_path)?;
    # writeln!(file, "user_id,email")?;
    # writeln!(file, "1,alice@example.com")?;

    // Field-level metadata describes one column.
    let user_id = Field::new("user_id", DataType::Int64, false);
    let email = Field::new("email", DataType::Utf8, true)
        .with_metadata(HashMap::from([
            ("pii".to_string(), "true".to_string()),
        ]));

    // Schema-level metadata describes the dataset contract.
    let users_schema = Schema::new_with_metadata(
        vec![user_id, email],
        HashMap::from([
            ("schema_version".to_string(), "v1".to_string()),
        ]),
    );

    let path = "users.csv";
    # let path = csv_path.to_string_lossy().to_string();
    let users_df = ctx.read_csv(path, CsvReadOptions::new()
        .schema(&users_schema)
        .has_header(true)
    ).await?;

    assert_eq!(
        users_df.schema().metadata().get("schema_version"),
        Some(&"v1".to_string())
    );
    assert_eq!(
        users_df.schema().field_with_unqualified_name("email")?.metadata().get("pii"),
        Some(&"true".to_string())
    );

    assert_batches_eq!(
        &[
            "+---------+-------------------+",
            "| user_id | email             |",
            "+---------+-------------------+",
            "| 1       | alice@example.com |",
            "+---------+-------------------+",
        ],
        &users_df.collect().await?
    );

    Ok(())
}
```

When Parquet schema metadata must survive a file round trip, call [`ParquetReadOptions::skip_metadata()`] with `false` so the reader preserves metadata stored in the Parquet footer.

Metadata controls meaning, not execution constraints. For optimizer-visible constraints, use DataFusion's constraint APIs rather than field metadata; see [Defining a `DFSchema` Directly](schema-creation.md#defining-a-dfschema-directly).

Metadata is one kind of schema context attached at read time. Partitioned datasets add another: columns derived from directory paths rather than from file contents.

---

## Partitioned Datasets with ListingTable

**Partitioned datasets build one `DataFrame` schema from two sources: file columns from file contents and partition columns from directory paths.**

Directory-backed datasets often behave like one table even when their columns come from two places: file contents and directory paths. The schema is the logical contract that unifies those sources, letting [`ListingTable`] expose matching files as one queryable table. Hive-style layouts encode partition values in directory names such as `/data/events/year=2024/month=01/...`.

Use [`ListingTableConfig::with_schema()`] to supply the file `SchemaRef`: fields physically stored in each file. Use [`ListingOptions::with_table_partition_cols()`] to supply the partition fields: names and Arrow types parsed from path segments. `ListingTable` combines both sources into the provider's table schema; when `ctx.table("events")` creates the `DataFrame`, DataFusion wraps that Arrow schema as the plan's `DFSchema`.

That resulting `DFSchema` is why expressions can reference `year` and `month` like ordinary columns: the fields exist in the `DataFrame` schema, while the values come from directory names. Once DataFusion knows the partition column names and types, a `.filter()` predicate in the `LogicalPlan` can prune directories before file scanning begins. If the declared names, types, or nesting order do not match the directory layout, partition values may parse incorrectly and filters may scan more directories than expected.

```rust,no_run
// no_run: requires a filesystem with Hive-style partition layout.
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

    // File schema: columns physically stored in each Parquet file.
    // ListingTable combines this SchemaRef with the partition fields below.
    let file_schema = Arc::new(Schema::new(vec![
        Field::new("event_id", DataType::Utf8, false),
        Field::new("payload", DataType::Utf8, true),
    ]));

    // Partition schema for paths such as:
    // /data/events/year=2024/month=01/part.parquet
    let listing_options = ListingOptions::new(Arc::new(ParquetFormat::default()))
        .with_table_partition_cols(vec![
            ("year".into(),  DataType::Int32),
            ("month".into(), DataType::Int32),
        ]);

    let config = ListingTableConfig::new(ListingTableUrl::parse("file:///data/events/")?)
        .with_listing_options(listing_options)
        .with_schema(file_schema);
    let table = Arc::new(ListingTable::try_new(config)?);
    ctx.register_table("events", table)?;

    // The resulting DataFrame schema includes event_id, payload, year, and month.
    let df = ctx.table("events").await?
        .filter(col("year").eq(lit(2024_i32)))?;

    // Inspect the physical plan: DataSourceExec should list only file groups
    // under year=2024 when partition pruning is active.
    df.explain(false, false)?.show().await?;

    Ok(())
}
```

:::{admonition} Verify partition pruning
:class: tip
Partition pruning is the optimization that skips directories whose path-derived values cannot satisfy a filter. In the example above, `col("year").eq(lit(2024_i32))` should avoid listing or scanning files under other `year=...` directories. Use [`.explain()`] to inspect the physical plan: the `DataSourceExec` file groups should be restricted to matching partition paths, not followed by a late filter over every listed file. If pruning is missing, verify the partition column name, data type, and directory order.
:::

---

## Conclusion & Further Reading

**Applying schemas at the read boundary means matching each source of column meaning to the API that owns it.**

Raw input becomes reliable only after DataFusion knows the column names, data types, nullability, metadata, and partition fields that define the `DataFrame` contract. The complication is that those facts do not always come from one place: CSV and JSON need explicit reader schemas, self-describing formats may still need canonical schemas across files, Parquet metadata must be preserved intentionally, and Hive-style datasets carry some columns in paths rather than file contents.

Use explicit schemas for text formats, canonical schemas for self-describing datasets, schema metadata for semantic context, and separate partition-column declarations for directory layouts. After reading, inspect the resulting schema before relying on downstream transformations.

### Further Reading

Continue with these pages when you need to define, infer, inspect, or transform schemas after the read boundary.

- [Creating Schemas](schema-creation.md) — defining Arrow `Schema` and `DFSchema` contracts before reading
- [Schema Inference](schema-inference.md) — how CSV and JSON sampling derives schemas and where inference is risky
- [Inspecting and Validating Schemas](schema-inspection.md) — checking the applied `DFSchema` before execution
- [Type Coercion](type-coercion.md) — how the analyzer widens expression types and when explicit casts are needed
- [Transforming Schemas](schema-transformation.md) — changing qualifiers, combining schemas, and adapting existing `DFSchema` values

<!-- Link references -->

[`ArrowReadOptions::schema()`]: https://docs.rs/datafusion/latest/datafusion/datasource/file_format/options/struct.ArrowReadOptions.html#method.schema
[`AvroReadOptions::schema()`]: https://docs.rs/datafusion/latest/datafusion/datasource/file_format/options/struct.AvroReadOptions.html#method.schema
[`CsvReadOptions::has_header()`]: https://docs.rs/datafusion/latest/datafusion/datasource/file_format/options/struct.CsvReadOptions.html#method.has_header
[`CsvReadOptions::null_regex()`]: https://docs.rs/datafusion/latest/datafusion/datasource/file_format/options/struct.CsvReadOptions.html#method.null_regex
[`CsvReadOptions::schema()`]: https://docs.rs/datafusion/latest/datafusion/datasource/file_format/options/struct.CsvReadOptions.html#method.schema
[`CsvReadOptions::schema_infer_max_records()`]: https://docs.rs/datafusion/latest/datafusion/datasource/file_format/options/struct.CsvReadOptions.html#method.schema_infer_max_records
[`CsvReadOptions::truncated_rows(true)`]: https://docs.rs/datafusion/latest/datafusion/datasource/file_format/options/struct.CsvReadOptions.html#method.truncated_rows
[`DFSchema`]: https://docs.rs/datafusion/latest/datafusion/common/struct.DFSchema.html
[`ListingOptions::with_table_partition_cols()`]: https://docs.rs/datafusion/latest/datafusion/datasource/listing/struct.ListingOptions.html#method.with_table_partition_cols
[`ListingTable`]: https://docs.rs/datafusion/latest/datafusion/datasource/listing/struct.ListingTable.html
[`ListingTableConfig::with_schema()`]: https://docs.rs/datafusion/latest/datafusion/datasource/listing/struct.ListingTableConfig.html#method.with_schema
[`LogicalPlan`]: https://docs.rs/datafusion/latest/datafusion/logical_expr/enum.LogicalPlan.html
[`NdJsonReadOptions`]: https://docs.rs/datafusion/latest/datafusion/datasource/file_format/options/struct.NdJsonReadOptions.html
[`NdJsonReadOptions::schema()`]: https://docs.rs/datafusion/latest/datafusion/datasource/file_format/options/struct.NdJsonReadOptions.html#method.schema
[`NdJsonReadOptions::schema_infer_max_records()`]: https://docs.rs/datafusion/latest/datafusion/datasource/file_format/options/struct.NdJsonReadOptions.html#method.schema_infer_max_records
[`ParquetReadOptions::schema()`]: https://docs.rs/datafusion/latest/datafusion/datasource/file_format/options/struct.ParquetReadOptions.html#method.schema
[`ParquetReadOptions::skip_metadata()`]: https://docs.rs/datafusion/latest/datafusion/datasource/file_format/options/struct.ParquetReadOptions.html#method.skip_metadata
[`Schema`]: https://docs.rs/arrow-schema/latest/arrow_schema/struct.Schema.html
[`Schema::try_merge()`]: https://docs.rs/arrow-schema/latest/arrow_schema/struct.Schema.html#method.try_merge
[`ctx.read_json()`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.read_json
[`.explain()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.explain
