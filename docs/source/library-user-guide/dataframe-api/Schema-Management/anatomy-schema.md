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

# The Anatomy of a DataFusion DataFrame Schema

<!--TODO

1. ABSTRACT
2. INTRODUCTION
-->

```{contents} Table of Contents for the Anatomy DataFrame Schema
:local:
:depth: 2
```

## Introduction (placeholder)

**Dissecting the schema reveals how DataFusion structures data: from the DataFrame down to each field's type, nullability, and meaning.**

Every DataFrame carries a [`DFSchema`] describing its columns and their properties. `DFSchema` wraps an Arrow [`Schema`] and adds query-planning context (table qualifiers and functional dependencies). Understanding the four field properties—name, type, nullability, and metadata—is key to diagnosing schema mismatch errors and handling data safely and performantly.

```text
┌───────────────────────────────────────────────────────────┐
│ DataFrame                                                 │
│   └── LogicalPlan         CURRENT TOPIC (YOU ARE HERE!)   │
│            └── DFSchema  <----┘                           │
│                 ├── inner: Arc<Schema>   (Arrow Schema)   │
│                 │        └── Field[]     (Arrow Fields)   │
│                 │             ├── name                    │
│                 │             ├── data_type               │
│                 │             ├── nullable                │
│                 │             └── metadata                │
│                 ├── field_qualifiers ([`TableReference`]) │
│                 └── functional_dependencies               │
└───────────────────────────────────────────────────────────┘
```

The example below creates a DataFrame using the `dataframe!` macro, casts a column, and inspects the resulting schema. To access the underlying Arrow Schema, use [`.inner()`] (returns [`&SchemaRef`]) or [`.as_arrow()`] (returns [`&Schema`]).

```rust
use datafusion::prelude::*;
use datafusion::arrow::datatypes::{DataType, TimeUnit};

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let df = dataframe!(
        "user_id"    => [1_i64],
        "email"      => [Some("alice@example.com")],
        "created_at" => [1735689600_i64],
        "active"     => [true]
    )?;

    // Cast created_at from Int64 to Timestamp
    let df = df.with_column(
        "created_at",
        cast(col("created_at"), DataType::Timestamp(TimeUnit::Second, None))
    )?;

    // Print the schema structure
    println!("{:#?}", df.schema().inner());

    Ok(())
}
```

Output — each field shows its four properties (name, data_type, nullable, metadata):

```text
Schema {
    fields: [
        Field { name: "user_id", data_type: Int64, nullable: true, metadata: {} },
        Field { name: "email", data_type: Utf8, nullable: true, metadata: {} },
        Field { name: "created_at", data_type: Timestamp(Second, None), nullable: true, metadata: {} },
        Field { name: "active", data_type: Boolean, nullable: true, metadata: {} },
    ],
    metadata: {},
}
```

> **Why types matter:** <br>
> Correct types unlock query optimization. A `Timestamp` column enables date-range pruning, while the same bytes as `Int64` only support numeric comparisons.

---

### Schema Field Properties

**Understanding schema fields is essential for debugging mismatches and designing robust pipelines.**

In the hierarchy of the DataFrame Schema, we are now at the [`Field`] level.

> **Important:**<br>
> `Field` is an **Arrow type** ( [`arrow::datatypes::Field`]), not a DataFusion type. DFSchema _wraps_ an Arrow [`Schema`] and adds query-planning context on top.

```text
┌───────────────────────────────────────────────────────────┐
│ DataFrame                                                 │
│   └── LogicalPlan                                         │
│            └── DFSchema                                   │
│                 ├── inner: Arc<Schema>   ← Arrow Schema   │
│                 │        └── Field[]     ← CURRENT TOPIC  │
│                 │             ├── name    (YOU ARE HERE!) │
│                 │             ├── data_type               │
│                 │             ├── nullable                │
│                 │             └── metadata                │
│                 ├── field_qualifiers: Vec<TableReference> │
│                 └── functional_dependencies               │
└───────────────────────────────────────────────────────────┘
```

**What DFSchema adds to Arrow Schema:**

| Component                 | Access via                                                                                                            | Purpose                                                                  |
| :------------------------ | :-------------------------------------------------------------------------------------------------------------------- | :----------------------------------------------------------------------- |
| `inner` (Arrow Schema)    | [`df.schema().inner()`][`.inner()`] returns `&SchemaRef`, [`df.schema().as_arrow()`][`.as_arrow()`] returns `&Schema` | Field definitions (name, type, nullable, metadata)                       |
| `field_qualifiers`        | [`df.schema().iter()`][`.iter()`] yields `(Option<&TableReference>, &Arc<Field>)` pairs                               | Track which table each field came from (e.g., `users.id` vs `orders.id`) |
| `functional_dependencies` | [`df.schema().with_functional_dependencies()`][`.with_functional_dependencies()`] to set; internal to optimizer       | Enable optimizations based on key relationships (e.g., primary keys)     |

**Where Arrow Schema originates:**

| Source                                                  | Returns                           | Example                             |
| :------------------------------------------------------ | :-------------------------------- | :---------------------------------- |
| [`TableProvider::schema()`]                             | `SchemaRef` (Arrow)               | Custom data sources, catalog tables |
| [`ctx.read_parquet(...)`][`.read_parquet()`]            | Arrow Schema from file metadata   | Self-describing formats             |
| [`ctx.read_csv(...).schema(...)`]                       | Explicit Arrow Schema you provide | Text formats requiring schema       |
| [`Schema::new(vec![Field::new(...)])`][`Schema::new()`] | Constructed Arrow Schema          | Programmatic schema definition      |

> **Key insight:**<br> When you call [`df.schema()`], you get a `&DFSchema`. To access the underlying Arrow Schema, use [`.inner()`] (returns `&SchemaRef`) or [`.as_arrow()`] (returns `&Schema`). The Arrow Schema is what file writers (Parquet, IPC) and Arrow compute kernels expect.

Each column in a DataFrame is defined by four properties that control how data is stored, accessed, and validated. These properties determine how your data is interpreted—for example:<br>
The same bytes (`1735689600_i64`) become a timestamp (`2025-01-01T00:00:00`) when the field declares `Timestamp` as its type.

| Property                     | Role                                     | Operations Affected                              |
| :--------------------------- | :--------------------------------------- | :----------------------------------------------- |
| **Primary**                  | Essential for performant data processing | Query engine                                     |
| [`field.name`][`field`]      | Column identity                          | joins, selects, filters, group by, union_by_name |
| [`field.data_type`][`field`] | Storage & compute                        | kernel selection, type coercion, optimization    |
| [`field.nullable`][`field`]  | Null handling                            | validity bitmaps, null-safe operations           |
| **Secondary**                | _Essential for giving data meaning_      | _Human understanding & tuning_                   |
| [`field.metadata`][`field`]  | Semantic context                         | descriptions, units, lineage, PII classification |

The query engine uses the **primary properties** to plan and execute queries efficiently. **Metadata** (secondary property), while preserved throughout processing, serves a different purpose: it gives your data _meaning_ so you (or downstream systems) can interpret results correctly and make informed decisions about schema evolution.

In the remainder of this section, we will focus on four practical aspects of schema management:

- [1. Column Names](#column-names)
- [2. Column Order](#column-order)
- [3. Column Count](#column-count)
- [4. Column Types](#column-types)

(column-names)=

#### 1. Column Names

The column [`field.name`][`field`] is the primary identifier for a column in the DataFrame API. Operations like [`.select()`], [`.with_column()`], and [`.union_by_name()`] all rely on the column name to perform their work.

> **The #1 Schema Mismatch Cause:** <br>
> In the Rust DataFrame API, column names are **case-sensitive strings**. `col("Region")` and `col("region")` reference _different_ columns—this catches many users off guard.<br>

> **Note:** <br>
> This differs from DataFusion's SQL parser, where unquoted identifiers are normalized to lowercase by default. When mixing DataFrame API calls with SQL queries, be aware of this distinction.<br>

> **Best Practice:** <br>
> Enforce a consistent naming convention (e.g., all **snake_case** or **camelCase**) at your ingestion boundary.<br>

(column-order)=

#### 2. Column Order

DataFusion's DataFrame API is **name-based, not positional**. For operations like [`.union_by_name()`], the physical column order doesn't matter—DataFusion aligns columns by name, making pipelines resilient to upstream ordering changes.

> **DataFrame API Advantage:** <br>
> Unlike **traditional** [`UNION ALL`] which requires matching column positions, the DataFrame API's name-based approach is inherently safer. You don't need to worry about upstream schema reordering breaking your pipeline.

> **SQL equivalent:** `UNION BY NAME` <br>
> DataFusion's SQL-API parser also supports `UNION BY NAME` syntax (inspired by [DuckDB]). Both produce the same `LogicalPlan`.
>
> ```sql
> SELECT *
> FROM table_a
> UNION BY NAME
> SELECT *
> FROM table_b
> ```

(column-count)=

#### 3. Column Count

When combining DataFrames with [`.union_by_name()`], differences in column count are handled gracefully: missing columns are filled with NULL values. This deliberate behavior supports schema evolution—new columns appear with NULL for historical rows, and dropped columns remain explicit rather than causing silent failures.

> **Note:** <br>
> This flexibility applies to [`.union_by_name()`] only. The positional [`.union()`] requires **identical column counts** in both DataFrames—any mismatch will fail during planning.

> **Important:** <br>
> While [`.union_by_name()`] handles _missing_ columns automatically, it does **not** silently handle _type mismatches_ for columns that exist in both DataFrames. When the same column name appears with different types (e.g., `Int32` vs `Int64`), DataFusion's [type coercion analyzer][`TypeCoercion`] attempts to find a common type. If no safe coercion path exists, the query will fail during analysis—forcing you to be explicit about how to resolve the ambiguity.

(column-types)=

#### 4. Column Types

Types drive planning-time validation, coercion, and operator selection—prefer widening over narrowing. In DataFusion's DataFrame API, every column must have a specific [Apache Arrow `DataType`][arrow dtype] that determines its storage format and computational behavior.

**Common Arrow data types in DataFusion:**

| Category           | Arrow Types                                                                | Example Values                        |           Common Use Cases            |
| :----------------- | -------------------------------------------------------------------------- | ------------------------------------- | :-----------------------------------: |
| **Integers**       | `Int8`, `Int16`, `Int32`, `Int64`<br>`UInt8`, `UInt16`, `UInt32`, `UInt64` | `42`, `-100`, `0`                     |        IDs, counts, quantities        |
| **Floating-Point** | `Float32`, `Float64`                                                       | `3.14`, `-0.001`                      | Measurements, scientific data, ratios |
| **Decimal**        | `Decimal128(precision, scale)`                                             | `99.99`, `1234.5678`                  |   Financial data, currency, prices    |
| **Strings**        | `Utf8`, `LargeUtf8`                                                        | `"hello"`, `"データ融合"`             |    Names, descriptions, categories    |
| **Temporal**       | `Date32`, `Date64`<br>`Timestamp(unit, timezone)`                          | `2024-01-15`<br>`2024-01-15 14:30:00` |          Event times, dates           |
| **Boolean**        | `Boolean`                                                                  | `true`, `false`                       |           Flags, conditions           |
| **Binary**         | `Binary`, `LargeBinary`                                                    | `[0x12, 0x34]`                        |           Raw data, hashes            |
| **Nested Types**   | `Struct(Fields)`, `List(Field)`                                            | `{"a": 1}`, `[1, 2, 3]`               |  JSON/Parquet data, complex objects   |

For a complete reference of all supported types, see the [SQL Data Types guide](../../user-guide/sql/data_types.md).

#### Nested Types

Beyond primitive types, DataFusion fully supports Arrow's nested types: [`List`], [`Struct`], [`Map`], and [`Union`]. These enable complex data structures like JSON objects, arrays of values, or key-value maps—common in Parquet files and semi-structured data.

Nested types follow the same schema rules but add complexity in coercion and comparison. For a comprehensive reference on nested type structures and memory layouts, see the [Apache Arrow Data Types documentation][arrow data types].

---

### Schema Field Features

Properties define _what_ a column is (name, type). Features define _how_ it behaves. Nullability directly affects query execution—validity bitmaps, null-safe operations, and schema merging rules. Metadata provides semantic meaning without affecting computation—it travels with the data, but the query engine doesn't use it for optimization.

(schema-field-nullability)=

#### Nullability

The [`field.nullable`][`field`] flag—the third property in our Schema Field Properties table—is a critical part of a field's type definition. When merging schemas (for example, via [`.union_by_name()`]), DataFusion follows a simple, safe rule:

> **The Golden Rule of Nullability:**<br>
> If a column is nullable in any of the input schemas, it will be nullable in the output schema.

This is a widening conversion: a non‑nullable column can always be represented in a nullable one, but not the other way around.

> **How Arrow stores NULLs:** Arrow tracks nulls with a **validity bitmap**—one bit per row, separate from the data buffer. This makes null checks fast and memory-efficient (no per-null object overhead like Python's `NONE`). The data buffer still exists at null positions, but the validity bit marks it as "ignore this value."

For a deeper discussion of how NULL values behave in expressions, filters, and joins, see:

- [Concepts: Handling Null Values](./concepts.md#handling-null-values).

#### Metadata

The [`field.metadata`][`field`] property—the fourth in our Schema Field Properties table—provides semantic annotations beyond types: column descriptions, units of measure, data lineage, or custom tags. DataFusion **preserves** metadata when reading from formats like Parquet that embed it.

**Common metadata use cases:**

| Use Case           | Example Key     | Example Value                     |
| ------------------ | --------------- | --------------------------------- |
| Column description | `description`   | `"User's primary email address"`  |
| Units of measure   | `unit`          | `"milliseconds"`                  |
| Data lineage       | `source_system` | `"orders_db.public.transactions"` |
| PII classification | `pii_level`     | `"sensitive"`                     |

**Two levels of metadata:**

DataFusion supports metadata at two levels, both accessible via [`DFSchema`]:

- **Schema-level**: [`df.schema().metadata()`][`dfschema::metadata`] — annotations for the entire dataset
- **Field-level**: [`field.metadata()`][`field.metadata()`] — annotations per column (accessed via [`df.schema().fields()`] or [`df.schema().inner().fields()`][`.inner()`])

For the full metadata API, see the [`DFSchema` documentation][`DFSchema`].
