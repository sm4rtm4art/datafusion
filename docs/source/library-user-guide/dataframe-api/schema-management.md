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

# Schema Management

**The “health” phase of the DataFrame lifecycle: inspect, validate, and evolve schema.**

Schema management is the foundation of a robust data pipeline. It defines how you declare, evolve, and reconcile the structure of your data as it flows through DataFusion. In the [DataFrame lifecycle metaphor](./index.md#the-dataframe-lifecycle), this is the "health" phase—keeping schemas explicit and stable prevents subtle type drift and makes transformations predictable.

**Why Schemas Matter:** <br>
Accurate types are critical for both **correctness** and **performance**. They allow the DataFusion optimizer to:

- Push down predicates efficiently.
- Select the fastest vectorized compute kernels.
- Leverage columnar statistics to skip irrelevant data.

> **Style Note:** <br>
> In this guide, all code elements are highlighted with backticks. DataFrame methods are written as `.method()` (e.g., `.select()`) to reflect the chaining syntax central to the API. This distinguishes them from standalone functions (e.g., `col()`) and static constructors (e.g., `SessionContext::new()`). Rust types are formatted as `TypeName` (e.g., `SchemaRef`).

```{contents}
:local:
:depth: 2
```

## Where Schemas Come From

**The schema is the backbone of every DataFrame—it structures the data and enables the query optimizer to plan efficient execution.**

In DataFusion, a DataFrame schema ([`DFSchema`]) wraps Arrow's [`Schema`] and adds table qualifiers for unambiguous column references (e.g., `users.id` vs `orders.id`). Each field specifies a name, data type, nullability, and optional metadata.

### The Schema Ownership Flow

Understanding where schemas live is key to working with DataFusion. The schema flows through three stages, illustrated in the diagram below:

1. **Origin (Catalog)**: <br>
   Registered tables store their Arrow [`Schema`] in the catalog via [`TableProvider`]. This is the source of truth for table definitions.

2. **Ownership (LogicalPlan)**: <br>
   When you build a query, the [`LogicalPlanBuilder`] copies the Arrow [`Schema`] from the [`TableProvider`], converts it to a [`DFSchema`], and embeds it in the plan node (e.g., `TableScan.projected_schema`). Each transformation creates a new plan node with its own derived schema.

3. **Access (DataFrame)**: <br>
   The DataFrame wraps the [`LogicalPlan`] and delegates [`df.schema()`] to [`plan.schema()`]. The DataFrame itself does not store the schema—it lives in the plan.

```text
DataFusion Schema Ownership Flow

┌─────────────────────────────────────────────────────────────┐
│ 1. SCHEMA ORIGIN (SessionState / Catalog)                   │
│    Source of truth for *registered* tables.                 │
│                                                             │
│   SessionState                                              │
│     └── CatalogList                                         │
│          └── Catalog ("public")                             │
│               └── Schema ("default")                        │
│                    └── TableProvider ("users")              │
│                         └── schema() -> Arrow Schema        │
└─────────────────────────────────────────────────────────────┘
                               │
                               ▼
                       (Plan Creation)
            The schema is copied from the provider
            and embedded into the LogicalPlan.
                               │
                               ▼
┌─────────────────────────────────────────────────────────────┐
│ 2. SCHEMA OWNER (LogicalPlan)                               │
│    Source of truth for the *current transformation*.        │
│                                                             │
│   LogicalPlan::TableScan                                    │
│     ├── table_name: "users"                                 │
│     └── projected_schema: DFSchema (The Copy)               │
└─────────────────────────────────────────────────────────────┘
                               │
                               ▼
                       (API Wrapper)
            The DataFrame wraps the plan to provide
            a user-friendly API.
                               │
                               ▼
┌─────────────────────────────────────────────────────────────┐
│ 3. USER API (DataFrame)                                     │
│                                                             │
│   DataFrame                                                 │
│     ├── session_state: SessionState (Config Snapshot)       │
│     └── plan: LogicalPlan (Holds the DFSchema)              │
│                                                             │
│   df.schema() ────delegates────► plan.schema()              │
└─────────────────────────────────────────────────────────────┘
```

### Types of Schemas

**In data systems, a schema is fundamentally a structural contract—a blueprint defining how data is organized. However, the scope of this contract changes drastically depending on the architectural layer.**

DataFusion uses the term "schema" for distinct concepts ranging from database namespaces to memory layouts. Distinguishing these scopes is critical for navigating the API:

- **Catalog Schema ([`SchemaProvider`])**:<br>
  A namespace in the catalog hierarchy (like "public" in Postgres). Contains registered tables. Stored in [`SessionState.catalog_list`].

- **Arrow Schema ([`Schema`])**:<br>
  The generic columnar schema from Apache Arrow. Defines field names, data types, and nullability. This is what [`TableProvider.schema()`] returns. Arrow's `Schema` describes _data_—it knows nothing about table names or query context.

- **Arrow SchemaRef (`Arc<Schema>`)**:<br>
  Simply `Arc<Schema>`—Arrow's reference-counted schema type. You'll see `SchemaRef` throughout DataFusion APIs as the "currency" for passing schemas cheaply between functions without cloning.

- **DataFrame Schema ([`DFSchema`])**:<br>
  DataFusion's wrapper around Arrow `Schema`. Adds **table qualifiers** so the planner can resolve ambiguous column references (e.g., distinguishing `users.id` from `orders.id` in a join). Embedded in the [`LogicalPlan`]. This is what [`df.schema()`] returns.

> **Why `DFSchema` instead of Arrow's `Schema`?** <br>
> Arrow's `Schema` describes _data_. `DFSchema` describes the _plan_—it adds table qualifiers for column resolution during query planning. When you need the underlying Arrow schema, use `.inner()` (returns `&SchemaRef`) or `.as_arrow()` (returns `&Schema`).

> **Logical vs Physical Schema:** <br> > `df.schema()` returns the **logical** schema—what the plan _expects_ to produce. The actual physical memory layout during execution (e.g., dictionary encoding for strings, or nullable flags adjusted by optimizer passes) may differ. This is handled transparently by the physical plan; you rarely need to worry about it unless implementing a custom `TableProvider`.

### How Schemas are Determined

DataFusion determines the initial schema in one of three ways, depending on your data source:

1.  **Self-Describing Formats ([Parquet], Avro, Arrow):** <br>
    The schema is embedded in the file metadata. Types are known instantly at scan time.
2.  **Text Formats (CSV, JSON):** <br>
    Types must be either **provided explicitly** (recommended) or **inferred** from a data sample (risk of drift).
3.  **Custom Sources (TableProvider):**<br>
    The source of truth is the `.schema()`-method implemented by the provider. This contract must remain stable to ensure predictable query behavior.

For a deep dive into the underlying [Apache Arrow] type system, see the [Arrow Schema Specification][arrow schema].

> **DataFrame vs SQL:** <br>
> Both APIs produce the same [`DataFrame`] containing the same [`LogicalPlan`] with identical schemas. The DataFrame API provides compile-time visibility into schema changes—each method returns a new [`DataFrame`] whose schema you can inspect programmatically before execution.

## The Anatomy of a DataFusion Schema

Every DataFrame carries a [`DFSchema`] describing its columns. Calling [`.schema()`] returns `&DFSchema`—a borrowed reference to this struct. Understanding its properties (name, type, nullability) is key to diagnosing schema mismatch errors.

The following diagram shows the location of the DFSchema in the DataFrame hierarchy:

```text
┌─────────────────────────────────────────────────────┐
│ DataFrame                                           │
│   ├── SessionState (config, catalog, rules)         │
│   └── LogicalPlan                                   │
│            └── DFSchema  ← You are here!            │
│                 └── Field[]                         │
└─────────────────────────────────────────────────────┘
```

**Schema access and inspection methods:**

To extract the content of the `&DFSchema`, the following methods are available:

| Method                      | Returns        | Use Case                                           |
| --------------------------- | -------------- | -------------------------------------------------- |
| `df.schema()`               | `&DFSchema`    | DataFusion schema with table qualifiers            |
| `df.schema().to_string()`   | `String`       | Quick field list: `fields:[a, b, c]`               |
| `df.schema().tree_string()` | `impl Display` | Human-readable tree (like Spark's `printSchema()`) |
| `df.schema().inner()`       | `&SchemaRef`   | Arrow Schema as `Arc<Schema>`                      |
| `df.schema().as_arrow()`    | `&Schema`      | Arrow Schema reference                             |
| `df.schema().fields()`      | `Iterator`     | Iterate over `(qualifier, field)` pairs            |

This example creates a DataFrame with common primitive types and demonstrates each inspection method:

```rust
use datafusion::prelude::*;
use datafusion::assert_batches_eq;
use datafusion::arrow::datatypes::{DataType, TimeUnit};

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    // Create a DataFrame with the dataframe! macro
    // Note: email uses Option<&str> to include a NULL value
    let df = dataframe!(
        "user_id"    => [1_i64, 2_i64, 3_i64],
        "email"      => [Some("alice@example.com"), None, Some("carol@example.com")],
        "created_at" => [1735689600_i64, 1735776000_i64, 1735862400_i64],  // Unix timestamps (2025-01-01, 02, 03)
        "active"     => [true, false, true]
    )?;

    // Cast created_at from Int64 to Timestamp (same bytes, different interpretation!)
    // Using select() with alias() to transform columns and keep clean field names
    let df = df.select(vec![
        col("user_id").alias("user_id"),
        col("email").alias("email"),
        cast(col("created_at"), DataType::Timestamp(TimeUnit::Second, None))
            .alias("created_at"),
        col("active").alias("active"),
    ])?;

    // First, let's see what the data looks like
    let batches = df.clone().collect().await?;
    assert_batches_eq!(
        &[
            "+---------+-------------------+---------------------+--------+",
            "| user_id | email             | created_at          | active |",
            "+---------+-------------------+---------------------+--------+",
            "| 1       | alice@example.com | 2025-01-01T00:00:00 | true   |",
            "| 2       |                   | 2025-01-02T00:00:00 | false  |",
            "| 3       | carol@example.com | 2025-01-03T00:00:00 | true   |",
            "+---------+-------------------+---------------------+--------+",
        ],
        &batches
    );

    // Method 1: Compact field list (Display format)
    // Useful for quick checks of field names
    assert_eq!(
        df.schema().to_string(),
        "fields:[user_id, email, created_at, active], metadata:{}"
    );

    // Method 2: Tree format with types and nullability
    // Best for understanding schema structure at a glance
    assert_eq!(
        df.schema().tree_string().to_string(),
        r#"root
 |-- user_id: int64 (nullable = true)
 |-- email: utf8 (nullable = true)
 |-- created_at: timestamp (nullable = true)
 |-- active: boolean (nullable = true)"#
    );

    Ok(())
}
```

> **Note:** <br>
> The [`dataframe!`] macro sets all columns to `nullable = true` by default. Notice how `email` shows empty for the second row—that's the `NULL` value we inserted with `None`.
>
> **Production Note:** The `dataframe!` macro creates an in-memory table, ideal for examples and quick prototyping. In production, use `ctx.read_parquet(...)`, `ctx.read_csv(...)`, or `ctx.read_table(...)` to load data from external sources.

The [`.tree_string()`] method provides a human-readable representation similar to Spark's [`printSchema()`]. For **programmatic schema verification**, construct the expected schema and compare directly:

```rust
use datafusion::prelude::*;
use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let df = dataframe!(
        "user_id"    => [1_i64],
        "email"      => [Some("alice@example.com")],
        "created_at" => [1735689600_i64],
        "active"     => [true]
    )?;

    // Cast created_at from Int64 to Timestamp using with_column()
    // This is cleaner than select() when modifying a single column
    let df = df.with_column(
        "created_at",
        cast(col("created_at"), DataType::Timestamp(TimeUnit::Second, None))
    )?;

    // Construct expected schema for precise verification
    // This serves as documentation AND a rigid test
    let expected_schema = Schema::new(vec![
        Field::new("user_id", DataType::Int64, true),
        Field::new("email", DataType::Utf8, true),
        Field::new("created_at", DataType::Timestamp(TimeUnit::Second, None), true),
        Field::new("active", DataType::Boolean, true),
    ]);

    // Assert the inner Arrow schema matches exactly
    assert_eq!(df.schema().inner().as_ref(), &expected_schema);

    Ok(())
}
```

The debug representation (`format!("{:#?}", df.schema().inner())`) shows:

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

This approach is **type-safe** and **self-documenting**—the expected schema code shows readers exactly what fields, types, and nullability to expect.

> **Why types matter:** <br>
> Correct types unlock query optimization. A `Timestamp` column enables date-range pruning, while the same bytes as `Int64` only support numeric comparisons. This will be discussed in more details in the following sections.

### Schema Field Properties

**Understanding schema fields is essential for debugging mismatches and designing robust pipelines.**

Before diving into properties, let's locate where we are in the DataFrame hierarchy and how this fits to the context of schema management:

```text
┌─────────────────────────────────────────────────────┐
│ DataFrame                                           │
│   ├── SessionState (config, catalog, rules)         │
│   └── LogicalPlan                                   │
│            └── DFSchema                             │
│                 └── Field[]     <-- You are here!   │
│                      ├── name                       │
│                      ├── data_type                  │
│                      ├── nullable                   │
│                      └── metadata                   │
└─────────────────────────────────────────────────────┘
```

Each column in a DataFrame is defined by four properties that control how data is stored, accessed, and validated. These properties determine how your data is interpreted—for example, the same bytes (`1735689600_i64`) become a timestamp (`2025-01-01T00:00:00`) when the schema declares the field as `Timestamp`.

| Property             | Description                                              |
| -------------------- | -------------------------------------------------------- |
| [`name`][field]      | Column identifier used by DataFrame operations           |
| [`data_type`][field] | Arrow `DataType` describing logical and physical type    |
| [`nullable`][field]  | Whether the column may contain `NULL` values             |
| [`metadata`][field]  | Arbitrary key/value pairs for extra semantic information |

In the remainder of this section, we will focus on four practical aspects of schema management:

- [Column Names](#1-column-names)
- [Column Count](#2-column-count)
- [Column Order](#3-column-order)
- [Column Types](#4-column-types)

#### 1. Column Names

The column [`name`][field] is the primary identifier for a column in the DataFrame API. Operations like [`.select()`], [`.with_column()`], and [`.union_by_name()`] all rely on the column name to perform their work.

> **The #1 Schema Mismatch Cause:** <br>
> In the Rust DataFrame API, column names are **case-sensitive strings**. `col("Region")` and `col("region")` reference _different_ columns—this catches many users off guard.
>
> **Note:** This differs from SQL, where unquoted identifiers are normalized to lowercase. When mixing DataFrame API calls with SQL queries, be aware of this distinction.
>
> **Best Practice:** <br>
> Enforce a consistent naming convention (e.g., all `snake_case`) at your ingestion boundary. <br>
> See [Pattern 2: Schema Adapter Layer](#pattern-2-schema-adapter-layer) for how to normalize names.

#### 2. Column Count

When combining DataFrames with [`.union_by_name()`], differences in column count are handled gracefully: Missing columns are filled with NULL values. This deliberate behavior supports schema evolution—new columns appear with NULL for historical rows, and dropped columns remain explicit rather than causing silent failures.

> **Important:** While `union_by_name` handles _missing_ columns automatically, it does **not** silently handle _type mismatches_ for columns that exist in both DataFrames. When the same column name appears with different types (e.g., `Int32` vs `Int64`), DataFusion's [type coercion analyzer][typecoercion] attempts to find a common type. If no safe coercion path exists, the query will fail during analysis—forcing you to be explicit about how to resolve the ambiguity.

#### 3. Column Order

DataFusion's DataFrame API is **name-based, not positional**. For operations like [`.union_by_name()`], the physical column order doesn't matter—DataFusion aligns columns by name, making pipelines resilient to upstream ordering changes.

> **DataFrame API Advantage:** <br>
> Unlike traditional `UNION ALL` which requires matching column positions, the DataFrame API's name-based approach is inherently safer. You don't need to worry about upstream schema reordering breaking your pipeline.

> **SQL equivalent:** `UNION BY NAME` <br>
> DataFusion's SQL parser also supports `UNION BY NAME` syntax (inspired by [DuckDB]). Both produce the same `LogicalPlan`.
>
> ```sql
> SELECT *
> FROM table_a
> UNION BY NAME
> SELECT *
> FROM table_b
> ```

#### 4. Column Types (The Type System)

Types drive planning-time validation, coercion, and operator selection—prefer widening over narrowing. In DataFusion's DataFrame API, every column must have a specific [Apache Arrow `DataType`][arrow dtype] that determines its storage format and computational behavior.

**Why types matter for DataFrames:**

- **Data integrity**:<br>
  Types prevent mixing incompatible data (e.g., strings with numbers)
- **Performance**:<br>
  Knowing types enables optimized columnar storage and vectorized operations
- **Predictability**:<br>
  Type rules ensure consistent behavior across operations

**Common Arrow data types in DataFusion:**

| Category           |                                Arrow Types                                 |            Example Values             |           Common Use Cases            |
| :----------------- | :------------------------------------------------------------------------: | :-----------------------------------: | :-----------------------------------: |
| **Integers**       | `Int8`, `Int16`, `Int32`, `Int64`<br>`UInt8`, `UInt16`, `UInt32`, `UInt64` |           `42`, `-100`, `0`           |        IDs, counts, quantities        |
| **Floating-Point** |                            `Float32`, `Float64`                            |           `3.14`, `-0.001`            | Measurements, scientific data, ratios |
| **Decimal**        |                       `Decimal128(precision, scale)`                       |         `99.99`, `1234.5678`          |   Financial data, currency, prices    |
| **Strings**        |                            `Utf8`, `LargeUtf8`                             |       `"hello"`, `"データ融合"`       |    Names, descriptions, categories    |
| **Temporal**       |             `Date32`, `Date64`<br>`Timestamp(unit, timezone)`              | `2024-01-15`<br>`2024-01-15 14:30:00` |          Event times, dates           |
| **Boolean**        |                                 `Boolean`                                  |            `true`, `false`            |           Flags, conditions           |
| **Binary**         |                          `Binary`, `LargeBinary`                           |            `[0x12, 0x34]`             |           Raw data, hashes            |
| **Nested Types**   |                      `Struct(Fields)`, `List(Field)`                       |        `{"a": 1}`, `[1, 2, 3]`        |  JSON/Parquet data, complex objects   |

For a complete reference of all supported types, see the [SQL Data Types guide](../../user-guide/sql/data_types.md).

#### Nullability

The [`nullable`][field] flag—the third property in our Schema Field Properties table—is a critical part of a field's type definition. When merging schemas (for example, via [`.union_by_name()`]), DataFusion follows a simple, safe rule:

> **The Golden Rule of Nullability:**<br>
> If a column is nullable in any of the input schemas, it will be nullable in the output schema.

This is a widening conversion: a non‑nullable column can always be represented in a nullable one, but not the other way around. For a deeper discussion of how NULL values behave in expressions, filters, and joins. <br>
See: [Concepts: Handling Null Values](./concepts.md#handling-null-values).

#### Metadata

The [`metadata`][field] property—the fourth in our Schema Field Properties table—provides semantic annotations beyond types: column descriptions, units of measure, data lineage, or custom tags. DataFusion **preserves** metadata when reading from formats like Parquet that embed it.

**Common metadata use cases:**

| Use Case           | Example Key     | Example Value                     |
| ------------------ | --------------- | --------------------------------- |
| Column description | `description`   | `"User's primary email address"`  |
| Units of measure   | `unit`          | `"milliseconds"`                  |
| Data lineage       | `source_system` | `"orders_db.public.transactions"` |
| PII classification | `pii_level`     | `"sensitive"`                     |

**Two levels of metadata:**

DataFusion supports metadata at two levels, both accessible via [`DFSchema`]:

- **Schema-level**: [`df.schema().metadata()`][dfschema::metadata] — annotations for the entire dataset
- **Field-level**: `field.metadata()` — annotations per column (accessed via [`df.schema().inner().fields()`][dfschema::inner])

**Typical workflow:**

When reading from Parquet or other self-describing formats, metadata comes along automatically. For raw data (bronze layer) without embedded metadata, you can define it when creating your schema.

The following example shows both how to **create** a schema with metadata and how to **inspect** it from a DataFrame:

```rust
use datafusion::prelude::*;
use datafusion::arrow::datatypes::{Schema, Field, DataType};
use datafusion::arrow::record_batch::RecordBatch;
use std::collections::HashMap;
use std::sync::Arc;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    // Define field-level metadata (e.g., for a sensor data pipeline)
    let mut temp_meta = HashMap::new();
    temp_meta.insert("unit".to_string(), "celsius".to_string());
    temp_meta.insert("description".to_string(), "Engine temperature".to_string());

    let mut rpm_meta = HashMap::new();
    rpm_meta.insert("unit".to_string(), "rpm".to_string());

    // Create schema with annotated fields
    let schema = Arc::new(Schema::new(vec![
        Field::new("temperature", DataType::Float64, false).with_metadata(temp_meta),
        Field::new("rpm", DataType::Int64, false).with_metadata(rpm_meta),
        Field::new("status", DataType::Utf8, true),  // No metadata
    ]));

    // Create DataFrame from schema
    let ctx = SessionContext::new();
    let df = ctx.read_batch(RecordBatch::new_empty(Arc::clone(&schema)))?;

    // INSPECT: Access schema-level metadata (empty in this example)
    let schema_metadata = df.schema().metadata();
    assert!(schema_metadata.is_empty());

    // INSPECT: Access metadata for a specific field by name
    let temp_field = df.schema().field_with_name(None, "temperature")?;
    assert_eq!(temp_field.metadata().get("unit"), Some(&"celsius".to_string()));

    // INSPECT: Iterate over all fields to discover metadata
    for field in df.schema().inner().fields() {
        let meta = field.metadata();
        if let Some(unit) = meta.get("unit") {
            println!("{}: unit={}", field.name(), unit);
        }
    }

    Ok(())
}

// Output:
// temperature: unit=celsius
// rpm: unit=rpm
```

> **Governance Note:**<br>
> While DataFusion preserves metadata, it does not enforce it. For data governance workflows, validate metadata upstream or use a catalog system like [Unity Catalog] or [Project Nessie] to manage semantic annotations.

For the full metadata API, see the [`DFSchema` documentation][dfschema].

#### Nested Types

Beyond primitive types, DataFusion fully supports Arrow's nested types: [`List`], [`Struct`], [`Map`], and [`Union`]. These enable complex data structures like JSON objects, arrays of values, or key-value maps—common in Parquet files and semi-structured data.

Nested types follow the same schema rules but add complexity in coercion and comparison. For a comprehensive reference on nested type structures and memory layouts, see the [Apache Arrow Data Types documentation][arrow-data-types].

### Type Behavior in DataFrames

DataFusion's type system has two distinct modes of operation, designed for a balance of convenience and safety. The behavior depends on whether you are working within a single DataFrame's expression or combining two different DataFrames.

#### Mode 1: Automatic Coercion in Expressions

For convenience and intuitive use, DataFusion automatically promotes types to a common, wider type when they are mixed within an expression. This applies to functions like [`.select()`], [`.with_column()`], and [`.filter()`]. The promotion is always "widening" and follows the safe upcasting paths defined in the Type Coercion Hierarchy to prevent data loss.

```rust
use datafusion::prelude::*;
use datafusion::arrow::datatypes::DataType;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    // Create a DataFrame with Int32 and Int64 columns
    let df = dataframe!(
        "int32_col" => [1_i32, 2_i32],
        "int64_col" => [100_i64, 200_i64]
    )?;

    // Adding Int32 + Int64 produces Int64 (automatic widening)
    let result = df.select(vec![
        (col("int32_col") + col("int64_col")).alias("sum")
    ])?;

    // Verify the result type is Int64 (widened from Int32)
    let field = result.schema().field_with_name(None, "sum")?;
    assert_eq!(field.data_type(), &DataType::Int64);

    Ok(())
}
```

#### Mode 2: Strict Matching for Set Operations

For safety and to prevent silent data corruption, **set operations** like [`.union()`], `.except()`, and `.intersect()` require columns in corresponding positions to have compatible types.

If the types do not match exactly, DataFusion's type coercion analyzer will attempt to find a common type. However, when no safe coercion path exists, you'll need to cast explicitly. This is a deliberate design choice—it forces you to be explicit about how to resolve type ambiguity.

> **Note on Joins:** Join keys are an exception—DataFusion automatically coerces join keys to a common type (e.g., `Int32 = Int64` becomes `Int64 = Int64`). This happens transparently via the `TypeCoercion` analyzer rule, so you rarely need to cast join keys manually.

```rust
use datafusion::prelude::*;
use datafusion::arrow::datatypes::DataType;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    // Semantic naming: df_i32 and df_i64 make the type mismatch obvious
    let df_i32 = dataframe!("id" => [1_i32, 2_i32])?;
    let df_i64 = dataframe!("id" => [3_i64, 4_i64])?;

    // This will FAIL because Int32 and Int64 are not an exact match.
    // let bad = df_i32.union(df_i64)?;

    // The Fix: explicitly cast one of the columns to match the other.
    // Using the cast() free function from the prelude - simple and no extra imports needed.
    // (There's also a cast_to() method, but it requires importing ExprSchemable trait)
    let df_i32_fixed = df_i32.clone().with_column(
        "id",
        cast(col("id"), DataType::Int64)
    )?;

    // This now works because both `id` columns are Int64.
    let _good = df_i32_fixed.union(df_i64)?;

    Ok(())
}
```

**The Golden Rule of Type Casting**:<br>
Always widen types (e.g., `Int32 → Int64`) rather than narrow them to prevent data loss. Narrowing (e.g., `Int64 → Int32`) risks silent data corruption unless you have explicitly proven that no values will be truncated.

#### Type Coercion Hierarchy (safe upcasting paths):

Use this hierarchy to understand why DataFusion chooses a wider, common type when you mix types inside expressions, and how argument coercion works in function calls as defined by the [`TypeSignature`][typesignature]. It explains what conversions are safe (no data loss) and where explicit casts are required. The diagram covers the main type families DataFusion can upcast automatically: numeric (including decimal), strings, and temporal types.

```text
┌ Numeric Widening ───────────────────────────────────────────────┐
│                                                                 │
│ Signed:   Int8  ──► Int16  ──► Int32  ──► Int64 ───┐            │
│                                                    │            │
│ Unsigned: UInt8 ──► UInt16 ──► UInt32 ──► UInt64 ──┤            │
│                                                    │            │
│                                       (Precision)  ▼   (Range)  │
│                                       Decimal128 ──┬──► Float64 │
│                                                    │            │
│ Floats:             Float16 ──► Float32 ───────────┘            │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘

┌ Temporal Widening ──────────────────────────────────────────────┐
│                                                                 │
│ Dates:    Date32 ──► Date64 ──► Timestamp (ns) ──► +Timezone    │
│                                                                 │
│ Times:    Time32 ──► Time64                                     │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘

┌── Variable Width Compatibility (Views & Large Types) ───────────┐
│                                                                 │
│   Strings:  Utf8   ◄──► LargeUtf8   ◄──► Utf8View               │
│                                                                 │
│   Binary:   Binary ◄──► LargeBinary ◄──► BinaryView             │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

> **Note on `Date64`:** While Arrow defines `Date64`, it's rarely used as a target type in DataFusion. In practice, dates are typically coerced directly to `Timestamp` for temporal operations.

How to read this:

- **Numeric**
  - Mixing integers promotes to the widest integer (e.g., `Int32 + Int64 → Int64`)
  - Mixing integers and floats promotes to float (e.g., `Int32 + Float64 → Float64`)
  - Decimals take precedence with integers when possible; mixed with floats, the result is typically `Float64` (precision/scale permitting)
- **Strings**
  - `Utf8`, `LargeUtf8`, and `Utf8View` are aligned via planner-inserted casts; many functions prefer `Utf8View` as the target type
  - No automatic coercion from string columns to numeric/temporal types (string literals may be coerced to the target type in some contexts)
- **Temporal**
  - `Date32` combined with `Timestamp` promotes to `Timestamp`
  - Timezones must match in expressions; cast explicitly to align (some functions accept "any timezone" via a wildcard in their signatures)
- **Boolean and NULL**
  - Boolean does not auto-coerce to numeric
  - `NULL` adopts the other side's type in expressions (safe widening); otherwise remains `NULL`

Examples of automatic coercion in expressions:

- `Int32 + Float64` → both promoted to `Float64`
- `Date32` compared with `Timestamp` → `Date32` promoted to `Timestamp`
- `Decimal128(38, 9)` + `Int32` → `Decimal128(38, 9)` (if precision/scale allow)
- `Utf8` || `LargeUtf8` (string concatenation) → aligned string type

**Remember:** Automatic coercion applies to expressions (e.g., [`.select()`], [`.with_column()`], [`.filter()`]) and join keys. Set operations like [`.union()`] are stricter—while DataFusion will attempt coercion, incompatible types may require explicit casting.

| Left Operand    | Operator | Right Operand | Result Type         | Reasoning                                  |
| :-------------- | :------: | :------------ | :------------------ | :----------------------------------------- |
| `Int32`         |   `+`    | `Float64`     | **`Float64`**       | Floats dominate integers.                  |
| `Int8`          |   `+`    | `Int64`       | **`Int64`**         | Widens to largest integer container.       |
| `Date32`        |   `=`    | `Timestamp`   | **`Timestamp`**     | Date promoted to Timestamp for comparison. |
| `Decimal(10,2)` |   `+`    | `Int32`       | **`Decimal(10,2)`** | Integer fits inside Decimal safely.        |
| `Utf8`          |  `\|\|`  | `LargeUtf8`   | **`LargeUtf8`**     | Strings align to the larger container.     |

---

<!-- ==========================================================================
     REFERENCE List as Limiter for Focusing on the Above sections

     ========================================================================== -->

<!-- Place refrences below this line -->

<!-- External References -->

[duckdb]: https://duckdb.org/docs/sql/query_syntax/setops.html#union-by-name
[arrow-data-types]: https://arrow.apache.org/docs/python/data.html
[`printschema()`]: https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.printSchema.html
[unity catalog]: https://www.unitycatalog.io/
[project nessie]: https://projectnessie.org/

<!-- DataFusion Core Types -->

[typecoercion]: https://docs.rs/datafusion/latest/datafusion/optimizer/analyzer/type_coercion/struct.TypeCoercion.html
[`schema`]: https://docs.rs/datafusion/latest/datafusion/common/arrow/datatypes/struct.Schema.html
[`field`]: https://docs.rs/datafusion/latest/datafusion/common/arrow/datatypes/struct.Field.html
[`datatype`]: https://docs.rs/datafusion/latest/datafusion/common/arrow/datatypes/enum.DataType.html

<!-- Dataframe Methods -->

[`.tree_string()`]: https://docs.rs/datafusion/latest/datafusion/common/struct.DFSchema.html#method.tree_string
[`.union_by_name()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.union_by_name

<!-- External Standards -->

[parquet]: https://parquet.apache.org/docs/file-format/
[apache arrow]: https://arrow.apache.org/
[arrow schema]: https://docs.rs/arrow/latest/arrow/datatypes/struct.Schema.html
[arrow schema docs]: https://arrow.apache.org/cookbook/py/schema.html
[arrow schema rust]: https://github.com/apache/arrow-rs/tree/main/arrow/examples
[arrow dtype]: https://arrow.apache.org/docs/python/api/datatypes.html
[`arrow` crate]: https://docs.rs/arrow/latest/arrow/
[`arrow::compute::can_cast_types()`]: https://docs.rs/arrow/latest/arrow/compute/fn.can_cast_types.html
[`can_cast_types()`]: https://docs.rs/arrow/latest/arrow/compute/fn.can_cast_types.html
[`schema`]: https://docs.rs/datafusion/latest/datafusion/common/arrow/datatypes/struct.Schema.html
[`field`]: https://docs.rs/datafusion/latest/datafusion/common/arrow/datatypes/struct.Field.html
[`datatype`]: https://docs.rs/datafusion/latest/datafusion/common/arrow/datatypes/enum.DataType.html

<!-- DataFusion Core Types -->

[`schemaprovider`]: https://docs.rs/datafusion/latest/datafusion/catalog/trait.SchemaProvider.html
[`tableprovider.schema()`]: https://docs.rs/datafusion/latest/datafusion/datasource/trait.TableProvider.html#tymethod.schema
[`dfschema`]: https://docs.rs/datafusion/latest/datafusion/common/struct.DFSchema.html
[dfschema::inner]: https://docs.rs/datafusion/latest/datafusion/common/struct.DFSchema.html#method.inner
[`dfschema::field_with_name()`]: https://docs.rs/datafusion/latest/datafusion/common/struct.DFSchema.html#method.field_with_name
[`dfschema::logically_equivalent_names_and_types()`]: https://docs.rs/datafusion/latest/datafusion/common/struct.DFSchema.html#method.logically_equivalent_names_and_types
[`sessioncontext`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html
[`tableprovider`]: https://docs.rs/datafusion/latest/datafusion/datasource/trait.TableProvider.html
[tableprovider::schema]: https://docs.rs/datafusion/latest/datafusion/datasource/trait.TableProvider.html#tymethod.schema
[`listingtable`]: https://docs.rs/datafusion/latest/datafusion/datasource/listing/struct.ListingTable.html
[`dataframe!`]: https://docs.rs/datafusion/latest/datafusion/macro.dataframe.html
[`schemaref`]: https://docs.rs/datafusion/latest/datafusion/common/arrow/datatypes/type.SchemaRef.html
[`datafusionerror`]: https://docs.rs/datafusion/latest/datafusion/common/enum.DataFusionError.html
[`datafusionerror::plan`]: https://docs.rs/datafusion/latest/datafusion/common/enum.DataFusionError.html#variant.Plan
[`datafusionerror::schemaerror`]: https://docs.rs/datafusion/latest/datafusion/common/enum.DataFusionError.html#variant.SchemaError
[`null`]: https://docs.rs/datafusion/latest/datafusion/logical_expr/sqlparser/dialect/keywords/constant.NULL.html

<!-- DataFusion Methods -->

[`.alias()`]: https://docs.rs/datafusion/latest/datafusion/logical_expr/enum.Expr.html#method.alias
[`col()`]: https://docs.rs/datafusion/latest/datafusion/logical_expr/fn.col.html
[`.cast_to()`]: https://docs.rs/datafusion/latest/datafusion/logical_expr/enum.Expr.html#method.cast_to
[`.collect()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.collect
[`.distinct()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.distinct
[`.explain()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.explain
[`.filter()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.filter
[`.join()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.join
[`.read_parquet()`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.read_parquet
[`.schema()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.schema
[`.select()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.select
[`.show()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.show
[`.to_string_pretty()`]: https://docs.rs/serde_json/latest/serde_json/fn.to_string_pretty.html
[`.union()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.union
[`.union_by_name()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.union_by_name
[`.union_by_name_distinct()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.union_by_name_distinct
[`.with_column()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.with_column
[`coalesce`]: https://docs.rs/datafusion-functions/latest/datafusion_functions/core/expr_fn/fn.coalesce.html
[`avg()`]: https://docs.rs/datafusion-functions-aggregate/latest/datafusion_functions_aggregate/average/index.html
[`count()`]: https://docs.rs/datafusion-functions-aggregate/latest/datafusion_functions_aggregate/count/index.html
[`max()`]: https://docs.rs/datafusion-functions-aggregate/latest/datafusion_functions_aggregate/min_max/index.html
[`median()`]: https://docs.rs/datafusion-functions-aggregate/latest/datafusion_functions_aggregate/median/index.html
[`min()`]: https://docs.rs/datafusion-functions-aggregate/latest/datafusion_functions_aggregate/min_max/index.html
[`stddev()`]: https://docs.rs/datafusion-functions-aggregate/latest/datafusion_functions_aggregate/stddev/index.html
[typesignature]: https://docs.rs/datafusion/latest/datafusion/logical_expr/enum.TypeSignature.html

<!-- DFSchema Methods (Advanced) -->

[`.as_arrow()`]: https://docs.rs/datafusion/latest/datafusion/common/struct.DFSchema.html#method.as_arrow
[`.datatype_is_logically_equal()`]: https://docs.rs/datafusion/latest/datafusion/common/struct.DFSchema.html#method.datatype_is_logically_equal
[`.datatype_is_semantically_equal()`]: https://docs.rs/datafusion/latest/datafusion/common/struct.DFSchema.html#method.datatype_is_semantically_equal
[`.field_names()`]: https://docs.rs/datafusion/latest/datafusion/common/struct.DFSchema.html#method.field_names
[`.field_with_name()`]: https://docs.rs/datafusion/latest/datafusion/common/struct.DFSchema.html#method.field_with_name
[`field_with_name()`]: https://docs.rs/datafusion/latest/datafusion/common/struct.DFSchema.html#method.field_with_name
[`.field_with_qualified_name()`]: https://docs.rs/datafusion/latest/datafusion/common/struct.DFSchema.html#method.field_with_qualified_name
[`.field_with_unqualified_name()`]: https://docs.rs/datafusion/latest/datafusion/common/struct.DFSchema.html#method.field_with_unqualified_name
[`.fields()`]: https://docs.rs/datafusion/latest/datafusion/common/struct.DFSchema.html#method.fields
[`.has_column()`]: https://docs.rs/datafusion/latest/datafusion/common/struct.DFSchema.html#method.has_column
[`.has_column_with_qualified_name()`]: https://docs.rs/datafusion/latest/datafusion/common/struct.DFSchema.html#method.has_column_with_qualified_name
[`.has_equivalent_names_and_types()`]: https://docs.rs/datafusion/latest/datafusion/common/struct.DFSchema.html#method.has_equivalent_names_and_types
[`.index_of_column()`]: https://docs.rs/datafusion/latest/datafusion/common/struct.DFSchema.html#method.index_of_column
[`.logically_equivalent_names_and_types()`]: https://docs.rs/datafusion/latest/datafusion/common/struct.DFSchema.html#method.logically_equivalent_names_and_types
[`.matches_arrow_schema()`]: https://docs.rs/datafusion/latest/datafusion/common/struct.DFSchema.html#method.matches_arrow_schema
[`.maybe_index_of_column()`]: https://docs.rs/datafusion/latest/datafusion/common/struct.DFSchema.html#method.maybe_index_of_column
[`nullable()`]: https://docs.rs/datafusion/latest/datafusion/common/trait.ExprSchema.html#method.nullable
[`.qualified_field_with_name()`]: https://docs.rs/datafusion/latest/datafusion/common/struct.DFSchema.html#method.qualified_field_with_name
[field]: https://docs.rs/datafusion/latest/datafusion/common/arrow/datatypes/struct.Field.html

<!-- IO & Configuration -->

[`csvreadoptions`]: https://docs.rs/datafusion/latest/datafusion/datasource/file_format/options/struct.CsvReadOptions.html
[`ndjsonreadoptions`]: https://docs.rs/datafusion/latest/datafusion/datasource/file_format/options/struct.NdJsonReadOptions.html
[`parquetreadoptions`]: https://docs.rs/datafusion/latest/datafusion/datasource/file_format/options/struct.ParquetReadOptions.html
[`has_header`]: https://docs.rs/datafusion/latest/datafusion/datasource/file_format/options/struct.CsvReadOptions.html#structfield.has_header
[`infer_schema_max_records`]: https://docs.rs/deltalake/latest/deltalake/datafusion/datasource/file_format/options/struct.CsvReadOptions.html#method.schema_infer_max_records
[`schema_infer_max_records`]: https://docs.rs/datafusion/latest/datafusion/datasource/file_format/options/struct.CsvReadOptions.html#method.schema_infer_max_records
[`truncated_rows`]: https://docs.rs/datafusion/latest/datafusion/datasource/file_format/options/struct.CsvReadOptions.html#method.truncated_rows

<!-- External Reading -->

[avro-evolution]: https://avro.apache.org/docs/current/specification/#schema-resolution
[kleppmann]: https://dataintensive.net/
[parquet-dremio]: https://medium.com/data-engineering-with-dremio/all-about-parquet-part-04-schema-evolution-in-parquet-c2c2b1aa6141
[parquet-evolution]: https://spark.apache.org/docs/latest/sql-data-sources-parquet.html#schema-merging
[schema mismatch medium]: https://medium.com/data-engineering-with-dremio/schema-mismatch-error-understanding-and-resolving-8d6c1e1a7e1a

<!-- Invisible References (Sorted by Category) -->

[`.with_column()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.with_column
[`.cast_to()`]: https://docs.rs/datafusion/latest/datafusion/logical_expr/enum.Expr.html#method.cast_to
[`.alias()`]: https://docs.rs/datafusion/latest/datafusion/logical_expr/enum.Expr.html#method.alias
[`schema_infer_max_records`]: https://docs.rs/datafusion/latest/datafusion/config/struct.ConfigOptions.html
[`.explain()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.explain
[`.collect()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.collect
[`.show()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.show
[`schema_infer_max_records`]: https://docs.rs/datafusion/latest/datafusion/datasource/file_format/options/struct.CsvReadOptions.html#method.schema_infer_max_records
[`truncated_rows`]: https://docs.rs/datafusion/latest/datafusion/datasource/file_format/options/struct.CsvReadOptions.html#method.truncated_rows
[`has_header`]: https://docs.rs/datafusion/latest/datafusion/datasource/file_format/options/struct.CsvReadOptions.html#structfield.has_header
[`datafusionerror::plan`]: https://docs.rs/datafusion/latest/datafusion/common/enum.DataFusionError.html#variant.Plan
[`datafusionerror::schemaerror`]: https://docs.rs/datafusion/latest/datafusion/common/enum.DataFusionError.html#variant.SchemaError
[`datafusionerror`]: https://docs.rs/datafusion/latest/datafusion/common/enum.DataFusionError.html
[`csvreadoptions`]: https://docs.rs/datafusion/latest/datafusion/datasource/file_format/options/struct.CsvReadOptions.html
[`ndjsonreadoptions`]: https://docs.rs/datafusion/latest/datafusion/datasource/file_format/options/struct.NdJsonReadOptions.html
[`parquetreadoptions`]: https://docs.rs/datafusion/latest/datafusion/datasource/file_format/options/struct.ParquetReadOptions.html
[`arrow_csv::readerbuilder::with_truncated_rows`]: https://docs.rs/arrow-csv/latest/arrow_csv/reader/struct.ReaderBuilder.html#method.with_truncated_rows
[`arrow` crate]: https://docs.rs/arrow/latest/arrow/
[apache arrow]: https://arrow.apache.org/
[arrow schema]: https://docs.rs/arrow/latest/arrow/datatypes/struct.Schema.html
[arrow schema docs]: https://arrow.apache.org/cookbook/py/schema.html
[arrow schema docs.rs]: https://docs.rs/arrow/latest/arrow/datatypes/struct.Schema.html
[arrow dtype]: https://arrow.apache.org/docs/python/api/datatypes.html
[`col()`]: https://docs.rs/datafusion/latest/datafusion/logical_expr/fn.col.html
[`count()`]: https://docs.rs/datafusion-functions-aggregate/latest/datafusion_functions_aggregate/count/index.html
[`median()`]: https://docs.rs/datafusion-functions-aggregate/latest/datafusion_functions_aggregate/median/index.html
[`min()`]: https://docs.rs/datafusion-functions-aggregate/latest/datafusion_functions_aggregate/min_max/index.html
[`max()`]: https://docs.rs/datafusion-functions-aggregate/latest/datafusion_functions_aggregate/min_max/index.html
[`avg()`]: https://docs.rs/datafusion-functions-aggregate/latest/datafusion_functions_aggregate/average/index.html
[`stddev()`]: https://docs.rs/datafusion-functions-aggregate/latest/datafusion_functions_aggregate/stddev/index.html
[`.distinct()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.distinct
[`schemaref`]: https://docs.rs/datafusion/latest/datafusion/common/arrow/datatypes/type.SchemaRef.html
[`dfschema::inner()`]: https://docs.rs/datafusion/latest/datafusion/common/struct.DFSchema.html#method.inner
[`.filter()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.filter
[`.join()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.join
[`.union()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.union
[`.union_by_name()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.union_by_name
[`.union_by_name_distinct()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.union_by_name_distinct
[dataframe.schema]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.schema
[`.schema()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.schema
[`.to_string_pretty()`]: https://docs.rs/serde_json/latest/serde_json/fn.to_string_pretty.html
[`.with_column()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.with_column
[`dfschema`]: https://docs.rs/datafusion/latest/datafusion/common/struct.DFSchema.html
[`dfschema::logically_equivalent_names_and_types()`]: https://docs.rs/datafusion/latest/datafusion/common/struct.DFSchema.html#method.logically_equivalent_names_and_types
[`.logically_equivalent_names_and_types()`]: https://docs.rs/datafusion/latest/datafusion/common/struct.DFSchema.html#method.logically_equivalent_names_and_types
[`dfschema::field_with_name()`]: https://docs.rs/datafusion/latest/datafusion/common/struct.DFSchema.html#method.field_with_name
[`field_with_name()`]: https://docs.rs/datafusion/latest/datafusion/common/struct.DFSchema.html#method.field_with_name
[`.field_with_name()`]: https://docs.rs/datafusion/latest/datafusion/common/struct.DFSchema.html#method.field_with_name
[`.has_equivalent_names_and_types()`]: https://docs.rs/datafusion/latest/datafusion/common/struct.DFSchema.html#method.has_equivalent_names_and_types
[`.matches_arrow_schema()`]: https://docs.rs/datafusion/latest/datafusion/common/struct.DFSchema.html#method.matches_arrow_schema
[`.datatype_is_logically_equal()`]: https://docs.rs/datafusion/latest/datafusion/common/struct.DFSchema.html#method.datatype_is_logically_equal
[`.datatype_is_semantically_equal()`]: https://docs.rs/datafusion/latest/datafusion/common/struct.DFSchema.html#method.datatype_is_semantically_equal
[`.field_with_qualified_name()`]: https://docs.rs/datafusion/latest/datafusion/common/struct.DFSchema.html#method.field_with_qualified_name
[`.field_with_unqualified_name()`]: https://docs.rs/datafusion/latest/datafusion/common/struct.DFSchema.html#method.field_with_unqualified_name
[`.qualified_field_with_name()`]: https://docs.rs/datafusion/latest/datafusion/common/struct.DFSchema.html#method.qualified_field_with_name
[`.maybe_index_of_column()`]: https://docs.rs/datafusion/latest/datafusion/common/struct.DFSchema.html#method.maybe_index_of_column
[`.index_of_column()`]: https://docs.rs/datafusion/latest/datafusion/common/struct.DFSchema.html#method.index_of_column
[`.field_names()`]: https://docs.rs/datafusion/latest/datafusion/common/struct.DFSchema.html#method.field_names
[`.fields()`]: https://docs.rs/datafusion/latest/datafusion/common/struct.DFSchema.html#method.fields
[`.has_column()`]: https://docs.rs/datafusion/latest/datafusion/common/struct.DFSchema.html#method.has_column
[`.has_column_with_qualified_name()`]: https://docs.rs/datafusion/latest/datafusion/common/struct.DFSchema.html#method.has_column_with_qualified_name
[`.as_arrow()`]: https://docs.rs/datafusion/latest/datafusion/common/struct.DFSchema.html#method.as_arrow
[dfschema::inner]: https://docs.rs/datafusion/latest/datafusion/common/struct.DFSchema.html#method.inner
[dfschema::metadata]: https://docs.rs/datafusion/latest/datafusion/common/struct.DFSchema.html#method.metadata
[`dfschema`]: https://docs.rs/datafusion/latest/datafusion/common/struct.DFSchema.html
[`schema`]: https://docs.rs/arrow/latest/arrow/datatypes/struct.Schema.html
[`field`]: https://docs.rs/arrow/latest/arrow/datatypes/struct.Field.html
[`datatype`]: https://docs.rs/arrow/latest/arrow/datatypes/enum.DataType.html
[`arrow::compute::can_cast_types()`]: https://docs.rs/arrow/latest/arrow/compute/fn.can_cast_types.html
[`can_cast_types()`]: https://docs.rs/arrow/latest/arrow/compute/fn.can_cast_types.html
[`.cast_to()`]: https://docs.rs/datafusion/latest/datafusion/logical_expr/struct.Expr.html#method.cast_to
[`sessioncontext`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html
[`.read_parquet()`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.read_parquet
[`.select()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.select
[`coalesce`]: https://docs.rs/datafusion-functions/latest/datafusion_functions/core/expr_fn/fn.coalesce.html
[`tableprovider`]: https://docs.rs/datafusion/latest/datafusion/datasource/trait.TableProvider.html
[`schema_infer_max_records`]: https://docs.rs/datafusion/latest/datafusion/datasource/file_format/options/struct.CsvReadOptions.html#structfield.schema_infer_max_records
[typesignature]: https://github.com/apache/datafusion/blob/main/datafusion/expr-common/src/signature.rs#L154-L249
[`listingtable`]: https://docs.rs/datafusion/latest/datafusion/datasource/listing/struct.ListingTable.html
[schema mismatch medium]: https://medium.com/@rakeshchanda/schema-mismatch-understanding-and-resolving-eadf3251f786
[`dataframe!`]: https://docs.rs/datafusion/latest/datafusion/macro.dataframe.html
[tableprovider::schema]: https://docs.rs/datafusion/latest/datafusion/datasource/trait.TableProvider.html#tymethod.schema
[parquet-evolution]: https://spark.apache.org/docs/latest/sql-data-sources-parquet.html#schema-merging
[avro-evolution]: https://avro.apache.org/docs/current/specification/#schema-resolution
[kleppmann]: https://dataintensive.net/
[parquet-dremio]: https://medium.com/data-engineering-with-dremio/all-about-parquet-part-04-schema-evolution-in-parquet-c2c2b1aa6141
[`infer_schema_max_records`]: https://docs.rs/deltalake/latest/deltalake/datafusion/datasource/file_format/options/struct.CsvReadOptions.html#method.schema_infer_max_records
[`field`]: https://docs.rs/datafusion/latest/datafusion/common/arrow/datatypes/struct.Field.html
[`schema`]: https://docs.rs/datafusion/latest/datafusion/common/arrow/datatypes/struct.Schema.html
[`nullable()`]: https://docs.rs/datafusion/latest/datafusion/common/trait.ExprSchema.html#method.nullable

<>

## Handling Missing Data & Nullability

**Nullability is a fundamental schema property that determines whether a column can contain NULL values.**

In DataFusion, nullability is not just metadata—it's a critical part of your data contract that affects query behavior, performance, and correctness. Every field in a schema declares whether it can be nullable (`true`) or must always have a value (`false`). This section covers the foundational concepts of nullability and practical patterns for handling missing data.

### What: Understanding Nullability in Schemas

When you define a field, the third parameter controls nullability:

```rust,ignore
use datafusion::arrow::datatypes::{Field, DataType};

fn main() {
    // Non-nullable: This field MUST always have a value
    let _user_id = Field::new("user_id", DataType::Int64, false);

    // Nullable: This field MAY contain NULL values
    let _email = Field::new("email", DataType::Utf8, true);
}
```

### Why: Real-World Data is Messy

Missing data is inevitable in production systems:

- **Incomplete records**: User didn't provide optional information
- **Schema evolution**: New columns added over time (NULL in old records)
- **Failed computations**: Division by zero, parsing errors produce NULLs
- **Outer joins**: Non-matching records filled with NULLs
- **Data quality issues**: Source system inconsistencies

### How: Schema Defines Structure, Queries Handle the Data

The schema defines what's **allowed**; your queries define what to **do** about it:

```rust,ignore
use datafusion::prelude::*;
use datafusion::arrow::array::{Int64Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use std::sync::Arc;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();

    // Schema definition: email and score CAN be NULL, user_id CANNOT
    let schema = Arc::new(Schema::new(vec![
        Field::new("user_id", DataType::Int64, false),   // Required field
        Field::new("email", DataType::Utf8, true),        // Optional field
        Field::new("status", DataType::Utf8, true),       // Optional field
        Field::new("score", DataType::Int64, true),       // Optional field
    ]));

    // Create a record batch with actual data including NULL values
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec![Some("a@x.com"), None, Some("c@x.com")])),
            Arc::new(StringArray::from(vec![Some("active"), None, Some("inactive")])),
            Arc::new(Int64Array::from(vec![Some(10), None, Some(42)])),
        ],
    )?;

    let df = ctx.read_batch(batch)?;

    df.show().await?;
    // Output showing NULL values in nullable columns (NULLs appear as empty cells):
    // +---------+-----------+----------+-------+
    // | user_id | email     | status   | score |
    // +---------+-----------+----------+-------+
    // | 1       | a@x.com   | active   | 10    |
    // | 2       |           |          |       | <-- NULL values
    // | 3       | c@x.com   | inactive | 42    |
    // +---------+-----------+----------+-------+

    Ok(())
}
```

### Common Patterns for Handling Missing Data

```rust,ignore
use datafusion::prelude::*;
use datafusion::functions::expr_fn::coalesce;
use datafusion::functions_aggregate::expr_fn::count;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let df = dataframe!(
        "email" => [Some("a@x.com"), None, Some("c@x.com")],
        "status" => [Some("active"), None, Some("inactive")],
        "score" => [Some(10_i64), None, Some(42_i64)]
    )?;

    // Pattern 1: Diagnose - understand the extent of missing data
    let _stats = df.clone().aggregate(vec![], vec![
        count(lit(1)).alias("total_rows"),
        count(col("email")).alias("non_null_emails"),  // COUNT ignores NULLs
    ])?;

    // Pattern 2: Fill - provide default values for NULLs
    let df = df.with_column("status",
        coalesce(vec![col("status"), lit("pending")])  // Use first non-NULL value
    )?;

    // Pattern 3: Conditional fill - complex default logic
    let df = df.with_column(
        "score",
        when(col("score").is_null(), lit(0))
            .otherwise(col("score"))?
    )?;

    // Pattern 4: Filter - drop incomplete records
    let _complete_df = df.filter(
        col("email").is_not_null()
            .and(col("score").is_not_null())
    )?;

    Ok(())
}
```

### Decision Guide: Fill vs. Drop

| Strategy                  | When to Use                                      | Example                                 |
| :------------------------ | :----------------------------------------------- | :-------------------------------------- |
| **Fill with default**     | Reasonable default exists AND row still valuable | Missing status → "pending"              |
| **Fill with computation** | Can derive from other columns                    | Missing full_name → concat(first, last) |
| **Drop row**              | Required field missing OR would skew analysis    | Missing primary key                     |
| **Keep NULL**             | NULL is meaningful (unknown ≠ default)           | Missing survey response                 |

### The Golden Rule of Nullability

> **When merging schemas, nullability widens:** If a field is nullable in ANY input schema, it becomes nullable in the output schema.

This is a safety mechanism—DataFusion never assumes data exists where it might not:

```text
// Schema 1: email is NOT nullable
// Schema 2: email IS nullable
// Merged schema: email IS nullable (safer choice)
```

**See also:**

- Concepts: [Handling Null Values](./concepts.md#handling-null-values) — SQL NULL semantics, three‑valued logic
- Transformations guide: Advanced NULL handling patterns

---

## Defining Schemas

Define schemas explicitly to get planning-time validation, stable types, and predictable downstream behavior.

The most robust way to manage schemas in DataFusion is to define them explicitly in your code. This is done using the [`Schema`], [`Field`], and [`DataType`] objects from the [`arrow` crate].

As described in [The Anatomy of a DataFusion Schema](#the-anatomy-of-a-datafusion-schema), defining a schema gives your pipeline stability and performance. In short:

- **Data quality**: Avoids inference drift in text formats (CSV/NDJSON) and ensures consistent types across runs.
- **Performance**: Lets the optimizer pick vectorized kernels and push down filters with correct types.
- **Predictability**: Ensures unions/joins and downstream transformations behave consistently.

A schema specifies:

- **Field names** (case-sensitive)
- **Data types** ([`DataType`], e.g., `Int64`, `Utf8`, `Timestamp`)
- **Nullability** (whether `NULL` is allowed)
- **Optional metadata** (key/value annotations for lineage, semantics)

### Basic Schema Construction

Build schemas with [`Schema`], [`Field`], and [`DataType`]; reuse them via `Arc<Schema>`.

A minimal schema example demonstrating the core components:

```rust,ignore
use std::sync::Arc;
use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};

fn main() {
    // Define the structure of your data
    let schema: Arc<Schema> = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),        // not nullable
        Field::new("name", DataType::Utf8, true),        // nullable
        Field::new(
            "created_at",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            false
        ),
    ]));

    // This schema can be applied to readers (see "Applying Schemas to File Readers")
    println!("Schema has {} fields", schema.fields().len());
}
```

Each `Field` in the schema specifies:

- **Name**: The column identifier (case-sensitive)
- **DataType**: The type of values the column holds
- **Nullable**: Whether `NULL` values are permitted

> **Note**: Always use [`SchemaRef`] (`Arc<Schema>`) for efficient sharing. Cloning an `Arc` is O(1) and avoids deep copies of the schema structure.

### Default Values

Schemas define structure only, not default values. To provide defaults for `NULL` values, apply transformations after reading:

```rust,ignore
use datafusion::prelude::*;
use datafusion::functions::expr_fn::coalesce;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let df = dataframe!(
        "name" => [Some("Alice"), None, Some("Carol")]
    )?;

    let df = df.with_column(
        "name",
        coalesce(vec![col("name"), lit("Unknown")])
    )?;

    df.show().await?;
    Ok(())
}
```

See [Nullability and Default Values](#nullability-and-default-values) for more patterns.

> **Best practice:** In production, always prefer **explicit schemas** over inference to prevent drift and ensure consistency.

### Configuring Common Field Types

Certain data types require specific configuration to ensure correctness and prevent data loss. This section covers the most common cases.

#### Decimal Types: Precision and Scale

**Why decimals matter**: Floating-point types (Float32/Float64) can introduce rounding errors for financial calculations. Decimals provide exact arithmetic for monetary values.

**What you need to specify**:

- **Precision**: Total number of digits (maximum 38 for Decimal128)
- **Scale**: Digits after the decimal point

**Example**: `Decimal128(10, 2)`

- Can store: `12345678.99` (8 digits + 2 decimals = 10 total)
- Cannot store: `123456789.99` (11 digits, exceeds precision)
- Cannot store: `1234567.999` (3 decimals, exceeds scale)

```rust,ignore
use datafusion::arrow::datatypes::{DataType, Field};

fn main() {
    // For currency: typically 2 decimal places
    let _price = Field::new("price", DataType::Decimal128(19, 2), false);

    // For percentages: more decimal places
    let _rate = Field::new("rate", DataType::Decimal128(10, 6), false);  // e.g., 0.123456
}
```

> **Tip**: When casting between decimals, ensure the target has enough precision **AND** scale. Casting `Decimal128(10, 2)` to `Decimal128(8, 2)` will fail if values exceed 6 integer digits.

#### Timestamp Types: Timezone Handling

**Why timezone matters**: A timestamp can represent either an absolute moment in time (with timezone) or a local time (without timezone). Mixing them causes errors.

**Your two choices**:

| Type                 |             Code Example              | What it stores                                    | When to use                                                            |
| :------------------- | :-----------------------------------: | :------------------------------------------------ | :--------------------------------------------------------------------- |
| **With timezone**    | `Timestamp(Microsecond, Some("UTC"))` | A specific instant (e.g., "2024-01-15 10:00 UTC") | Server logs, transactions, anything that happened at a specific moment |
| **Without timezone** |    `Timestamp(Microsecond, None)`     | A local time (e.g., "2024-01-15 10:00")           | Scheduled events, opening hours, anything relative to local time       |

**Common mistake**: Mixing the two types in operations

```rust,ignore
use datafusion::prelude::*;
use datafusion::arrow::datatypes::{DataType, TimeUnit};

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    // Example: casting timestamps to align types
    let df = dataframe!(
        "value" => [1, 2, 3]
    )?;

    // When you have timestamp columns with different timezone settings,
    // you need to cast them to the same type before comparing:
    // let good = with_tz.gt(
    //     without_tz.cast_to(
    //         &DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
    //         df.schema()
    //     )?
    // );

    // Demonstrate DataType creation (no error expected)
    let _ts_type = DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()));

    Ok(())
}
```

> **Best practice**: Pick one strategy for your entire pipeline. Most systems use UTC timestamps throughout.

<!-- TODO: Verify `TimeUnit` import and `Timestamp` timezone signature (Option<String>) against current Arrow/DataFusion -->

#### Advanced: Field Metadata

Field metadata is used to embed rich, contextual information—such as column descriptions, data lineage, or security classifications—directly into the schema as key-value pairs. While this information is not used by the DataFusion query engine, it is preserved where possible, making it a powerful tool for external systems, documentation, and compliance.

Common Use Cases:

- Constraints: primary_key, unique, foreign_key
- Data Lineage: source_system, ingest_time, source_column
- Compliance & Security: pii (Personally Identifiable Information), encryption_required
- Documentation: description, owner, version

```rust,ignore
use std::collections::HashMap;
use std::sync::Arc;
use datafusion::arrow::datatypes::{DataType, Field, Schema};

fn main() -> datafusion::error::Result<()> {
    // --- Attaching Metadata ---

    // Field-level metadata
    let id_field = Field::new("user_id", DataType::Int64, false).with_metadata(HashMap::from([
        ("primary_key".to_string(), "true".to_string()),
        ("source_system".to_string(), "crm".to_string()),
    ]));

    // Schema-level metadata (for the whole dataset)
    let schema_meta = HashMap::from([
        ("schema_version".to_string(), "v2.1".to_string()),
        ("owner".to_string(), "Analytics Team".to_string()),
    ]);

    let schema = Arc::new(Schema::new_with_metadata(
        vec![
            id_field,
            Field::new("email", DataType::Utf8, true)
                .with_metadata(HashMap::from([("pii".to_string(), "true".to_string())])),
        ],
        schema_meta,
    ));

    // --- Reading Metadata Back ---

    // From a field
    let field = schema.field_with_name("user_id")?;
    let is_pk = field.metadata().get("primary_key") == Some(&"true".to_string());
    assert!(is_pk);

    // From the schema
    let version = schema.metadata().get("schema_version");
    assert_eq!(version, Some(&"v2.1".to_string()));

    Ok(())
}
```

**Best Practices and Considerations**

- **Standardize your format**: use lowercase snake_case keys and parseable values (e.g., `"true"`, ISO 8601 timestamps/durations).
- **Re‑attach intentionally**: derived/aggregated columns don't inherit metadata—add it on the final output schema if needed.
- **Verify format support**: Arrow IPC preserves metadata; Parquet varies; CSV/NDJSON don't—treat as best‑effort across formats.
- **Reconcile on merge**: when sources disagree, prefer a canonical schema and explicitly resolve conflicts.
- **Keep it small**: avoid large blobs; store long docs externally and reference via a short key (e.g., `doc_url`).
- **Validate early**: add lightweight checks in tests/pipeline (e.g., require `owner`, `schema_version`, `pii` flags where applicable).

### Schema Inference: behavior and limits

Schema inference is sampling-based and format-dependent. Key points:

- **Sampling window:** only fields seen within [`schema_infer_max_records`] become columns; later unseen fields are ignored (no new columns are added).
- **CSV specifics:** parsing is positional; row-length mismatches error by default (use [`truncated_rows(true)`][`truncated_rows`] to allow shorter rows and fill NULLs for nullable fields).
- **NDJSON specifics:** alignment is name-based; missing keys become NULL only if the field is part of the inferred (or explicit) schema.
- **Types:** string tokens are not auto-cast to numeric/temporal types; choose explicit schemas where precision or safety matters (e.g., `Decimal128` for currency).
- **Configuration:** tune [`CsvReadOptions::schema_infer_max_records(...)`][`schema_infer_max_records`] and [`NdJsonReadOptions::schema_infer_max_records(...)`]([`schema_infer_max_records`]) to control sampling depth.

#### Schema Inference vs. Explicit Schemas

**When to use explicit schemas:**

- Production pipelines (prevents drift and ensures data quality)
- When you need specific types (e.g., `Decimal128` instead of `Float64`)
- Multi-file reads where schemas may vary slightly

**When inference is acceptable:**

- Interactive exploration and prototyping
- Single-file reads with consistent structure
- When you can validate the inferred schema before processing

**Increasing inference sample size:**

If you must use inference, increase the number of records sampled to reduce the risk of missing types:

```rust,ignore
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();

    let options = CsvReadOptions::new()
        .schema_infer_max_records(10_000);  // Default is 1000
    // let df = ctx.read_csv("data.csv", options).await?;

    // Demonstrate the option is set
    println!("Options configured");
    Ok(())
}
```

> **Warning:** Schema inference can drift as data evolves. A column that starts as integers may later contain decimals, causing runtime errors. Always validate inferred schemas before deploying to production.

<!-- TODO: Add link for `with_schema_infer_max_records` -->

<!-- TODO: Add a compact table comparing inference behavior (CSV vs NDJSON), with examples and links to the central guidance in creating-dataframes.md. -->

---

## Applying Schemas and Modeling Data

A schema defines the structure of your data—column names, types, nullability, and nested structures. Applying schemas when reading files enables planning-time validation, improves query performance, and ensures data quality. This section covers schema strategies for different file formats, handling schema evolution, partition pruning, and modeling nested data. <br> **See also:**

- [Schemas and Data Types](concepts.md#schemas-and-data-types) for fundamentals and
- [Creating DataFrames](./creating-dataframes.md) for file reading basics.

**Jump to:**

- [CSV](#strategy-1-text-formats-csv--ndjson--enforce-schemas)
- [NDJSON](#strategy-1-text-formats-csv--ndjson--enforce-schemas)
- [Parquet](#strategy-2-self-describing-formats-parquetavroarrow--merge--normalize)
- [Partitions](#strategy-3-partitioned-datasets--pruning-with-listingtable)
- [Nested Data](#strategy-4-nested-data--structlistmap-modeling)

### Strategy 1: Text Formats (CSV & NDJSON) — Enforce Schemas

**Text formats don't embed type information, so you must provide a schema for production workloads.**

Text-based formats are not self-describing; they don't embed type information. Without an explicit schema, DataFusion must infer types from a sample of rows set by [`schema_infer_max_records`]. This is fast for exploration but risky in production, as data drift can cause silent errors.

By providing a schema, you **enforce a contract** on the raw data, ensuring stability and correctness through planning-time validation.

| Format     |   Alignment    | Key Behaviors                                                                                                                       |
| :--------- | :------------: | :---------------------------------------------------------------------------------------------------------------------------------- |
| **CSV**    | **Positional** | Fields are mapped to schema columns by their order. The schema dictates the name, type, and nullability for each position.          |
| **NDJSON** | **Name-based** | JSON keys are mapped to schema fields by name. Order doesn't matter. This allows for more flexibility with missing or extra fields. |

#### CSV — Positional Alignment

Here, we dictate the exact types, including using `Decimal128` for currency to avoid floating-point errors. This prevents schema inference from incorrectly choosing `Int32` for an ID or `Float64` for money.

**Alignment** refers to how fields map to schema columns. For CSV, mapping is **positional**: the first column maps to the first field in your schema, the second to the second, and so on. Header names (if present with [`has_header(true)`][`has_header`]) are read but field order determines the mapping.

What the schema and options control:

- **Types**: values are parsed into the declared Arrow types (e.g., `Decimal128(19,2)` for currency).
- **Missing/extra columns**: by default, row length mismatches error; set [`truncated_rows(true)`][`truncated_rows`] to allow short rows and fill missing nullable columns with NULLs; extra columns still error.
- **Format details**: single-byte `delimiter`, `quote`, optional `escape`, `comment`, and `terminator`; newlines-in-values can be enabled explicitly.

```rust,ignore
use datafusion::prelude::*;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use std::sync::Arc;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();

    // Define the canonical schema for our sales data.
    let sales_schema = Arc::new(Schema::new(vec![
        Field::new("order_id", DataType::Int64, false),
        Field::new("customer_id", DataType::Utf8, false),
        Field::new("amount", DataType::Decimal128(19, 2), true),
    ]));

    // Configure CSV options with explicit schema
    let _options = CsvReadOptions::new()
        .schema(&sales_schema) // Enforce our canonical types
        .has_header(true)
        .delimiter(b',');

    // In practice, you would read from a file:
    // let df = ctx.read_csv("sales_data.csv", options).await?;

    // Output example with enforced types:
    // +----------+-------------+--------+
    // | order_id | customer_id | amount |
    // +----------+-------------+--------+
    // | 1001     | CUST-001    | 99.99  |
    // | 1002     | CUST-002    | 150.50 |
    // | 1003     | CUST-001    | 75.25  |
    // +----------+-------------+--------+

    Ok(())
}
```

**Production best practice:** Always provide explicit schemas for CSV in production. Schema inference samples only the first 1000 rows (default [`schema_infer_max_records`]) and can miss type variations in later data. Common pitfalls: IDs inferred as `Int32` then overflow, currency inferred as `Float64` (rounding errors), sparse columns inferred as `Utf8`.

#### NDJSON — Name-Based Alignment with Flexible Structure

NDJSON records are JSON objects; schemas are applied by field **name** (unlike CSV's positional alignment). This provides more flexibility for handling evolving data structures.

Key behaviors:

- **Alignment**: name-based mapping from JSON keys to schema fields—order doesn't matter.
- **Missing keys**: with an explicit schema, missing fields become NULL; without a schema, only keys seen during inference are included (later unseen keys are ignored).
- **Extra keys**: keys not present in the schema are ignored (no error, no column).
- **Types**: when a schema is provided, JSON values are cast to the declared Arrow types; invalid casts raise errors.
- **Nested data**: supports `Struct`, `List`, `Map` (CSV cannot express nested types).

```rust,ignore
use datafusion::prelude::*;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use std::sync::Arc;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true), // nullable field
    ]));

    // Configure NDJSON options with explicit schema
    let _options = NdJsonReadOptions::default().schema(&schema);

    // Example data (data.ndjson):
    // {"id": 1, "name": "Alice"}
    // {"id": 2}  // 'name' is missing -> becomes NULL

    // In practice: let df = ctx.read_json("data.ndjson", options).await?;

    // Output example with NULL for missing 'name' field:
    // +----+-------+
    // | id | name  |
    // +----+-------+
    // | 1  | Alice |
    // | 2  | NULL  |
    // +----+-------+

    Ok(())
}
```

### Strategy 2: Self-Describing Formats (Parquet/Avro/Arrow) — Merge & Normalize

**Self-describing formats embed schemas, but DataFusion automatically merges evolved schemas and you should normalize to canonical types.**

Self-describing formats are powerful because they embed a schema in each file. In practice, schemas evolve: IDs widen from `Int32` to `Int64`, decimals gain precision, and new columns appear. DataFusion handles this by automatically merging file schemas into a compatible super‑schema when you read multiple files.

**What auto-merge does:**

- Types are widened: e.g., `Int32` with `Int64` → `Int64`; `Decimal128(19,2)` with `Decimal128(38,9)` → `Decimal128(38,9)`.
- Columns are added: a column present only in some files appears in the merged schema as nullable.

#### Example: Automatic schema merging

```rust,ignore
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();

    // When reading multiple Parquet files with evolved schemas,
    // DataFusion automatically merges them:
    //
    // v1_sales.parquet (embedded schema):
    //   id: Int32
    //   amount: Decimal128(19, 2)
    //
    // v2_sales.parquet (evolved schema):
    //   id: Int64
    //   amount: Decimal128(38, 9)
    //   region: Utf8  // new column added
    //
    // let df = ctx.read_parquet(
    //     vec!["v1_sales.parquet", "v2_sales.parquet"],
    //     ParquetReadOptions::default()
    // ).await?;

    // Resulting merged schema (computed by DataFusion):
    //   id: Int64 (widened from Int32)
    //   amount: Decimal128(38, 9) (widened)
    //   region: Utf8 (nullable, since it's absent in v1)

    // Output showing auto-merged data with NULLs for missing 'region':
    // +----+--------+--------+
    // | id | amount | region |
    // +----+--------+--------+
    // | 1  | 99.99  | NULL   |
    // | 2  | 150.50 | NULL   |
    // | 3  | 75.25  | WEST   |
    // | 4  | 200.00 | EAST   |
    // +----+--------+--------+

    Ok(())
}
```

#### Defensive pattern: enforce a canonical schema

**Production best practice:** After reading self-describing formats, normalize to your application's canonical schema using [`.select()`] and [`.cast_to()`] to ensure stable downstream contracts.

For production pipelines, enforce a strict, predictable schema after read. This lets you explicitly drop columns you don't need and ensures type consistency.

```rust,ignore
use datafusion::prelude::*;
use datafusion::arrow::datatypes::DataType;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    // Example: normalizing to canonical schema after reading Parquet
    let df = dataframe!(
        "id" => [1_i32, 2_i32, 3_i64, 4_i64],  // Mixed types from different files
        "amount" => [99.99, 150.50, 75.25, 200.00]
    )?;

    // Normalize to your application's canonical schema
    let canonical_df = df.clone().select(vec![
        col("id").cast_to(&DataType::Int64, df.schema())?.alias("id"),
        col("amount").cast_to(&DataType::Float64, df.schema())?.alias("amount"),
    ])?;

    canonical_df.show().await?;
    // Result: strict canonical schema
    // +----+--------+
    // | id | amount |
    // +----+--------+
    // | 1  | 99.99  |
    // | 2  | 150.50 |
    // | 3  | 75.25  |
    // | 4  | 200.00 |
    // +----+--------+

    Ok(())
}
```

### Strategy 3: Partitioned Datasets — Pruning with ListingTable

**Hive-style partitioning lets DataFusion skip entire directories based on query filters, reducing data scanned.**

For very large datasets, organizing files into a directory structure based on column values (e.g., `year` and `month`) is a highly effective performance strategy. DataFusion's [`ListingTable`] is designed for this layout. It performs partition pruning, using your query's filters to skip reading entire directories that don't match. Partition pruning can skip entire directories, reducing scanned data volume. Use [`.explain()`] to verify pruning in your query plan. (For more about explain, see [Reading Explain Plans](../../user-guide/explain-usage.md))

**Example: Pruning a Parquet Dataset**

Imagine your data is stored like this:

```text
/data/events/year=2023/month=12/...
/data/events/year=2024/month=01/...
/data/events/year=2024/month=02/...
```

The following code sets up a `ListingTable` that will only read files from the `year=2024` directories when a filter is applied.

```rust,ignore
use std::sync::Arc;
use datafusion::prelude::*;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::{ListingOptions, ListingTableConfig, ListingTableUrl};

fn main() -> datafusion::error::Result<()> {
    // 1. Define the schema of the DATA INSIDE the Parquet files.
    //    Do NOT include the partition columns ('year', 'month') here.
    let file_schema = Arc::new(Schema::new(vec![
        Field::new("event_id", DataType::Utf8, false),
        Field::new("payload", DataType::Utf8, true),
    ]));

    // 2. Describe the directory structure and file format.
    //    The order of partition columns must match the directory nesting.
    let listing_options = ListingOptions::new(Arc::new(ParquetFormat::default()))
        .with_file_extension("parquet")
        .with_table_partition_cols(vec![
            ("year".into(),  DataType::Int32),
            ("month".into(), DataType::Int8),
        ]);

    // 3. Configure the ListingTable (in practice you'd use an actual path)
    let _config = ListingTableConfig::new(ListingTableUrl::parse("/data/events")?)
        .with_listing_options(listing_options)
        .with_schema(file_schema);

    // In practice:
    // let table = Arc::new(ListingTable::try_new(config)?);
    // let df = ctx.read_table(table)?;
    // let df_2024 = df.filter(col("year").eq(lit(2024)))?;
    //
    // Verify pruning with explain - look for partition pruning information:
    // df_2024.explain(false, false)?.show().await?;

    Ok(())
}
```

### Strategy 4: Nested Data — Struct/List/Map Modeling

Nested types preserve data relationships and avoid lossy flattening of hierarchical data.

Real-world data is often hierarchical. Instead of flattening JSON or Parquet sources and losing valuable structure, you can use DataFusion's nested types (`Struct`, `List`, `Map`) to model your domain accurately.

#### Defining Nested Schemas

Here's how to define a schema for a complex object with nested fields, arrays, and key-value attributes.

```rust,ignore
use std::sync::Arc;
use datafusion::arrow::datatypes::{DataType, Field, Schema, Fields};

fn main() {
    // Struct: A nested object with its own fields.
    let metadata_type = DataType::Struct(Fields::from(vec![
        Field::new("source", DataType::Utf8, true),
        Field::new("version", DataType::Int32, true),
    ]));

    // List: A variable-length array of a single type.
    let tags_type = DataType::List(
        Arc::new(Field::new("tag", DataType::Utf8, true))
    );

    // Map: Key-value pairs (represented as List<Struct<key,value>>)
    let attributes_type = DataType::Map(
        Arc::new(Field::new("entries",
            DataType::Struct(Fields::from(vec![
                Field::new("key", DataType::Utf8, false),
                Field::new("value", DataType::Int64, true),
            ])),
            false
        )),
        false, // keys_sorted
    );

    let schema = Arc::new(Schema::new(vec![
        Field::new("metadata", metadata_type, true),
        Field::new("tags", tags_type, true),
        Field::new("attributes", attributes_type, true),
    ]));

    println!("Schema has {} fields", schema.fields().len());
}
```

#### Querying Nested Fields

Defining the structure is the first half; the real power comes from querying it directly.

```rust,ignore
use datafusion::prelude::*;
use datafusion::functions_nested::expr_fn::array_element;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    // For nested field access, use bracket notation or get_field
    // Pattern 1: Extract a nested field from a Struct.
    // let sources_df = df.select(vec![
    //     col("metadata")["source"].alias("source"),
    // ])?;

    // Pattern 2: Filter based on a nested field's value.
    // let v2_plus_df = df.filter(
    //     col("metadata")["version"].gt_eq(lit(2))
    // )?;

    // Pattern 3: Extract the first element from a List (1-based indexing).
    // let first_tags_df = df.select(vec![
    //     array_element(col("tags"), lit(1)).alias("first_tag"),
    // ])?;

    // Demonstrate array_element import is valid
    let _expr = array_element(col("tags"), lit(1));

    Ok(())
}
```

**Pro-Tips for Type Selection:**

- **Standard vs. Large**: Use `LargeUtf8` or `LargeList` only when a single value (e.g., one string) might exceed 2GB. Standard types are generally more efficient.
- **Maps**: DataFusion does not enforce key uniqueness in maps. If your data might have duplicate keys, be prepared to handle them in your query logic.

---

## Schema Reuse and Versioning

**Centralized schemas prevent drift; explicit versioning tracks evolution.**

Scattered schema definitions—inlined in readers, duplicated in tests—inevitably diverge. By centralizing schemas in a dedicated module and versioning them explicitly (v1, v2, v3), you create a single source of truth that all components reference. This makes schema changes visible in code review, enables compatibility testing between versions, and documents exactly which contract each pipeline component expects.

```rust,ignore
// schemas.rs - Single source of truth for all schemas
use std::sync::Arc;
use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};

// v1 schema (baseline, backward‑compatible contract):
//   id:        Int64, required
//   name:      Utf8,  nullable
//   email:     Utf8,  nullable
pub fn customer_schema_v1() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("email", DataType::Utf8, true),
    ]))
}

// v2 schema (adds created_at; remains backward compatible):
//   id:         Int64, required
//   name:       Utf8,  nullable
//   email:      Utf8,  nullable
//   created_at: Timestamp(Microsecond, "UTC"), required  <-- New field
pub fn customer_schema_v2() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("email", DataType::Utf8, true),
        Field::new("created_at", DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())), false),
    ]))
}

fn main() {
    // Usage: centralized schemas for reuse
    let v1 = customer_schema_v1();
    let v2 = customer_schema_v2();
    println!("v1 has {} fields, v2 has {} fields", v1.fields().len(), v2.fields().len());

    // Test validates against v1 compatibility
    assert!(customer_schema_v2().field_with_name("id").is_ok());
}
```

In practice, you would use these schemas with readers:

```rust,ignore
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    // Demonstrate schema versioning concept
    // In practice:
    // let df_v1 = ctx.read_csv("customers_old.csv",
    //     CsvReadOptions::new().schema(&customer_schema_v1())
    // ).await?;
    // let df_v2 = ctx.read_csv("customers_new.csv",
    //     CsvReadOptions::new().schema(&customer_schema_v2())
    // ).await?;

    // When fusing v1 and v2 data:
    // - Add missing columns with NULLs
    // - Align types via cast_to
    // - Use union_by_name for name-based alignment

    Ok(())
}
```

Version new schemas when fields change. Store version in metadata (`schema.metadata.insert("version", "2")`). Test that DataFrames match expected versions (see [Schema Validation](#schema-validation)). Document breaking changes in a migration guide.

### The `TableProvider` Schema Contract

When you implement a custom [`TableProvider`], its [`schema()`][tableprovider::schema] method is a strict contract. The optimizer, join planner, and union logic all rely on it being stable and consistent across every call. Violating this contract can lead to query failures or silent data corruption.

| ✅ Best Practice                                                                       | ❌ Anti-Pattern                                                      |
| :------------------------------------------------------------------------------------- | :------------------------------------------------------------------- |
| Return the exact same [`SchemaRef`] on every call (cloning an `Arc<Schema>` is cheap). | Never change field order, types, or nullability between scans.       |
| Define the schema once when the provider is created and store it.                      | Derive the schema dynamically from the underlying data on each call. |

Example (standalone): capture one `SchemaRef` at construction and return clones on every call. In a real [`TableProvider`], [`.schema()`] would delegate to the stored `SchemaRef`.

```rust,ignore
use std::sync::Arc;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};

// Minimal, self-contained example showing a stable schema contract
struct MySource {
    schema: SchemaRef,
}

impl MySource {
    fn new() -> Self {
        let schema: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new(
                "created_at",
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                false
            ),
        ]));
        Self { schema }
    }

    // Stable across calls: always clone the stored SchemaRef
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

fn main() {
    let src = MySource::new();
    let s1 = src.schema();
    let s2 = src.schema();
    assert_eq!(s1.as_ref(), s2.as_ref()); // same logical schema every time
}
```

### Type Control with Macros and Literals

The [`dataframe!`] macro infers types from Rust literals—integers default to `Int32`, not `Int64`—which breaks [`.union()`] and [`.join()`] when types don't match exactly. (See also: [DataFrame macro](./creating-dataframes.md#5-from-inline-data-using-the-dataframe-macro)) Either cast after creation or use typed arrays from the start:

```rust,ignore
use datafusion::prelude::*;
use datafusion::arrow::datatypes::DataType;
use datafusion::arrow::array::Int64Array;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    // Problem: inferred as Int32
    let df = dataframe!("id" => [1, 2])?;

    // Solution 1: Cast to match schema
    let df = df.clone().with_column("id",
        col("id").cast_to(&DataType::Int64, df.schema())?
    )?;

    // Solution 2: Use typed arrays
    let _df = dataframe!(
        "id" => Int64Array::from(vec![1_i64, 2_i64])
    )?;

    df.show().await?;
    Ok(())
}
```

---

## Schema Evolution Patterns

**Schema evolution is the ongoing change of field names, types, and presence as data and systems grow.**
In DataFusion, some changes are absorbed automatically (name‑aligned unions, file‑level schema merging) while others require explicit normalization. This section shows what typically changes, how DataFusion handles it, and why you should still normalize to a canonical schema to keep pipelines stable and predictable.

### Common Evolution Scenarios

| Change Type                 | Risk Level |             Example             | Migration Strategy                 |
| :-------------------------- | :--------: | :-----------------------------: | :--------------------------------- |
| **Add nullable column**     |    Low     |  New `customer_segment` field   | Automatic via [`.union_by_name()`] |
| **Add non-nullable column** |   Medium   | Required `created_at` timestamp | Backfill or schema adapter         |
| **Widen type**              |    Low     |        `Int32` → `Int64`        | Automatic cast in readers          |
| **Narrow type**             |    High    |        `Int64` → `Int32`        | Validate then explicit cast        |
| **Rename column**           |    High    |    `custId` → `customer_id`     | Adapter layer with aliases         |
| **Remove column**           |   Medium   |      Drop deprecated field      | [`.select()`] to exclude           |
| **Change semantics**        |    High    |       `amount` USD → EUR        | Migration script required          |

Guidance:

- **Iterative ingestion**: align types with [`.cast_to()`] and merge shape with [`.union_by_name()`].
- **Self‑describing formats**: rely on automatic merge, then normalize via [`.select()`] and `.cast_to()`.
- **Renames/semantic shifts**: add an adapter layer until upstream and downstream agree.

Rules of thumb:

- **Prefer additive and widening changes**; add new fields as nullable.
- **Avoid in‑place renames**; publish aliases during transition.
- **Keep a canonical schema** and validate against it (see [Schema Reuse and Versioning](#schema-reuse-and-versioning), [Schema Validation](#schema-validation)).

See also: [Handling Missing Data & Nullability](#handling-missing-data--nullability), [Automatic Schema Merging for File Sources](#automatic-schema-merging-for-file-sources), [Performance Considerations](#performance-considerations).

### Pattern 1: Forward-Compatible Schema Design

Design schemas that can evolve without breaking existing readers or writers. By adding new fields as nullable and widening types (e.g., `Int32 → Int64` ), you preserve backward compatibility—old data remains valid and old queries continue to work, while new code can leverage the enhanced schema. This approach keeps pipelines stable as requirements grow, avoiding the cost and risk of rewriting historical data.

```rust,ignore
use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use std::sync::Arc;

// V1: Initial schema
pub fn orders_schema_v1() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("order_id", DataType::Int64, false),
        Field::new("customer_id", DataType::Int64, false),
        Field::new("amount", DataType::Decimal128(19, 2), false),
    ]))
}

// V2: Add optional columns (backward compatible)
pub fn orders_schema_v2() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("order_id", DataType::Int64, false),
        Field::new("customer_id", DataType::Int64, false),
        Field::new("amount", DataType::Decimal128(19, 2), false),
        Field::new("region", DataType::Utf8, true),
        Field::new("created_at", DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())), true),
    ]))
}

// V3: Widen precision (backward compatible)
pub fn orders_schema_v3() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("order_id", DataType::Int64, false),
        Field::new("customer_id", DataType::Int64, false),
        Field::new("amount", DataType::Decimal128(38, 9), false),
        Field::new("region", DataType::Utf8, true),
        Field::new("created_at", DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())), true),
    ]))
}

fn main() {
    let v1 = orders_schema_v1();
    let v2 = orders_schema_v2();
    let v3 = orders_schema_v3();
    println!("v1: {} fields, v2: {} fields, v3: {} fields",
        v1.fields().len(), v2.fields().len(), v3.fields().len());
}
```

### Pattern 2: Schema Adapter Layer

When source schemas diverge—legacy systems use `custId` vs. `customer_id`, or decimal precision drifts from `Decimal128(19,2)` to `Decimal128(38,9)`—a schema adapter normalizes variants before they reach your core logic. By inspecting the incoming schema and applying targeted renames ([`.alias()`]) and type casts ([`.cast_to()`]), you isolate schema churn at the pipeline's edge. Upstream systems evolve at different paces while your queries work against a single, stable contract.

```rust,ignore
use datafusion::prelude::*;
use datafusion::arrow::datatypes::DataType;
use datafusion::error::Result;

/// Adapter that normalizes various legacy schemas to current canonical schema
async fn normalize_orders(df: DataFrame) -> Result<DataFrame> {
    let schema = df.schema();

    // Detect schema version and adapt accordingly
    let normalized = if schema.field_with_name("custId").is_ok() {
        // Legacy schema: rename and cast
        df.select(vec![
            col("orderId").alias("order_id"),
            col("custId").cast_to(&DataType::Int64, schema)?.alias("customer_id"),
            col("amt").cast_to(&DataType::Decimal128(38, 9), schema)?.alias("amount"),
        ])?

    } else if schema.field_with_name("customer_id").is_ok() {
        // Modern schema: just ensure types are correct
        df.select(vec![
            col("order_id"),
            col("customer_id").cast_to(&DataType::Int64, schema)?,
            col("amount").cast_to(&DataType::Decimal128(38, 9), schema)?,
        ])?
    } else {
        return Err(datafusion::error::DataFusionError::Plan(
            "Unrecognized orders schema".to_string()
        ));
    };

    Ok(normalized)
}

#[tokio::main]
async fn main() -> Result<()> {
    // Modern schema example
    let df = dataframe!(
        "order_id" => [1001_i64],
        "customer_id" => [42_i64],
        "amount" => [9.99]
    )?;

    let normalized = normalize_orders(df).await?;
    normalized.show().await?;
    Ok(())
}
```

### Pattern 3: Schema Migration Testing

Seemingly harmless schema edits—dropping a field, narrowing a type, tightening nullability—can break pipelines or corrupt data. Guard against this with backward‑compatibility tests. For each new version, verify:

1. **Consistancy** every v1 field still exists in v2
2. **Types** are identical or widened (e.g., `Int32 → Int64`, `Decimal(19,2) → Decimal(38,9)`)
3. **Bullability** does not tighten (nullable → required is forbidden; required → nullable is safe).

These checks encode additive/widening evolution and catch regressions early (see also [avro-evolution], [kleppmann]).

```rust,ignore
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use std::sync::Arc;

fn orders_schema_v1() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("order_id", DataType::Int64, false),
        Field::new("customer_id", DataType::Int64, false),
    ]))
}

fn orders_schema_v2() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("order_id", DataType::Int64, false),
        Field::new("customer_id", DataType::Int64, false),
        Field::new("region", DataType::Utf8, true),  // New nullable field
    ]))
}

fn is_widening(from: &DataType, to: &DataType) -> bool {
    use DataType::*;
    matches!(
        (from, to),
        (Int8, Int16 | Int32 | Int64) |
        (Int16, Int32 | Int64) |
        (Int32, Int64) |
        (Float32, Float64)
    )
}

fn main() {
    let v1 = orders_schema_v1();
    let v2 = orders_schema_v2();

    // Verify backward compatibility: all v1 fields exist in v2
    for v1_field in v1.fields() {
        let v2_field = v2.field_with_name(v1_field.name())
            .expect(&format!("Field '{}' missing in v2", v1_field.name()));

        assert!(
            v2_field.data_type() == v1_field.data_type() ||
            is_widening(v1_field.data_type(), v2_field.data_type()),
            "Type changed for '{}'", v1_field.name()
        );
    }
    println!("Schema v2 is backward compatible with v1");
}
```

### Pattern 4: Handling Breaking Changes

**Use a multi-phase migration to safely roll out incompatible schema changes.**

Some changes are inherently breaking—renaming core fields, dropping columns, or narrowing types. A “big bang” cutover is risky and hard to roll back. A staged migration protects downstream consumers with a no‑downtime path, clear observability, and a deterministic rollback plan (see also [avro-evolution], [kleppmann]).

**Phase 1: Dual Writing**

```rust,ignore
use datafusion::prelude::*;
use datafusion::error::Result;
use datafusion::dataframe::DataFrameWriteOptions;

// Write data in both old and new formats during the transition
async fn write_dual_format(df: DataFrame) -> Result<()> {
    // Write v1 format for old consumers (minimal, stable contract)
    let v1_df = df.clone().select(vec![
        col("order_id"),
        col("customer_id"),
        col("amount"),
    ])?;
    // v1_df.write_parquet("data/v1/orders", DataFrameWriteOptions::default()).await?;

    // Write v2 format for new consumers
    // df.write_parquet("data/v2/orders", DataFrameWriteOptions::default()).await?;

    // Demonstrate the pattern compiles
    let _ = v1_df;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let df = dataframe!(
        "order_id" => [1_i64],
        "customer_id" => [42_i64],
        "amount" => [9.99],
        "region" => ["WEST"]
    )?;
    write_dual_format(df).await?;
    Ok(())
}
```

**Phase 2: Migration and Validation**

- Deploy new code that reads from v2.
- Keep v1 available as a fallback.
- Monitor v1/v2 read volumes, error rates, and data parity.

**Phase 3: Cleanup**

- Remove v1 read paths and decommission dual‑writing.
- Archive or delete v1 data.

### Execution Strategy & Common Pitfalls

| ✅ Best Practices                                                   | ❌ Common Pitfalls                                                                                                 |
| :------------------------------------------------------------------ | :----------------------------------------------------------------------------------------------------------------- |
| Use feature flags to canary the new schema, then broaden rollout.   | “Big bang” cutovers without a dual‑writing phase and a tested rollback plan.                                       |
| Backfill v2 so consumers see a consistent historical view.          | Relying on silent positional unions in SQL; prefer name‑aligned [`.union_by_name()`] with explicit casts/defaults. |
| Define success metrics (error rates, data parity) before you begin. | Failing to coordinate timelines and impact with downstream teams.                                                  |

### References

See also:

- [Handling Missing Data & Nullability](#handling-missing-data--nullability)
- [Automatic Schema Merging for File Sources](#automatic-schema-merging-for-file-sources)
- [Performance Considerations](#performance-considerations)

Further reading:

- [Parquet schema evolution][parquet-evolution]
- [Medium-article: All About Parquet Part 04][parquet-dremio]
- [Avro schema resolution][avro-evolution]
- [Designing Data-Intensive Applications][kleppmann]

---

## Further Reading

Resources for understanding Arrow’s type system, schema metadata, and DataFusion’s coercion rules—useful when debugging schema mismatches, unexpected casts, or expensive conversions.

### Arrow & Memory Model (Essential)

| Resource                                                                                      | Description                                                                                                                           |
| --------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------- |
| [Apache Arrow Columnar Format](https://arrow.apache.org/docs/format/Columnar.html)            | Physical memory layout, validity bitmaps, and variable-size views (for example, `StringView`) — explains why some casts are expensive |
| [Arrow Schema IPC Message](https://arrow.apache.org/docs/format/Columnar.html#schema-message) | How fields, metadata, and nullability are serialized — helpful when diagnosing “schema mismatch” errors                               |

### Storage ↔ Memory Type Mapping

| Resource                                                                                                             | Description                                                                                                           |
| -------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------- |
| [Parquet Logical Types](https://github.com/apache/parquet-format/blob/master/LogicalTypes.md)                        | How Parquet logical types (`DECIMAL`, timestamps, etc.) map into Arrow types                                          |
| [DataFusion Type Coercion Rules](https://docs.rs/datafusion/latest/datafusion/logical_expr/type_coercion/index.html) | The exact rules DataFusion uses to reconcile type differences (for example, joining or unioning `Int32` with `Int64`) |

### Execution & Optimization

| Resource                                                                                        | Description                                                                                                              |
| ----------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------ |
| [DataFusion Optimizer Rules](https://docs.rs/datafusion/latest/datafusion/optimizer/index.html) | How the optimizer rewrites plans (it may insert implicit `CAST`s); start with `type_coercion` and `simplify_expressions` |

### Books (Foundational)

| Resource                                                 | Description                                                                                                                                                                                                                                                              |
| -------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| The Data Model Resource Book (Vol 1–3) — Len Silverston  | Universal data models for common domains (Vol [1](https://www.oreilly.com/library/view/the-data-model/9780471380238/), [2](https://www.oreilly.com/library/view/the-data-model/9780471353485/), [3](https://www.oreilly.com/library/view/the-data-model/9780470178454/)) |
| Patterns of Data Modeling — David Hay                    | Conceptual modeling patterns that translate well to analytical schemas ([O’Reilly](https://www.oreilly.com/library/view/patterns-of-data/9781439819906/))                                                                                                                |
| The Data Warehouse Toolkit — Kimball & Ross              | Dimensional modeling (star schemas) for analytics ([O’Reilly](https://www.oreilly.com/library/view/the-data-warehouse/9781118530801/))                                                                                                                                   |
| Designing Data-Intensive Applications — Martin Kleppmann | Schema evolution and encoding trade-offs ([O’Reilly](https://www.oreilly.com/library/view/designing-data-intensive-applications/9781491903063/))                                                                                                                         |
| How Query Engines Work — Andy Grove                      | Query engine internals (DataFusion’s creator) ([Leanpub](https://leanpub.com/how-query-engines-work))                                                                                                                                                                    |

---

---

<

<!-- TODO: MAJOR RESTRUCTURE NEEDED - This section has gold content but poor organization -->
<!-- TODO: Move a concise "Type System essentials" primer to the start of this section (automatic coercion vs strict matching; safest widening rule; joins/unions require exact types). Either relocate the full type-system section here or add a 4–6 line summary with a link to details below. -->
<!-- TODO: Reorder subsections for flow: Type System → Four Mismatch Types → Diagnose → Resolve → Automatic Schema Merging → At‑a‑glance Checklist. -->
<!-- TODO: Add a compact table mapping each mismatch type → recommended fix (linking to code snippets). -->
<!-- TODO: Consider splitting into two sections: "Schema Fundamentals" (type system, coercion rules) and "Schema Problem Solving" (mismatches, debugging, recovery) -->
<!-- TODO: Add a visual diagram showing the type coercion hierarchy -->
<!-- TODO: Add more real-world examples from common scenarios (JSON ingestion, database migrations, API versioning) -->

<!-- TODO: Move a concise "Type System essentials" primer to the start of this section (automatic coercion vs strict matching; safest widening rule; joins/unions require exact types). Either relocate the full type-system section here or add a 4–6 line summary with a link to details below. -->
<!-- TODO: Add examples showing each coercion in practice -->
<!-- TODO: Explain what happens with Option<T> types -->
<!-- TODO: Document string type coercions (Utf8 vs LargeUtf8 vs Binary) -->

<!--
. Start with a primer on the Type System. (Fulfills Move this section earlier, CRITICAL foundational knowledge)
This sets the stage by explaining the "rules of the game."

2. Introduce the Four Mismatch Types. (Fulfills Reorder subsections)
This provides the core mental model for classifying any problem.

3. Show how to Diagnose the problem. (Fulfills Reorder subsections)
This teaches the mandatory first step.

4. Show how to Resolve the problem. (Fulfills Reorder subsections)
* Present union_by_name as the superpower for Name, Order, and Count.
* Present the cast-then-union pattern as the solution for Type.
* Present the select toolkit for advanced control.

5. Present the Alternative Strategies. (Fulfills Reorder subsections)
* Show "Automatic Schema Merging on Read" as the batch-loading alternative.

6. Provide the Summary and Checklists. (Fulfills Add a compact table, Extract a 5–7 line "At a glance" checklist)
This summarizes the key takeaways for quick reference.
-->

<!-- TODO: Placement: Move this subsection to the very start of "Schema Management" (before Root Causes) -->
<!-- TODO: Goal: Teach readers how to author explicit schemas and when to prefer them over inference -->
<!-- TODO: Include: A minimal, copy-pastable example for building an Arrow Schema -->
<!--
Topics to cover (each with a short, runnable snippet):
1) Building a Schema programmatically
   - Use Arrow types: `use datafusion::arrow::datatypes::{DataType, Field, Schema};`
   - `let schema = Arc::new(Schema::new(vec![ Field::new("id", DataType::Int64, false), Field::new("name", DataType::Utf8, true), ]));`
   - Explain `nullable` and why it matters
   - Use `SchemaRef` (`Arc<Schema>`) consistently
2) Applying schemas per source format
   - CSV: `CsvReadOptions::new().schema(&schema)` and when to avoid inference
   - JSON/NDJSON: show `JsonReadOptions`/`NdJsonReadOptions` equivalent if available; otherwise add note to verify API
   - Parquet: schema is embedded; show how to enforce a target logical schema via `.select()`/casts
   - Avro/Arrow IPC: note schema behavior and options briefly
3) Inference vs explicit schemas
   - When to use explicit schemas (production); when inference is acceptable (exploration)
   - If inferring, show how to increase `infer_schema_max_records`
4) Complex and time-related types
   - `Decimal128/256(precision, scale)` with guidance on choosing values
   - `Timestamp` with/without timezone; note timezone semantics
   - `Struct`, `List`, `Map` examples; Binary vs LargeBinary; Utf8 vs LargeUtf8
5) Nullability and defaults
   - Clarify that schemas do not encode default values
   - Show adding defaults with `.with_column()`/`.select()` (`coalesce`, `lit`) after scan
6) Reuse and versioning
   - Keep a canonical schema in code or a schema registry file
   - Reuse `SchemaRef` across reads and tests
7) Macros and literals
   - Show how `dataframe!` infers types; casting to enforce target types; using `lit()` safely
8) References
   - Link to Arrow `DataType`, `Field`, `Schema` docs and DataFusion read options per format
-->

<!-- TODO: Convert this callout into a Sphinx admonition (tip) for consistency with the rest of the docs. -->
