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

Schema management is the foundation of a robust data pipeline. It defines how you declare, evolve, and reconcile the structure of your data as it flows through DataFusion.

**Why Schemas Matter:**
Accurate types are critical for both **correctness** and **performance**. They allow the DataFusion optimizer to:

- Push down predicates efficiently.
- Select the fastest vectorized compute kernels.
- Leverage columnar statistics to skip irrelevant data.

### Where Schemas Come From

DataFusion determines your schema in one of three ways, depending on your data source:

1.  **Self-Describing Formats (Parquet, Avro, Arrow):** The schema is embedded in the file metadata. Types are known instantly at scan time.
2.  **Text Formats (CSV, JSON):** Types must be either **provided explicitly** (Recommended) or **inferred** from a data sample (Risk of drift).
3.  **Custom Sources (TableProvider):** The source of truth is the `schema()` method implemented by the provider. This contract must remain stable to ensure predictable query behavior.

For a deep dive into the underlying [Apache Arrow] type system, see the [Arrow Schema Specification][arrow schema].

> **Style Note:** In this guide, all code elements are highlighted with backticks. DataFrame methods are written as `.method()` (e.g., `.select()`) to reflect the chaining syntax central to the API. This distinguishes them from standalone functions (e.g., `col()`) and static constructors (e.g., `SessionContext::new()`). Rust types are formatted as `TypeName` (e.g., `SchemaRef`).

```{contents}
:local:
:depth: 2
```

This guide provides the fundamental concepts and practical tools within DataFusion's DataFrame API to handle these challenges effectively, making your data pipelines more robust and resilient to change.

## The Anatomy of a DataFusion Schema

A schema is the contract that defines the structure of your data. In DataFusion, this contract is composed of several key properties. Understanding each one is the key to diagnosing and solving mismatch errors. You can access the schema of a DataFrame at any time with the [`.schema()`] method.

Let's create a DataFrame and inspect its schema to see what it contains:

```rust
use datafusion::prelude::*;
use std::sync::Arc;
use datafusion::arrow::array::TimestampNanosecondArray;

// Create a DataFrame with a variety of column types.
let df = dataframe!(
    "user_id" => [1_i64, 2_i64, 3_i64],
    "email" => [Some("a@x.com"), None, Some("c@x.com")],
    "signup_date" => TimestampNanosecondArray::from(vec![
        Some(1704110400000000000), // 2024-01-01T12:00:00Z
        Some(1704196800000000000), // 2024-01-02T12:00:00Z
        Some(1704283200000000000), // 2024-01-03T12:00:00Z
    ]).with_timezone_opt(Some("UTC".to_string())),
)?;

// The default display format provides a simple, readable summary.
println!("{}", df.schema().to_string_pretty());

#[cfg(test)]
{
    // Optional: verify the values shape using the snapshot-style helper
    assert_batches_eq!(
        &[
            "+---------+-----------+---------------------------+",
            "| user_id | email     | signup_date               |",
            "+---------+-----------+---------------------------+",
            "| 1       | a@x.com   | 2024-01-01T12:00:00Z      |",
            "| 2       |           | 2024-01-02T12:00:00Z      |",
            "| 3       | c@x.com   | 2024-01-03T12:00:00Z      |",
            "+---------+-----------+---------------------------+",
        ],
        &df.collect().await?
    );
}
```

The output is a clean summary of the most important properties:

```text
-----------------
user_id:      Int64
email:        Utf8 (nullable)
signup_date:  Timestamp(Nanosecond, Some("UTC"))
-----------------
```

This [`DFSchema`] object is a DataFusion wrapper around the core [Apache Arrow `Schema`][arrow schema] type. To see all the internal details (like dictionary flags and metadata), you can get the underlying Arrow schema using [`df.schema().inner()`][DFSchema::inner] and print its debug view:

```rust
// The full Arrow schema shows all details.
println!("{:#?}", df.schema().inner());
```

This provides a much more verbose, low-level representation:

```text
Schema {
    fields: [
        Field { name: "user_id", data_type: Int64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} },
        Field { name: "email", data_type: Utf8, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} },
        Field { name: "signup_date", data_type: Timestamp(Nanosecond, Some("UTC")), nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} },
    ],
    metadata: {},
}
```

Reading this representation:

- **[`name`][Field]**: The column's identifier, used by DataFrame operations.
- **[`data_type`][Field]**: The Arrow `DataType` describing the column's logical and physical type.
- **[`nullable`][Field]**: Whether the column may contain [`NULL`] values (`true` = `NULL`s allowed).
- **[`metadata`][Field]**: Arbitrary key/value pairs that can be attached to a field or the whole schema. This is often used by file readers to add extra semantic information.

In the remainder of this section, we will focus on four practical aspects of schema management:

- [Column Names](#column-names)
- [Column Count](#column-count)
- [Column Order](#column-order)
- [Column Types](#column-types)

### 1. Column Names

The name is the primary identifier for a column in the DataFrame API. Operations like [`.select()`], [`.with_column()`], and [`.union_by_name()`] all rely on the column name to perform their work.

> **Important:** Column names in DataFusion are **case-sensitive**. A mismatch in capitalization (e.g., `Region` vs. `region`) is treated as a different column.
>
> **Best Practice:** Because names are case-sensitive, always enforce a consistent naming convention (e.g., all snake_case) at your ingestion boundary. See [Strategy C: The Schema Adapter](#strategy-c-the-surgical-fix-name-mismatches-complex-logic) for how to normalize names.

#### 2. Column Count

When aligning by name (for example, with [`.union_by_name()`]), missing columns are filled with `NULL` values. This refers to the number of columns in a DataFrame. When you combine two DataFrames, a difference in column count is handled gracefully by certain operations.

For example, if you use [`.union_by_name()`] to merge a DataFrame with a new column, the resulting DataFrame will contain the new column, with `NULL` values filling the rows that came from the DataFrame where it was missing. This is a deliberate feature to handle schema evolution safely. Dropped columns are handled similarly. This prevents silent errors by making the merged schema explicit.

### 3. Column Order

DataFusion's DataFrame API is **name-based, not positional**. Many SQL engines also support name-based alignment; use [`.union_by_name()`] to make alignment explicit and resilient to upstream ordering changes.

This means that for most operations, especially [`.union_by_name()`], the physical order of the columns in the files or DataFrames does not matter. DataFusion will correctly align `col_A` from one DataFrame (`DataFrame_A`) with `col_A` from another (`DataFrame_B`), regardless of their position. This makes pipelines resilient to changes in column ordering from upstream sources.

### 4. Column Types (The Type System)

Types drive planning-time validation, coercion, and operator selection—prefer widening over narrowing. In DataFusion's DataFrame API, every column must have a specific [Apache Arrow `DataType`][arrow dtype] that determines its storage format and computational behavior.

**Why types matter for DataFrames:**

- **Data integrity**: Types prevent mixing incompatible data (e.g., strings with numbers)
- **Performance**: Knowing types enables optimized columnar storage and vectorized operations
- **Predictability**: Type rules ensure consistent behavior across operations

**Common Arrow data types in DataFusion:**

| Category           |                                Arrow Types                                 |            Example Values             |           Common Use Cases            |
| :----------------- | :------------------------------------------------------------------------: | :-----------------------------------: | :-----------------------------------: |
| **Integers**       | `Int8`, `Int16`, `Int32`, `Int64`<br>`UInt8`, `UInt16`, `UInt32`, `UInt64` |           `42`, `-100`, `0`           |        IDs, counts, quantities        |
| **Floating-Point** |                            `Float32`, `Float64`                            |           `3.14`, `-0.001`            | Measurements, scientific data, ratios |
| **Decimal**        |                       `Decimal128(precision, scale)`                       |         `99.99`, `1234.5678`          |   Financial data, currency, prices    |
| **Strings**        |                            `Utf8`, `LargeUtf8`                             |         `"hello"`, `"データ"`         |    Names, descriptions, categories    |
| **Temporal**       |             `Date32`, `Date64`<br>`Timestamp(unit, timezone)`              | `2024-01-15`<br>`2024-01-15 14:30:00` |          Event times, dates           |
| **Boolean**        |                                 `Boolean`                                  |            `true`, `false`            |           Flags, conditions           |
| **Binary**         |                          `Binary`, `LargeBinary`                           |            `[0x12, 0x34]`             |           Raw data, hashes            |
| **Nested Types**   |                      `Struct(Fields)`, `List(Field)`                       |        `{"a": 1}`, `[1, 2, 3]`        |  JSON/Parquet data, complex objects   |

For a complete reference of all supported types, see the [SQL Data Types guide](../../user-guide/sql/data_types.md).

### A Note on Nullability

The `nullable` flag on a field is a critical part of its type definition. When merging schemas (for example, via [`.union_by_name()`]), DataFusion follows a simple, safe rule:

> The Golden Rule of Nullability: If a column is nullable in any of the input schemas, it will be nullable in the output schema.

This is a widening conversion: a non‑nullable column can always be represented in a nullable one, but not the other way around. For a deeper discussion of how NULL values behave in expressions, filters, and joins, see [Handling Missing Data & Nullability](#handling-missing-data--nullability) or
the [Handling Null Values guide](./concepts.md#handling-null-values).

### Type Behavior in DataFrames

DataFusion's type system has two distinct modes of operation, designed for a balance of convenience and safety. The behavior depends on whether you are working within a single DataFrame's expression or combining two different DataFrames.

#### Mode 1: Automatic Coercion in Expressions

For convenience and intuitive use, DataFusion automatically promotes types to a common, wider type when they are mixed within an expression. This applies to functions like [`.select()`], [`.with_column()`], and [`.filter()`]. The promotion is always "widening" and follows the safe upcasting paths defined in the Type Coercion Hierarchy to prevent data loss.

```rust
use datafusion::prelude::*;

// `int32_col` is safely promoted to Int64 to match `int64_col`.
let expr = col("int32_col") + col("int64_col");  // → Result is Int64

// `age` (an integer column) is safely promoted to Float64 for the comparison.
let cmp = col("age").gt(lit(25.5));
```

#### Mode 2: Strict Matching for Joins and Unions

For safety and to prevent silent data corruption, operations that combine entire DataFrames—specifically [`.join()`] and [`.union()`]—require the columns being merged or joined to have the exact same data type.

If the types do not match, DataFusion will return a schema error. This is a deliberate design choice. It forces you to be explicit about how to resolve the ambiguity, rather than having the engine guess and potentially corrupt your data.

```rust
use datafusion::prelude::*;
use datafusion::arrow::datatypes::DataType;

let df1 = dataframe!("id" => [1_i32, 2_i32])?;
let df2 = dataframe!("id" => [3_i64, 4_i64])?;

// This will FAIL because Int32 and Int64 are not an exact match.
// let bad = df1.union(df2)?;

// The Fix: explicitly cast one of the columns to match the other.
let df1_fixed = df1.with_column(
    "id",
    col("id").cast_to(&DataType::Int64, df1.schema())?
)?;

// This now works because both `id` columns are Int64.
let good = df1_fixed.union(df2)?;
```

**The Golden Rule of Type Casting**: Always widen types (e.g., `Int32 → Int64`) rather than narrow them to prevent data loss. Narrowing (e.g., `Int64 → Int32`) risks silent data corruption unless you have explicitly proven that no values will be truncated.

#### Type Coercion Hierarchy (safe upcasting paths):

Use this hierarchy to understand why DataFusion chooses a wider, common type when you mix types inside expressions, and how argument coercion works in function calls as defined by the [`TypeSignature`][typesignature]. It explains what conversions are safe (no data loss) and where explicit casts are required. The diagram covers the main families DataFusion can upcast automatically in expressions (not unions/joins): numeric (including decimal), strings, and temporal types.

```
Numeric types:
Int8 → Int16 → Int32 → Int64 → Float32 → Float64
                ↓
            Decimal128

String types:
Utf8 ↔ LargeUtf8 ↔ Utf8View
(Utf8View has highest precedence in function signatures)

Temporal types:
Date32 → Date64 → Timestamp → Timestamp with timezone
```

How to read this:

- **Numeric**
  - Mixing integers promotes to the widest integer (e.g., `Int32 + Int64 → Int64`)
  - Mixing integers and floats promotes to float (e.g., `Int32 + Float64 → Float64`)
  - Decimals take precedence with integers when possible; mixed with floats, the result is typically `Float64` (precision/scale permitting)
- **Strings**
  - `Utf8`, `LargeUtf8`, and `Utf8View` are aligned via planner-inserted casts; many functions prefer `Utf8View` as the target type
  - No automatic coercion from string columns to numeric/temporal types (string literals may be coerced to the target type in some contexts)
- **Temporal**
  - `Date32/Date64` combined with `Timestamp` promotes to `Timestamp`
  - Timezones must match in expressions; cast explicitly to align (some functions accept "any timezone" via a wildcard in their signatures)
- **Boolean and NULL**
  - Boolean does not auto-coerce to numeric
  - `NULL` adopts the other side's type in expressions (safe widening); otherwise remains `NULL`

Examples of automatic coercion in expressions:

- `Int32 + Float64` → both promoted to `Float64`
- `Date32` compared with `Timestamp` → `Date32` promoted to `Timestamp`
- `Decimal128(38, 9)` + `Int32` → `Decimal128(38, 9)` (if precision/scale allow)
- `Utf8` || `LargeUtf8` (string concatenation) → aligned string type

**Remember:** automatic coercion applies to expressions (e.g., [`.select()`], [`.with_column()`], [`.filter()`]), not to DataFrame-combining operations like [`.union()`] or [`.join()`], which require exact type matches.

---

## Handling Missing Data & Nullability

**Nullability is a fundamental schema property that determines whether a column can contain NULL values.**

In DataFusion, nullability is not just metadata—it's a critical part of your data contract that affects query behavior, performance, and correctness. Every field in a schema declares whether it can be nullable (`true`) or must always have a value (`false`). This section covers the foundational concepts of nullability and practical patterns for handling missing data.

### What: Understanding Nullability in Schemas

When you define a field, the third parameter controls nullability:

```rust
use datafusion::arrow::datatypes::{Field, DataType};

// Non-nullable: This field MUST always have a value
Field::new("user_id", DataType::Int64, false)

// Nullable: This field MAY contain NULL values
Field::new("email", DataType::Utf8, true)
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

```rust
use datafusion::prelude::*;
use datafusion::arrow::array::{Int64Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use std::sync::Arc;

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
```

### Common Patterns for Handling Missing Data

```rust
use datafusion::prelude::*;
use datafusion::functions::expr_fn::coalesce;
use datafusion::functions_aggregate::expr_fn::count;

// Pattern 1: Diagnose - understand the extent of missing data
let stats = df.aggregate(vec![], vec![
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
let complete_df = df.filter(
    col("email").is_not_null()
        .and(col("score").is_not_null())
)?;
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

```rust
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

```rust
use std::sync::Arc;
use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};

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
```

Each `Field` in the schema specifies:

- **Name**: The column identifier (case-sensitive)
- **DataType**: The type of values the column holds
- **Nullable**: Whether `NULL` values are permitted

> **Note**: Always use [`SchemaRef`] (`Arc<Schema>`) for efficient sharing. Cloning an `Arc` is O(1) and avoids deep copies of the schema structure.

### Default Values

Schemas define structure only, not default values. To provide defaults for `NULL` values, apply transformations after reading:

```rust
use datafusion::prelude::*; // for col, lit
use datafusion::functions::expr_fn::coalesce;

let df = df.with_column(
    "name",
    coalesce(vec![col("name"), lit("Unknown")])
)?;
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

```rust
// For currency: typically 2 decimal places
Field::new("price", DataType::Decimal128(19, 2), false),

// For percentages: more decimal places
Field::new("rate", DataType::Decimal128(10, 6), false),  // e.g., 0.123456
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

```rust
use datafusion::prelude::*; // for col
use datafusion::arrow::datatypes::{DataType, TimeUnit};

// This will cause a type error:
let with_tz = col("created_at");     // Timestamp(..., Some("UTC"))
let without_tz = col("scheduled_at"); // Timestamp(..., None)
let bad = with_tz.gt(without_tz);     // ERROR: incompatible types!

// Solution: Convert to the same type first
let good = with_tz.gt(
    without_tz.cast_to(
        &DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        schema
    )?
);
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

```rust
use std::collections::HashMap;
use std::sync::Arc;
use datafusion::arrow::datatypes::{DataType, Field, Schema};

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

```rust
let mut options = CsvReadOptions::new();
options.schema_infer_max_records = 10_000;  // Default is 1000
let df = ctx.read_csv("data.csv", options).await?;
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

```rust
use datafusion::prelude::*;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use std::sync::Arc;

let ctx = SessionContext::new();

// Define the canonical schema for our sales data.
let sales_schema = Arc::new(Schema::new(vec![
    Field::new("order_id", DataType::Int64, false),
    Field::new("customer_id", DataType::Utf8, false),
    Field::new("amount", DataType::Decimal128(19, 2), true),
]));

// Apply the schema and configure format-specific options.
let df = ctx.read_csv(
    "sales_data.csv",
    CsvReadOptions::new()
        .schema(&sales_schema) // Enforce our canonical types (schema).
        .has_header(true)
        .delimiter(b',')

).await?;

// Show the results with enforced types:
df.show().await?;
// Output example:
// +----------+-------------+--------+
// | order_id | customer_id | amount |
// +----------+-------------+--------+
// | 1001     | CUST-001    | 99.99  |
// | 1002     | CUST-002    | 150.50 |
// | 1003     | CUST-001    | 75.25  |
// +----------+-------------+--------+
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

```rust
use datafusion::prelude::*;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use std::sync::Arc;

let ctx = SessionContext::new();

let schema = Arc::new(Schema::new(vec![
    Field::new("id", DataType::Int64, false),
    Field::new("name", DataType::Utf8, true), // nullable field
]));

// Example (data.ndjson):
// {"id": 1, "name": "Alice"}
// {"id": 2}  // 'name' is missing -> becomes NULL

let df = ctx.read_json(
    "data.ndjson",
    NdJsonReadOptions::default().schema(&schema)
).await?;

// Show results with NULL for missing 'name' field:
df.show().await?;
// Output example:
// +----+-------+
// | id | name  |
// +----+-------+
// | 1  | Alice |
// | 2  | NULL  |
// +----+-------+
```

### Strategy 2: Self-Describing Formats (Parquet/Avro/Arrow) — Merge & Normalize

**Self-describing formats embed schemas, but DataFusion automatically merges evolved schemas and you should normalize to canonical types.**

Self-describing formats are powerful because they embed a schema in each file. In practice, schemas evolve: IDs widen from `Int32` to `Int64`, decimals gain precision, and new columns appear. DataFusion handles this by automatically merging file schemas into a compatible super‑schema when you read multiple files.

**What auto-merge does:**

- Types are widened: e.g., `Int32` with `Int64` → `Int64`; `Decimal128(19,2)` with `Decimal128(38,9)` → `Decimal128(38,9)`.
- Columns are added: a column present only in some files appears in the merged schema as nullable.

#### Example: Automatic schema merging

```rust
use datafusion::prelude::*;

// v1_sales.parquet (embedded schema):
//   id: Int32
//   amount: Decimal128(19, 2)
//
// v2_sales.parquet (evolved schema):
//   id: Int64
//   amount: Decimal128(38, 9)
//   region: Utf8  // new column added

let ctx = SessionContext::new();
let df = ctx.read_parquet(
    vec!["v1_sales.parquet", "v2_sales.parquet"],
    ParquetReadOptions::default()
).await?;

// Resulting merged schema (computed by DataFusion):
//   id: Int64
//   amount: Decimal128(38, 9)
//   region: Utf8 (nullable, since it's absent in v1)

df.show().await?;
// Output showing auto-merged data with NULLs for missing 'region':
// +----+--------+--------+
// | id | amount | region |
// +----+--------+--------+
// | 1  | 99.99  | NULL   |
// | 2  | 150.50 | NULL   |
// | 3  | 75.25  | WEST   |
// | 4  | 200.00 | EAST   |
// +----+--------+--------+
```

#### Defensive pattern: enforce a canonical schema

**Production best practice:** After reading self-describing formats, normalize to your application's canonical schema using [`.select()`] and [`.cast_to()`] to ensure stable downstream contracts.

For production pipelines, enforce a strict, predictable schema after read. This lets you explicitly drop columns you don't need and ensures type consistency.

```rust
use datafusion::prelude::*;
use datafusion::arrow::datatypes::DataType;

let ctx = SessionContext::new();

// Read files with evolved schemas (from previous example)
let df = ctx.read_parquet(
    vec!["v1_sales.parquet", "v2_sales.parquet"],
    ParquetReadOptions::default()
).await?;

// Normalize to your application's canonical schema
let canonical_df = df.select(vec![
    col("id").cast_to(&DataType::Int64, df.schema())?.alias("id"),
    col("amount").cast_to(&DataType::Decimal128(38, 9), df.schema())?.alias("amount"),
    // deliberately omit 'region'
])?;

canonical_df.show().await?;
// Result: strict canonical schema, 'region' dropped
// +----+-----------+
// | id | amount    |
// +----+-----------+
// | 1  | 99.990000 |
// | 2  | 150.500000|
// | 3  | 75.250000 |
// | 4  | 200.000000|
// +----+-----------+
```

### Strategy 3: Partitioned Datasets — Pruning with ListingTable

**Hive-style partitioning lets DataFusion skip entire directories based on query filters, reducing data scanned.**

For very large datasets, organizing files into a directory structure based on column values (e.g., `year` and `month`) is a highly effective performance strategy. DataFusion's [`ListingTable`] is designed for this layout. It performs partition pruning, using your query's filters to skip reading entire directories that don't match. Partition pruning can skip entire directories, reducing scanned data volume. Use [`.explain()`] to verify pruning in your query plan. (For more about explain, see [Reading Explain Plans](../../user-guide/explain-usage.md))

**Example: Pruning a Parquet Dataset**

Imagine your data is stored like this:

```
/data/events/year=2023/month=12/...
/data/events/year=2024/month=01/...
/data/events/year=2024/month=02/...
```

The following code sets up a `ListingTable` that will only read files from the `year=2024` directories when a filter is applied.

```rust
use std::sync::Arc;
use datafusion::prelude::*;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::{ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl};

let ctx = SessionContext::new();

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

let config = ListingTableConfig::new(ListingTableUrl::parse("/data/events")?)
    .with_listing_options(listing_options)
    .with_schema(file_schema); // Provide the file-only schema.

// 3. Create the table and run a pruned query.
let table = Arc::new(ListingTable::try_new(config)?);
let df = ctx.read_table(table)?;

// This filter is pushed down to the scan. DataFusion will only access
// the /data/events/year=2024/ subdirectories, skipping all others.
let df_2024 = df.filter(col("year").eq(lit(2024)))?;

// Verify pruning with explain:
df_2024.explain(false, false)?.show().await?;
// Look for partition pruning information in the physical plan output, e.g.:
// ParquetExec: file_groups=..., projection=..., output_ordering=[]
//   Pruning: pushed filters on partition columns: year = 2024
```

### Strategy 4: Nested Data — Struct/List/Map Modeling

Nested types preserve data relationships and avoid lossy flattening of hierarchical data.

Real-world data is often hierarchical. Instead of flattening JSON or Parquet sources and losing valuable structure, you can use DataFusion's nested types (`Struct`, `List`, `Map`) to model your domain accurately.

#### Defining Nested Schemas

Here's how to define a schema for a complex object with nested fields, arrays, and key-value attributes.

```rust
use std::sync::Arc;
use datafusion::arrow::datatypes::{DataType, Field, Schema};

// Struct: A nested object with its own fields.
let metadata_type = DataType::Struct(vec![
    Field::new("source", DataType::Utf8, true),
    Field::new("version", DataType::Int32, true),
]);

// List: A variable-length array of a single type.
let tags_type = DataType::List(
    Arc::new(Field::new("tag", DataType::Utf8, true))
);

// Map: Key-value pairs (represented as List<Struct<key,value>>)
let attributes_type = DataType::Map(
    Arc::new(Field::new("entries",
        DataType::Struct(vec![
            Field::new("key", DataType::Utf8, false),
            Field::new("value", DataType::Int64, true),
        ]),
        false
    )),
    false, // keys_sorted
);

let schema = Arc::new(Schema::new(vec![
    Field::new("metadata", metadata_type, true),
    Field::new("tags", tags_type, true),
    Field::new("attributes", attributes_type, true),
]));
```

#### Querying Nested Fields

Defining the structure is the first half; the real power comes from querying it directly.

```rust
use datafusion::prelude::*;
use datafusion::functions_nested::expr_fn::{get_field, array_element};

// Pattern 1: Extract a nested field from a Struct.
// This is the SQL-equivalent of `metadata.source`.
let sources_df = df.select(vec![
    get_field(col("metadata"), "source").alias("source"),
])?;

// Pattern 2: Filter based on a nested field's value.
let v2_plus_df = df.filter(
    get_field(col("metadata"), "version").gt_eq(lit(2))
)?;

// Pattern 3: Extract the first element from a List (1-based indexing).
let first_tags_df = df.select(vec![
    array_element(col("tags"), lit(1)).alias("first_tag"),
])?;

first_tags_df.show().await?;
// Output showing extracted nested field:
// +-----------+
// | first_tag |
// +-----------+
// | urgent    |
// | feature   |
// +-----------+
```

**Pro-Tips for Type Selection:**

- **Standard vs. Large**: Use `LargeUtf8` or `LargeList` only when a single value (e.g., one string) might exceed 2GB. Standard types are generally more efficient.
- **Maps**: DataFusion does not enforce key uniqueness in maps. If your data might have duplicate keys, be prepared to handle them in your query logic.

---

## Schema Reuse and Versioning

**Centralized schemas prevent drift; explicit versioning tracks evolution.**

Scattered schema definitions—inlined in readers, duplicated in tests—inevitably diverge. By centralizing schemas in a dedicated module and versioning them explicitly (v1, v2, v3), you create a single source of truth that all components reference. This makes schema changes visible in code review, enables compatibility testing between versions, and documents exactly which contract each pipeline component expects.

```rust
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

// Usage across your codebase:
use crate::schemas;
let ctx = SessionContext::new();
use datafusion::scalar::ScalarValue;

// Reader uses v1 schema
let df_v1 = ctx.read_csv("customers_old.csv",
    CsvReadOptions::new().schema(&schemas::customer_schema_v1())
).await?;


// Reader uses v2 schema
let df_v2 = ctx.read_csv("customers_new.csv",
    CsvReadOptions::new().schema(&schemas::customer_schema_v2())
).await?;

// Test validates against v1 compatibility
assert!(schemas::customer_schema_v2().field_with_name("id").is_ok());

// Fuse v1 and v2: add missing column to v1, align types, then name‑aligned union
let df_v1_aligned = df_v1.with_column(
    "created_at",
    // Add NULLs for v1 and cast to the target timestamp type
    lit(ScalarValue::Null).cast_to(
        &DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        df_v1.schema()
    )?
)?;

let fused = df_v1_aligned.union_by_name(df_v2)?;
// fused schema:
//   id: Int64
//   name: Utf8 (nullable)
//   email: Utf8 (nullable)
//   created_at: Timestamp(Microsecond, "UTC") (nullable due to v1 NULLs)
```

Version new schemas when fields change. Store version in metadata (`schema.metadata.insert("version", "2")`). Test that DataFrames match expected versions (see [Schema Validation](#schema-validation)). Document breaking changes in a migration guide.

### The `TableProvider` Schema Contract

When you implement a custom [`TableProvider`], its [`schema()`][TableProvider::schema] method is a strict contract. The optimizer, join planner, and union logic all rely on it being stable and consistent across every call. Violating this contract can lead to query failures or silent data corruption.

| ✅ Best Practice                                                                       | ❌ Anti-Pattern                                                      |
| :------------------------------------------------------------------------------------- | :------------------------------------------------------------------- |
| Return the exact same [`SchemaRef`] on every call (cloning an `Arc<Schema>` is cheap). | Never change field order, types, or nullability between scans.       |
| Define the schema once when the provider is created and store it.                      | Derive the schema dynamically from the underlying data on each call. |

Example (standalone): capture one `SchemaRef` at construction and return clones on every call. In a real [`TableProvider`], [`.schema()`] would delegate to the stored `SchemaRef`.

```rust
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

let src = MySource::new();
let s1 = src.schema();
let s2 = src.schema();
assert_eq!(s1.as_ref(), s2.as_ref()); // same logical schema every time
```

### Type Control with Macros and Literals

The [`dataframe!`] macro infers types from Rust literals—integers default to `Int32`, not `Int64`—which breaks [`.union()`] and [`.join()`] when types don't match exactly. (See also: [DataFrame macro](./creating-dataframes.md#5-from-inline-data-using-the-dataframe-macro)) Either cast after creation or use typed arrays from the start:

```rust
use datafusion::prelude::*;
use datafusion::arrow::datatypes::DataType;
use datafusion::arrow::array::Int64Array;

// Problem: inferred as Int32
let df = dataframe!("id" => [1, 2])?;

// Solution 1: Cast to match schema
let df = df.with_column("id",
    col("id").cast_to(&DataType::Int64, df.schema())?
)?;

// Solution 2: Use typed arrays
let df = dataframe!(
    "id" => Int64Array::from(vec![1_i64, 2_i64])
)?;
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

- **Iterative ingestion**: align types with `.cast_to()` and merge shape with [`.union_by_name()`].
- **Self‑describing formats**: rely on automatic merge, then normalize via [`.select()`] and `.cast_to()`.
- **Renames/semantic shifts**: add an adapter layer until upstream and downstream agree.

Rules of thumb:

- **Prefer additive and widening changes**; add new fields as nullable.
- **Avoid in‑place renames**; publish aliases during transition.
- **Keep a canonical schema** and validate against it (see [Schema Reuse and Versioning](#schema-reuse-and-versioning), [Schema Validation](#schema-validation)).

See also: [Handling Missing Data & Nullability](#handling-missing-data--nullability), [Automatic Schema Merging for File Sources](#automatic-schema-merging-for-file-sources), [Performance Considerations](#performance-considerations).

### Pattern 1: Forward-Compatible Schema Design

Design schemas that can evolve without breaking existing readers or writers. By adding new fields as nullable and widening types (e.g., `Int32 → Int64` ), you preserve backward compatibility—old data remains valid and old queries continue to work, while new code can leverage the enhanced schema. This approach keeps pipelines stable as requirements grow, avoiding the cost and risk of rewriting historical data.

```rust
use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use std::sync::Arc;

// V1: Initial schema
//   order_id:    Int64,                      required
//   customer_id: Int64,                      required
//   amount:      Decimal128(19, 2),          required
pub fn orders_schema_v1() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("order_id", DataType::Int64, false),
        Field::new("customer_id", DataType::Int64, false),
        Field::new("amount", DataType::Decimal128(19, 2), false),
    ]))
}

// V2: Add optional columns (backward compatible)
//   Changes vs v1:
//     + region:     Utf8,                     nullable
//     + created_at: Timestamp(Microsecond, "UTC"), nullable
//   (All v1 fields preserved unchanged; additions are nullable to keep v1 files readable.)
pub fn orders_schema_v2() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("order_id", DataType::Int64, false),
        Field::new("customer_id", DataType::Int64, false),
        Field::new("amount", DataType::Decimal128(19, 2), false),
        // New fields are nullable to support old data
        Field::new("region", DataType::Utf8, true),
        Field::new("created_at", DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())), true),
    ]))
}

// V3: Widen precision (backward compatible)
//   Changes vs v2:
//     ~ amount: Decimal128(19, 2) → Decimal128(38, 9)  (widening precision/scale)
//   (Widening avoids data loss; other fields unchanged.)
pub fn orders_schema_v3() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("order_id", DataType::Int64, false),
        Field::new("customer_id", DataType::Int64, false),
        Field::new("amount", DataType::Decimal128(38, 9), false),  // Increased precision
        Field::new("region", DataType::Utf8, true),
        Field::new("created_at", DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())), true),
    ]))
}
```

### Pattern 2: Schema Adapter Layer

When source schemas diverge—legacy systems use `custId` vs. `customer_id`, or decimal precision drifts from `Decimal128(19,2)` to `Decimal128(38,9)`—a schema adapter normalizes variants before they reach your core logic. By inspecting the incoming schema and applying targeted renames ([`.alias()`]) and type casts ([`.cast_to()`]), you isolate schema churn at the pipeline's edge. Upstream systems evolve at different paces while your queries work against a single, stable contract.

```rust
use datafusion::prelude::*;
use datafusion::arrow::datatypes::DataType;

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

// Input (legacy)
// +---------+--------+------+
// | orderId | custId | amt  |
// +---------+--------+------+
// | 1001    | 42     | 9.99 |
// +---------+--------+------+
//
// After normalize_orders(...)
// +----------+-------------+-----------+
// | order_id | customer_id | amount    |
// +----------+-------------+-----------+
// | 1001     | 42          | 9.990000  |
// +----------+-------------+-----------+
```

### Pattern 3: Schema Migration Testing

Seemingly harmless schema edits—dropping a field, narrowing a type, tightening nullability—can break pipelines or corrupt data. Guard against this with backward‑compatibility tests. For each new version, verify:

1. **Consistancy** every v1 field still exists in v2
2. **Types** are identical or widened (e.g., `Int32 → Int64`, `Decimal(19,2) → Decimal(38,9)`)
3. **Bullability** does not tighten (nullable → required is forbidden; required → nullable is safe).

These checks encode additive/widening evolution and catch regressions early (see also [avro-evolution], [kleppmann]).

```rust
use datafusion::arrow::datatypes::Schema;
use std::sync::Arc;

/// Test that v2 schema is backward compatible with v1
#[test]
fn test_schema_backward_compatibility() {
    let v1 = orders_schema_v1();
    let v2 = orders_schema_v2();

    // All v1 fields must exist in v2 with compatible types
    for v1_field in v1.fields() {
        let v2_field = v2.field_with_name(v1_field.name())
            .expect(&format!("Field '{}' missing in v2", v1_field.name()));

        // Type must be same or wider
        assert!(
            v2_field.data_type() == v1_field.data_type() ||
            is_widening(v1_field.data_type(), v2_field.data_type()),
            "Type changed for '{}': {:?} → {:?}",
            v1_field.name(),
            v1_field.data_type(),
            v2_field.data_type()
        );

        // Nullability can only widen (false → true)
        if !v1_field.is_nullable() {
            assert!(
                !v2_field.is_nullable(),
                "Field '{}' changed from non-nullable to nullable",
                v1_field.name()
            );
        }
    }
}

fn is_widening(from: &DataType, to: &DataType) -> bool {
    use datafusion::arrow::datatypes::DataType::*;
    matches!(
        (from, to),
        (Int8, Int16 | Int32 | Int64) |
        (Int16, Int32 | Int64) |
        (Int32, Int64) |
        (Float32, Float64) |
        (Decimal128(p1, s1), Decimal128(p2, s2)) if p2 >= p1 && s2 >= s1
    )
}
```

### Pattern 4: Handling Breaking Changes

**Use a multi-phase migration to safely roll out incompatible schema changes.**

Some changes are inherently breaking—renaming core fields, dropping columns, or narrowing types. A “big bang” cutover is risky and hard to roll back. A staged migration protects downstream consumers with a no‑downtime path, clear observability, and a deterministic rollback plan (see also [avro-evolution], [kleppmann]).

**Phase 1: Dual Writing**

```rust
use datafusion::prelude::*;
use datafusion::error::Result;

// Write data in both old and new formats during the transition
async fn write_dual_format(
    ctx: &SessionContext,
    df: &DataFrame
) -> Result<()> {
    // Write v1 format for old consumers (minimal, stable contract)
    let v1_df = df.select(vec![
        col("order_id"),
        col("customer_id"),
        col("amount"),
    ])?;
    v1_df.write_parquet("data/v1/orders", DataFrameWriteOptions::default()).await?;

    // Write v2 format for new consumers
    df.write_parquet("data/v2/orders", DataFrameWriteOptions::default()).await?;

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

## Schema and Data Validation

**Validation protects your pipeline at two levels: schema validation ensures structure is correct, data validation ensures values are correct.**

In production, data arrives from untrusted sources: third-party APIs change field types without warning, CSV uploads contain malformed values, and even "stable" partners send out-of-range data. DataFusion trusts what you give it—it won't automatically validate schemas or data quality.

This section covers:

- **Schema validation** (metadata checks: field names, types, nullability)
- **Data validation** (value checks: ranges, uniqueness, patterns)
- **When to use external libraries** for complex validation rules

> **Important distinction**: Schema validation checks metadata only (microseconds, no data scan). Data validation scans actual row values (performance depends on data size).

### Schema Validation (Structure & Types)

**Schema validation ensures your data has the right structure—correct field names, compatible types, expected nullability—before you process it.**

DataFusion's [`DFSchema`] provides methods for comparing schemas, looking up fields, and checking type compatibility. Below are three commonly used patterns—for additional methods, see the [`DFSchema` documentation](https://docs.rs/datafusion/latest/datafusion/common/struct.DFSchema.html).

#### Comparing Schemas for Compatibility

Use [`DFSchema::logically_equivalent_names_and_types()`] to verify two schemas are compatible before combining DataFrames. This checks for logical equivalence: same field names, compatible types, and matching qualifiers. Essential for validating unions, joins, or ensuring schema contracts haven't changed between versions.

```rust
use datafusion::prelude::*;
use datafusion::error::Result;

async fn merge_regional_sales(ctx: &SessionContext) -> Result<DataFrame> {
    // Load sales data from two regions
    let us_sales = ctx.read_parquet("data/us_sales.parquet",
        ParquetReadOptions::default()).await?;
    let eu_sales = ctx.read_parquet("data/eu_sales.parquet",
        ParquetReadOptions::default()).await?;

    // Before combining, verify schemas match
    let us_schema = us_sales.schema();
    let eu_schema = eu_sales.schema();

    if !us_schema.logically_equivalent_names_and_types(&eu_schema) {
        return Err(DataFusionError::Plan(format!(
            "Regional schemas incompatible. US: {:?}, EU: {:?}",
            us_schema.field_names(),
            eu_schema.field_names()
        )));
    }

    // Schemas match; safe to union
    let combined = us_sales.union(eu_sales)?;
    Ok(combined)
}
```

#### Validating Type Cast Safety

Use [`arrow::compute::can_cast_types()`] to check if one type can be safely cast to another without data loss. This follows Arrow's type coercion rules: widening conversions (`Int32`→ `Int64`, `Decimal(19,2)` → `Decimal(38,9)`) are safe; narrowing conversions risk truncation and require explicit validation. Critical for schema normalization and preventing silent data corruption.

```rust
use datafusion::arrow::compute::can_cast_types;
use datafusion::arrow::datatypes::DataType;

let from_type = DataType::Int32;
let to_type = DataType::Int64;

if can_cast_types(&from_type, &to_type) {
    // Safe: Int32 fits in Int64 without truncation
    df = df.with_column("id", col("id").cast_to(&to_type, df.schema())?)?;
} else {
    return Err(/* incompatible types */);
}

// Examples:
// ✅ Int8 → Int64          (widening, safe)
// ✅ Decimal(19,2) → Decimal(38,9)  (precision increase, safe)
// ❌ Int64 → Int32         (narrowing, unsafe—truncates large values)
// ❌ Utf8 → Int64          (incompatible types)
```

#### Looking Up Required Fields

Use [`DFSchema::field_with_name()`] to look up a field by name and retrieve its metadata (type, nullability). Essential for validating that required fields exist before processing and for checking field types match expectations. Returns an error if the field is missing, enabling fail-fast validation.

```rust
use datafusion::prelude::*;
use datafusion::arrow::datatypes::DataType;

// Fail fast if field missing or wrong type
let field = df.schema()
    .field_with_name("customer_id")
    .map_err(|_| DataFusionError::Plan("Missing required field: 'customer_id'".into()))?;

if field.data_type() != &DataType::Int64 {
    return Err(DataFusionError::Plan(format!(
        "Field 'customer_id' must be Int64, got {:?}",
        field.data_type()
    )));
}
```

> **Performance**: These checks operate on schema metadata (typically <1KB), completing in microseconds regardless of DataFrame size.

### Production Pattern: Validate-Then-Normalize

The most robust production pattern validates critical fields, then normalizes compatible types. This catches breaking changes (missing fields, incompatible types) while tolerating minor drift ( `Int32` where you expect `Int64`, slightly different decimal precision).

#### Flexible Schema Enforcement (pattern)

Pattern, not an API: validate that required fields exist, and where types differ but are castable, align them with [`.cast_to()`] before union/join; otherwise fail fast. This keeps schema normalization explicit and cheap (metadata checks up front, casts executed at runtime).

Example (inline): validate presence and normalize a single field before union/join.

```rust
use datafusion::prelude::*;
use datafusion::arrow::compute::can_cast_types;
use datafusion::arrow::datatypes::DataType;
use datafusion::error::{DataFusionError, Result};

async fn pipeline_step(mut df: DataFrame) -> Result<DataFrame> {
    // 1) Validate presence
    let field = df.schema()
        .field_with_name("order_id")
        .map_err(|_| DataFusionError::Plan("Missing required field: 'order_id'".into()))?;

    // 2) If types differ, check cast safety and normalize
    if field.data_type() != &DataType::Int64 {
        if can_cast_types(field.data_type(), &DataType::Int64) {
            df = df.with_column(
                "order_id",
                col("order_id").cast_to(&DataType::Int64, df.schema())?
            )?;
        } else {
            return Err(DataFusionError::Plan(format!(
                "Cannot cast 'order_id' from {:?} to Int64",
                field.data_type()
            )));
        }
    }

    // 3) Proceed: union_by_name / join now see a consistent Int64 'order_id'
    Ok(df)
}
```

When to use:

- Partner integrations with minor drift (e.g., `Int32` vs `Int64`)
- Multi-tenant inputs that vary slightly
- Data lakes with independently evolving producers

> Note:<br>
> A full “helper” implementation belongs in recipes/advanced docs. See the [advanced section](dataframes-advance.md) for a complete version.

### Strict Validation for Compliance Workloads

For regulatory/financial data, every field must match exactly—same name, type, nullability, and order. Any deviation signals data corruption or upstream contract violation.

#### Strict Schema Equality (fail-fast pattern)

This pattern performs a strict equality check: field count, field order, field names, data types, and nullability must all match exactly. It uses Arrow's `Field::PartialEq` implementation for precise comparison and fails fast on the first mismatch.
An example of this strategy is shown in the following:

```rust
use datafusion::prelude::*;
use datafusion::arrow::datatypes::Schema;

// Inline strict equality check (order-sensitive)
let actual = df.schema().as_arrow();
let expected: &Schema = &sox_report_schema_v2(); // your expected contract

// 1) Verify field count
assert_eq!(
    actual.fields().len(),
    expected.fields().len(),
    "Field count mismatch"
);

// 2) Validate each field exactly (name, type, nullability, metadata)
for (i, expected_field) in expected.fields().iter().enumerate() {
    let actual_field = &actual.fields()[i];
    assert!(
        actual_field == expected_field,
        "Field[{}] mismatch: expected {:?}, got {:?}",
        i, expected_field, actual_field
    );
}
// Proceed: schemas match exactly
```

**What it does:**

- Checks field count matches exactly
- Iterates fields in order, comparing each with `==` (uses `Field::PartialEq`)
- Returns error immediately on first mismatch
- Validates: name, type, nullability, metadata

**When to use:**

- Regulatory compliance (SOX, GDPR, HIPAA) where schema drift indicates audit failure
- Financial transactions where field order/type changes could cause accounting errors
- API contracts with strict SLAs where schema changes require versioned rollout
- Critical ML inference pipelines where type mismatches corrupt predictions

> **Performance note**: Schema validation checks metadata (typically <1KB), not data. Completes in microseconds even for billion-row DataFrames.

### Data Validation (Actual Values)

**After schema validation confirms structure, data validation ensures values meet business rules—no negative amounts, valid emails, unique IDs.**

Unlike schema checks, data validation scans actual row values. Use DataFrame operations to detect violations, then fail fast or quarantine bad data.

<!--TODO: Start here -->

#### Validating Value Constraints

Value constraints enforce the domain rules that a schema alone cannot express: positive monetary amounts, realistic percentages, mandatory identifiers, and so on. Use them to short-circuit bad data before it propagates into joins, aggregates, or downstream systems.

**Three-step workflow**

1. **Express violations:** as a boolean expression with [`col()`], comparisons, and logical connectors.
2. **Use [`.filter()`]:** to isolate the violating rows so you can inspect or count them cheaply.
3. **Decide how to respond:** fail fast, write the rows to quarantine, or continue with the negated filter (for example `violation_filter.clone().not()` shown below).

Typical predicates:

| Purpose              |                        Example expression                         |
| -------------------- | :---------------------------------------------------------------: |
| Range checks         |       `col("amount").between(lit(0.01), lit(1_000_000.0))`        |
| Completeness checks  |               `col("required_field").is_not_null()`               |
| Format checks        |                 `col("email").like(lit("%@%.%"))`                 |
| Composite conditions | chain `.and()`, `.or()`, `.not()` for multi-column business rules |

```rust
use datafusion::prelude::*;

// Validate critical orders before closing the books
let df = ctx.read_parquet("orders.parquet", ParquetReadOptions::default()).await?;

// Build a filter for all violations
let violation_filter =
    col("amount").lt_eq(lit(0))                         // Negative or zero amounts
        .or(col("amount").gt(lit(1_000_000)))           // Suspiciously large amounts
        .or(col("order_id").is_null())                  // Missing order ID
        .or(col("customer_email").is_null())            // Missing email
        .or(col("customer_email").not_like(lit("%@%"))); // Invalid email format

// Materialize violations
let violations = df.filter(violation_filter.clone())?;
let violation_count = violations.count().await?;

if violation_count > 0 {
    println!("{violation_count} invalid orders detected");
    violations.show_limit(10).await?;

    // Option A: fail fast
    return Err(DataFusionError::Plan(format!("{violation_count} validation failures")));

    // Option B: quarantine then continue
    // violations.write_parquet("quarantine/bad_orders.parquet", options).await?;
    // let clean = df.filter(violation_filter.clone().not())?;
}
```

**Run these checks** at ingestion boundaries, after transformations that change business meaning (currency conversion, enrichment joins), and right before high-impact sinks such as financial exports or ML feature generation.

#### Checking for Duplicates

Duplicate detection protects unique keys (orders, invoices, device IDs) whose duplication leads to double-counted revenue, ambiguous joins, or inflated metrics. The DataFrame API gives you a compact pattern for detecting these issues and then deciding whether to fail, remediate automatically, or route them for human review.

**Workflow:**

1. **Group by:** the field that should be unique and compute a [`count()`] aggregate per key.
2. **Filter for `count > 1`:** to materialize only the duplicates.
3. **Inspect the keys and/or join:** back to the original DataFrame to fetch full offending records.

```rust
use datafusion::prelude::*;
use datafusion_functions_aggregate::count::count;

// In practice you would put this logic into a small validation helper that
// returns a DataFrame of violations (or an error) instead of printing.
let df = ctx.read_csv("orders.csv", CsvReadOptions::default()).await?;

// 1. Aggregate by the key that should be unique and count occurrences
let duplicates = df
    .aggregate(
        vec![col("order_id")],
        vec![count(col("order_id")).alias("count")]
    )?
    .filter(col("count").gt(lit(1)))?;

// 2. Check if any duplicates exist at all
let has_duplicates = duplicates.count().await? > 0;

if has_duplicates {
    println!("Duplicate order IDs found");

    // 3. Inspect a summary: which IDs are worst offenders?
    duplicates
        .sort(col("count").sort(false, true))?
        .show_limit(20).await?;

    // 4. Join back to the original DataFrame to get full duplicate records
    let duplicate_ids = duplicates.select(vec![col("order_id")])?;
    let duplicate_records = df.join(
        duplicate_ids,
        JoinType::Inner,
        &["order_id"],
        &["order_id"],
        None
    )?;

    println!("Full duplicate records:");
    duplicate_records.show_limit(50).await?;
}
```

For multi-terabyte tables, restrict the scan to recent partitions or call the DataFrame method [`.distinct()`] (for example on a projected key column) to shrink the working set. Feed the duplicate list into remediation automation (fix-and-retry jobs, support tickets) rather than leaving the insight in console logs.

#### Range and Statistical Validation

While row-level checks catch individual bad values, statistical validation reveals the hidden problems that kill production systems: a pricing algorithm stuck at zero, sensor readings that gradually drift out of calibration, or that batch job that silently stopped processing half your data last Tuesday. These issues slip through traditional validation because each record looks fine in isolation—it's only when you aggregate that the pattern emerges.

Statistical validation compares your data's shape against known good baselines. When metrics deviate beyond expected ranges, you catch problems before they compound into disasters.

**Key metrics to monitor:**

- **Bounds:** [`min()`] / [`max()`] ensure values stay within physical or business limits
- **Central tendency:** [`avg()`] or [`median()`] detect systematic shifts in your data
- **Spread:** [`stddev()`] catches when your normally stable metrics start going haywire
- **Completeness:** [`count()`] or [`count_distinct()`][`count()`] verifies you're receiving expected volumes

```rust
use datafusion::prelude::*;
use datafusion_functions_aggregate::{
    min_max::{max, min},
    average::avg,
    stddev::stddev,
    count::count
};

// Real-world example: Validate daily transaction data against baselines
let df = ctx.read_parquet("daily_transactions.parquet", ParquetReadOptions::default()).await?;

// 1. Compute key statistics in a single pass
let stats = df.aggregate(
    vec![],
    vec![
        count(col("transaction_id")).alias("total_transactions"),
        min(col("amount")).alias("min_amount"),
        max(col("amount")).alias("max_amount"),
        avg(col("amount")).alias("avg_amount"),
        stddev(col("amount")).alias("stddev_amount"),
    ]
)?;

// 2. Define validation thresholds (in production, load from config or historical data)
struct ValidationThresholds {
    min_allowed: f64,        // Legal minimum (e.g., $0.01)
    max_allowed: f64,        // Fraud prevention ceiling
    expected_avg: f64,       // Yesterday's average
    expected_count: i64,     // Typical daily volume
    max_stddev_ratio: f64,   // Volatility alarm threshold
}

let thresholds = ValidationThresholds {
    min_allowed: 0.01,
    max_allowed: 10_000.0,
    expected_avg: 247.50,
    expected_count: 50_000,
    max_stddev_ratio: 2.0,
};

// 3. Extract and validate metrics
let batch = &stats.collect().await?[0];
use datafusion::arrow::array::{Float64Array, Int64Array};

// Helper function to extract values (in practice, use a proper extraction utility)
let extract_f64 = |name: &str| -> f64 {
    batch.column_by_name(name)
        .and_then(|c| c.as_any().downcast_ref::<Float64Array>())
        .unwrap()
        .value(0)
};

let actual_count = batch.column_by_name("total_transactions")
    .and_then(|c| c.as_any().downcast_ref::<Int64Array>())
    .unwrap()
    .value(0);

let min_amount = extract_f64("min_amount");
let max_amount = extract_f64("max_amount");
let avg_amount = extract_f64("avg_amount");
let stddev_amount = extract_f64("stddev_amount");

// 4. Apply validation rules and collect violations
let mut violations = Vec::new();

if min_amount < thresholds.min_allowed {
    violations.push(format!("[ERROR] Min amount ${:.2} below floor ${:.2}",
        min_amount, thresholds.min_allowed));
}

if max_amount > thresholds.max_allowed {
    violations.push(format!("[ERROR] Max amount ${:.2} exceeds ceiling ${:.2}",
        max_amount, thresholds.max_allowed));
}

let avg_deviation = ((avg_amount - thresholds.expected_avg) / thresholds.expected_avg).abs();
if avg_deviation > 0.25 {
    violations.push(format!("[WARN] Average shifted {:.0}% from baseline", avg_deviation * 100.0));
}

let count_deviation = ((actual_count - thresholds.expected_count) as f64 / thresholds.expected_count as f64).abs();
if count_deviation > 0.30 {
    violations.push(format!("[WARN] Volume changed {:.0}% from normal", count_deviation * 100.0));
}

// 5. Take action based on severity
if !violations.is_empty() {
    println!("Data quality issues detected:");
    for violation in &violations {
        println!("  {}", violation);
    }

    // In production: send alerts, pause downstream processing, or trigger remediation
    if violations.iter().any(|v| v.starts_with("[ERROR]")) {
        return Err(DataFusionError::Plan("Critical validation failures detected".into()));
    }
} else {
    println!("[OK] All metrics within expected ranges");
}
```

**Pro tip:** Instead of hardcoding thresholds, compute rolling baselines from recent history:

```rust
// Calculate dynamic thresholds from the last 7 days
let historical = ctx.read_parquet("transactions_last_7d.parquet", ParquetReadOptions::default()).await?;
let baseline = historical
    .aggregate(vec![], vec![
        avg(col("daily_avg")).alias("expected_avg"),
        stddev(col("daily_avg")).alias("expected_stddev"),
    ])?
    .collect().await?;

// Today's average should be within 2 standard deviations of the weekly pattern
```

This approach adapts to natural patterns (weekend dips, month-end spikes) while still catching true anomalies.

### When to Use External Validation Libraries

DataFusion excels at data-level validation—checking ranges, detecting duplicates, computing statistics. But sometimes you need specialized validation that goes beyond what SQL expressions can handle. External validators complement DataFusion by handling complex formats, business rules, and compliance requirements that would be painful to implement in query logic.

**Use external validators when you encounter:**

1. **Complex format validation** that requires parsing algorithms:

   - Email addresses, phone numbers, URLs
   - Financial identifiers (IBAN, BIC, credit card numbers)
   - Geographic data (postal codes, coordinates)
   - Industry standards (ISBN, VIN, MAC addresses)

2. **Business rule engines** that maintain state across records:

   - Order workflow state machines
   - Approval chain validation
   - Complex discount eligibility rules
   - Cross-entity referential integrity

3. **Compliance and schema standards:**
   - JSON Schema, Protobuf, or Avro schema validation
   - Industry standards (HL7, EDI, OpenAPI)
   - Regulatory formats (tax forms, medical records)

**Integration pattern:** Validate during ingestion, process with DataFusion:

```rust
use validator::Validate;
use datafusion::prelude::*;

// 1. Define validation rules using external validator
#[derive(Validate)]
struct CustomerRecord {
    #[validate(email)]
    email: String,
    #[validate(phone)]
    phone: String,
    #[validate(credit_card)]
    payment_method: String,
    #[validate(range(min = 18, max = 150))]
    age: u8,
}

// 2. Pre-validate during data ingestion
async fn ingest_with_validation(raw_data: Vec<RawRecord>) -> Result<DataFrame> {
    let mut valid_records = Vec::new();
    let mut validation_errors = Vec::new();

    for record in raw_data {
        let customer = CustomerRecord::from(record);
        match customer.validate() {
            Ok(_) => valid_records.push(customer),
            Err(e) => validation_errors.push((record.id, e.to_string())),
        }
    }

    // Log validation errors for monitoring/remediation
    if !validation_errors.is_empty() {
        println!("Rejected {} records with validation errors", validation_errors.len());
        // Write to dead letter queue, alert ops team, etc.
    }

    // 3. Convert valid records to DataFrame for bulk processing
    let ctx = SessionContext::new();
    ctx.read_json(
        valid_records.to_json_bytes(),
        NdJsonReadOptions::default()
    ).await
}

// 4. Now use DataFusion for large-scale analytics on clean data
let clean_df = ingest_with_validation(raw_records).await?;
let insights = clean_df
    .filter(col("age").gt(lit(25)))?
    .aggregate(
        vec![col("payment_method")],
        vec![avg(col("purchase_amount"))]
    )?;
```

**Key insight:** External validators act as quality gates at ingestion boundaries. They reject malformed records before they enter your analytical pipeline, preventing garbage data from polluting aggregations and causing mysterious failures hours later in production reports.

### Building Your Validation Strategy

Data validation isn't about catching every possible error—it's about catching the errors that matter before they cause damage. Build your validation in layers, starting with cheap checks and progressively adding sophistication where the risk justifies the complexity:

**Layer 1: Schema validation** (milliseconds, catches 30% of issues)

- Verify column presence, types, and nullability
- Runs on metadata alone—no data scanning required
- Catches structural breaks immediately after schema changes

**Layer 2: Value validation** (seconds, catches 50% of issues)

- Apply [`.filter()`] expressions for business rules
- Check formats, ranges, and relationships
- Identifies bad records while processing the good ones

**Layer 3: Statistical validation** (seconds to minutes, catches 15% of issues)

- Compute aggregates and compare to baselines
- Detect systemic problems invisible at the row level
- Critical for catching gradual degradation

**Layer 4: External validators** (variable, catches the remaining 5%)

- Complex domain formats and compliance rules
- Only where DataFrame operations fall short
- Apply at ingestion boundaries, not in batch processing

**The 80/20 rule of validation:** Most production issues come from simple problems—missing columns, null values where they shouldn't be, numbers outside reasonable ranges. Start there. Add complexity only after you've proven you need it through actual failures in production.

See also:

- [Schema Evolution Patterns](#schema-evolution-patterns)
- [Error Recovery](#error-recovery)

---

## Error Recovery

Production pipelines eventually encounter schema surprises. While the SQL and DataFrame APIs build the same logical plan inside [`SessionContext`], the DataFrame API makes it easier to express reusable recovery logic, add observability, and integrate with Rust's control flow. Because plans are lazily executed, schema errors typically surface only when you call [`.collect()`], [`.show()`], or `.write_*()`. Early detection and recovery logic keep long-running jobs from failing late in the process. In Rust, these problems usually surface as [`DataFusionError::SchemaError`] or [`DataFusionError::Plan`], both variants of the central [`DataFusionError`] type.

**Typical failure scenarios:**

- **Missing or extra columns**: A publisher changes their format or optional fields appear.
- **Type mismatches**: An `Int32` column becomes `Int64` or `Decimal`, or a nullable field becomes required.
- **Inconsistent partitions**: Schema drift occurs across different files or days within the same dataset.
- **Unreadable data**: Corrupt files or truncated row groups prevent deserialization.

### Choosing a Recovery Strategy

Select a strategy based on the business impact of incomplete data. Start strict to ensure correctness, then add tolerant pathways where partial data is acceptable.

| Strategy                | Best for                                                | Pros                                                       | Watch outs                                                        |
| :---------------------- | :------------------------------------------------------ | :--------------------------------------------------------- | :---------------------------------------------------------------- |
| **Fail fast & strict**  | Curated, high-trust datasets or OLTP-style workloads    | Catches issues before expensive work; easy to reason about | Stops the whole job; requires intervention to fix data            |
| **Tolerant & per-file** | Data lakes, multi-tenant ingestion, external data feeds | Keeps healthy data flowing; allows incremental cleanup     | Requires robust logging to ensure skipped/bad files are addressed |

### Robust Read with Schema Normalization

When files drift slightly from the contract—for example, a column type changes or a new field appears—a strict read will fail. This pattern shows how to build resilience into your reads by attempting the happy path first, then falling back to normalization when needed.

**The problem:** You have a known schema contract, but upstream files occasionally drift (new columns appear, types change from Int32 to Int64, etc.). You want your pipeline to keep working rather than failing immediately.

**The solution:** Create a wrapper function that tries to read with your expected schema first. If that fails, read with inference and normalize the result to match your contract.

Below is a complete example showing two helper functions you can copy into your own crate:

1. `robust_read_with_schema_recovery` - the main wrapper that handles the try/fallback logic
2. `normalize_to_schema` - reshapes any DataFrame to match a target schema

```rust
use datafusion::prelude::*;
use datafusion::error::{DataFusionError, Result};

// Example helper you can add to your own crate; not part of DataFusion's public API.
// This function wraps the standard read_parquet() with schema recovery logic.
async fn robust_read_with_schema_recovery(
    ctx: &SessionContext,
    path: &str,
    expected_schema: Arc<Schema>
) -> Result<DataFrame> {
    // Attempt 1: read with the contract schema — ideal for strict pipelines
    match ctx.read_parquet(path, ParquetReadOptions::default().schema(&expected_schema)).await {
        Ok(df) => return Ok(df),
        Err(e) => {
            eprintln!("[WARN] Failed to read with expected schema: {e}");
            // Attempt 2: let DataFusion infer, then normalize column-by-column
            let df = ctx.read_parquet(path, ParquetReadOptions::default()).await?;
            normalize_to_schema(df, &expected_schema).await
        }
    }
}

// This helper takes any DataFrame and reshapes it to match the target schema.
// It's called by robust_read_with_schema_recovery when the initial read fails.
async fn normalize_to_schema(
    df: DataFrame,
    target: &Schema
) -> Result<DataFrame> {
    // Actual schema inferred or read from the file(s)
    let actual_schema = df.schema();
    // Build a projection that reshapes `df` into the target schema column‑by‑column
    let mut select_exprs = Vec::new();

    // For each field in the target schema, either:
    //   * reuse the existing column as‑is,
    //   * cast it to the target type, or
    //   * synthesize a NULL column (if the field is nullable) / fail (if required).
    for target_field in target.fields() {
        match actual_schema.field_with_name(target_field.name()) {
            Ok(actual_field) => {
                // Column exists in the incoming data: keep or cast as needed
                if actual_field.data_type() == target_field.data_type() {
                    select_exprs.push(col(target_field.name()));
                } else {
                    select_exprs.push(
                        col(target_field.name())
                            .cast_to(target_field.data_type(), actual_schema)?
                            .alias(target_field.name())
                    );
                }
            },
            Err(_) => {
                if target_field.is_nullable() {
                    // Column is missing but nullable in the contract: fill with NULLs
                    select_exprs.push(lit(ScalarValue::Null).alias(target_field.name()));
                } else {
                    // Missing required column: treat as a hard schema violation
                    return Err(DataFusionError::Plan(format!(
                        "Cannot normalize: missing required field '{}'",
                        target_field.name()
                    )));
                }
            }
        }
    }
    // Produce a DataFrame whose schema matches `target` exactly
    df.select(select_exprs)
}
```

**How to use these helpers in your code:**

```rust
// Instead of this (which fails on schema drift):
let df = ctx.read_parquet("data/orders.parquet", ParquetReadOptions::default()).await?;

// Use this (which recovers from minor schema changes):
let expected_schema = Arc::new(Schema::new(vec![
    Field::new("order_id", DataType::Int64, false),
    Field::new("amount", DataType::Decimal128(19, 2), false),
]));
let df = robust_read_with_schema_recovery(&ctx, "data/orders.parquet", expected_schema).await?;
```

**When to use:** Use this pattern when you own the "golden" schema and want to be resilient to minor upstream drift without modifying the core pipeline logic. The helpers are built from DataFusion's public APIs: [`SessionContext`], [`.read_parquet()`], [`.schema()`], [`.select()`], [`.cast_to()`], and [`DataFusionError::Plan`].

### Fail-Fast Schema Diagnostics

For strict pipelines, you often want to abort before starting a long compute phase if the data violates the contract. Instead of waiting for a runtime error deep in an aggregation, this function compares the actual Arrow schema against expectations and returns a descriptive [`DataFusionError::Plan`] listing all differences.

```rust
use datafusion::prelude::*;
use datafusion::arrow::datatypes::Schema;
use datafusion::error::Result;
use std::sync::Arc;

/// Validate schemas before building expensive logical plans
fn compare_schemas(actual: &Schema, expected: &Schema) -> Result<()> {
    // Collect human-readable differences between the two schemas
    let mut errors = Vec::new();

    // First pass: ensure every expected field exists with compatible type and nullability
    for expected_field in expected.fields() {
        match actual.field_with_name(expected_field.name()) {
            Ok(actual_field) => {
                // Same name, but type changed
                if actual_field.data_type() != expected_field.data_type() {
                    errors.push(format!(
                        "  • '{}': type mismatch - expected {:?}, got {:?}",
                        expected_field.name(),
                        expected_field.data_type(),
                        actual_field.data_type()
                    ));
                }
                // Same name and type, but nullability changed
                if actual_field.is_nullable() != expected_field.is_nullable() {
                    errors.push(format!(
                        "  • '{}': nullability mismatch - expected nullable={}, got nullable={}",
                        expected_field.name(),
                        expected_field.is_nullable(),
                        actual_field.is_nullable()
                    ));
                }
            },
            // Field missing entirely from the actual schema
            Err(_) => errors.push(format!(
                "  • '{}': MISSING in actual schema",
                expected_field.name()
            )),
        }
    }

    // Second pass: detect any extra fields that are not part of the expected contract
    for actual_field in actual.fields() {
        if expected.field_with_name(actual_field.name()).is_err() {
            errors.push(format!(
                "  • '{}': UNEXPECTED field in actual schema",
                actual_field.name()
            ));
        }
    }

    // Either succeed silently or return a single Plan error summarizing all differences
    if errors.is_empty() {
        Ok(())
    } else {
        Err(datafusion::error::DataFusionError::Plan(format!(
            "Schema validation failed with {} error(s):\n{}",
            errors.len(),
            errors.join("\n")
        )))
    }
}
```

Call the here defined example function `compare_schemas` on the first `RecordBatch` or during table registration to fail immediately with a readable diff.

### Partial Failure Tolerance for Multi-File Reads

In large directories, a single corrupt file or bad partition shouldn't necessarily halt the entire job. This pattern wraps the reader to isolate failures, collecting successful DataFrames while logging errors for later remediation. The helper function `read_files_with_schema_tolerance` shown below builds on `robust_read_with_schema_recovery` to implement this pattern; like the previous helpers, it is intended as a copy‑and‑adapt example rather than a function provided by the DataFusion crate.

```rust
use datafusion::prelude::*;
use datafusion::error::Result;

// Example helper that builds on `robust_read_with_schema_recovery` for multi-file directories.
async fn read_files_with_schema_tolerance(
    ctx: &SessionContext,
    paths: Vec<&str>,
    expected_schema: Arc<Schema>
) -> Result<Vec<DataFrame>> {
    // DataFrames that we were able to read and normalize successfully
    let mut successful = Vec::new();
    // (path, error) pairs for files that failed schema validation or recovery
    let mut failed = Vec::new();

    // Attempt to read each file independently using the robust schema recovery helper
    for path in paths {
        match robust_read_with_schema_recovery(ctx, path, expected_schema.clone()).await {
            Ok(df) => {
                println!("[OK] Successfully processed: {path}");
                successful.push(df);
            },
            Err(e) => {
                eprintln!("[ERROR] Failed to process {path}: {e}");
                failed.push((path, e.to_string()));
            }
        }
    }

    // If nothing succeeded, treat this as a hard failure for the whole job
    if successful.is_empty() {
        return Err(datafusion::error::DataFusionError::Plan(
            format!("All {} file(s) failed to process", failed.len())
        ));
    }

    // If some files failed, log a summary but still return the good DataFrames
    if !failed.is_empty() {
        eprintln!(
            "[WARN] Proceeding with {} of {} files ({} failures logged)",
            successful.len(),
            paths.len(),
            failed.len()
        );
    }

    Ok(successful)
}
```

You can then combine the successful DataFrames using [`.union_by_name()`] before continuing with your analysis.

### Safe Type Coercion Guardrails

Blindly casting columns (e.g., `Int32` to `Int64`) can fail at runtime if the types are incompatible. This helper checks whether Arrow permits the cast before attempting it. If the cast is unsafe, you can choose to handle it gracefully—for example, by filling with `NULL` or dropping the column—rather than crashing the query.

```rust
use datafusion::prelude::*;
use datafusion::arrow::compute::can_cast_types;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::ScalarValue;
use datafusion::error::{DataFusionError, Result};

fn safe_cast_column(
    df: DataFrame,
    column: &str,
    target_type: DataType
) -> Result<DataFrame> {
    // Look up the column's current type in the DataFrame schema
    let schema = df.schema();
    let field = schema.field_with_name(column)?;

    // Fast path: already the right type, nothing to do
    if field.data_type() == &target_type {
        return Ok(df);
    }

    // Preferred path: Arrow says this cast is safe (typically widening casts)
    if can_cast_types(field.data_type(), &target_type) {
        df.with_column(
            column,
            col(column)
                .cast_to(&target_type, schema)?
                .alias(column)
        )
    // Fallback for nullable columns: log and fill with NULLs instead of failing hard
    } else if field.is_nullable() {
        eprintln!(
            "[WARN] Cannot cast {} from {:?} to {:?}, filling NULLs instead",
            column,
            field.data_type(),
            target_type
        );
        df.with_column(column, lit(ScalarValue::Null).alias(column))
    // Non-nullable + unsafe cast: treat as a hard schema violation
    } else {
        Err(DataFusionError::Plan(format!(
            "Cannot coerce non-nullable column '{}' from {:?} to {:?}",
            column,
            field.data_type(),
            target_type
        )))
    }
}
```

### Observability and Logging

Even tolerant pipelines only work if failures are visible. Structured logging gives you a breadcrumb trail without complicating your core logic: record which file, which strategy you used (strict vs tolerant), and why recovery failed so you can revisit bad inputs later.

```rust
use datafusion::prelude::*;
use datafusion::error::Result;
use tracing::warn;

async fn log_schema_result(
    path: &str,
    strategy: &str,
    result: Result<DataFrame>
) -> Result<DataFrame> {
    // Attach file path and strategy to every schema recovery attempt
    match result {
        // Happy path: just return the DataFrame unchanged
        Ok(df) => Ok(df),
        // Failure path: emit a structured warning, then propagate the error
        Err(e) => {
            warn!(file = path, strategy, error = %e, "schema recovery failed");
            Err(e)
        }
    }
}
```

You can wrap calls to a helper like `robust_read_with_schema_recovery` with `log_schema_result` so every schema issue is tagged with the file path and strategy in your logs. For comprehensive monitoring patterns—including metrics, sampling, and alerting—consult [Production Patterns](./dataframes-advance.md#7-production-patterns), which expands on these examples.

**See also:**

- [Error Handling & Recovery](./creating-dataframes.md#error-handling--recovery) — I/O and inference failure patterns.
- [Transformations](./transformations.md#type-casting) — Details on casting and coercion.
- [Advanced DataFrame Topics](./dataframes-advance.md) — Deep dives into productionizing DataFusion.

---

## Performance Considerations

**Schema decisions cascade through your entire pipeline—get them right early to avoid expensive corrections later.**

In DataFusion's columnar execution model, schema operations range from free metadata manipulations to expensive full-table scans. This section reveals the performance implications of schema choices, helping you build pipelines that are both correct and fast. We'll explore why some operations that look similar have vastly different costs, and how to structure your schema management for optimal performance.

### Understanding DataFusion's Execution Model

DataFusion separates **logical planning** (what to do) from **physical execution** (how to do it). Schema operations primarily happen during logical planning, which means:

- **Planning is cheap**: Building transformation plans (metadata operations) costs microseconds
- **Execution is expensive**: Actually transforming data (type casts, scans) costs proportional to data size
- **Lazy evaluation wins**: Operations accumulate until [`.collect()`], [`.show()`], or `.write_*()` triggers execution

This distinction explains why chaining multiple `.select()` calls is essentially free, but executing them processes every row. For more see {[DataFrame Concepts](./concepts.md#data-model--schema)}

### The Performance Hierarchy

#### Zero-Cost Operations (Metadata Only)

These operations modify the logical plan without touching data. They complete instantly regardless of dataset size:

```rust
// Column renaming: Updates metadata mapping, no data movement
let renamed = df.select(vec![
    col("customer_id").alias("client_id"),  // Zero cost: just rewrites the plan
    col("amount"),
])?;

// Union planning: Builds a plan to combine DataFrames later
let union_plan = df1.union_by_name(df2)?;  // Free now, executed when you call collect()

// Schema extension: Adds a logical NULL column to the plan
let extended = df.with_column(
    "processing_date",
    lit(ScalarValue::Null)  // No array allocation until execution
)?;

// Column reordering: Just rearranges the projection list
let reordered = df.select(vec![col("email"), col("name"), col("id")])?;
```

**Why they're free**: These operations only modify the query plan's metadata. The actual column data remains untouched until execution. You can chain hundreds of these operations with negligible overhead.

#### Low-Cost Operations (Optimized Data Access)

These operations read data but leverage DataFusion's columnar optimizations to minimize work:

```rust
// Predicate pushdown: For Parquet/ORC, uses column statistics to skip row groups
let filtered = df.filter(col("year").eq(lit(2024)))?;
// Can eliminate 95%+ of data before it's even loaded into memory

// Projection pushdown: Only deserializes requested columns from storage
let projected = df.select(vec![col("id"), col("name")])?;
// If your Parquet file has 50 columns but you need 2, reads only 4% of the data

// Partition pruning: Skips entire files/directories based on partition columns
let pruned = partitioned_df.filter(col("date").gt(lit("2024-01-01")))?;
// With daily partitions, reads only recent directories, not historical data

// Early filtering in joins: Reduces data before expensive join operations
let optimized_join = large_df
    .filter(col("active").eq(lit(true)))?  // Filter first
    .join(lookup_df, JoinType::Inner, &["id"], &["id"], None)?;
```

**Why they're fast**:

- **Columnar storage**: Operations work on compressed column chunks, not row-by-row
- **Predicate pushdown**: Filters apply at the storage layer, preventing unnecessary I/O
- **Statistics pruning**: Min/max statistics in Parquet eliminate entire row groups without reading them
- **Lazy materialization**: Columns are only decompressed when actually needed

#### High-Cost Operations (Full Data Transformations)

These operations require reading, transforming, and often materializing entire datasets:

```rust
// Type casting: Allocates new arrays and converts every value
let casted = df.with_column(
    "id",
    col("id").cast_to(&DataType::Int64, df.schema())?
)?;
// Cost: O(n) memory allocation + conversion compute for every row

// String transformations: Process every character of every value
let normalized = df.with_column(
    "email",
    lower(trim(col("email")))  // Two passes over all string data
)?;

// Schema-changing aggregations: Must scan entire dataset
let grouped = df.aggregate(
    vec![col("category")],
    vec![count(col("sales")), sum(col("amount"))]
)?;
// Must read all rows to compute aggregates, even with partition pruning

// Deduplication: Requires sorting or hashing all rows
let distinct = df.select(vec![col("customer_id")])?.distinct()?;
// Memory cost: hash table with every unique value

// Wide type conversions: Exponentially expensive for precision changes
let wide_decimal = df.with_column(
    "amount",
    col("amount").cast_to(&DataType::Decimal256(76, 38), df.schema())?
)?;
// Allocates 4x the memory of Decimal128
```

**Why they're expensive**:

- **Memory allocation**: Creating new arrays for transformed data doubles memory usage
- **CPU intensity**: Type conversions, string operations, and decimal arithmetic are compute-heavy
- **No pushdown**: These operations can't be delegated to the storage layer
- **Materialization barriers**: Operations like `distinct()` must see all data before producing any output
- **Cache effects**: Processing entire columns invalidates CPU caches repeatedly

### Schema-Specific Performance Patterns

Understanding how schema operations interact reveals optimization opportunities:

#### Pattern: Type Harmonization During Joins

```rust
// ❌ Expensive: Cast happens during join execution
let result = int32_df.join(int64_df, JoinType::Inner, &["id"], &["id"], None)?;
// Runtime error: Schema mismatch!

// ✅ Better: Align types before the join
let aligned = int32_df.with_column(
    "id",
    col("id").cast_to(&DataType::Int64, int32_df.schema())?
)?;
let result = aligned.join(int64_df, JoinType::Inner, &["id"], &["id"], None)?;
// Cast happens once, join proceeds efficiently
```

#### Pattern: Nullability and Memory Overhead

```rust
// Nullable columns require additional bitmap allocation
let nullable_schema = Schema::new(vec![
    Field::new("id", DataType::Int64, true),      // +12.5% memory for null bitmap
    Field::new("amount", DataType::Float64, true), // Another bitmap
]);

// Non-nullable columns are more memory-efficient
let strict_schema = Schema::new(vec![
    Field::new("id", DataType::Int64, false),      // No null bitmap needed
    Field::new("amount", DataType::Float64, false), // Pure columnar data
]);
```

**Impact**: For a billion-row dataset, nullable columns add ~125MB per column just for null tracking.

#### Pattern: Schema Inference vs. Explicit Schemas

```rust
// ❌ Expensive: Infers schema by scanning data, then reads again
let inferred = ctx.read_csv("large_file.csv", CsvReadOptions::new()).await?;
// Cost: 2 full scans (inference + actual read)

// ✅ Optimal: Single scan with predetermined schema
let explicit = ctx.read_csv(
    "large_file.csv",
    CsvReadOptions::new().schema(&schema)
).await?;
// Cost: 1 scan with direct type parsing
```

**Measurement**: For a 10GB CSV, explicit schemas save 8-10 seconds of inference time.

### Performance Best Practices

These patterns optimize schema operations for production workloads:

#### 1. Front-Load Schema Decisions

**Principle**: Define schemas at data ingestion, not during processing.

```rust
// ✅ OPTIMAL: Schema defined at read time
let schema = Arc::new(Schema::new(vec![
    Field::new("id", DataType::Int64, false),
    Field::new("amount", DataType::Decimal128(19, 2), false),
    Field::new("created_at", DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())), false),
]));

let df = ctx.read_csv("sales.csv", CsvReadOptions::new().schema(&schema)).await?;
// Single pass: Parse directly into target types

// ❌ WASTEFUL: Infer, then fix schema issues
let df = ctx.read_csv("sales.csv", CsvReadOptions::new()).await?;
let df = df.with_column("id", col("id").cast_to(&DataType::Int64, df.schema())?)?;
let df = df.with_column("amount", col("amount").cast_to(&DataType::Decimal128(19, 2), df.schema())?)?;
// Three passes: Inference scan + read + two cast operations
```

**Performance impact**: For a 1GB CSV with 10 columns, explicit schemas save:

- Inference scan: ~2 seconds
- Per-column cast: ~0.5 seconds each
- Total savings: 7+ seconds per file

#### 2. Order Operations by Selectivity

**Principle**: Apply selective operations first to reduce data volume for expensive operations.

```rust
// ✅ OPTIMAL: Filter → Project → Cast
let optimized = df
    .filter(col("status").eq(lit("active")))?     // Reduces to 10% of data
    .select(vec![col("id"), col("amount")])?      // Drops unneeded columns
    .with_column("id", col("id").cast_to(&DataType::Int64, df.schema())?)?;
// Cast operates on 10% of original data

// ❌ WASTEFUL: Cast → Project → Filter
let wasteful = df
    .with_column("id", col("id").cast_to(&DataType::Int64, df.schema())?)?  // Processes 100%
    .select(vec![col("id"), col("amount")])?
    .filter(col("amount").gt(lit(100)))?;         // Discards 90% of processed data
```

**Rule of thumb**: Order operations by their selectivity ratio:

1. Filters (can eliminate 90%+ of data)
2. Projections (reduce width)
3. Type casts (transform remaining data)
4. Joins/Aggregations (most expensive)

#### 3. Consolidate Schema Transformations

**Principle**: Batch related schema changes into a single operation to minimize data passes.

```rust
// ✅ OPTIMAL: Single-pass transformation
let normalized = df.select(vec![
    col("user_id").cast_to(&DataType::Int64, df.schema())?.alias("id"),
    col("purchase_amount").cast_to(&DataType::Decimal128(19, 2), df.schema())?.alias("amount"),
    lower(trim(col("email"))).alias("email"),  // Multiple string ops in one pass
    when(col("region").is_null(), lit("UNKNOWN"))
        .otherwise(upper(col("region")))?
        .alias("region"),
])?;
// Result: One execution pass handles all transformations

// ❌ WASTEFUL: Sequential transformations
let df = df.with_column("id", col("user_id").cast_to(&DataType::Int64, df.schema())?)?;
let df = df.with_column("amount", col("purchase_amount").cast_to(&DataType::Decimal128(19, 2), df.schema())?)?;
let df = df.with_column("email", lower(trim(col("email"))))?;
let df = df.with_column("region", upper(col("region")))?;
// Result: Four separate execution passes over the data
```

**Performance gain**: Consolidation reduces memory bandwidth usage by 75% and improves cache locality.

#### 4. Design for Partition Pruning

**Principle**: Structure data layout to enable physical file skipping.

```rust
// Schema design that enables efficient pruning
let listing_options = ListingOptions::new(Arc::new(ParquetFormat::default()))
    .with_table_partition_cols(vec![
        ("year".into(), DataType::Int16),     // High cardinality partition
        ("month".into(), DataType::Int8),      // Medium cardinality
        ("region".into(), DataType::Utf8),     // Low cardinality
    ]);

// ✅ OPTIMAL: Filter on partition columns first
let efficient = partitioned_table
    .filter(col("year").eq(lit(2024)))?           // Prunes 90% of directories
    .filter(col("month").in_list(vec![lit(1), lit(2), lit(3)], false))?  // Prunes 75% more
    .filter(col("amount").gt(lit(100)))?;         // Operates on 2.5% of data

// ❌ WASTEFUL: Filter on non-partition columns
let inefficient = partitioned_table
    .filter(col("amount").gt(lit(100)))?          // Must open every file
    .filter(col("year").eq(lit(2024)))?;          // Pruning happens too late
```

**Impact**: With 5 years of daily data (1,825 files), partition pruning reduces I/O from 1,825 file opens to just 365.

### Measuring and Profiling Schema Performance

#### Using EXPLAIN Plans

```rust
// Analyze the query plan to identify bottlenecks
let plan = df.explain(true, true)?;  // verbose=true, analyze=true
plan.show().await?;

// Key indicators to examine:
// 1. "ParquetExec: pruned_files=2/100" → Good partition pruning
// 2. "FilterExec" before "ProjectionExec" → Good operation order
// 3. "CAST(id AS Int64)" appearing multiple times → Consolidation needed
// 4. "SortExec" or "CoalescePartitionsExec" → Memory-intensive operations
```

#### Schema Operation Profiling

```rust
use std::time::Instant;

// Profile schema operations in isolation
async fn profile_schema_operation<F>(name: &str, mut op: F) -> Result<DataFrame>
where
    F: FnMut() -> Result<DataFrame>,
{
    let start = Instant::now();
    let df = op()?;

    // Force planning (but not execution)
    let _ = df.logical_plan();
    let planning_time = start.elapsed();

    // Execute and measure
    let exec_start = Instant::now();
    let batches = df.collect().await?;
    let exec_time = exec_start.elapsed();

    println!("{name}:");
    println!("  Planning: {:?}", planning_time);
    println!("  Execution: {:?}", exec_time);
    println!("  Rows: {}", batches.iter().map(|b| b.num_rows()).sum::<usize>());

    ctx.read_batches(batches)
}

// Usage
let df = profile_schema_operation("Type casting", || {
    source_df.with_column("id", col("id").cast_to(&DataType::Int64, source_df.schema())?)
}).await?;
```

### Common Schema Performance Pitfalls

1. **Cascading type mismatches**: One wrong type forces casts throughout the pipeline
2. **Nullable explosion**: Unnecessary nullability adds 12.5% memory overhead per column
3. **Schema inference in loops**: Re-inferring schemas for each file in a directory
4. **Ignored statistics**: Not leveraging Parquet min/max statistics for pruning
5. **Premature materialization**: Calling [`.collect()`] before applying filters

**Remember**: The best schema optimization is avoiding the need for it. Invest time in schema design upfront to minimize runtime corrections.

### Quick Reference: Schema Performance Cheat Sheet

| Operation                              |      Cost       |      When to Use       | Alternative                         |
| :------------------------------------- | :-------------: | :--------------------: | :---------------------------------- |
| **[`.select()`] with aliases**         |      Free       |    Renaming columns    | N/A - always safe                   |
| **[`.union_by_name()`]**               | Free (planning) |  Combining DataFrames  | [`.union()`] if exact schema match  |
| **[`.filter()`] on partition columns** |    Very Low     |  Early data reduction  | N/A - always do this first          |
| **[`.filter()`] on regular columns**   |       Low       |  Selective filtering   | Push to storage if possible         |
| **[`.cast_to()`] on single column**    |     Medium      |     Type alignment     | Define correct type at read time    |
| **Multiple [`.with_column()`] calls**  |      High       |         Never          | Batch into single [`.select()`]     |
| **[`.distinct()`] on full DataFrame**  |    Very High    | Deduplication required | [`.distinct()`] on key columns only |
| **Schema inference**                   |      High       |  Never in production   | Always provide explicit schema      |

### Key Takeaways

1. **Schema operations are not created equal**: Metadata changes are free, data transformations are expensive
2. **Front-load schema decisions**: Fix schemas at read time, not during processing
3. **Operation order matters**: Filter → Project → Transform → Join/Aggregate
4. **Batch transformations**: One pass with multiple operations beats multiple passes
5. **Measure, don't guess**: Use [`.explain()`] to verify optimization assumptions (For more details, see [Reading Explain Plans](../../user-guide/explain-usage.md)
   )

By understanding these performance characteristics, you can build schema management strategies that scale from gigabytes to terabytes while maintaining sub-second query response times.

---

## Debugging and Resolving Schema Mismatches

**Diagnose schema conflicts by classifying them into four types (Count, Order, Name, Type), resolve them using resilient union strategies or explicit casting, and implement robust error handling.**

In a perfect world, schemas never change. In the real world, **schemas drift constantly.** As an example the following shoul be mentioned:

- **Source Drift:** A database migration widens `user_id` from `Int32` to `Int64`.
- **Evolution:** A nightly export gains a new `customer_segment` column.
- **Inconsistency:** One CSV uses `Region` (capitalized) while another uses `region`.

When these shifts happen, DataFusion surfaces schema errors to protect data integrity. This section is your troubleshooting playbook.

### The Four Types of Mismatch

No matter the real-world cause—whether it's a migration, a typo, or a new feature—the conflict always manifests as one of **four technical problems**. Diagnosing which one you have is the first step to fixing it:

1.  **Count Mismatch:** DataFrames have a different number of columns (e.g., a new feature added a field).
2.  **Order Mismatch:** Columns are in a different sequence.
3.  **Name Mismatch:** A column has a different name or capitalization (`Region` vs `region`).
4.  **Type Mismatch:** A column has a different data type (`Int32` vs `Int64`).

The rest of this guide maps these four problems to specific solutions using [`.union_by_name()`] (for shape/name issues) and explicit casting (for type issues).

### Phase 1: Diagnosis

When an error fires like:

```
Schema error: Union schemas have different number of fields: 3 vs 4`
```

The [`.schema()`] method is your primary diagnostic tool. Compare both schemas side-by-side to spot the difference:

```rust
use datafusion::prelude::*;

// Assume df1 and df2 are failing to union.
// Print and compare their schemas side-by-side.
println!("=== Schema 1 ===");
for field in df1.schema().fields() {
    println!("{:20} {:?} nullable={}", field.name(), field.data_type(), field.is_nullable());
}

println!("\n=== Schema 2 ===");
for field in df2.schema().fields() {
    println!("{:20} {:?} nullable={}", field.name(), field.data_type(), field.is_nullable());
}
```

**Example Output:**

```text
=== Schema 1 ===
customer_id          Int32 nullable=false
amount               Float64 nullable=false
region               Utf8  nullable=true

=== Schema 2 ===
customer_id          Int64 nullable=false    ← Type Mismatch (Int32 vs Int64)
amount               Float64 nullable=false
Region               Utf8  nullable=true     ← Name Mismatch (region vs Region)
category             Utf8  nullable=true     ← Count Mismatch (Extra column)
```

### Phase 2: Resolution Strategies

Once diagnosed, choose the strategy that fits your mismatch type.

#### Strategy A: The "Resilient" Fix (Count & Order Mismatches)

**Best for:** Handling added/removed columns or shuffled column order. **This strategy solves ~90% of real-world schema mismatches.**

Use [`.union_by_name()`]. Unlike standard SQL unions which match by position, this method matches by column name and fills missing columns with `NULL`. For more information see the section [The Anatomy of a DataFusion Schema](#the-anatomy-of-a-datafusion-schema).

```rust
use datafusion::prelude::*;
use datafusion::assert_batches_eq;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();

    // Schema: [a, b]
    let df1 = ctx.read_csv("tests/data/example.csv", CsvReadOptions::default()).await?
        .select_columns(&["a", "b"])?;

    // Schema: [b, a, c] (Different order, extra column)
    let df2 = ctx.read_csv("tests/data/example.csv", CsvReadOptions::default()).await?
        .select_columns(&["b", "a", "c"])?;

    // Fails: df1.union(df2) -> Schema Error
    // Succeeds: Aligns by name, fills missing 'c' in df1 with NULL
    let unified = df1.union_by_name(df2)?;

    Ok(())
}
```

#### Strategy B: The "Alignment" Fix (Type Mismatches)

**Best for:** Unifying numeric types (e.g., Int32/Int64) or compatible formats.

Use [`.cast_to()`] inside a [`.with_column()`] transformation.

> **The Golden Rule of Casting:**
> Always cast the **narrower** type up to the **widest** common type (e.g., `Int32` → `Int64`). This is safe and prevents data loss. Never cast down unless you are certain values won't be truncated. For a detailed hierarchy of safe conversions, see [Type Coercion Hierarchy](#mode-2-strict-matching-for-joins-and-unions).

```rust
use datafusion::prelude::*;
use datafusion::arrow::datatypes::DataType;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();

    // df1: id is Int32
    let df1 = ctx.sql("SELECT 1::int as id").await?;

    // df2: id is Int64
    let df2 = ctx.sql("SELECT 2::bigint as id").await?;

    // Fix: Explicitly cast df1's 'id' to Int64 to match df2
    let df1_aligned = df1.with_column(
        "id",
        col("id").cast_to(&DataType::Int64, df1.schema())?
    )?;

    // Now they can be safely combined
    let result = df1_aligned.union(df2)?;
    Ok(())
}
```

#### Strategy C: The "Schema Adapter" (Name Mismatches & Complex Logic)

**Best for:** Renaming columns, fixing capitalization, or enforcing a strict target schema.

Use [`.select()`] with [`.alias()`] to manually map the problematic schema to your target shape. This acts as a "schema adapter": it picks exactly the columns you want and **normalizes** them (e.g. renaming `Region` to `region` to enforce snake_case).

> **Golden Rule of Normalization:**
> Enforce your schema boundaries explicitly. Don't just hope upstream data stays clean—use `.select()` + `.alias()` to build an explicit "Adapter Layer" that renames and filters columns before they enter your core logic.

```rust
// Problem: Incoming data has "Region" (capped), target expects "region" (lowercase)
let df_fixed = df_incoming.select(vec![
    col("Region").alias("region"), // 1. Rename "Region" -> "region"
    col("amount"),                 // 2. Keep "amount" as-is
    col("customer_id"),            // 3. Keep "customer_id"
    // Note: Any other columns in df_incoming are dropped here, enforcing the schema.
])?;
```

### Automatic Schema Merging

DataFusion automatically attempts to merge schemas when reading multiple files (e.g., `ctx.read_parquet(...)`). As detailed in [Strategy 2: Self-Describing Formats](#strategy-2-self-describing-formats-parquetavroarrow--merge--normalize), this process promotes types (widening) and handles missing columns (nullability).

**If automatic merging fails**, it is usually due to a strict incompatibility (e.g., `Int64` vs `String`). In these cases, you must fall back to **manual alignment**: read the files as separate DataFrames, apply **Strategy B (Casting)**, and then union them.

### Summary Checklist

| Problem   | Symptom                              | Solution                          |
| :-------- | :----------------------------------- | :-------------------------------- |
| **Shape** | "Different number of fields"         | Use [`.union_by_name()`]          |
| **Order** | Columns swapped                      | Use [`.union_by_name()`]          |
| **Type**  | "Incompatible types Int32 and Int64" | Use [`.cast_to()`] (cast up)      |
| **Name**  | "Field not found"                    | Use [`.alias()`] in [`.select()`] |

> **Performance Note:**
>
> [`.select()`] and [`.union_by_name()`] are logical transformations (metadata only) and are essentially free. [`.cast_to()`] requires rewriting data at execution time and has a computational cost. Always prefer fixing schemas at the source (write time) over casting at read time.

### Debugging Resources

- [Reading Explain Plans](../../user-guide/explain-usage.md) - How to debug query plans when schemas fail.
- Medium: [Schema Mismatch Error: Understanding and Resolving][schema mismatch medium]
- Stackademic: [Apache Spark Basics: Schema Enforcement vs Inference](https://blog.stackademic.com/apache-spark-basics-101-schema-enforcement-vs-schema-inference-78b6f35cec10) - Concepts of schema enforcement that apply universally to data engines.

---

## Schema Management References

This concludes the guide to Schema Management in DataFusion. Below are the technical specifications and architectural references required to debug type mismatches and memory layout issues when using the DataFrame API.

### 1. The Physical Memory Model (Essential)

_You cannot effectively use the DataFusion DataFrame API without understanding the underlying Arrow memory format. These specs explain "zero-copy" operations and why certain casts are expensive._

- **[Apache Arrow Columnar Format](https://arrow.apache.org/docs/format/Columnar.html)**
  - **Focus on:** _Physical Memory Layout_ and _Variable-size Binary View Layout_ (StringView).
  - **Why:** Explains why `StringView` is faster than `String` in DataFusion and how to optimize your schema for it.
- **[Arrow Schema IPC Message](https://arrow.apache.org/docs/format/Columnar.html#schema-message)**
  - **Why:** Debugging "Schema Mismatch" errors often requires understanding how Arrow serializes field metadata and nullability flags.

### 2. On-Disk to In-Memory Mapping

_Schema management often fails at the boundary between storage (Parquet/CSV) and memory (Arrow). Use these references to understand type coercion._

- **[Parquet Logical Types](https://github.com/apache/parquet-format/blob/master/LogicalTypes.md)**
  - **Why:** Explains how DataFusion maps complex Parquet types (like `INT96` timestamps or `DECIMAL`) into Arrow types.
- **[DataFusion Type Coercion Rules](https://docs.rs/datafusion/latest/datafusion/logical_expr/type_coercion/index.html)**
  - **Why:** This module source code documents the exact rules DataFusion uses to resolve type conflicts (e.g., joining `Int32` with `Int64`).

### 3. Execution & Optimization

_Understanding how your schema design impacts the Query Plan._

- **[DataFusion Optimizer Rules](https://docs.rs/datafusion/latest/datafusion/optimizer/index.html)**
  - **Focus on:** `type_coercion` and `simplify_expressions`.
  - **Why:** See how DataFusion automatically rewrites your DataFrame operations to handle schema discrepancies, sometimes adding hidden `CAST` operations that affect performance.

## Advanced Reading: Foundational Resources

The DataFusion DataFrame API builds upon established patterns in database theory and system design. For users wishing to deepen their understanding of data modeling, schema evolution, and query engine internals, we recommend the following industry-standard resources.

### Data Modeling & Schema Design

_Foundational texts on structuring data for consistency, scalability, and analytics._

- **The Data Model Resource Book (Vol 1-3)** by Len Silverston
  The standard library of universal data models for common business domains.
  [Vol 1](https://www.oreilly.com/library/view/the-data-model/9780471380238/), [Vol 2](https://www.oreilly.com/library/view/the-data-model/9780471353485/), [Vol 3](https://www.oreilly.com/library/view/the-data-model/9780470178454/)

- **Patterns of Data Modeling** by David Hay
  A comprehensive guide to the conceptual structures that underlie robust data design.
  [Patterns of Data Modeling](https://www.oreilly.com/library/view/patterns-of-data/9781439819906/)

- **The Data Warehouse Toolkit** by Ralph Kimball & Margy Ross
  The definitive guide to Dimensional Modeling (Star Schemas), the most common design pattern for analytical query engines like DataFusion.
  [The Data Warehouse Toolkit](https://www.oreilly.com/library/view/the-data-warehouse/9781118530801/)

### Database Internals & Systems

_Essential reading for understanding the mechanisms of storage, encoding, and execution._

- **Designing Data-Intensive Applications** by Martin Kleppmann
  A modern classic on distributed systems. Chapters on **Encoding and Evolution** are particularly relevant for understanding schema management in modern data pipelines.
  [Designing Data-Intensive Applications](https://www.oreilly.com/library/view/designing-data-intensive-applications/9781491903063/)

- **How Query Engines Work** by Andy Grove
  Written by the creator of DataFusion, this book bridges the gap between general database theory and the specific implementation of a query engine.
  [How Query Engines Work](https://leanpub.com/how-query-engines-work)

---

---

<!-- Invisible References (Sorted by Category) -->

<!-- External Standards -->

[Apache Arrow]: https://arrow.apache.org/
[arrow schema]: https://docs.rs/arrow/latest/arrow/datatypes/struct.Schema.html
[arrow schema docs]: https://arrow.apache.org/cookbook/py/schema.html
[arrow schema rust]: https://github.com/apache/arrow-rs/tree/main/arrow/examples
[arrow dtype]: https://arrow.apache.org/docs/python/api/datatypes.html
[`arrow` crate]: https://docs.rs/arrow/latest/arrow/
[`arrow::compute::can_cast_types()`]: https://docs.rs/arrow/latest/arrow/compute/fn.can_cast_types.html
[`can_cast_types()`]: https://docs.rs/arrow/latest/arrow/compute/fn.can_cast_types.html
[`Schema`]: https://docs.rs/datafusion/latest/datafusion/common/arrow/datatypes/struct.Schema.html
[`Field`]: https://docs.rs/datafusion/latest/datafusion/common/arrow/datatypes/struct.Field.html
[`DataType`]: https://docs.rs/datafusion/latest/datafusion/common/arrow/datatypes/enum.DataType.html

<!-- DataFusion Core Types -->

[`DFSchema`]: https://docs.rs/datafusion/latest/datafusion/common/struct.DFSchema.html
[DFSchema::inner]: https://docs.rs/datafusion/latest/datafusion/common/struct.DFSchema.html#method.inner
[`DFSchema::field_with_name()`]: https://docs.rs/datafusion/latest/datafusion/common/struct.DFSchema.html#method.field_with_name
[`DFSchema::logically_equivalent_names_and_types()`]: https://docs.rs/datafusion/latest/datafusion/common/struct.DFSchema.html#method.logically_equivalent_names_and_types
[`SessionContext`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html
[`TableProvider`]: https://docs.rs/datafusion/latest/datafusion/datasource/trait.TableProvider.html
[TableProvider::schema]: https://docs.rs/datafusion/latest/datafusion/datasource/trait.TableProvider.html#tymethod.schema
[`ListingTable`]: https://docs.rs/datafusion/latest/datafusion/datasource/listing/struct.ListingTable.html
[`dataframe!`]: https://docs.rs/datafusion/latest/datafusion/macro.dataframe.html
[`SchemaRef`]: https://docs.rs/datafusion/latest/datafusion/common/arrow/datatypes/type.SchemaRef.html
[`DataFusionError`]: https://docs.rs/datafusion/latest/datafusion/common/enum.DataFusionError.html
[`DataFusionError::Plan`]: https://docs.rs/datafusion/latest/datafusion/common/enum.DataFusionError.html#variant.Plan
[`DataFusionError::SchemaError`]: https://docs.rs/datafusion/latest/datafusion/common/enum.DataFusionError.html#variant.SchemaError
[`NULL`]: https://docs.rs/datafusion/latest/datafusion/logical_expr/sqlparser/dialect/keywords/constant.NULL.html

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
[Field]: https://docs.rs/datafusion/latest/datafusion/common/arrow/datatypes/struct.Field.html

<!-- IO & Configuration -->

[`CsvReadOptions`]: https://docs.rs/datafusion/latest/datafusion/datasource/file_format/options/struct.CsvReadOptions.html
[`NdJsonReadOptions`]: https://docs.rs/datafusion/latest/datafusion/datasource/file_format/options/struct.NdJsonReadOptions.html
[`ParquetReadOptions`]: https://docs.rs/datafusion/latest/datafusion/datasource/file_format/options/struct.ParquetReadOptions.html
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

<!-- Invisible References (Sorted by Category)
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
[`DataFusionError::Plan`]: https://docs.rs/datafusion/latest/datafusion/common/enum.DataFusionError.html#variant.Plan
[`DataFusionError::SchemaError`]: https://docs.rs/datafusion/latest/datafusion/common/enum.DataFusionError.html#variant.SchemaError
[`DataFusionError`]: https://docs.rs/datafusion/latest/datafusion/common/enum.DataFusionError.html
[`CsvReadOptions`]: https://docs.rs/datafusion/latest/datafusion/datasource/file_format/options/struct.CsvReadOptions.html
[`NdJsonReadOptions`]: https://docs.rs/datafusion/latest/datafusion/datasource/file_format/options/struct.NdJsonReadOptions.html
[`ParquetReadOptions`]: https://docs.rs/datafusion/latest/datafusion/datasource/file_format/options/struct.ParquetReadOptions.html
[`arrow_csv::ReaderBuilder::with_truncated_rows`]: https://docs.rs/arrow-csv/latest/arrow_csv/reader/struct.ReaderBuilder.html#method.with_truncated_rows
[`arrow` crate]: https://docs.rs/arrow/latest/arrow/
[Apache Arrow]: https://arrow.apache.org/
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
[`SchemaRef`]: https://docs.rs/datafusion/latest/datafusion/common/arrow/datatypes/type.SchemaRef.html
[`DFSchema::inner()`]: https://docs.rs/datafusion/latest/datafusion/common/struct.DFSchema.html#method.inner
[`.filter()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.filter
[`.join()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.join
[`.union()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.union
[`.union_by_name()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.union_by_name
[`.union_by_name_distinct()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.union_by_name_distinct
[DataFrame.schema]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.schema
[`.schema()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.schema
[`.to_string_pretty()`]: https://docs.rs/serde_json/latest/serde_json/fn.to_string_pretty.html
[`.with_column()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.with_column
[`DFSchema`]: https://docs.rs/datafusion/latest/datafusion/common/struct.DFSchema.html
[`DFSchema::logically_equivalent_names_and_types()`]: https://docs.rs/datafusion/latest/datafusion/common/struct.DFSchema.html#method.logically_equivalent_names_and_types
[`.logically_equivalent_names_and_types()`]: https://docs.rs/datafusion/latest/datafusion/common/struct.DFSchema.html#method.logically_equivalent_names_and_types
[`DFSchema::field_with_name()`]: https://docs.rs/datafusion/latest/datafusion/common/struct.DFSchema.html#method.field_with_name
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
[DFSchema::inner]: https://docs.rs/datafusion/latest/datafusion/common/struct.DFSchema.html#method.inner
[`Schema`]: https://docs.rs/arrow/latest/arrow/datatypes/struct.Schema.html
[`Field`]: https://docs.rs/arrow/latest/arrow/datatypes/struct.Field.html
[`DataType`]: https://docs.rs/arrow/latest/arrow/datatypes/enum.DataType.html
[`arrow::compute::can_cast_types()`]: https://docs.rs/arrow/latest/arrow/compute/fn.can_cast_types.html
[`can_cast_types()`]: https://docs.rs/arrow/latest/arrow/compute/fn.can_cast_types.html
[`.cast_to()`]: https://docs.rs/datafusion/latest/datafusion/logical_expr/struct.Expr.html#method.cast_to
[`SessionContext`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html
[`.read_parquet()`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.read_parquet
[`.select()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.select
[`coalesce`]: https://docs.rs/datafusion-functions/latest/datafusion_functions/core/expr_fn/fn.coalesce.html
[`TableProvider`]: https://docs.rs/datafusion/latest/datafusion/datasource/trait.TableProvider.html
[`schema_infer_max_records`]: https://docs.rs/datafusion/latest/datafusion/datasource/file_format/options/struct.CsvReadOptions.html#structfield.schema_infer_max_records
[typesignature]: https://github.com/apache/datafusion/blob/main/datafusion/expr-common/src/signature.rs#L154-L249
[`ListingTable`]: https://docs.rs/datafusion/latest/datafusion/datasource/listing/struct.ListingTable.html
[schema mismatch medium]: https://medium.com/@rakeshchanda/schema-mismatch-understanding-and-resolving-eadf3251f786
[`dataframe!`]: https://docs.rs/datafusion/latest/datafusion/macro.dataframe.html
[TableProvider::schema]: https://docs.rs/datafusion/latest/datafusion/datasource/trait.TableProvider.html#tymethod.schema
[parquet-evolution]: https://spark.apache.org/docs/latest/sql-data-sources-parquet.html#schema-merging
[avro-evolution]: https://avro.apache.org/docs/current/specification/#schema-resolution
[kleppmann]: https://dataintensive.net/
[parquet-dremio]: https://medium.com/data-engineering-with-dremio/all-about-parquet-part-04-schema-evolution-in-parquet-c2c2b1aa6141
[`infer_schema_max_records`]: https://docs.rs/deltalake/latest/deltalake/datafusion/datasource/file_format/options/struct.CsvReadOptions.html#method.schema_infer_max_records
[`Field`]: https://docs.rs/datafusion/latest/datafusion/common/arrow/datatypes/struct.Field.html
[`Schema`]: https://docs.rs/datafusion/latest/datafusion/common/arrow/datatypes/struct.Schema.html
[`nullable()`]: https://docs.rs/datafusion/latest/datafusion/common/trait.ExprSchema.html#method.nullable

<>

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
