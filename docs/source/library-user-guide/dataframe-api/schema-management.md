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

# Schema Management

Schema management is the process of defining, evolving, and reconciling the structure of your data as it moves through a pipeline. Accurate data types are critical for both correctness and performance: they let the optimizer push predicates, pick efficient vectorized kernels, and leverage columnar statistics. In self‑describing formats (Parquet, Avro, Arrow IPC) schemas are embedded and types are known at scan time; in text formats (CSV, NDJSON/JSON) types come from either an explicit schema you provide or inference (e.g., via [`schema_infer_max_records`]), which can drift and should be avoided in production. For custom sources implemented via a [`TableProvider`], the schema returned by [`.schema()`] is the source of truth for column names, data types, and nullability—keep it stable across scans so unions/joins and downstream optimizations behave predictably. For background on [Apache Arrow] schemas and fields (names, data types, nullability, metadata), see the [Arrow schema documentation][arrow schema docs] or the [Arrow schema docs.rs][arrow schema docs.rs].

This guide provides the fundamental concepts and practical tools within DataFusion's DataFrame API to handle these challenges effectively, making your data pipelines more robust and resilient to change.

<!-- TODO: NEW SUBSECTION - Defining Schemas -->

## The Anatomy of a DataFusion Schema

A schema is the contract that defines the structure of your data. In DataFusion, this contract is composed of several key properties. Understanding each one is the key to diagnosing and solving mismatch errors. You can access the schema of a DataFrame at any time with the [`df.schema()`][DataFrame.schema] method.

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

- **`name`**: The column's identifier, used by DataFrame operations.
- **`data_type`**: The Arrow `DataType` describing the column's logical and physical type.
- **`nullable`**: Whether the column may contain `NULL` values (`true` = `NULL`s allowed).
- **`metadata`**: Arbitrary key/value pairs that can be attached to a field or the whole schema. This is often used by file readers to add extra semantic information.

In the remainder of this section, we will focus on four practical aspects of schema management:

- [Column Names](#column-names)
- [Column Count](#column-count)
- [Column Order](#column-order)
- [Column Types](#column-types)

### 1. Column Names

The name is the primary identifier for a column in the DataFrame API. Operations like [`.select()`], [`.with_column()`], and [`.union_by_name()`] all rely on the column name to perform their work.

> **Important:** Column names in DataFusion are **case-sensitive**. A mismatch in capitalization (e.g., `Region` vs. `region`) is treated as a different column.

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

This is a widening conversion: a non‑nullable column can always be represented in a nullable one, but not the other way around. For a deeper discussion of how NULL values behave in expressions, filters, and joins, see the [Handling Null Values guide](./concepts.md#handling-null-values).

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

**Remember:** automatic coercion applies to expressions (e.g., `.select()`, `.with_column()`, `.filter()`), not to DataFrame-combining operations like `.union()` or `.join()`, which require exact type matches.

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

Build schemas with `Schema`, `Field`, and `DataType`; reuse them via `Arc<Schema>`.

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
- **CSV specifics:** parsing is positional; row-length mismatches error by default (use [`.truncated_rows(true)`][`.truncated_rows()] to allow shorter rows and fill NULLs for nullable fields).
- **NDJSON specifics:** alignment is name-based; missing keys become NULL only if the field is part of the inferred (or explicit) schema.
- **Types:** string tokens are not auto-cast to numeric/temporal types; choose explicit schemas where precision or safety matters (e.g., `Decimal128` for currency).
- **Configuration:** tune `CsvReadOptions::schema_infer_max_records(...)` and `NdJsonReadOptions::schema_infer_max_records(...)` to control sampling depth.

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

- [Schemas and Data Types](../concepts.md#schemas-and-data-types) for fundamentals and
- [Creating DataFrames](./creating-dataframes.md) for file reading basics.

**Jump to:** [CSV](#strategy-1-text-formats-csv--ndjson--enforce-schemas) | [NDJSON](#strategy-1-text-formats-csv--ndjson--enforce-schemas) | [Parquet](#strategy-2-self-describing-formats-parquetavroarrow--merge--normalize) | [Partitions](#strategy-3-partitioned-datasets--pruning-with-listingtable) | [Nested Data](#strategy-4-nested-data--structlistmap-modeling)

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

**Alignment** refers to how fields map to schema columns. For CSV, mapping is **positional**: the first column maps to the first field in your schema, the second to the second, and so on. Header names (if present with `has_header(true)`) are read but field order determines the mapping.

What the schema and options control:

- **Types**: values are parsed into the declared Arrow types (e.g., `Decimal128(19,2)` for currency).
- **Missing/extra columns**: by default, row length mismatches error; set `truncated_rows(true)` to allow short rows and fill missing nullable columns with NULLs; extra columns still error.
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

**Production best practice:** Always provide explicit schemas for CSV in production. Schema inference samples only the first 1000 rows (default `schema_infer_max_records`) and can miss type variations in later data. Common pitfalls: IDs inferred as `Int32` then overflow, currency inferred as `Float64` (rounding errors), sparse columns inferred as `Utf8`.

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

**Production best practice:** After reading self-describing formats, normalize to your application's canonical schema using `select()` and `cast_to()` to ensure stable downstream contracts.

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

For very large datasets, organizing files into a directory structure based on column values (e.g., `year` and `month`) is a highly effective performance strategy. DataFusion's [`ListingTable`] is designed for this layout. It performs partition pruning, using your query's filters to skip reading entire directories that don't match. Partition pruning can skip entire directories, reducing scanned data volume. Use `.explain()` to verify pruning in your query plan.

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

## Schema Validation

**Schema validation is your pipeline's immune system—it catches corrupt data before it infects downstream systems.**

In production, data arrives from untrusted sources: third-party APIs return unexpected fields, CSV uploads contain typos, and even "stable" partners change formats without warning. DataFusion trusts what you give it and won't validate schemas automatically.
This section shows :

    - DataFusion's built-in validation capabilities
    - practical production patterns
    - when to reach for external validation libraries.

### DataFusion's Built-in Validation Tools

DataFusion provides several schema utilities you can leverage before building custom validators:

```rust
use datafusion::prelude::*;
use datafusion::arrow::compute::can_cast_types;
use datafusion::arrow::datatypes::{DataType, Schema};

// 1. Check if two schemas are logically equivalent
let schema_a = df_a.schema();
let schema_b = df_b.schema();
if !schema_a.logically_equivalent_names_and_types(&schema_b) {
    return Err(/* schemas don't match */);
}

// 2. Check if a type can be safely cast (uses Arrow's can_cast_types)
let from_type = DataType::Int32;
let to_type = DataType::Int64;
if can_cast_types(&from_type, &to_type) {
    // Safe to cast Int32 → Int64
}

// 3. Field lookup with error handling
match df.schema().field_with_name("customer_id") {
    Ok(field) => { /* field exists, check type */ }
    Err(_) => { /* field missing */ }
}
```

These are fast, zero-copy checks on schema metadata—no data scanning required.

### Production Pattern: Validate-Then-Normalize

The most robust production pattern validates critical fields, then normalizes compatible types. This catches breaking changes while tolerating minor drift.

```rust
use datafusion::prelude::*;
use datafusion::arrow::compute::can_cast_types;
use datafusion::arrow::datatypes::{DataType, Field};

/// Production-grade validation: check required fields, auto-cast compatible types
async fn validate_and_normalize(
    mut df: DataFrame,
    required_fields: &[(String, DataType)]
) -> Result<DataFrame> {
    let schema = df.schema();

    for (name, target_type) in required_fields {
        let field = schema.field_with_name(name).map_err(|_| {
            DataFusionError::Plan(format!("Missing required field: '{}'", name))
        })?;

        // If types differ, check if we can cast
        if field.data_type() != target_type {
            if can_cast_types(field.data_type(), target_type) {
                // Auto-cast to target type
                df = df.with_column(
                    name,
                    col(name).cast_to(target_type, schema)?
                )?;
            } else {
                return Err(DataFusionError::Plan(format!(
                    "Incompatible type for '{}': {:?} cannot cast to {:?}",
                    name, field.data_type(), target_type
                )));
            }
        }
    }
    Ok(df)
}

// Real-world usage: validate partner API data
let df = ctx.read_json("api/daily_orders.json", json_opts).await?;
let df = validate_and_normalize(df, &[
    ("order_id".to_string(), DataType::Int64),
    ("amount".to_string(), DataType::Decimal128(38, 9)),
    ("timestamp".to_string(), DataType::Timestamp(TimeUnit::Microsecond, None)),
]).await?;
```

### Strict Validation for Compliance Workloads

For regulatory/financial data, every field must match exactly—same name, type, nullability, order. This is a fail-fast check on schema metadata.

```rust
use datafusion::prelude::*;

fn validate_exact_schema(df: &DataFrame, expected: &Schema) -> Result<()> {
    let actual = df.schema().as_arrow();

    if actual.fields().len() != expected.fields().len() {
        return Err(DataFusionError::Plan(format!(
            "Field count mismatch: expected {}, got {}",
            expected.fields().len(), actual.fields().len()
        )));
    }

    // Iterate fields; early return on first mismatch
    for (i, expected_field) in expected.fields().iter().enumerate() {
        let actual_field = &actual.fields()[i];
        if actual_field != expected_field {  // Uses Field's PartialEq
            return Err(DataFusionError::Plan(format!(
                "Field[{}] mismatch: expected {:?}, got {:?}",
                i, expected_field, actual_field
            )));
        }
    }
    Ok(())
}

// Usage: regulatory reporting where schema drift = data corruption
let df = ctx.read_csv("sarbanes_oxley_report.csv", csv_opts).await?;
validate_exact_schema(&df, &sox_report_schema_v2())?;
```

> **Performance note**: These validations check schema metadata (typically <1KB), not data. They complete in microseconds even for billion-row DataFrames.

### When to Use External Validation Libraries

For richer validation rules beyond schema checks—value ranges, regex patterns, cross-field constraints—use dedicated libraries:

| Library                                        | Use Case                                  | Example                                     |
| :--------------------------------------------- | :---------------------------------------- | :------------------------------------------ |
| **[validator](https://docs.rs/validator)**     | Derive-based validation with custom rules | Email format, numeric ranges, string length |
| **[jsonschema](https://docs.rs/jsonschema)**   | JSON Schema spec compliance               | API contracts, config validation            |
| **[serde_valid](https://docs.rs/serde_valid)** | Validation during deserialization         | Validate while parsing JSON/CSV             |

Example combining DataFusion with `validator`:

```rust
use validator::Validate;
use datafusion::prelude::*;

#[derive(Debug, Validate)]
struct OrderRecord {
    #[validate(range(min = 1))]
    order_id: i64,

    #[validate(range(min = 0.01, max = 1_000_000.0))]
    amount: f64,

    #[validate(email)]
    customer_email: String,
}

// 1. Validate schema with DataFusion
let df = validate_and_normalize(df, &required_fields).await?;

// 2. Validate business rules per-row (if needed)
let batches = df.collect().await?;
for batch in batches {
    // Convert to structs, validate with derive macros
    // (See validator docs for integration patterns)
}
```

### Choosing Your Approach

| Validation Need                          | DataFusion Built-in                            | External Library                                  |
| :--------------------------------------- | :--------------------------------------------- | :------------------------------------------------ |
| Schema structure (field names, types)    | ✅ Use `field_with_name()`, `can_cast_types()` |                                                   |
| Type compatibility checks                | ✅ Use `can_cast_types()`                      |                                                   |
| Auto-casting compatible types            | ✅ Use `.cast_to()` in DataFrame operations    |                                                   |
| Value-level validation (ranges, formats) |                                                | ✅ Use `validator`, `serde_valid`                 |
| Complex business rules                   |                                                | ✅ Write custom logic or use constraint libraries |

**Recommendation**: Start with DataFusion's schema validation for 90% of cases. Add external libraries only when you need value-level or complex cross-field validation.

See also: [Schema Evolution Patterns](#schema-evolution-patterns), [Error Recovery](#error-recovery), [SchemaAdapter](https://docs.rs/datafusion-datasource/latest/datafusion_datasource/schema_adapter/trait.SchemaAdapter.html) for automatic file schema mapping.

---

[`.truncated_rows()`]: https://github.com/apache/datafusion/blob/main/datafusion/core/src/datasource/file_format/options.rs#L233-L240
[`arrow_csv::ReaderBuilder::with_truncated_rows`]: https://docs.rs/arrow-csv/latest/arrow_csv/reader/struct.ReaderBuilder.html#method.with_truncated_rows
[`arrow` crate]: https://docs.rs/arrow/latest/arrow/
[Apache Arrow]: https://arrow.apache.org/
[arrow schema]: https://docs.rs/arrow/latest/arrow/datatypes/struct.Schema.html
[arrow schema docs]: https://arrow.apache.org/cookbook/py/schema.html
[arrow schema docs.rs]: https://docs.rs/arrow/latest/arrow/datatypes/struct.Schema.html
[arrow dtype]: https://arrow.apache.org/docs/python/api/datatypes.html
[`SchemaRef`]: https://docs.rs/datafusion/latest/datafusion/common/arrow/datatypes/type.SchemaRef.html
[`.inner()`]: https://docs.rs/datafusion/latest/datafusion/common/struct.DFSchema.html#method.inner
[`.filter()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.filter
[`.join()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.join
[`.union()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.union
[`.union_by_name()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.union_by_name
[DataFrame.schema]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.schema
[`.to_string_pretty()`]: https://docs.rs/serde_json/latest/serde_json/fn.to_string_pretty.html
[`.with_column()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.with_column
[`DFSchema`]: https://docs.rs/datafusion/latest/datafusion/common/struct.DFSchema.html
[DFSchema::inner]: https://docs.rs/datafusion/latest/datafusion/common/struct.DFSchema.html#method.inner
[`Schema`]: https://docs.rs/arrow/latest/arrow/datatypes/struct.Schema.html
[`Field`]: https://docs.rs/arrow/latest/arrow/datatypes/struct.Field.html
[`DataType`]: https://docs.rs/arrow/latest/arrow/datatypes/enum.DataType.html
[`SessionContext`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html
[`CsvReadOptions`]: https://docs.rs/datafusion/latest/datafusion/datasource/file_format/options/struct.CsvReadOptions.html
[`NdJsonReadOptions`]: https://docs.rs/datafusion/latest/datafusion/datasource/file_format/options/struct.NdJsonReadOptions.html
[`ParquetReadOptions`]: https://docs.rs/datafusion/latest/datafusion/datasource/file_format/options/struct.ParquetReadOptions.html
[`coalesce`]: https://docs.rs/datafusion-functions/latest/datafusion_functions/core/expr_fn/fn.coalesce.html
[`TableProvider`]: https://docs.rs/datafusion/latest/datafusion/datasource/trait.TableProvider.html
[`schema_infer_max_records`]: https://docs.rs/datafusion/latest/datafusion/datasource/file_format/options/struct.CsvReadOptions.html#structfield.schema_infer_max_records
[typesignature]: https://github.com/apache/datafusion/blob/main/datafusion/expr-common/src/signature.rs#L154-L249
[`ListingTable`]: https://docs.rs/datafusion/latest/datafusion/datasource/listing/struct.ListingTable.html

## Performance Considerations

Schema operations have different performance characteristics. Understanding the cost of each operation helps you build efficient pipelines.

### Zero-Cost Operations (Metadata Only)

These operations work at the logical plan level and don't touch actual data:

```rust
// ✅ Essentially free - just metadata manipulation
let renamed = df.select(vec![
    col("old_name").alias("new_name"),
    col("other_col"),
])?;

// ✅ Free - describes transformation, doesn't execute
let union_plan = df1.union_by_name(df2)?;

// ✅ Free - adds NULL column without scanning data
let with_new_col = df.with_column("new_col", lit(ScalarValue::Null))?;
```

### Low-Cost Operations

These operations touch data but are highly optimized:

```rust
// ⚡ Fast - predicate pushdown eliminates unnecessary reads
let filtered = df.filter(col("year").eq(lit(2024)))?;

// ⚡ Fast - projection pushdown reads only needed columns
let projected = df.select(vec![col("id"), col("name")])?;
```

### High-Cost Operations (Full Data Scans)

These operations require reading and transforming all data:

```rust
// 💰 Expensive - scans all data and creates new arrays
let casted = df.with_column(
    "id",
    col("id").cast_to(&DataType::Int64, df.schema())?
)?;

// 💰 Expensive - computes aggregates over entire dataset
let grouped = df.aggregate(
    vec![col("category")],
    vec![count(col("sales"))]
)?;

// 💰 Expensive - deduplication requires full scan
let distinct = df.distinct()?;
```

### Performance Best Practices

**1. Schema Alignment at Read Time**

```rust
// ✅ Good: Define schema upfront, avoid runtime casts
let schema = Arc::new(Schema::new(vec![
    Field::new("id", DataType::Int64, false),
    Field::new("amount", DataType::Decimal128(19, 2), false),
]));

let df = ctx.read_csv("data.csv", CsvReadOptions::new().schema(&schema)).await?;

// ❌ Bad: Infer then cast (doubles the work)
let df = ctx.read_csv("data.csv", CsvReadOptions::new()).await?;
let df = df.with_column("id", col("id").cast_to(&DataType::Int64, df.schema())?)?;
```

**2. Push Filters Before Casts**

```rust
// ✅ Good: Filter first (less data to cast)
let df = df
    .filter(col("year").eq(lit(2024)))?  // Reduces dataset
    .with_column("id", col("id").cast_to(&DataType::Int64, df.schema())?)?;

// ❌ Bad: Cast first (more data to process)
let df = df
    .with_column("id", col("id").cast_to(&DataType::Int64, df.schema())?)?
    .filter(col("year").eq(lit(2024)))?;
```

**3. Batch Schema Normalization**

```rust
// ✅ Good: Single select with multiple transformations
let normalized = df.select(vec![
    col("id").cast_to(&DataType::Int64, df.schema())?.alias("id"),
    col("amount").cast_to(&DataType::Decimal128(19, 2), df.schema())?.alias("amount"),
    col("name").alias("customer_name"),
])?;

// ❌ Bad: Multiple passes over data
let df = df.with_column("id", col("id").cast_to(&DataType::Int64, df.schema())?)?;
let df = df.with_column("amount", col("amount").cast_to(&DataType::Decimal128(19, 2), df.schema())?)?;
let df = df.with_column("customer_name", col("name"))?;
```

**4. Leverage Partition Pruning**

```rust
// ✅ Good: Partition by commonly filtered columns
// Directory structure: /data/year=2024/month=01/*.parquet

let listing_options = ListingOptions::new(Arc::new(ParquetFormat::default()))
    .with_table_partition_cols(vec![
        ("year".into(), DataType::Int32),
        ("month".into(), DataType::Int8),
    ]);

// Filter pushdown skips entire directories
let df = ctx.read_table(table)?
    .filter(col("year").eq(lit(2024)))?;  // Only reads year=2024 dirs
```

**Performance Measurement:**

```rust
// Always verify performance assumptions with explain plans
df.explain(false, false)?.show().await?;

// Look for:
// - "Projection" without "Filter" → might be reading too much
// - Multiple "Cast" operations → consolidate if possible
// - Missing partition pruning → check filter predicates
```

---

## Error Recovery

Production data pipelines must handle schema errors gracefully. This section covers strategies for detecting, recovering from, and logging schema-related failures.

### Common Schema Error Patterns

```rust
use datafusion::prelude::*;
use datafusion::error::{DataFusionError, Result};

async fn robust_read_with_schema_recovery(
    ctx: &SessionContext,
    path: &str,
    expected_schema: Arc<Schema>
) -> Result<DataFrame> {
    // Attempt 1: Try with expected schema
    match ctx.read_parquet(path, ParquetReadOptions::default().schema(&expected_schema)).await {
        Ok(df) => {
            println!("✓ Successfully read with expected schema");
            return Ok(df);
        },
        Err(e) => {
            eprintln!("⚠ Failed to read with expected schema: {}", e);

            // Attempt 2: Read with auto-detected schema and validate
            match ctx.read_parquet(path, ParquetReadOptions::default()).await {
                Ok(df) => {
                    println!("✓ Read succeeded with auto-detected schema");

                    // Try to normalize to expected schema
                    match normalize_to_schema(df, &expected_schema).await {
                        Ok(normalized) => {
                            println!("✓ Successfully normalized to expected schema");
                            Ok(normalized)
                        },
                        Err(norm_err) => {
                            eprintln!("✗ Normalization failed: {}", norm_err);
                            Err(norm_err)
                        }
                    }
                },
                Err(read_err) => {
                    eprintln!("✗ All read attempts failed");
                    Err(read_err)
                }
            }
        }
    }
}

async fn normalize_to_schema(
    df: DataFrame,
    target: &Schema
) -> Result<DataFrame> {
    let actual_schema = df.schema();
    let mut select_exprs = Vec::new();

    for target_field in target.fields() {
        match actual_schema.field_with_name(target_field.name()) {
            Ok(actual_field) => {
                // Field exists - cast if needed
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
                // Field missing - add NULL if nullable
                if target_field.is_nullable() {
                    select_exprs.push(
                        lit(ScalarValue::Null).alias(target_field.name())
                    );
                } else {
                    return Err(DataFusionError::Plan(format!(
                        "Cannot normalize: missing required field '{}'",
                        target_field.name()
                    )));
                }
            }
        }
    }

    df.select(select_exprs)
}
```

### Schema Validation with Detailed Diagnostics

```rust
use datafusion::prelude::*;
use datafusion::arrow::datatypes::Schema;
use datafusion::error::Result;
use std::sync::Arc;

/// Comprehensive schema comparison with detailed error reporting
fn compare_schemas(
    actual: &Schema,
    expected: &Schema
) -> Result<()> {
    let mut errors = Vec::new();

    // Check for missing fields
    for expected_field in expected.fields() {
        match actual.field_with_name(expected_field.name()) {
            Ok(actual_field) => {
                // Type mismatch
                if actual_field.data_type() != expected_field.data_type() {
                    errors.push(format!(
                        "  • '{}': type mismatch - expected {:?}, got {:?}",
                        expected_field.name(),
                        expected_field.data_type(),
                        actual_field.data_type()
                    ));
                }

                // Nullability mismatch
                if actual_field.is_nullable() != expected_field.is_nullable() {
                    errors.push(format!(
                        "  • '{}': nullability mismatch - expected nullable={}, got nullable={}",
                        expected_field.name(),
                        expected_field.is_nullable(),
                        actual_field.is_nullable()
                    ));
                }
            },
            Err(_) => {
                errors.push(format!(
                    "  • '{}': MISSING in actual schema",
                    expected_field.name()
                ));
            }
        }
    }

    // Check for extra fields
    for actual_field in actual.fields() {
        if expected.field_with_name(actual_field.name()).is_err() {
            errors.push(format!(
                "  • '{}': UNEXPECTED field in actual schema",
                actual_field.name()
            ));
        }
    }

    if !errors.is_empty() {
        let error_msg = format!(
            "Schema validation failed with {} error(s):\n{}",
            errors.len(),
            errors.join("\n")
        );
        return Err(datafusion::error::DataFusionError::Plan(error_msg));
    }

    Ok(())
}
```

### Multi-File Read with Partial Failure Handling

```rust
use datafusion::prelude::*;
use datafusion::error::Result;

async fn read_files_with_schema_tolerance(
    ctx: &SessionContext,
    paths: Vec<&str>,
    expected_schema: Arc<Schema>
) -> Result<Vec<DataFrame>> {
    let mut successful = Vec::new();
    let mut failed = Vec::new();

    for path in paths {
        match robust_read_with_schema_recovery(ctx, path, expected_schema.clone()).await {
            Ok(df) => {
                println!("✓ Successfully processed: {}", path);
                successful.push(df);
            },
            Err(e) => {
                eprintln!("✗ Failed to process {}: {}", path, e);
                failed.push((path, e.to_string()));
            }
        }
    }

    if successful.is_empty() {
        return Err(datafusion::error::DataFusionError::Plan(
            format!("All {} file(s) failed to process", failed.len())
        ));
    }

    if !failed.is_empty() {
        eprintln!(
            "⚠ Warning: {} of {} files failed. Proceeding with {} successful files.",
            failed.len(),
            paths.len(),
            successful.len()
        );
    }

    Ok(successful)
}
```

**See also:**

- [Error Handling & Recovery](./creating-dataframes.md#error-handling--recovery) — Comprehensive error handling patterns for DataFrame creation, including I/O errors, schema inference failures, and timeout handling

---

## TOPIC ?

### The Root Cause: Why Schemas Break in the Real World

In modern data platforms, schemas rarely stay still. Most changes fall into three buckets:

- **Source-system drift** – the authoritative source shifts  
  • Database migration: widen `user_id` Int32 → Int64  
  • API v2: rename `userID` → `user_guid`, int → string

- **Feature evolution** – new business requirements  
  • Added column: nightly export gains `customer_segment`  
  • Dropped column: deprecated field removed

- **Inconsistent sources** – systems or humans never aligned  
  • Join Salesforce (`ContactId`) with Zendesk (`requester_id`)  
  • CSV header `Region` vs `region` (case-sensitive)

> **Schema-on-read:** In data-lake practice, old files are immutable; new ones carry the new schema. DataFusion unifies them at query time ([`.union_by_name()`], reader-level merging), avoiding risky rewrites.

<!-- TODO: Add concrete error messages for each mismatch type to help users recognize them -->

### The Four Types of Mismatch

No matter the root cause, a schema conflict will always manifest as one of four technical problems. Your first step in diagnosing any issue is to identify which of these you are facing:

1.  **Count Mismatch:** DataFrames have a different number of columns.
2.  **Order Mismatch:** Columns are in a different order.
3.  **Name Mismatch:** A column has a different name or capitalization (e.g., `cust_id` vs `customer_id`).
4.  **Type Mismatch:** A column has a different data type (e.g., `Int32` vs `Int64`).

The rest of this guide is structured around this powerful mental model. We will show you how a single tool, `.union_by_name()`, solves the first three problems, and how explicit casting solves the fourth.

<!--TODO definitly implement a proper solution for every kind in the following !-->

<!-- TODO: Replace the generic TODO above with concrete per-mismatch examples (Count/Order/Name/Type), each with runnable snippets. -->

<!-- TODO: Fix typo: "Missmatch" → "Mismatch". -->

<!-- TODO: Fix typo: "Missmatch" → "Mismatch" -->
<!-- TODO: Add a helper function/macro for pretty-printing schema comparisons -->
<!-- TODO: Show how to use df.explain() to see where schema errors occur in complex queries -->

### Diagnose the Schema Missmatch

When an error fires

```
Schema error: Union schemas have different number of fields: 3 vs 4
Arrow error: Schema error: Cannot merge incompatible data types Int32 and Utf8
```

The [`df.schema()`][`.schema()`] method on a DataFrame tells you what the problem is.

Compare both schemas side-by-side like in the following example:

```rust
use datafusion::prelude::*;
use datafusion::arrow::datatypes::DataType;

// Assume df1 and df2 are failing to union due to a schema error.
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

Example output revealing the problems:

```
=== Schema 1 ===
customer_id          Int32 nullable=false
amount               Float64 nullable=false
region               Utf8  nullable=true

=== Schema 2 ===
customer_id          Int64 nullable=false    ← Type changed!
amount               Float64 nullable=false
Region               Utf8  nullable=true     ← Human error!
category             Utf8  nullable=true     ← New column!
```

This immediately surfaces the root cause (type drift, new column, naming inconsistency), turning a confusing error into an actionable checklist.

<!-- TODO: Break this into clearer subsections: "Quick Fixes" vs "Strategic Solutions" -->
<!-- TODO: Add decision tree: When to use union_by_name vs select vs cast_to -->

### Resolve - Strategies for Aligning Schemas

<!-- TODO: Show the failure case first (what happens without union_by_name), then the solution -->

### The Default Strategy: The `union_by_name()` Superpower

For 90% of cases, this is the first and best tool you should reach for. It is the idiomatic DataFrame solution because it solves the most common and dangerous problems of schema evolution safely and elegantly.

Its power comes from two capabilities SQL `UNION`/`UNION ALL` don't combine:

1. **Name‑aligned, not positional**: SQL `UNION`/`UNION ALL` match by position and can misalign when order changes; [`.union_by_name()`] and [`.union_by_name_distinct()`] align by header.
2. **Choice of semantics + flexible shape:** Creates a unified superset schema (fills missing columns with `NULL`) and lets you choose duplicates behavior — [`.union_by_name()`] keeps duplicates (SQL `UNION ALL`), [`.union_by_name_distinct()`] deduplicates (SQL `UNION`).

This combination of safety and flexibility is a primary reason the DataFrame API is so effective for data engineering.

<!-- TODO: Title is confusing - simplify to "The Two-Step Pattern" or "Type Alignment Pattern" -->

### Process: The Core Pattern: Align Types, then Unify Shape

While [`.union_by_name()`] handles shape and order, it still requires that matched columns have compatible types. If you have a type mismatch (e.g., `Int32` vs. `Int64`), you must align the types first.

The example below demonstrates the complete, robust pattern for handling a real-world schema drift scenario.

```rust
use datafusion::prelude::*;
use datafusion::arrow::datatypes::DataType;

// df1: 'id' is explicitly defined as Int64.
let df1 = dataframe!(
    "id" => [1_i64, 2_i64],
    "name" => ["Alice", "Bob"]
)?;

// df2: 'id' is inferred as Int32, has a different column order, and an extra column.
let df2 = dataframe!(
    "name" => ["Carol", "Dave"],
    "id" => [3, 4], // Rust integer literals default to Int32
    "email" => ["c@ex.com", "d@ex.com"]
)?;

// Step 1: Align the schemas by casting df2's 'id' column to Int64.
let df2_aligned = df2.with_column(
    "id",
    col("id").cast_to(&DataType::Int64, df2.schema())?
)?;

// Step 2: Now union_by_name can safely merge the aligned DataFrames.
let result = df1.union_by_name(df2_aligned)?;

// Resulting Schema: (id: Int64, name: Utf8, email: Utf8 nullable)
// Verify the result:
#[cfg(test)]
use datafusion::assert_batches_eq;

#[cfg(test)]
assert_batches_eq!(
    &[
        "+----+-------+-----------+",
        "| id | name  | email     |",
        "+----+-------+-----------+",
        "| 1  | Alice |           |",
        "| 2  | Bob   |           |",
        "| 3  | Carol | c@ex.com  |",
        "| 4  | Dave  | d@ex.com  |",
        "+----+-------+-----------+",
    ],
    &result.collect().await?
);
```

<!-- TODO: Link to API docs for `with_column`, `cast_to`, `col`, `lit`. -->
<!-- TODO: Fix wording "Best Practise" → "Best Practice"; convert to Sphinx admonition (e.g., .. tip::). -->

> **Best Practise - "The Golden Rule of Type Casting"- :**<br>
> To prevent data loss, you must **always cast the narrower type up to the widest common type.** For example, cast `Int32` up to `Int64`. This "widening conversion" is always safe.<br> > **Never cast** a wider type down to a narrower one (e.g., `Int64` -> `Int32`) unless you have explicitly proven that no values will be truncated. Doing so risks silent data corruption.

---

<!-- TODO: Add more examples showing the power of select() for complex transformations -->
<!-- TODO: Show how to handle nullable vs non-nullable mismatches -->

<!-- TODO: Add reference links: [Schema], [Field], [DataType], [TimeUnit], [SessionContext], [CsvReadOptions], [JsonReadOptions], [NdJsonReadOptions], [ParquetReadOptions], [coalesce] -->

<!-- TODO: This "Root Cause" section should come AFTER understanding the type system and mismatch types -->

### Precision toolkit: [`.select()`] for full control

Use this when you need surgical control over names, types, and semantics:

- **Semantic transforms:** map legacy integer codes to strings with `when(...).otherwise(...)`
- **Explicit rename/cast:** align columns that differ in name or type
- **Custom defaults:** add missing columns with meaningful values (not just `NULL`)

<!-- TODO: The table format is hard to read - consider reformatting or using a different layout -->

### Strategic choice: resilient vs strict union

Choose based on how you want the pipeline to behave when upstream schemas evolve.

| Strategy     | Resilient union ([`.union_by_name()`])                               | Strict union ([`.union()`])                                               |
| ------------ | -------------------------------------------------------------------- | ------------------------------------------------------------------------- |
| Philosophy   | Robust default; tolerates non-breaking changes (e.g., extra columns) | Fail-fast; asserts identical name, type, and order                        |
| When to use  | Pipelines that must keep working if non-critical columns are added   | Locked-down pipelines where any schema deviation should error immediately |
| What it does | Matches by name; order ignored; missing columns filled with `NULL`   | Matches by position; errors if shape/order/types don't match exactly      |

<!-- TODO: Polish copy: "An example is shown as the following :" → "Example:". -->

An example is shown as the following :

```rust
use datafusion::prelude::*;
use datafusion::arrow::datatypes::DataType;

// jan_df schema: (customer_id: Int32, status_code: Int32)
// feb_df schema: (id: Int64, status: Utf8, category: Utf8)

// Step 1: Manually align jan_df to match the target schema's core columns.
let jan_aligned = jan_df.select(vec![
    col("customer_id").cast_to(&DataType::Int64, jan_df.schema())?.alias("id"),
    when(col("status_code").eq(lit(1)), lit("active"))
        .otherwise(lit("inactive"))?
        .alias("status"),
])?;

// Step 2A: The Resilient Strategy (using union_by_name)
// This is robust. It will succeed even though feb_df has an extra 'category' column.
let resilient_union = jan_aligned.clone().union_by_name(feb_df.clone())?;


// Step 2B: The Strict Strategy (using union)
// This is a fail-fast assertion. It requires selecting the exact same columns in the same order.
let strict_union = jan_aligned
    .select(vec![col("id"), col("status")])?
    .union(
        feb_df.select(vec![col("id"), col("status")])?
    )?;
```

**Hint:** Prefer union_by_name() unless you intentionally want a fail-fast positional check after alignment.

---

<!-- TODO: This should probably be moved earlier as it's a common first encounter with schema issues -->
<!-- TODO: Add examples for JSON and CSV files, not just Parquet -->
<!-- TODO: Explain what happens when automatic merging fails and how to debug it -->

### Automatic Schema Merging for File Sources

When you pass multiple files of the same format (e.g., a list of Parquet files...), DataFusion computes a unified superset schema:

**The Rules of the Merge:**

- **It is Named, Not Positional:** It matches columns by their headers, making it robust against different column orders.
- **It is Widening:** It promotes compatible numeric types to the widest common type (e.g., a mix of `Int32` and `Int64` columns will become `Int64`).
- **It is Additive:** It marks columns that are missing in some files as nullable.
- **It is Safe:** It will error on incompatible type conflicts that would lead to data corruption (e.g., `Int32` vs. `Utf8`).

**Limitations to be aware of:**

- **Nested type mismatches fail**: While simple types can be widened (e.g., `Int32` → `Int64`), nested structures must match exactly. For example, `List<Struct{a:Int32}>` and `List<Struct{a:Int64}>` are incompatible and will cause a merge error, even though the difference is only in the nested field's width.
- **Metadata is discarded by default**: Schema-level and field-level metadata are dropped during merge unless you explicitly configure Parquet to preserve it (via `skip_metadata(false)` on `ParquetReadOptions`). If you rely on metadata annotations (e.g., for lineage or compliance), ensure consistent metadata across files or handle reconciliation explicitly.

```rust
use datafusion::prelude::*;

// Assume two files with schema drift:
// v1.parquet: (id: Int32, name: Utf8)
// v2.parquet: (id: Int64, name: Utf8, email: Utf8)
let df = ctx.read_parquet(
    vec!["v1.parquet", "v2.parquet"],
    ParquetReadOptions::default()
).await?;

// The resulting schema is automatically unified.
println!("Unified Schema:\n{}", df.schema().to_string_pretty());

/* Expected Output:
Unified Schema:
-----------------
id: Int64
name: Utf8
email: Utf8 (nullable)
-----------------
*/

// The data from both files is merged with NULLs for missing columns:
#[cfg(test)]
assert_batches_eq!(
    &[
        "+----+------+----------+",
        "| id | name | email    |",
        "+----+------+----------+",
        "| 1  | A    |          |",  // v1.parquet: email didn't exist
        "| 2  | B    |          |",
        "| 3  | C    | c@ex.com |",  // v2.parquet: has email column
        "| 4  | D    | d@ex.com |",
        "+----+------+----------+",
    ],
    &df.collect().await?
);
```

**Strategic guidance: choosing the right ingestion pattern**

| Aspect      | Automatic Batch Read (this section)                                    | Iterative DataFrame Union ([`.union_by_name()`])                       |
| :---------- | ---------------------------------------------------------------------- | ---------------------------------------------------------------------- |
| Best for    | Batch loading a full, known set of files/partitions at once            | Incremental pipelines where files arrive one by one                    |
| Control     | Automatic; merge rules are built-in                                    | Full control; inject casts/renames/defaults per file via [`.select()`] |
| Performance | Often higher: planner can optimize the entire multi-file scan together | Flexible; per-file work may add overhead but enables custom logic      |

<!-- TODO: Convert this HTML-styled warning into a Sphinx admonition (`.. warning::`) for consistency with the docs. -->

> **⚠️"A Critical Note on CSVs: Always Provide an Explicit Schema"**<br>
> Schema inference for CSV files ([`schema_infer_max_records`]) is a common source of drift. A single "dirty" row outside the initial scan can change the inferred type between reads. <br> **In production, always provide an explicit schema**:

```rust
use std::sync::Arc;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::prelude::*;
// Define the canonical schema you expect.
let schema = Arc::new(Schema::new(vec![
    Field::new("id", DataType::Int64, false),
    Field::new("name", DataType::Utf8, false),
]));
// --- Best Practice: Provide the schema explicitly ---
let df_safe = ctx.read_csv(
    "data.csv",
    CsvReadOptions::new().schema(&schema)
).await?;
// --- If you must rely on inference, increase the sample size ---
let df_inferred = ctx.read_csv(
    "data.csv",
    CsvReadOptions::new()
    .with_schema_infer_max_records(10000) // Increase sample to 10k rows
).await?;
```

---

<!-- TODO: Extract a 5–7 line "At a glance" checklist and place it near the start of the section (keep the full version here). -->

<!-- TODO: This checklist is great but buried at the end - consider moving key points up front -->
<!-- TODO: Add a "Quick Reference Card" format that users can print/bookmark -->

## Summary: A Strategic Checklist

Now that you have the tools, here is a strategic summary for building and debugging robust pipelines.

### The Debugging Workflow

When schemas won't align, check for these four mismatch types in order:

1. **Column Count Mismatch**: Are columns missing or extra?

   - **Fix**: Use `.union_by_name()` to handle this automatically.

2. **Column Order Mismatch**: Are the columns in a different order?

   - **Fix**: Use `.union_by_name()` to safely ignore positional order.

3. **Column Name Mismatch**: Are there typos or capitalization differences?

   - **Fix**: Use [`.alias()`] within a `.select()` expression to standardize names.

4. **Column Type Mismatch**: Are the types incompatible?
   - **Fix**: Use `.cast_to()` to unify types, always casting up to the widest common type to prevent data loss.

<!-- .cast_to() is NOT metadata-only - it transforms actual data and has a performance cost -->

<!-- TODO: Add anti-patterns section: "What NOT to do" -->
<!-- TODO: Add performance implications of different approaches -->

### High-Level Best Practices

- **At Write Time**: Enforce consistent schemas at the source if possible. An ounce of prevention is worth a pound of cure.
- **At Read Time**: Prefer `.union_by_name()` for its resilience against common schema evolution problems.
- **In Pipelines**: Validate schemas at critical steps. Fail fast if a schema deviates unexpectedly.
- **For Debugging**: Inspect schemas liberally. `println!("{:#?}", df.schema())` is your best friend.

> **Performance Note**
>
> **Metadata operations** like `.select()` and `.union_by_name()` work at the logical plan level and are essentially free—they describe transformations without touching data until execution.
>
> **Data operations** like `.cast_to()` perform actual data transformation and have a runtime cost proportional to your dataset size. Type casts materialize new arrays with converted values, requiring a full scan and compute pass over the data.
>
> When designing pipelines, prefer schema alignment at read time (via explicit schemas) over runtime casts to minimize compute overhead.

<!-- TODO: Move this section earlier (just after the intro) or insert a condensed primer there; keep full details here. -->

<!-- TODO: This is CRITICAL foundational knowledge - should be at the START of the section! -->
<!-- TODO: Add examples of what happens when you violate these rules -->
<!-- TODO: Explain nullable type coercion rules -->

### References <!--TODO References -->

- Medium Article [Schema Mismatch Error:- Understanding and Resolving][schema mismatch medium]

[schema mismatch medium]: https://medium.com/@rakeshchanda/schema-mismatch-understanding-and-resolving-eadf3251f786
[`.union_by_name()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.union_by_name
[`.schema()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.schema
[`infer_schema_max_records`]: https://docs.rs/deltalake/latest/deltalake/datafusion/datasource/file_format/options/struct.CsvReadOptions.html#method.schema_infer_max_records
[`.union()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.union
[`.select()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.select
[`.union_by_name_distinct()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.union_by_name_distinct
[`.alias()`]: https://docs.rs/datafusion-expr/latest/datafusion_expr/struct.Expr.html#method.alias
[`.cast_to()`]: https://docs.rs/datafusion-expr/latest/datafusion_expr/struct.Expr.html#method.cast_to
[`.with_column()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.with_column
[`dataframe!`]: https://docs.rs/datafusion/latest/datafusion/macro.dataframe.html
[TableProvider::schema]: https://docs.rs/datafusion/latest/datafusion/datasource/trait.TableProvider.html#tymethod.schema
[parquet-evolution]: https://spark.apache.org/docs/latest/sql-data-sources-parquet.html#schema-merging
[avro-evolution]: https://avro.apache.org/docs/current/specification/#schema-resolution
[kleppmann]: https://dataintensive.net/
[parquet-dremio]: https://medium.com/data-engineering-with-dremio/all-about-parquet-part-04-schema-evolution-in-parquet-c2c2b1aa6141

---

<!--TODO; Execution section -->
