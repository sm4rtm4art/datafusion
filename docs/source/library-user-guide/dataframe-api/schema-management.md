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

- **`name`**: The column’s identifier, used by DataFrame operations.
- **`data_type`**: The Arrow `DataType` describing the column’s logical and physical type.
- **`nullable`**: Whether the column may contain `NULL` values (`true` = `NULL`s allowed).
- **`metadata`**: Arbitrary key/value pairs that can be attached to a field or the whole schema. This is often used by file readers to add extra semantic information.

In the remainder of this section, we will focus on four practical aspects of schema management:

- [Column Names](#column-names)
- [Column Count](#column-count)
- [Column Order](#column-order)
- [Column Types](#column-types)

(column-names)=

### 1. Column Names

The name is the primary identifier for a column in the DataFrame API. Operations like [`.select()`], [`.with_column()`], and [`.union_by_name()`] all rely on the column name to perform their work.

> **Important:** Column names in DataFusion are **case-sensitive**. A mismatch in capitalization (e.g., `Region` vs. `region`) is treated as a different column.

#### 2. Column Count

This refers to the number of columns in a DataFrame. When you combine two DataFrames, a difference in column count is handled gracefully by certain operations.

For example, if you use [`.union_by_name()`] to merge a DataFrame with a new column, the resulting DataFrame will contain the new column, with `NULL` values filling the rows that came from the DataFrame where it was missing. This is a deliberate feature to handle schema evolution safely. Dropped columns are handled similarly. This prevents silent errors by making the merged schema explicit.

### 3. Column Order

DataFusion's DataFrame API is primarily **name-based, not positional**. This is a key difference and a major advantage compared to traditional SQL.

This means that for most operations, especially [`.union_by_name()`], the physical order of the columns in the files or DataFrames does not matter. DataFusion will correctly align `col_A` from one DataFrame (`DataFrame_A`) with `col_A` from another (`DataFrame_B`), regardless of their position. This makes pipelines resilient to changes in column ordering from upstream sources.

### 4. Column Types (The Type System)

Data types define what kind of values each column can hold and how operations behave on them. In DataFusion's DataFrame API, every column must have a specific [Apache Arrow `Dtype`][arrow dtype] that determines its storage format and computational behavior.

**Why types matter for DataFrames:**

- **Data integrity**: Types prevent mixing incompatible data (e.g., strings with numbers)
- **Performance**: Knowing types enables optimized columnar storage and vectorized operations
- **Predictability**: Type rules ensure consistent behavior across operations

**Common Arrow data types in DataFusion:**

| Category           | Arrow Types                                                                | Example Values                        | Common Use Cases                      |
| :----------------- | :------------------------------------------------------------------------- | :------------------------------------ | :------------------------------------ |
| **Integers**       | `Int8`, `Int16`, `Int32`, `Int64`<br>`UInt8`, `UInt16`, `UInt32`, `UInt64` | `42`, `-100`, `0`                     | IDs, counts, quantities               |
| **Floating-Point** | `Float32`, `Float64`                                                       | `3.14`, `-0.001`                      | Measurements, scientific data, ratios |
| **Decimal**        | `Decimal128(precision, scale)`                                             | `99.99`, `1234.5678`                  | Financial data, currency, prices      |
| **Strings**        | `Utf8`, `LargeUtf8`                                                        | `"hello"`, `"データ"`                 | Names, descriptions, categories       |
| **Temporal**       | `Date32`, `Date64`<br>`Timestamp(unit, timezone)`                          | `2024-01-15`<br>`2024-01-15 14:30:00` | Event times, dates                    |
| **Boolean**        | `Boolean`                                                                  | `true`, `false`                       | Flags, conditions                     |
| **Binary**         | `Binary`, `LargeBinary`                                                    | `[0x12, 0x34]`                        | Raw data, hashes                      |
| **Nested Types**   | `Struct(Fields)`, `List(Field)`                                            | `{"a": 1}`, `[1, 2, 3]`               | JSON/Parquet data, complex objects    |

For a complete reference of all supported types, see the [SQL Data Types guide](../../user-guide/sql/data_types.md).

### A Note on Nullability

The `nullable` flag on a field is a critical part of its type definition. When merging schemas (for example, via [`.union_by_name()`]), DataFusion follows a simple, safe rule:

> The Golden Rule of Nullability: If a column is nullable in any of the input schemas, it will be nullable in the output schema.

This is a widening conversion: a non‑nullable column can always be represented in a nullable one, but not the other way around. For a deeper discussion of how NULL values behave in expressions, filters, and joins, see the [Handling Null Values guide](./concepts.md#handling-null-values).

### Type Behavior in DataFrames

DataFusion's type system has two distinct modes of operation, designed for a balance of convenience and safety. The behavior depends on whether you are working within a single DataFrame's expression or combining two different DataFrames.

#### Mode 1: Automatic Coercion in Expressions

For convenience and intuitive use, DataFusion automatically promotes types to a common, wider type when they are mixed within an expression. This applies to functions like [`.select()`], [`.with_column()`], and [`.filter()`]. The promotion is always “widening” and follows the safe upcasting paths defined in the Type Coercion Hierarchy to prevent data loss.

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

## Defining Schemas

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

| Type                 | Code Example                          | What it stores                                    | When to use                                                            |
| -------------------- | ------------------------------------- | ------------------------------------------------- | ---------------------------------------------------------------------- |
| **With timezone**    | `Timestamp(Microsecond, Some("UTC"))` | A specific instant (e.g., "2024-01-15 10:00 UTC") | Server logs, transactions, anything that happened at a specific moment |
| **Without timezone** | `Timestamp(Microsecond, None)`        | A local time (e.g., "2024-01-15 10:00")           | Scheduled events, opening hours, anything relative to local time       |

**Common mistake**: Mixing the two types in operations

```rust
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
- **Re‑attach intentionally**: derived/aggregated columns don’t inherit metadata—add it on the final output schema if needed.
- **Verify format support**: Arrow IPC preserves metadata; Parquet varies; CSV/NDJSON don’t—treat as best‑effort across formats.
- **Reconcile on merge**: when sources disagree, prefer a canonical schema and explicitly resolve conflicts.
- **Keep it small**: avoid large blobs; store long docs externally and reference via a short key (e.g., `doc_url`).
- **Validate early**: add lightweight checks in tests/pipeline (e.g., require `owner`, `schema_version`, `pii` flags where applicable).

### Schema Inference: behavior and limits

Schema inference is sampling-based and format-dependent. Key points:

- Sampling window: only fields seen within `schema_infer_max_records` become columns; later unseen fields are ignored (no new columns are added).
- CSV specifics: parsing is positional; row-length mismatches error by default (use `truncated_rows(true)` to allow shorter rows and fill NULLs for nullable fields).
- NDJSON specifics: alignment is name-based; missing keys become NULL only if the field is part of the inferred (or explicit) schema.
- Types: string tokens are not auto-cast to numeric/temporal types; choose explicit schemas where precision or safety matters (e.g., `Decimal128` for currency).
- Configuration: tune `CsvReadOptions::schema_infer_max_records(...)` and `NdJsonReadOptions::schema_infer_max_records(...)` to control sampling depth.

<!-- TODO: Add a compact table comparing inference behavior (CSV vs NDJSON), with examples and links to the central guidance in creating-dataframes.md. -->

---

## Applying Schemas and Modeling Data Structures

A schema is your contract with the data. Applying it at read-time guarantees that every downstream transformation sees the same column names, types, and nullability—no matter how messy the raw files are.

### Applying Schemas to File Readers

Different file formats require different strategies:

| Format family                              | Has embedded schema?    | When to supply your own?    | Typical pitfalls without one                                    |
| ------------------------------------------ | ----------------------- | --------------------------- | --------------------------------------------------------------- |
| Text (CSV, NDJSON)                         | ❌                      | **Always** in production    | Type drift (Int32 → Int64), strings where numbers were expected |
| Self-describing (Parquet, Avro, Arrow IPC) | ✅                      | When mixing file versions   | Added/dropped columns, widened types                            |
| Semi-structured (regular JSON)             | ❌ (structure inferred) | When you need precise types | Sparse keys, mixed numeric / string values                      |

> **Rule of thumb**: If the format cannot guarantee a stable schema on its own, pass one explicitly.

#### Formats Without Embedded Schemas (CSV & NDJSON)

CSV and NDJSON are not self-describing; they do not carry type information. Without an explicit schema, DataFusion must infer types from a sample of rows, which can change as data evolves and lead to instability.

Provide an explicit schema to guarantee column types and nullability consistently across all records and runs.

##### CSV (explicit schema at read time)

CSV is positional: field 1 maps to column 1, etc. A schema fixes the expected column order, names, and types at read time and avoids inference drift.

See also: [Schema Inference: behavior and limits](#schema-inference-behavior-and-limits).

What the schema and options control:

- Alignment: positional mapping to the declared fields (header names are read if `has_header(true)`).
- Types: values are parsed into the declared Arrow types (e.g., `Decimal128(19,2)` for currency).
- Missing/extra columns: by default, row length mismatches error; set `truncated_rows(true)` to allow short rows and fill missing nullable columns with NULLs; extra columns still error.
- Format details: single-byte `delimiter`, `quote`, optional `escape`, `comment`, and `terminator`; newlines-in-values can be enabled explicitly.

```rust
use datafusion::prelude::*;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use std::sync::Arc;

let ctx = SessionContext::new();

let schema = Arc::new(Schema::new(vec![
    Field::new("id", DataType::Int64, false),
    Field::new("amount", DataType::Decimal128(19, 2), true),
]));

// Apply the schema; configure header/delimiter if needed
let df = ctx.read_csv(
    "data.csv",
    CsvReadOptions::new()
        .schema(&schema)       // enforce canonical types
        .has_header(true)      // file has a header row
        .delimiter(b',')       // single-byte delimiter (e.g., b';' for semicolon)
        // .quote(b'"')        // optional: quote character (default b'"')
        // .comment(b'#')      // optional: ignore lines starting with '#'
        // .truncated_rows(true) // optional: allow fewer columns and fill NULLs for nullable fields
).await?;
```

**Without a schema**, DataFusion samples the first `schema_infer_max_records` rows (default: 1000). Common pitfalls: IDs inferred as `Int32` then overflow, currency inferred as `Float64` (rounding), sparse columns inferred as `Utf8`.

##### NDJSON (line-delimited JSON)

NDJSON records are JSON objects; schemas are applied by field name (unlike CSV’s positional alignment). Key behaviors:

See also: [Schema Inference: behavior and limits](#schema-inference-behavior-and-limits).

- Alignment: name-based mapping from JSON keys to schema fields.
- Missing keys: with an explicit schema, missing fields become NULL; without a schema, only keys seen during inference are included (later unseen keys are ignored).
- Extra keys: keys not present in the schema are ignored (no error, no column).
- Types: JSON tokens (number/string/bool) are preserved; strings are not auto-cast to numeric. The schema enforces the target Arrow types.
- Nested data: supports `Struct`, `List`, `Map` (CSV cannot express nested types).
- Delimiters: NDJSON is newline-delimited; there is no delimiter option.

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
```

#### Self-Describing Formats (Parquet/Avro/Arrow)

These formats embed schemas in file metadata, but file versions may differ over time. DataFusion merges them automatically, but you may need to normalize for downstream operations.

```rust
use datafusion::arrow::datatypes::DataType;

// Read files with potentially different schemas
let df = ctx.read_parquet(
    vec!["v1.parquet", "v2.parquet"],
    ParquetReadOptions::default()
).await?;

// Normalize to canonical schema (widen types to prevent data loss)
let df = df.select(vec![
    col("id").cast_to(&DataType::Int64, df.schema())?.alias("id"),
    col("name"),
    col("amount").cast_to(&DataType::Decimal128(38, 9), df.schema())?.alias("amount"),
])?;
```

**Why normalize**: Joins and unions require exact type matches. Different file versions may have evolved schemas (e.g., Int32 → Int64).

#### Partitioned Datasets (ListingTable)

For Hive-style partitioned data, use ListingTable for partition pruning.

```rust
use std::sync::Arc;
use datafusion::arrow::datatypes::DataType;
use datafusion::datasource::file_format::csv::CsvFormat;
use datafusion::datasource::listing::{ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl};

let listing_options = ListingOptions::new(Arc::new(CsvFormat::default()))
    .with_file_extension("csv")
    .with_schema(schema.clone())
    .with_table_partition_cols(vec![("year".into(), DataType::Int32)]);

let config = ListingTableConfig::new(ListingTableUrl::parse("/data/events")?)
    .with_listing_options(listing_options)
    .with_schema(schema);

let table = Arc::new(ListingTable::try_new(config)?);
let df = ctx.read_table(table)?; // WHERE year=2024 skips other years
```

### Modeling Complex and Nested Types

Arrow's rich type system models real-world structures precisely, especially for JSON/Parquet.

```rust
use std::sync::Arc;
use datafusion::arrow::datatypes::{DataType, Field, Schema};

// Struct: grouped fields (like a record/object)
let metadata_type = DataType::Struct(vec![
    Field::new("source", DataType::Utf8, true),
    Field::new("version", DataType::Int32, true),
]);

// List: variable-length array
let tags_type = DataType::List(
    Arc::new(Field::new("tag", DataType::Utf8, true))
);

// Map: key-value pairs (represented as List<Struct<key,value>>)
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

// FixedSizeList: arrays with known length
let coords_type = DataType::FixedSizeList(
    Arc::new(Field::new("coord", DataType::Float64, false)),
    3  // Always 3 elements
);

let schema = Arc::new(Schema::new(vec![
    Field::new("metadata", metadata_type, true),
    Field::new("tags", tags_type, true),
    Field::new("attributes", attributes_type, true),
    Field::new("position", coords_type, false),
]));
```

**Type selection guidelines:**

- **Standard types** (`Utf8`, `Binary`, `List`): Use for typical data (<2GB per value)
- **Large variants** (`LargeUtf8`, `LargeBinary`, `LargeList`): Only when values may exceed 2GB (64-bit offsets)
- **Maps**: No automatic key uniqueness enforcement—handle duplicates in your pipeline
- **Union types**: Less common; verify end-to-end support before adoption

### Handling Null Values and Defaults

Schemas don't encode defaults—apply them after reading:

```rust
use datafusion::functions::expr_fn::coalesce;

// Replace NULLs with defaults
let df = df.with_column("name",
    coalesce(vec![col("name"), lit("unknown")])
)?;

// Conditional defaults
let df = df.with_column("status",
    when(col("status").is_null(), lit("pending"))
        .otherwise(col("status"))?
)?;
```

<!-- TODO: Add link to `coalesce`, `when`, `otherwise` function docs -->

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

### Schema Inference vs. Explicit Schemas

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
options.schema_infer_max_records = 10_000;  // Default is typically 1000
let df = ctx.read_csv("data.csv", options).await?;
```

> **Warning:** Schema inference can drift as data evolves. A column that starts as integers may later contain decimals, causing runtime errors. Always validate inferred schemas before deploying to production.

<!-- TODO: Add link for `with_schema_infer_max_records` -->

### Schema Reuse and Versioning

**Centralize schema definitions:**

Keep canonical schemas in a dedicated module for consistency across your codebase:

```rust
// schemas.rs
use std::sync::Arc;
use datafusion::arrow::datatypes::{DataType, Field, Schema};

pub fn customer_schema_v1() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("email", DataType::Utf8, true),
    ]))
}

pub fn customer_schema_v2() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("email", DataType::Utf8, true),
        Field::new("created_at", DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())), false),
    ]))
}
```

**Version tracking best practices:**

- Bump schema versions when adding/removing/changing fields
- Store version metadata in your schema or as a separate constant
- Use tests to validate that DataFrames match expected schema versions
- Document breaking changes in your schema evolution log

**Custom TableProvider Contract:**

If implementing a custom `TableProvider`, your `schema()` method is DataFusion's single source of truth:

```rust
impl TableProvider for MyCustomSource {
    fn schema(&self) -> SchemaRef {
        // Return a stable SchemaRef across all scans
        // Changing this between scans can break unions/joins
        self.schema.clone()
    }
}
```

Keep the returned `SchemaRef` stable so optimizers and unions behave predictably.

### Type Control with Macros and Literals

The `dataframe!` macro infers types from Rust literals, which may not always match your target schema:

```rust
// dataframe! may infer integers as Int32 by default.
let df_tmp = dataframe!(
    "id" => [1, 2],      // Inferred as Int32
    "name" => ["A", "B"] // Inferred as Utf8
)?;

// Normalize to target types explicitly:
let df_tmp = df_tmp.with_column(
    "id",
    col("id").cast_to(&DataType::Int64, df_tmp.schema())?
)?;

// Or use typed arrays directly:
use datafusion::arrow::array::Int64Array;
let df_typed = dataframe!(
    "id" => Int64Array::from(vec![1_i64, 2_i64]),
    "name" => ["A", "B"]
)?;
```

<!-- TODO: verify default integer inference for dataframe! -->

<!-- TODO: Add reference links: [Schema], [Field], [DataType], [TimeUnit], [SessionContext], [CsvReadOptions], [JsonReadOptions], [NdJsonReadOptions], [ParquetReadOptions], [coalesce] -->

<!-- TODO: This "Root Cause" section should come AFTER understanding the type system and mismatch types -->

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

Its power comes from two capabilities SQL `UNION`/`UNION ALL` don’t combine:

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
    "id" => [3, 4], //dataframe! returns int32 by default
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

     // +----+-------+-----------+
     // | id | name  | email     |
     // +----+-------+-----------+
     // | 1  | Alice | NULL      |
     // | 2  | Bob   | NULL      |
     // | 3  | Carol | c@ex.com  |
     // | 4  | Dave  | d@ex.com  |
     // +----+-------+-----------+
```

<!-- TODO: Verify the default integer type returned by `dataframe!` macro and update the inline comment in the example if needed. -->
<!-- TODO: Link to API docs for `with_column`, `cast_to`, `col`, `lit`. -->
<!-- TODO: Fix wording "Best Practise" → "Best Practice"; convert to Sphinx admonition (e.g., .. tip::). -->

> **Best Practise - "The Golden Rule of Type Casting"- :**<br>
> To prevent data loss, you must **always cast the narrower type up to the widest common type.** For example, cast `Int32` up to `Int64`. This "widening conversion" is always safe.<br> > **Never cast** a wider type down to a narrower one (e.g., `Int64` -> `Int32`) unless you have explicitly proven that no values will be truncated. Doing so risks silent data corruption.

---

<!-- TODO: Add more examples showing the power of select() for complex transformations -->
<!-- TODO: Show how to handle nullable vs non-nullable mismatches -->

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
| What it does | Matches by name; order ignored; missing columns filled with `NULL`   | Matches by position; errors if shape/order/types don’t match exactly      |

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
*/
```

**Strategic guidance: choosing the right ingestion pattern**

| Aspect      | Automatic Batch Read (this section)                                    | Iterative DataFrame Union ([`.union_by_name()`])                       |
| ----------- | ---------------------------------------------------------------------- | ---------------------------------------------------------------------- |
| Best for    | Batch loading a full, known set of files/partitions at once            | Incremental pipelines where files arrive one by one                    |
| Control     | Automatic; merge rules are built-in                                    | Full control; inject casts/renames/defaults per file via [`.select()`] |
| Performance | Often higher: planner can optimize the entire multi-file scan together | Flexible; per-file work may add overhead but enables custom logic      |

<!-- TODO: Convert this HTML-styled warning into a Sphinx admonition (`.. warning::`) for consistency with the docs. -->

> **⚠️"A Critical Note on CSVs: Always Provide an Explicit Schema"**<br>
> Schema inference for CSV files ([`infer_schema_max_records`]) is a common source of drift. A single "dirty" row outside the initial scan can change the inferred type between reads. <br> **In production, always provide an explicit schema**:

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

   - **Fix**: Use `.alias()` within a `.select()` expression to standardize names.

4. **Column Type Mismatch**: Are the types incompatible?
   - **Fix**: Use `.cast_to()` to unify types, always casting up to the widest common type to prevent data loss.

<!-- TODO: Add anti-patterns section: "What NOT to do" -->
<!-- TODO: Add performance implications of different approaches -->

### High-Level Best Practices

- **At Write Time**: Enforce consistent schemas at the source if possible. An ounce of prevention is worth a pound of cure.
- **At Read Time**: Prefer `.union_by_name()` for its resilience against common schema evolution problems.
- **In Pipelines**: Validate schemas at critical steps. Fail fast if a schema deviates unexpectedly.
- **For Debugging**: Inspect schemas liberally. `println!("{:#?}", df.schema())` is your best friend.

> **Performance Note**
>
> Schema operations like `.select()` and `.union_by_name()` are metadata-only and essentially free. However, `.cast_to()` is a full data transformation and has a performance cost proportional to your dataset size.

<!-- TODO: Verify performance claims and add a citation/benchmark link or remove "essentially free" wording if inaccurate. -->

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

---

<!--TODO; Execution section -->
