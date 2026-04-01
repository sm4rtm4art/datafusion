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

# Type Coercion: Auto-Alignment vs Explicit Casting

<!-- TODO: ABSTRACT — written last -->

:::{admonition} Style Note
:class: note
:collapsible: closed

In this document, code elements follow a consistent pattern:

- **DataFrame methods:** `df.method()` (e.g., `df.select(...)`, `df.filter(...)`)
- **DFSchema instance methods:** `df.schema().method()` (e.g., `df.schema().fields()`)
- **DFSchema associated functions:** `DFSchema::function()` (e.g., `DFSchema::try_from(...)`)
- **Standalone functions:** `function()` (e.g., `col(...)`, `lit(...)`, `cast(...)`)
- **Constructors:** `Type::new()` (e.g., `SessionContext::new()`)
- **Types:** `TypeName` (e.g., `SchemaRef`, `RecordBatch`)
- **Lazy transformations:** return a `DataFrame` and build the `LogicalPlan`
- **Actions:** (`.collect()`, `.show()`) trigger execution

:::

```{contents} Type Coercion: Auto-Alignment vs Explicit Casting
:local:
:depth: 2
```

## Arrow Data Types in DataFusion

**The Arrow [`DataType`] is the foundation of every column — determining storage layout, kernel selection, and every implicit coercion decision the query engine makes.**

DataFusion operates on Apache Arrow's columnar memory format. Every [`DataFrame`] column carries an Arrow [`Field`], and each [`Field`] declares a [`DataType`] that determines how the underlying bytes are interpreted. The same raw value (`1735689600_i64`) becomes a date (`2025-01-01T00:00:00`) when the field declares `DataType::Timestamp` instead of `DataType::Int64` — unlocking temporal arithmetic, date-range pruning, and timezone-aware comparisons that an integer type cannot provide. The [`DataType`] also governs coercion: when two columns of different types meet in an expression or set operation, the [`TypeCoercion`] analyzer consults the [coercion hierarchy](#the-coercion-hierarchy) to determine whether a safe widening path exists.

The table below lists the common Arrow data types encountered in DataFusion. Each entry corresponds to a variant of the [`DataType`] enum (e.g., `DataType::Int32`, `DataType::Utf8`).

| Category           | Arrow Types                                                                | Example Values                        |           Common Use Cases            |
| :----------------- | :------------------------------------------------------------------------- | :------------------------------------ | :-----------------------------------: |
| **Integers**       | `Int8`, `Int16`, `Int32`, `Int64`<br>`UInt8`, `UInt16`, `UInt32`, `UInt64` | `42`, `-100`, `0`                     |        IDs, counts, quantities        |
| **Floating-Point** | `Float16`, `Float32`, `Float64`                                            | `3.14`, `-0.001`                      | Measurements, scientific data, ratios |
| **Decimal**        | `Decimal128(precision, scale)`                                             | `99.99`, `1234.5678`                  |   Financial data, currency, prices    |
| **Strings**        | `Utf8`, `LargeUtf8`, `Utf8View`                                            | `"hello"`, `"データ融合"`             |    Names, descriptions, categories    |
| **Temporal**       | `Date32`, `Date64`<br>`Timestamp(unit, tz)`, `Duration`, `Interval`        | `2024-01-15`<br>`2024-01-15 14:30:00` |     Event times, dates, durations     |
| **Boolean**        | `Boolean`                                                                  | `true`, `false`                       |           Flags, conditions           |
| **Binary**         | `Binary`, `LargeBinary`, `BinaryView`                                      | `[0x12, 0x34]`                        |           Raw data, hashes            |
| **Nested Types**   | `Struct(Fields)`, `List(Field)`, `Map`                                     | `{"a": 1}`, `[1, 2, 3]`               |  JSON/Parquet data, complex objects   |

Beyond primitive types, DataFusion fully supports Arrow's nested types (`List`, `Struct`, `Map`, `Union`) for complex data structures common in Parquet and semi-structured sources. For details on nested type structures and per-field properties, see [Anatomy of a Schema — Data Type](anatomy-schema.md#data-type).

:::{admonition} DataFrame API types are a superset of SQL types
:class: note
The DataFrame API works directly with Arrow [`DataType`] enum variants, which is a superset of the types available through SQL syntax. Types like `Utf8View`, `BinaryView`, `Float16`, and `Duration` have no SQL literal syntax but are fully usable through the DataFrame API via `cast()` and `DataType::` constructors.
:::

:::{admonition} Type references
:class: seealso

- [SQL Data Types](../../sql/data_types.md) — SQL-to-Arrow type mapping, `arrow_typeof()`, and `arrow_cast()` functions
- [Apache Arrow Data Types][arrow data types] — complete Arrow type system, memory layouts, and encoding details
  :::

## Type Coercion

**The [`TypeCoercion`] analyzer — running between plan construction and optimization — inserts implicit widening casts that promote narrower types to wider ones without data loss, following the principle: always widen, never narrow.**

Type coercion is the automatic conversion of one data type to another to make an operation valid. DataFusion's [`TypeCoercion`] analyzer rule inspects every node in the [`LogicalPlan`] and inserts `CAST` expressions where a safe widening path exists — you don't write `cast()` by hand for `DataType::Int32 + DataType::Int64`. Narrowing (e.g., `Float64` to `Int32`) is never performed implicitly because it risks silent data loss.

The [`TypeCoercion`] analyzer runs as part of the **analysis phase** — after the [`LogicalPlan`] is constructed but before the optimizer rewrites the plan. This means the schema you inspect via `df.schema()` reflects the pre-analysis state; types only settle to their final coerced form when execution is triggered (`.collect()`, `.show()`). The coercion applies to both expressions (arithmetic, comparisons, filters) and set operations (unions, intersections, exceptions), but the failure behavior differs: expressions fail on the specific incompatible operation, while set operations fail when any column pair across the entire schema has no safe coercion path.

### The Coercion Hierarchy

**The hierarchy defines every safe widening path — arrows always point toward the wider type, and no coercion ever narrows.**

The diagram below shows the safe upcasting paths that the [`TypeCoercion`] analyzer uses to resolve type mismatches. Each arrow represents an implicit cast that preserves data without loss — "safe" means no information loss and no runtime error. When an operation mixes two types connected by an arrow, the analyzer automatically inserts a `CAST` from the narrower type to the wider one.

Types in **different boxes have no implicit coercion path** between them. `Boolean` and `Int32`, for example, belong to separate families with no connecting arrow — mixing them requires an explicit `cast()`. When no path exists, expressions fail with a coercion error and set operations are rejected during analysis.

```text
┌ Numeric Widening ───────────────────────────────────────────────┐
│                                                                 │
│ Signed:   Int8  ──► Int16  ──► Int32  ──► Int64 ───┐            │
│                                                    │            │
│ Unsigned: UInt8 ──► UInt16 ──► UInt32 ──► UInt64 ──┤            │
│                                                    │            │
│ Signed ∩ Unsigned: UInt64 + Int64 ──► Decimal128   │            │
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

┌── Variable Width (coercion is one-way toward view types) ───────┐
│                                                                 │
│   Strings:  Utf8  ──► LargeUtf8  ──► Utf8View                   │
│                                                                 │
│   Binary:   Binary ──► LargeBinary ──► BinaryView               │
│                                                                 │
│   (StringArray → StringViewArray is cheap; the reverse requires │
│    physical memory allocation — so the engine always coerces    │
│    toward the view type)                                        │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

:::{admonition} Decimal128 → Float64 trades precision for range
:class: caution
`Decimal128` provides exact arithmetic (up to 38 digits), while `Float64` offers only ~15–17 significant digits. For financial or high-precision data, prefer keeping values as `Decimal128` and only convert to `Float64` when approximate results are acceptable (e.g., charting, statistical aggregates).
:::

### Data Type Interaction Rules

**Each type family follows its own coercion rules — and the rules differ between arithmetic and comparison operators.**

Arithmetic operators (`+`, `-`, `*`, `/`) are strictly numeric: both operands must be numeric types, and the result follows the numeric widening hierarchy. Comparison operators (`=`, `>`, `<`, `!=`) are broader: they accept cross-family pairs like string vs. numeric, resolving them via type-specific coercion paths. The rules below apply per type family.

- **Numeric:** Integers widen to the smallest container that holds both ranges (`DataType::Int32 + DataType::Int64 → Int64`). Mixed with floats, the result is `Float64`. Decimals preserve precision when combined with integers. **Signed + unsigned mixing** uses `Decimal128` when neither integer type can hold the other's full range — `UInt64 + Int64` produces `Decimal128(20, 0)` because `Int64` cannot represent `u64::MAX` and `UInt64` cannot represent negative values.

- **Temporal:** Dates promote to `Timestamp` for comparisons and arithmetic. Timezones must match — cast explicitly to align them. `Date64` is rarely used; dates typically coerce directly to `Timestamp(Nanosecond)`.

- **Strings:** `Utf8`, `LargeUtf8`, and `Utf8View` coerce toward the view type (`Utf8View`) because `StringArray → StringViewArray` is cheap while the reverse requires memory allocation. In **comparison operators**, a string column mixed with a numeric column coerces both to the string type (e.g., `col("name").gt(lit(42))` compares as strings). In **arithmetic**, string columns are rejected — only numeric types are valid.

- **Boolean:** Does not auto-coerce to numeric or any other type family. Use explicit `cast(col("flag"), DataType::Int32)` when needed.

- **NULL:** Adopts the other operand's type in expressions — this is safe widening. When both operands are `NULL`, the type defaults to `Utf8View`.

:::{admonition} Comparison coercion has two variants
:class: caution
Binary comparison operators (`col("x").gt(col("y"))`) use `comparison_coercion()`, which coerces string + numeric → **string**. Some scalar functions with `Comparable` signatures (e.g., `nullif`) use `comparison_coercion_numeric()`, which coerces string + numeric → **numeric**. When debugging unexpected comparison results, check which coercion path the operation uses.
:::

### Automatic Widening in Expressions

**Expressions auto-widen to the common, wider type — `Int32 + Int64` produces `Int64` without an explicit cast.**

DataFusion automatically promotes types to a common, wider type when they are mixed within an expression. This applies to methods like [`.select()`], [`.with_column()`], and [`.filter()`]. The promotion always follows the safe upcasting paths defined in the [coercion hierarchy](#the-coercion-hierarchy) to prevent data loss.

```rust
use datafusion::prelude::*;
use datafusion::assert_batches_eq;
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

    // Execute eagerly and verify the computed values
    let batches = result.collect().await?;
    assert_batches_eq!(
        &[
            "+-----+",
            "| sum |",
            "+-----+",
            "| 101 |",  // 1 + 100
            "| 202 |",  // 2 + 200
            "+-----+",
        ],
        &batches
    );

    Ok(())
}
```

### Coercion in Set Operations

**Set operations coerce compatible types automatically — `Int32` union `Int64` widens to `Int64` just like expressions — but fail when no safe coercion path exists between column pairs.**

Set operations like [`.union()`], [`.except()`], and [`.intersect()`] match columns by position and apply the same [`TypeCoercion`] analyzer to find a common type for each column pair. When the types are compatible (connected in the [coercion hierarchy](#the-coercion-hierarchy)), the narrower type is widened automatically. When no safe path exists — for example, `Boolean` and `Int32` have no common type — the query fails during analysis rather than producing wrong results.

```rust
use datafusion::prelude::*;
use datafusion::assert_batches_sorted_eq;
use datafusion::arrow::datatypes::DataType;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    // Two DataFrames with compatible but different integer types
    let orders_v1 = dataframe!("order_id" => [1_i32, 2_i32])?;
    let orders_v2 = dataframe!("order_id" => [3_i64, 4_i64])?;

    // Union succeeds — TypeCoercion widens Int32 to Int64
    let combined = orders_v1.union(orders_v2)?;

    // Execute eagerly and verify all four rows are present
    let batches = combined.collect().await?;
    assert_batches_sorted_eq!(
        &[
            "+----------+",
            "| order_id |",
            "+----------+",
            "| 1        |",
            "| 2        |",
            "| 3        |",
            "| 4        |",
            "+----------+",
        ],
        &batches
    );

    Ok(())
}
```

When the types are incompatible, the [`TypeCoercion`] analyzer rejects the plan:

```rust,no_run
use datafusion::prelude::*;

// Boolean and Int32 have no common type — this fails during analysis:
// "Incompatible inputs for Union: Previous inputs were of type Boolean,
//  but got incompatible type Int32 on column 'flag'"
let flags    = dataframe!("flag" => [true, false]).unwrap();
let counters = dataframe!("flag" => [1_i32, 0_i32]).unwrap();
let combined = flags.union(counters).unwrap();
// combined.collect().await fails — no safe coercion path
```

When no automatic coercion path exists, use `cast()` to align types explicitly before the set operation. See [Explicit Casting](#explicit-casting) below.

:::{admonition} Join keys are auto-coerced
:class: note
Join keys are an exception to strict positional matching — DataFusion automatically coerces join keys to a common type (e.g., `Int32 = Int64` becomes `Int64 = Int64`). This happens transparently via the [`TypeCoercion`] analyzer rule, so you rarely need to cast join keys manually.
:::

### Literal Coercion

**Scalar literals in expressions adopt the type required by the context — `lit(42_i32)` compared against an `Int64` column widens to `Int64` automatically.**

When a literal value is used in an expression alongside a typed column, the [`TypeCoercion`] analyzer coerces the literal to match the column's type (or their common wider type). This is the same widening principle at work — `lit(42_i32)` compared against an `Int64` column becomes an `Int64` value, and the string literal `"2024-01-15"` compared against a `Date32` column is parsed as a date.

```rust
use datafusion::prelude::*;
use datafusion::assert_batches_eq;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let df = dataframe!(
        "sensor_id" => [1_i64, 2_i64, 3_i64],
        "temperature" => [22.5, 38.1, 19.8]
    )?;

    // lit(30) is i32 by default, but temperature is Float64 —
    // the TypeCoercion analyzer widens 30 to Float64 for the comparison
    let hot_sensors = df.filter(col("temperature").gt(lit(30)))?;

    let batches = hot_sensors.collect().await?;
    assert_batches_eq!(
        &[
            "+-----------+-------------+",
            "| sensor_id | temperature |",
            "+-----------+-------------+",
            "| 2         | 38.1        |",
            "+-----------+-------------+",
        ],
        &batches
    );

    Ok(())
}
```

:::{admonition} String literals and temporal columns
:class: note
String literals compared against temporal columns (`Date32`, `Timestamp`) are parsed as the corresponding temporal type. This enables `col("event_date").gt(lit("2024-01-15"))` without an explicit cast — the string `"2024-01-15"` is interpreted as a `Date32` value.
:::

## Explicit Casting

**When automatic coercion cannot resolve a type mismatch — or when you want to control the target type — `cast()` and `try_cast()` provide explicit conversion.**

Automatic coercion covers safe widenings within the [coercion hierarchy](#the-coercion-hierarchy), but some conversions require explicit action: incompatible types in set operations, narrowing conversions (e.g., `Float64` to `Int32`), or cross-family conversions (e.g., `Boolean` to `Int32`). The `cast()` and `try_cast()` functions give you direct control over these conversions.

### cast() — Hard Cast

**`cast()` converts a column to the target type and fails the query if any value cannot be converted.**

The `cast()` function creates a `CAST` expression in the [`LogicalPlan`]. If a value cannot be represented in the target type, the query fails at execution time. Use `cast()` when you know the conversion is safe and want a hard failure on unexpected values.

```rust
use datafusion::prelude::*;
use datafusion::assert_batches_sorted_eq;
use datafusion::arrow::datatypes::DataType;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let flags = dataframe!("flag" => [true, false, true])?;
    let counts = dataframe!("flag" => [0_i32, 1_i32, 0_i32])?;

    // Boolean and Int32 have no automatic coercion path —
    // cast Boolean to Int32 explicitly before the union
    let flags_as_int = flags.with_column(
        "flag",
        cast(col("flag"), DataType::Int32)
    )?;

    let combined = flags_as_int.union(counts)?;

    let batches = combined.collect().await?;
    assert_batches_sorted_eq!(
        &[
            "+------+",
            "| flag |",
            "+------+",
            "| 0    |",
            "| 0    |",
            "| 1    |",
            "| 1    |",
            "| 1    |",
            "| 0    |",
            "+------+",
        ],
        &batches
    );

    Ok(())
}
```

### try_cast() — Soft Cast

**`try_cast()` converts values to the target type but returns `NULL` instead of failing when a value cannot be converted.**

Use `try_cast()` when data quality is uncertain and you prefer `NULL` over a query failure. This is particularly useful for user-provided data, mixed-format columns, or ETL pipelines where partial results are preferable to hard errors.

```rust
use datafusion::prelude::*;
use datafusion::assert_batches_eq;
use datafusion::arrow::datatypes::DataType;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let df = dataframe!(
        "value" => ["42", "not_a_number", "100"]
    )?;

    // try_cast returns NULL for "not_a_number" instead of failing
    let result = df.select(vec![
        try_cast(col("value"), DataType::Int64).alias("parsed")
    ])?;

    let batches = result.collect().await?;
    assert_batches_eq!(
        &[
            "+--------+",
            "| parsed |",
            "+--------+",
            "| 42     |",
            "|        |",
            "| 100    |",
            "+--------+",
        ],
        &batches
    );

    Ok(())
}
```

:::{admonition} SQL equivalents
:class: note
In DataFusion SQL, `CAST(col AS type)` corresponds to `cast()`, and `TRY_CAST(col AS type)` corresponds to `try_cast()`. The `arrow_cast()` SQL function provides Arrow-specific casting with full type syntax (e.g., `arrow_cast(col, 'Timestamp(Second, None)')`), and `arrow_typeof()` returns the Arrow type of any expression — useful for debugging coercion behavior. See [SQL Data Types](../../sql/data_types.md) for details.
:::

## Conclusion & Further Reading

**Type coercion bridges the gap between mixed-type data and the query engine's need for uniform types — widening automatically where safe, requiring explicit casts where no safe path exists.**

The Arrow data type determines how every column is stored and computed. DataFusion's [`TypeCoercion`] analyzer inserts implicit widening casts in both expressions and set operations, following the coercion hierarchy toward the wider type. When no safe path exists, `cast()` and `try_cast()` provide explicit control — with hard failure or `NULL` fallback, respectively.

:::{admonition} Next steps
:class: seealso

- [Anatomy of a Schema](anatomy-schema.md) — per-column field properties (`name`, `data_type`, `nullable`, `metadata`)
- [Inspecting and Validating Schemas](inspecting-and-validating.md) — display, access, and programmatic field inspection
- [Schema Transformation](schema-transformation.md) — qualifier manipulation, combining schemas, nullability handling
- [DataFrame Methods](dataframe-methods.md) — methods that change the schema (`.with_column()`, `.with_column_renamed()`)
- [Handling Null Values](../Concepts/null-handling.md) — NULL behavior in expressions, filters, and joins
  :::

<!-- Link references -->

[`DataFrame`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html
[`DFSchema`]: https://docs.rs/datafusion/latest/datafusion/common/dfschema/struct.DFSchema.html
[`DataType`]: https://docs.rs/arrow/latest/arrow/datatypes/enum.DataType.html
[`Field`]: https://docs.rs/arrow/latest/arrow/datatypes/struct.Field.html
[`TypeCoercion`]: https://docs.rs/datafusion/latest/datafusion/optimizer/analyzer/type_coercion/struct.TypeCoercion.html
[`LogicalPlan`]: https://docs.rs/datafusion/latest/datafusion/logical_expr/enum.LogicalPlan.html
[arrow data types]: https://arrow.apache.org/docs/format/Columnar.html#data-type-descriptions
[`.select()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.select
[`.with_column()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.with_column
[`.filter()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.filter
[`.union()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.union
[`.except()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.except
[`.intersect()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.intersect
