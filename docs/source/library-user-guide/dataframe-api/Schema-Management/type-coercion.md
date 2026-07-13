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

**DataFusion resolves most type mismatches automatically at plan time — and gives you [`cast()`] and [`try_cast()`] for the rest.**

Real-world data arrives in mixed types — integers alongside floats, dates as strings, booleans where counters are expected. The [`TypeCoercion`] analyzer widens operands within the same type family and parses literals to match their context, all before any data flows. When no safe automatic path exists, explicit casts provide full control. This guide explains the coercion hierarchy, how the analyzer applies it in expressions and set operations, and when to reach for explicit casts.

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

**The Arrow [`DataType`] is the foundation of every value in DataFusion — determining how bytes are stored, which kernels operate on them, and how the [`TypeCoercion`] analyzer resolves mismatches.**

DataFusion operates on Apache Arrow's columnar memory format. Every [`DataFrame`] column carries an Arrow [`Field`], and each [`Field`] declares a [`DataType`] that determines how the underlying bytes are interpreted. The same raw value (`1735689600_i64`) becomes a date (`2025-01-01T00:00:00`) when the field declares `DataType::Timestamp` instead of `DataType::Int64` — unlocking temporal arithmetic, date-range pruning, and timezone-aware comparisons that an integer type cannot provide.

The table below lists the common Arrow data types encountered in DataFusion. Each entry corresponds to a variant of the [`DataType`] enum (e.g., `DataType::Int32`, `DataType::Utf8`). For the complete SQL-to-Arrow type mapping, see [SQL Data Types](../../sql/data_types.md).

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

Beyond primitive types, DataFusion fully supports Arrow's nested types (`List`, `Struct`, `Map`, `Union`) for complex data structures common in Parquet and semi-structured sources. For details on nested type structures and per-field properties, see [Anatomy of a Schema — Data Type](schema-anatomy.md#data-type).

:::{admonition} DataFrame API types are a superset of SQL types
:class: note
The DataFrame API works directly with Arrow [`DataType`] enum variants, which is a superset of the types available through SQL syntax. Types like `Utf8View`, `BinaryView`, `Float16`, and `Duration` have no SQL literal syntax but are fully usable through the DataFrame API via [`cast()`] and `DataType::` constructors.
:::

:::{admonition} Type references
:class: seealso
[Apache Arrow Data Types][arrow data types] — complete Arrow type system, memory layouts, and encoding details
:::

## Type Coercion

**The [`TypeCoercion`] analyzer inserts implicit widening casts between plan construction and optimization — always widening to the broader type within the same family, never narrowing, and rejecting incompatible types outright.**

Type coercion is the automatic conversion of one data type to another to make an operation valid. The [`TypeCoercion`] analyzer rule walks every node in the [`LogicalPlan`] and inserts `CAST` expressions where a safe widening path exists — you don't write [`cast()`] by hand for `DataType::Int32 + DataType::Int64`. Narrowing (e.g., `Float64` to `Int32`) is never performed implicitly because it risks silent data loss. Coercion is a one-time **plan rewrite**, not a per-row runtime operation.

```text
df.select(...)                                              df.collect()
      │                                                          │
      ▼                                                          ▼
┌─────────────┐   ┌──────────┐   ┌───────────┐   ┌──────────────┐   ┌───────────┐
│ LogicalPlan │──▶│ Analyzer │──▶│ Optimizer │──▶│ PhysicalPlan │──▶│ Execution │
└─────────────┘   └──────────┘   └───────────┘   └──────────────┘   └───────────┘
                       │
                       └── TypeCoercion inserts CAST nodes here
```

The [`TypeCoercion`] analyzer runs during [`SessionState`]`.optimize()` — the first step when [`.collect()`] or [`.show()`] triggers execution. The schema you inspect via [`.schema()`] reflects the pre-analysis state; to observe the coerced types — including the inserted `CAST` nodes — call [`.explain()`]`(false, false)`.

The coercion applies to both expressions (arithmetic, comparisons, filters) and set operations (unions, intersections, exceptions), but the failure behavior differs: expressions fail on the specific incompatible operation, while set operations fail when any column at the same ordinal position has no safe coercion path. In both cases, an explicit [`cast()`] is required to bridge incompatible types — see [Explicit Casting](#explicit-casting).

### The Coercion Hierarchy

**The hierarchy defines every safe widening path — arrows always point toward the wider type, and no coercion ever narrows.**

The diagram below shows the safe upcasting paths that the [`TypeCoercion`] analyzer uses to resolve type mismatches. Each arrow represents an implicit cast that preserves data without loss — "safe" means no information loss and no runtime error. When an operation mixes two types connected by an arrow, the analyzer automatically inserts a `CAST` from the narrower type to the wider one.

Types in **different boxes have no implicit coercion path** between them. `Boolean` and `Int32`, for example, belong to separate families with no connecting arrow — mixing them requires an explicit [`cast()`]. When no path exists, expressions fail with a coercion error and set operations are rejected during analysis.

```text
┌ Numeric Widening ───────────────────────────────────────────────┐
│                                                                 │
│ Signed:   Int8  ──► Int16  ──► Int32  ──► Int64 ───┐            │
│                                                    │            │
│ Unsigned: UInt8 ──► UInt16 ──► UInt32 ──► UInt64 ──┤            │
│                                                    │            │
│ Signed ∩ Unsigned: UInt64 + Int64 ──► Decimal128   │            │
│                                       (Precision)  ▼            │
│                                       Decimal128 ──┤            │
│                                                    │            │
│ Floats:             Float16 ──► Float32 ──► Float64             │
│                                                                 │
│ Decimal + Float:    Decimal128 + Float__ ──► Float64  (lossy!)  │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘

┌ Temporal Widening ──────────────────────────────────────────────┐
│                                                                 │
│ Dates:      Date32 ──► Date64                                   │
│             Date32/Date64 + Timestamp ──► Timestamp(ns)         │
│                                                                 │
│ Timezones:  Comparisons coerce to the left-hand timezone.       │
│             Arithmetic requires matching timezones — cast first. │
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
When a `Decimal128` value is mixed with a `Float` type, the result widens to `Float64`. Two `Decimal128` values stay as `Decimal128` — the lossy conversion only happens when mixing families. `Decimal128` provides exact arithmetic (up to 38 digits), while `Float64` offers only ~15–17 significant digits. For financial or high-precision data, prefer keeping values as `Decimal128` and only convert to `Float64` when approximate results are acceptable (e.g., charting, statistical aggregates).
:::

### Data Type Interaction Rules

**Each type family follows its own coercion rules — and the rules differ between arithmetic and comparison operators.**

The hierarchy above defines *which* types widen, but the *operator* determines which coercion paths are available. Arithmetic operators (`+`, `-`, `*`, `/`) restrict to numeric families — both operands must be numeric, and the result follows the numeric widening hierarchy. Comparison operators (`=`, `>`, `<`, `!=`) are broader: they accept cross-family pairs like string vs. numeric and resolve them via type-specific coercion paths.

#### Numeric
Widens to the smallest type that holds both ranges.

- `Int32 + Int64` → `Int64`
- `Int64 + Float32` → `Float32` — float takes precedence when mixed with integers
- `UInt64 + Int64` → `Decimal128(20, 0)` — neither type can hold the other's full range

#### Temporal
Dates promote to `Timestamp` when mixed with temporal types.

- `Date32 + Timestamp(ns)` → `Timestamp(ns)`
- Comparisons coerce to the left-hand timezone (non-strict matching).
- Arithmetic requires matching timezones (UTC and `+00:00` are equivalent) — cast explicitly to align.

#### Strings
Coercion favors view types (`StringArray → StringViewArray` is cheap O(1); the reverse allocates).

- `Utf8 + Utf8View` → `Utf8View`
- Comparisons: `Utf8 > Int32` → `Utf8` — lexicographic, so `"12" < "9"` is `true`.
- Arithmetic: `Utf8 + Int32` → **Error** — strings are rejected.

#### Boolean
Does not auto-coerce to any other type family.

- `Boolean + Int32` → **Error** — use `cast(col("flag"), DataType::Int32)`.

#### NULL
Adopts the other operand's type (safe widening).

- `NULL + Int32` → `Int32`
- `NULL + NULL` → `Int64` (arithmetic) — the engine assigns a concrete numeric type so the operation returns `NULL`.

:::{admonition} Comparison coercion has two variants
:class: caution
Binary comparison operators (`col("x").gt(col("y"))`) use [`comparison_coercion()`], which coerces string + numeric → **string**. Some scalar functions with `Comparable` signatures (e.g., `nullif`) use [`comparison_coercion_numeric()`], which coerces string + numeric → **numeric**. When debugging unexpected comparison results, check which coercion path the operation uses.
:::

The [`TypeCoercion`] analyzer applies these rules in three contexts — expressions, set operations, and literals — each described below.

### Automatic Widening in Expressions

**Mixed-type expressions resolve automatically — the [`TypeCoercion`] analyzer widens the narrower operand so arithmetic and filtering work without explicit conversion.**

Any method that adds an expression node to the [`LogicalPlan`] triggers the same widening logic. The analyzer finds the common wider type from the [coercion hierarchy](#the-coercion-hierarchy) and wraps the narrower operand in a `CAST`. If no safe path exists (e.g., `Boolean + Int32`), the plan is rejected during analysis rather than failing silently at execution time.

```rust
use datafusion::prelude::*;
use datafusion::arrow::datatypes::DataType;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
let df = dataframe!(
    "int32_col" => [1_i32, 2_i32],
    "int64_col" => [100_i64, 200_i64]
)?;

// Int32 + Int64 — the analyzer widens Int32 to Int64
let result = df.select(vec![
    (col("int32_col") + col("int64_col")).alias("sum")
])?;

// The result schema proves the type change: Int32 was widened to Int64
let field = result.schema().field_with_name(None, "sum")?;
assert_eq!(field.data_type(), &DataType::Int64);

// Output schema: { sum: Int64 }  (not Int32 — the analyzer widened it)
#     let batches = result.collect().await?;
#     use datafusion::assert_batches_eq;
#     assert_batches_eq!(
#         &[
#             "+-----+",
#             "| sum |",
#             "+-----+",
#             "| 101 |",
#             "| 202 |",
#             "+-----+",
#         ],
#         &batches
#     );
    Ok(())
}
```

:::{admonition} Inspecting implicit CASTs without executing
:class: tip
`df.explain(false, false)?.show().await?` reveals the `CAST` nodes the analyzer inserted — for example, `Projection: CAST(int32_col AS Int64) + int64_col AS sum`. This is the fastest way to verify coercion behavior without running the query against data.
:::

### Coercion in Set Operations

**Set operations must resolve every column at the same ordinal position — one incompatible pair rejects the entire plan.**

Unlike expressions, where coercion targets a single operand pair, set operations (union, except, intersect) align two full schemas column-by-column. The [`TypeCoercion`] analyzer widens each pair independently using the same [coercion hierarchy](#the-coercion-hierarchy). Compatible pairs (e.g., `Int32` and `Int64`) widen silently. If any column pair has no safe coercion path (e.g., `Boolean` and `Int32`), the entire plan is rejected during analysis — before any data flows. Use `cast()` to align types explicitly before the set operation — see [Explicit Casting](#explicit-casting).

```rust
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
// Boolean and Int32 have no common type
let flags    = dataframe!("flag" => [true, false])?;
let counters = dataframe!("flag" => [1_i32, 0_i32])?;

// union() builds the plan — the error surfaces during execution
// when the TypeCoercion analyzer rejects the incompatible pair
let combined = flags.union(counters)?;
let result = combined.collect().await;
assert!(result.is_err());
// Error: "Incompatible inputs for Union: Previous inputs were
//  of type Boolean, but got incompatible type Int32 on column 'flag'"
     Ok(())
}
```

:::{admonition} Join keys are auto-coerced
:class: note
Join keys are an exception to strict positional matching — DataFusion automatically coerces join keys to a common type (e.g., `Int32 = Int64` becomes `Int64 = Int64`). This happens transparently via the [`TypeCoercion`] analyzer rule, so you rarely need to cast join keys manually.
:::

### Literal Coercion

**Literals adopt the type of their context — the analyzer widens or parses them to match the column they interact with.**

The [`TypeCoercion`] analyzer resolves literal types the same way it resolves column types: by finding a common wider type. Rust's type inference determines the initial literal type (`lit(30)` → `Int32`, `lit(3.14)` → `Float64`), and the analyzer then widens the literal to match the column. This is particularly powerful for temporal columns: string literals like `"2024-01-15"` are automatically parsed as the corresponding temporal type when compared against `Date32` or `Timestamp` columns — no explicit cast needed.

```rust
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
let df = dataframe!(
    "sensor_id" => [1_i64, 2_i64, 3_i64],
    "temperature" => [22.5, 38.1, 19.8]
)?;

// lit(30) is Int32, but temperature is Float64 —
// the analyzer widens 30 to Float64 for the comparison
let hot_sensors = df.filter(col("temperature").gt(lit(30)))?;

// explain() confirms: Filter: temperature > CAST(Int32(30) AS Float64)
#     use datafusion::assert_batches_eq;
#     let batches = hot_sensors.collect().await?;
#     assert_batches_eq!(
#         &[
#             "+-----------+-------------+",
#             "| sensor_id | temperature |",
#             "+-----------+-------------+",
#             "| 2         | 38.1        |",
#             "+-----------+-------------+",
#         ],
#         &batches
#     );
    Ok(())
}
```

:::{admonition} Debugging coercion failures
:class: tip
When a query fails with a type coercion error, follow these steps:

1. **Inspect column types:** [`.schema()`] shows the pre-analysis types of every column.
2. **Check the plan:** [`.explain()`]`(false, false)?.show().await?` reveals which `CAST` nodes the analyzer inserted — and where it could not.
3. **Use `arrow_typeof()` in SQL:** `SELECT arrow_typeof(column) FROM table` returns the concrete Arrow type of any expression.
4. **Apply explicit casts:** Use [`cast()`] or [`try_cast()`] to bridge incompatible types before the failing operation.
   :::

## Explicit Casting

**[`cast()`] and [`try_cast()`] give explicit control over type conversion — use them when automatic coercion has no safe path or when you need a specific target type.**

Automatic coercion covers safe widenings within the [coercion hierarchy](#the-coercion-hierarchy), but some conversions require explicit action: incompatible types in set operations, narrowing conversions (e.g., `Float64` to `Int32`), or cross-family conversions (e.g., `Boolean` to `Int32`). The two functions differ in failure behavior: [`cast()`] fails the query on unconvertible values (hard cast), while [`try_cast()`] returns `NULL` instead (soft cast). Choose based on whether partial results are acceptable.

### Hard Cast — cast() 

**[`cast()`] converts a column to the target type and fails the query if any value cannot be converted.**

The [`cast()`] function creates a `CAST` expression in the [`LogicalPlan`] — like automatic coercion, it operates at plan level, not per-row. If a value cannot be represented in the target type, the query fails at execution time. Use [`cast()`] when the conversion is guaranteed safe and you want a hard failure on unexpected values: aligning incompatible types before a union, narrowing a wide type for storage (e.g., `Float64` → `Int32` when values are known to be integral), or converting across type families (e.g., `Boolean` → `Int32`).

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

### Soft Cast — try_cast() 

**[`try_cast()`] preserves partial results — unconvertible values become `NULL` instead of failing the query.**

Where [`cast()`] treats any conversion failure as fatal, [`try_cast()`] substitutes `NULL` and continues. This makes [`try_cast()`] the safer choice for ETL pipelines, user-provided data, or mixed-format columns where data quality is uncertain. The resulting `NULL` values propagate through downstream expressions following standard [null-handling rules](../Concepts/null-handling.md) — filter them with `.is_not_null()` or replace them with `coalesce()`.

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
In DataFusion SQL, `CAST(col AS type)` corresponds to [`cast()`], and `TRY_CAST(col AS type)` corresponds to [`try_cast()`]. The `arrow_cast()` SQL function provides Arrow-specific casting with full type syntax (e.g., `arrow_cast(col, 'Timestamp(Second, None)')`), and `arrow_typeof()` returns the Arrow type of any expression — useful for debugging coercion behavior. See [SQL Data Types](../../sql/data_types.md) for details.
:::

## Conclusion

**The [`TypeCoercion`] analyzer resolves most type mismatches automatically — use [`cast()`] or [`try_cast()`] for the rest.**

Automatic coercion widens within type families and parses literals to match their context. Set operations require column-by-column compatibility. When the analyzer rejects a mismatch, [`cast()`] provides a hard conversion that fails on bad values, while [`try_cast()`] substitutes `NULL` for resilient pipelines. Use `explain()` to inspect the `CAST` nodes the analyzer inserts, and `.schema()` to verify result types before execution.

:::{admonition} Related documents
:class: seealso

- [Anatomy of a Schema](schema-anatomy.md) — per-column field properties (`name`, `data_type`, `nullable`, `metadata`)
- [Inspecting and Validating Schemas](schema-inspection.md) — display, access, and programmatic field inspection
- [Schema Transformation](schema-transformation.md) — qualifier manipulation, combining schemas, nullability handling
- [DataFrame Methods](schema-dataframe-methods.md) — methods that change the schema (`.with_column()`, `.with_column_renamed()`)
- [Handling Null Values](../Concepts/null-handling.md) — NULL behavior in expressions, filters, and joins
:::

<!-- Link references -->

[`DataFrame`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html
[`DFSchema`]: https://docs.rs/datafusion/latest/datafusion/common/struct.DFSchema.html
[`DataType`]: https://docs.rs/arrow/latest/arrow/datatypes/enum.DataType.html
[`Field`]: https://docs.rs/arrow/latest/arrow/datatypes/struct.Field.html
[`TypeCoercion`]: https://docs.rs/datafusion/latest/datafusion/optimizer/analyzer/type_coercion/struct.TypeCoercion.html
[`LogicalPlan`]: https://docs.rs/datafusion/latest/datafusion/logical_expr/enum.LogicalPlan.html
[`SessionState`]: https://docs.rs/datafusion/latest/datafusion/execution/session_state/struct.SessionState.html
[arrow data types]: https://arrow.apache.org/docs/format/Columnar.html#data-type-descriptions
[`.select()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.select
[`.with_column()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.with_column
[`.filter()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.filter
[`.union()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.union
[`.except()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.except
[`.intersect()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.intersect
[`.collect()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.collect
[`.show()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.show
[`.explain()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.explain
[`.schema()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.schema
[`cast()`]: https://docs.rs/datafusion/latest/datafusion/prelude/fn.cast.html
[`try_cast()`]: https://docs.rs/datafusion/latest/datafusion/prelude/fn.try_cast.html
[`comparison_coercion()`]: https://docs.rs/datafusion/latest/datafusion/logical_expr/type_coercion/binary/fn.comparison_coercion.html
[`comparison_coercion_numeric()`]: https://docs.rs/datafusion/latest/datafusion/expr_common/type_coercion/binary/fn.comparison_coercion_numeric.html
[`TableProvider`]: https://docs.rs/datafusion/latest/datafusion/catalog/trait.TableProvider.html
[`TableProvider::schema()`]: https://docs.rs/datafusion/latest/datafusion/catalog/trait.TableProvider.html#tymethod.schema
