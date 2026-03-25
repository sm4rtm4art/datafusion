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

<!--TODO

1. ABSTRACT
2. Fix cross-references to other files in this directory

-->

```{contents} Type Coercion: Auto-Alignment vs Explicit Casting
:local:
:depth: 2
```

## Introduction (placeholder)

### Mode 1: Automatic Coercion in Expressions

For convenience and intuitive use, DataFusion automatically promotes types to a common, wider type when they are mixed within an expression. This applies to functions like [`.select()`], [`.with_column()`], and [`.filter()`]. The promotion is always "widening" and follows the safe upcasting paths defined in the Type Coercion Hierarchy to prevent data loss.

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

    // See the computed values
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

### Mode 2: Strict Matching for Set Operations

For safety and to prevent silent data corruption, **set operations** like [`.union()`], [`.except()`], and [`.intersect()`] require columns in corresponding positions to have compatible types.

If the types do not match exactly, DataFusion's type coercion analyzer will attempt to find a common type. However, when no safe coercion path exists, you'll need to cast explicitly. This is a deliberate design choice—it forces you to be explicit about how to resolve type ambiguity.

> **Note on Joins:**<br>
> Join keys are an exception—DataFusion automatically coerces join keys to a common type (e.g., `Int32 = Int64` becomes `Int64 = Int64`). This happens transparently via the [`TypeCoercion`] analyzer rule, so you rarely need to cast join keys manually.

```rust
use datafusion::prelude::*;
use datafusion::assert_batches_sorted_eq;
use datafusion::arrow::datatypes::DataType;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {

    let df_i32 = dataframe!("id" => [1_i32, 2_i32])?;
    let df_i64 = dataframe!("id" => [3_i64, 4_i64])?;

    // This will FAIL because Int32 and Int64 are not an exact match.
    // let bad = df_i32.union(df_i64)?;

    // The Fix: explicitly cast one of the columns to match the other.
    // Using the cast() free function from the prelude - simple and no extra imports needed.
    let df_i32_fixed = df_i32.clone().with_column(
        "id",
        cast(col("id"), DataType::Int64)
    )?;

    // This now works because both `id` columns are Int64.
    let combined = df_i32_fixed.union(df_i64)?;

    // Verify: all four rows are present, all as Int64
    // Note: union does not guarantee row order, so we use sorted comparison
    let batches = combined.collect().await?;
    assert_batches_sorted_eq!(
        &[
            "+----+",
            "| id |",
            "+----+",
            "| 1  |",
            "| 2  |",
            "| 3  |",
            "| 4  |",
            "+----+",
        ],
        &batches
    );

    Ok(())
}
```

### Type Coercion Hierarchy

**Coercion** is the automatic conversion of one type to another to make an operation valid. When you write `Int32 + Int64`, DataFusion _coerces_ the `Int32` to `Int64` before adding—you don't have to cast explicitly.

The diagram below shows safe upcasting paths—conversions that preserve data without loss. Arrows indicate the direction of automatic widening.

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

> **Note:** <br>
> The `Decimal128 → Float64` path in this diagram widens **range** but can lose **precision**. `Decimal128` provides exact arithmetic (up to 38 digits), while `Float64` offers only ~15-17 significant digits. For financial or high-precision data, prefer keeping values as `Decimal128` and only convert to `Float64` when approximate results are acceptable (e.g., charting, statistical aggregates).

#### Key rules:

**Data Type interactions**

- **Numeric**:<br> Integers widen to the largest container (`Int32 + Int64 → Int64`). Mixed with floats, the result is `Float64`. Decimals preserve precision when combined with integers.

- **Temporal**:<br> Dates promote to `Timestamp` for comparisons and arithmetic. Timezones must match—cast explicitly to align them. `Date64` is rarely used; dates typically coerce directly to `Timestamp(ns)`.

- **Strings**:<br> `Utf8`, `LargeUtf8`, and `Utf8View` auto-align via planner-inserted casts. No automatic coercion from string columns to numeric/temporal types (though string _literals_ may be coerced in some contexts).

- **Boolean**:<br> Does not auto-coerce to numeric. Use explicit `CAST(bool_col AS Int32)` if needed.

- **NULL**:<br> Adopts the other operand's type in expressions—this is safe widening. A standalone `NULL` remains untyped until context determines it.

**The core rules:**

- **Expressions** auto-widen for convenience (`Int32 + Int64 → Int64`)
- **Set operations** require explicit alignment for safety
- **Always widen, never narrow** — narrowing risks silent data loss

### Further Reading

Now that you understand schema structure and type coercion, you're ready to work with schemas in practice:

- **[DataFrame Methods That Change the Schema](#dataframe-methods-that-change-the-schema)** — Add, rename, project, drop columns
- **[Inspecting Schemas](#inspecting-schemas)** — Display and programmatically query schema properties
- **[Creating Schemas](#creating-schemas)** — Construct schemas explicitly for type safety
- **[Transforming Schemas](#transforming-schemas)** — Modify qualifiers, combine schemas, handle nullability
- **[Validating Schemas](#validating-schemas)** — Check existence, compare schemas, verify compatibility
- **[Concepts: Handling Null Values](./concepts.md#handling-null-values)** — Deep dive into NULL behavior in expressions, filters, and joins
- **Type coercion internals:** [`TypeCoercion`] analyzer behavior for nested and scalar types
