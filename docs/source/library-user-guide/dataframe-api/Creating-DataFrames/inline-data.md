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

# Creating DataFrames from Inline Data

**Create DataFrames directly from code — the self-contained
toolkit for tests, examples, and prototyping.**

DataFusion's inline creation methods let you build DataFrames
entirely from code. The [`dataframe!`] macro is the most concise
path: pass column names and Rust arrays, and it returns a ready-to-use
[`DataFrame`] with Arrow types inferred automatically. When the
macro's inference is not enough, [`.from_columns()`] accepts
pre-built Arrow arrays with explicit types. And [`.read_empty()`]
provides a minimal one-row context for scalar expression evaluation.
This page also covers DataFusion's assertion macros —
[`assert_batches_eq!`] and [`assert_batches_sorted_eq!`] — which
together with [`dataframe!`] form a self-contained testing toolkit.

**Key methods and macros:**

| Method / Macro                | Purpose                                             |
| ----------------------------- | --------------------------------------------------- |
| [`dataframe!`]                | Create a [`DataFrame`] from Rust literals           |
| [`.from_columns()`]           | Create from pre-built Arrow arrays (explicit types) |
| [`.read_empty()`]             | One-row, zero-column DataFrame for expressions      |
| [`assert_batches_eq!`]        | Strict batch comparison (values and order)          |
| [`assert_batches_sorted_eq!`] | Order-independent batch comparison                  |
| [`assert_contains!`]          | Partial string match (e.g., `EXPLAIN` plans)        |
| [`assert_not_contains!`]      | Negative string match                               |

```{contents} Table of Contents
:local:
:depth: 2
```

## The `dataframe!` Macro

**Build a [`DataFrame`] from Rust literals in a single expression —
no files, no network, no [`SessionContext`] setup required.**

The [`dataframe!`] macro is DataFusion's most concise creation path.
It accepts column names and Rust arrays, infers the corresponding
Arrow types, wraps the data in a [`MemTable`], and returns a lazy
[`DataFrame`] ready for the full builder API. Because the macro
creates its own default [`SessionContext`] internally, the resulting
[`DataFrame`] is completely self-contained — ideal for unit tests,
documentation examples, and rapid prototyping.

**Syntax:**

```text
dataframe!(
    "column_name" => [value1, value2, ...],
    "column_name" => [value1, value2, ...],
)
```

- **Column name**: A string literal (e.g., `"sensor_id"`).
- **`=>`**: Associates the name with its data.
- **Data**: A Rust array or `Vec` literal (e.g., `[1, 2, 3]`).

```rust
use datafusion::prelude::*;
use datafusion::error::Result;
use datafusion::assert_batches_eq;

#[tokio::main]
async fn main() -> Result<()> {
    let df = dataframe!(
        "id" => [1, 2, 3],
        "name" => ["Alice", "Bob", "Carol"]
    )?;

    let batches = df.collect().await?;
    assert_batches_eq!(
        &[
            "+----+-------+",
            "| id | name  |",
            "+----+-------+",
            "| 1  | Alice |",
            "| 2  | Bob   |",
            "| 3  | Carol |",
            "+----+-------+",
        ],
        &batches
    );

    Ok(())
}
```

The macro infers Arrow types from Rust literals via the
`IntoArrayRef` trait. The following types are supported:

| Rust type                 | Arrow type           |
| ------------------------- | -------------------- |
| `bool`                    | `Boolean`            |
| `i8`, `i16`, `i32`, `i64` | `Int8` … `Int64`     |
| `u8`, `u16`, `u32`, `u64` | `UInt8` … `UInt64`   |
| `f32`, `f64`              | `Float32`, `Float64` |
| `&str`, `String`          | `Utf8`               |

Wrap any of these in `Option<T>` to express nulls. For the full
DataFusion type system, see:

- [Data Types](../../user-guide/sql/data_types.md)
- [Type Coercion](../Schema-Management/type-coercion.md)

:::{admonition} Own SessionContext
:class: note
[`dataframe!`] creates a new default [`SessionContext`] internally.
The returned [`DataFrame`] lives in an isolated session — it cannot
be joined with tables registered in another [`SessionContext`], and
it does not inherit custom configuration (batch size, parallelism,
optimizer rules). To use inline data alongside existing tables,
`.collect()` the [`DataFrame`] into [`RecordBatch`]es and re-import
them via [`.read_batch()`] on your target context (see
[Creating DataFrames from RecordBatches](from-memory.md)).
:::

Calling [`dataframe!`] with no arguments — `dataframe!()` — produces
an empty [`DataFrame`] with zero rows and zero columns, equivalent
to `ctx.read_batch(RecordBatch::new_empty(Arc::new(Schema::empty())))`.

### Null Values with `Option<T>`

**Represent missing data with Rust's `Option<T>` type — `None`
maps directly to Arrow's null representation.**

When test data requires missing values, wrap column entries in
`Option<T>`. `None` becomes Arrow's null, which appears as an
empty cell in assertion output.

```rust
use datafusion::prelude::*;
use datafusion::error::Result;
use datafusion::assert_batches_eq;

#[tokio::main]
async fn main() -> Result<()> {
    let df = dataframe!(
        "id"    => [1, 2, 3],
        "value" => [Some("foo"), None, Some("bar")],
        "score" => [Some(100), Some(200), None]
    )?;

    let batches = df.collect().await?;
    assert_batches_eq!(
        &[
            "+----+-------+-------+",
            "| id | value | score |",
            "+----+-------+-------+",
            "| 1  | foo   | 100   |",
            "| 2  |       | 200   |",
            "| 3  | bar   |       |",
            "+----+-------+-------+",
        ],
        &batches
    );

    Ok(())
}
```

For the distinction between `None`, SQL `NULL`, and `NaN`, see
[Understanding Null Values](../../user-guide/dataframe.md#understanding-null-values-none-null-and-nan).

### Programmatic Data Generation

**Inject dynamically generated `Vec`s into the macro — useful for
fuzz tests, parameterized test matrices, or synthetic benchmarks.**

The right-hand side of `=>` is not limited to array literals — the
[`dataframe!`] macro accepts any expression that evaluates to a
`Vec<T>` or `&[T]` where `T` implements `IntoArrayRef`. This means
you can generate test data with `for` loops, iterator chains
(`.map().collect()`), the `rand` crate, or any other Rust logic
that produces a supported `Vec`:

```rust
use datafusion::prelude::*;
use datafusion::functions_aggregate::expr_fn::sum;
use datafusion::assert_batches_eq;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let mut departments = Vec::new();
    let mut salaries = Vec::new();

    for i in 0..4 {
        if i < 2 {
            departments.push("Sales");
            salaries.push(50000 + (i * 5000));
        } else {
            departments.push("Engineering");
            salaries.push(80000 + ((i - 2) * 5000));
        }
    }

    let df = dataframe!(
        "department" => departments,
        "salary"     => salaries
    )?;

    let result = df
        .aggregate(vec![col("department")], vec![sum(col("salary")).alias("total")])?
        .filter(col("total").gt(lit(100000)))?
        .sort(vec![col("total").sort(false, true)])?;

    let batches = result.collect().await?;
    assert_batches_eq!(
        &[
            "+-------------+--------+",
            "| department  | total  |",
            "+-------------+--------+",
            "| Engineering | 165000 |",
            "| Sales       | 105000 |",
            "+-------------+--------+",
        ],
        &batches
    );

    Ok(())
}
```

---

## Explicit Arrow Types with `.from_columns()`

**When the [`dataframe!`] macro's type inference is not enough,
[`.from_columns()`] lets you supply pre-built Arrow arrays with
exact types.**

The [`dataframe!`] macro infers Arrow types from Rust literals —
`i32` becomes `Int32`, `&str` becomes `Utf8`. When you need a
specific Arrow type that differs from the default inference (e.g.,
`Int64` instead of `Int32`), or when you already have Arrow arrays
from another library, use [`DataFrame::from_columns()`][`.from_columns()`].

| Use [`.from_columns()`] when …    | Example                                                    |
| --------------------------------- | ---------------------------------------------------------- |
| You already have Arrow arrays     | Output from another Arrow-native library or computation    |
| You need a specific Arrow type    | `Int64` instead of inferred `Int32`, `Timestamp` precision |
| You are bridging external systems | Arrays from Arrow IPC, Flight, or custom `TableProvider`s  |

```rust
use std::sync::Arc;
use datafusion::prelude::*;
use datafusion::arrow::array::{ArrayRef, Int64Array, StringArray};
use datafusion::error::Result;
use datafusion::assert_batches_eq;

#[tokio::main]
async fn main() -> Result<()> {
    let df = DataFrame::from_columns(vec![
        ("id", Arc::new(Int64Array::from(vec![1_i64, 2, 3])) as ArrayRef),
        ("name", Arc::new(StringArray::from(vec!["Alice", "Bob", "Carol"])) as ArrayRef),
    ])?;

    let batches = df.collect().await?;
    assert_batches_eq!(
        &[
            "+----+-------+",
            "| id | name  |",
            "+----+-------+",
            "| 1  | Alice |",
            "| 2  | Bob   |",
            "| 3  | Carol |",
            "+----+-------+",
        ],
        &batches
    );

    Ok(())
}
```

:::{admonition} Own SessionContext
:class: note
Like [`dataframe!`], [`.from_columns()`] creates a new default
[`SessionContext`] internally. The returned [`DataFrame`] is not
attached to any user-provided context.
:::

:::{admonition} Decision rule
:class: tip
Start with [`dataframe!`] — it is readable and sufficient for most
tests. Switch to [`.from_columns()`] only when you already have
Arrow arrays or need explicit type control that the macro cannot
infer.
:::

---

## Expression Evaluation with `.read_empty()`

**Create a one-row, zero-column [`DataFrame`] for evaluating scalar
expressions — similar to Oracle's `DUAL` or PostgreSQL's
`SELECT` without `FROM`.**

[`.read_empty()`] is useful when you need to compute a value without
any underlying data — for example, evaluating a mathematical
expression, calling `now()`, or testing a UDF in isolation. The
resulting [`DataFrame`] contains exactly one row and zero columns;
you add columns via [`.select()`] with literal or function
expressions.

```rust
use datafusion::prelude::*;
use datafusion::error::Result;
use datafusion::assert_batches_eq;

#[tokio::main]
async fn main() -> Result<()> {
    let ctx = SessionContext::new();

    let result = ctx.read_empty()?
        .select(vec![
            lit(5).mul(lit(5)).alias("result"),
        ])?
        .collect()
        .await?;

    assert_batches_eq!(
        &[
            "+--------+",
            "| result |",
            "+--------+",
            "| 25     |",
            "+--------+",
        ],
        &result
    );

    Ok(())
}
```

### Empty Schema Placeholder

**Create a zero-row [`DataFrame`] that preserves a specific schema
— useful for safe `UNION`s, pipeline stubs, and edge-case testing.**

When you need a [`DataFrame`] with **zero rows** but a defined
schema (e.g., to handle "no data found" cases while keeping a
`.union()` valid), do not use [`.read_empty()`] — that produces one
row with zero columns. Instead, create an empty [`RecordBatch`]
with the target schema and wrap it via [`.read_batch()`]:

```rust
use datafusion::prelude::*;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::assert_batches_eq;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();

    let df_real = dataframe!("id" => [1, 2, 3])?;

    let schema = df_real.schema().inner().clone();
    let empty_batch = RecordBatch::new_empty(schema);
    let df_empty = ctx.read_batch(empty_batch)?;

    assert_eq!(df_empty.clone().count().await?, 0);

    // Safe union: both DataFrames share the "id" column
    let result = df_real.union(df_empty)?
        .collect()
        .await?;

    assert_batches_eq!(
        &[
            "+----+",
            "| id |",
            "+----+",
            "| 1  |",
            "| 2  |",
            "| 3  |",
            "+----+",
        ],
        &result
    );

    Ok(())
}
```

:::{admonition} Key distinction
:class: warning

- **[`.read_empty()`]**: 1 row, 0 columns — for scalar expression evaluation.
- **`RecordBatch::new_empty(schema)`**: 0 rows, N columns — for schema-compliant pipeline stubs.

:::

---

## Verifying DataFrame Results

**DataFusion provides assertion macros that format Arrow
[`RecordBatch`]es into readable ASCII tables and compare them
against expected output — the standard way to validate
transformations in tests.**

Every code example in this documentation uses these macros. They
handle the complexity of formatting columnar data so you can focus
on the expected result as a simple string table. The two data
macros cover ordered and unordered output; the two string macros
cover partial and negative matching on text like `EXPLAIN` plans
or error messages.

| Macro                          | Best use case                                                                                                 |
| :----------------------------- | :------------------------------------------------------------------------------------------------------------ |
| **Data Verification**          |                                                                                                               |
| [`assert_batches_eq!`]         | **Strict.** Checks values _and_ row order. Use after an explicit [`.sort()`].                                 |
| [`assert_batches_sorted_eq!`]  | **Order-independent.** Sorts both sides before comparing. Use when parallel execution may scramble row order. |
| **String & Plan Verification** |                                                                                                               |
| [`assert_contains!`]           | **Partial match.** Checks if a string contains a phrase — e.g., an `EXPLAIN` plan contains `"FilterExec"`.    |
| [`assert_not_contains!`]       | **Negative match.** Ensures a phrase is absent — e.g., verifying an optimizer removed a `"Filter"` node.      |

:::{admonition} Copy-paste workflow
:class: tip
When [`assert_batches_eq!`] fails, the error message prints the
actual output in the exact ASCII format the macro expects. Copy
this output from your terminal and paste it directly into your
test to update the expected result.
:::

---

## Choosing the Right Approach

**All inline creation methods produce a lazy [`DataFrame`] without
external dependencies — choose based on type control and data
shape.**

The three creation methods on this page serve different needs but
share a common trait: the data lives entirely in your Rust source.
No files, no network calls, no pre-existing catalog entries.

| Method              | Input              | Type control | SessionContext     | Best for                                    |
| ------------------- | ------------------ | ------------ | ------------------ | ------------------------------------------- |
| [`dataframe!`]      | Rust literals/Vecs | Inferred     | Created internally | Tests, examples, prototyping                |
| [`.from_columns()`] | Arrow `ArrayRef`s  | Explicit     | Created internally | Specific Arrow types, bridging Arrow arrays |
| [`.read_empty()`]   | None               | N/A          | User-provided      | Scalar expressions, UDF testing             |

---

## Bringing It Together

[`dataframe!`] is the fastest way to get data into a [`DataFrame`]
— a single expression turns Rust literals into a lazy query plan
with no external dependencies. When the macro's type inference is
not enough, [`.from_columns()`] gives explicit control over Arrow
types. And [`.read_empty()`] provides a minimal one-row context for
evaluating scalar expressions. Combined with DataFusion's assertion
macros — [`assert_batches_eq!`] and [`assert_batches_sorted_eq!`] —
these tools form a self-contained testing toolkit: create, transform,
verify, all within a single Rust source file.

---

## References

**Concepts & Guides:**

- [Data Types](../../user-guide/sql/data_types.md) — SQL-to-Arrow type mappings
- [Type Coercion](../Schema-Management/type-coercion.md) — Automatic type promotion and explicit casting
- [Understanding Null Values](../../user-guide/dataframe.md#understanding-null-values-none-null-and-nan) — `None`, SQL `NULL`, and `NaN`
- [Creating DataFrames from RecordBatches](from-memory.md) — `.read_batch()`, `.read_batches()`, and `MemTable`

**API Documentation:**

- [`dataframe!`](https://docs.rs/datafusion/latest/datafusion/macro.dataframe.html) — Create DataFrames from Rust literals
- [`DataFrame::from_columns()`](https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.from_columns) — Create from pre-built Arrow arrays
- [`SessionContext::read_empty()`](https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.read_empty) — One-row, zero-column DataFrame
- [`assert_batches_eq!`](https://docs.rs/datafusion/latest/datafusion/macro.assert_batches_eq.html) — Strict batch comparison
- [`assert_batches_sorted_eq!`](https://docs.rs/datafusion/latest/datafusion/macro.assert_batches_sorted_eq.html) — Order-independent batch comparison
- [`assert_contains!`](https://docs.rs/datafusion/latest/datafusion/macro.assert_contains.html) — Partial string match
- [`assert_not_contains!`](https://docs.rs/datafusion/latest/datafusion/macro.assert_not_contains.html) — Negative string match

---
