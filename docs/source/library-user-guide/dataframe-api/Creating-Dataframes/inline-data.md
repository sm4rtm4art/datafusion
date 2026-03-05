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

# Creating DataFrames From Inline Data (using the [`dataframe!`] macro)

<!--TODO

1. ABSTRACT
2. INTRODUCTION
-->

```{contents}
:local:
:depth: 2
:caption: Creating DataFrames From Inline Data
```

## Introduction (placeholder)

**Create DataFrames from Rust literals—perfect for tests, examples, and prototyping without external data dependencies.**

This approach shines when your data is small, temporary, and lives entirely in code.

**Perfect for:**

- **Unit tests**: Verify transformations work correctly without file I/O overhead or test data management
- **Documentation examples**: Create self-contained, runnable code snippets that anyone can execute
- **Prototyping**: Quickly experiment with DataFusion's API and operations in REPL or notebooks
- **Benchmarking**: Generate controlled test data with known characteristics for performance testing

**Not ideal for:**

- Production data pipelines (use file-based or streaming sources instead)
- Large datasets (literals are compiled into your binary and loaded into memory)
- Dynamic data (values must be known at compile time)

> **DataFrame API advantage**:<br>
> SQL has no direct equivalent for inline test data. SQL's `VALUES` clause requires a `SessionContext` and produces a query result—not a reusable DataFrame you can transform programmatically.

#### 1. [`dataframe!`] macro: Basic syntax

The dataframe! macro uses a declarative, column-oriented syntax. It mimics the structure of a hash map, where keys are column names and values are lists of data.

**Syntax Pattern:**

```text
dataframe! (
    "column_name" => [value1, value2, ...],
     ... )
```

- Column Name: A string literal (e.g., "id").

* Operator: The => arrow associates the name with its data.

- Data: A Rust vector or array literal (e.g., [1, 2, 3]).

> Note:<br>
> This macro automatically creates a new default SessionContext to host the DataFrame. If you need to attach the data to an existing context (e.g., to share configuration), use ctx.read_batch() instead.

```rust
use datafusion::prelude::*;
use datafusion::error::Result;
use datafusion::assert_batches_eq;

#[tokio::test]
async fn test_dataframe_macro_basic() -> Result<()> {
    // Create DataFrame from inline data
    let df = dataframe!(
        "id" => [1, 2, 3],
        "name" => ["Alice", "Bob", "Carol"]
    )?;

    // Verify the DataFrame contains expected data
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

#### Complete testing workflow

The [`dataframe!`] macro pairs perfectly with [`assert_batches_eq!`] for validating DataFrame transformations.

Here is a complete unit test showing the **Three-Step Pattern**.

> **Sophisticated Usage:**<br>
> Notice step 1. Instead of hardcoding literals, we use a standard Rust loop to generate the data programmatically. This demonstrates how to inject dynamic data (e.g., from a fuzzer or random generator) into the declarative macro.

```rust
use datafusion::prelude::*;
use datafusion::assert_batches_eq;

#[tokio::test]
async fn test_filter_and_aggregate() -> datafusion::error::Result<()> {
    // 1) CREATE: Generate data programmatically
    // We want to simulate:
    // - 2 entries for "Sales" (Base salary 50k)
    // - 2 entries for "Engineering" (Base salary 80k)
    let mut departments = Vec::new();
    let mut salaries = Vec::new();

    for i in 0..4 {
        if i < 2 {
            departments.push("Sales");
            salaries.push(50000 + (i * 5000)); // 50000, 55000
        } else {
            departments.push("Engineering");
            salaries.push(80000 + ((i - 2) * 5000)); // 80000, 85000
        }
    }

    // Inject the generated vectors directly into the macro
    let df = dataframe!(
        "department" => departments,
        "salary" => salaries
    )?;

    // 2) TRANSFORM: Apply the operations you want to test
    let result = df
        .aggregate(vec![col("department")], vec![sum(col("salary")).alias("total")])?
        .filter(col("total").gt(lit(100000)))?
        .sort(vec![col("total").sort(false, true)])?;

    // 3) VERIFY: Assert the exact expected output
    // Sales: 50k + 55k = 105k
    // Eng:   80k + 85k = 165k
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

This three-step pattern (**CREATE → TRANSFORM → VERIFY**) is your blueprint for testing DataFrames.

#### Testing macros for asserting results

DataFusion provides specialized macros to verify your results. These handle the complexity of formatting Arrow RecordBatches so you don't have to manually iterate over rows.

| Macro                          | Best Use Case                                                                                                                                              |
| :----------------------------- | :--------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Data Verification**          |                                                                                                                                                            |
| [`assert_batches_eq!`]         | **Strict Check.** Use when the output is deterministic (e.g., after a [`.sort()`]). Checks values _and_ order.                                             |
| [`assert_batches_sorted_eq!`]  | **Loose Check.** Use when parallel execution might scramble row order (e.g., aggregations). It sorts both sides before comparing.                          |
| **String & Plan Verification** |                                                                                                                                                            |
| [`assert_contains!`]           | **Partial Match.** Use to check if an error message contains a specific phrase, or if an `EXPLAIN` plan contains a specific operator (e.g., "FilterExec"). |
| [`assert_not_contains!`]       | **Negative Check.** Use to ensure a specific operator was optimized away (e.g., ensuring a "Filter" is no longer present after optimization).              |

> **Pro Tip: The Copy-Paste Workflow**<br>
> When [`assert_batches_eq!`] fails, it prints the actual output in the exact ASCII format expected by the macro. You can simply copy this output from your terminal and paste it into your test code to update the expected result.

#### Special cases

The basic [`dataframe!`] syntax handles most scenarios, but two situations require additional techniques:

**Null values** — Use Rust's `Option<T>` type to represent missing data:

```rust
use datafusion::prelude::*;
# use datafusion::error::Result;
# #[tokio::main]
# async fn main() -> Result<()> {
let df = dataframe!(
    "id" => [1, 2, 3],
    "value" => [Some("foo"), None, Some("bar")],  // Option<T> for nulls
    "score" => [Some(100), Some(200), None]
)?;
# df.show().await?;
# Ok(())
# }
```

> For a deeper understanding of Null handling, see: <br> [Understanding Null Values](../../user-guide/dataframe.md#understanding-null-values-none-null-and-nan) for distinctions between `None`, SQL `NULL`, and `NaN`.

> **Explicit Arrow types** <br>
> The [`dataframe!`] macro infers Arrow types from Rust literals (e.g., `i32` → `Int32`, `&str` → `Utf8`). Use [`DataFrame::from_columns()`][`.from_columns()`] when you need direct control:

| Use [`.from_columns()`] when... | Example                                                                     |
| :------------------------------ | :-------------------------------------------------------------------------- |
| You already have Arrow arrays   | Output from another Arrow-native library or computation                     |
| You need a specific Arrow type  | `Int64` instead of inferred `Int32`, or `Timestamp` with specific precision |
| You're bridging systems         | Receiving arrays from Arrow IPC, Flight, or custom TableProviders           |

```rust
use std::sync::Arc;
use datafusion::prelude::*;
use datafusion::arrow::array::{ArrayRef, Int64Array, StringArray};
use datafusion::error::Result;

#[tokio::main]
async fn main() -> Result<()> {
    // Explicit Int64 (dataframe! would infer Int32 from literals)
    let df = DataFrame::from_columns(vec![
        ("id", Arc::new(Int64Array::from(vec![1_i64, 2, 3])) as ArrayRef),
        ("name", Arc::new(StringArray::from(vec!["Alice", "Bob", "Carol"])) as ArrayRef),
    ])?;
    df.show().await?;
    Ok(())
}
```

> **Decision rule**:<br>
> Start with `dataframe!`—it's readable and sufficient for most tests. Switch to `from_columns()` only when you already have Arrow arrays or need explicit type control that the macro can't infer.

### 2. Generative Data (Calculations & Placeholders)

Sometimes you need a DataFrame purely to evaluate expressions, or you need a schema-compliant "empty" table to handle edge cases in pipelines.

#### 1. The "Calculation Root" ([`ctx.read_empty()`][`.read_empty()`])\*\*

This creates a DataFrame with **one row and zero columns**. It acts like a "blank sheet" (similar to `DUAL` in Oracle or a `SELECT` without `FROM` in Postgres) that allows you to execute scalar expressions.

```rust
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();

    // Create a single-row, column-less DataFrame
    let df = ctx.read_empty()?;

    // Use it to evaluate scalar expressions
    let result = df.select(vec![
        lit(5).mul(lit(5)).alias("result"), // 5 * 5
        now().alias("execution_time")       // Current time
    ])?;

    // FIX: Clone the DataFrame to count it, so we don't consume 'result'
    assert_eq!(result.clone().count().await?, 1);

    result.show().await?;
    Ok(())
}
```

#### 2. The Empty Placeholder (Safe Unions)

If you need a DataFrame with **zero rows** but a specific schema (e.g., to handle "no data found" cases while keeping a `UNION` valid), do **not** use `read_empty()`. Instead, use `read_batch` with an empty `RecordBatch`.

> **Use Case:**<br> > _The "Structural Placeholder."_<br>
> This creates a valid DataFrame object that contains no data. It acts like an empty container that satisfies function signatures and pipeline requirements (like UNION schemas or Parquet writers) when the actual data is missing or filtered out.
>
> **Testing Tip:** <br>
> Use this to verify that your functions handle "no results" scenarios gracefully without crashing (e.g., avoiding division-by-zero errors in aggregations).

```rust
use datafusion::prelude::*;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::assert_batches_eq;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();

    // 1. Simulate an existing DataFrame
    let df_real = dataframe!("id" => [1, 2, 3])?;

    // 2. Get the schema
    // FIX: Use .inner().clone() to get the Arc<Schema>
    let schema = df_real.schema().inner().clone();

    // 3. Create a truly empty DataFrame (0 rows) with that exact schema
    let empty_batch = RecordBatch::new_empty(schema);
    let df_empty = ctx.read_batch(empty_batch)?;

    // VERIFICATION 1: Prove it is actually empty
    // (We clone here just to be safe, though count() is the last usage of df_empty)
    assert_eq!(df_empty.clone().count().await?, 0);

    // 4. Safe Union: This works because both have column "id"
    let combined = df_real.union(df_empty)?;

    // VERIFICATION 2: Prove the union worked (3 rows + 0 rows = 3 rows)
    let result = combined.collect().await?;
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

> **Key Distinction:**
>
> - **`read_empty()`**: 1 Row, 0 Columns. (Used for logic/math).
> - **`RecordBatch::new_empty()`**: 0 Rows, N Columns. (Used for data pipelines).

#### Inline data References

**DataFusion:**

- [`dataframe!` macro](https://docs.rs/datafusion/latest/datafusion/macro.dataframe.html) — Create DataFrames from literals
- [`assert_batches_eq!`](https://docs.rs/datafusion/latest/datafusion/macro.assert_batches_eq.html) — Test DataFrame outputs
- [`assert_batches_sorted_eq!`](https://docs.rs/datafusion/latest/datafusion/macro.assert_batches_sorted_eq.html) — Order-insensitive test comparison
- [`DataFrame::from_columns()`](https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.from_columns) — Create from Arrow arrays
- [`ctx.read_empty()`](https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.read_empty) — Create empty DataFrame

---
