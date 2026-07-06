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

<!--TODO (restructuring notes, agreed 2026-07-06)

1. ABSTRACT
2. INTRODUCTION
3. DONE (2026-07-06) — "Unnesting Arrays" (retitled "Unnesting with Arrow
   List Columns" to avoid a duplicate anchor) and "Controlling Unnest
   Behavior with Options" arrived from set-operations.md as raw material —
   merge with the thinner unnest treatment above during rework.
4. BROKEN LINK — "see the workaround in [DataFrame-Unique Methods]
   (#dataframe-unique-methods)" uses a local anchor for a cross-file target;
   repair after the dataframe-specifics.md decision.
5. POSITION — opens the DataFrame-native part (after the hybrid-sql bridge).
-->

# Reshaping Data



:::{admonition} Style Note
:class: note
:collapsible: closed

In this document, code elements follow a consistent pattern:

- **DataFrame methods:** `.method()` (e.g., `.select()`, `.filter()`)
- **Standalone functions:** `function()` (e.g., `col()`, `lit()`)
- **Constructors:** `Type::new()` (e.g., `SessionContext::new()`)
- **Types:** `TypeName` (e.g., `SchemaRef`, `RecordBatch`)
- **Lazy transformations:** return a `DataFrame` and build the `LogicalPlan`
- **Actions:** (`.collect()`, `.show()`) trigger execution

:::

```{contents} Table of Content
:local:
:depth: 2
```

## Introduction (placeholder)

**Reshaping transforms the structure of your data—changing rows to columns or columns to rows without altering the underlying values.** <br>

Two common reshaping patterns exist in data processing:

| Operation          | What it does                              | DataFrame support        |
| ------------------ | ----------------------------------------- | ------------------------ |
| **Explode/Unnest** | Expands array elements into separate rows | ✅ [`.unnest_columns()`] |
| **Melt/Unpivot**   | Converts columns into rows (wide → long)  | ❌ Not available         |

Unnesting is essential when working with nested JSON data, multi-valued fields, or array columns from Parquet files. For melt/unpivot operations, see the workaround in [DataFrame-Unique Methods](#dataframe-unique-methods).

> **See also:** [pandas.DataFrame.explode], [PySpark explode] — similar operations in other DataFrame libraries.

### Unnesting / Exploding Arrays

Unnesting expands each element of an array column into a **separate row**, duplicating the other columns. This is essential when working with nested JSON, multi-valued fields, or array columns from Parquet files.

```rust
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();

    // Input: customers with array of order IDs
    // +----------+------------+
    // | customer | orders     |
    // +----------+------------+
    // | Alice    | [1, 2, 3]  |
    // | Bob      | [4, 5]     |
    // +----------+------------+
    let df = ctx.sql("
        SELECT * FROM (VALUES
            ('Alice', ARRAY[1, 2, 3]),
            ('Bob', ARRAY[4, 5])
        ) AS t(customer, orders)
    ").await?;

    // Unnest expands array elements into separate rows
    let expanded = df.unnest_columns(&["orders"])?;

    expanded.show().await?;
    // Output: one row per array element
    // +----------+--------+
    // | customer | orders |
    // +----------+--------+
    // | Alice    | 1      |
    // | Alice    | 2      |
    // | Alice    | 3      |
    // | Bob      | 4      |
    // | Bob      | 5      |
    // +----------+--------+

    Ok(())
}
```

> **See also:** [PySpark explode] — similar operation in Spark DataFrames.

<!-- MOVED HERE from set-operations.md (monolith-split repair, 2026-07-06) —
raw material; rework pending: merge with the thinner unnest example above
(duplicate coverage of `.unnest_columns()`). -->

### Unnesting with Arrow List Columns

[`.unnest_columns()`] explodes array (list) columns into multiple rows—one row per array element. SQL's `UNNEST` syntax varies significantly across databases; DataFusion provides a consistent API.

```rust
use datafusion::prelude::*;
use datafusion::error::Result;
use datafusion::assert_batches_eq;
use std::sync::Arc;
use arrow::array::{ArrayRef, StringArray, ListArray, Int32Array};
use arrow::datatypes::{DataType, Field};
use arrow::buffer::OffsetBuffer;

#[tokio::main]
async fn main() -> Result<()> {
    // Create a DataFrame with a list column
    let tags_field = Arc::new(Field::new_list_field(DataType::Utf8, true));
    let tags = ListArray::new(
        tags_field,
        OffsetBuffer::from_lengths([2, 1]),  // Alice has 2 tags, Bob has 1
        Arc::new(StringArray::from(vec!["vip", "early", "new"])),
        None
    );

    let df = DataFrame::from_columns(vec![
        ("customer", Arc::new(StringArray::from(vec!["Alice", "Bob"])) as ArrayRef),
        ("tags", Arc::new(tags) as ArrayRef),
    ])?;

    // Explode the tags array into rows
    let expanded = df.unnest_columns(&["tags"])?;

    let results = expanded.collect().await?;
    assert_batches_eq!(
        &[
            "+----------+-------+",
            "| customer | tags  |",
            "+----------+-------+",
            "| Alice    | vip   |",
            "| Alice    | early |",
            "| Bob      | new   |",
            "+----------+-------+",
        ],
        &results
    );
    Ok(())
}
```

### Controlling Unnest Behavior with Options

[`.unnest_columns_with_options()`] provides fine-grained control via [`UnnestOptions`]:

```rust
use datafusion::prelude::*;
use datafusion::error::Result;
use datafusion::assert_batches_eq;
use datafusion_common::UnnestOptions;
use std::sync::Arc;
use arrow::array::{ArrayRef, StringArray, ListArray};
use arrow::datatypes::{DataType, Field};
use arrow::buffer::OffsetBuffer;

#[tokio::main]
async fn main() -> Result<()> {
    // Create data with null and empty arrays
    let tags_field = Arc::new(Field::new_list_field(DataType::Utf8, true));

    // Alice: ["vip"], Bob: null, Carol: [] (empty)
    let tags = ListArray::new(
        tags_field,
        OffsetBuffer::from_lengths([1, 0, 0]),
        Arc::new(StringArray::from(vec!["vip"])),
        Some(vec![true, false, true].into())  // Bob's entry is null
    );

    let df = DataFrame::from_columns(vec![
        ("customer", Arc::new(StringArray::from(vec!["Alice", "Bob", "Carol"])) as ArrayRef),
        ("tags", Arc::new(tags) as ArrayRef),
    ])?;

    // Default: preserve_nulls = true (keeps null rows)
    let with_nulls = df.clone()
        .unnest_columns_with_options(&["tags"], UnnestOptions::new())?;

    let results = with_nulls.collect().await?;
    assert_batches_eq!(
        &[
            "+----------+------+",
            "| customer | tags |",
            "+----------+------+",
            "| Alice    | vip  |",
            "| Bob      |      |",
            "+----------+------+",
        ],
        &results
    );

    // Skip nulls and empty arrays
    let without_nulls = df
        .unnest_columns_with_options(
            &["tags"],
            UnnestOptions::new().with_preserve_nulls(false)
        )?;

    let results = without_nulls.collect().await?;
    assert_batches_eq!(
        &[
            "+----------+------+",
            "| customer | tags |",
            "+----------+------+",
            "| Alice    | vip  |",
            "+----------+------+",
        ],
        &results
    );
    Ok(())
}
```

**UnnestOptions fields:**

| Option           | Default | Effect                                                               |
| ---------------- | ------- | -------------------------------------------------------------------- |
| `preserve_nulls` | `true`  | Keep rows where the array is `NULL` (outputs `NULL` for that column) |
| `recursions`     | `[]`    | For nested arrays, specify recursion depth per column                |

> **Nested arrays:** For deeply nested structures (e.g., `List<List<Int>>`), use `RecursionUnnestOption` to control how many levels to flatten.

[`unnestoptions`]: https://docs.rs/datafusion/latest/datafusion/common/struct.UnnestOptions.html

---
