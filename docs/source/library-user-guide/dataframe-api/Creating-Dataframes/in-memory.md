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

# Creating DataFrames Form Arrow [`RecordBatch`]es

<!--TODO

1. ABSTRACT
2. INTRODUCTION
-->

```{contents}
:local:
:depth: 2
:caption: Creation DataFrames Form Arrow [`RecordBatch`]es
```

<!--### 4. From Arrow [`RecordBatch`]es: The Native Pathway-->

## Introduction (placeholder)

**Create DataFrames directly from in-memory Arrow `RecordBatch`es—the engine's native format—often with zero-copy overhead.**

When your data is already in [Arrow format], this is the most direct route into DataFusion. No parsing, no schema inference—the data is already in the engine's native format.

A [`RecordBatch`] commonly arrives from:

- **Network streams**: [Arrow Flight] for high-performance data transfer
- **File readers**: Libraries that deserialize into Arrow (e.g., Parquet → RecordBatch)
- **Your application**: Programmatically constructed data or output from other Arrow-native components

Once you have a RecordBatch, you choose between two creation methods:

#### The Architectural Choice: Read vs. Register

When you have a [`RecordBatch`], you face a fundamental decision:

| Aspect            | **One-Shot Query** ([`.read_batch()`])  | **Reusable Table** ([`.register_batch()`])                  |
| ----------------- | --------------------------------------- | ----------------------------------------------------------- |
| **What it does**  | Creates an ephemeral DataFrame directly | Adds the batch to the catalog under a name                  |
| **When to use**   | Immediate, one-off transformations      | Multiple references or SQL access needed                    |
| **How to access** | Pass the DataFrame object around        | Reference by name: [`ctx.table("name")`][`.table()`] or SQL |
| **Analogy**       | Like a temporary variable               | Like a temporary view in a database                         |

#### Pattern 1: One-Shot Query with [`.read_batch()`]

Use this when you want to process a batch immediately and don't need to reference it again. The DataFrame is created directly—no catalog entry, no name.

- [`.read_batch(batch)`][`.read_batch()`] — single RecordBatch
- [`.read_batches(vec![batch1, batch2, ...])`][`.read_batches()`] — multiple RecordBatches

> **Note:** Multiple batches must have identical schemas. They're treated as partitions of one logical table—not physically concatenated—enabling parallel processing.

**Example:** Processing a batch immediately after receiving it:

```rust
use std::sync::Arc;
use datafusion::prelude::*;
use datafusion::arrow::array::{ArrayRef, Int32Array, Float64Array};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::Result;
use datafusion::assert_batches_eq;

#[tokio::test]
async fn test_read_batch_one_shot() -> Result<()> {
    let ctx = SessionContext::new();

    // Assume this batch came from Arrow Flight or another source
    let batch = RecordBatch::try_from_iter(vec![
        ("product_id", Arc::new(Int32Array::from(vec![1, 2, 3, 4])) as ArrayRef),
        ("revenue", Arc::new(Float64Array::from(vec![1200.0, 450.0, 890.0, 2100.0])) as ArrayRef),
    ])?;

    // Process immediately and discard
    let df = ctx.read_batch(batch)?
        .filter(col("revenue").gt(lit(500.0)))?
        .sort(vec![col("revenue").sort(false, true)])?;

    // Verify the filtered and sorted results
    let batches = df.collect().await?;
    assert_batches_eq!(
        &[
            "+------------+---------+",
            "| product_id | revenue |",
            "+------------+---------+",
            "| 4          | 2100.0  |",
            "| 1          | 1200.0  |",
            "| 3          | 890.0   |",
            "+------------+---------+",
        ],
        &batches
    );

    Ok(())
}
```

#### Pattern 2: Reusable Table with [`.register_batch()`]

When you need the data accessible from multiple places—or want SQL access—register the batch as a named table:

```rust
use std::sync::Arc;
use datafusion::prelude::*;
use datafusion::arrow::array::{ArrayRef, Int32Array, Float64Array};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::Result;
use datafusion::assert_batches_eq;

#[tokio::test]
async fn test_register_batch_reusable() -> Result<()> {
    let ctx = SessionContext::new();

    // Create a batch (imagine this came from Arrow Flight or another source)
    let batch = RecordBatch::try_from_iter(vec![
        ("product_id", Arc::new(Int32Array::from(vec![1, 2, 3, 4])) as ArrayRef),
        ("revenue", Arc::new(Float64Array::from(vec![1200.0, 450.0, 890.0, 2100.0])) as ArrayRef),
    ])?;

    // Register the batch as a named table
    ctx.register_batch("live_sales", batch)?;

    // Now query it multiple times, even from SQL
    let high_revenue = ctx.sql(
        "SELECT product_id, revenue
        FROM live_sales
        WHERE revenue > 1000
        ORDER BY revenue DESC"
    ).await?;

    let batches = high_revenue.collect().await?;
    assert_batches_eq!(
        &[
            "+------------+---------+",
            "| product_id | revenue |",
            "+------------+---------+",
            "| 4          | 2100.0  |",
            "| 1          | 1200.0  |",
            "+------------+---------+",
        ],
        &batches
    );

    // Can also access via DataFrame API
    let all_products = ctx.table("live_sales").await?
        .select_columns(&["product_id"])?
        .collect().await?;
    assert_eq!(all_products.len(), 1);  // One batch returned

    Ok(())
}
```

#### Common Pitfalls

When constructing `RecordBatch`es manually, these invariants must hold:

- **Equal length**: All arrays (columns) in a batch must have exactly the same row count
- **Nullable columns**: Must be built with `Option<T>`; non-nullable columns must not contain `None`
- **Multiple batches**: Schemas must be identical (names, types, order, nullability)

> **Need help debugging?** See the full checklist in [Arrow Introduction](../../user-guide/arrow-introduction.md)

#### Record Batch References

**DataFusion:**

- [Arrow Introduction](../../user-guide/arrow-introduction.md) — RecordBatch fundamentals and debugging
- [`SessionContext::read_batch()`](https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.read_batch) — One-shot DataFrame from RecordBatch
- [`SessionContext::register_batch()`](https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.register_batch) — Register RecordBatch as table

**Arrow Ecosystem:**

- [`RecordBatch`](https://docs.rs/arrow/latest/arrow/record_batch/struct.RecordBatch.html) — Arrow's columnar in-memory format
- [Arrow Flight](https://arrow.apache.org/docs/format/Flight.html) — Network protocol for Arrow data

---
