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

<!--
MOVE HANDSHAKE: Cartesian-product failure modes, match validation, NULL-key
guidance, troubleshooting, and bounded plan inspection arrived from
../joins.md. The migration source remains unchanged for coordinator comparison.

LOCAL TODO OWNERS: JOIN-TODO-001, JOIN-TODO-009, JOIN-TODO-010,
JOIN-TODO-011, JOIN-TODO-012, JOIN-TODO-013, JOIN-TODO-014, JOIN-TODO-015,
JOIN-TODO-016, JOIN-TODO-019, JOIN-TODO-021, JOIN-TODO-025, and
JOIN-TODO-026.
-->
<!-- JOIN-TODO-001: Add the title-line highlighting sentence, abstract, Key Methods table, first-H2 framing, and conclusion after this leaf stabilizes. -->
<!-- JOIN-TODO-025: Register this leaf as a doctest after Author approval. -->

# Join Validation

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

```{contents} Table of Contents for Join Validation
:local:
:depth: 2
```

<!-- JOIN-TODO-005 JOIN-TODO-011 JOIN-TODO-014: Correct the trigger and safe-diagnosis advice; distinguish intentional SQL CROSS JOIN; convert warning styling to MyST. -->

## Avoid Accidental Cartesian Products

Empty join keys produce a **Cartesian product**â€”every left row paired with every right row. This is almost never intentional and can crash your query or exhaust memory.

| Left rows | Right rows | Result rows       | Scale                              |
| --------- | ---------- | ----------------- | ---------------------------------- |
| 3         | 4          | 12                | Tiny dataset, still 4Ã— larger     |
| 1,000     | 1,000      | 1,000,000         | 1 million rows                     |
| 1,000,000 | 1,000,000  | 1,000,000,000,000 | **1 trillion rows** â€” will crash |

```rust
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let customers_df = dataframe!(
        "id" => [1, 2, 3],
        "name" => ["Alice", "Bob", "Carol"]
    )?;

    let orders_df = dataframe!(
        "order_id" => [101, 102, 103, 104],
        "customer_id" => [1, 1, 2, 99],
        "amount" => [100, 200, 150, 300]
    )?;

    // âŒ DANGEROUS: Empty keys = Cartesian product
    let cartesian = customers_df.clone().join(
        orders_df.clone(),
        JoinType::Inner,
        &[],  // No join keys!
        &[],
        None
    )?;

    cartesian.show().await?;
    // +----+-------+----------+-------------+--------+
    // | id | name  | order_id | customer_id | amount |
    // +----+-------+----------+-------------+--------+
    // | 1  | Alice | 101      | 1           | 100    |  â† Alice Ã— order 101
    // | 1  | Alice | 102      | 1           | 200    |  â† Alice Ã— order 102
    // | 1  | Alice | 103      | 2           | 150    |  â† Alice Ã— order 103 (not her order!)
    // | 1  | Alice | 104      | 99          | 300    |  â† Alice Ã— order 104 (not her order!)
    // | 2  | Bob   | 101      | 1           | 100    |  â† Bob Ã— order 101 (not his order!)
    // | ... 7 more rows ... |
    // +----+-------+----------+-------------+--------+
    // Total: 3 Ã— 4 = 12 rows â€” every combination!

    // âœ… CORRECT: Always specify join keys
    let correct = customers_df.clone().join(
        orders_df.clone(),
        JoinType::Inner,
        &["id"],         // left key
        &["customer_id"], // right key
        None
    )?;

    println!("Row count: {}", correct.clone().count().await?);
    // Row count: 3  â† Only matching rows (AliceÃ—2, BobÃ—1)

    Ok(())
}
```

<!-- JOIN-TODO-011: Unequal key-array lengths error during planning; only a genuinely empty inner-join condition becomes a cross join. `.count()` still executes the explosive plan. -->

**âš ï¸ Warning:** <br>
If a join returns unexpectedly many rows, check your keys. An empty or mismatched key array silently produces a Cartesian product. Use [`.count()`] before [`.collect()`] to verify.

**If you need a Cartesian product:** <br>
Use SQL via [`ctx.sql("SELECT ... FROM a CROSS JOIN b")`][`sessioncontext::sql()`]. The DataFrame API has no `JoinType::Cross`â€”empty keys with `Inner` produces the same result but reads like a bug.

---

<!-- JOIN-TODO-009 JOIN-TODO-010 JOIN-TODO-011 JOIN-TODO-012: Rebuild this as validation by symptom: coverage, multiplication, NULL policy, Cartesian risk, schema, and wrong matches. -->

## Validate and Inspect Join Results

Joins can silently produce unexpected results. When something looks wrong, check these common issues:

| Symptom             | Common Causes                                                             | Diagnosis                                                                   |
| :------------------ | :------------------------------------------------------------------------ | :-------------------------------------------------------------------------- |
| **Empty result**    | Key values don't match, trailing whitespace, case mismatch, NULLs in keys | Inspect both sides: `.select(vec![col("key")]).distinct().show().await?`    |
| **Too many rows**   | Duplicate keys create row multiplication, accidental Cartesian product    | Check key uniqueness: `.select(vec![col("key")]).distinct().count().await?` |
| **Missing columns** | Wrong column names after join, schema mismatch                            | Inspect schema: [`.schema()`] and use [`.alias()`] to qualify               |
| **Wrong matches**   | Keys have different types (string vs int), encoding issues                | Compare types: `df.schema().field_with_name("key")?.data_type()`            |

<!-- JOIN-TODO-009 JOIN-TODO-012: Delete output/left "retention" percentages and fixed thresholds; validate distinct matched keys, unmatched keys, multiplicity, and schema separately. -->

### Check Match Coverage and Row Multiplication

DataFusion doesn't have built-in join validation, but you can build a simple check to catch silent data loss:

```rust
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let customers_df = dataframe!(
        "id" => [1, 2, 3],
        "name" => ["Alice", "Bob", "Carol"]
    )?;

    let orders_df = dataframe!(
        "order_id" => [101, 102, 103, 104],
        "customer_id" => [1, 1, 2, 99],
        "amount" => [100, 200, 150, 300]
    )?;

    // Count rows before join
    let left_count = customers_df.clone().count().await?;

    // Perform the join
    let joined = customers_df.clone().join(
        orders_df.clone(),
        JoinType::Inner,
        &["id"],
        &["customer_id"],
        None
    )?;
    let joined_count = joined.clone().count().await?;

    // Calculate retention rate
    let retention_pct = (joined_count as f64 / left_count as f64) * 100.0;
    println!("Rows: {} â†’ {} ({:.1}% retention)", left_count, joined_count, retention_pct);

    // âš ï¸ Alert if too much data was dropped
    if retention_pct < 50.0 {
        eprintln!("WARNING: Join dropped {:.1}% of rows! Check keys, NULLs, types.",
            100.0 - retention_pct);
    }

    Ok(())
}
```

| Join Type    | Expected Retention     | Warning Sign                       |
| ------------ | ---------------------- | ---------------------------------- |
| **Inner**    | Varies by data overlap | < 50% often indicates key mismatch |
| **Left**     | 100% of left rows      | < 100% means something is wrong    |
| **LeftSemi** | â‰¤ 100% (filtered)    | 0% = no matches at all             |
| **LeftAnti** | Complement of Semi     | 100% = nothing matched             |

### Inspect Key Coverage

**Step 1:** Inspect inputs before joining

Before joining, verify that key values actually overlap. Use [`.distinct()`] to see the unique key values on each sideâ€”if they don't match, your join will produce empty or unexpected results.

```rust
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let customers_df = dataframe!(
        "id" => [1, 2, 3],
        "name" => ["Alice", "Bob", "Carol"]
    )?;

    let orders_df = dataframe!(
        "order_id" => [101, 102, 103, 104],
        "customer_id" => [1, 1, 2, 99],
        "amount" => [100, 200, 150, 300]
    )?;

    // Verify key values exist and match on both sides
    println!("Left keys:");
    customers_df.clone()
        .select(vec![col("id")])?
        .distinct()?  // Unique values only
        .show().await?;
    // +----+
    // | id |
    // +----+
    // | 1  |
    // | 2  |
    // | 3  |
    // +----+

    println!("Right keys:");
    orders_df.clone()
        .select(vec![col("customer_id")])?
        .distinct()?
        .show().await?;
    // +-------------+
    // | customer_id |
    // +-------------+
    // | 1           |
    // | 2           |
    // | 99          |  â† No matching customer! Will be dropped in Inner join
    // +-------------+

    Ok(())
}
```

<!-- JOIN-TODO-010: Preserve default NULL non-matching behavior, but remove sentinel replacement as a universal fix and describe filter_null_join_keys only as an optimization. -->

### Account for NULL Join Keys

In SQL semantics, `NULL = NULL` returns `UNKNOWN` (not `TRUE`), so NULL keys **never match**. This silently drops rows.

```rust
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let df = dataframe!(
        "customer_id" => [Some(1), Some(2), None],
        "value" => [100, 200, 300]
    )?;

    // Count NULLs in join key
    let null_count = df.clone()
        .filter(col("customer_id").is_null())?
        .count().await?;
    println!("NULL keys: {}", null_count);

    // Fix: Replace NULLs with sentinel value before joining
    let df = df.with_column("customer_id", coalesce(vec![col("customer_id"), lit(-1)]))?;
    df.show().await?;

    Ok(())
}
```

<!-- JOIN-TODO-010: Explain this setting as an optimizer behavior, not a semantic NULL-matching option. -->

> **Config option:** <br>
> DataFusion has [`datafusion.optimizer.filter_null_join_keys`][`datafusion.optimizer`] to automatically filter NULL keys.

<!-- JOIN-TODO-013: Turn this into bounded `.explain()` guidance; `analyze = true` executes the plan and is not a safe first diagnostic for suspected explosion. -->

### Inspect the Planned Join

DataFusion's [`.explain()`] is your window into how the query optimizer transformed your join. It reveals which algorithm was selected, whether predicates were pushed down, and potential performance issues.

```rust
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let customers_df = dataframe!(
        "id" => [1, 2, 3],
        "name" => ["Alice", "Bob", "Carol"]
    )?;

    let orders_df = dataframe!(
        "order_id" => [101, 102],
        "customer_id" => [1, 2],
        "amount" => [100, 200]
    )?;

    let joined_df = customers_df.join(
        orders_df,
        JoinType::Inner,
        &["id"],
        &["customer_id"],
        None
    )?;

    // explain(verbose, analyze) - verbose=true shows optimized plan
    joined_df.explain(true, false)?.show().await?;

    Ok(())
}
```

**What to look for in the plan:**

| Node                 |                       Meaning                       | Performance                            |
| -------------------- | :-------------------------------------------------: | -------------------------------------- |
| `HashJoinExec`       | Hash-based join (builds hash table from right side) | âœ… Fast for equi-joins                |
| `SortMergeJoinExec`  |             Sort both sides, then merge             | âœ… Good for large sorted data         |
| `NestedLoopJoinExec` |               Compares every row pair               | âš ï¸ Slow â€” only for non-equi joins |
| `CrossJoinExec`      |                  Cartesian product                  | âŒ Usually a bug                       |

**Signs of a healthy plan:**

- Predicates pushed into `ParquetExec` or `CsvExec` (filter early)
- `HashJoinExec` or `SortMergeJoinExec` for equi-joins
- Smaller table on the **build side** (right side of hash join)

**âš ï¸ Warning signs:**

- `NestedLoopJoinExec` when you expected equi-join â†’ check if optimizer couldn't extract equality predicates
- `CrossJoinExec` â†’ accidental Cartesian product
- Filters appearing **after** the join instead of pushed down

```text
Example output (simplified):
HashJoinExec: mode=Partitioned, join_type=Inner
  left: ParquetExec: file=customers.parquet, predicate=id IS NOT NULL
  right: ParquetExec: file=orders.parquet, predicate=customer_id IS NOT NULL
                      â†‘ Good! NULL filter pushed down
```

<!-- JOIN-TODO-013: Retain only with an explicit execution warning and after non-executing plan inspection. -->

> **Pro tip:** Use `.explain(true, true)?` (analyze=true) to see actual row counts and timing after executionâ€”helps identify which join leg is the bottleneck.

[`.alias()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.alias
[`.collect()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.collect
[`.count()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.count
[`.distinct()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.distinct
[`.explain()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.explain
[`.schema()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.schema
[`datafusion.optimizer`]: https://docs.rs/datafusion/latest/datafusion/optimizer/index.html
[`sessioncontext::sql()`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.sql
