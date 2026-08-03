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
MOVE HANDSHAKE: Join construction, composite-key, expression-condition, and
outer-join-filter material arrived from ../joins.md. The migration source
remains unchanged for coordinator comparison.

LOCAL TODO OWNERS: JOIN-TODO-001, JOIN-TODO-004, JOIN-TODO-005,
JOIN-TODO-006, JOIN-TODO-014, JOIN-TODO-015, JOIN-TODO-016, JOIN-TODO-018,
JOIN-TODO-019, JOIN-TODO-021, JOIN-TODO-022, JOIN-TODO-025, and
JOIN-TODO-026.
-->
<!-- JOIN-TODO-001: Add the title-line highlighting sentence, abstract, Key Methods table, first-H2 framing, and conclusion after this leaf stabilizes. -->
<!-- JOIN-TODO-025: Register this leaf as a doctest after Author approval. -->

# Join Conditions

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

```{contents} Table of Contents for Join Conditions
:local:
:depth: 2
```

<!-- JOIN-TODO-004 JOIN-TODO-005: The construction boundary is established; revise this H2 subtree in the next iteration. -->

## Build Joins with Keys and Conditions

<!-- JOIN-TODO-004: Move the signature and method-choice material into separate `.join()` and `.join_on()` construction subsections; preserve AND/OR behavior but remove physical-algorithm guarantees. -->

The [`.join()`] method signature in the datafusion dataframe-API:

```rust
use datafusion::prelude::*;  // Includes JoinType

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let left_df = dataframe!("id" => [1, 2])?;
    let right_df = dataframe!("customer_id" => [1, 2])?;

    let joined = left_df.join(
        right_df,                    // 1. Right DataFrame
        JoinType::Inner,             // 2. Join type
        &["id"],                     // 3. Left key columns
        &["customer_id"],            // 4. Right key columns
        None,                        // 5. Optional filter expression
    )?;

    joined.show().await?;
    Ok(())
}
```

**Two ways to specify joins:**

- [`.join()`] â€” Pass column names (`&[&str]`) for each side plus an optional `filter: Option<Expr>`. DataFusion builds equality predicates from the columns.
- [`.join_on()`] â€” Pass the full join condition as `Expr`s. Internally this wraps [`.join()`] with empty key lists and a combined filter expression (`expr_1 AND expr_2 ...`). Optimizer passes then extract equality predicates and treat them as equi-join keys.

<!-- JOIN-TODO-004: Rewrite this claim; construction method does not guarantee a specific physical join algorithm. -->

After optimization, both methods produce equivalent plansâ€”**no performance difference** for standard equi-joins. However, [`.join()`] is the "safer" choice: you explicitly declare equi-join keys, guaranteeing hash/sort-merge algorithms. With [`.join_on()`], if the optimizer can't extract equality predicates from your expression, it may fall back to nested loop joins.

Pick whichever reads better for your use case.

**Gotcha: [`.join_on()`] uses AND, not OR**

Multiple expressions passed to [`.join_on()`] are combined with [`AND`]:

```rust
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let left = dataframe!("a" => [1], "b" => [2])?.alias("l")?;
    let right = dataframe!("a2" => [1], "b2" => [2])?.alias("r")?;
    // This means: a = a2 AND b = b2 (not OR!)
    let joined = left.join_on(right, JoinType::Inner, [col("l.a").eq(col("r.a2")), col("l.b").eq(col("r.b2"))])?;
    joined.show().await?;
    Ok(())
}
```

For [`OR`] logic, build a single expression:

```rust
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let left = dataframe!("a" => [1], "b" => [2])?.alias("l")?;
    let right = dataframe!("a2" => [1], "b2" => [2])?.alias("r")?;
    // Match if EITHER a or b matches
    let joined = left.join_on(right, JoinType::Inner, [col("l.a").eq(col("r.a2")).or(col("l.b").eq(col("r.b2")))])?;
    joined.show().await?;
    Ok(())
}
```

**Trade-off: DataFrame vs SQL**

| DataFrame API Advantages                                                                                                                                                     | SQL Advantages                                                          |
| ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------- |
| **First-class Semi/Anti joins** â€” `JoinType::LeftAnti`, `LeftSemi` etc. are explicit; no workarounds needed (unlike PySpark where you'd use `LEFT JOIN` + `WHERE IS NULL`) | **Visual clarity** â€” Multi-table joins read naturally in SQL syntax   |
| **Type-safe composition** â€” Build joins conditionally with `if/else`; compiler catches column typos                                                                        | **Familiar syntax** â€” Standard `ON` clause understood by any SQL user |
| **Chained transformations** â€” `.join().filter().select()` flows naturally                                                                                                  | Copy-paste ready\*\* â€” Test queries directly in SQL tools             |
| **Complex conditions** â€” [`.join_on()`] accepts any `Expr`, not just column equality                                                                                       | **Self-documenting** â€” SQL is often readable by non-programmers       |

> **DataFusion-specific advantage:** Unlike many DataFrame libraries, DataFusion exposes the _full_ set of join types ([`LeftSemi`], [`RightSemi`], [`LeftAnti`], [`RightAnti`], [`LeftMark`], [`RightMark`]) as first-class operationsâ€”no need to emulate anti-joins with outer joins and null checks.

<!-- JOIN-TODO-022: Decide whether source-system join guidance has a supported owner and scenario-specific evidence. -->

**Performance note:** <br>
For joins via row-based [`TableProvider`], consider whether the join should happen at the source. If both tables are in Postgres with foreign key indexes, the DB's index-backed joins may outperform transferring data to DataFusion. For cross-source joins or large analytical joins without indexes, DataFusion's hash/sort-merge algorithms excel.

---

<!-- JOIN-TODO-004 JOIN-TODO-008 JOIN-TODO-016 JOIN-TODO-018: Move composite keys to construction, schema-name handling to composition, and remove the unsupported temporal percentage claim. -->

## Match Multiple Key Columns

Join on multiple columns when a single key isn't enough to uniquely identify matchesâ€”common with composite keys or temporal constraints.

```rust
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    // Regional sales: same product_id can exist in different regions
    let inventory_df = dataframe!(
        "product_id" => [1, 1, 2, 2],
        "region" => ["East", "West", "East", "West"],
        "stock" => [100, 50, 200, 75]
    )?;

    // Use different column names to avoid duplicate field error
    let sales_df = dataframe!(
        "sale_product_id" => [1, 1, 2],
        "sale_region" => ["East", "West", "East"],
        "sold" => [30, 20, 80]
    )?;

    // Multi-key join: match on BOTH product_id AND region
    let joined = inventory_df.join(
        sales_df,
        JoinType::Left,  // Keep all inventory, even unsold
        &["product_id", "region"],
        &["sale_product_id", "sale_region"],
        None
    )?;

    joined.show().await?;
    // +------------+--------+-------+-----------------+-------------+------+
    // | product_id | region | stock | sale_product_id | sale_region | sold |
    // +------------+--------+-------+-----------------+-------------+------+
    // | 1          | East   | 100   | 1               | East        | 30   |
    // | 1          | West   | 50    | 1               | West        | 20   |
    // | 2          | East   | 200   | 2               | East        | 80   |
    // | 2          | West   | 75    |                 |             |      |
    // +------------+--------+-------+-----------------+-------------+------+

    Ok(())
}
```

> **Pro tip for time-dependent data:** <br>

<!-- JOIN-TODO-016: Delete or replace this unsupported temporal mismatch statistic with a scenario-specific, sourced example. -->

> Multi-key joins on temporal columns work well when truncated to appropriate granularity using [`date_trunc()`]. Joining on `DATE` (day) has minimal edge cases (~0.004% at midnight); joining on raw `TIMESTAMP` (milliseconds) risks silent mismatches.

---

<!-- JOIN-TODO-004: Move to `.join_on()` construction; explain qualified conditions, AND reduction, explicit OR, and optimizer extraction without method-level performance promises. -->

## Join with Complex Conditions

Sometimes you need more than simple column equality. Range joins ("orders placed within 7 days of signup"), inequality predicates ("amount > threshold"), or compound logic ("match on id AND status = 'active'") require expressions that [`.join()`] can't express with just column names.

[`.join_on()`] accepts arbitrary boolean expressions as join conditions. Internally it wraps [`.join()`] with empty key lists and passes your expressions as a filterâ€”the optimizer then extracts any equality predicates for efficient hash/sort-merge execution.

| Use Case           | Example Condition                                                       |
| ------------------ | ----------------------------------------------------------------------- |
| **Range join**     | `order_date.between(start_date, end_date)`                              |
| **Inequality**     | `col("amount").gt(col("threshold"))`                                    |
| **Compound logic** | `col("id").eq(col("customer_id")).and(col("status").eq(lit("active")))` |

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

    // Give DataFrames aliases so we can qualify column references
    let customers = customers_df.clone().alias("customers")?;
    let orders = orders_df.clone().alias("orders")?;

    // Join with compound condition: match on id AND filter amount > 100
    let high_value = customers.join_on(
        orders,
        JoinType::Inner,
        [col("customers.id").eq(col("orders.customer_id"))
            .and(col("orders.amount").gt(lit(100)))]
    )?;

    high_value.show().await?;
    // +----+-------+----------+-------------+--------+
    // | id | name  | order_id | customer_id | amount |
    // +----+-------+----------+-------------+--------+
    // | 1  | Alice | 102      | 1           | 200    |
    // | 2  | Bob   | 103      | 2           | 150    |
    // +----+-------+----------+-------------+--------+
    // Alice's order 101 (amount=100) excludedâ€”doesn't meet amount > 100
    // Carol excludedâ€”no orders at all

    Ok(())
}
```

> **Tip:** <br>
> When using [`.join_on()`], column names may clash between tables. Use [`.alias()`] to qualify references: `col("customers.id")` vs `col("orders.id")`.

<!-- JOIN-TODO-004 JOIN-TODO-006: Promote this correctness boundary within construction and show why an ON-like filter differs from a later `.filter()` for outer joins. -->

### The `filter` Argument on Outer Joins

The [`.join()`] method's fifth parameter is [`filter: Option<Expr>`][join_filter_param]â€”easy to overlook in the signature but powerful for outer joins. This filter has **subtle but important semantics**: it applies only to _matched_ rows, not to preserved unmatched rows.

This distinction matters because:

- A [`WHERE`] clause **after** the join would filter out unmatched rows (turning your Left Join into an Inner Join)
- The `filter` argument applies **during** the join, controlling which matches are considered valid while still preserving unmatched rows

| Approach                                      | Behavior                         | Result                              |
| --------------------------------------------- | -------------------------------- | ----------------------------------- |
| [`.join(..., Some(filter))`][`.join()`]       | Filter is part of join condition | Unmatched rows preserved with NULLs |
| [`.join(..., None).filter(...)`][`.filter()`] | Filter applied after join        | Unmatched rows may be removed       |

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

    // Left join with filter: Carol still appears, but only orders > 100 attach
    let result = customers_df.clone().join(
        orders_df.clone(),
        JoinType::Left,
        &["id"],                           // left_cols
        &["customer_id"],                  // right_cols
        Some(col("amount").gt(lit(100))),  // filter (5th param) - applied only to matched rows
    )?;

    result.show().await?;
    // +----+-------+----------+-------------+--------+
    // | id | name  | order_id | customer_id | amount |
    // +----+-------+----------+-------------+--------+
    // | 1  | Alice | 102      | 1           | 200    |  â†  > 100 attached
    // | 2  | Bob   | 103      | 2           | 150    |
    // | 3  | Carol |          |             |        |  â† Preserved!
    // +----+-------+----------+-------------+--------+
    // Alice's order 101 (amount=100) excluded by filter
    // Carol preserved because Left Join keeps all left rows

    Ok(())
}
```

> **Mental model:** <br>
> Think of `filter` as part of the _join condition_, not a `WHERE` after the join. It controls which matches are valid during the join itself.

> **Also applies to [`.join_on()`]:** <br>
> Since [`.join_on()`] is implemented as [`.join()`] with empty key lists and combined `on_exprs` as `filter`, the same semantics apply.

[`.alias()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.alias
[`.filter()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.filter
[`.join()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.join
[`.join_on()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.join_on
[join_filter_param]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.join "See the 'filter' parameter in the join() signature"
[`leftanti`]: https://docs.rs/datafusion/latest/datafusion/logical_expr/enum.JoinType.html#variant.LeftAnti "Left rows that have NO match (no right columns)"
[`leftsemi`]: https://docs.rs/datafusion/latest/datafusion/logical_expr/enum.JoinType.html#variant.LeftSemi "Left rows that have a match (no right columns)"
[`leftmark`]: https://docs.rs/datafusion/latest/datafusion/logical_expr/enum.JoinType.html#variant.LeftMark "Mark join for EXISTS subquery decorrelation"
[`rightanti`]: https://docs.rs/datafusion/latest/datafusion/logical_expr/enum.JoinType.html#variant.RightAnti
[`rightsemi`]: https://docs.rs/datafusion/latest/datafusion/logical_expr/enum.JoinType.html#variant.RightSemi
[`rightmark`]: https://docs.rs/datafusion/latest/datafusion/logical_expr/enum.JoinType.html#variant.RightMark "Mark join for EXISTS subquery decorrelation"
[`tableprovider`]: https://docs.rs/datafusion/latest/datafusion/catalog/trait.TableProvider.html
[`date_trunc()`]: https://docs.rs/datafusion/latest/datafusion/functions/datetime/expr_fn/fn.date_trunc.html
[`and`]: ../../../../user-guide/sql/operators.md#logical-operators
[`where`]: ../../../../user-guide/sql/select.md#where-clause
[`or`]: ../../../../user-guide/sql/operators.md#logical-operators
