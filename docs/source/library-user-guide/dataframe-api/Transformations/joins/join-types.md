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
MOVE HANDSHAKE: Join-family orientation and inner, outer, semi, and anti join
material arrived from ../joins.md. The migration source remains unchanged for
coordinator comparison.

LOCAL TODO OWNERS: JOIN-TODO-001, JOIN-TODO-006, JOIN-TODO-007,
JOIN-TODO-009, JOIN-TODO-014, JOIN-TODO-015, JOIN-TODO-016, JOIN-TODO-017,
JOIN-TODO-019, JOIN-TODO-021, JOIN-TODO-025, and JOIN-TODO-026.
-->
<!-- JOIN-TODO-001: Add the title-line highlighting sentence, abstract, Key Methods table, first-H2 framing, and conclusion after this leaf stabilizes. -->
<!-- JOIN-TODO-007: Keep public mark joins as a bounded specialist note until a supported workflow is approved. -->
<!-- JOIN-TODO-025: Register this leaf as a doctest after Author approval. -->

# Join Types

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

```{contents} Table of Contents for Join Types
:local:
:depth: 2
```

<!-- JOIN-TODO-006 JOIN-TODO-007 JOIN-TODO-015: Rebuild this as the single preservation decision table and add only a bounded specialist note for mark joins. -->

## Choose What the Join Preserves

Joins control how rows from two tables are matched and combined. The key decisions are:

1.  what happens to rows that _don't_ match
2.  which columns appear in the result.

Inner joins discard non-matches; outer joins preserve them with NULLs. Semi and Anti joins answer existence questions without adding columns from the right table.

| Join Type            | Returns                   | Use Case                                    |
| :------------------- | :------------------------ | :------------------------------------------ |
| [`Inner`]            | Matches from both sides   | Standard joinâ€”only matching rows          |
| [`Left`]             | All left + matching right | Keep all left rows (NULL if no match)       |
| [`Right`]            | All right + matching left | Keep all right rows (NULL if no match)      |
| [`Full`]             | Everything from both      | See all data, matched or not                |
| [`LeftSemi`]         | Left rows WITH matches    | "Which left rows have a match?"             |
| [`LeftAnti`]         | Left rows WITHOUT matches | "Which left rows have NO match?"            |
| ~~Cross~~ (SQL only) | Cartesian product         | All combinations (see Anti-Pattern section) |

> **Note:** The DataFrame API has no `JoinType::Cross`. Cartesian products are represented as `Inner` joins with empty key lists or as [`CROSS JOIN`] in SQL.

> **Learn more:** You may want to check out this source [Join tutorial] or [Semi and Anti joins explained].

---

<!-- JOIN-TODO-006 JOIN-TODO-009: Rename and place this under preservation semantics; explain possible row multiplication and route coverage checks to validation. -->

## Return Matching Rows with an Inner Join

**An Inner join emits one combined row per matching key pair and drops every row that has no match on the other side.**

The example data is deliberately imperfect: Carol has no orders, and order 104 carries `customer_id = 99`, which no customer row matches — so neither appears in the result.

```rust
use datafusion::prelude::*;
use datafusion::assert_batches_sorted_eq;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    // customers_df [id, name] — Carol has no orders
    let customers_df = dataframe!(
        "id" => [1, 2, 3],
        "name" => ["Alice", "Bob", "Carol"]
    )?;

    // orders_df [order_id, customer_id, amount] — order 104 has no customer
    let orders_df = dataframe!(
        "order_id" => [101, 102, 103, 104],
        "customer_id" => [1, 1, 2, 99],
        "amount" => [100, 200, 150, 300]
    )?;

    // build lazy plan: keep only rows that match on both sides
    let matched_df = customers_df.join(
        orders_df,
        JoinType::Inner,
        &["id"],
        &["customer_id"],
        None,
    )?;

    // execute
    let batches = matched_df.collect().await?;
    assert_batches_sorted_eq!(
        &[
            "+----+-------+----------+-------------+--------+",
            "| id | name  | order_id | customer_id | amount |",
            "+----+-------+----------+-------------+--------+",
            "| 1  | Alice | 101      | 1           | 100    |",
            "| 1  | Alice | 102      | 1           | 200    |",
            "| 2  | Bob   | 103      | 2           | 150    |",
            "+----+-------+----------+-------------+--------+",
        ],
        &batches
    );

    Ok(())
}
```

**When to use Inner Join:**

- **Enrich data** — Attach related information (customer details → their orders)
- **Filter by relationship** — Keep only rows that have a match on the other side
- **Combine normalized tables** — Reassemble data split across multiple tables

Comparing whole rows between two `DataFrame`s with identical schemas is a set operation rather than a join: use [`.intersect()`] for that. For intersection and difference, see [Set Operations](../set-operations.md#intersection-and-difference).

:::{admonition} Survivorship bias hides the rows a join dropped
:class: caution

Inner, Semi, and Anti joins remove non-matching rows without reporting them: Carol and order 104 are simply absent above. Chain several such joins and the loss compounds, because each step keeps only the rows that survived the previous one — the classic [survivorship bias][survivorship_bias].

To see what a join discarded, reach for the type that preserves the side you care about: an [Outer Join](#intermediate-leftrightfull-joins) marks the missing side with `NULL`, `Left` keeps rows `Inner` would drop, and `Right` does the same for the other input.
:::

---

(intermediate-leftrightfull-joins)=

<!-- JOIN-TODO-006 JOIN-TODO-016: Consolidate outer-join preservation and null extension here; remove the unsupported "~90%" claim. -->

## Preserve Unmatched Rows with Outer Joins

Where Inner Join keeps only the intersection (rows matching on both sides), **"partial" outer joins (left, right and full) preserve rows that don't match**â€”filling missing columns with `NULL`. This makes data gaps visible instead of silently dropping them.

| Join Type | Keeps                                           | Typical Use Case                                          |
| :-------- | :---------------------------------------------- | :-------------------------------------------------------- |
| **Left**  | All left rows, matching right data if available | Customer reportsâ€”keep all customers, show orders if any |
| **Right** | All right rows, matching left data if available | Orphan detectionâ€”find orders without valid customers    |
| **Full**  | Everything from both sides                      | Data reconciliationâ€”find ALL discrepancies              |

<!-- JOIN-TODO-016: Remove the unsourced "~90%" generalization; retain only neutral selection guidance. -->

Left Join handles ~90% of outer join use cases. Right Join can usually be rewritten as Left Join by swapping tables. Full Join is for reconciliation scenarios.

### Left Join â€” Enrich Your Primary Data

Keep **all rows from the left table**, enrich with matching data from the right table. No match? Right-side columns become `NULL`.

```rust
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    // customers_df:
    // +----+-------+
    // | id | name  |
    // +----+-------+
    // | 1  | Alice |
    // | 2  | Bob   |
    // | 3  | Carol |  â† Has no orders
    // +----+-------+
    let customers_df = dataframe!(
        "id" => [1, 2, 3],
        "name" => ["Alice", "Bob", "Carol"]
    )?;

    // orders_df:
    // +----------+-------------+--------+
    // | order_id | customer_id | amount |
    // +----------+-------------+--------+
    // | 101      | 1           | 100    |
    // | 102      | 1           | 200    |
    // | 103      | 2           | 150    |
    // | 104      | 99          | 300    |  â† Orphan
    // +----------+-------------+--------+
    let orders_df = dataframe!(
        "order_id" => [101, 102, 103, 104],
        "customer_id" => [1, 1, 2, 99],
        "amount" => [100, 200, 150, 300]
    )?;

    // "All customers with their orders (if any)"
    let left_result = customers_df.clone().join(
        orders_df.clone(),
        JoinType::Left,
        &["id"],
        &["customer_id"],
        None
    )?;

    left_result.show().await?;
    // +----+-------+----------+-------------+--------+
    // | id | name  | order_id | customer_id | amount |
    // +----+-------+----------+-------------+--------+
    // | 1  | Alice | 101      | 1           | 100    |
    // | 1  | Alice | 102      | 1           | 200    |
    // | 2  | Bob   | 103      | 2           | 150    |
    // | 3  | Carol |          |             |        |  â† Preserved with NULLs
    // +----+-------+----------+-------------+--------+

    Ok(())
}
```

### Right Join â€” Find Orphaned Records

Keep all rows from the right tableâ€”useful for finding records that reference non-existent parents (like order 104 referencing customer 99).

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

    // "All orders, showing customer info (if customer exists)"
    let right_result = customers_df.clone().join(
        orders_df.clone(),
        JoinType::Right,
        &["id"],
        &["customer_id"],
        None
    )?;

    right_result.show().await?;
    // +----+-------+----------+-------------+--------+
    // | id | name  | order_id | customer_id | amount |
    // +----+-------+----------+-------------+--------+
    // | 1  | Alice | 101      | 1           | 100    |
    // | 1  | Alice | 102      | 1           | 200    |
    // | 2  | Bob   | 103      | 2           | 150    |
    // |    |       | 104      | 99          | 300    |  â† Orphan! No customer 99
    // +----+-------+----------+-------------+--------+

    Ok(())
}
```

**Tip:** <br>
Right Join is just Left Join with swapped tables. `A.join(B, Right)` = `B.join(A, Left)`. Most teams use Left Join exclusively for consistencyâ€”put your "main" table first.

### Full Join â€” Complete Reconciliation

Keep **all rows from both tables**. Where there's no match, fill the "other side" with NULLs. This is the only join that guarantees you see *everything*â€”matched, unmatched left, AND unmatched right.

**When to use Full Join:**

- **Data reconciliation** â€” Comparing two data sources to find ALL discrepancies
- **Migration validation** â€” Ensuring old and new systems have the same records
- **Audit trails** â€” "Show me what's in A but not B, what's in B but not A, and what's in both"

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

    // "Show me everything: matched, unmatched left, AND unmatched right"
    let full_result = customers_df.clone().join(
        orders_df.clone(),
        JoinType::Full,
        &["id"],
        &["customer_id"],
        None
    )?;

    full_result.show().await?;
    // +----+-------+----------+-------------+--------+
    // | id | name  | order_id | customer_id | amount |
    // +----+-------+----------+-------------+--------+
    // | 1  | Alice | 101      | 1           | 100    |  â† Matched
    // | 1  | Alice | 102      | 1           | 200    |  â† Matched
    // | 2  | Bob   | 103      | 2           | 150    |  â† Matched
    // | 3  | Carol |          |             |        |  â† Left only (no orders)
    // |    |       | 104      | 99          | 300    |  â† Right only (orphan)
    // +----+-------+----------+-------------+--------+

    Ok(())
}
```

**Data Quality Pattern:** Full Join + NULL filters = powerful reconciliation tool:

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

    let full_result = customers_df.join(
        orders_df,
        JoinType::Full,
        &["id"],
        &["customer_id"],
        None
    )?;

    // Find customers WITHOUT orders (left-only)
    let inactive = full_result.clone().filter(col("order_id").is_null())?;

    // Find orphaned orders (right-only, invalid customer_id)
    let orphans = full_result.clone().filter(col("id").is_null())?;

    // Find matched records (both sides present)
    let matched = full_result.filter(
        col("id").is_not_null().and(col("order_id").is_not_null())
    )?;

    inactive.show().await?;
    orphans.show().await?;
    matched.show().await?;

    Ok(())
}
```

This pattern is invaluable for ETL pipelines, data migration validation, and debugging referential integrity issues.

---

<!-- JOIN-TODO-006 JOIN-TODO-007 JOIN-TODO-017: Reframe these as existence/non-existence preservation choices; keep right variants, bound mark variants, and remove unconditional efficiency claims. -->

## Test for Matches with Semi and Anti Joins

**What makes them special?** <br>
Semi and Anti joins are **filtering joins**â€”they filter the left table based on existence in the right table, but **never add columns** from the right table. This is fundamentally different from Inner/Left/Right/Full joins which combine data.

| Join Type    | Question                         | Returns                           | SQL Equivalent                               |
| :----------- | :------------------------------- | :-------------------------------- | :------------------------------------------- |
| **LeftSemi** | "Which left rows HAVE a match?"  | Left columns only, matched rows   | `WHERE EXISTS (SELECT 1 FROM right ...)`     |
| **LeftAnti** | "Which left rows have NO match?" | Left columns only, unmatched rows | `WHERE NOT EXISTS (SELECT 1 FROM right ...)` |

**Why use them instead of alternatives?**

| Alternative                | Problem                                                             | Semi/Anti Advantage                                                |
| :------------------------- | :------------------------------------------------------------------ | :----------------------------------------------------------------- |
| Inner Join + Distinct      | Creates duplicates if right has multiple matches, then removes them | Semi join handles this automaticallyâ€”one output row per left row |
| Left Join + WHERE NULL     | Joins everything first, then filters                                | Anti join filters during joinâ€”more efficient                     |
| `IN (SELECT ...)` subquery | Can be slower, harder to optimize                                   | Semi join is the optimized physical plan for `IN`                  |

### LeftSemi â€” "Which Rows Have Matches?"

Returns left rows that have **at least one match** in the right table. Even if a customer has 10 orders, they appear only once.

```rust
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    // customers_df:                 orders_df:
    // +----+-------+                +----------+-------------+--------+
    // | id | name  |                | order_id | customer_id | amount |
    // +----+-------+                +----------+-------------+--------+
    // | 1  | Alice |                | 101      | 1           | 100    |
    // | 2  | Bob   |                | 102      | 1           | 200    |
    // | 3  | Carol |                | 103      | 2           | 150    |
    // +----+-------+                | 104      | 99          | 300    |
    //                               +----------+-------------+--------+
    let customers_df = dataframe!(
        "id" => [1, 2, 3],
        "name" => ["Alice", "Bob", "Carol"]
    )?;

    let orders_df = dataframe!(
        "order_id" => [101, 102, 103, 104],
        "customer_id" => [1, 1, 2, 99],
        "amount" => [100, 200, 150, 300]
    )?;

    // "Which customers have placed at least one order?"
    let active_customers = customers_df.clone().join(
        orders_df.clone(),
        JoinType::LeftSemi,
        &["id"],
        &["customer_id"],
        None
    )?;

    active_customers.show().await?;
    // +----+-------+
    // | id | name  |
    // +----+-------+
    // | 1  | Alice |  â† Has 2 orders, appears once
    // | 2  | Bob   |  â† Has 1 order
    // +----+-------+
    // Note: Carol (id=3) excludedâ€”no orders
    // Note: No order columns! Just filtered customers.

    Ok(())
}
```

**Use cases for LeftSemi:**

- Find active customers (have placed orders)
- Find products that have been sold (exist in order_items)
- Filter to "things that are referenced somewhere"

### LeftAnti â€” "Which Rows Have No Matches?"

Returns left rows that have **zero matches** in the right table. The inverse of Semi join.

```rust
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    // customers_df:                 orders_df:
    // +----+-------+                +----------+-------------+--------+
    // | id | name  |                | order_id | customer_id | amount |
    // +----+-------+                +----------+-------------+--------+
    // | 1  | Alice |                | 101      | 1           | 100    |
    // | 2  | Bob   |                | 102      | 1           | 200    |
    // | 3  | Carol |                | 103      | 2           | 150    |
    // +----+-------+                | 104      | 99          | 300    |
    //                               +----------+-------------+--------+
    let customers_df = dataframe!(
        "id" => [1, 2, 3],
        "name" => ["Alice", "Bob", "Carol"]
    )?;

    let orders_df = dataframe!(
        "order_id" => [101, 102, 103, 104],
        "customer_id" => [1, 1, 2, 99],
        "amount" => [100, 200, 150, 300]
    )?;

    // "Which customers have NEVER placed an order?"
    let inactive_customers = customers_df.clone().join(
        orders_df.clone(),
        JoinType::LeftAnti,
        &["id"],
        &["customer_id"],
        None
    )?;

    inactive_customers.show().await?;
    // +----+-------+
    // | id | name  |
    // +----+-------+
    // | 3  | Carol |  â† No orders found
    // +----+-------+
    // Alice and Bob excludedâ€”they have orders

    Ok(())
}
```

**Use cases for LeftAnti:**

- Find inactive customers (never ordered)
- Find dead inventory (products never sold)
- Data cleanup: "Find records missing required relationships"
- Complement of Semi: `Semi âˆª Anti = Full Left Table`

### Why Not Just Use Left Join + Filter?

A common question: "Can't I just do Left Join and filter for NULLs?"

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

    // âŒ Less efficient: Join everything, then filter
    let inactive_v1 = customers_df.clone()
        .join(orders_df.clone(), JoinType::Left, &["id"], &["customer_id"], None)?
        .filter(col("order_id").is_null())?;

    // âœ… More efficient: Anti join filters during the join
    let inactive_v2 = customers_df.clone()
        .join(orders_df.clone(), JoinType::LeftAnti, &["id"], &["customer_id"], None)?;

    // Both produce the same result:
    // +----+-------+
    // | id | name  |
    // +----+-------+
    // | 3  | Carol |
    // +----+-------+
    inactive_v1.show().await?;
    inactive_v2.show().await?;

    Ok(())
}
```

Both produce the same result, but Anti join:

- Doesn't create intermediate joined rows
- Doesn't add (and then ignore) right-side columns
- Optimizer can use more efficient algorithms (e.g., hash-based existence check)

> **Learn more:** See [Semi and Anti joins explained] for why these deserve first-class syntax in SQL.

> **Other variants:** <br>
> DataFusion's [`JoinType`] also includes `RightSemi`, `RightAnti`, and mark variants for advanced use cases. For most DataFrame work, stick to left variants and swap input tables if needed.

> **Mark joins:** <br> > [`LeftMark`]/[`RightMark`] are used internally to decorrelate `EXISTS` subqueries. They return all rows from one side plus an extra boolean "mark" column indicating whether any match exists on the other side. Most DataFrame code won't use them directly, but you may see them in `EXPLAIN` plans for complex SQL with `EXISTS` predicates.

[`.intersect()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.intersect
[`jointype`]: https://docs.rs/datafusion/latest/datafusion/logical_expr/enum.JoinType.html
[`full`]: https://docs.rs/datafusion/latest/datafusion/logical_expr/enum.JoinType.html#variant.Full "All rows from both tables (NULL where no match)"
[`inner`]: https://docs.rs/datafusion/latest/datafusion/logical_expr/enum.JoinType.html#variant.Inner "Only rows with matches in both tables"
[`left`]: https://docs.rs/datafusion/latest/datafusion/logical_expr/enum.JoinType.html#variant.Left "All left rows + matching right rows (NULL if no match)"
[`right`]: https://docs.rs/datafusion/latest/datafusion/logical_expr/enum.JoinType.html#variant.Right "All right rows + matching left rows (NULL if no match)"
[`leftanti`]: https://docs.rs/datafusion/latest/datafusion/logical_expr/enum.JoinType.html#variant.LeftAnti "Left rows that have NO match (no right columns)"
[`leftsemi`]: https://docs.rs/datafusion/latest/datafusion/logical_expr/enum.JoinType.html#variant.LeftSemi "Left rows that have a match (no right columns)"
[`leftmark`]: https://docs.rs/datafusion/latest/datafusion/logical_expr/enum.JoinType.html#variant.LeftMark "Mark join for EXISTS subquery decorrelation"
[`rightmark`]: https://docs.rs/datafusion/latest/datafusion/logical_expr/enum.JoinType.html#variant.RightMark "Mark join for EXISTS subquery decorrelation"
[`cross join`]: ../../../../user-guide/sql/select.md#cross-join
[join tutorial]: https://blog.jooq.org/say-no-to-venn-diagrams-when-explaining-joins/ "Why Venn diagrams mislead when explaining joins"
[semi and anti joins explained]: https://blog.jooq.org/semi-join-and-anti-join-should-have-its-own-syntax-in-sql/ "Why Semi/Anti joins deserve first-class syntax"
[survivorship_bias]: https://en.wikipedia.org/wiki/Survivorship_bias
[set operations]: ../set-operations.md
