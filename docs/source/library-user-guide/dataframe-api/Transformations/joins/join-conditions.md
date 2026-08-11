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
LOCAL TODO OWNERS: JOIN-TODO-001, JOIN-TODO-015, JOIN-TODO-019, and
JOIN-TODO-026.
-->
<!-- JOIN-TODO-001: Key Methods table added; finalize highlight/abstract polish with the leaf. -->

# Join Conditions

**Use [`.join()`] for named equality keys, [`.join_on()`] for Boolean conditions, and place predicates according to whether they define matches or filter joined rows.**

Build joins from named keys or Boolean conditions, then decide whether a predicate participates in matching or filters the joined result. The `.join()` `filter` argument adds an ON-like matching predicate, while a later [`.filter()`] is WHERE-like result filtering.

Use SQL when its `USING`, `NATURAL JOIN`, `CROSS JOIN`, or `LATERAL` forms express the relationship more directly than a DataFrame method.

**Key Methods**

| Method                                      | Purpose                                                                          |
| :------------------------------------------ | :------------------------------------------------------------------------------- |
| [`.join()`](#join-named-equality-keys)      | Match on paired named equality keys; optional `filter` adds an ON-like predicate |
| [`.join_on()`](#join_on-boolean-conditions) | Match on complete Boolean conditions                                             |
| [`.filter()`]                               | Filter joined rows afterward (WHERE-like); not the `.join()` `filter` argument   |

:::{admonition} Style Note
:class: note
:collapsible: open

In this document, code elements follow a consistent pattern:

- **DataFrame methods:** `.method()` (e.g., `.select()`, `.filter()`)
- **Standalone functions:** `function()` (e.g., `col()`, `lit()`)
- **Constructors:** `Type::new()` (e.g., `SessionContext::new()`)
- **Types:** `TypeName` (e.g., `SchemaRef`, `RecordBatch`)
- **Lazy transformations:** return a `DataFrame` and build the `LogicalPlan`
- **Actions:** (`.collect()`, `.show()`) trigger execution
- **Input roles:** Base DataFrame = method receiver/left input; Extension DataFrame = right argument/right input; `JoinType` controls preservation.
- **Filter terms:** `filter` = [`.join()`] argument participating in matching; [`.filter()`] = `DataFrame` method filtering rows at its pipeline position.

:::

```{contents} Table of Contents for Join Conditions
:local:
:depth: 2
```

## Join Methods: Matching Rows in the DataFrame API

**A join supplies two logical inputs, a `JoinType`, and a matching condition that identifies which row combinations can match.**

Joining two `DataFrame` inputs requires a left input, a right input, a `JoinType`, and a matching condition. The **Base DataFrame** is the method receiver and left input, while the **Extension DataFrame** is the right argument and right input; these are call-position mnemonics, and `JoinType` owns preservation and payload. Both public join methods build lazily.

SQL writes `FROM left JOIN right ON condition`, while the DataFrame API has no `ON` clause. [`.join()`] supplies paired named-column slices plus `filter: Option<Expr>`, and [`.join_on()`] accepts complete Boolean conditions; they are alternate builders for the same logical join.

(join-named-equality-keys)=

### `.join()`: Named Equality Keys

**[`.join()`] expresses equality between paired named columns, including composite keys.**

Use [`.join()`] when paired named columns express equality. Call [`.join()`] on the Base DataFrame, which becomes the left input, and pass the Extension DataFrame as its first argument, which becomes the right input. Then supply a `JoinType` and equal-position left and right key slices. The final `filter: Option<Expr>` argument is required by the Rust signature: pass `None` when no additional predicate exists, or `Some(expr)` for an additional ON-like matching predicate as the filter subsection below describes.

```rust
use datafusion::assert_batches_sorted_eq;
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let inventory_df = dataframe!(
        "product_id" => [1_i64, 1, 2],
        "region" => ["east", "west", "east"],
        "stock" => [100_i64, 50, 200]
    )?;
    let sales_df = dataframe!(
        "sale_product_id" => [1_i64, 2],
        "sale_region" => ["west", "east"],
        "sold" => [20_i64, 80]
    )?;

    // Pair each left key with the key at the same position on the right.
    let joined = inventory_df.join(
        sales_df,
        JoinType::Inner,
        &["product_id", "region"],
        &["sale_product_id", "sale_region"],
        // No additional ON-like predicate; matching uses only the paired keys.
        None,
    )?;

    let batches = joined.collect().await?;
    assert_batches_sorted_eq!(
        &[
            "+------------+--------+-------+-----------------+-------------+------+",
            "| product_id | region | stock | sale_product_id | sale_region | sold |",
            "+------------+--------+-------+-----------------+-------------+------+",
            "| 1          | west   | 50    | 1               | west        | 20   |",
            "| 2          | east   | 200   | 2               | east        | 80   |",
            "+------------+--------+-------+-----------------+-------------+------+",
        ],
        &batches
    );

    Ok(())
}
```

Unequal key-slice lengths and unresolved columns are planning errors that must be corrected. Choose [`.join_on()`] when a valid complete matching condition is clearer as Boolean expressions for non-equality, explicit `OR`, computed expressions, or null-safe logic; for null-safe matching, use [Null Handling](../../Concepts/null-handling.md#null-handling-in-join)'s `binary_expr(..., Operator::IsNotDistinctFrom, ...)` pattern.

#### [`.join()`] filter: `Option<Expr>`

[`.join()`] accepts an optional fifth `Option<Expr>` argument after its named equality-key slices. Despite the Rust parameter name `filter`, `.join(..., Some(expr))` is still ON-like matching, while [`.join_on()`] conditions also define matching. A subsequent [`.filter()`] is WHERE-like result filtering and may remove NULL-extended rows.

:::{admonition} Distinguish join and row filters
:class: caution

**Trap: the join `filter` parameter ≠ the [`.filter()`] method ≠ SQL `WHERE`.**

The comparison below maps match construction and result filtering to SQL clauses.

| DataFrame spelling / placement     | Role                          | SQL analogue              |
| ---------------------------------- | ----------------------------- | ------------------------- |
| [`.join()`] key slices             | Equality matching             | [`ON`][sql-join]-like     |
| [`.join()`] `filter: Option<Expr>` | Additional matching predicate | [`ON`][sql-join]-like     |
| [`.join_on([...])`][`.join_on()`]  | Complete Boolean condition    | [`ON`][sql-join]-like     |
| [`.filter()`] after a join         | Result-row filtering          | [`WHERE`][sql-where]-like |

:::

For a left join, a predicate such as `amount > 100` in the join condition keeps the NULL-extended row for a customer without an order, while the same predicate after joining removes it.

```rust
use datafusion::assert_batches_sorted_eq;
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    // Before: customers include Carol, who has no order.
    let customers_df = dataframe!(
        "id" => [1_i64, 2, 3],
        "name" => ["Alice", "Bob", "Carol"]
    )?;
    let orders_df = dataframe!(
        "order_id" => [101_i64, 102, 103],
        "customer_id" => [1_i64, 1, 2],
        "amount" => [100_i64, 200, 150]
    )?;

    // This predicate decides which left-join pairs match; Carol remains preserved.
    let join_filtered = customers_df.clone().join(
        orders_df.clone(),
        JoinType::Left,
        &["id"],
        &["customer_id"],
        Some(col("amount").gt(lit(100))),
    )?;
    // This predicate filters result rows, so Carol's NULL-extended row is removed.
    let post_join_filtered = customers_df
        .join(
            orders_df,
            JoinType::Left,
            &["id"],
            &["customer_id"],
            None,
        )?
        .filter(col("amount").gt(lit(100)))?;

    let join_filtered_batches = join_filtered.collect().await?;
    assert_batches_sorted_eq!(
        &[
            "+----+-------+----------+-------------+--------+",
            "| id | name  | order_id | customer_id | amount |",
            "+----+-------+----------+-------------+--------+",
            "| 1  | Alice | 102      | 1           | 200    |",
            "| 2  | Bob   | 103      | 2           | 150    |",
            "| 3  | Carol |          |             |        |",
            "+----+-------+----------+-------------+--------+",
        ],
        &join_filtered_batches
    );

    let post_join_filtered_batches = post_join_filtered.collect().await?;
    assert_batches_sorted_eq!(
        &[
            "+----+-------+----------+-------------+--------+",
            "| id | name  | order_id | customer_id | amount |",
            "+----+-------+----------+-------------+--------+",
            "| 1  | Alice | 102      | 1           | 200    |",
            "| 2  | Bob   | 103      | 2           | 150    |",
            "+----+-------+----------+-------------+--------+",
        ],
        &post_join_filtered_batches
    );

    Ok(())
}
```

(join_on-boolean-conditions)=

### `.join_on()`: Boolean Conditions

**[`.join_on()`] expresses complete Boolean conditions, including non-equality, explicit `OR`, and null-safe comparisons.**

Use [`.join_on()`] when matching cannot be represented solely by paired named equality keys or reads more clearly as a complete Boolean condition. It accepts complete Boolean conditions through `impl IntoIterator<Item = Expr>`; every condition must resolve to a Boolean, including non-equality conditions such as `col("orders.created_at").gt_eq(col("promotions.starts_at"))`.

Multiple iterator items are `AND`-combined, so build `OR` inside one `Expr`. For equality that treats `NULL` values as equal, construct the condition as described in [Null Handling](../../Concepts/null-handling.md#null-handling-in-join). Qualify column references when names could be ambiguous; aliases provide those qualifiers, while [join workflows](join-workflows.md) covers aliasing and schema-shaping recipes.

```rust
use datafusion::assert_batches_sorted_eq;
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let customers = dataframe!(
        "customer_id" => [1_i64, 2],
        "region" => ["east", "west"]
    )?
    .alias("customers")?;
    let accounts = dataframe!(
        "account_customer_id" => [1_i64, 3],
        "account_region" => ["east", "west"]
    )?
    .alias("accounts")?;

    // [AND] Both the customer ID and region must match.
    let all_conditions = customers.clone().join_on(
        accounts.clone(),
        JoinType::Inner,
        [
            col("customers.customer_id").eq(col("accounts.account_customer_id")),
            col("customers.region").eq(col("accounts.account_region")),
        ],
    )?;
    let batches = all_conditions.collect().await?;
    assert_batches_sorted_eq!(
        &[
            "+-------------+--------+---------------------+----------------+",
            "| customer_id | region | account_customer_id | account_region |",
            "+-------------+--------+---------------------+----------------+",
            "| 1           | east   | 1                   | east           |",
            "+-------------+--------+---------------------+----------------+",
        ],
        &batches
    );

    // [OR] Either the customer ID or region may match.
    let either_condition = col("customers.customer_id")
        .eq(col("accounts.account_customer_id"))
        .or(col("customers.region").eq(col("accounts.account_region")));

    let either_condition_join = customers.join_on(
        accounts,
        JoinType::Inner,
        [either_condition],
    )?;
    let batches = either_condition_join.collect().await?;
    assert_batches_sorted_eq!(
        &[
            "+-------------+--------+---------------------+----------------+",
            "| customer_id | region | account_customer_id | account_region |",
            "+-------------+--------+---------------------+----------------+",
            "| 1           | east   | 1                   | east           |",
            "| 2           | west   | 3                   | west           |",
            "+-------------+--------+---------------------+----------------+",
        ],
        &batches
    );

    Ok(())
}
```

Plain named equality is the more explicit [`.join()`] form. Non-Boolean, unresolved, or ambiguous expressions fail planning; qualify columns when needed.

---

## When the DataFrame API Has No Dedicated Join Form

**SQL directly names `USING`, `NATURAL JOIN`, `CROSS JOIN`, and `LATERAL`, for which the DataFrame API has no dedicated methods.**

Those four are the complete set of documented SQL join forms without a DataFrame counterpart; other SQL join types map to [`.join()`] / [`.join_on()`] and [`JoinType`][jointype]. This subsection stays deliberately short: it marks that boundary and points to SQL rather than re-teaching those forms here.

Use SQL when one of those forms makes the relationship clearer. `USING` is a condition spelling, not a join type. A conditionless inner join can represent a Cartesian product, but SQL `CROSS JOIN` states that intent directly.

Both APIs use the same planning and execution pipeline, so choose the interface that expresses the relationship more clearly rather than for speed. Use [hybrid SQL](../hybrid-sql.md) to cross the SQL/DataFrame boundary, the [SQL JOIN reference](../../../../user-guide/sql/select.md#join-clause) for syntax, and the [subquery reference](../../../../user-guide/sql/subqueries.md) for `LATERAL` context. Use [Join Validation](join-validation.md) to avoid accidental Cartesian products.

---

## Conclusion

Use [`.join()`] for paired named equality keys and [`.join_on()`] for complete Boolean conditions. Put matching predicates in the join like SQL `ON`, filter result rows afterward like `WHERE`, and use SQL when its dedicated join forms express the relationship more clearly.

### Further Reading

- [Join Concepts](join-concepts.md) — Predict join results through matching, preservation, cardinality, and payload.
- [Join Types](join-types.md#jointype-catalogue) — Choose which matched and unmatched rows survive.
- [Join Validation](join-validation.md) — Check coverage, row multiplication, Cartesian products, and plans.
- [Mixing SQL and DataFrames](../hybrid-sql.md) — Move between SQL and DataFrame pipelines.
- [Null Handling](../../Concepts/null-handling.md#null-handling-in-join) — Construct ordinary and null-safe equality conditions.
- [DataFusion SQL `JOIN` reference](../../../../user-guide/sql/select.md#join-clause) — Use DataFusion's supported SQL join syntax.

[`.filter()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.filter
[`.join()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.join
[`.join_on()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.join_on
[jointype]: https://docs.rs/datafusion/latest/datafusion/common/enum.JoinType.html
[sql-join]: ../../../../user-guide/sql/select.md#join-clause
[sql-where]: ../../../../user-guide/sql/select.md#where-clause
