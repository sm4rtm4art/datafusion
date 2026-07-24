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
MOVE HANDSHAKE: Result-schema shaping, self-join, and chained multi-way join
material arrived from ../joins.md. The migration source remains unchanged for
coordinator comparison.

LOCAL TODO OWNERS: JOIN-TODO-001, JOIN-TODO-004, JOIN-TODO-008,
JOIN-TODO-014, JOIN-TODO-015, JOIN-TODO-016, JOIN-TODO-018, JOIN-TODO-019,
JOIN-TODO-021, JOIN-TODO-025, and JOIN-TODO-026.
-->
<!-- JOIN-TODO-001: Add the title-line highlighting sentence, abstract, Key Methods table, first-H2 framing, and conclusion after this leaf stabilizes. -->
<!-- JOIN-TODO-025: Register this leaf as a doctest after Author approval. -->
# Join Workflows

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

```{contents} Table of Contents for Join Workflows
:local:
:depth: 2
```

<!-- JOIN-TODO-004 JOIN-TODO-008 JOIN-TODO-016 JOIN-TODO-018: Schema-shaping material split from the inherited multi-key subtree; distinguish qualification and ambiguity from actual schema errors. -->
## Shape the Result Schema

**To avoid duplicate columns**, use different column names on the right side, then select only what you need:

```rust
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let inventory_df = dataframe!(
        "product_id" => [1, 1, 2, 2],
        "region" => ["East", "West", "East", "West"],
        "stock" => [100, 50, 200, 75]
    )?;

    // Use different column names for join keys on right side
    let sales_df = dataframe!(
        "sale_product_id" => [1, 1, 2],
        "sale_region" => ["East", "West", "East"],
        "sold" => [30, 20, 80]
    )?;

    let joined = inventory_df.join(
        sales_df,
        JoinType::Left,
        &["product_id", "region"],
        &["sale_product_id", "sale_region"],
        None
    )?;

    // Select only the columns you need (left-side keys + data)
    let result = joined.select(vec![
        col("product_id"),
        col("region"),
        col("stock"),
        col("sold"),
    ])?;

    result.show().await?;
    // +------------+--------+-------+------+
    // | product_id | region | stock | sold |
    // +------------+--------+-------+------+
    // | 1          | East   | 100   | 30   |
    // | 1          | West   | 50    | 20   |
    // | 2          | East   | 200   | 80   |
    // | 2          | West   | 75    |      |  ← No sales
    // +------------+--------+-------+------+

    Ok(())
}
```

**⚠️ Handling Same-Named Columns**

DataFusion's [`.join()`] preserves columns from both sides. When join keys share names, use one of these patterns:

| Pattern                        | When to use                                                |
| ------------------------------ | ---------------------------------------------------------- |
| **[`.select()`] after join**   | Simple joins—pick the columns you need                     |
| **[`.alias()`] before join**   | Complex multi-way joins—qualify with `col("alias.column")` |
| **[`.with_column_renamed()`]** | Rename conflicting columns before joining                  |

**Tip:** Call [`.schema()`] after joining to see actual column names.

---

(self-joins-and-qualified-columns)=

<!-- JOIN-TODO-008: Move self-joins to composition after the shared aliasing, qualification, renaming, and projection guidance. -->
## Join a DataFrame to Itself

A **self-join** joins a table with itself—essential for hierarchical data. Left Join preserves all rows even if they have no match (like Alice, who has no referrer).

```rust
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    // Extended customers: add referred_by (which customer referred them)
    let customers_referrals = dataframe!(
        "id" => [1, 2, 3],
        "name" => ["Alice", "Bob", "Carol"],
        "referred_by" => [None::<i64>, Some(1), Some(1)]  // Alice referred Bob and Carol
    )?;

    // Self-join: alias both sides to disambiguate
    let customer = customers_referrals.clone().alias("customer")?;
    let referrer = customers_referrals.clone().alias("referrer")?;

    let with_referrers = customer.join(
        referrer,
        JoinType::Left,  // Keep customers without referrers (Alice)
        &["referred_by"],
        &["id"],
        None
    )?.select(vec![
        col("customer.name").alias("customer"),
        col("referrer.name").alias("referred_by"),
    ])?;

    with_referrers.show().await?;
    // +----------+-------------+
    // | customer | referred_by |
    // +----------+-------------+
    // | Alice    | NULL        |  ← No referrer
    // | Bob      | Alice       |
    // | Carol    | Alice       |
    // +----------+-------------+

    Ok(())
}
```

**Key pattern:** <br>
Use [`.alias()`] to create two "views" of the same DataFrame, then join with qualified column names (`customer.name`, `referrer.name`).

**Common self-join patterns:**

- **Hierarchy traversal:** employees → managers, categories → parent categories
- **Sequential comparison:** this_year.sales vs last_year.sales (join on product_id)
- **Finding pairs:** "Which products are often bought together?" (order_items self-join)

---

<!-- JOIN-TODO-008 JOIN-TODO-016: Move to composition; teach preservation and readability at each leg without prescribing a physical build side or unsupported planning benefits. -->
## Chain Joins Across Multiple DataFrames

**Chain [`.join()`] calls to combine 3+ tables—each join produces a new DataFrame that feeds into the next.**

Real-world data is often normalized across multiple tables. A business question like "which customers have paid orders?" requires combining customers → orders → payments. Each chained Inner Join acts as a filter—only rows matching _all_ join conditions survive.

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

    // Introduce a payments table: only orders 101 and 103 have payment records
    // payments_df:
    // +------------------+---------+
    // | payment_order_id | status  |
    // +------------------+---------+
    // | 101              | paid    |
    // | 103              | pending |
    // +------------------+---------+
    let payments_df = dataframe!(
        "payment_order_id" => [101, 103],
        "status" => ["paid", "pending"]
    )?;

    // 3-way join: customers → orders → payments
    // Use .select() to keep only the columns we need (avoids duplicates)
    let result = customers_df.clone()
        .join(orders_df.clone(), JoinType::Inner, &["id"], &["customer_id"], None)?
        .join(payments_df.clone(), JoinType::Inner, &["order_id"], &["payment_order_id"], None)?
        .select(vec![ // for better visibility of the result df
            col("name"),
            col("order_id"),
            col("amount"),
            col("status"),
        ])?;

    result.show().await?;
    // +-------+----------+--------+---------+
    // | name  | order_id | amount | status  |
    // +-------+----------+--------+---------+
    // | Alice | 101      | 100    | paid    |
    // | Bob   | 103      | 150    | pending |
    // +-------+----------+--------+---------+
    //
    // What got filtered out:
    // - Alice's order 102: no payment record
    // - Carol: no orders at all
    // - Order 104: orphan (customer_id 99 doesn't exist)

    Ok(())
}
```

> **Tip:** <br>
> Use Left Joins at intermediate steps if you need to preserve unmatched rows (e.g., customers without payments).

<!-- JOIN-TODO-016: Keep logical sequencing/readability advice only; verify or remove optimizer-reordering, build-right, and planning-overhead prescriptions. -->
### Join Order Matters

The order you chain joins affects both **readability** and **performance**. General principles:

| Principle                                     | Why                                                                        |
| --------------------------------------------- | -------------------------------------------------------------------------- |
| **Start with your "main" table (Left-Table)** | Reads naturally: "customers with their orders and payments"                |
| **Filter early**                              | Reduces intermediate result size before expensive joins                    |
| **Smaller tables on the right**               | Hash joins build from the right side—smaller = faster                      |
| **Let the optimizer help**                    | DataFusion may reorder joins, but good initial order reduces planning work |

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

    let payments_df = dataframe!(
        "payment_order_id" => [101, 103],
        "status" => ["paid", "pending"]
    )?;

    // ✅ Good: Start with the table you're "asking about"
    // "Which customers have payments?"
    let result = customers_df.clone()
        .join(orders_df.clone(), JoinType::Inner, &["id"], &["customer_id"], None)?
        .join(payments_df.clone(), JoinType::Inner, &["order_id"], &["payment_order_id"], None)?;

    result.show().await?;

    // ✅ Also good: Start with filtered data to reduce intermediate size
    let high_value_orders = orders_df.clone().filter(col("amount").gt(lit(100)))?;
    let result = high_value_orders
        .join(customers_df.clone(), JoinType::Inner, &["customer_id"], &["id"], None)?
        .join(payments_df.clone(), JoinType::Inner, &["order_id"], &["payment_order_id"], None)?;

    result.show().await?;

    Ok(())
}
```

> **Performance tip:** <br>
> The optimizer reorders joins when beneficial, but good initial ordering reduces planning overhead. Use [`.explain()`] to see the actual execution plan.

<!-- JOIN-TODO-008 JOIN-TODO-018: Merge with the shared result-schema guidance and distinguish qualification/ambiguity from actual schema errors. -->
### Managing Column Proliferation

Multi-way joins accumulate columns from every table. With each join, you get **all columns from both sides**—including duplicate key columns. Chain [`.select()`] at the end to keep only what you need:

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

    let payments_df = dataframe!(
        "payment_order_id" => [101, 103],
        "status" => ["paid", "pending"]
    )?;

    // Without .select(): 2 + 3 + 2 = 7 columns (with redundant id, customer_id, order_id, payment_order_id)
    // With .select(): pick only the 4 columns that matter
    let result = customers_df.clone()
        .join(orders_df.clone(), JoinType::Inner, &["id"], &["customer_id"], None)?
        .join(payments_df.clone(), JoinType::Inner, &["order_id"], &["payment_order_id"], None)?
        .select(vec![     // <--- This select removes the clutter
            col("name").alias("customer"),
            col("order_id"),
            col("amount"),
            col("status").alias("payment_status"),
        ])?;

    result.show().await?;
    // +----------+----------+--------+----------------+
    // | customer | order_id | amount | payment_status |
    // +----------+----------+--------+----------------+
    // | Alice    | 101      | 100    | paid           |
    // | Bob      | 103      | 150    | pending        |
    // +----------+----------+--------+----------------+

    Ok(())
}
```

> **When SQL might be clearer:** <br>
> Multi-way joins with 4+ tables can become hard to read as chained method calls. Consider [`SessionContext::sql()`] for complex [star-schema queries][databricks_star_schema] where SQL's visual structure helps.

[`.alias()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.alias
[`.explain()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.explain
[`.join()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.join
[`.schema()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.schema
[`.select()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.select
[`.with_column_renamed()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.with_column_renamed
[`sessioncontext::sql()`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.sql
[databricks_star_schema]: https://www.databricks.com/glossary/star-schema
