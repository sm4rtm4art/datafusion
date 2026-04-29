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

<!--TODO

1. ABSTRACT
2. INTRODUCTION
-->

# Subqueries



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

**Subqueries embed one query inside another—enabling comparisons against computed values or filtered datasets.** <br>

Use subqueries when a filter or expression depends on data from another query. In the DataFrame API, you build subqueries by creating a [`LogicalPlan`] and wrapping it with the appropriate function.

| Subquery type | What it returns       | Expression function   | SQL example                                     |
| ------------- | --------------------- | --------------------- | ----------------------------------------------- |
| **Scalar**    | Single value          | [`scalar_subquery()`] | `WHERE amount > (SELECT AVG(amount) FROM t)`    |
| **IN**        | List membership       | [`in_subquery()`]     | `WHERE id IN (SELECT customer_id FROM premium)` |
| **EXISTS**    | Boolean (rows exist?) | [`exists()`]          | `WHERE EXISTS (SELECT 1 FROM orders WHERE ...)` |

> **Trade-off: DataFrame vs SQL**
>
> - **DataFrame shines:** Type-safe subquery construction; reusable subquery plans as variables; subqueries can be built conditionally
> - **SQL shines:** Nested syntax is more readable; familiar to SQL users; less boilerplate for simple cases

### Scalar Subqueries

A scalar subquery returns **exactly one value** used in comparisons. Common use cases: filtering against an aggregate (average, max, count) or looking up a single reference value.

**Pattern:** Build the subquery as a [`LogicalPlan`] via [`.into_unoptimized_plan()`], then wrap with [`scalar_subquery()`]:

```rust
use datafusion::prelude::*;
use datafusion::functions_aggregate::expr_fn::avg;
use std::sync::Arc;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();

    // Input: orders table
    // +----------+--------+
    // | order_id | amount |
    // +----------+--------+
    // | 1        | 100    |
    // | 2        | 200    |
    // | 3        | 150    |
    // | 4        | 300    |
    // +----------+--------+
    let orders = dataframe!(
        "order_id" => [1, 2, 3, 4],
        "amount" => [100, 200, 150, 300]
    )?;
    ctx.register_table("orders", orders.clone().into_view())?;

    // Build subquery: SELECT AVG(amount) FROM orders → 187.5
    let avg_subquery = ctx.table("orders").await?
        .aggregate(vec![], vec![avg(col("amount"))])?
        .select(vec![avg(col("amount"))])?
        .into_unoptimized_plan();

    // Filter: WHERE amount > (subquery)
    let result = ctx.table("orders").await?
        .filter(col("amount").gt(scalar_subquery(Arc::new(avg_subquery))))?;

    result.show().await?;
    // Output: orders where amount > 187.5
    // +----------+--------+
    // | order_id | amount |
    // +----------+--------+
    // | 2        | 200    |
    // | 4        | 300    |
    // +----------+--------+

    Ok(())
}
```

### IN Subqueries

An IN subquery checks if a value **exists in a list** returned by another query. Common use cases: filtering by membership in a lookup table, finding related records, or excluding specific IDs.

**Pattern:** Build the subquery returning a single column, wrap with `in_subquery(column, plan)`:

```rust
use datafusion::prelude::*;
use std::sync::Arc;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();

    // Input: customers and premium lookup tables
    // customers:              premium:
    // +----+-------+          +-------------+
    // | id | name  |          | customer_id |
    // +----+-------+          +-------------+
    // | 1  | Alice |          | 1           |
    // | 2  | Bob   |          | 3           |
    // | 3  | Carol |          +-------------+
    // +----+-------+
    let customers = dataframe!(
        "id" => [1, 2, 3],
        "name" => ["Alice", "Bob", "Carol"]
    )?;

    let premium_customers = dataframe!(
        "customer_id" => [1, 3]
    )?;

    ctx.register_table("customers", customers.into_view())?;
    ctx.register_table("premium", premium_customers.into_view())?;

    // Build subquery: SELECT customer_id FROM premium
    let premium_ids = ctx.table("premium").await?
        .select(vec![col("customer_id")])?
        .into_unoptimized_plan();

    // Filter: WHERE id IN (subquery)
    let result = ctx.table("customers").await?
        .filter(in_subquery(col("id"), Arc::new(premium_ids)))?;

    result.show().await?;
    // Output: customers where id IN (1, 3)
    // +----+-------+
    // | id | name  |
    // +----+-------+
    // | 1  | Alice |
    // | 3  | Carol |
    // +----+-------+

    Ok(())
}
```

---

### Summary: Shared Transformations

**Every operation in this chapter has a direct SQL equivalent—the logic is identical, only the syntax differs.** Both APIs compile to the same logical plan, so performance is equivalent.

| Operation        | DataFrame                            | SQL                  |
| ---------------- | ------------------------------------ | -------------------- |
| Select columns   | `.select()`, `.select_columns()`     | `SELECT`             |
| Filter rows      | `.filter()`                          | `WHERE`              |
| Aggregate        | `.aggregate()`                       | `GROUP BY`           |
| Join tables      | `.join()`                            | `JOIN`               |
| Sort results     | `.sort()`                            | `ORDER BY`           |
| Limit rows       | `.limit()`                           | `LIMIT`              |
| Set operations   | `.union()`, `.intersect()`           | `UNION`, `INTERSECT` |
| Window functions | `.window()` + builder                | `OVER (...)`         |
| Unnest arrays    | `.unnest_columns()`                  | `UNNEST`             |
| Subqueries       | `scalar_subquery()`, `in_subquery()` | `(SELECT ...)`       |

> **When to use which?**
>
> - **DataFrame API:** Complex logic with conditionals, reusable pipelines, compile-time checks, or when building queries programmatically.
> - **SQL API:** Ad-hoc exploration, familiar syntax, or when porting existing SQL queries.

**Next:** [DataFrame-Unique Methods](#dataframe-unique-methods) covers operations that have no direct SQL equivalent.

**See also:**

- [SQL SELECT reference](../../user-guide/sql/select.md) — detailed SQL syntax
- [Window Functions](../../user-guide/sql/window_functions.md) — all window functions
- [Subqueries](../../user-guide/sql/subqueries.md) — SQL subquery patterns

---
