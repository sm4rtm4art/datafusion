<!--
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

# Expressions: The Building Blocks of Queries

```{contents} Table of Contents for Expressions
:local:
:depth: 2
```

**If `DataFrame` is the container and `LogicalPlan` holds the relational operators (Filter, Join, Projection), then `Expr` represents the row-level logic _inside_ those operators.**

Every time you write `col("amount").gt(lit(1000))`, you're building an expression tree — a small program that DataFusion evaluates against each batch of data at execution time. Understanding `Expr` connects the "what you write" (DataFrame methods) with "what DataFusion executes" (optimized computations over Arrow arrays).

---

## What is an `Expr`?

An `Expr` (short for "expression") is a tree structure describing a computation. Each node in the tree is a variant of the `Expr` enum — a column reference, a literal value, a binary operation, a function call, and so on. DataFusion assembles these trees when you call DataFrame methods like `.filter()`, `.select()`, or `.aggregate()`.

For example, the filter condition `amount > 1000 AND region = 'EMEA'` becomes:

```text
            ┌─────────────────┐
            │    BinaryExpr   │
            │     op: AND     │
            └─────────────────┘
                 ▲         ▲
         ┌───────┘         └────────┐
         │                          │
┌─────────────────┐      ┌─────────────────┐
│   BinaryExpr    │      │   BinaryExpr    │
│    op: >        │      │    op: =        │
└─────────────────┘      └─────────────────┘
     ▲       ▲                ▲         ▲
     │       │                │         │
  Col      Lit             Col        Lit
 "amount"  1000          "region"   "EMEA"
```

This is the same tree structure used by compilers and databases — DataFusion's optimizer can rearrange, simplify, and push down these trees before any data is processed.

---

## How DataFrame Methods Use `Expr`

DataFrame transformation methods accept `Expr` arguments. The builder functions `col()`, `lit()`, and operator methods like `.gt()` and `.eq()` construct these expression trees for you:

| DataFrame Method   | What it accepts                      | What happens to the `Expr`                   |
| :----------------- | :----------------------------------- | :------------------------------------------- |
| `.filter(expr)`    | A single boolean `Expr`              | Wraps the plan in a `Filter` node            |
| `.select(exprs)`   | A `Vec<Expr>` of column expressions  | Wraps the plan in a `Projection` node        |
| `.aggregate(g, a)` | Group-by `Expr`s + aggregate `Expr`s | Wraps the plan in an `Aggregate` node        |
| `.sort(exprs)`     | Sort expressions with ordering       | Wraps the plan in a `Sort` node              |
| `.with_column(n, e)` | A name and an `Expr`              | Adds a computed column via `Projection`      |

Each call adds a new `LogicalPlan` node containing the `Expr` trees you provided — the DataFrame grows lazily, no data moves.

---

## Building Expressions: The Common Patterns

The `datafusion::prelude` module exports the most commonly used expression builders:

```rust
use datafusion::prelude::*;
use datafusion::error::Result;
use datafusion::functions_aggregate::expr_fn::sum;

#[tokio::main]
async fn main() -> Result<()> {
    let df = dataframe!(
        "region" => ["EMEA", "APAC", "EMEA", "APAC"],
        "product" => ["Widget", "Widget", "Gadget", "Gadget"],
        "amount" => [1500, 800, 2000, 1200]
    )?;

    // col("name") — reference a column by name
    // lit(value)  — embed a literal value
    // .gt(), .eq(), .and() — build comparison/logic trees
    let filtered = df.filter(
        col("amount").gt(lit(1000))
            .and(col("region").eq(lit("EMEA")))
    )?;

    // Aggregate expressions: group-by columns + aggregate functions
    let aggregated = filtered.aggregate(
        vec![col("product")],
        vec![sum(col("amount")).alias("total_amount")],
    )?;

    aggregated.show().await?;
    // +----------+--------------+
    // | product  | total_amount |
    // +----------+--------------+
    // | Widget   | 1500         |
    // | Gadget   | 2000         |
    // +----------+--------------+

    Ok(())
}
```

**Key builder functions:**

| Function            | Creates                        | Example                                 |
| :------------------ | :----------------------------- | :-------------------------------------- |
| `col("name")`       | Column reference               | `col("amount")`                         |
| `lit(value)`         | Literal value                  | `lit(1000)`, `lit("EMEA")`              |
| `.alias("name")`    | Renames the expression output  | `sum(col("x")).alias("total")`          |
| `.gt()`, `.lt()`, `.eq()` | Comparison operators      | `col("a").gt(lit(10))`                  |
| `.and()`, `.or()`   | Logical combinators            | `expr1.and(expr2)`                      |
| `cast(expr, type)`  | Type conversion                | `cast(col("ts"), DataType::Timestamp(..))` |

---

## Expressions Are Dialect-Agnostic

`Expr` trees are independent of SQL syntax — they represent pure computation. Whether you build an `Expr` via `col("a").gt(lit(10))` or DataFusion parses it from `WHERE a > 10`, the resulting `Expr` tree is identical. This is why the SQL and DataFrame APIs produce the same `LogicalPlan`: both construct the same `Expr` trees, just through different interfaces.

The optimizer works on `Expr` trees directly — simplifying `lit(1) + lit(2)` to `lit(3)`, pushing filters through joins, and eliminating redundant computations — regardless of how the expressions were originally constructed.

---

## When Schema Validation Happens

`Expr` trees reference columns by name (e.g., `col("amount")`), but DataFusion does **not** validate these names when you build the expression. Validation happens later, during **logical planning**, when the `Expr` is resolved against the plan's `DFSchema`. If a column doesn't exist or a type mismatch occurs, you'll get a runtime error at planning time — not at expression construction time.

> **Best practice:** <br>
> Use `.schema()` on your DataFrame to inspect available columns before building complex expressions. For details on schema inspection and validation, see [Schema Management](../Schema-Management/index.md).

---

> **Further reading:** <br>
>
> - [Working with Exprs](../../working-with-exprs.md) — implementation guide for custom UDFs and expression rewriting
> - [Expressions guide](../../user-guide/expressions.md) — expression syntax and usage patterns
> - [`Expr` API docs](https://docs.rs/datafusion-expr/latest/datafusion_expr/expr/enum.Expr.html) — full enum reference
