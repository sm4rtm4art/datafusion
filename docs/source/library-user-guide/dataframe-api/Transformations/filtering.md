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

# Filtering Rows with Expressions

**The DataFrame API builds filtering rules as typed, composable [`Expr`] values, making null behavior and application-driven criteria explicit without interpolating runtime values into SQL expression text.**

Analytical applications rarely need every row from each input, and their filtering criteria are often determined by application state or user input. DataFusion narrows a `DataFrame` with [`.filter()`], using Boolean-compatible expressions that can be composed, validated against the input schema, and handled through Rust's normal `Result` flow. This page covers comparisons, Boolean composition, membership tests, ranges, patterns, null-aware rules, and predicates assembled from optional application inputs. It also explains why some predicate errors surface later and why filter pushdown is a performance optimization rather than a semantic guarantee.

**Key Methods**

| Method        | Purpose                                                         |
| :------------ | :-------------------------------------------------------------- |
| [`.filter()`] | Retain rows whose Boolean-compatible `Expr` evaluates to `true` |

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

```{contents} Table of Contents for Filtering Rows with Expressions
:local:
:depth: 2
```

## Construct Filtering Predicates

**Filtering applies predicate expressions to decide which rows survive, preserving the DataFrame's columns while potentially reducing its cardinality.**

Calling [`.filter()`] adds a filtering condition to the DataFrame's logical plan. The condition is represented as an `Expr` rather than evaluated immediately, so constructing the filtered `DataFrame` does not read the input data. An action such as [`.collect()`] later triggers planning and execution.

For the broader expression model, see [Expressions](../Concepts/expressions.md).

During execution, DataFusion evaluates the predicate against incoming record batches. Logically, the predicate produces one Boolean-compatible result for each input row: `true` keeps the row, while `false` or `NULL` discards it.

Filtering predicates range from individual comparisons to composed Boolean conditions, membership tests, ranges, patterns, and explicit null checks. The following sections build those forms using a shared `orders_df` dataset. DataFusion must resolve each predicate as Boolean-compatible; [Know When Predicate Errors Surface](#know-when-predicate-errors-surface) explains where invalid references or incompatible types fail.

| Filtering need                    | Representative expression                              |
| :-------------------------------- | :----------------------------------------------------- |
| Compare values                    | `col("amount").gt(lit(150))`                           |
| Combine Boolean conditions        | `high_value.and(multiple_items)`                       |
| Test membership, ranges, patterns | `in_list(...)`, `.between(...)`, `.like(...)`          |
| Handle missing values             | `.is_null()`, `.is_not_null()`, null-aware composition |

### Compare Values

**A comparison combines value expressions into the Boolean predicate required by [`.filter()`].**

[`col()`] references the values of an input column, while [`lit()`] represents a constant value. These are value expressions: neither one alone determines whether a row should remain. A comparison method combines compatible operands into a Boolean `Expr` that can be passed to [`.filter()`].

Use [`.eq()`] and [`.not_eq()`] for equality comparisons; [`.gt()`] and [`.gt_eq()`] for greater-than comparisons; and [`.lt()`] and [`.lt_eq()`] for less-than comparisons. The operands can be columns, literals, or larger compatible expressions.

This example keeps orders whose `amount` is greater than 150:

```rust
use datafusion::assert_batches_sorted_eq;
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let orders_df = dataframe!(
        "order_id" => [101, 102, 103, 104],
        "customer_id" => vec![Some(1), Some(1), Some(2), None],
        "product" => ["Widget", "Gadget", "Widget", "Gizmo"],
        "amount" => [100, 200, 150, 300],
        "quantity" => [2, 1, 3, 1]
    )?;

    let result = orders_df
        .filter(col("amount").gt(lit(150)))?
        .collect()
        .await?;

    assert_batches_sorted_eq!(
        &[
            "+----------+-------------+---------+--------+----------+",
            "| order_id | customer_id | product | amount | quantity |",
            "+----------+-------------+---------+--------+----------+",
            "| 102      | 1           | Gadget  | 200    | 1        |",
            "| 104      |             | Gizmo   | 300    | 1        |",
            "+----------+-------------+---------+--------+----------+",
        ],
        &result
    );

    Ok(())
}
```

:::{admonition} SQL comparison equivalents
:class: seealso

DataFrame comparison methods construct the same logical comparison operations expressed by SQL operators.

| Comparison            | DataFrame expression             | SQL expression  |
| :-------------------- | :------------------------------- | :-------------- |
| Equal                 | `col("amount").eq(lit(150))`     | `amount = 150`  |
| Not equal             | `col("amount").not_eq(lit(150))` | `amount <> 150` |
| Greater than          | `col("amount").gt(lit(150))`     | `amount > 150`  |
| Greater than or equal | `col("amount").gt_eq(lit(150))`  | `amount >= 150` |
| Less than             | `col("amount").lt(lit(150))`     | `amount < 150`  |
| Less than or equal    | `col("amount").lt_eq(lit(150))`  | `amount <= 150` |

The DataFrame API constructs an `Expr` directly. SQL parses the corresponding expression text into the same logical expression model. For APIs that parse SQL expressions within a DataFrame pipeline, see [SQL-Expression Bridge Methods](hybrid-sql.md#sql-expression-bridge-methods).
:::

### Combine Boolean Conditions

**Boolean composition combines complete predicates into one row-survival rule, making the intended grouping explicit when `AND`, `OR`, and `NOT` interact.**

A comparison answers one filtering question, but application rules commonly depend on several conditions. Use [`.and()`] when every condition must evaluate to `true`, [`.or()`] when any condition may evaluate to `true`, and [`.not()`] to invert a Boolean expression.

When a predicate mixes `AND` and `OR`, build and name its meaningful parts before combining them. Intermediate expressions expose the intended grouping directly and make later changes less likely to alter the business rule accidentally.

The following example keeps an order when either:

1. its `amount` is at least 150 and its `quantity` is at least 2; or
2. its `product` is `Gadget`.

```rust
use datafusion::assert_batches_sorted_eq;
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let orders_df = dataframe!(
        "order_id" => [101, 102, 103, 104],
        "customer_id" => vec![Some(1), Some(1), Some(2), None],
        "product" => ["Widget", "Gadget", "Widget", "Gizmo"],
        "amount" => [100, 200, 150, 300],
        "quantity" => [2, 1, 3, 1]
    )?;

    let multi_quantity_order = col("amount")
        .gt_eq(lit(150))
        .and(col("quantity").gt_eq(lit(2)));

    let gadget_order = col("product").eq(lit("Gadget"));
    let predicate = multi_quantity_order.or(gadget_order);

    let result = orders_df.filter(predicate)?.collect().await?;

    assert_batches_sorted_eq!(
        &[
            "+----------+-------------+---------+--------+----------+",
            "| order_id | customer_id | product | amount | quantity |",
            "+----------+-------------+---------+--------+----------+",
            "| 102      | 1           | Gadget  | 200    | 1        |",
            "| 103      | 2           | Widget  | 150    | 3        |",
            "+----------+-------------+---------+--------+----------+",
        ],
        &result
    );

    Ok(())
}
```

:::{admonition} SQL Boolean equivalents
:class: seealso

| DataFrame composition | SQL composition  |
| :-------------------- | :--------------- |
| `left.and(right)`     | `left AND right` |
| `left.or(right)`      | `left OR right`  |
| `predicate.not()`     | `NOT predicate`  |

The complete predicate above corresponds to:

```sql
WHERE (amount >= 150 AND quantity >= 2)
   OR product = 'Gadget'
```

:::

### Test Membership, Ranges, and Patterns

**Specialized predicate methods express membership, inclusive ranges, and text patterns directly, avoiding longer Boolean chains that obscure the filtering rule.**

Individual comparisons can represent these conditions manually. A set membership test could be written as several equality expressions joined with [`.or()`], while an inclusive range could be written using lower- and upper-bound comparisons joined with [`.and()`]. Those forms are valid, but the resulting expression describes the mechanics rather than the intended rule.

Use [`.in_list()`] when a value must belong to a finite set, [`.between()`] when it must fall within an inclusive interval, and [`.like()`] or [`.ilike()`] when text must match a SQL-style pattern.

| Filtering need           | DataFrame expression                    | Negated form                           |
| :----------------------- | :-------------------------------------- | :------------------------------------- |
| Membership               | `col("product").in_list(values, false)` | `col("product").in_list(values, true)` |
| Inclusive range          | `col("amount").between(low, high)`      | `col("amount").not_between(low, high)` |
| Case-sensitive pattern   | `col("product").like(pattern)`          | `col("product").not_like(pattern)`     |
| Case-insensitive pattern | `col("product").ilike(pattern)`         | `col("product").not_ilike(pattern)`    |

For [`.in_list()`], the Boolean argument controls whether the expression is negated: `false` represents `IN`, while `true` represents `NOT IN`. The [`.between()`] method includes both its lower and upper boundaries. Pattern expressions use `%` to match a sequence of characters and `_` to match one character.

The following example evaluates one predicate from each family against the shared `orders_df` dataset:

```rust
use datafusion::assert_batches_sorted_eq;
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let orders_df = dataframe!(
        "order_id" => [101, 102, 103, 104],
        "customer_id" => vec![Some(1), Some(1), Some(2), None],
        "product" => ["Widget", "Gadget", "Widget", "Gizmo"],
        "amount" => [100, 200, 150, 300],
        "quantity" => [2, 1, 3, 1]
    )?;

    let selected_products = orders_df
        .clone()
        .filter(col("product").in_list(
            vec![lit("Widget"), lit("Gizmo")],
            false,
        ))?
        .select(vec![col("order_id"), col("product")])?
        .collect()
        .await?;

    assert_batches_sorted_eq!(
        &[
            "+----------+---------+",
            "| order_id | product |",
            "+----------+---------+",
            "| 101      | Widget  |",
            "| 103      | Widget  |",
            "| 104      | Gizmo   |",
            "+----------+---------+",
        ],
        &selected_products
    );

    let selected_amounts = orders_df
        .clone()
        .filter(col("amount").between(lit(150), lit(200)))?
        .select(vec![col("order_id"), col("amount")])?
        .collect()
        .await?;

    assert_batches_sorted_eq!(
        &[
            "+----------+--------+",
            "| order_id | amount |",
            "+----------+--------+",
            "| 102      | 200    |",
            "| 103      | 150    |",
            "+----------+--------+",
        ],
        &selected_amounts
    );

    let matching_products = orders_df
        .filter(col("product").ilike(lit("g%")))?
        .select(vec![col("order_id"), col("product")])?
        .collect()
        .await?;

    assert_batches_sorted_eq!(
        &[
            "+----------+---------+",
            "| order_id | product |",
            "+----------+---------+",
            "| 102      | Gadget  |",
            "| 104      | Gizmo   |",
            "+----------+---------+",
        ],
        &matching_products
    );

    Ok(())
}
```

:::{admonition} SQL predicate equivalents
:class: seealso

| Filtering need  | DataFrame expression                        | SQL expression                   |
| :-------------- | :------------------------------------------ | :------------------------------- |
| Membership      | `col("product").in_list(values, false)`     | `product IN ('Widget', 'Gizmo')` |
| Inclusive range | `col("amount").between(lit(150), lit(200))` | `amount BETWEEN 150 AND 200`     |
| Pattern         | `col("product").ilike(lit("g%"))`           | `product ILIKE 'g%'`             |

The corresponding negated SQL forms are `NOT IN`, `NOT BETWEEN`, `NOT LIKE`, and `NOT ILIKE`. For APIs that parse SQL expressions within a DataFrame pipeline, see [SQL-Expression Bridge Methods](hybrid-sql.md#sql-expression-bridge-methods).
:::

These predicates remain nullable: missing operands or list values can affect whether the result is `true`, `false`, or `NULL`. The next section examines those null semantics explicitly.

### Account for NULL Results

**`NULL` represents an unknown predicate result—not `false`—so filtering logic must state whether missing values should be rejected, selected, or preserved.**

Comparisons, membership tests, ranges, and patterns can produce `NULL` when a nullable operand contains no value. DataFusion follows three-valued Boolean logic: a predicate can evaluate to `true`, `false`, or `NULL`, and [`.filter()`] retains only rows producing `true`.

| Predicate result | Meaning                            | Filter behavior |
| :--------------- | :--------------------------------- | :-------------- |
| `true`           | The condition is satisfied         | Keep the row    |
| `false`          | The condition is not satisfied     | Discard the row |
| `NULL`           | The condition cannot be determined | Discard the row |

Negation does not convert an unknown result into a match. When a predicate evaluates to `NULL`, calling [`.not()`] on it also produces `NULL`.

Use null-specific expressions to state the intended policy:

| Filtering intention                              | Expression                         |
| :----------------------------------------------- | :--------------------------------- |
| Select missing values                            | `col("customer_id").is_null()`     |
| Select known values                              | `col("customer_id").is_not_null()` |
| Test whether a predicate is unknown              | `predicate.is_unknown()`           |
| Keep predicate results that are `true` or `NULL` | `predicate.is_not_false()`         |

The following example first keeps only definite matches for customer `1`. It then changes the rule explicitly to retain both matching orders and orders whose customer is unknown.

```rust
use datafusion::assert_batches_sorted_eq;
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let orders_df = dataframe!(
        "order_id" => [101, 102, 103, 104],
        "customer_id" => vec![Some(1), Some(1), Some(2), None],
        "product" => ["Widget", "Gadget", "Widget", "Gizmo"],
        "amount" => [100, 200, 150, 300],
        "quantity" => [2, 1, 3, 1]
    )?;

    let customer_one = col("customer_id").eq(lit(1));

    let definite_matches = orders_df
        .clone()
        .filter(customer_one.clone())?
        .select(vec![col("order_id"), col("customer_id")])?
        .collect()
        .await?;

    assert_batches_sorted_eq!(
        &[
            "+----------+-------------+",
            "| order_id | customer_id |",
            "+----------+-------------+",
            "| 101      | 1           |",
            "| 102      | 1           |",
            "+----------+-------------+",
        ],
        &definite_matches
    );

    let matches_or_missing = orders_df
        .filter(customer_one.or(col("customer_id").is_null()))?
        .select(vec![col("order_id"), col("customer_id")])?
        .collect()
        .await?;

    assert_batches_sorted_eq!(
        &[
            "+----------+-------------+",
            "| order_id | customer_id |",
            "+----------+-------------+",
            "| 101      | 1           |",
            "| 102      | 1           |",
            "| 104      |             |",
            "+----------+-------------+",
        ],
        &matches_or_missing
    );

    Ok(())
}
```

The first predicate returns `NULL` for order 104 because its `customer_id` is missing, so that row does not survive. The second predicate makes the application rule explicit: an order survives when its customer equals `1` **or** its customer is unknown.

:::{admonition} SQL null-predicate equivalents
:class: seealso

| Filtering intention          | DataFrame expression               | SQL expression                   |
| :--------------------------- | :--------------------------------- | :------------------------------- |
| Select missing values        | `col("customer_id").is_null()`     | `customer_id IS NULL`            |
| Select known values          | `col("customer_id").is_not_null()` | `customer_id IS NOT NULL`        |
| Detect an unknown comparison | `predicate.is_unknown()`           | `(customer_id = 1) IS UNKNOWN`   |
| Keep true or unknown results | `predicate.is_not_false()`         | `(customer_id = 1) IS NOT FALSE` |

The explicit condition used in the example corresponds to:

```sql
WHERE customer_id = 1
   OR customer_id IS NULL
```

For APIs that parse SQL expressions within a DataFrame pipeline, see [SQL-Expression Bridge Methods](hybrid-sql.md#sql-expression-bridge-methods).
:::

Use [`.is_not_false()`] when every unknown result from a predicate should survive. Prefer an explicit `.or(...is_null())` branch when missingness in one particular column defines the business rule; it documents exactly which absent value receives special treatment.

Do not apply [`coalesce()`] as a generic correction. `coalesce(vec![predicate, lit(false)])` does not change filtering behavior because both `false` and `NULL` are already discarded. `coalesce(vec![predicate, lit(true)])` retains every unknown result, which may be broader than the intended policy.

---

## Build Predicates from Application Inputs

**Application inputs often determine which filters apply, so construct each supplied criterion as an `Expr`, combine the active expressions, and decide explicitly what no active criteria should mean.**

The preceding examples define complete predicates directly in the source code. Applications commonly receive filtering criteria later through request parameters, command-line options, configuration, or other user input. Some criteria may be present while others are absent.

These values are available when the application constructs the DataFrame's logical plan; they are not physical runtime filters created during query execution. Wrap each supplied value with [`lit()`] and combine it with the appropriate column expression.

### Combine Optional Criteria

**Convert each supplied criterion into an `Expr`, discard absent criteria, and reduce the remaining expressions into one predicate.**

A hard-coded predicate has a fixed shape, but an application may receive only some of its possible filtering values. Represent the resulting predicate as `Option<Expr>`: `Some(predicate)` means at least one criterion is active, while `None` means that no filtering criterion was supplied.

The following helper converts each present value into an expression, removes absent criteria, and combines the remaining expressions with [`.and()`]. The example supplies both a minimum amount and a maximum quantity.

```rust
use datafusion::assert_batches_sorted_eq;
use datafusion::prelude::*;

fn build_order_predicate(
    min_amount: Option<i32>,
    max_quantity: Option<i32>,
) -> Option<Expr> {
    [
        min_amount.map(|amount| col("amount").gt_eq(lit(amount))),
        max_quantity.map(|quantity| col("quantity").lt_eq(lit(quantity))),
    ]
    .into_iter()
    .flatten()
    .reduce(|left, right| left.and(right))
}

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let orders_df = dataframe!(
        "order_id" => [101, 102, 103, 104],
        "customer_id" => vec![Some(1), Some(1), Some(2), None],
        "product" => ["Widget", "Gadget", "Widget", "Gizmo"],
        "amount" => [100, 200, 150, 300],
        "quantity" => [2, 1, 3, 1]
    )?;

    let predicate = build_order_predicate(Some(150), Some(2));
    let filtered_df = match predicate {
        Some(predicate) => orders_df.filter(predicate)?,
        None => orders_df,
    };

    let result = filtered_df
        .select(vec![col("order_id"), col("amount"), col("quantity")])?
        .collect()
        .await?;

    assert_batches_sorted_eq!(
        &[
            "+----------+--------+----------+",
            "| order_id | amount | quantity |",
            "+----------+--------+----------+",
            "| 102      | 200    | 1        |",
            "| 104      | 300    | 1        |",
            "+----------+--------+----------+",
        ],
        &result
    );

    Ok(())
}
```

Because both values are present, the helper constructs `amount >= 150 AND quantity <= 2`. Passing `None` for either argument omits only that criterion. Passing `None` for both arguments leaves no expressions to reduce and therefore returns `None`.

:::{admonition} Keep application values typed
:class: tip

Use [`lit()`] to convert application values into typed literal expressions. Do not construct SQL fragments by interpolating those values into strings. For APIs that intentionally accept SQL expression text, see [SQL-Expression Bridge Methods](hybrid-sql.md#sql-expression-bridge-methods).
:::

### Choose the No-Criteria Behavior

**An empty set of criteria is an application-policy decision: it can mean no filtering, all rows, no rows, or an invalid request.**

`Option<Expr>` preserves the empty case until the application chooses its meaning. There is no universal default because different interfaces assign different semantics to missing or empty input.

| No-criteria policy | Representation                                | Appropriate meaning                                                      |
| :----------------- | :-------------------------------------------- | :----------------------------------------------------------------------- |
| Do not filter      | Return `None` and reuse the input `DataFrame` | Optional search criteria were omitted, so return all input rows          |
| Match all rows     | Use `lit(true)`                               | A surrounding helper requires an `Expr` even when no restriction applies |
| Match no rows      | Use `lit(false)`                              | An explicitly empty allow-list means that no value is permitted          |
| Reject the request | Return an application error                   | At least one filtering criterion is required                             |

Also distinguish an absent criterion from a present but empty value. For example, no product criterion may mean “include every product,” while an explicitly empty list of allowed products may mean “include no products.” Define that behavior at the application boundary before constructing the predicate.

---

## Understand Validation and Filter Pushdown

**Filtering is lazy but not validation-free: DataFusion may reject a predicate while constructing the logical plan, and later optimizers may rewrite or push that predicate without changing which rows qualify.**

Calling [`.filter()`] records a logical row-selection requirement and validates it where possible. Later planning determines how the predicate executes and whether it can move closer to the data source. For the complete path from logical-plan construction to execution, see [Execution Lifecycle](../Concepts/execution-lifecycle.md).

### Know When Predicate Errors Surface

**Predicate validation is incremental: some mistakes are rejected while [`.filter()`] builds the logical plan, while others surface during later planning or execution.**

DataFusion resolves predicates against the current input schema and checks their result type where enough information is available. Errors requiring type coercion, physical-expression conversion, or actual input values may surface only during later planning or execution.

| Problem                           | Where it may first surface            | Why                                                         |
| :-------------------------------- | :------------------------------------ | :---------------------------------------------------------- |
| Missing or ambiguous column       | Calling [`.filter()`]                 | The expression is resolved against the current input schema |
| Resolved non-Boolean predicate    | Calling [`.filter()`]                 | A logical filter requires a Boolean-compatible result       |
| Incompatible or unresolved types  | Logical analysis or physical planning | Additional type coercion or physical conversion is required |
| Data-dependent expression failure | Execution triggered by an action      | The failure depends on values in an input batch             |

These are possible earliest boundaries, not a fixed error schedule. Propagate both `orders_df.filter(predicate)?` and actions such as `filtered_df.collect().await?`.

### Treat Filter Pushdown as Conditional

**Filter pushdown is a provider capability and optimizer decision, not a guarantee attached to every [`.filter()`] call.**

During optimization, DataFusion may simplify predicates, merge adjacent filters, move conditions closer to their inputs, or offer them to a table provider so that fewer rows are retrieved.

Table providers report their support for each offered predicate through [`TableProviderFilterPushDown`]:

| Provider response | Provider behavior                                   | DataFusion behavior                                |
| :---------------- | :-------------------------------------------------- | :------------------------------------------------- |
| `Unsupported`     | Does not apply the predicate during retrieval       | Evaluates the filter outside the provider scan     |
| `Inexact`         | Uses the predicate but may return nonmatching rows  | Retains a residual filter to guarantee correctness |
| `Exact`           | Guarantees that returned rows satisfy the predicate | Does not require an additional residual filter     |

The resulting plan can take any of these conceptual shapes:

```text
Unsupported:
Filter: amount > 150
  TableScan: orders

Exact:
TableScan: orders, filters=[amount > 150]

Inexact:
Filter: amount > 150
  TableScan: orders, filters=[amount > 150]
```

Predicate placement depends on the provider, expression, intervening operators, optimizer rules, and configuration. Treat these as conceptual shapes rather than stable formatted output.

:::{admonition} Pushdown affects cost, not filtering semantics
:class: important

A query must return the same qualifying rows whether a predicate is pushed into the provider, evaluated by a residual `Filter`, or split between both locations. Pushdown can reduce data retrieval and downstream processing, but application correctness must not depend on it.

Inspect the optimized plan for the actual table provider and DataFusion version when predicate placement matters for performance.
:::

---

## Conclusion

**Filtering adds a predicate to the logical plan so that only rows evaluating to `true` remain, preserving the DataFrame's columns while potentially reducing its cardinality.**

Construct predicates from comparisons, Boolean composition, membership tests, ranges, patterns, and explicit null policies. When filtering criteria come from application inputs, build typed `Expr` values for the active criteria and define the no-criteria behavior explicitly. Propagate errors both while constructing the logical plan and when an action triggers later planning or execution.

Filter pushdown may reduce the amount of data read and processed, but it remains a provider capability and optimizer decision; it does not change which rows qualify. After filtering narrows the dataset, use sorting and limiting to control the order and number of rows returned. Continue with [Sorting and Limiting](sorting-limiting.md).

<!-- DataFusion types -->

[`expr`]: https://docs.rs/datafusion/latest/datafusion/logical_expr/enum.Expr.html
[`tableproviderfilterpushdown`]: https://docs.rs/datafusion/latest/datafusion/logical_expr/enum.TableProviderFilterPushDown.html

<!-- DataFrame methods -->

[`.filter()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.filter
[`.collect()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.collect

<!-- Expression constructors and methods -->

[`col()`]: https://docs.rs/datafusion/latest/datafusion/logical_expr/fn.col.html
[`lit()`]: https://docs.rs/datafusion/latest/datafusion/logical_expr/fn.lit.html
[`.eq()`]: https://docs.rs/datafusion/latest/datafusion/logical_expr/enum.Expr.html#method.eq
[`.not_eq()`]: https://docs.rs/datafusion/latest/datafusion/logical_expr/enum.Expr.html#method.not_eq
[`.gt()`]: https://docs.rs/datafusion/latest/datafusion/logical_expr/enum.Expr.html#method.gt
[`.gt_eq()`]: https://docs.rs/datafusion/latest/datafusion/logical_expr/enum.Expr.html#method.gt_eq
[`.lt()`]: https://docs.rs/datafusion/latest/datafusion/logical_expr/enum.Expr.html#method.lt
[`.lt_eq()`]: https://docs.rs/datafusion/latest/datafusion/logical_expr/enum.Expr.html#method.lt_eq
[`.and()`]: https://docs.rs/datafusion/latest/datafusion/logical_expr/enum.Expr.html#method.and
[`.or()`]: https://docs.rs/datafusion/latest/datafusion/logical_expr/enum.Expr.html#method.or
[`.not()`]: https://docs.rs/datafusion/latest/datafusion/logical_expr/enum.Expr.html#method.not
[`.in_list()`]: https://docs.rs/datafusion/latest/datafusion/logical_expr/enum.Expr.html#method.in_list
[`.between()`]: https://docs.rs/datafusion/latest/datafusion/logical_expr/enum.Expr.html#method.between
[`.like()`]: https://docs.rs/datafusion/latest/datafusion/logical_expr/enum.Expr.html#method.like
[`.ilike()`]: https://docs.rs/datafusion/latest/datafusion/logical_expr/enum.Expr.html#method.ilike
[`.is_not_false()`]: https://docs.rs/datafusion/latest/datafusion/logical_expr/enum.Expr.html#method.is_not_false
[`coalesce()`]: https://docs.rs/datafusion/latest/datafusion/functions/expr_fn/fn.coalesce.html
