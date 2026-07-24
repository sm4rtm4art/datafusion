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


<!--TODO (Stage 2 scaffold, approved 2026-07-18)

PAGE ROLE
- Action-oriented transformation leaf page.
- Follows filtering conceptually: retain rows by predicate → order surviving
  rows → take a bounded positional slice.
- Owns ordering and limiting usage; recaps the transformation contract only
  where needed for a jump-in reader.

OWNED METHODS
- `.sort_by(Vec<Expr>)`
- `.sort(Vec<SortExpr>)`
- `.limit(skip, fetch)`

OWNERSHIP BOUNDARIES
- `.distinct()` and `.distinct_on()` → `set-operations.md`
- `.show_limit()` and other execution/display actions → Writing DataFrames
- Ranking and per-group Top-N → `window-functions.md`
- Detailed Top-K and dynamic-filter internals → optimizer/architecture docs
- SQL `ORDER BY`, `LIMIT`, and `OFFSET` depth → SQL user guide

NARRATIVE
1. Establish an order.
2. Take a bounded slice.
3. Combine both for deterministic Top-N.

STAGE PLAN
- Stage 4 complete: `## Sorting Rows`, `## Limiting Rows`, and `## Returning Global Top-N Rows`.
- Stage 6 complete: opening block, body sections, and conclusion drafted.
- Remaining: final cleanup, transition decision, link audit, and repository validation.
-->

# Sorting and Limiting Rows

**Order rows, take bounded slices, and combine both transformations to return deterministic Top-N results.**

Analytical queries often need more than the right rows: they need those rows in a meaningful sequence and sometimes only a bounded portion of that sequence. In DataFusion, `.sort()` and `.sort_by()` establish order, while `.limit(skip, fetch)` skips rows and bounds the result without evaluating a predicate. This page shows how sort-key precedence, direction, null placement, and method order determine which rows are returned, then combines sorting and limiting for deterministic global Top-N results. Because limiting is positional, place it deliberately and establish a sufficiently complete sort order whenever the selected rows must be reproducible.

| Method | Purpose |
| --- | --- |
| [`.sort()`](#defining-a-sort-order) | Define sort-key precedence, direction, and null placement explicitly. |
| [`.sort_by()`](#defining-a-sort-order) | Apply the fixed `ASC NULLS LAST` policy to each sort expression. |
| [`.limit()`](#taking-a-bounded-slice) | Skip input rows and return an optionally bounded result. |

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

```{contents} Table of Contents for Sorting and Limiting Rows
:local:
:depth: 2
```

## Sorting Rows

**Sorting makes a DataFrame's row order explicit, so position-dependent results do not rely on an incidental output sequence.**

DataFusion does not treat an observed row sequence as an ordering contract. Establish an explicit sort whenever the result depends on which rows are first, last, highest, or lowest.

A sort order is an ordered list of key expressions: earlier keys establish precedence, and later keys resolve ties. The next section shows how the DataFrame API defines that order.

### Defining a Sort Order

**A sort order combines one or more key expressions with precedence, direction, and null placement.**

The DataFrame API provides [`.sort()`][sort-method] and [`.sort_by()`][sort-by-method]. Both add a sort operation to the DataFrame's `LogicalPlan`; rows are reordered only when the plan executes. Use [`.sort()`][sort-method] to control direction and null placement for each key, or [`.sort_by()`][sort-by-method] when every key should use its fixed `ASC NULLS LAST` policy.

Sort expressions are applied in vector order. The first expression is the primary key, and each later expression orders only rows that tie on every preceding key. DataFusion moves complete rows according to those keys rather than sorting columns independently.

:::{admonition} DataFrame sorting and SQL `ORDER BY`
:class: note

SQL uses [`ORDER BY`][sql-order-by] with `ASC`, `DESC`, `NULLS FIRST`, and `NULLS LAST`. The DataFrame API expresses these choices through [`.sort()`][sort-method] and [`SortExpr`][sort-expr-type]; [`.sort_by()`][sort-by-method] is the fixed `ASC NULLS LAST` convenience form.

:::

Pass [`.sort()`][sort-method] a `Vec<SortExpr>` containing the columns or other expressions that should order the rows. Each [`SortExpr`][sort-expr-type] defines the direction and null placement for one key. The [`Expr::sort()`][expr-sort-method] method constructs each `SortExpr`:

- `asc`: `true` sorts ascending; `false` sorts descending.
- `nulls_first`: `true` places nulls first; `false` places nulls last.

The following example sorts non-null amounts from highest to lowest, places a missing amount last, and uses `order_id` to resolve rows with equal amounts.

```rust
use datafusion::assert_batches_eq;
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let orders_df = dataframe!(
        "order_id" => [1003_i64, 1001, 1002, 1004],
        "product" => ["Keyboard", "Monitor", "Desk", "Mouse"],
        "amount" => vec![Some(125_i64), None, Some(125), Some(80)],
    )?;

    // Build the lazy plan.
    let sorted_df = orders_df.sort(vec![
        col("amount").sort(false, false),  // DESC, nulls last
        col("order_id").sort(true, false), // ASC, nulls last
    ])?;

    // Execute the plan and verify the requested order.
    let batches = sorted_df.collect().await?;
    assert_batches_eq!(
        &[
            "+----------+----------+--------+",
            "| order_id | product  | amount |",
            "+----------+----------+--------+",
            "| 1002     | Desk     | 125    |",
            "| 1003     | Keyboard | 125    |",
            "| 1004     | Mouse    | 80     |",
            "| 1001     | Monitor  |        |",
            "+----------+----------+--------+",
        ],
        &batches
    );

    Ok(())
}
```

When every key should use ascending order with nulls last, [`.sort_by()`][sort-by-method] removes the repeated option arguments. It accepts a `Vec<Expr>`, converts each expression to `expr.sort(true, false)`, and delegates to [`.sort()`][sort-method].

```rust
# use datafusion::assert_batches_eq;
# use datafusion::prelude::*;
#
# #[tokio::main]
# async fn main() -> datafusion::error::Result<()> {
# let orders_df = dataframe!(
#     "order_id" => [1003_i64, 1001, 1002],
#     "product" => ["Mouse", "Monitor", "Monitor"],
# )?;
let sorted_df =
    orders_df.sort_by(vec![col("product"), col("order_id")])?;

# let batches = sorted_df.collect().await?;
# assert_batches_eq!(
#     &[
#         "+----------+---------+",
#         "| order_id | product |",
#         "+----------+---------+",
#         "| 1001     | Monitor |",
#         "| 1002     | Monitor |",
#         "| 1003     | Mouse   |",
#         "+----------+---------+",
#     ],
#     &batches
# );
# Ok(())
# }
```

Use `.sort()` instead when any key needs descending order, nulls first, or another per-key policy.

:::{admonition} Ties require a complete ordering
:class: caution

Sort expressions resolve rows only until all supplied keys compare equally. When exact row order matters, add another expression that distinguishes the remaining ties; do not rely on the previous or incidental order of fully tied rows.

:::

---

## Limiting Rows

**Limiting takes a positional slice from a DataFrame, skipping rows first and returning at most the requested number.**

[`.limit()`][limit-method] changes a DataFrame's cardinality by adding a [`Limit`][limit-plan] operation to its `LogicalPlan`. Unlike [`.filter()`](filtering.md), it does not compare each row with a predicate. It skips rows from its input sequence and then returns up to an optional maximum.

The method's position in a DataFrame chain is part of the query. Calling `.limit()` before another transformation bounds that transformation's input; calling it afterward bounds the transformation's output. DataFusion may move or propagate a limit during optimization only when the rewritten plan preserves the same result.

At execution time, the physical limit skips rows from its input stream and, when `fetch` is `Some(n)`, forwards at most the next `n` rows.

:::{admonition} Place limits deliberately
:class: caution

A limit applies at its position in the logical plan. Placing `.limit()` before a join, aggregation, or sort restricts that operator's input; placing it afterward restricts the operator's output.

When `fetch` is `Some(n)`, DataFusion can stop consuming input after enough rows have been produced and may push the bound into eligible scans or operators. These optimizations preserve the query's result; manually moving `.limit()` may not.

:::

### Taking a Bounded Slice

**Use `.limit(skip, fetch)` to express SQL-style `OFFSET` and `LIMIT` in one DataFrame transformation.**

The first argument, `skip`, specifies how many input rows to discard before returning any rows. The second argument, `fetch`, sets the maximum number of following rows:

| Call | Result |
| --- | --- |
| `.limit(0, Some(n))` | Return at most `n` input rows. |
| `.limit(skip, Some(n))` | Skip `skip` rows, then return at most `n` rows. |
| `.limit(skip, None)` | Skip `skip` rows, then return all remaining rows. |

These arguments correspond to SQL [`OFFSET` and `LIMIT`][sql-limit]. The following example establishes an order by `order_id`, skips the first ordered row, and returns the next two rows.

```rust
use datafusion::assert_batches_eq;
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let orders_df = dataframe!(
        "order_id" => [1004_i64, 1001, 1003, 1002],
        "product" => ["Mouse", "Monitor", "Keyboard", "Desk"],
    )?;

    // Order the input before taking a positional slice.
    let limited_df = orders_df
        .sort_by(vec![col("order_id")])?
        .limit(1, Some(2))?;

    let batches = limited_df.collect().await?;
    assert_batches_eq!(
        &[
            "+----------+----------+",
            "| order_id | product  |",
            "+----------+----------+",
            "| 1002     | Desk     |",
            "| 1003     | Keyboard |",
            "+----------+----------+",
        ],
        &batches
    );

    Ok(())
}
```

:::{admonition} "First rows" require an explicit order
:class: caution

Without a sufficiently complete sort order, `.limit()` still bounds the result, but the selected rows do not represent a deterministic business-defined ordering such as the earliest, latest, highest, or lowest values.

:::


---

## Returning Global Top-N Rows

**Return the highest, lowest, earliest, or latest N rows by defining a global ordering and then applying `.limit()`.**

Many analytical queries need only a small globally ordered result, such as the highest-value orders or the latest events. The DataFrame API does not provide a separate Top-N method; compose [`.sort()`][sort-method] with [`.limit()`][limit-method] to define the ordering and then bound the result.

Neither transformation is sufficient by itself. `.sort()` defines the global ordering without reducing the row count, while `.limit()` bounds its input without defining what "top" means. Add enough tie-breaker expressions to distinguish rows at the N-row boundary.

The following example returns the three highest-value orders. `amount` defines the primary order, `order_id` resolves equal amounts, and `.limit(0, Some(3))` returns the first three rows of that order.

```rust
use datafusion::assert_batches_eq;
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let orders_df = dataframe!(
        "order_id" => [1005_i64, 1002, 1004, 1001, 1003],
        "product" => ["Chair", "Desk", "Monitor", "Keyboard", "Mouse"],
        "amount" => vec![
            Some(80_i64),
            Some(125),
            None,
            Some(125),
            Some(80),
        ],
    )?;

    let top_orders = orders_df
        .sort(vec![
            col("amount").sort(false, false),  // DESC, nulls last
            col("order_id").sort(true, false), // ASC tie-breaker
        ])?
        .limit(0, Some(3))?;

    let batches = top_orders.collect().await?;
    assert_batches_eq!(
        &[
            "+----------+----------+--------+",
            "| order_id | product  | amount |",
            "+----------+----------+--------+",
            "| 1001     | Keyboard | 125    |",
            "| 1002     | Desk     | 125    |",
            "| 1003     | Mouse    | 80     |",
            "+----------+----------+--------+",
        ],
        &batches
    );

    Ok(())
}
```

The method order is part of the query:

- **Global Top-N:** define a global order, then return its first N rows with `.sort(sort_exprs)?.limit(0, Some(n))?`.
- **Sorted subset:** select up to N input rows, then sort only that subset with `.limit(0, Some(n))?.sort(sort_exprs)?`.

The second form returns the same Top-N result only when the input already satisfies the full ordering defined by `sort_exprs`.

:::{admonition} Eligible plans may use TopK execution
:class: note

The DataFrame pattern `.sort(...).limit(0, Some(n))` corresponds to SQL `ORDER BY ... LIMIT n`; both use DataFusion's shared planning pipeline, where eligible bounded sorts may use the TopK execution path. Dynamic-filter pushdown can additionally help compatible scans skip irrelevant data, but the benefit depends on the physical plan, configuration, and data source; see [Dynamic Filters: Passing Information Between Operators During Execution][dynamic-filters-blog].

:::

---

## Conclusion

Sorting and limiting control different properties of a DataFrame result. [`.sort()`][sort-method] and [`.sort_by()`][sort-by-method] establish an explicit row order, while [`.limit()`][limit-method] takes a positional slice by skipping rows and optionally bounding how many rows follow.

Combined as `.sort(...).limit(...)`, these transformations return a global Top-N result. The sort expressions define what highest, lowest, earliest, or latest means, and a sufficiently complete sort order makes the N selected rows deterministic. Reversing the method order limits the input first and sorts only that subset, which is a different query.

### Further Reading

- See the SQL user guide for [`ORDER BY`][sql-order-by] and [`LIMIT` with `OFFSET`][sql-limit].
- Use [window functions](window-functions.md) for ranking and per-group Top-N results.
- See [set operations](set-operations.md) for duplicate-row removal with `.distinct()`.
- Execution and display actions such as `.collect()`, `.show()`, and `.show_limit()` belong with Writing DataFrames.
- For physical TopK execution and dynamic-filter pushdown, see [Dynamic Filters: Passing Information Between Operators During Execution][dynamic-filters-blog].

[sort-by-method]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.sort_by
[sort-method]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.sort
[expr-sort-method]: https://docs.rs/datafusion/latest/datafusion/logical_expr/enum.Expr.html#method.sort
[sql-order-by]: ../../../user-guide/sql/select.md#order-by-clause
[sort-expr-type]: https://docs.rs/datafusion/latest/datafusion/logical_expr/struct.SortExpr.html
[limit-method]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.limit
[limit-plan]: https://docs.rs/datafusion/latest/datafusion/logical_expr/logical_plan/struct.Limit.html
[sql-limit]: ../../../user-guide/sql/select.md#limit-and-offset-clauses
[dynamic-filters-blog]: https://datafusion.apache.org/blog/2025/09/10/dynamic-filters/
