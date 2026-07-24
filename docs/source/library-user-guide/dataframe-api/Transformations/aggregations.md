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

# Aggregating Rows

**Aggregation turns row-level data into analytical answers by combining related rows into summaries that filtering alone cannot produce.**

Analytical questions often require one result from several related rows rather than one result for every input row. SQL expresses these analyses with aggregate functions and optional clauses such as `GROUP BY`; DataFusion's DataFrame API creates the same grouped or whole-input summaries with [`.aggregate()`][aggregate-method], grouping expressions, and aggregate expressions. This page shows how to choose the result grain, select aggregate expressions with the intended count and contribution semantics, and position filters before or after aggregation. It also identifies common cases where a valid aggregation can still answer the wrong question.

:::{admonition} Style Note
:class: note
:collapsible: closed

In this document, code elements follow a consistent pattern:

- **DataFrame methods:** `.method()` (e.g., `.select()`, `.filter()`)
- **Standalone functions:** `function()` (e.g., `col()`, `lit()`)
- **Constructors:** `Type::new()` (e.g., `SessionContext::new()`)
- **Types:** `TypeName` (e.g., `SchemaRef`, [`RecordBatch`])
- **Lazy transformations:** return a `DataFrame` and build the`LogicalPlan`
- **Actions:** (`.collect()`, `.show()`) trigger execution

:::

```{contents} Table of Contents for Aggregating Rows
:local:
:depth: 2
```

## Changing the Grain with `.aggregate()`

**Aggregation changes the grain of a [`DataFrame`][dataframe-type]: [`.aggregate()`][aggregate-method] turns detail rows into one summary row for each group—or one row for the entire input.**

Expressions used by transformations such as [`.select()`][select-method] and [`.filter()`][filter-method] evaluate each input row to produce or test a value. Some analytical questions instead depend on values from multiple rows, such as totals, averages, and counts. Aggregate functions also produce [`Expr`][expr-type] values, but [`.aggregate()`][aggregate-method] evaluates those expressions across related rows rather than independently for each row.

**Grain** describes what one row in a [`DataFrame`][dataframe-type] represents. Before aggregation, each row represents an individual record at the input grain. [`.aggregate()`][aggregate-method] replaces that grain with a summary grain: each output row represents either one distinct combination of grouping-expression values or, when [`group_expr`][aggregate-method] is empty, the complete input. Choosing [`group_expr`][aggregate-method] therefore determines what each output row means and how many summary rows the operation can produce. See [transformation concepts](transformation-concepts.md) for the broader grain model and its relationship to other transformations.

```text
┌─────────────────────────────┐
│ Input grain                 │
│ one row per input record    │
└──────────────┬──────────────┘
               ▼
┌─────────────────────────────────────────┐
│ .aggregate(group_expr, aggr_expr)       │
│                                         │
│ group_expr → defines the groups         │
│ aggr_expr  → computes each summary      │
└──────────────┬──────────────────────────┘
               ▼
┌─────────────────────────────┐
│ Summary grain               │
│ one row per group           │
│ or one row for all input    │
└─────────────────────────────┘
```

The two expression vectors passed to [`.aggregate()`][aggregate-method] separate the operation into two decisions: [`group_expr`][aggregate-method] **defines groups**, while [`aggr_expr`][aggregate-method] **computes summaries**. Together, they determine what each output row represents and which summary values it contains.

:::{admonition} DataFrame and SQL APIs
:class: note

SQL expresses aggregation with functions such as `AVG`, `MIN`, `MAX`, and
`COUNT`, together with an optional `GROUP BY` clause. The DataFrame API is
well suited to grouping and aggregate expressions assembled from Rust values,
functions, or control flow; SQL is often more concise for a fixed declarative
query. Both APIs use the same DataFusion optimizer and execution engine.

:::

### Using Grouping and Aggregate Expressions

**The complete [`group_expr`][aggregate-method] vector defines each output group, while [`aggr_expr`][aggregate-method] computes one or more summary values for every group.**

One expression in [`group_expr`][aggregate-method] produces one result row for each distinct key. Multiple grouping expressions produce one result row for each distinct combination of their values. Each expression in [`aggr_expr`][aggregate-method] adds a summary column at that grain.

| Method argument | Representative expressions | Purpose |
| :-------------- | :------------------------- | :------ |
| [`group_expr`][aggregate-method] | [`col("customer_id")`][col-function] | Group rows by one value |
| [`group_expr`][aggregate-method] | [`col("customer_id")`][col-function], [`col("product")`][col-function] | Group rows by a combination of values |
| [`aggr_expr`][aggregate-method] | [`sum(col("amount"))`][sum-function], [`avg(col("amount"))`][avg-function] | Calculate totals or averages for each group |
| [`aggr_expr`][aggregate-method] | [`min(col("amount"))`][min-function], [`max(col("amount"))`][max-function], [`count_all()`][count-all-function] | Calculate ranges or counts for each group |

Use [`.alias()`][expr-alias-method] to give aggregate-result columns stable names for later transformations, sorting, and assertions.

```rust
use datafusion::prelude::*;
use datafusion::assert_batches_sorted_eq;
use datafusion::functions_aggregate::count::count_all;
use datafusion::functions_aggregate::expr_fn::{avg, sum};

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let orders_df = dataframe!(
        "order_id" => [1001_i64, 1002, 1003, 1004, 1005, 1006],
        "customer_id" => ["C-01", "C-02", "C-01", "C-03", "C-02", "C-01"],
        "product" => ["Laptop", "Monitor", "Monitor", "Keyboard", "Laptop", "Monitor"],
        "quantity" => [1_i64, 2, 1, 3, 1, 2],
        "amount" => [120_i64, 80, 50, 150, 100, 70]
    )?;

    // Group by one expression and compute several summaries for each customer.
    let customer_summary = orders_df.clone().aggregate(
        vec![col("customer_id")],
        vec![
            sum(col("amount")).alias("total_amount"),
            avg(col("amount")).alias("average_amount"),
            count_all().alias("order_count"),
        ],
    )?;

    println!("{}", customer_summary.schema().tree_string());
    // root
    //  |-- customer_id: utf8 (nullable = true)
    //  |-- total_amount: int64 (nullable = true)
    //  |-- average_amount: float64 (nullable = true)
    //  |-- order_count: int64 (nullable = false)

    let customer_batches = customer_summary.collect().await?;
    assert_batches_sorted_eq!(
        &[
            "+-------------+--------------+----------------+-------------+",
            "| customer_id | total_amount | average_amount | order_count |",
            "+-------------+--------------+----------------+-------------+",
            "| C-01        | 240          | 80.0           | 3           |",
            "| C-02        | 180          | 90.0           | 2           |",
            "| C-03        | 150          | 150.0          | 1           |",
            "+-------------+--------------+----------------+-------------+",
        ],
        &customer_batches
    );

    // The complete key combination defines the customer-product grain.
    let product_summary = orders_df.aggregate(
        vec![col("customer_id"), col("product")],
        vec![sum(col("quantity")).alias("total_quantity")],
    )?;

    let product_batches = product_summary.collect().await?;
    assert_batches_sorted_eq!(
        &[
            "+-------------+----------+----------------+",
            "| customer_id | product  | total_quantity |",
            "+-------------+----------+----------------+",
            "| C-01        | Laptop   | 1              |",
            "| C-01        | Monitor  | 3              |",
            "| C-02        | Laptop   | 1              |",
            "| C-02        | Monitor  | 2              |",
            "| C-03        | Keyboard | 3              |",
            "+-------------+----------+----------------+",
        ],
        &product_batches
    );

    Ok(())
}
```

:::{admonition} SQL Equivalent
:class: note

SQL places grouping keys in `GROUP BY` and aggregate functions in the `SELECT`
list:

```sql
SELECT
    customer_id,
    SUM(amount) AS total_amount,
    AVG(amount) AS average_amount,
    COUNT(*) AS order_count
FROM orders
GROUP BY customer_id;
```

Adding `product` to both the `SELECT` list and `GROUP BY` clause produces the
customer-product grain.

:::

Grouping does not necessarily reduce the row count. If [`group_expr`][aggregate-method] contains a unique identifier such as `order_id`, every input row forms its own group and the result can contain as many rows as the input.

:::{admonition} Aggregate Expressions Define the Output Schema
:class: caution

For an ordinary [`.aggregate()`][aggregate-method] call, the result schema is formed from the grouping expressions and aggregate expressions; unrelated input columns do not carry through automatically. [`.aggregate()`][aggregate-method] returns a new [`DataFrame`][dataframe-type] plan and consumes its receiver. Clone the input first when both the detail and summary plans are needed. An unaliased aggregate receives a generated expression name, so use [`.alias()`][expr-alias-method] when the result column will be displayed, asserted, filtered, sorted, or reused.

:::

### Aggregating the Entire DataFrame

**An empty [`group_expr`][aggregate-method] vector treats the complete input as one group, so every expression in [`aggr_expr`][aggregate-method] contributes one value to a single summary row.**

Grouped aggregation produces one output row for each distinct combination of grouping-expression values. Global aggregation passes no grouping expressions, so all input rows contribute to the same group and the output grain becomes one row for the complete input. Because [`group_expr`][aggregate-method] is empty, the result schema contains only the aggregate-expression columns.

Use global aggregation for dataset-wide measures such as the total order value, average order value, and total number of orders. Multiple expressions in [`aggr_expr`][aggregate-method] calculate these measures together at the same whole-input grain.

```rust
use datafusion::prelude::*;
use datafusion::assert_batches_eq;
use datafusion::functions_aggregate::count::count_all;
use datafusion::functions_aggregate::expr_fn::{avg, sum};

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let orders_df = dataframe!(
        "order_id" => [1001_i64, 1002, 1003, 1004, 1005, 1006],
        "customer_id" => ["C-01", "C-02", "C-01", "C-03", "C-02", "C-01"],
        "product" => ["Laptop", "Monitor", "Monitor", "Keyboard", "Laptop", "Monitor"],
        "quantity" => [1_i64, 2, 1, 3, 1, 2],
        "amount" => [120_i64, 80, 50, 150, 100, 70]
    )?;

    // No grouping expressions: summarize the complete input.
    let order_summary = orders_df.aggregate(
        vec![],
        vec![
            sum(col("amount")).alias("total_amount"),
            avg(col("amount")).alias("average_amount"),
            count_all().alias("order_count"),
        ],
    )?;

    let summary_batches = order_summary.collect().await?;
    assert_batches_eq!(
        &[
            "+--------------+----------------+-------------+",
            "| total_amount | average_amount | order_count |",
            "+--------------+----------------+-------------+",
            "| 570          | 95.0           | 6           |",
            "+--------------+----------------+-------------+",
        ],
        &summary_batches
    );

    Ok(())
}
```

:::{admonition} SQL Equivalent
:class: note

A global aggregate uses aggregate functions without `GROUP BY`:

```sql
SELECT
    SUM(amount) AS total_amount,
    AVG(amount) AS average_amount,
    COUNT(*) AS order_count
FROM orders;
```

The empty DataFrame [`group_expr`][aggregate-method] vector expresses the same whole-input grain.

:::

:::{admonition} Empty Inputs Still Produce a Global Summary
:class: caution

A global aggregate has one group representing the complete input. When no rows contribute, [`count_all()`][count-all-function] returns `0`, while aggregates such as [`sum()`][sum-function], [`avg()`][avg-function], [`min()`][min-function], and [`max()`][max-function] return `NULL`. Account for those nullable results when later expressions consume the summary.

:::

Whether the result represents individual groups or the complete input, its usefulness depends on choosing aggregate expressions with the correct semantics.

---

## Choosing Aggregate Expressions

**Choosing the wrong aggregate expression can produce a valid summary that answers the wrong analytical question.**

After [`group_expr`][aggregate-method] establishes what each result row represents, every expression in [`aggr_expr`][aggregate-method] determines what that row measures. Common choices such as [`sum()`][sum-function], [`avg()`][avg-function], [`min()`][min-function], and [`max()`][max-function] can appear together in one [`.aggregate()`][aggregate-method] call and produce separate, aliased summary columns.

The important decisions concern what contributes to each result. A count may represent rows, non-null values, or distinct values; a condition may transform the value contributed by each row or restrict one summary to matching rows; and statistical, approximate, or collection aggregates may change the meaning or shape of the output. The complete function inventory remains in the [SQL aggregate-function reference](../../../user-guide/sql/aggregate_functions.md), the [Expression API](../../../user-guide/expressions.md), and the [Rust aggregate-expression API][aggregate-expression-api].

### Matching Aggregate Expressions to the Analytical Question

**Start with the analytical result you need, then choose the aggregate expression and supporting conditions that produce that result at the established grain.**

The following families cover common aggregation decisions without replacing the complete expression and function references.

| Analytical question | Representative expressions | Key decision |
| :------------------ | :------------------------- | :----------- |
| What is the total, average, or range? | [`sum()`][sum-function], [`avg()`][avg-function], [`min()`][min-function], [`max()`][max-function] | Choose the measure that describes the group |
| How many rows or values are present? | [`count_all()`][count-all-function], [`count()`][count-function], [`count_distinct()`][count-distinct-function] | Distinguish rows, non-null values, and distinct values |
| Which values or rows should contribute to one summary? | [`when(...).otherwise(...)`][case-builder], aggregate [`.filter(...).build()?`][expr-function-ext] | Map each row to a conditional value or exclude non-matching rows from one aggregate |
| What does the distribution or relationship look like? | [`median()`][median-function], [`stddev()`][stddev-function], [`corr()`][corr-function] | Select the statistic that answers the question and supports the input types |
| Is a compact estimate appropriate for the required accuracy and scale? | [`approx_distinct()`][approx-distinct-function], [`approx_median()`][approx-median-function], [`approx_percentile_cont()`][approx-percentile-function] | Review the named function's algorithm, controls, and documented guarantees |
| Should grouped values remain available in one field? | [`array_agg()`][array-agg-function], [`string_agg()`][string-agg-function] | Choose the result type and specify ordering when element order carries meaning |

Aggregate expressions can be composed with general [`Expr`][expr-type] values. Comparison expressions such as [`.gt_eq()`][expr-gt-eq-method] and [`.lt()`][expr-lt-method], together with logical expressions such as [`.and()`][expr-and-method], [`.or()`][expr-or-method], and [`.not()`][expr-not-method], build boolean conditions. Conditional expressions created with [`when(...).otherwise(...)`][case-builder] use those conditions to choose the value contributed by each row. Aggregate [`.filter(...).build()?`][expr-function-ext] uses a condition more narrowly to include or exclude rows from one aggregate expression. See the [Expression API](../../../user-guide/expressions.md) for the complete comparison, logical, and conditional expression APIs.

:::{admonition} Compact Estimates Have Function-Specific Trade-Offs
:class: caution

Large distinct-count and percentile analyses can require substantial aggregate state. [`approx_distinct()`][approx-distinct-function] processes input values with a HyperLogLog sketch, while [`approx_median()`][approx-median-function] and [`approx_percentile_cont()`][approx-percentile-function] use t-digest summaries. These algorithms maintain compact state as values are processed, making estimates practical when their resource benefits justify function-specific accuracy trade-offs. Use an exact aggregate when the result must be exact, and review the named approximate function's controls and guarantees before relying on its estimate.

:::

### Counting Rows, Values, and Distinct Values

**Counts answer different questions depending on whether they include rows, non-null values, or distinct values.**

Consider three similar questions: How many orders contributed to the summary? How many orders contain a coupon code? How many different coupon codes were used? DataFusion answers them with different aggregate-expression functions, while similarly named DataFrame APIs perform different operations.

| API | Kind | Result |
| :-- | :--- | :----- |
| [`count_all()`][count-all-function] | Aggregate-expression function | Counts every row contributing to each group |
| [`count(expr)`][count-function] | Aggregate-expression function | Counts rows where `expr` is not `NULL` |
| [`count_distinct(expr)`][count-distinct-function] | Aggregate-expression function | Counts distinct non-null values of `expr` |
| [`DataFrame::count().await?`][dataframe-count-method] | DataFrame action | Executes the current plan and returns its number of result rows |
| [`.distinct()`][distinct-method] | DataFrame transformation | Removes duplicate rows from a [`DataFrame`][dataframe-type] |

The following global aggregate uses a focused six-order frame with an optional coupon code. Six orders contribute to the summary, three contain a coupon code, and those values contain two distinct codes.

```rust
use datafusion::prelude::*;
use datafusion::assert_batches_eq;
use datafusion::functions_aggregate::count::count_all;
use datafusion::functions_aggregate::expr_fn::{count, count_distinct};

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let orders_df = dataframe!(
        "order_id" => [1001_i64, 1002, 1003, 1004, 1005, 1006],
        "coupon_code" => [
            Some("NEW"),
            None,
            Some("SAVE"),
            None,
            Some("NEW"),
            None,
        ]
    )?;

    let count_summary = orders_df.aggregate(
        vec![],
        vec![
            count_all().alias("row_count"),
            count(col("coupon_code")).alias("coupon_count"),
            count_distinct(col("coupon_code")).alias("distinct_coupon_count"),
        ],
    )?;

    let batches = count_summary.collect().await?;
    assert_batches_eq!(
        &[
            "+-----------+--------------+-----------------------+",
            "| row_count | coupon_count | distinct_coupon_count |",
            "+-----------+--------------+-----------------------+",
            "| 6         | 3            | 2                     |",
            "+-----------+--------------+-----------------------+",
        ],
        &batches
    );

    Ok(())
}
```

### Computing Conditional Summaries

**A DataFrame filter changes the rows available to every downstream summary; an aggregate filter changes only the summary to which it is attached.**

Conditional summaries begin with a boolean [`Expr`][expr-type]. Comparison expressions and logical expressions combine column values, literals, and predicates into a reusable condition; the condition does not remove rows or calculate a summary by itself.

Use [`when()`][when-function] and [`otherwise()`][otherwise-method] when each input row must first be mapped to a conditional value. The resulting [`CaseBuilder`][case-builder] expression can contribute different values for matching and non-matching rows before an aggregate combines them. For example, a conditional expression can contribute an order amount when a threshold matches and `0` otherwise.

When the requirement is only to include or exclude rows from one summary, attach the condition directly to the aggregate expression with [`ExprFunctionExt`][expr-function-ext]. This preserves other summaries calculated from the complete group.

```rust
use datafusion::prelude::*;
use datafusion::assert_batches_sorted_eq;
use datafusion::functions_aggregate::expr_fn::sum;
use datafusion::logical_expr::expr_fn::ExprFunctionExt;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let orders_df = dataframe!(
        "order_id" => [1001_i64, 1002, 1003, 1004, 1005, 1006],
        "customer_id" => ["C-01", "C-02", "C-01", "C-03", "C-02", "C-01"],
        "amount" => [120_i64, 80, 50, 150, 100, 70]
    )?;

    // Build a reusable boolean condition from a comparison expression.
    let high_value_order = col("amount").gt_eq(lit(70_i64));

    // Restrict only this aggregate to matching rows.
    let qualifying_amount = sum(col("amount"))
        .filter(high_value_order)
        .build()?
        .alias("qualifying_amount");

    let customer_summary = orders_df.aggregate(
        vec![col("customer_id")],
        vec![
            // Includes every order in the customer group.
            sum(col("amount")).alias("total_amount"),
            // Includes only orders worth at least 70.
            qualifying_amount,
        ],
    )?;

    let batches = customer_summary.collect().await?;
    assert_batches_sorted_eq!(
        &[
            "+-------------+--------------+-------------------+",
            "| customer_id | total_amount | qualifying_amount |",
            "+-------------+--------------+-------------------+",
            "| C-01        | 240          | 190               |",
            "| C-02        | 180          | 180               |",
            "| C-03        | 150          | 150               |",
            "+-------------+--------------+-------------------+",
        ],
        &batches
    );

    Ok(())
}
```

An aggregate filter controls participation, while [`when(...).otherwise(...)`][case-builder] controls the value contributed by every row. The distinction matters for averages, counts, null values, and any calculation where contributing `0` differs from not contributing.

:::{admonition} SQL Equivalent
:class: note

SQL applies the same condition to one aggregate with `FILTER (WHERE ...)`:

```sql
SELECT
    customer_id,
    SUM(amount) AS total_amount,
    SUM(amount) FILTER (WHERE amount >= 70) AS qualifying_amount
FROM orders
GROUP BY customer_id;
```

:::

Once each summary is defined, decide which detail rows should contribute and which completed summaries should remain.

---

## Positioning Filters Around `.aggregate()`

**The result changes with filter placement: filter before [`.aggregate()`][aggregate-method] to change which detail rows contribute, or afterward to keep only selected summaries.**

After choosing the output grain and aggregate expressions, decide which stage each predicate should evaluate. A filter before aggregation uses columns from the detail rows and changes the input available to every summary. A filter after aggregation uses grouping columns or aliased aggregate columns and removes completed summary rows.

Both method orders produce valid logical plans, but they answer different analytical questions:

| Pipeline | Predicate sees | Analytical effect | SQL equivalent |
| :------- | :------------- | :---------------- | :------------- |
| `.filter(...).aggregate(...)` | Detail-row columns | Excludes rows from every downstream aggregate expression | `WHERE` |
| `.aggregate(...).filter(...)` | Grouping and aliased summary columns | Removes completed summary rows without recalculating them | `HAVING` |

The following pipeline first totals only orders worth at least `70` for each customer. It then retains customers whose qualifying total is at least `180`.

```rust
use datafusion::assert_batches_sorted_eq;
use datafusion::functions_aggregate::expr_fn::sum;
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let orders_df = dataframe!(
        "order_id" => [1001_i64, 1002, 1003, 1004, 1005, 1006],
        "customer_id" => ["C-01", "C-02", "C-01", "C-03", "C-02", "C-01"],
        "amount" => [120_i64, 80, 50, 150, 100, 70]
    )?;

    // Filter detail rows before aggregation: excluded orders contribute to no summary.
    let qualifying_totals = orders_df
        .filter(col("amount").gt_eq(lit(70_i64)))?
        .aggregate(
            vec![col("customer_id")],
            vec![sum(col("amount")).alias("qualifying_total")],
        )?;

    let summary_batches = qualifying_totals.clone().collect().await?;
    assert_batches_sorted_eq!(
        &[
            "+-------------+------------------+",
            "| customer_id | qualifying_total |",
            "+-------------+------------------+",
            "| C-01        | 190              |",
            "| C-02        | 180              |",
            "| C-03        | 150              |",
            "+-------------+------------------+",
        ],
        &summary_batches
    );

    // Filter summary rows after aggregation: the totals remain unchanged.
    let retained_customers = qualifying_totals
        .filter(col("qualifying_total").gt_eq(lit(180_i64)))?;

    let retained_batches = retained_customers.collect().await?;
    assert_batches_sorted_eq!(
        &[
            "+-------------+------------------+",
            "| customer_id | qualifying_total |",
            "+-------------+------------------+",
            "| C-01        | 190              |",
            "| C-02        | 180              |",
            "+-------------+------------------+",
        ],
        &retained_batches
    );

    Ok(())
}
```

:::{admonition} SQL Equivalent
:class: note

SQL places the two predicates in different clauses:

```sql
SELECT
    customer_id,
    SUM(amount) AS qualifying_total
FROM orders
WHERE amount >= 70
GROUP BY customer_id
HAVING SUM(amount) >= 180;
```

`WHERE` filters detail rows before aggregation, while `HAVING` filters grouped results afterward. DataFrame method order expresses the same two scopes directly in the transformation chain.

:::

:::{admonition} Filtering at the Wrong Stage Changes the Question
:class: warning

A pre-aggregation filter excludes detail rows from every downstream summary. A post-aggregation filter leaves those calculations intact and removes result groups afterward. Both plans can execute successfully and produce plausible output, so place the predicate according to whether it describes detail rows or completed summaries.

:::

Treat filter placement as part of the analytical specification, not merely as method-chain formatting.

---

## Conclusion

When individual rows cannot answer an analytical question, [`.aggregate()`][aggregate-method] turns related rows into summaries at the required grain. [`group_expr`][aggregate-method] defines what each result row represents, while [`aggr_expr`][aggregate-method] defines what that row measures, supporting grouped and whole-input analyses comparable to SQL aggregate functions, `GROUP BY`, `WHERE`, and `HAVING`. A valid plan can still answer the wrong question when count semantics or filter placement select the wrong contributing rows, so verify both before relying on the result. When an analysis must compute across related rows without collapsing them, continue to [Window Functions](window-functions.md), which preserve the input grain.

### Further Reading

- [Transformation Concepts](transformation-concepts.md) — The grain model used by aggregation and the conceptual contrast with window functions.
- [Expression API](../../../user-guide/expressions.md) — Comparison, logical, and conditional expressions that can be composed into aggregate expressions.
- [Aggregate Functions Reference](../../../user-guide/sql/aggregate_functions.md) — The complete built-in function inventory, SQL syntax, and detailed aggregate semantics.
- [`datafusion::functions_aggregate::expr_fn`][aggregate-expression-api] — Rust builders for aggregate [`Expr`][expr-type] values.
- [`.aggregate()`][aggregate-method] — The current DataFrame method signature and API documentation.

---


<!-- REFERENCES --> 
[aggregate-expression-api]: https://docs.rs/datafusion/latest/datafusion/functions_aggregate/expr_fn/index.html
[aggregate-method]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.aggregate
[approx-distinct-function]: https://docs.rs/datafusion/latest/datafusion/functions_aggregate/expr_fn/fn.approx_distinct.html
[approx-median-function]: https://docs.rs/datafusion/latest/datafusion/functions_aggregate/expr_fn/fn.approx_median.html
[approx-percentile-function]: https://docs.rs/datafusion/latest/datafusion/functions_aggregate/expr_fn/fn.approx_percentile_cont.html
[array-agg-function]: https://docs.rs/datafusion/latest/datafusion/functions_aggregate/expr_fn/fn.array_agg.html
[avg-function]: https://docs.rs/datafusion/latest/datafusion/functions_aggregate/expr_fn/fn.avg.html
[case-builder]: https://docs.rs/datafusion/latest/datafusion/logical_expr/conditional_expressions/struct.CaseBuilder.html
[col-function]: https://docs.rs/datafusion/latest/datafusion/logical_expr/expr_fn/fn.col.html
[collect-method]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.collect
[corr-function]: https://docs.rs/datafusion/latest/datafusion/functions_aggregate/expr_fn/fn.corr.html
[count-all-function]: https://docs.rs/datafusion/latest/datafusion/functions_aggregate/count/fn.count_all.html
[count-distinct-function]: https://docs.rs/datafusion/latest/datafusion/functions_aggregate/expr_fn/fn.count_distinct.html
[count-function]: https://docs.rs/datafusion/latest/datafusion/functions_aggregate/expr_fn/fn.count.html
[dataframe-count-method]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.count
[dataframe-type]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html
[distinct-method]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.distinct
[expr-alias-method]: https://docs.rs/datafusion/latest/datafusion/logical_expr/enum.Expr.html#method.alias
[expr-and-method]: https://docs.rs/datafusion/latest/datafusion/logical_expr/enum.Expr.html#method.and
[expr-function-ext]: https://docs.rs/datafusion/latest/datafusion/logical_expr/expr_fn/trait.ExprFunctionExt.html
[expr-gt-eq-method]: https://docs.rs/datafusion/latest/datafusion/logical_expr/enum.Expr.html#method.gt_eq
[expr-lt-method]: https://docs.rs/datafusion/latest/datafusion/logical_expr/enum.Expr.html#method.lt
[expr-not-method]: https://docs.rs/datafusion/latest/datafusion/logical_expr/enum.Expr.html#method.not
[expr-or-method]: https://docs.rs/datafusion/latest/datafusion/logical_expr/enum.Expr.html#method.or
[expr-type]: https://docs.rs/datafusion/latest/datafusion/logical_expr/enum.Expr.html
[filter-method]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.filter
[lit-function]: https://docs.rs/datafusion/latest/datafusion/logical_expr/expr_fn/fn.lit.html
[logical-plan-type]: https://docs.rs/datafusion/latest/datafusion/logical_expr/enum.LogicalPlan.html
[max-function]: https://docs.rs/datafusion/latest/datafusion/functions_aggregate/expr_fn/fn.max.html
[median-function]: https://docs.rs/datafusion/latest/datafusion/functions_aggregate/expr_fn/fn.median.html
[min-function]: https://docs.rs/datafusion/latest/datafusion/functions_aggregate/expr_fn/fn.min.html
[otherwise-method]: https://docs.rs/datafusion/latest/datafusion/logical_expr/conditional_expressions/struct.CaseBuilder.html#method.otherwise
[record-batch-type]: https://docs.rs/arrow/latest/arrow/record_batch/struct.RecordBatch.html
[schema-ref-type]: https://docs.rs/arrow/latest/arrow/datatypes/type.SchemaRef.html
[select-method]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.select
[session-context-new]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.new
[show-method]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.show
[stddev-function]: https://docs.rs/datafusion/latest/datafusion/functions_aggregate/expr_fn/fn.stddev.html
[string-agg-function]: https://docs.rs/datafusion/latest/datafusion/functions_aggregate/expr_fn/fn.string_agg.html
[sum-function]: https://docs.rs/datafusion/latest/datafusion/functions_aggregate/expr_fn/fn.sum.html
[when-function]: https://docs.rs/datafusion/latest/datafusion/logical_expr/expr_fn/fn.when.html
