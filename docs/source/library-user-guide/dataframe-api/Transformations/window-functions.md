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

# Window Functions

**Analyze each row in relation to its group—rank it, compare it, or summarize related rows—without collapsing the result.**

Window-oriented transformations add per-row analytical context from related rows without changing what one row represents. They occupy the space between ordinary expressions, which use values from the current row, and aggregations, which replace detail rows with group-level results.

This page shows how to construct built-in window expressions with DataFusion’s Rust [`DataFrame`] API, choose among ranking, aggregate, and positional calculations, configure partitioning, ordering, and frames where they affect correctness, and compose the resulting columns with later derivation, filtering, and sorting.

:::{admonition} Style Note
:class: note
:collapsible: closed

In this document, code elements follow a consistent pattern:

- **DataFrame methods:** `.method()` (e.g., `.select()`, `.filter()`)
- **Standalone functions:** `function()` (e.g., `col()`, `lit()`)
- **Constructors:** `Type::new()` (e.g., `SessionContext::new()`)
- **Types:** `TypeName` (e.g., `SchemaRef`, `RecordBatch`)
- **Lazy transformations:** return a `DataFrame` and build the `LogicalPlan`
- **Actions:** ([`.collect()`], `.show()`) trigger execution

:::

```{contents} Table of Contents for Window Functions
:local:
:depth: 2
```

## Window Functions and Window Expressions

**Window expressions enrich each input row with rankings, running aggregates, or related-row values; the window function supplies the calculation, and its specification defines the context.**

A **window function** supplies the calculation, such as assigning a rank, accumulating values, or reading a value from another row. A **window specification** supplies the related-row context through partitioning, ordering, and, when relevant, a frame. Together, the function and specification form a **window expression**.

The relationship is conceptual rather than an execution diagram:

```text
┌──────────────────────┐     ┌──────────────────────────────┐
│ Window function      │  +  │ Window specification         │
│ calculation          │     │ partition · order · frame    │
└──────────┬───────────┘     └──────────────┬───────────────┘
           └───────────────┬────────────────┘
                           ▼
                ┌─────────────────────┐
                │ Window expression   │
                └──────────┬──────────┘
                           ▼
                ┌─────────────────────┐
                │ `.window()`         │
                │ same grain + result │
                └─────────────────────┘
```

An ordinary expression calculates from values in the current row. An aggregation uses related rows by replacing detail-row grain with summary grain. A window expression also uses related-row context, but [`.window()`] appends the analytical result while retaining the input grain: each output row still represents the same input row. See [Transformation Concepts] for the broader grain model and its relationship to other transformations.

:::{admonition} Choose the API That Makes the Window Logic Clear
:class: note

Use the SQL API when the window calculation has a fixed, declarative structure. SQL provides compact, purpose-built syntax for window functions, including `OVER (...)`, named `WINDOW` specifications, explicit frames, aggregate windows with `FILTER`, and `QUALIFY` for filtering window results.

Use the DataFrame API when Rust code needs to generate, reuse, or conditionally configure window expressions, or compose their results directly with other DataFrame transformations.

Both APIs produce DataFusion logical plans and use the same optimizer and execution engine. Choose between them for clarity, maintainability, and composition—not for an assumed execution-speed advantage.

:::

The DataFrame API maps this model onto one shared construction pattern.

### Configure a Window Function for `.window()`

**Select the calculation, configure its partitioning, ordering, and optional frame, then complete the expression and add its result with [`.window()`].**

Window helper functions such as [`row_number()`], [`lag()`], and [`last_value()`] start the fluent expression builder. [`ExprFunctionExt`] configures the window specification, [`.build()?`] completes the resulting [`Expr`], and [`.window()`] adds one or more evaluated result columns to the lazy `DataFrame`.

| Phase                   | DataFrame API shape                                                    | Result                                              |
| ----------------------- | ---------------------------------------------------------------------- | --------------------------------------------------- |
| Select the calculation  | `row_number()`, `lag()`, or `last_value()`                             | Start with the required window function             |
| Configure the context   | [`.partition_by()`], [`.order_by()`], and optional [`.window_frame()`] | Define the related rows the calculation interprets  |
| Complete the expression | `.build()?.alias(...)`                                                 | Produce the completed `Expr`                        |
| Add the result          | `.window(vec![...])`                                                   | Append the evaluated result to the lazy `DataFrame` |

The following example demonstrates that shared shape with `row_number()`:

```rust
use datafusion::prelude::*;
use datafusion::assert_batches_sorted_eq;
use datafusion::functions_window::expr_fn::row_number;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let sales = dataframe!(
        "region" => ["East", "East", "West", "West"],
        "day" => [1_i64, 2, 1, 2],
        "sales" => [100_i64, 200, 300, 250]
    )?;

    // Select and configure the window function.
    let sales_rank = row_number()
        .partition_by(vec![col("region")])
        .order_by(vec![col("sales").sort(false, false)])
        .build()?
        .alias("sales_rank");

    // Add the completed window expression to the lazy DataFrame.
    let ranked_sales = sales.window(vec![sales_rank])?;

    // Execute and verify the result.
    let result_batches = ranked_sales.collect().await?;
    assert_batches_sorted_eq!(
        &[
            "+--------+-----+-------+------------+",
            "| region | day | sales | sales_rank |",
            "+--------+-----+-------+------------+",
            "| East   | 1   | 100   | 2          |",
            "| East   | 2   | 200   | 1          |",
            "| West   | 1   | 300   | 1          |",
            "| West   | 2   | 250   | 2          |",
            "+--------+-----+-------+------------+",
        ],
        &result_batches
    );

    Ok(())
}
```

:::{admonition} SQL Equivalent
:class: note

For this fixed window specification, SQL expresses the calculation more compactly:

```sql
SELECT
    region,
    day,
    sales,
    ROW_NUMBER() OVER (
        PARTITION BY region
        ORDER BY sales DESC
    ) AS sales_rank
FROM sales;
```

The `ROW_NUMBER()` call supplies the calculation, while `OVER (...)` supplies its partitioning and ordering specification.
:::

Partitioning, ordering, and frames affect calculation families in different ways. The next section organizes those choices by the value the window expression must produce.

---

## Choose a Window Calculation

**Choose the window function by the result each row needs: a position within its partition, a summary over related rows, or a value selected by offset or frame position.**

The preceding section established the shared construction pattern: select the window function, configure its specification, complete the window expression, and add it with `.window()`. The selected function supplies the calculation.

Partitioning, ordering, and frames affect calculation families differently. Use the required result to choose the family, then configure the specification that gives that calculation its meaning.

| Result each row needs                      | Calculation family   | Representative functions                                                    |
| ------------------------------------------ | -------------------- | --------------------------------------------------------------------------- |
| Position within its partition              | Ranking              | [`row_number()`], [`rank()`], [`dense_rank()`]                              |
| Summary over related rows                  | Aggregate window     | [`sum()`], [`avg()`], [`count()`]                                           |
| Value selected by offset or frame position | Navigation and value | [`lag()`], [`lead()`], [`first_value()`], [`last_value()`], [`nth_value()`] |

The sections begin with ranking, continue with frame-sensitive aggregate windows, and finish with offset-based and frame-relative value functions.

### Rank Rows and Decide How Ties Count

**Ranking functions turn window order into a position for each row; choose distinct row numbers, gapped ranks, or gapless ranks according to how ties should count.**

Ranking functions assign a position within each partition. Use them for leaderboards, deduplication, pagination, and workflows that later filter by rank.

Rows are peers when their window ordering values are equal. The three primary ranking functions interpret those peers differently:

| Function         | Tie behavior                                                                               | Example with `ORDER BY value ASC` | Use when                                                       |
| ---------------- | ------------------------------------------------------------------------------------------ | --------------------------------- | -------------------------------------------------------------- |
| [`row_number()`] | Assigns distinct positions; tied rows have no defined relative order without a tie-breaker | `1, 2, 3, 4` for `10, 20, 20, 30` | Every row needs a unique position                              |
| [`rank()`]       | Assigns peers the same rank and leaves a gap before the next rank                          | `1, 2, 2, 4`                      | Peers should count equally and skipped positions matter        |
| [`dense_rank()`] | Assigns peers the same rank without leaving a gap                                          | `1, 2, 2, 3`                      | Peers should count equally and ranks should remain consecutive |

The following example evaluates all three functions over the same partitions and tied ordering values. The tied rows have identical visible inputs, so the assertion verifies the ranking results without depending on which physical peer receives each `row_number()` value.

```rust
use datafusion::prelude::*;
use datafusion::assert_batches_sorted_eq;
use datafusion::functions_window::expr_fn::{dense_rank, rank, row_number};

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let scores = dataframe!(
        "team" => ["A", "A", "A", "A", "B", "B", "B"],
        "value" => [10_i64, 20, 20, 30, 5, 5, 15]
    )?;

    let partition_by = vec![col("team")];
    let order_by = vec![col("value").sort(true, false)];

    let row_position = row_number()
        .partition_by(partition_by.clone())
        .order_by(order_by.clone())
        .build()?
        .alias("row_number");
    let gapped_rank = rank()
        .partition_by(partition_by.clone())
        .order_by(order_by.clone())
        .build()?
        .alias("rank");
    let gapless_rank = dense_rank()
        .partition_by(partition_by)
        .order_by(order_by)
        .build()?
        .alias("dense_rank");

    let batches = scores
        .window(vec![row_position, gapped_rank, gapless_rank])?
        .collect()
        .await?;

    assert_batches_sorted_eq!(
        &[
            "+------+-------+------------+------+------------+",
            "| team | value | row_number | rank | dense_rank |",
            "+------+-------+------------+------+------------+",
            "| A    | 10    | 1          | 1    | 1          |",
            "| A    | 20    | 2          | 2    | 2          |",
            "| A    | 20    | 3          | 2    | 2          |",
            "| A    | 30    | 4          | 4    | 3          |",
            "| B    | 5     | 1          | 1    | 1          |",
            "| B    | 5     | 2          | 1    | 1          |",
            "| B    | 15    | 3          | 3    | 2          |",
            "+------+-------+------------+------+------------+",
        ],
        &batches
    );

    Ok(())
}
```

Ranking functions use window ordering to assign positions. Add enough ordering expressions to break ties whenever particular rows must receive repeatable positions.

### Compute Cumulative and Moving Aggregates

**Aggregate windows calculate a summary for every retained row, with the active frame determining which related rows contribute to each result.**

Aggregate windows add a summary without collapsing the DataFrame grain. A running total accumulates values as the current row advances, while a moving calculation summarizes a bounded set of nearby rows; the difference comes from the rows included in each frame.

#### Choose a Frame Pattern

A frame-sensitive calculation answers one question for each row: which rows in this partition should contribute to the current result? Partitioning determines where the calculation restarts. For the `ROWS` frames demonstrated here, ordering establishes the physical row sequence from which the bounds are measured.

| Pattern        | What it computes                                       | Explicit frame                                     |
| -------------- | ------------------------------------------------------ | -------------------------------------------------- |
| Running total  | Rows from the partition start through the current row  | `ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW` |
| Moving average | The current row and a bounded number of preceding rows | `ROWS BETWEEN N PRECEDING AND CURRENT ROW`         |

The cumulative frame expands as the current row advances. A moving frame retains a bounded range; the example below uses `N = 1`, so each average contains the current row and, when available, its immediately preceding row. See [DataFusion SQL Window Functions] and the Rust [`WindowFrame`] API for complete frame units and boundary forms.

#### Build Aggregate Window Expressions

The built-in [`sum_udaf()`] and [`avg_udaf()`] factories return [`AggregateUDF`] definitions. [`WindowFunctionDefinition::AggregateUDF(...)`] allows an aggregate definition to supply the window calculation, and [`WindowFunction::new(...)`] constructs the aggregate-backed window-function expression. The fluent [`ExprFunctionExt`] builder then configures its partitioning, ordering, and explicit frame.

```rust
use datafusion::prelude::*;
use datafusion::assert_batches_sorted_eq;
use datafusion::common::ScalarValue;
use datafusion::functions_aggregate::{average::avg_udaf, sum::sum_udaf};
use datafusion::logical_expr::{
    expr::WindowFunction, WindowFrame, WindowFrameBound, WindowFrameUnits,
    WindowFunctionDefinition,
};

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let sales = dataframe!(
        "region" => ["East", "East", "East", "West", "West"],
        "day" => [1_i64, 2, 3, 1, 2],
        "sales" => [100_i64, 200, 150, 300, 250]
    )?;

    let partition_by = vec![col("region")];
    let order_by = vec![col("day").sort(true, false)];

    let cumulative_frame = WindowFrame::new_bounds(
        WindowFrameUnits::Rows,
        WindowFrameBound::Preceding(ScalarValue::UInt64(None)),
        WindowFrameBound::CurrentRow,
    );
    let moving_frame = WindowFrame::new_bounds(
        WindowFrameUnits::Rows,
        WindowFrameBound::Preceding(ScalarValue::UInt64(Some(1))),
        WindowFrameBound::CurrentRow,
    );

    let running_total = Expr::from(WindowFunction::new(
        WindowFunctionDefinition::AggregateUDF(sum_udaf()),
        vec![col("sales")],
    ))
    .partition_by(partition_by.clone())
    .order_by(order_by.clone())
    .window_frame(cumulative_frame)
    .build()?
    .alias("running_total");

    let moving_avg_2 = Expr::from(WindowFunction::new(
        WindowFunctionDefinition::AggregateUDF(avg_udaf()),
        vec![col("sales")],
    ))
    .partition_by(partition_by)
    .order_by(order_by)
    .window_frame(moving_frame)
    .build()?
    .alias("moving_avg_2");

    let batches = sales
        .window(vec![running_total, moving_avg_2])?
        .collect()
        .await?;

    assert_batches_sorted_eq!(
        &[
            "+--------+-----+-------+---------------+--------------+",
            "| region | day | sales | running_total | moving_avg_2 |",
            "+--------+-----+-------+---------------+--------------+",
            "| East   | 1   | 100   | 100           | 100.0        |",
            "| East   | 2   | 200   | 300           | 150.0        |",
            "| East   | 3   | 150   | 450           | 175.0        |",
            "| West   | 1   | 300   | 300           | 300.0        |",
            "| West   | 2   | 250   | 550           | 275.0        |",
            "+--------+-----+-------+---------------+--------------+",
        ],
        &batches
    );

    Ok(())
}
```

:::{admonition} SQL Equivalent
:class: note

SQL turns each aggregate into a window calculation by attaching `OVER (...)`:

```sql
SELECT
    region,
    day,
    sales,
    SUM(sales) OVER (
        PARTITION BY region
        ORDER BY day
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS running_total,
    AVG(sales) OVER (
        PARTITION BY region
        ORDER BY day
        ROWS BETWEEN 1 PRECEDING AND CURRENT ROW
    ) AS moving_avg_2
FROM sales;
```

The DataFrame API instead represents each aggregate definition as a window function before configuring the same partitioning, ordering, and explicit frame.

:::

#### Specify Frames Deliberately

When [`.window_frame()`] is omitted from an existing [`Expr::WindowFunction`], the
[`ExprFunctionExt`] builder supplies a frame during [`.build()?`]. When
[`.order_by()`] is not configured, it uses
`ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING`. When `.order_by()`
is configured with one or more sort expressions, it uses
`ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`.

These defaults describe the Rust expression-builder path and are not universal
SQL defaults.

Calling `.order_by()` on an [`Expr::AggregateFunction`] configures the aggregate's
own ordering inputs; it does not create an `OVER` specification or convert the
aggregate into an [`Expr::WindowFunction`].

:::{admonition} Ordering Still Matters
:class: caution

A `ROWS` frame selects rows by physical position. Add ordering expressions that
break ties when exact bounded-frame membership must be repeatable.

:::

### Read Values by Position

**Read previous, next, first, last, or nth values for each row: offsets navigate
the ordered partition, while frame-relative functions select within the active
frame.**

Some calculations need an existing value from another position rather than a rank or summary—for example, the preceding sale, the next event, or the final value in a region.

DataFusion supports two position models. `lag()` and `lead()` measure an offset from the current row within its ordered partition. `first_value()`, `last_value()`, and `nth_value()` select a position within the active frame, so changing the frame can change their result.

| Position model  | Functions                                            | Reads relative to                        | Main specification concern                        |
| --------------- | ---------------------------------------------------- | ---------------------------------------- | ------------------------------------------------- |
| Offset-relative | [`lag()`], [`lead()`]                                | The current row in its ordered partition | Ordering determines previous and next             |
| Frame-relative  | [`first_value()`], [`last_value()`], [`nth_value()`] | A position within the active frame       | Frame membership determines which positions exist |

The Rust helpers [`lag()`] and [`lead()`] expose the offset and fallback as `Option` arguments. Passing `None` for the offset selects the default offset of `1`. Passing `None` for the fallback produces `NULL` when the requested position falls outside the partition. A supplied fallback must be type-compatible with the selected expression.

The following example reads the adjacent sales and makes the final sale in each region available to every row.

```rust
use datafusion::prelude::*;
use datafusion::assert_batches_sorted_eq;
use datafusion::common::ScalarValue;
use datafusion::functions_window::expr_fn::{lag, last_value, lead};
use datafusion::logical_expr::{WindowFrame, WindowFrameBound, WindowFrameUnits};

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let sales = dataframe!(
        "region" => ["East", "East", "East", "West", "West"],
        "day" => [1_i64, 2, 3, 1, 2],
        "sales" => [100_i64, 200, 150, 300, 250]
    )?;

    // Reuse one partition and ordering specification for all lookups.
    let partition_by = vec![col("region")];
    let order_by = vec![col("day").sort(true, false)];

    // Read one row before and after the current row.
    let previous_sales = lag(col("sales"), Some(1), None)
        .partition_by(partition_by.clone())
        .order_by(order_by.clone())
        .build()?
        .alias("previous_sales");
    let next_sales = lead(col("sales"), Some(1), None)
        .partition_by(partition_by.clone())
        .order_by(order_by.clone())
        .build()?
        .alias("next_sales");

    // Make the partition's final value visible from every current row.
    let whole_partition = WindowFrame::new_bounds(
        WindowFrameUnits::Rows,
        WindowFrameBound::Preceding(ScalarValue::UInt64(None)),
        WindowFrameBound::Following(ScalarValue::UInt64(None)),
    );
    let final_region_sales = last_value(col("sales"))
        .partition_by(partition_by)
        .order_by(order_by)
        .window_frame(whole_partition)
        .build()?
        .alias("final_region_sales");

    let batches = sales
        .window(vec![previous_sales, next_sales, final_region_sales])?
        .collect()
        .await?;

    assert_batches_sorted_eq!(
        &[
            "+--------+-----+-------+----------------+------------+--------------------+",
            "| region | day | sales | previous_sales | next_sales | final_region_sales |",
            "+--------+-----+-------+----------------+------------+--------------------+",
            "| East   | 1   | 100   |                | 200        | 150                |",
            "| East   | 2   | 200   | 100            | 150        | 150                |",
            "| East   | 3   | 150   | 200            |            | 150                |",
            "| West   | 1   | 300   |                | 250        | 250                |",
            "| West   | 2   | 250   | 300            |            | 250                |",
            "+--------+-----+-------+----------------+------------+--------------------+",
        ],
        &batches
    );

    Ok(())
}
```

:::{admonition} Position Depends on the Specification
:class: caution

When the requested offset falls outside the partition, `lag()` and `lead()`
return the supplied fallback, or `NULL` when none is supplied. Add ordering
expressions that break ties when “previous” or “next” must identify repeatable
physical rows.

Frame-relative functions read only from the active frame. The explicit
whole-partition frame in the example makes `last_value()` return the final value
in the partition. With
`ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`, it would instead return the
current row's value. [`nth_value()`] counts positions from
`1` and returns `NULL` when the requested position does not exist in the frame.

:::

## Apply Window Results

**Turn window results into a DataFrame workflow by deriving new columns,
filtering ranked rows, and sorting the final result.**

Window expressions usually provide intermediate results rather than the final
answer. A report might compare each sale with the preceding day, retain only
the highest-performing days in each region, and then arrange those rows for
presentation.

Add the required window columns first with [`.window()`], then compose them with ordinary
DataFrame transformations such as [`.with_column()`] and [`.filter()`]. The order inside a window specification controls
the calculation; use a separate [`.sort()`] when the final DataFrame must have a
defined presentation order.

### Build a Per-Group Top-N Change Report

For each region, the following workflow calculates the change from the preceding
day, retains the two highest-sales days, and presents the result in a stable
order. Ranking uses sales performance, while the previous-value lookup uses
chronological order. Because those expressions require different sort keys,
the workflow adds them in separate `.window()` calls before filtering.

```rust
use datafusion::prelude::*;
use datafusion::assert_batches_eq;
use datafusion::functions_window::expr_fn::{lag, row_number};

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let sales = dataframe!(
        "region" => ["East", "East", "East", "West", "West"],
        "day" => [1_i64, 2, 3, 1, 2],
        "sales" => [100_i64, 200, 150, 300, 250]
    )?;

    let sales_rank = row_number()
        .partition_by(vec![col("region")])
        .order_by(vec![
            col("sales").sort(false, false),
            col("day").sort(true, false),
        ])
        .build()?
        .alias("sales_rank");

    let previous_sales = lag(col("sales"), Some(1), None)
        .partition_by(vec![col("region")])
        .order_by(vec![col("day").sort(true, false)])
        .build()?
        .alias("previous_sales");

    let result = sales
        .window(vec![sales_rank])?
        .window(vec![previous_sales])?
        .with_column(
            "daily_change",
            col("sales") - col("previous_sales"),
        )?
        .filter(col("sales_rank").lt_eq(lit(2_u64)))?
        .sort(vec![
            col("region").sort(true, false),
            col("sales_rank").sort(true, false),
        ])?;

    let batches = result.collect().await?;

    assert_batches_eq!(
        &[
            "+--------+-----+-------+------------+----------------+--------------+",
            "| region | day | sales | sales_rank | previous_sales | daily_change |",
            "+--------+-----+-------+------------+----------------+--------------+",
            "| East   | 2   | 200   | 1          | 100            | 100          |",
            "| East   | 3   | 150   | 2          | 200            | -50          |",
            "| West   | 1   | 300   | 1          |                |              |",
            "| West   | 2   | 250   | 2          | 300            | -50          |",
            "+--------+-----+-------+------------+----------------+--------------+",
        ],
        &batches
    );

    Ok(())
}
```

The window expressions are evaluated before the rank filter. As a result,
`daily_change` compares each retained row with the preceding day in the complete
regional history, even when that preceding row is not retained in the final
Top-N result.

Filtering on `row_number()` returns at most two rows per region. Use `rank()`
instead when all rows tied at the cutoff should remain, which can produce more
than two rows per region.

---

## Conclusion

Window expressions add analytical context while preserving the input-row grain. The window function supplies the calculation, while partitioning, ordering, and an optional frame define the related rows that give the calculation its meaning.

The three calculation families answer different questions. Ranking functions assign positions within an ordered partition. Aggregate windows produce summaries over a frame of related rows. Navigation and value functions read values by offset from the current row or by position within the active frame. Their results depend on the specification, particularly when ordering values tie, frames are bounded, or requested positions do not exist.

In the DataFrame API, `.window()` adds analytical columns that remain available to ordinary transformations. Compute window columns before downstream filters when the calculation must use the complete partition, and sort the final DataFrame separately when presentation order matters.

### Further Reading

**DataFusion references**

- [DataFusion SQL Window Functions] documents supported SQL window functions, window specifications, and frame syntax.
- [DataFusion `SELECT` syntax] covers named `WINDOW` specifications and `QUALIFY`.
- [`DataFrame::window()`] defines the Rust method for adding completed window expressions.
- [Built-in Rust window-expression helpers] documents the available window-function helpers.
- [`ExprFunctionExt`], [`WindowFunction`], and [`WindowFrame`] define the Rust construction and configuration APIs used on this page.

**External conceptual resources**

- [PostgreSQL Window Functions Tutorial] introduces the relational window-function model and common SQL patterns.
- [DuckDB Window Functions] provides additional examples involving ranking, offsets, aggregates, and frames.
- [Polars Window Functions] presents a comparative DataFrame-expression approach to calculations over related rows.

<!-- DataFrame API -->

[`DataFrame`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html
[`.window()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.window
[`DataFrame::window()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.window
[`.with_column()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.with_column
[`.filter()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.filter
[`.sort()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.sort
[`.collect()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.collect

<!-- Expressions and builder API -->

[`Expr`]: https://docs.rs/datafusion-expr/latest/datafusion_expr/expr/enum.Expr.html
[`Expr::WindowFunction`]: https://docs.rs/datafusion-expr/latest/datafusion_expr/expr/enum.Expr.html#variant.WindowFunction
[`Expr::AggregateFunction`]: https://docs.rs/datafusion-expr/latest/datafusion_expr/expr/enum.Expr.html#variant.AggregateFunction
[`ExprFunctionExt`]: https://docs.rs/datafusion-expr/latest/datafusion_expr/expr_fn/trait.ExprFunctionExt.html
[`.partition_by()`]: https://docs.rs/datafusion-expr/latest/datafusion_expr/expr_fn/trait.ExprFunctionExt.html#tymethod.partition_by
[`.order_by()`]: https://docs.rs/datafusion-expr/latest/datafusion_expr/expr_fn/trait.ExprFunctionExt.html#tymethod.order_by
[`.window_frame()`]: https://docs.rs/datafusion-expr/latest/datafusion_expr/expr_fn/trait.ExprFunctionExt.html#tymethod.window_frame
[`.build()?`]: https://docs.rs/datafusion-expr/latest/datafusion_expr/expr_fn/struct.ExprFuncBuilder.html#method.build

<!-- Built-in window helpers -->

[`row_number()`]: https://docs.rs/datafusion/latest/datafusion/functions_window/expr_fn/fn.row_number.html
[`rank()`]: https://docs.rs/datafusion/latest/datafusion/functions_window/expr_fn/fn.rank.html
[`dense_rank()`]: https://docs.rs/datafusion/latest/datafusion/functions_window/expr_fn/fn.dense_rank.html
[`lag()`]: https://docs.rs/datafusion/latest/datafusion/functions_window/expr_fn/fn.lag.html
[`lead()`]: https://docs.rs/datafusion/latest/datafusion/functions_window/expr_fn/fn.lead.html
[`first_value()`]: https://docs.rs/datafusion/latest/datafusion/functions_window/expr_fn/fn.first_value.html
[`last_value()`]: https://docs.rs/datafusion/latest/datafusion/functions_window/expr_fn/fn.last_value.html
[`nth_value()`]: https://docs.rs/datafusion/latest/datafusion/functions_window/expr_fn/fn.nth_value.html

<!-- Aggregate helpers and aggregate-backed windows -->

[`sum()`]: https://docs.rs/datafusion/latest/datafusion/functions_aggregate/expr_fn/fn.sum.html
[`avg()`]: https://docs.rs/datafusion/latest/datafusion/functions_aggregate/expr_fn/fn.avg.html
[`count()`]: https://docs.rs/datafusion/latest/datafusion/functions_aggregate/expr_fn/fn.count.html
[`sum_udaf()`]: https://docs.rs/datafusion-functions-aggregate/latest/datafusion_functions_aggregate/sum/fn.sum_udaf.html
[`avg_udaf()`]: https://docs.rs/datafusion-functions-aggregate/latest/datafusion_functions_aggregate/average/fn.avg_udaf.html
[`AggregateUDF`]: https://docs.rs/datafusion-expr/latest/datafusion_expr/struct.AggregateUDF.html
[`WindowFunction`]: https://docs.rs/datafusion-expr/latest/datafusion_expr/expr/struct.WindowFunction.html
[`WindowFunction::new(...)`]: https://docs.rs/datafusion-expr/latest/datafusion_expr/expr/struct.WindowFunction.html#method.new
[`WindowFunctionDefinition::AggregateUDF(...)`]: https://docs.rs/datafusion-expr/latest/datafusion_expr/expr/enum.WindowFunctionDefinition.html#variant.AggregateUDF
[`WindowFrame`]: https://docs.rs/datafusion-expr/latest/datafusion_expr/window_frame/struct.WindowFrame.html

<!-- DataFusion documentation -->

[Transformation Concepts]: transformation-concepts.md
[DataFusion SQL Window Functions]: https://datafusion.apache.org/user-guide/sql/window_functions.html
[DataFusion `SELECT` syntax]: https://datafusion.apache.org/user-guide/sql/select.html
[Built-in Rust window-expression helpers]: https://docs.rs/datafusion-functions-window/latest/datafusion_functions_window/expr_fn/index.html

<!-- External conceptual resources -->

[PostgreSQL Window Functions Tutorial]: https://www.postgresql.org/docs/current/tutorial-window.html
[DuckDB Window Functions]: https://duckdb.org/docs/stable/sql/functions/window_functions
[Polars Window Functions]: https://docs.pola.rs/user-guide/expressions/window-functions/
