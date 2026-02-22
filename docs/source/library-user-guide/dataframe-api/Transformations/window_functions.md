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

Window functions compute analytics (running totals, rankings, moving averages) **per row** without collapsing rows like `GROUP BY` does. Each row "sees" a window of related rows, defined by `PARTITION BY`, `ORDER BY`, and an optional frame.

In the DataFrame API you build window expressions with the [`ExprFunctionExt`] builder:

1. Start with a window or aggregate function (e.g. `row_number()`, `sum(col("sales"))`)
2. Optionally call `.partition_by([...])` to group rows
3. Call `.order_by([...])` to define ordering within each partition
4. Optionally call `.window_frame(...)` to override the default frame
5. Call `.build()?` to get an `Expr` and pass it to `.window([...])`

The following example shows how to build a window expression for the `sales_rank` column:

```rust
use datafusion::prelude::*;
use datafusion::functions_window::expr_fn::row_number;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let df = dataframe!(
        "region" => ["East", "West"],
        "sales" => [100, 200]
    )?;

    let df = df.window(vec![
        row_number()
            .partition_by(vec![col("region")])
            .order_by(vec![col("sales").sort(false, false)])  // DESC
            .build()?
            .alias("sales_rank"),
    ])?;

    df.show().await?;

    Ok(())
}
```

**SQL equivalent:**

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

> **DataFrame vs. SQL**
>
> **DataFrame API**
>
> - Composable builder (`.partition_by().order_by().build()`) that you can reuse.
> - Column typos caught at compile time (when using `col("...")` centrally).
> - Multiple different window specs in a single `.window([...])` call.
>
> **SQL**
>
> - Compact `OVER (...)` syntax for simple cases.
> - Named windows (`WINDOW w AS (...)`) to reduce repetition.
> - Frame specs (`ROWS BETWEEN ...`) read very naturally.
>
> See [Window Functions](../../user-guide/sql/window_functions.md) for the full list of SQL window functions.

**Performance note:** <br>
Window functions require sorting by [`PARTITION BY`][window_function] and [`ORDER BY`] columns. If your data resides in a row-based database (PostgreSQL, MySQL) via [`TableProvider`] with indexes on these columns, consider pushing the window operation to the source. However, when combining multiple window functions over the same partition, DataFusion optimizes by sharing the sort.

[window_function]: ../../user-guide/sql/window_functions.md

## Basic: Ranking

Ranking functions assign a position to each row based on sort order within a group. Common use cases include leaderboards, top-N queries, and pagination. The builder pattern constructs the window specification:

| DataFrame method  | Purpose                                              | SQL equivalent |
| ----------------- | ---------------------------------------------------- | -------------- |
| `row_number()`    | The window function—assigns unique sequential rank   | `ROW_NUMBER()` |
| `.partition_by()` | Divides rows into groups; ranking restarts per group | `PARTITION BY` |
| `.order_by()`     | Determines sort order (first in order = rank 1)      | `ORDER BY`     |
| `.build()`        | Finalizes the expression into an `Expr`              | —              |

We'll use this sample dataset throughout the window function examples:

```rust
use datafusion::prelude::*;
use datafusion::functions_window::expr_fn::row_number;
use datafusion::assert_batches_sorted_eq;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    // Sample data: daily sales by region (used throughout this section)
    let sales = dataframe!(
        "region" => ["East", "East", "East", "West", "West"],
        "day" => [1, 2, 3, 1, 2],
        "sales" => [100, 200, 150, 300, 250]
    )?;

    // Verify the input data
    let results = sales.clone().collect().await?;
    assert_batches_sorted_eq!(
        &[
            "+--------+-----+-------+",
            "| region | day | sales |",
            "+--------+-----+-------+",
            "| East   | 1   | 100   |",
            "| East   | 2   | 200   |",
            "| East   | 3   | 150   |",
            "| West   | 1   | 300   |",
            "| West   | 2   | 250   |",
            "+--------+-----+-------+",
        ],
        &results
    );

    // Rank all days by sales (highest = rank 1)
    let ranked = sales.clone().window(vec![
        row_number()
            .order_by(vec![col("sales").sort(false, false)])  // DESC
            .build()?
            .alias("sales_rank")
    ])?;

    ranked.show().await?;
    // Output:
    // +--------+-----+-------+------------+
    // | region | day | sales | sales_rank |
    // +--------+-----+-------+------------+
    // | West   | 1   | 300   | 1          |  <- best overall
    // | West   | 2   | 250   | 2          |
    // | East   | 2   | 200   | 3          |
    // | East   | 3   | 150   | 4          |
    // | East   | 1   | 100   | 5          |  <- worst overall
    // +--------+-----+-------+------------+

    Ok(())
}
```

To rank **within groups**, add `.partition_by()`. Each group gets its own ranking:

```rust
use datafusion::prelude::*;
use datafusion::functions_window::expr_fn::row_number;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let sales = dataframe!(
        "region" => ["East", "East", "East", "West", "West"],
        "day" => [1, 2, 3, 1, 2],
        "sales" => [100, 200, 150, 300, 250]
    )?;

    let ranked_by_region = sales.clone().window(vec![
        row_number()
            .partition_by(vec![col("region")])  // restart ranking per region
            .order_by(vec![col("sales").sort(false, false)])
            .build()?
            .alias("region_rank")
    ])?;

    ranked_by_region.show().await?;
    // Output:
    // +--------+-----+-------+-------------+
    // | region | day | sales | region_rank |
    // +--------+-----+-------+-------------+
    // | East   | 2   | 200   | 1           |  <- best in East
    // | East   | 3   | 150   | 2           |
    // | East   | 1   | 100   | 3           |
    // | West   | 1   | 300   | 1           |  <- best in West
    // | West   | 2   | 250   | 2           |
    // +--------+-----+-------+-------------+

    Ok(())
}
```

**Choosing a ranking function:**

Different ranking functions handle ties (equal values) differently. Choose based on whether you need unique positions or want to preserve tie information:

| Function       | Ties behavior                    | Example (values: 10, 20, 20, 30) | Use when                                              |
| -------------- | -------------------------------- | -------------------------------- | ----------------------------------------------------- |
| `row_number()` | Unique ranks, arbitrary for ties | 1, 2, 3, 4                       | You need unique positions (pagination, deduplication) |
| `rank()`       | Same rank, then skip             | 1, 2, 2, 4                       | Ties matter, gaps acceptable (competition rankings)   |
| `dense_rank()` | Same rank, no gaps               | 1, 2, 2, 3                       | Ties matter, no gaps wanted (top-N categories)        |

For further reading, you may want to read [pyspark-rank-function-with-examples].

### Intermediate: Running Totals and Aggregates

Aggregate functions like [`sum()`] and [`avg()`] become window functions when combined with the builder pattern. Instead of collapsing all rows into one result, they compute a value for each row based on its window frame—the set of rows considered for the calculation.

| Pattern            | What it computes                 | Window frame                            | Method needed      |
| ------------------ | -------------------------------- | --------------------------------------- | ------------------ |
| **Running total**  | Cumulative sum up to current row | Default (unbounded preceding → current) | Just `.order_by()` |
| **Moving average** | Average over sliding window      | Custom (N preceding → current)          | `.window_frame()`  |

```rust
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();

    // Create sales data
    let sales = dataframe!(
        "region" => ["East", "East", "East", "West", "West"],
        "day" => [1, 2, 3, 1, 2],
        "sales" => [100, 200, 150, 300, 250]
    )?;
    ctx.register_table("sales", sales.into_view())?;

    // Running total per region using SQL window function
    let with_running_total = ctx.sql("
        SELECT region, day, sales,
               SUM(sales) OVER (PARTITION BY region ORDER BY day) as running_total
        FROM sales
    ").await?;

    with_running_total.show().await?;
    // Output:
    // +--------+-----+-------+---------------+
    // | region | day | sales | running_total |
    // +--------+-----+-------+---------------+
    // | East   | 1   | 100   | 100           |  <- day 1 only
    // | East   | 2   | 200   | 300           |  <- 100 + 200
    // | East   | 3   | 150   | 450           |  <- 100 + 200 + 150
    // | West   | 1   | 300   | 300           |  <- restarts for West
    // | West   | 2   | 250   | 550           |  <- 300 + 250
    // +--------+-----+-------+---------------+

    Ok(())
}
```

**Custom window frames** control exactly which rows are included. For a moving average over current + 1 preceding row:

```rust
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();

    // Create sales data
    let sales = dataframe!(
        "region" => ["East", "East", "East", "West", "West"],
        "day" => [1, 2, 3, 1, 2],
        "sales" => [100, 200, 150, 300, 250]
    )?;
    ctx.register_table("sales", sales.into_view())?;

    // 2-day moving average per region using SQL window function
    let with_moving_avg = ctx.sql("
        SELECT region, day, sales,
               AVG(sales) OVER (
                   PARTITION BY region
                   ORDER BY day
                   ROWS BETWEEN 1 PRECEDING AND CURRENT ROW
               ) as moving_avg_2
        FROM sales
    ").await?;

    with_moving_avg.show().await?;
    // Output:
    // +--------+-----+-------+--------------+
    // | region | day | sales | moving_avg_2 |
    // +--------+-----+-------+--------------+
    // | East   | 1   | 100   | 100.0        |  <- only day 1 available
    // | East   | 2   | 200   | 150.0        |  <- avg(100, 200)
    // | East   | 3   | 150   | 175.0        |  <- avg(200, 150)
    // | West   | 1   | 300   | 300.0        |  <- restarts for West
    // | West   | 2   | 250   | 275.0        |  <- avg(300, 250)
    // +--------+-----+-------+--------------+

    Ok(())
}
```

> **Window frame default behavior:** <br>
> When you specify `.order_by()` without `.window_frame()`, the default frame is `RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`—which gives you a running total. This matches SQL standard behavior.

### Advanced: lead() and lag()

Compare each row with its neighbors—useful for calculating day-over-day changes, detecting trends, or finding gaps in sequences.

| Function                      | Direction         | Returns                 | When no neighbor      |
| ----------------------------- | ----------------- | ----------------------- | --------------------- |
| `lag(expr, offset, default)`  | N rows **before** | Value from previous row | `NULL` (or `default`) |
| `lead(expr, offset, default)` | N rows **after**  | Value from next row     | `NULL` (or `default`) |

```rust
use datafusion::prelude::*;
use datafusion::functions_window::expr_fn::{lag, lead};

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let sales = dataframe!(
        "region" => ["East", "East", "East", "West", "West"],
        "day" => [1, 2, 3, 1, 2],
        "sales" => [100, 200, 150, 300, 250]
    )?;

    // Compare today's sales with yesterday's and tomorrow's (per region)
    let with_comparison = sales.clone().window(vec![
        lag(col("sales"), Some(1), None)
            .partition_by(vec![col("region")])
            .order_by(vec![col("day").sort(true, false)])
            .build()?
            .alias("prev_day_sales"),
        lead(col("sales"), Some(1), None)
            .partition_by(vec![col("region")])
            .order_by(vec![col("day").sort(true, false)])
            .build()?
            .alias("next_day_sales")
    ])?;

    with_comparison.show().await?;
    // Output:
    // +--------+-----+-------+----------------+----------------+
    // | region | day | sales | prev_day_sales | next_day_sales |
    // +--------+-----+-------+----------------+----------------+
    // | East   | 1   | 100   |                | 200            |  <- no prev day
    // | East   | 2   | 200   | 100            | 150            |
    // | East   | 3   | 150   | 200            |                |  <- no next day
    // | West   | 1   | 300   |                | 250            |  <- restarts
    // | West   | 2   | 250   | 300            |                |
    // +--------+-----+-------+----------------+----------------+

    Ok(())
}
```

**Calculating day-over-day change:** Combine `lag()` with arithmetic to compute deltas. This pattern is common for trend analysis—showing growth, decline, or anomalies between consecutive periods:

```rust
use datafusion::prelude::*;
use datafusion::functions_window::expr_fn::lag;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let sales = dataframe!(
        "region" => ["East", "East", "East", "West", "West"],
        "day" => [1, 2, 3, 1, 2],
        "sales" => [100, 200, 150, 300, 250]
    )?;

    // Calculate change from previous day
    let with_change = sales.clone().window(vec![
        lag(col("sales"), Some(1), None)
            .partition_by(vec![col("region")])
            .order_by(vec![col("day").sort(true, false)])
            .build()?
            .alias("prev_sales")
    ])?
    .with_column("daily_change", col("sales") - col("prev_sales"))?;

    with_change.show().await?;
    // Output:
    // +--------+-----+-------+------------+--------------+
    // | region | day | sales | prev_sales | daily_change |
    // +--------+-----+-------+------------+--------------+
    // | East   | 1   | 100   |            |              |  <- NULL - NULL = NULL
    // | East   | 2   | 200   | 100        | 100          |  <- +100 growth
    // | East   | 3   | 150   | 200        | -50          |  <- -50 decline
    // | West   | 1   | 300   |            |              |
    // | West   | 2   | 250   | 300        | -50          |
    // +--------+-----+-------+------------+--------------+

    Ok(())
}
```

### **Troubleshooting Window Functions:**

| Symptom                                    | Likely cause                   | Solution                                             |
| ------------------------------------------ | ------------------------------ | ---------------------------------------------------- |
| Window spans entire DataFrame              | Missing `.partition_by()`      | Add `.partition_by(vec![col("group_col")])`          |
| Wrong row gets rank 1                      | Sort direction incorrect       | Check `.sort(asc, nulls_first)` — `false` = DESC     |
| `lag()`/`lead()` returns unexpected `NULL` | At partition boundary          | Expected behavior; use `default` parameter if needed |
| Running total includes wrong rows          | Default frame vs custom        | Add `.window_frame()` for precise control            |
| `last_value()` returns current row         | Default frame stops at current | Use `UNBOUNDED FOLLOWING` in custom frame            |

> **Tip:** <br>
> When debugging, add `.sort()` after `.window()` to see results in a predictable order—window functions don't guarantee output row order.
