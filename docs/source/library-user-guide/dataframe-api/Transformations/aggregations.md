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

# Aggregation Patterns

**Aggregation collapses rows into summary statistics—transforming thousands of individual records into meaningful totals, averages, and counts that reveal patterns in your data.**

Aggregate functions like [`sum()`], [`avg()`], [`count()`], [`min()`], and [`max()`] reduce multiple values to a single result. The [`.aggregate()`] method takes two arguments:

1. **Grouping columns** — partition rows into groups (like SQL's `GROUP BY`)
2. **Aggregate expressions** — compute summaries per group

Without grouping columns (empty `vec![]`), aggregations summarize the entire DataFrame.

**SQL equivalent:** `SELECT dept, SUM(salary) FROM employees GROUP BY dept`

> **Trade-off: DataFrame vs SQL**
>
> - **DataFrame shines:** Programmatic grouping keys, conditional aggregations via [`when()`], multiple aggregations in one call
> - **SQL shines:** Declarative [`GROUP BY`] syntax, [`HAVING`] clause more intuitive than chained [`.filter()`]

**Performance note:** <br>
Aggregations are **column-wise analytics** — exactly where DataFusion's columnar approach excels. Vectorized operations on compressed Arrow arrays outperform row-by-row processing for large datasets. However, if your data resides in a row-based database via [`TableProvider`] and you're doing a simple `COUNT(*)` or `SUM` on an indexed column, pushing the aggregation to the source may avoid data transfer entirely.

### Basic Aggregation

Group rows by one or more columns, then compute summary statistics for each group. Import aggregate functions from `datafusion::functions_aggregate::expr_fn` and use [`.alias()`] to name the output columns.

This example establishes `employees_df`—used throughout this section.

```rust
use datafusion::prelude::*;
use datafusion::functions_aggregate::expr_fn::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    // Sample data: employees_df [department, employee, salary]
    let employees_df = dataframe!(
        "department" => ["Sales", "Sales", "Engineering", "Engineering"],
        "employee" => ["Alice", "Bob", "Carol", "Dave"],
        "salary" => [50000, 55000, 80000, 85000]
    )?;

    // Group by department, compute multiple aggregations
    let result = employees_df.clone().aggregate(
        vec![col("department")],
        vec![
            sum(col("salary")).alias("total_salary"),
            avg(col("salary")).alias("avg_salary"),
            count(col("employee")).alias("employee_count")
        ]
    )?;

    result.show().await?;
    // +-------------+--------------+------------+----------------+
    // | department  | total_salary | avg_salary | employee_count |
    // +-------------+--------------+------------+----------------+
    // | Engineering | 165000       | 82500.0    | 2              |
    // | Sales       | 105000       | 52500.0    | 2              |
    // +-------------+--------------+------------+----------------+

    Ok(())
}
```

> **Limitation:** The _result_ of [`.aggregate()`] contains only grouping columns and aggregated expressions—the `employee` column is gone. If you need it, include it in the group-by or aggregate it (e.g., `array_agg(col("employee"))`). The original DataFrame is immutable; `employees_df` still has all columns.

### Intermediate: Multi-Level Grouping and HAVING-Style Filtering

To replicate SQL's [`HAVING`] clause, chain [`.filter()`] _after_ [`.aggregate()`]—the filter sees the aggregated column names.

```rust
use datafusion::prelude::*;
use datafusion::functions_aggregate::expr_fn::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let employees_df = dataframe!(
        "department" => ["Sales", "Sales", "Engineering", "Engineering"],
        "employee" => ["Alice", "Bob", "Carol", "Dave"],
        "salary" => [50000, 55000, 80000, 85000]
    )?;

    // HAVING equivalent: filter on aggregated values
    let high_budget_depts = employees_df.clone()
        .aggregate(
            vec![col("department")],
            vec![sum(col("salary")).alias("total_salary")]
        )?
        .filter(col("total_salary").gt(lit(100000)))?;  // HAVING total_salary > 100000

    high_budget_depts.show().await?;
    // +-------------+--------------+
    // | department  | total_salary |
    // +-------------+--------------+
    // | Engineering | 165000       |
    // +-------------+--------------+
    // Sales (105000) filtered out — doesn't meet HAVING condition

    Ok(())
}
```

> **Key insight:** In SQL, [`HAVING`] filters _after_ grouping while [`WHERE`] filters _before_ ([SQL clause order]). In DataFrames, method order achieves the same: `.filter().aggregate()` = WHERE, `.aggregate().filter()` = HAVING.

### Advanced: All Aggregate Functions

DataFusion provides a comprehensive set of aggregate functions beyond the basics. Import them from [`datafusion::functions_aggregate::expr_fn`][expr_fn] and combine multiple aggregations in a single [`.aggregate()`] call for efficiency.

| Category    | Functions                                                                           |
| :---------- | :---------------------------------------------------------------------------------- |
| Basic       | [`count()`], [`sum()`], [`avg()`], [`min()`], [`max()`]                             |
| Statistical | [`stddev()`], [`var_sample()`], [`var_pop()`], [`median()`], [`approx_median()`]\*  |
| Distinct    | [`count_distinct()`], [`approx_distinct()`]\*                                       |
| Conditional | [`sum(when(...).otherwise(...))`][`CaseBuilder`]\*\* — aggregate only matching rows |
| Collection  | [`array_agg()`], [`string_agg()`]                                                   |

**\*Approximate functions** use probabilistic algorithms (e.g., [HyperLogLog] for [`approx_distinct()`]) that trade exactness for speed and memory. Use them on large datasets where exact results would be too expensive—typical error is <2%.

\*\* **conditional** -functions are available through the [`CaseBuilder`] struct.
See::

- [`when()`]
- [`otherwise()`]

The following example demonstrates several aggregate functions in a single call:

```rust
use datafusion::prelude::*;
use datafusion::functions_aggregate::expr_fn::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let employees_df = dataframe!(
        "department" => ["Sales", "Sales", "Engineering", "Engineering"],
        "employee" => ["Alice", "Bob", "Carol", "Dave"],
        "salary" => [50000, 55000, 80000, 85000]
    )?;

    let stats = employees_df.clone().aggregate(
        vec![col("department")],
        vec![
            count(col("employee")).alias("count"),
            sum(col("salary")).alias("sum"),
            avg(col("salary")).alias("avg"),
            min(col("salary")).alias("min"),
            max(col("salary")).alias("max"),
            stddev(col("salary")).alias("stddev"),
            // Conditional: count employees earning > 70k
            sum(when(col("salary").gt(lit(70000)), lit(1))
                .otherwise(lit(0))?).alias("high_earners")
        ]
    )?;

    stats.show().await?;
    // +-------------+-------+--------+---------+-------+-------+---------+--------------+
    // | department  | count | sum    | avg     | min   | max   | stddev  | high_earners |
    // +-------------+-------+--------+---------+-------+-------+---------+--------------+
    // | Engineering | 2     | 165000 | 82500.0 | 80000 | 85000 | 3535.53 | 2            |
    // | Sales       | 2     | 105000 | 52500.0 | 50000 | 55000 | 3535.53 | 0            |
    // +-------------+-------+--------+---------+-------+-------+---------+--------------+

    Ok(())
}
```

> **Tip:** See the full list of aggregate functions in the [Aggregate Functions Reference](../../user-guide/sql/aggregate_functions.md).

### Advanced: Aggregation Without Grouping

Pass an empty `vec![]` as the grouping columns to aggregate the entire DataFrame into a single row—equivalent to SQL without a `GROUP BY` clause.

```rust
use datafusion::prelude::*;
use datafusion::functions_aggregate::expr_fn::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let employees_df = dataframe!(
        "department" => ["Sales", "Sales", "Engineering", "Engineering"],
        "employee" => ["Alice", "Bob", "Carol", "Dave"],
        "salary" => [50000, 55000, 80000, 85000]
    )?;

    // Aggregate entire DataFrame (no GROUP BY) — summarize all rows
    let company_totals = employees_df.clone().aggregate(
        vec![],  // Empty group by = entire DataFrame
        vec![
            sum(col("salary")).alias("company_total"),
            avg(col("salary")).alias("company_avg")
        ]
    )?;

    company_totals.show().await?;
    // +---------------+-------------+
    // | company_total | company_avg |
    // +---------------+-------------+
    // | 270000        | 67500.0     |
    // +---------------+-------------+

    Ok(())
}
```

### Aggregation Troubleshooting

| Symptom          | Cause                                        | Fix                                                                           |
| :--------------- | :------------------------------------------- | :---------------------------------------------------------------------------- |
| Wrong results    | Unexpected grouping keys                     | Verify with `df.select(vec![col("key")]).distinct()?.show().await?`           |
| Column not found | Non-aggregated columns disappear             | Include in group-by or aggregate (e.g., [`array_agg()`])                      |
| Nulls skipped    | Aggregate functions ignore `NULL` by default | Use [`count(*)`][`count()`] for row count, or [`coalesce()`] to replace nulls |

### Further Reading

**DataFusion Resources:**

- [Aggregate Functions Reference (SQL)](../../user-guide/sql/aggregate_functions.md) — Complete list of built-in aggregate functions with SQL examples
- [`datafusion-functions-aggregate` crate](https://docs.rs/datafusion-functions-aggregate/latest/datafusion_functions_aggregate/) — Rust API docs for all aggregate function implementations

**Concepts & Theory:**

- [SQL GROUP BY (PostgreSQL docs)](https://www.postgresql.org/docs/current/queries-table-expressions.html#QUERIES-GROUP) — Canonical reference for grouping semantics
- [HyperLogLog Algorithm](https://en.wikipedia.org/wiki/HyperLogLog) — How `approx_distinct()` achieves O(1) memory

---
