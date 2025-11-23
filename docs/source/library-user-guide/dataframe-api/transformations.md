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

# DataFrame Transformations

Master the art of data transformation with DataFusion's powerful DataFrame API. This comprehensive guide takes you from basic operations to advanced patterns, with practical examples, debugging techniques, and real-world workflows.

> **Style Note:** In this guide, all code elements are highlighted with backticks. DataFrame methods are written as `.method()` (e.g., `.select()`) to reflect the chaining syntax central to the API. This distinguishes them from standalone functions (e.g., `col()`) and static constructors (e.g., `SessionContext::new()`). Rust types are formatted as `TypeName` (e.g., `SchemaRef`).

```{contents}
:local:
:depth: 2
```

## Introduction: Why DataFrames Matter

DataFrames provide a programmatic, type-safe way to transform data that complements SQL while offering unique advantages. While SQL excels at declarative queries, DataFrames shine when you need to:

- **Build queries dynamically** based on runtime conditions
- **Compose reusable transformations** as functions
- **Leverage type safety** to catch errors at compile time
- **Iterate over columns or datasets** programmatically
- **Chain operations fluently** for readable data pipelines

**Key characteristics of DataFrames:**

- **Immutable**: Each transformation returns a new DataFrame, enabling safe composition
- **Lazy evaluation**: Operations build a query plan ([Lazy Evaluation]); execution happens only when you call [`collect()`], [`show()`], or stream results
- **Optimized automatically**: DataFusion applies projection pushdown, predicate pushdown, and other optimizations transparently (similar to [Apache Spark DataFrames])

### How to Use This Guide

This guide is designed for both learning and reference:

**📖 Learning Path** (recommended for newcomers):

1. Start with [Quick Start](#quick-start-5-minutes) to see DataFrames in action
2. Follow [The Data Cleaning Journey](#the-data-cleaning-journey) to learn core concepts through a narrative workflow
3. Explore [Deep Dive sections](#selection-projection-mastery) for comprehensive coverage of each operation

**📚 Reference Path** (for experienced users):

- Jump directly to any [Deep Dive section](#selection-projection-mastery) for detailed API coverage
- Use the [SQL ↔ DataFrame Reference Table](#dataframe-sql-quick-reference) for quick lookups
- Check [Troubleshooting](#troubleshooting-guide) when you encounter errors
- Consult [Anti-Patterns](#anti-pattern-gallery) to avoid common pitfalls

**🔍 When You're Stuck**:

- Quick fixes: Look for _"Quick Debug"_ tips in each transformation section
- Systematic debugging: See [The DataFrame Detective Toolkit](#the-dataframe-detective-toolkit)
- Common errors: Check the [Troubleshooting Guide](#troubleshooting-guide)

## Quick Start (5 Minutes)

Let's see the power of DataFrames with a complete example. This 20-line program loads data, cleans it, analyzes it, and outputs results:

```rust
use datafusion::prelude::*;
use datafusion::functions_aggregate::expr_fn::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    // Create sample sales data with quality issues
    let sales = dataframe!(
        "product" => ["Laptop", "Mouse", "laptop", "Keyboard", "mouse"],
        "revenue" => [1200, 25, 1100, 75, 30],
        "quantity" => [2, 50, 1, 10, 45]
    )?;

    // Transform: normalize names, aggregate, and rank
    let result = sales
        .with_column("product", lower(col("product")))?  // Normalize to lowercase
        .aggregate(
            vec![col("product")],
            vec![
                sum(col("revenue")).alias("total_revenue"),
                sum(col("quantity")).alias("total_quantity")
            ]
        )?
        .sort(vec![col("total_revenue").sort(false, true)])?;  // DESC

    result.show().await?;
    // +---------+---------------+----------------+
    // | product | total_revenue | total_quantity |
    // +---------+---------------+----------------+
    // | laptop  | 2300          | 3              |
    // | keyboard| 75            | 10             |
    // | mouse   | 55            | 95             |
    // +---------+---------------+----------------+

    Ok(())
}
```

**What just happened?**

1. **Created data** with the `dataframe!` macro
2. **Normalized** product names with a computed column
3. **Aggregated** revenue and quantity by product
4. **Sorted** results by revenue (highest first)
5. **Displayed** the results

Notice how operations **chain together** fluently, the **query is built lazily**, and DataFusion **optimizes automatically**. Now let's dive deeper!

## DataFrame ↔ SQL Quick Reference

DataFrames provide a programmatic API for data transformations. If you're coming from SQL, this table maps familiar SQL operations to their DataFrame equivalents:

| Category             | SQL Operation                   | DataFrame Method(s)                                     | Key Differences                                                                          |
| -------------------- | ------------------------------- | ------------------------------------------------------- | ---------------------------------------------------------------------------------------- |
| **Filtering**        | `WHERE condition`               | [`filter()`]                                            | Chainable predicates with `.and()`, `.or()`; helpers: `in_list()`, `like()`, `between()` |
| **Selection**        | `SELECT col1, col2`             | [`select_columns()`], [`select()`]                      | [`select()`] supports expressions; see [SQL SELECT](../user-guide/sql/index.md)          |
| **Selection**        | `SELECT expr AS alias`          | [`select()`], [`with_column()`]                         | [`with_column()`] adds or replaces columns (not in SQL)                                  |
| **Selection**        | `... AS new_name` (rename)      | [`with_column_renamed()`]                               | Rename one column at a time                                                              |
| **Aggregation**      | `GROUP BY ... aggregate(...)`   | [`aggregate()`]                                         | See [Aggregate functions](../user-guide/sql/aggregate_functions.md)                      |
| **Aggregation**      | `HAVING condition`              | [`aggregate()`] then [`filter()`]                       | HAVING implemented as post-aggregation filter                                            |
| **Joins**            | `INNER/LEFT/RIGHT/FULL JOIN`    | [`join()`], [`join_on()`]                               | [`join_on()`] supports arbitrary join conditions; includes `Semi` and `Anti` joins       |
| **Joins**            | `CROSS JOIN`                    | [`join()`] with empty keys                              | Pass empty `&[]` for left and right keys                                                 |
| **Sorting/Limiting** | `ORDER BY ... LIMIT n OFFSET m` | [`sort()`], [`limit()`]                                 | `limit(skip, fetch)`; [`show_limit()`] for quick preview without full execution          |
| **Set Operations**   | `UNION ALL`                     | [`union()`]                                             | —                                                                                        |
| **Set Operations**   | `UNION` (distinct)              | [`union_distinct()`]                                    | —                                                                                        |
| **Set Operations**   | `UNION ALL` by name             | [`union_by_name()`]                                     | **DataFrame-only:** aligns by column name, not position                                  |
| **Set Operations**   | `UNION` by name (distinct)      | [`union_by_name_distinct()`]                            | **DataFrame-only:** name-based alignment + deduplication                                 |
| **Set Operations**   | `DISTINCT`                      | [`distinct()`]                                          | —                                                                                        |
| **Set Operations**   | `DISTINCT ON (cols)`            | [`distinct_on()`]                                       | **DataFrame-only:** Postgres-style, not ANSI SQL                                         |
| **Set Operations**   | `INTERSECT` / `EXCEPT`          | [`intersect()`], [`except()`]                           | —                                                                                        |
| **Window Functions** | `ROW_NUMBER() OVER (...)`       | [`window()`]                                            | Add window exprs; see [Window functions](../user-guide/sql/window_functions.md)          |
| **Reshaping**        | `UNNEST` / `EXPLODE`            | [`unnest_columns()`], [`unnest_columns_with_options()`] | Fine-grained control via [`unnest_columns_with_options()`]                               |

**Key differences from SQL:**

- **Lazy evaluation**: DataFrame methods build a plan; execution happens on [`collect()`], [`show()`], etc.
- **Immutable**: Each method returns a new DataFrame
- **Programmatic**: Easy to conditionally build queries, loop over columns, parameterize transformations
- **Type-safe**: Rust compiler catches many errors at compile time

> **See also:** [Mixing SQL and DataFrames](#mixing-sql-and-dataframes) for combining both approaches

## The Data Cleaning Journey

Let's learn DataFrame transformations by solving a real problem: cleaning messy sales data. We'll encounter common data quality issues and fix them step-by-step, introducing DataFrame operations as we need them.

> **Inspired by**: [PySpark DataFrame API] data preparation patterns

### Meet Your Data: The Challenge

You've received a sales dataset exported from a legacy system. It has all the problems you'd expect from real-world data:

```rust
use datafusion::prelude::*;
use datafusion::functions_aggregate::expr_fn::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    // Our messy sales data - note the quality issues!
    let sales = dataframe!(
        "order_id" => [1, 2, 2, 3, 4, 4, None],
        "customer" => ["Alice", "bob", "BOB", " Charlie ", "Dave", "Dave", "Eve"],
        "amount" => [100.0, -50.0, 200.0, 150.0, f64::NAN, 300.0, 99999.0],
        "status" => ["complete", "pending", "PENDING", "complete", None, "complete", "pending"],
        "date" => ["2024-01-01", "2024-01-02", "2024-01-02", "invalid", "2024-01-04", "2024-01-04", "2024-01-05"]
    )?;

    sales.show().await?;
    Ok(())
}
```

**Output:**

```
+----------+----------+--------+----------+------------+
| order_id | customer | amount | status   | date       |
+----------+----------+--------+----------+------------+
| 1        | Alice    | 100.0  | complete | 2024-01-01 |
| 2        | bob      | -50.0  | pending  | 2024-01-02 |
| 2        | BOB      | 200.0  | PENDING  | 2024-01-02 |
| 3        | Charlie  | 150.0  | complete | invalid    |
| 4        | Dave     | NaN    | NULL     | 2024-01-04 |
| 4        | Dave     | 300.0  | complete | 2024-01-04 |
| NULL     | Eve      | 99999.0| pending  | 2024-01-05 |
+----------+----------+--------+----------+------------+
```

**Problems we can see:**

- **Duplicates**: order_id 2 and 4 appear twice
- **Inconsistent casing**: "bob" vs "BOB", "pending" vs "PENDING"
- **Extra whitespace**: " Charlie " has leading/trailing spaces
- **Invalid data**: negative amounts, NaN values, invalid dates
- **Outliers**: 99999.0 is suspiciously high
- **Nulls**: Missing order_id and status values

**Our goal**: Transform this into clean, analyzable data. By the end, we'll have total sales by customer with all issues resolved.

### Step 1: Understanding What We Have

Before cleaning, let's inspect the data structure:

```rust
// Check the schema
println!("Schema: {:?}", sales.schema());

// Count total rows
let count = sales.clone().count().await?;
println!("Total rows: {}", count);

// Preview first few rows
sales.clone().limit(0, Some(3))?.show().await?;
```

_Quick Debug_: Always start with `show()` and `schema()` to understand your data before transforming it. Use `limit()` to preview large datasets without loading everything.

### Step 2: Filtering Out Invalid Records

First, let's remove rows with critical missing data or obvious errors:

```rust
// Remove rows where order_id is null or amount is negative/NaN/outlier
let step1 = sales
    .filter(col("order_id").is_not_null())?
    .filter(col("amount").gt(lit(0)))?         // Remove negatives
    .filter(col("amount").lt(lit(10000)))?;    // Remove outliers

step1.show().await?;
```

**Output after Step 1:**

```
+----------+----------+--------+----------+------------+
| order_id | customer | amount | status   | date       |
+----------+----------+--------+----------+------------+
| 1        | Alice    | 100.0  | complete | 2024-01-01 |
| 2        | BOB      | 200.0  | PENDING  | 2024-01-02 |
| 3        | Charlie  | 150.0  | complete | invalid    |
| 4        | Dave     | 300.0  | complete | 2024-01-04 |
+----------+----------+--------+----------+------------+
```

_Quick Debug_: If `filter()` returns no rows, check for nulls with `col("column").is_not_null()` first. DataFusion's null handling follows [Three-valued logic] (SQL standard).

### Step 3: Cleaning Text Data

Now let's normalize the text columns—lowercase everything and remove extra spaces:

```rust
use datafusion::functions::string::expr_fn::*;

let step2 = step1
    .with_column("customer", trim(vec![lower(col("customer"))]))?
    .with_column("status", lower(col("status")))?;

step2.show().await?;
```

**Output after Step 2:**

```
+----------+----------+--------+----------+------------+
| order_id | customer | amount | status   | date       |
+----------+----------+--------+----------+------------+
| 1        | alice    | 100.0  | complete | 2024-01-01 |
| 2        | bob      | 200.0  | pending  | 2024-01-02 |
| 3        | charlie  | 150.0  | complete | invalid    |
| 4        | dave     | 300.0  | complete | 2024-01-04 |
+----------+----------+--------+----------+------------+
```

_Quick Debug_: Join failing unexpectedly? Check for trailing spaces with `trim()`. Case mismatches are another common culprit—use `lower()` or `upper()` to normalize.

### Step 4: Removing Duplicates

We still have duplicate order_ids. Let's use `distinct()` based on order_id to keep only unique records:

```rust
// For more control, use distinct_on to specify which columns define uniqueness
let step3 = step2.distinct_on(
    vec![col("order_id")],
    vec![col("amount").sort(false, true)],  // Keep the row with highest amount
    None
)?;

step3.show().await?;
```

**Output after Step 3:**

```
+----------+----------+--------+----------+------------+
| order_id | customer | amount | status   | date       |
+----------+----------+--------+----------+------------+
| 1        | alice    | 100.0  | complete | 2024-01-01 |
| 2        | bob      | 200.0  | pending  | 2024-01-02 |
| 3        | charlie  | 150.0  | complete | invalid    |
| 4        | dave     | 300.0  | complete | 2024-01-04 |
+----------+----------+--------+----------+------------+
```

_Quick Debug_: Still seeing duplicates after `distinct()`? Make sure you're checking all the columns that define uniqueness. Use `distinct_on()` for more control.

### Step 5: Computing Derived Columns

Let's add some useful computed columns:

```rust
let step4 = step3
    .with_column("total_value", col("amount"))?  // Could add quantity here
    .with_column("year", lit(2024))?;             // Extract from date in real scenario

step4.show().await?;
```

### Step 6: Aggregating for Insights

Finally, let's aggregate by customer to see total sales:

```rust
let final_result = step4
    .aggregate(
        vec![col("customer")],
        vec![
            sum(col("amount")).alias("total_sales"),
            count(col("order_id")).alias("order_count"),
            avg(col("amount")).alias("avg_order_value")
        ]
    )?
    .sort(vec![col("total_sales").sort(false, true)])?;  // Highest sales first

final_result.show().await?;
```

**Final Output:**

```
+----------+-------------+-------------+-----------------+
| customer | total_sales | order_count | avg_order_value |
+----------+-------------+-------------+-----------------+
| dave     | 300.0       | 1           | 300.0           |
| bob      | 200.0       | 1           | 200.0           |
| charlie  | 150.0       | 1           | 150.0           |
| alice    | 100.0       | 1           | 100.0           |
+----------+-------------+-------------+-----------------+
```

_Quick Debug_: Wrong totals? Verify your grouping keys with a quick `step4.select(vec![col("customer")]).distinct()?.show().await?` before aggregating.

### What We Learned

Through this cleaning journey, we used:

- **Filtering**: `filter()` with predicates to remove invalid data
- **Text operations**: `lower()`, `trim()` for normalization
- **Deduplication**: `distinct_on()` for controlled duplicate removal
- **Computed columns**: `with_column()` for derived values
- **Aggregation**: `aggregate()` with multiple aggregate functions
- **Sorting**: `sort()` to order results

**The complete pipeline** (all steps chained):

```rust
let clean_result = sales
    .filter(col("order_id").is_not_null())?
    .filter(col("amount").gt(lit(0)).and(col("amount").lt(lit(10000))))?
    .with_column("customer", trim(vec![lower(col("customer"))]))?
    .with_column("status", lower(col("status")))?
    .distinct_on(vec![col("order_id")], vec![col("order_date").sort(true, true)], None)?
    .aggregate(
        vec![col("customer")],
        vec![
            sum(col("amount")).alias("total_sales"),
            count(col("order_id")).alias("order_count")
        ]
    )?
    .sort(vec![col("total_sales").sort(false, true)])?;
```

Notice how **method chaining** creates a readable pipeline, and DataFusion **optimizes the entire query** before execution!

Now that you've seen the concepts in action, let's explore each transformation in depth.

## Deep Dive: Transformation Reference

This section provides comprehensive coverage of each DataFrame operation. Use it as a reference when you need detailed information about specific transformations.

### Selection and Projection Mastery

Select specific columns, add computed columns, or rename columns using [`select()`], [`select_columns()`], [`with_column()`], [`with_column_renamed()`], and [`drop_columns()`].

**SQL equivalent:** `SELECT a, b AS b_renamed, a + b AS sum FROM table`

#### Basic Selection

```rust
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let df = dataframe!(
        "product" => ["Laptop", "Mouse", "Keyboard"],
        "price" => [1200, 25, 75],
        "quantity" => [5, 50, 30],
        "category" => ["Electronics", "Accessories", "Accessories"]
    )?;

    // Select specific columns by name
    let df = df.select_columns(&["product", "price"])?;
    df.show().await?;

    Ok(())
}
```

#### Intermediate: Expressions and Computed Columns

```rust
// Select with expressions (computed columns)
let df = df.select(vec![
    col("product"),
    col("price"),
    (col("price") * col("quantity")).alias("total_value"),
    when(col("price").gt(lit(100)), lit("Premium"))
        .otherwise(lit("Standard"))?
        .alias("tier")
])?;

// Add a new column (or replace existing)
let df = df.with_column("discounted_price", col("price") * lit(0.9))?;

// Rename a column
let df = df.with_column_renamed("total_value", "revenue")?;

// Drop specific columns
let df = df.drop_columns(&["category"])?;
```

#### Advanced: Dynamic Column Selection

```rust
// Programmatically select columns based on conditions
let schema = df.schema();
let numeric_cols: Vec<_> = schema
    .fields()
    .iter()
    .filter(|f| matches!(f.data_type(), DataType::Int32 | DataType::Float64))
    .map(|f| col(f.name()))
    .collect();

let numeric_df = df.select(numeric_cols)?;
```

#### Anti-Pattern: Over-Selection

```rust
// DON'T: Select all columns when you only need a few
let bad = df.select(vec![col("*")])?  // Reads everything
    .filter(col("price").gt(lit(100)))?;

// DO: Select only what you need (projection pushdown optimization)
let good = df
    .select(vec![col("product"), col("price")])?
    .filter(col("price").gt(lit(100)))?;
```

_Quick Debug_: **Column not found error?** Check column names with `df.schema()`. DataFusion is case-sensitive. Use `df.schema().field_names()` to list all available columns.

> **Going deeper:**
>
> - **Advanced:** [`select_exprs()`] for SQL-like string expressions
> - **Examples:** See `datafusion-examples/examples/expr_api.rs` for complex expression patterns
> - **Performance:** Projection pushdown automatically optimizes column reads

### Filtering Excellence

Filter rows based on conditions using [`filter()`]. Predicates can be simple or arbitrarily complex.

**SQL equivalent:** `WHERE condition`

#### Basic Filtering

```rust
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let df = dataframe!(
        "product" => ["Laptop", "Mouse", "Keyboard", "Monitor"],
        "price" => [1200, 25, 75, 300],
        "quantity" => [5, 50, 30, 10]
    )?;

    // Simple filter
    let df = df.filter(col("price").gt(lit(100)))?;
    df.show().await?;

    Ok(())
}
```

#### Intermediate: Complex Predicates

```rust
// Multiple conditions with AND/OR
let df = df.filter(
    col("price").gt(lit(50))
        .and(col("quantity").lt(lit(40)))
        .or(col("product").eq(lit("Laptop")))
)?;

// Null handling
let df = df.filter(col("price").is_not_null())?;

// IN list
let df = df.filter(in_list(
    col("product"),
    vec![lit("Laptop"), lit("Monitor")],
    false  // negated
)?)?;

// String matching
let df = df.filter(col("product").like(lit("%top%")))?;  // Contains "top"

// Range checks
let df = df.filter(col("price").between(lit(50), lit(500)))?;
```

#### Advanced: Dynamic Filter Building

```rust
// Build filters programmatically based on runtime conditions
fn build_filter(min_price: Option<i32>, max_quantity: Option<i32>) -> Expr {
    let mut conditions = vec![lit(true)];  // Start with always-true

    if let Some(price) = min_price {
        conditions.push(col("price").gt_eq(lit(price)));
    }

    if let Some(qty) = max_quantity {
        conditions.push(col("quantity").lt_eq(lit(qty)));
    }

    // Combine all conditions with AND
    conditions.into_iter().reduce(|acc, cond| acc.and(cond)).unwrap()
}

let filter = build_filter(Some(100), None);
let df = df.filter(filter)?;
```

#### Anti-Pattern: Multiple Sequential Filters

```rust
// DON'T: Chain multiple filter calls (creates separate plan nodes)
let bad = df
    .filter(col("price").gt(lit(50)))?
    .filter(col("quantity").lt(lit(100)))?
    .filter(col("product").is_not_null())?;

// DO: Combine into single filter (more efficient)
let good = df.filter(
    col("price").gt(lit(50))
        .and(col("quantity").lt(lit(100)))
        .and(col("product").is_not_null())
)?;
```

_Quick Debug_: **Filter returning no rows?** Check for nulls—[Three-valued logic] means `NULL > 5` is `NULL` (not true or false). Always handle nulls explicitly with `.is_not_null()` when needed.

_Quick Debug_: **Filter too slow?** Ensure filter columns are in your data source's partition scheme for predicate pushdown. Check the optimized plan with `df.explain(false, false)?` to verify pushdown happened.

### Aggregation Patterns

Use [`aggregate()`] for GROUP BY operations with aggregate functions to summarize data.

**SQL equivalent:** `SELECT dept, SUM(salary) FROM table GROUP BY dept`

#### Basic Aggregation

```rust
use datafusion::prelude::*;
use datafusion::functions_aggregate::expr_fn::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let df = dataframe!(
        "department" => ["Sales", "Sales", "Engineering", "Engineering"],
        "employee" => ["Alice", "Bob", "Carol", "Dave"],
        "salary" => [50000, 55000, 80000, 85000]
    )?;

    // Group by with multiple aggregations
    let result = df.aggregate(
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
    // | Engineering | 165000       | 82500      | 2              |
    // | Sales       | 105000       | 52500      | 2              |
    // +-------------+--------------+------------+----------------+

    Ok(())
}
```

#### Intermediate: Multi-Level Grouping and HAVING-Style Filtering

```rust
// Multi-column grouping
let df = df.aggregate(
    vec![col("department"), col("location")],  // GROUP BY dept, location
    vec![sum(col("salary")).alias("total")]
)?;

// HAVING clause equivalent: filter after aggregation
let result = df
    .aggregate(vec![col("department")], vec![sum(col("salary")).alias("total")])?
    .filter(col("total").gt(lit(100000)))?;  // HAVING total > 100000
```

#### Advanced: All Aggregate Functions

```rust
use datafusion::functions_aggregate::expr_fn::*;

let stats = df.aggregate(
    vec![col("department")],
    vec![
        count(col("employee")).alias("count"),
        sum(col("salary")).alias("sum"),
        avg(col("salary")).alias("avg"),
        min(col("salary")).alias("min"),
        max(col("salary")).alias("max"),
        stddev(col("salary")).alias("stddev"),
        variance(col("salary")).alias("variance"),
        // Count distinct
        count_distinct(col("employee")).alias("unique_employees"),
        // Conditional aggregation
        sum(when(col("salary").gt(lit(70000)), lit(1))
            .otherwise(lit(0))?).alias("high_earners")
    ]
)?;
```

#### Advanced: Aggregation Without Grouping

```rust
// Aggregate entire DataFrame (no GROUP BY)
let total = df.aggregate(
    vec![],  // Empty group by
    vec![
        sum(col("salary")).alias("company_total"),
        avg(col("salary")).alias("company_avg")
    ]
)?;
```

_Quick Debug_: **Wrong aggregation results?** Verify grouping keys before aggregating: `df.select(vec![col("dept")]).distinct()?.show().await?` to see unique values.

_Quick Debug_: **Column not found in aggregation?** After `aggregate()`, only the grouping columns and aggregated expressions are available. If you need other columns, include them in the group by or aggregate them.

### When DataFrames Collide: Join Patterns

DataFusion supports all standard [SQL-99 Standard] join types plus powerful Semi and Anti joins.

**SQL equivalent:** `SELECT * FROM left JOIN right ON left.id = right.user_id`

#### Understanding Join Types: Visual Guide

Given two tables:

**customers** (left):

```
+----+-------+
| id | name  |
+----+-------+
| 1  | Alice |
| 2  | Bob   |
| 3  | Carol |
+----+-------+
```

**orders** (right):

```
+----------+-------------+--------+
| order_id | customer_id | amount |
+----------+-------------+--------+
| 101      | 1           | 100    |
| 102      | 1           | 200    |
| 103      | 2           | 150    |
| 104      | 99          | 300    |
+----------+-------------+--------+
```

**Join Results:**

| Join Type    | Returns                       | Use Case                                       | Result Rows                        |
| ------------ | ----------------------------- | ---------------------------------------------- | ---------------------------------- |
| **Inner**    | Matches from both sides       | Standard join, only matching records           | 3 (Alice×2, Bob×1)                 |
| **Left**     | All left + matches from right | Keep all customers, show their orders (if any) | 4 (Alice×2, Bob×1, Carol×0)        |
| **Right**    | All right + matches from left | Keep all orders, show customer (if exists)     | 4 (orders 101,102,103,104)         |
| **Full**     | Everything from both sides    | Union of Left and Right                        | 5 (all customers + orphaned order) |
| **LeftSemi** | Left rows that have matches   | "Which customers have orders?"                 | 2 (Alice, Bob)                     |
| **LeftAnti** | Left rows with no matches     | "Which customers have NO orders?"              | 1 (Carol)                          |
| **Cross**    | Cartesian product             | All combinations (use carefully!)              | 12 (3×4)                           |

#### Basic: Inner Join

```rust
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let customers = dataframe!(
        "id" => [1, 2, 3],
        "name" => ["Alice", "Bob", "Carol"]
    )?;

    let orders = dataframe!(
        "order_id" => [101, 102, 103],
        "customer_id" => [1, 1, 2],
        "amount" => [100, 200, 150]
    )?;

    // Inner join: only customers with orders
    let df = customers.join(
        orders,
        JoinType::Inner,
        &["id"],
        &["customer_id"],
        None
    )?;

    df.show().await?;
    // +----+-------+----------+-------------+--------+
    // | id | name  | order_id | customer_id | amount |
    // +----+-------+----------+-------------+--------+
    // | 1  | Alice | 101      | 1           | 100    |
    // | 1  | Alice | 102      | 1           | 200    |
    // | 2  | Bob   | 103      | 2           | 150    |
    // +----+-------+----------+-------------+--------+

    Ok(())
}
```

#### Intermediate: Left/Right/Full Joins

```rust
// Left join: all customers, even without orders
let left_df = customers.join(
    orders.clone(),
    JoinType::Left,
    &["id"],
    &["customer_id"],
    None
)?;
// Result includes Carol with NULL order values

// Right join: all orders, even orphaned ones
let right_df = customers.join(
    orders.clone(),
    JoinType::Right,
    &["id"],
    &["customer_id"],
    None
)?;
// Result includes order 104 with NULL customer

// Full outer join: everything
let full_df = customers.join(
    orders,
    JoinType::Full,
    &["id"],
    &["customer_id"],
    None
)?;
```

#### Intermediate: Semi and Anti Joins

```rust
// LeftSemi: customers who HAVE orders (like EXISTS in SQL)
let with_orders = customers.clone().join(
    orders.clone(),
    JoinType::LeftSemi,
    &["id"],
    &["customer_id"],
    None
)?;
// Returns: Alice, Bob (only customer columns, no order data)

// LeftAnti: customers with NO orders (like NOT EXISTS)
let without_orders = customers.join(
    orders.clone(),
    JoinType::LeftAnti,
    &["id"],
    &["customer_id"],
    None
)?;
// Returns: Carol
```

#### Advanced: Multi-Way Joins

```rust
let products = dataframe!(
    "product_id" => [1, 2],
    "product_name" => ["Widget", "Gadget"]
)?;

let order_items = dataframe!(
    "order_id" => [101, 102],
    "product_id" => [1, 2]
)?;

// Join three tables
let result = orders
    .join(customers, JoinType::Inner, &["customer_id"], &["id"], None)?
    .join(order_items, JoinType::Inner, &["order_id"], &["order_id"], None)?
    .join(products, JoinType::Inner, &["product_id"], &["product_id"], None)?;
```

#### Advanced: Join with Complex Conditions

```rust
// Join with additional filter (not just equality)
let df = customers.join_on(
    orders,
    JoinType::Inner,
    col("customers.id").eq(col("orders.customer_id"))
        .and(col("orders.amount").gt(lit(100)))
)?;
```

#### Anti-Pattern: Accidental Cartesian Product

```rust
// DANGEROUS: Cross join without filter (3 × 4 = 12 rows)
let bad = customers.join(
    orders,
    JoinType::Inner,
    &[],  // Empty keys = Cartesian product!
    &[],
    None
)?;

// DO: Always specify join keys
let good = customers.join(
    orders,
    JoinType::Inner,
    &["id"],
    &["customer_id"],
    None
)?;
```

_Quick Debug_: **Join returns empty?** Check for: (1) key column name mismatches, (2) trailing spaces in data (`trim()` both sides), (3) case sensitivity, (4) null values in keys. Use `show()` on both DataFrames before joining to inspect keys.

_Quick Debug_: **Too many rows after join?** You might have duplicates in the join keys. Check with `df.select(vec![col("key")]).distinct().count()` on both sides. Consider using `distinct_on()` before joining if needed.

_Quick Debug_: **Schema confusion?** After a join, both tables' columns are available, but if they have the same name, you need to qualify them: `col("customers.id")` vs `col("orders.id")`.

### Sorting and Limiting

Order results and implement pagination with [`sort()`] and [`limit()`].

**SQL equivalent:** `ORDER BY score DESC LIMIT 10 OFFSET 5`

#### Basic Sorting

```rust
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let df = dataframe!(
        "name" => ["Alice", "Bob", "Carol", "Dave"],
        "score" => [85, 92, 78, 95]
    )?;

    // Sort ascending
    let df = df.sort(vec![col("score").sort(true, true)])?;  // ASC, nulls last

    df.show().await?;
    Ok(())
}
```

#### Intermediate: Multi-Column Sorting and Pagination

```rust
// Sort by multiple columns
let df = df.sort(vec![
    col("score").sort(false, true), // DESC, nulls last (primary sort)
    col("name").sort(true, false)   // ASC, nulls first (tie-breaker)
])?;

// Pagination: skip 1, take 2 (OFFSET 1 LIMIT 2)
let page = df.limit(1, Some(2))?;

// Quick preview without full execution
let preview = df.show_limit(10).await?;  // Show first 10 rows
```

#### Advanced: Null Handling in Sorts

```rust
let df = dataframe!(
    "name" => ["Alice", "Bob", None, "Dave"],
    "score" => [85, 92, 78, None]
)?;

// Control null placement
let nulls_first = df.sort(vec![col("name").sort(true, false)])?;  // ASC, nulls first
let nulls_last = df.sort(vec![col("name").sort(true, true)])?;    // ASC, nulls last
```

_Quick Debug_: **Sort not working as expected?** Check for nulls—they can appear first or last depending on the null ordering parameter. Use `filter(col("column").is_not_null())` before sorting if needed.

### Set Operations and Deduplication

Combine DataFrames or remove duplicates using union, intersection, and distinct operations.

**SQL equivalent:** `SELECT * FROM df1 UNION SELECT * FROM df2`

#### Basic Set Operations

```rust
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let df1 = dataframe!("id" => [1, 2, 3])?;
    let df2 = dataframe!("id" => [3, 4, 5])?;

    // Union (preserves duplicates) - SQL: UNION ALL
    let union_all = df1.clone().union(df2.clone())?;
    // Result: [1, 2, 3, 3, 4, 5]

    // Union with duplicate removal - SQL: UNION
    let union_distinct = df1.clone().union_distinct(df2.clone())?;
    // Result: [1, 2, 3, 4, 5]

    // Distinct values
    let distinct = df1.distinct()?;

    Ok(())
}
```

#### Intermediate: DataFrame-Only Operations

```rust
// Union by name: aligns columns by name, not position (DataFrame-only!)
// Similar to Spark's unionByName - https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.unionByName.html
let sales_q1 = dataframe!(
    "product" => ["A", "B"],
    "revenue" => [100, 200]
)?;

let sales_q2 = dataframe!(
    "revenue" => [150, 250],  // Note: different column order
    "product" => ["A", "C"]
)?;

// Regular union would fail - different column order
// union_by_name works!
let combined = sales_q1.union_by_name(sales_q2)?;

// With deduplication
let combined_distinct = sales_q1.union_by_name_distinct(sales_q2)?;
```

#### Advanced: DISTINCT ON

```rust
// DISTINCT ON: keep first row for each unique value in specified columns
// PostgreSQL-specific feature - see: https://www.postgresql.org/docs/current/sql-select.html#SQL-DISTINCT
let df = dataframe!(
    "customer" => ["Alice", "Alice", "Bob", "Bob"],
    "order_date" => ["2024-01-01", "2024-01-05", "2024-01-02", "2024-01-03"],
    "amount" => [100, 200, 150, 175]
)?;

// Keep only the first order for each customer (PostgreSQL-style)
let first_orders = df.distinct_on(
    vec![col("customer")],           // Distinct on these columns
    vec![col("order_date").sort(true, true)],  // Order by (to control which row is kept)
    None
)?;
// Result: Alice's Jan 1 order, Bob's Jan 2 order
```

#### Other Set Operations

```rust
// Intersect: rows in both DataFrames
let common = df1.intersect(df2)?;

// Except: rows in df1 but not in df2 (SQL: EXCEPT / MINUS)
let df1_only = df1.except(df2)?;
```

_Quick Debug_: **Union fails with schema error?** The DataFrames must have the same schema (column names and types). Use `union_by_name()` for flexibility with column order, or use `select()` to align schemas before union.

_Quick Debug_: **distinct() not working?** It considers all columns. If you want distinct based on specific columns, use `distinct_on()` or select only those columns first.

### Window Functions

Apply calculations across rows related to the current row, like running totals, rankings, and moving averages.

**SQL equivalent:** `SELECT name, salary, ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) FROM employees`

> **Note:** Window functions follow the [SQL-99 Standard] specification. See also: [Window Functions](../user-guide/sql/window_functions.md) for all available window functions.

#### Basic: Ranking

```rust
use datafusion::prelude::*;
use datafusion::functions_window::expr_fn::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let df = dataframe!(
        "employee" => ["Alice", "Bob", "Carol", "Dave"],
        "department" => ["Sales", "Sales", "Eng", "Eng"],
        "salary" => [50000, 55000, 80000, 85000]
    )?;

    // Rank employees within each department by salary
    let result = df.window(vec![
        row_number()
            .partition_by(vec![col("department")])
            .order_by(vec![col("salary").sort(false, true)])
            .build()?
            .alias("rank")
    ])?;

    result.show().await?;
    Ok(())
}
```

#### Intermediate: Running Totals and Aggregates

```rust
use datafusion::functions_window::expr_fn::*;
use datafusion::functions_aggregate::expr_fn::*;

// Running total of sales
let df = df.window(vec![
    sum(col("amount"))
        .order_by(vec![col("date").sort(true, true)])
        .build()?
        .alias("running_total")
])?;

// Moving average (last 3 rows)
let df = df.window(vec![
    avg(col("amount"))
        .order_by(vec![col("date").sort(true, true)])
        .window_frame(WindowFrame::new(Some(false)))  // Customize frame
        .build()?
        .alias("moving_avg_3")
])?;
```

#### Advanced: lead() and lag()

```rust
// Compare with previous/next row
let df = df.window(vec![
    lag(col("amount"), Some(1), None)
        .partition_by(vec![col("customer")])
        .order_by(vec![col("date").sort(true, true)])
        .build()?
        .alias("prev_amount"),
    lead(col("amount"), Some(1), None)
        .partition_by(vec![col("customer")])
        .order_by(vec![col("date").sort(true, true)])
        .build()?
        .alias("next_amount")
])?;
```

_Quick Debug_: **Window function not partitioning correctly?** Ensure your `partition_by()` columns are the grouping you want. Without `partition_by()`, the window applies to the entire DataFrame.

### Reshaping Data

#### Unnesting / Exploding Arrays

```rust
// Expand array column into multiple rows (like SQL UNNEST or Spark explode)
// See also: PySpark explode - https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.explode.html
let df = dataframe!(
    "customer" => ["Alice", "Bob"],
    "orders" => [vec![1, 2, 3], vec![4, 5]]
)?;

// Unnest the orders array
let expanded = df.unnest_columns(&["orders"])?;
// Result:
// customer | orders
// Alice    | 1
// Alice    | 2
// Alice    | 3
// Bob      | 4
// Bob      | 5
```

## Advanced DataFrame Patterns

### Dynamic DataFrame Construction

Build DataFrames and queries programmatically at runtime based on schema discovery and conditional logic.

#### Runtime Schema Discovery

```rust
use datafusion::prelude::*;
use arrow::datatypes::DataType;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let df = dataframe!(
        "product" => ["Laptop", "Mouse"],
        "price" => [1200, 25],
        "quantity" => [5, 50],
        "name" => ["Dell XPS", "Logitech"]
    )?;

    // Discover all numeric columns at runtime
    let schema = df.schema();
    let numeric_cols: Vec<_> = schema
        .fields()
        .iter()
        .filter(|f| matches!(
            f.data_type(),
            DataType::Int32 | DataType::Int64 | DataType::Float32 | DataType::Float64
        ))
        .map(|f| col(f.name()))
        .collect();

    println!("Found {} numeric columns", numeric_cols.len());

    // Select only numeric columns
    let numeric_df = df.select(numeric_cols)?;
    numeric_df.show().await?;

    Ok(())
}
```

#### Iterator Patterns Over Columns

```rust
// Apply the same transformation to all numeric columns
let schema = df.schema();
let transformed_cols: Vec<_> = schema
    .fields()
    .iter()
    .map(|f| {
        if f.data_type().is_numeric() {
            // Multiply numeric columns by 2
            (col(f.name()) * lit(2)).alias(format!("{}_doubled", f.name()))
        } else {
            // Keep non-numeric as-is
            col(f.name())
        }
    })
    .collect();

let result = df.select(transformed_cols)?;
```

#### Metaprogramming: Reusable Transformation Functions

```rust
use datafusion::prelude::*;

// Reusable transformation function
fn normalize_text_columns(df: DataFrame) -> datafusion::error::Result<DataFrame> {
    use datafusion::functions::string::expr_fn::*;
    use arrow::datatypes::DataType;

    let schema = df.schema();
    let mut result = df;

    // Find all string columns and normalize them
    for field in schema.fields() {
        if matches!(field.data_type(), DataType::Utf8 | DataType::LargeUtf8) {
            result = result.with_column(
                field.name(),
                trim(vec![lower(col(field.name()))])
            )?;
        }
    }

    Ok(result)
}

// Usage
let cleaned_df = normalize_text_columns(df)?;
```

#### Conditional Query Building

```rust
// Build different queries based on runtime parameters
fn build_query(
    df: DataFrame,
    filter_price: Option<i32>,
    filter_category: Option<String>,
    sort_desc: bool,
) -> datafusion::error::Result<DataFrame> {
    let mut result = df;

    // Apply filters conditionally
    if let Some(price) = filter_price {
        result = result.filter(col("price").gt(lit(price)))?;
    }

    if let Some(category) = filter_category {
        result = result.filter(col("category").eq(lit(category)))?;
    }

    // Apply conditional sorting
    if sort_desc {
        result = result.sort(vec![col("price").sort(false, true)])?;
    } else {
        result = result.sort(vec![col("price").sort(true, true)])?;
    }

    Ok(result)
}
```

_Quick Debug_: When building queries dynamically, verify the generated column list or expressions with `println!("{:?}", cols)` before passing to DataFrame methods.

> **See also:**
>
> - [Apache Spark DataFrames] for similar dynamic construction patterns
> - [`expr_api.rs`] for advanced expression building examples
> - [`schema`] API documentation for schema introspection

### Memory Management & Streaming

Handle large datasets efficiently by understanding when to collect vs stream, and how to process data incrementally.

#### Understanding collect() vs show() vs execute_stream()

```rust
use datafusion::prelude::*;
use futures::StreamExt;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let df = dataframe!(
        "id" => [1, 2, 3, 4, 5],
        "value" => [100, 200, 300, 400, 500]
    )?;

    // show() - For humans: limits to 20 rows, formatted output
    // Safe for any size dataset
    df.clone().show().await?;

    // collect() - For programs: loads ALL data into memory
    // Only use for small datasets or after limit()
    let batches = df.clone().limit(0, Some(100))?.collect().await?;
    println!("Collected {} batches", batches.len());

    // execute_stream() - For large data: processes incrementally
    // Memory-efficient, handles datasets larger than RAM
    let mut stream = df.execute_stream().await?;
    while let Some(batch) = stream.next().await {
        let batch = batch?;
        println!("Processing batch with {} rows", batch.num_rows());
        // Process one batch at a time
    }

    Ok(())
}
```

#### Streaming Pattern for Large Datasets

```rust
use datafusion::prelude::*;
use futures::StreamExt;
use arrow::array::AsArray;

// Processes data using Arrow RecordBatch streaming
// See also: https://docs.rs/arrow/latest/arrow/record_batch/struct.RecordBatch.html
async fn process_large_dataset(df: DataFrame) -> datafusion::error::Result<()> {
    let mut stream = df.execute_stream().await?;
    let mut total_processed = 0;

    while let Some(batch_result) = stream.next().await {
        let batch = batch_result?;

        // Process batch incrementally
        let id_array = batch.column(0).as_primitive::<arrow::datatypes::Int32Type>();

        for value in id_array.iter() {
            if let Some(val) = value {
                // Process each value
                total_processed += 1;
            }
        }

        // Optional: report progress
        if total_processed % 10000 == 0 {
            println!("Processed {} rows so far...", total_processed);
        }
    }

    println!("Total rows processed: {}", total_processed);
    Ok(())
}
```

#### Batch Processing Pattern

```rust
// Process data in manageable chunks
async fn process_in_batches(
    df: DataFrame,
    batch_size: usize,
) -> datafusion::error::Result<Vec<i64>> {
    let total_rows = df.clone().count().await?;
    let mut results = Vec::new();

    for offset in (0..total_rows).step_by(batch_size) {
        let batch_df = df.clone()
            .limit(offset, Some(batch_size))?;

        let batch_data = batch_df.collect().await?;

        // Process this batch
        for record_batch in batch_data {
            // Process record_batch
            results.push(record_batch.num_rows() as i64);
        }
    }

    Ok(results)
}
```

_Quick Debug_: If you're running out of memory, check if you're calling `collect()` on large datasets. Replace with `execute_stream()` or add `limit()` before collecting.

> **See also:**
>
> - [`execute_stream()`] API documentation
> - [`recordbatch`] - Arrow RecordBatch format for understanding batch processing
> - [Best Practices](best-practices.md) for memory configuration

### Error Handling & Recovery

Build robust data pipelines with graceful error handling and recovery patterns.

#### Graceful Degradation

```rust
use datafusion::prelude::*;

async fn load_with_fallback() -> datafusion::error::Result<DataFrame> {
    // Try primary data source
    match SessionContext::new().read_csv("primary.csv", CsvReadOptions::default()).await {
        Ok(df) => {
            println!("Loaded primary source");
            Ok(df)
        }
        Err(_) => {
            println!("Primary source failed, using fallback");
            // Fallback to backup or synthetic data
            dataframe!(
                "id" => [1, 2, 3],
                "status" => ["fallback", "fallback", "fallback"]
            )
        }
    }
}
```

#### Soft Failures with try_cast

```rust
use datafusion::prelude::*;
use datafusion::functions::expr_fn::*;
use arrow::datatypes::DataType;

async fn safe_type_conversion(df: DataFrame) -> datafusion::error::Result<DataFrame> {
    // Use try_cast instead of cast for soft failures
    // Invalid values become NULL instead of causing errors
    let result = df.with_column(
        "price_numeric",
        try_cast(col("price_string"), DataType::Float64)
    )?;

    // Then handle NULLs gracefully
    let cleaned = result.with_column(
        "price_final",
        coalesce(vec![col("price_numeric"), lit(0.0)])
    )?;

    Ok(cleaned)
}
```

#### Transaction-Like Patterns

```rust
use datafusion::prelude::*;

async fn atomic_transformation(df: DataFrame) -> datafusion::error::Result<DataFrame> {
    // Build entire transformation pipeline
    let result = df
        .filter(col("amount").gt(lit(0)))?
        .with_column("processed", lit(true))?
        .aggregate(vec![col("category")], vec![sum(col("amount"))])?;

    // Validate before returning
    let count = result.clone().count().await?;

    if count == 0 {
        return Err(datafusion::error::DataFusionError::Execution(
            "Transformation resulted in empty dataset".to_string()
        ));
    }

    Ok(result)
}
```

#### Retry Pattern

```rust
use datafusion::prelude::*;
use std::time::Duration;
use tokio::time::sleep;

async fn with_retry<F, Fut>(
    operation: F,
    max_retries: u32,
) -> datafusion::error::Result<DataFrame>
where
    F: Fn() -> Fut,
    Fut: std::future::Future<Output = datafusion::error::Result<DataFrame>>,
{
    let mut attempts = 0;

    loop {
        match operation().await {
            Ok(df) => return Ok(df),
            Err(e) if attempts < max_retries => {
                attempts += 1;
                println!("Attempt {} failed, retrying... Error: {}", attempts, e);
                sleep(Duration::from_secs(2_u64.pow(attempts))).await;
            }
            Err(e) => return Err(e),
        }
    }
}

// Usage
let df = with_retry(
    || async {
        SessionContext::new()
            .read_csv("unstable_source.csv", CsvReadOptions::default())
            .await
    },
    3
).await?;
```

_Quick Debug_: Wrap transformation steps in separate functions that return `Result<DataFrame>`. This makes it easier to add try-catch logic and provides better error messages with context.

### Data Quality & Bias Inspection

Inspired by research on [Blue Elephants Inspecting Pandas], you can track data distribution changes throughout your pipeline to detect technical biases or data quality issues.

#### Tracking Tuple Identity with row_number()

```rust
use datafusion::prelude::*;
use datafusion::functions_window::expr_fn::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let df = dataframe!(
        "customer" => ["Alice", "Bob", "Carol", "Dave"],
        "age" => [25, 35, 45, 55],
        "income" => [50000, 75000, 60000, 90000]
    )?;

    // Add a stable tuple identifier to track rows through transformations
    let with_tid = df.window(vec![
        row_number()
            .order_by(vec![col("customer").sort(true, true)])
            .build()?
            .alias("tid")
    ])?;

    with_tid.show().await?;
    // +----------+-----+--------+-----+
    // | customer | age | income | tid |
    // +----------+-----+--------+-----+
    // | Alice    | 25  | 50000  | 1   |
    // | Bob      | 35  | 75000  | 2   |
    // | Carol    | 45  | 60000  | 3   |
    // | Dave     | 55  | 90000  | 4   |
    // +----------+-----+--------+-----+

    Ok(())
}
```

#### Computing Distribution Frequencies

```rust
use datafusion::prelude::*;
use datafusion::functions_aggregate::expr_fn::*;

/// Compute distribution of values in a sensitive column
async fn compute_distribution(
    df: DataFrame,
    sensitive_col: &str,
) -> datafusion::error::Result<DataFrame> {
    // Count occurrences per group
    let grouped = df.clone().aggregate(
        vec![col(sensitive_col)],
        vec![count(lit(1)).alias("count")]
    )?;

    // Total count for ratio calculation
    let total_count = df.clone().count().await? as f64;

    // Add ratio column
    let with_ratio = grouped.with_column(
        "ratio",
        col("count").cast_to(
            &arrow::datatypes::DataType::Float64,
            &grouped.schema()
        )? / lit(total_count)
    )?;

    with_ratio.sort(vec![col("ratio").sort(false, true)])?.show().await?;

    Ok(with_ratio)
}
```

#### Detecting Bias: Before and After Comparison

```rust
use datafusion::prelude::*;
use datafusion::functions_aggregate::expr_fn::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();

    // Original data with sensitive column
    let original = dataframe!(
        "tid" => [1, 2, 3, 4, 5, 6],
        "age_group" => ["young", "young", "middle", "middle", "senior", "senior"],
        "income" => [30000, 35000, 60000, 65000, 45000, 50000]
    )?;

    // Step 1: Register original for inspection
    ctx.register_table("step_0_original", original.clone().into_view())?;

    // Step 2: Apply transformation (e.g., filter high income)
    let filtered = original
        .clone()
        .filter(col("income").gt(lit(40000)))?;

    ctx.register_table("step_1_filtered", filtered.clone().into_view())?;

    // Step 3: Inspect distribution change using SQL
    let inspection = ctx.sql("
        SELECT
            s0.age_group,
            COUNT(s0.tid) as original_count,
            COUNT(s1.tid) as filtered_count,
            CAST(COUNT(s1.tid) AS DOUBLE) / CAST(COUNT(s0.tid) AS DOUBLE) as retention_ratio
        FROM step_0_original s0
        LEFT JOIN step_1_filtered s1 ON s0.tid = s1.tid
        GROUP BY s0.age_group
        ORDER BY retention_ratio
    ").await?;

    println!("Distribution change after filtering:");
    inspection.show().await?;
    // Shows which age groups were disproportionately filtered out

    Ok(())
}
```

**Output:**

```
+-----------+----------------+----------------+-----------------+
| age_group | original_count | filtered_count | retention_ratio |
+-----------+----------------+----------------+-----------------+
| young     | 2              | 0              | 0.0             | ← Bias detected!
| middle    | 2              | 2              | 1.0             |
| senior    | 2              | 2              | 1.0             |
+-----------+----------------+----------------+-----------------+
```

#### Reusable Distribution Tracking Function

```rust
use datafusion::prelude::*;
use datafusion::functions_aggregate::expr_fn::*;

/// Track distribution of a sensitive column through pipeline steps
async fn track_distribution(
    ctx: &SessionContext,
    step_name: &str,
    df: DataFrame,
    sensitive_col: &str,
) -> datafusion::error::Result<()> {
    // Register this step as a view
    ctx.register_table(step_name, df.clone().into_view())?;

    // Compute distribution
    let dist = df.aggregate(
        vec![col(sensitive_col)],
        vec![
            count(lit(1)).alias("count"),
        ],
    )?;

    println!("\nDistribution at {}:", step_name);
    dist.show().await?;

    Ok(())
}

// Usage in a pipeline
async fn audited_pipeline() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();

    let data = dataframe!(
        "gender" => ["F", "F", "M", "M", "M"],
        "score" => [85, 92, 78, 95, 88]
    )?;

    // Track at each step
    track_distribution(&ctx, "step_0_original", data.clone(), "gender").await?;

    let filtered = data.filter(col("score").gt(lit(90)))?;
    track_distribution(&ctx, "step_1_filtered", filtered.clone(), "gender").await?;
    // Check if filtering introduced gender imbalance

    Ok(())
}
```

**Key Insights from the Paper:**

1. **SQL CTEs mirror DataFrame steps**: Each DataFrame transformation can be registered as a view/CTE for SQL-based inspection
2. **Tuple identifiers enable lineage**: Adding a `tid` column lets you track individual rows through transformations
3. **Distribution changes reveal bias**: Comparing group counts before/after operations detects disproportionate filtering
4. **Hybrid approach is powerful**: Use DataFrames for pipeline construction, SQL for auditing and inspection

> **See also:**
>
> - [Blue Elephants Inspecting Pandas] - Research on ML pipeline inspection in SQL
> - [mlinspect] - Python framework for ML pipeline inspection
> - [Mixing SQL and DataFrames](#mixing-sql-and-dataframes) section

### Subqueries

Subqueries allow you to use the result of one query within another query. DataFusion supports scalar subqueries (returning a single value), IN subqueries (checking membership), and EXISTS subqueries (checking existence).

#### Scalar Subqueries

```rust
use datafusion::prelude::*;
use datafusion::logical_expr::expr::ScalarSubquery;
use datafusion::functions_aggregate::expr_fn::avg;
use std::sync::Arc;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();

    // Register tables
    let orders = dataframe!(
        "order_id" => [1, 2, 3, 4],
        "amount" => [100, 200, 150, 300]
    )?;
    ctx.register_table("orders", orders.clone().into_view())?;

    // Find orders above average
    let avg_subquery = ctx.table("orders").await?
        .aggregate(vec![], vec![avg(col("amount"))])?
        .select(vec![avg(col("amount"))])?
        .into_unoptimized_plan();

    let result = ctx.table("orders").await?
        .filter(col("amount").gt(scalar_subquery(Arc::new(avg_subquery))))?;

    result.show().await?;
    Ok(())
}
```

### IN Subqueries

```rust
use datafusion::prelude::*;
use std::sync::Arc;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();

    let customers = dataframe!(
        "id" => [1, 2, 3],
        "name" => ["Alice", "Bob", "Carol"]
    )?;

    let premium_customers = dataframe!(
        "customer_id" => [1, 3]
    )?;

    ctx.register_table("customers", customers.into_view())?;
    ctx.register_table("premium", premium_customers.into_view())?;

    // Find customers who are premium
    let premium_ids = ctx.table("premium").await?
        .select(vec![col("customer_id")])?
        .into_unoptimized_plan();

    let result = ctx.table("customers").await?
        .filter(in_subquery(col("id"), Arc::new(premium_ids)))?;

    result.show().await?;
    Ok(())
}
```

## Mixing SQL and DataFrames

One of DataFusion's strengths is the ability to seamlessly mix SQL and DataFrame APIs. You can start with SQL and refine with DataFrames, or build DataFrames and query them with SQL.

### SQL to DataFrame

Execute SQL to get initial results, then use DataFrame methods for additional transformations:

```rust
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();

    // Register a table
    ctx.sql("CREATE EXTERNAL TABLE users (
        id INT,
        name VARCHAR,
        age INT,
        city VARCHAR
    ) STORED AS CSV LOCATION 'users.csv'").await?.collect().await?;

    // Start with SQL, continue with DataFrame API
    let df = ctx.sql("SELECT * FROM users WHERE age > 21").await?
        .filter(col("city").eq(lit("NYC")))?
        .select(vec![col("name"), col("age")])?
        .sort(vec![col("age").sort(false, true)])?;

    df.show().await?;
    Ok(())
}
```

### DataFrame to SQL

Build a DataFrame, register it as a view, then query it with SQL:

```rust
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();

    // Create DataFrame programmatically
    let df = dataframe!(
        "product" => ["Laptop", "Mouse", "Keyboard", "Monitor"],
        "price" => [1200, 25, 75, 300],
        "quantity" => [5, 50, 30, 10]
    )?
    .filter(col("price").gt(lit(50)))?;

    // Register as a view for SQL access
    ctx.register_table("filtered_products", df.into_view())?;

    // Now query with SQL
    let result = ctx.sql("
        SELECT
            product,
            price * quantity as total_value
        FROM filtered_products
        ORDER BY total_value DESC
    ").await?;

    result.show().await?;
    Ok(())
}
```

### Combining Both Approaches

You can alternate between SQL and DataFrame operations as needed:

```rust
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();

    // Step 1: SQL for initial complex join
    let df1 = ctx.sql("
        SELECT o.order_id, o.customer_id, p.product_name, o.quantity
        FROM orders o
        JOIN products p ON o.product_id = p.id
    ").await?;

    // Step 2: DataFrame for programmatic filtering
    let df2 = df1.filter(col("quantity").gt(lit(10)))?;

    // Step 3: Register and use SQL for aggregation
    ctx.register_table("large_orders", df2.into_view())?;

    let final_result = ctx.sql("
        SELECT customer_id, COUNT(*) as order_count
        FROM large_orders
        GROUP BY customer_id
    ").await?;

    final_result.show().await?;
    Ok(())
}
```

### When to Use DataFrames vs SQL

**Use DataFrames when you need:**

- Dynamic query construction based on runtime conditions
- Type-safe, compile-time validated code
- Reusable transformation functions
- Programmatic iteration over columns or tables
- Complex conditional logic in your pipeline

**Use SQL when:**

- You have a fixed, well-defined query
- Team members are more comfortable with SQL
- The query is simple and declarative
- You're prototyping or exploring data interactively

**Mix both when:**

- You want the best of both worlds: SQL for complex joins/aggregations, DataFrames for dynamic processing
- Different parts of your pipeline suit different approaches

## The DataFrame Detective Toolkit

When things go wrong, you need systematic debugging techniques. This section teaches you methodological approaches to diagnose and fix DataFrame issues.

### Understanding Query Plans

DataFrames build a logical plan that gets optimized before execution. Understanding plans is key to debugging.

#### Viewing the Logical Plan

```rust
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let df = dataframe!(
        "product" => ["Laptop", "Mouse"],
        "price" => [1200, 25]
    )?
    .filter(col("price").gt(lit(50)))?
    .select(vec![col("product")])?;

    // View the logical plan (before optimization)
    println!("Logical Plan:\n{}", df.logical_plan().display_indent());

    Ok(())
}
```

**Output:**

```
Logical Plan:
Projection: product
  Filter: price > 50
    DataFrame: ...
```

#### Viewing the Optimized Plan

```rust
// See what optimizations were applied
let optimized_df = df.clone().explain(false, false)?;
optimized_df.show().await?;
```

This shows you:

- **Projection pushdown**: Reading only needed columns
- **Predicate pushdown**: Filtering as early as possible
- **Filter merging**: Combined multiple filters
- **Partition pruning**: Skipped irrelevant data

#### Analyzing Execution with EXPLAIN ANALYZE

```rust
// See actual execution statistics
let analyzed = df.explain(false, true)?;  // verbose=false, analyze=true
analyzed.show().await?;
```

This reveals:

- Actual row counts at each stage
- Execution time per operator
- Memory usage
- Partitioning information

_Use case_: Find bottlenecks in complex pipelines.

### Debugging Techniques

#### The "Break-the-Chain" Method

When a long pipeline fails, find the breaking point by progressively commenting out operations:

```rust
let result = df
    .filter(col("a").gt(lit(0)))?
    .with_column("b", col("a") * lit(2))?
    // .with_column("c", col("b") / col("zero"))?  // ← Comment this out
    .aggregate(vec![col("category")], vec![sum(col("b"))])?;

// If it works now, the commented line is the problem
```

Faster approach using `.show()` at each step:

```rust
let step1 = df.filter(col("a").gt(lit(0)))?;
step1.clone().show().await?;  // ← Verify step 1

let step2 = step1.with_column("b", col("a") * lit(2))?;
step2.clone().show().await?;  // ← Verify step 2

// Continue until you find the failing step
```

#### Binary Search Debugging

For very long pipelines (10+ operations), use binary search:

1. Comment out the second half
2. If it works, the problem is in the second half; if not, it's in the first half
3. Repeat within the problematic half
4. Converges quickly to the exact operation

#### Inspecting Intermediate Results

```rust
// Add .clone().show() to inspect without consuming the DataFrame
let df = original_df
    .filter(col("amount").gt(lit(0)))?;
df.clone().show().await?;  // ← Check what's here

let df = df.select(vec![col("customer"), col("amount")])?;
df.clone().show().await?;  // ← And here

let final_df = df.aggregate(vec![col("customer")], vec![sum(col("amount"))])?;
```

#### Schema Inspection

```rust
// Check the schema at any point
println!("Schema: {:?}", df.schema());
println!("Field names: {:?}", df.schema().field_names());

// Check a specific field's type
let field = df.schema().field_with_name("amount")?;
println!("Type of 'amount': {:?}", field.data_type());
```

#### Row Count Sanity Checks

```rust
// Quick row count at any stage
let count = df.clone().count().await?;
println!("Row count: {}", count);

// Expected vs actual
let expected = 100;
let actual = df.clone().count().await?;
assert_eq!(expected, actual, "Row count mismatch!");
```

### Common Debugging Scenarios

#### Scenario 1: "Why is my filter returning zero rows?"

**Diagnostic steps:**

1. Check for nulls: `df.filter(col("column").is_not_null())?.count().await?`
2. Inspect sample data: `df.limit(0, Some(5))?.show().await?`
3. Check data types: `df.schema()`
4. Verify filter logic with opposite condition: `col("x").lt_eq(lit(10))` instead of `.gt()`

#### Scenario 2: "My aggregation has wrong results"

**Diagnostic steps:**

1. Verify grouping keys: `df.select(vec![col("key")]).distinct()?.show().await?`
2. Check for nulls in group keys: `df.filter(col("key").is_null())?.count().await?`
3. Count rows per group: `df.aggregate(vec![col("key")], vec![count(lit(1))])?`
4. Inspect raw data before aggregation: `df.clone().show().await?`

#### Scenario 3: "Join returns unexpected row count"

**Diagnostic steps:**

1. Check for duplicates in keys: `left.select(vec![col("id")]).distinct().count()` vs `left.count()`
2. Inspect keys from both sides: `left.select(vec![col("id")]).show()` and `right.select(vec![col("id")]).show()`
3. Check for nulls in keys: `left.filter(col("id").is_null()).count()`
4. Verify key types match: `left.schema()` and `right.schema()`

#### Scenario 4: "Performance is terrible"

**Diagnostic steps:**

1. Check the optimized plan: `df.explain(false, false)?.show().await?`
2. Verify predicate pushdown happened (filter appears near data source)
3. Check partition count: Look for "Partitions" in explain output
4. Use `explain(false, true)?` to see actual execution times
5. Consider increasing parallelism: `SessionConfig::new().with_target_partitions(16)`

### Memory Management Debugging

#### Detecting Memory Issues

```rust
// DON'T: This loads all data into memory
// let all_data = huge_df.collect().await?;  // May OOM!

// DO: Stream the data
let mut stream = huge_df.execute_stream().await?;
use futures::StreamExt;
while let Some(batch) = stream.next().await {
    let batch = batch?;
    println!("Processing batch with {} rows", batch.num_rows());
    // Process batch by batch
}
```

#### Using show() vs collect()

```rust
// show() is for humans - limits rows and formats nicely
df.show().await?;  // Safe, only shows first 20 rows

// collect() is for programs - loads EVERYTHING
let batches = df.collect().await?;  // Dangerous with large data!

// Limit before collecting
let safe = df.limit(0, Some(1000))?.collect().await?;
```

## Anti-Pattern Gallery

Learn from common mistakes. Each anti-pattern shows what NOT to do and the correct approach.

### Anti-Pattern 1: Over-Selection Then Filter

```rust
// ❌ DON'T: Select all columns then filter (reads unnecessary data)
let bad = df
    .select(vec![col("*")])?
    .filter(col("price").gt(lit(100)))?;

// ✅ DO: Filter first, then select only what you need
let good = df
    .filter(col("price").gt(lit(100)))?
    .select(vec![col("product"), col("price")])?;
```

**Why**: Projection pushdown optimizes column reads, but only if you select early.

### Anti-Pattern 2: Multiple Sequential Filters

```rust
// ❌ DON'T: Chain filters (creates multiple plan nodes)
let bad = df
    .filter(col("price").gt(lit(50)))?
    .filter(col("quantity").lt(lit(100)))?
    .filter(col("category").eq(lit("Electronics")))?;

// ✅ DO: Combine into one filter
let good = df.filter(
    col("price").gt(lit(50))
        .and(col("quantity").lt(lit(100)))
        .and(col("category").eq(lit("Electronics")))
)?;
```

**Why**: Single filter is more efficient and easier to optimize.

### Anti-Pattern 3: Collecting Large Datasets

```rust
// ❌ DON'T: Collect millions of rows into memory
async fn process_big_data(df: DataFrame) -> Result<()> {
    let all_data = df.collect().await?;  // OOM risk!
    for batch in all_data {
        // process...
    }
    Ok(())
}

// ✅ DO: Stream the data
async fn process_big_data_streaming(df: DataFrame) -> Result<()> {
    let mut stream = df.execute_stream().await?;
    while let Some(batch) = stream.next().await {
        let batch = batch?;
        // Process batch by batch
    }
    Ok(())
}
```

**Why**: Streaming handles arbitrarily large datasets without memory issues.

### Anti-Pattern 4: Ignoring Null Handling

```rust
// ❌ DON'T: Assume no nulls
let bad = df.filter(col("amount").gt(lit(0)))?;
// Silently drops null amounts (maybe not intended!)

// ✅ DO: Be explicit about null handling
let good = df.filter(
    col("amount").is_not_null()
        .and(col("amount").gt(lit(0)))
)?;
```

**Why**: [Three-valued logic] (SQL standard) can surprise you. Be explicit about null handling.

### Anti-Pattern 5: Cartesian Products

```rust
// ❌ DON'T: Forget join keys (Cartesian product!)
let bad = left.join(right, JoinType::Inner, &[], &[], None)?;
// With 1000 rows each = 1,000,000 rows!

// ✅ DO: Always specify join conditions
let good = left.join(right, JoinType::Inner, &["id"], &["user_id"], None)?;
```

**Why**: Cartesian products explode quickly and are rarely intended.

### Anti-Pattern 6: Not Using Lazy Evaluation

```rust
// ❌ DON'T: Execute prematurely
async fn bad_pipeline(df: DataFrame) -> Result<DataFrame> {
    let step1 = df.filter(col("a").gt(lit(0)))?;
    step1.show().await?;  // ← Executes!

    let step2 = step1.select(vec![col("b")])?;
    step2.show().await?;  // ← Executes again!

    Ok(step2)
}

// ✅ DO: Build the entire plan, execute once
async fn good_pipeline(df: DataFrame) -> Result<DataFrame> {
    let result = df
        .filter(col("a").gt(lit(0)))?
        .select(vec![col("b")])?;
    // Only execute when you need results
    result.show().await?;  // Single execution
    Ok(result)
}
```

**Why**: DataFusion optimizes the entire query plan; premature execution prevents optimization.

## Troubleshooting Guide

Quick solutions to common error messages and problems.

### Error: "Schema mismatch"

**Common causes:**

- Union of DataFrames with different schemas
- Join on columns with different types

**Solutions:**

```rust
// Use union_by_name for flexibility
let result = df1.union_by_name(df2)?;

// Cast columns to match types
let df2_fixed = df2.with_column("id", cast(col("id"), DataType::Int64))?;
```

### Error: "Column 'X' not found"

**Common causes:**

- Typo in column name
- Column was dropped in previous transformation
- Trying to access column after aggregation

**Solutions:**

```rust
// Check available columns
println!("{:?}", df.schema().field_names());

// After aggregation, only group keys and aggregates exist
// Include needed columns in group by or as aggregates
```

### Error: "Type mismatch"

**Common causes:**

- Comparing/computing with incompatible types
- String column used in numeric operation

**Solutions:**

```rust
// Cast to correct type
let df = df.with_column("price", cast(col("price_str"), DataType::Float64))?;

// Or use try_cast to handle failures gracefully
let df = df.with_column("price", try_cast(col("price_str"), DataType::Float64))?;
```

### Problem: Filter returns zero rows unexpectedly

**Diagnostic:**

```rust
// Check for nulls
df.filter(col("column").is_null())?.count().await?;

// Inspect sample data
df.limit(0, Some(10))?.show().await?;
```

**Solution:**

```rust
// Handle nulls explicitly
df.filter(col("column").is_not_null().and(col("column").gt(lit(0))))?;
```

### Problem: Join returns empty result

**Diagnostic:**

```rust
// Check keys from both sides
left.select(vec![col("id")]).distinct()?.show().await?;
right.select(vec![col("user_id")]).distinct()?.show().await?;

// Check for type mismatches
println!("Left: {:?}", left.schema().field_with_name("id")?.data_type());
println!("Right: {:?}", right.schema().field_with_name("user_id")?.data_type());
```

**Solution:**

```rust
// Ensure types match and check for trailing spaces
let left_clean = left.with_column("id", trim(vec![col("id")]))?;
let right_clean = right.with_column("user_id", trim(vec![col("user_id")]))?;
```

### Problem: Out of memory

**Solutions:**

```rust
// Use streaming instead of collect()
let stream = df.execute_stream().await?;

// Or limit the data
let sample = df.limit(0, Some(10000))?;

// Increase partition size for better parallelism
let config = SessionConfig::new().with_target_partitions(16);
let ctx = SessionContext::with_config(config);
```

## Quick Reference Cheat Sheet

Common DataFrame operations at a glance:

| Task                 |                                   Code                                   | Notes                 |
| -------------------- | :----------------------------------------------------------------------: | --------------------- |
| **Create from data** |                    `dataframe!("col" => [1, 2, 3])?`                     | In-memory data        |
| **Read CSV**         |       `ctx.read_csv("file.csv", CsvReadOptions::default()).await?`       |                       |
| **Read Parquet**     | `ctx.read_parquet("file.parquet", ParquetReadOptions::default()).await?` |                       |
| **Select columns**   |                    `df.select_columns(&["a", "b"])?`                     | By name               |
| **Computed column**  |               `df.with_column("c", col("a") + col("b"))?`                | Add/replace           |
| **Rename column**    |                 `df.with_column_renamed("old", "new")?`                  |                       |
| **Filter rows**      |                    `df.filter(col("a").gt(lit(10)))?`                    | WHERE                 |
| **Aggregate**        |        `df.aggregate(vec![col("dept")], vec![sum(col("sal"))])?`         | GROUP BY              |
| **Join**             |        `df1.join(df2, JoinType::Inner, &["id"], &["id"], None)?`         |                       |
| **Sort**             |               `df.sort(vec![col("a").sort(false, true)])?`               | DESC, nulls last      |
| **Limit**            |                      `df.limit(skip, Some(fetch))?`                      | Pagination            |
| **Union**            |                            `df1.union(df2)?`                             | UNION ALL             |
| **Distinct**         |                             `df.distinct()?`                             | Remove duplicates     |
| **Show results**     |                            `df.show().await?`                            | Display (limited)     |
| **Collect**          |                          `df.collect().await?`                           | Load all (careful!)   |
| **Stream**           |                       `df.execute_stream().await?`                       | Process incrementally |
| **Explain plan**     |                `df.explain(false, false)?.show().await?`                 | Debug                 |
| **Count rows**       |                       `df.clone().count().await?`                        |                       |
| **Get schema**       |                              `df.schema()`                               | Column info           |

### Common Aggregates

```rust
use datafusion::functions_aggregate::expr_fn::*;

sum(col("amount"))
avg(col("amount"))
min(col("amount"))
max(col("amount"))
count(col("id"))
count_distinct(col("user_id"))
stddev(col("amount"))
variance(col("amount"))
```

### Common Expressions

```rust
// Comparison
col("a").eq(lit(5))
col("a").gt(lit(5))
col("a").lt_eq(lit(10))
col("a").between(lit(5), lit(10))

// Logic
col("a").and(col("b"))
col("a").or(col("b"))
col("a").not()

// Null handling
col("a").is_null()
col("a").is_not_null()

// String operations
col("name").like(lit("%Smith%"))
lower(col("name"))
upper(col("name"))
trim(vec![col("name")])

// Math
col("a") + col("b")
col("a") * lit(2)
col("price") * col("quantity")

// Conditional
when(col("amount").gt(lit(1000)), lit("High"))
    .when(col("amount").gt(lit(100)), lit("Medium"))
    .otherwise(lit("Low"))?
```

---

> **Prerequisites**:
>
> - [Creating DataFrames](creating-dataframes.md) - Know how to create a DataFrame
>
> **Next Steps**:
>
> - [Writing DataFrames](writing-dataframes.md) - Save your transformed data
> - [Best Practices](best-practices.md) - Optimize your queries
> - [Index](index.md) - Return to overview

## Further Reading

### DataFusion Documentation

- [DataFrame API Overview](index.md)
- [Creating DataFrames](creating-dataframes.md)
- [DataFrame Concepts](concepts.md)
- [Writing DataFrames](writing-dataframes.md)
- [Best Practices](best-practices.md)
- [SQL User Guide](../user-guide/sql/index.md)
- [Aggregate Functions](../user-guide/sql/aggregate_functions.md)
- [Window Functions](../user-guide/sql/window_functions.md)

### Examples

- [expr_api.rs] - Complex expression patterns
- [dataframe.rs](https://github.com/apache/datafusion/blob/main/datafusion-examples/examples/dataframe.rs) - Basic DataFrame operations
- [dataframe_transformations.rs](https://github.com/apache/datafusion/blob/main/datafusion-examples/examples/dataframe_transformations.rs) - Examples from this guide
- [All Examples](https://github.com/apache/datafusion/tree/main/datafusion-examples/examples)

### External Resources

- [Apache Arrow] - Underlying columnar format
- [Apache Spark DataFrames] - Similar API and concepts
- [PySpark DataFrame API] - Python DataFrame inspiration
- [PostgreSQL DISTINCT ON] - DISTINCT ON semantics
- [Lazy Evaluation] - Concept explanation
- [Three-valued logic] - SQL null handling

### Research & Advanced Topics

- [Blue Elephants Inspecting Pandas] - ML pipeline inspection and bias detection in SQL
- [mlinspect] - Framework for inspecting ML pipelines

### Concepts & Standards

- [SQL-99 Standard] - SQL language specification
- Projection pushdown, predicate pushdown - Query optimization techniques
- Lazy evaluation vs eager evaluation - Performance implications

```{include} references.md

```
