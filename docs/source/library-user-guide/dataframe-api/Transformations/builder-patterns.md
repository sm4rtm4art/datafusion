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

# Builder Methodology: Architecting with DataFrames

<!--TODO

1. ABSTRACT
2. INTRODUCTION
-->

```{contents} Table of Content
:local:
:depth: 2
```

## Introduction (placeholder)

**The DataFrame API isn't just SQL with different syntax—it's a programmatic _builder_ for query plans that integrates with Rust's type system, control flow, and tooling.**

When you write SQL, you write a _string_ that gets parsed, planned, and executed in one shot. When you use the DataFrame API, you're _constructing a query plan_ step by step, storing intermediate stages in Rust variables, branching the plan with `if/else`, and composing reusable transformations as functions. The plan doesn't execute until you explicitly ask for results.

This is the **[Builder Pattern][builder_pattern]**—a design pattern where you construct a complex object (the query plan) through a series of method calls, each returning a modified builder (a new DataFrame). The diagram below illustrates this two-phase architecture:

```text
                         THE BUILDER PATTERN
    ════════════════════════════════════════════════════════════

     LAZY PHASE (builds plan)              EAGER PHASE (runs)
    ┌────────────────────────────┐        ┌───────────────────┐
    │                            │        │                   │
    │  df ──► .filter() ──► step1│        │  .collect() ──► Data
    │          (new DF)   (new DF)        │  .show()    ──► Output
    │                        │   │        │  .count()   ──► Number
    │           ┌────────────┴───┼────────┼─────────────────────┐
    │           │                │        │                     │
    │           ▼                ▼        │                     │
    │     .aggregate()       .select()    │  SAME step1 feeds   │
    │       (new DF)          (new DF)    │  BOTH branches!     │
    │           │                │        │                     │
    │           ▼                ▼        │                     │
    │       summary          details ─────┼──► .show()          │
    │                                     │                     │
    └─────────────────────────────────────┴─────────────────────┘

    Key: Each method returns a NEW DataFrame (immutable).
         Use .clone() to branch: step1.clone().aggregate(...)
```

**What the diagram shows:**

- **Left side (Lazy Phase):** Each transformation method ([`.filter()`], [`.select()`], [`.aggregate()`]) returns a _new_ DataFrame containing an extended logical plan. No data moves yet—you're just building a blueprint.
- **Right side (Eager Phase):** Terminal actions ([`.collect()`], [`.show()`], [`.count()`]) trigger actual execution. Only then does DataFusion optimize the plan and process data.
- **Branching:** The variable `step1` can feed _multiple_ downstream paths. Unlike SQL CTEs (which exist only within a single query), Rust variables persist across your entire program scope.
- **Immutability:** The original `df` is unchanged after calling [`.filter()`]. Each method returns a fresh DataFrame, enabling safe parallel experimentation.

This architecture unlocks patterns impossible in SQL: dynamic query construction with Rust control flow, reusable transformation functions, and compile-time validation of your pipeline structure.

| Pattern                                                         | What It Enables                           | SQL Limitation                       |
| --------------------------------------------------------------- | ----------------------------------------- | ------------------------------------ |
| [Builder Pattern & Laziness](#the-builder-pattern-and-laziness) | Reuse intermediate plans as variables     | CTEs are query-scoped                |
| Dynamic Construction                                            | Rust `if/else` modifies the plan          | String concatenation, injection risk |
| [Encapsulation](#encapsulation-and-reusability)                 | Functions returning `Expr` or `DataFrame` | UDFs are hard to deploy/test         |
| [Memory & Streaming](#memory-management--streaming)             | Control collect vs stream execution       | No equivalent control                |
| [Error Handling](#error-handling-and-observability)             | Compile-time + runtime error separation   | All errors at runtime                |

**Running Example:** Throughout this section, we'll use a single `sales` DataFrame to demonstrate all patterns. This reduces cognitive load and shows how each technique applies to the same data:

```rust
use datafusion::prelude::*;
use datafusion::assert_batches_eq;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let sales = dataframe!(
        "order_id" => [1, 2, 3, 4, 5],
        "product" => ["Laptop", "Mouse", "Keyboard", "Monitor", "Laptop"],
        "category" => ["Electronics", "Accessories", "Accessories", "Electronics", "Electronics"],
        "price" => [1200, 25, 75, 350, 1100],
        "quantity" => [1, 5, 2, 1, 2],
        "customer" => ["Alice", "Bob", "Alice", "Carol", "Bob"]
    )?;

    let batches = sales.clone().collect().await?;
    assert_batches_eq!(
        [
            "+----------+----------+-------------+-------+----------+----------+",
            "| order_id | product  | category    | price | quantity | customer |",
            "+----------+----------+-------------+-------+----------+----------+",
            "| 1        | Laptop   | Electronics | 1200  | 1        | Alice    |",
            "| 2        | Mouse    | Accessories | 25    | 5        | Bob      |",
            "| 3        | Keyboard | Accessories | 75    | 2        | Alice    |",
            "| 4        | Monitor  | Electronics | 350   | 1        | Carol    |",
            "| 5        | Laptop   | Electronics | 1100  | 2        | Bob      |",
            "+----------+----------+-------------+-------+----------+----------+",
        ],
        &batches
    );

    Ok(())
}
```

### The Builder Pattern and Laziness

**Every DataFrame method returns a new DataFrame wrapping an extended [`LogicalPlan`]—no data moves until you call an action like [`.collect()`] or [`.show()`].**

Each transformation method (`.filter()`, `.select()`, `.sort()`) returns a _new_ DataFrame containing a logical plan—a description of _what_ to compute, not the computed result. The original DataFrame is immutable - it remains unchanged.

This lazy evaluation enables DataFusion's optimizer to see the entire pipeline before execution. It can push filters down, eliminate unused columns, and choose optimal join strategies—optimizations that would be impossible if execution happened at each step.

> **Projection Pushdown:**<br>
> When you call [`.select()`] to choose specific columns, DataFusion pushes this information down to the data source. For columnar formats like Parquet, this means only the bytes for selected columns are read from disk. This is a major performance advantage of Lazy Evaluation compared to eager systems (like Pandas) which often read the entire file into memory before filtering columns.

You chain method calls, storing intermediate DataFrames in Rust variables. Only when you call a terminal action ([`.collect()`], [`.show()`], [`.count()`]) does DataFusion optimize and execute the plan.

> **Fluent Interface:** <br>
> This chaining style is known as a [Fluent Interface][fluent_interface]—a design pattern where methods return `self` (or a modified copy) to enable readable chains. If you know Spark's DataFrame API, DataFusion's architecture is conceptually similar to [Spark's Catalyst Optimizer][catalyst_optimizer], but implemented in Rust.

> **See also:** [Concepts § Execution Model](concepts.md#execution-model-actions-vs-transformations) for a deeper dive into how DataFusion builds and optimizes logical plans.

**In rust code:**

```rust
use datafusion::prelude::*;
use datafusion::assert_batches_eq;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let sales = dataframe!(
        "order_id" => [1, 2, 3, 4, 5],
        "product" => ["Laptop", "Mouse", "Keyboard", "Monitor", "Laptop"],
        "category" => ["Electronics", "Accessories", "Accessories", "Electronics", "Electronics"],
        "price" => [1200, 25, 75, 350, 1100],
        "quantity" => [1, 5, 2, 1, 2],
        "customer" => ["Alice", "Bob", "Alice", "Carol", "Bob"]
    )?;

    // Each step builds a plan, doesn't execute
    let step1 = sales.clone().filter(col("price").gt(lit(100)))?;   // Plan: Filter
    let step2 = step1.select(vec![col("product"), col("price")])?;  // Plan: Filter → Project
    let step3 = step2.sort(vec![col("price").sort(false, true)])?;  // Plan: Filter → Project → Sort

    // Inspect the plan without executing
    println!("{}", step3.logical_plan().display_indent());

    // Only NOW does execution happen
    let batches = step3.collect().await?;
    assert_batches_eq!(
        [
            "+---------+-------+",
            "| product | price |",
            "+---------+-------+",
            "| Laptop  | 1200  |",
            "| Laptop  | 1100  |",
            "| Monitor | 350   |",
            "+---------+-------+",
        ],
        &batches
    );

    Ok(())
}
```

**Variables vs CTEs: A Mental Model Shift**

If you're coming from SQL, you might think of intermediate results like CTEs (`WITH step1 AS (...)`). DataFrames work differently: each step lives in a Rust variable that persists across your entire program scope—not just within a single query. This table highlights the key differences:

| Aspect               | DataFrame (Rust)                               | SQL (CTEs)              |
| -------------------- | ---------------------------------------------- | ----------------------- |
| Intermediate storage | Rust variables                                 | `WITH step1 AS (...)`   |
| Reuse across queries | Variable lives in scope                        | CTE is query-scoped     |
| Debugging            | [`.schema()`], [`.explain()`] at any point     | Must execute to inspect |
| Branching            | `step1.filter(...)` and `step1.aggregate(...)` | Duplicate the CTE       |

The practical benefit: you can inspect, branch, or reuse any intermediate DataFrame without re-executing the pipeline.

**Footgun:** DataFrame is _consumed_ by transformations. To reuse, call [`.clone()`]:

```rust
use datafusion::prelude::*;
use datafusion::functions_aggregate::expr_fn::sum;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let sales = dataframe!("category" => ["A"], "price" => [100])?;
    let filtered = sales.clone().filter(col("price").gt(lit(100)))?;  // sales still usable
    let aggregated = sales.aggregate(vec![col("category")], vec![sum(col("price"))])?;  // sales consumed
    Ok(())
}
```

(dynamic-pipeline-construction)=

### Dynamic Pipeline Construction

**Use Rust control flow (`if/else`, `match`, loops) to build query plans dynamically—something SQL strings make dangerous and error-prone.**

With SQL, dynamic queries often lead to string concatenation—a pattern prone to injection attacks and syntax errors. The DataFrame API eliminates both risks: values pass through [`lit()`] which properly escapes and types them, and Rust's type system ensures column references are valid at build time.

**Control Flow vs String Concatenation**

```rust
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let sales = dataframe!("category" => ["Electronics"], "price" => [100])?;
    let filter_category: Option<&str> = Some("Electronics");

    // ❌ SQL: String concatenation (injection risk, runtime errors)
    let mut query = "SELECT * FROM sales WHERE 1=1".to_string();
    if let Some(cat) = filter_category {
        query.push_str(&format!(" AND category = '{}'", cat));  // 💀 Injection!
    }

    // ✅ DataFrame: Type-safe, validated at build time
    let mut result = sales.clone();
    if let Some(cat) = filter_category {
        result = result.filter(col("category").eq(lit(cat)))?;  // Safe: lit() handles escaping
    }

    result.show().await?;
    Ok(())
}
```

Rust's ownership model adds another layer: the `?` operator ensures errors propagate correctly, and the compiler verifies that `result` is properly reassigned in each branch.

#### Schema-Driven Transformations

Sometimes you don't know the column names or types until runtime—perhaps you're building a generic data processing library, or working with user-uploaded files. The DataFrame API lets you introspect the schema and build transformations dynamically.

The pattern: call [`.schema()`] to get the DataFrame's structure, iterate over fields, and construct expressions based on each column's name and type. This is impossible with static SQL where the query text is fixed at write time.

Using our `sales` DataFrame, let's double all numeric columns while keeping string columns unchanged.

```rust
use datafusion::prelude::*;
use datafusion::assert_batches_eq;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let sales = dataframe!(
        "order_id" => [1, 2, 3, 4, 5],
        "product" => ["Laptop", "Mouse", "Keyboard", "Monitor", "Laptop"],
        "category" => ["Electronics", "Accessories", "Accessories", "Electronics", "Electronics"],
        "price" => [1200, 25, 75, 350, 1100],
        "quantity" => [1, 5, 2, 1, 2],
        "customer" => ["Alice", "Bob", "Alice", "Carol", "Bob"]
    )?;

    // Introspect schema at runtime
    let schema = sales.schema();

    // Build expressions dynamically based on column types
    // Note: We match multiple numeric types to handle real-world data
    let transformed_cols: Vec<_> = schema
        .fields()
        .iter()
        .map(|f| {
            if f.data_type().is_numeric() {
                // Double numeric columns (Int32, Int64, Float32, Float64, etc.)
                (col(f.name()) * lit(2)).alias(format!("{}_doubled", f.name()))
            } else {
                // Keep non-numeric columns as-is
                col(f.name())
            }
        })
        .collect();

    let doubled = sales.clone().select(transformed_cols)?;
    doubled.show().await?;

    Ok(())
}
```

Notice how `order_id`, `price`, and `quantity` (all `Int64`) were doubled and renamed, while `product`, `category`, and `customer` (strings) passed through unchanged.

> **Note:** <br>
> Arrow's [`DataType`] provides the helper method [`.is_numeric()`] which returns `true` for `Int8`, `Int16`, `Int32`, `Int64`, `UInt*`, `Float32`, `Float64`, and `Decimal` types—saving you from writing verbose match statements:

See more information at the [Schema Management](schema-management.md) section.

### Encapsulation and Reusability

**Move complex transformation logic into reusable Rust functions—no UDF registration, no deployment headaches, full unit-testability.**

SQL UDFs require registration with the execution context and have limited composability. Rust functions are native citizens: they compose naturally, benefit from IDE tooling, and can be unit-tested in isolation.

DataFusion supports encapsulation at **two levels**:

| Level               | Returns             | Use Case                        | Example                                                      |
| ------------------- | ------------------- | ------------------------------- | ------------------------------------------------------------ |
| **Column-level**    | `Expr`              | Reusable column transformations | `clean_currency("amount")` → use in `.select()`, `.filter()` |
| **DataFrame-level** | `Result<DataFrame>` | Multi-step pipeline stages      | `summarize_sales(df)` → filtering, aggregating, joining      |

Both approaches let you build a library of tested, composable transformations that work across any DataFrame with compatible schemas.

**Native Functions vs SQL UDFs**

| Aspect       | Rust Functions        | SQL UDFs                       |
| ------------ | --------------------- | ------------------------------ |
| Registration | None needed           | `ctx.register_udf(...)`        |
| Testing      | Standard `#[test]`    | Requires execution context     |
| IDE support  | Full autocomplete     | None                           |
| Composition  | Direct function calls | Limited nesting                |
| Distribution | Compiled into binary  | Must be registered per context |

> **Need actual UDFs?** <br>
> For custom scalar functions (UDFs) or aggregate functions (UDAFs) that must be registered with the context, see [Adding User Defined Functions](../../library-user-guide/functions/adding-udfs.md).

#### Functions Returning [`Expr`] (Column-Level)

When you find yourself writing the same column expression repeatedly—parsing dates, cleaning strings, computing derived values—extract it into a function that returns an [`Expr`]. This keeps your pipeline code clean and makes the logic testable in isolation.

The pattern: write a Rust function that takes column names (or other parameters) and returns an [`Expr`]. You can then use this expression anywhere DataFusion expects one: in [`.select()`], [`.with_column()`], [`.filter()`], etc.

```rust
use datafusion::prelude::*;

/// Calculate profit margin as a percentage
fn profit_margin(revenue_col: &str, cost_col: &str) -> Expr {
    ((col(revenue_col) - col(cost_col)) / col(revenue_col) * lit(100))
        .alias("profit_margin_pct")
}

/// Price category based on value
fn price_category(price_col: &str) -> datafusion::error::Result<Expr> {
    Ok(case(col(price_col).gt(lit(100)))
        .when(lit(true), lit("expensive"))
        .otherwise(lit("affordable"))?
        .alias("category"))
}

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let df = dataframe!(
        "product" => ["Laptop", "Mouse"],
        "revenue" => [150.0, 250.0],
        "cost" => [100.0, 150.0],
        "price" => [1200, 25]
    )?;

    // Usage: reusable across any DataFrame
    let df = df
        .with_column("margin", profit_margin("revenue", "cost"))?
        .with_column("price_tier", price_category("price")?)?;

    df.show().await?;
    Ok(())
}
```

These functions compose naturally—you can nest them, combine them with other expressions, or use them in aggregations.

#### Functions Returning `DataFrame` (Table-Level)

While [`Expr`] functions transform individual columns, sometimes you need to encapsulate an entire multi-step pipeline—filtering, joining, aggregating—into a reusable unit. Functions that take a [`DataFrame`] and return a [`Result<DataFrame>`] let you build composable pipeline stages.

This pattern shines when you have standard transformations applied across different datasets: data cleaning pipelines, report generators, or feature engineering steps for ML.

Using our `sales` DataFrame:

```rust
use datafusion::prelude::*;
use datafusion::functions_aggregate::expr_fn::*;
use datafusion::assert_batches_sorted_eq;

/// Calculate order totals and summarize by category
fn summarize_sales(df: DataFrame) -> datafusion::error::Result<DataFrame> {
    df.with_column("total", col("price") * col("quantity"))?
      .aggregate(
          vec![col("category")],
          vec![
              sum(col("total")).alias("revenue"),
              count(lit(1)).alias("order_count"),
          ]
      )
}

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let sales = dataframe!(
        "order_id" => [1, 2, 3, 4, 5],
        "product" => ["Laptop", "Mouse", "Keyboard", "Monitor", "Laptop"],
        "category" => ["Electronics", "Accessories", "Accessories", "Electronics", "Electronics"],
        "price" => [1200, 25, 75, 350, 1100],
        "quantity" => [1, 5, 2, 1, 2],
        "customer" => ["Alice", "Bob", "Alice", "Carol", "Bob"]
    )?;

    // Usage: apply the same summarization to any sales-like DataFrame
    let summary = summarize_sales(sales.clone())?;
    let batches = summary.collect().await?;
    assert_batches_sorted_eq!(
        [
            "+-------------+---------+-------------+",
            "| category    | revenue | order_count |",
            "+-------------+---------+-------------+",
            "| Accessories | 275     | 2           |",
            "| Electronics | 3750    | 3           |",
            "+-------------+---------+-------------+",
        ],
        &batches
    );

    Ok(())
}
```

You can chain these functions together to build complex pipelines from simple, tested building blocks.

#### Conditional Query Building

Real-world applications rarely have fixed queries. Users filter by different criteria, APIs accept optional parameters, and reports need configurable groupings. The DataFrame API lets you build queries conditionally using standard Rust control flow—`if let`, `match`, loops—without the SQL string concatenation anti-pattern.

This is where the builder pattern truly shines: <br>
Each transformation returns a new DataFrame, so you can conditionally apply steps based on runtime parameters while keeping the code readable and type-safe.

```rust
use datafusion::prelude::*;
use datafusion::assert_batches_eq;

/// Build a sales query with optional filters and configurable sorting
fn build_sales_query(
    df: DataFrame,
    min_price: Option<i32>,
    category: Option<String>,
    sort_desc: bool,
) -> datafusion::error::Result<DataFrame> {
    let mut result = df;

    // Apply filters only if parameters are provided
    if let Some(price) = min_price {
        result = result.filter(col("price").gt(lit(price)))?;
    }
    if let Some(cat) = category {
        result = result.filter(col("category").eq(lit(cat)))?;
    }

    // sort(ascending, nulls_first)
    // If sort_desc is true, ascending must be false (!sort_desc)
    result = result.sort(vec![col("price").sort(!sort_desc, true)])?;

    Ok(result)
}

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let sales = dataframe!(
        "order_id" => [1, 2, 3, 4, 5],
        "product" => ["Laptop", "Mouse", "Keyboard", "Monitor", "Laptop"],
        "category" => ["Electronics", "Accessories", "Accessories", "Electronics", "Electronics"],
        "price" => [1200, 25, 75, 350, 1100],
        "quantity" => [1, 5, 2, 1, 2],
        "customer" => ["Alice", "Bob", "Alice", "Carol", "Bob"]
    )?;

    // Example: high-value electronics, sorted by price descending
    let query = build_sales_query(
        sales.clone(),
        Some(100),                      // min_price: only items > $100
        Some("Electronics".into()),     // category: only Electronics
        true                            // sort_desc: highest price first
    )?;

    let batches = query.collect().await?;
    assert_batches_eq!(
        [
            "+----------+---------+-------------+-------+----------+----------+",
            "| order_id | product | category    | price | quantity | customer |",
            "+----------+---------+-------------+-------+----------+----------+",
            "| 1        | Laptop  | Electronics | 1200  | 1        | Alice    |",
            "| 5        | Laptop  | Electronics | 1100  | 2        | Bob      |",
            "| 4        | Monitor | Electronics | 350   | 1        | Carol    |",
            "+----------+---------+-------------+-------+----------+----------+",
        ],
        &batches
    );

    Ok(())
}
```

Compare this to SQL where you'd either write multiple query variants or resort to string concatenation—both error-prone and hard to test. Here, the logic is explicit, the types are checked, and you can unit-test `build_sales_query` with different parameter combinations.
