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

<!--TODO (reframe as DataFrame API composition capstone, 2026-07-18)
1. Keep this as a standalone page, but define its role precisely: explain how Rust code constructs, branches, parameterizes, reuses, and organizes lazy DataFrame plans.
2. Consider the title: # Composing DataFrame Pipelines
3. Build the storyline around: name intermediate stages → retain or branch plans → construct plans conditionally → extract reusable Expr builders → extract reusable DataFrame stages → define action and error boundaries
4. Treat this as an API-composition page, not another transformation-method catalogue. Link to selection, filtering, sorting, aggregation, and other method owners instead of reteaching their contracts.
5. Reassess inherited schema-driven projection content. Keep it only when it demonstrates a broader dynamic-pipeline technique; otherwise route to selection.md.
6. Verify and explain ownership semantics carefully: - transformations consume DataFrame values, - clone() preserves another plan handle, - cloning a DataFrame does not execute or materialize the data.
7. Compare Rust variables and functions with SQL statements, CTEs, parameters, and views fairly. Do not imply that SQL cannot compose or parameterize logic.
8. Standardize all examples on the documentation Rust rules: self-contained doctests, asserted output, no .show() as proof, and no unverified compile-time schema-safety claims.
9. End with a direct transition to data-quality.md: reusable plan and expression functions become especially useful when transformations enforce application-level validation rules.
-->

# Composing DataFrame Pipelines

**Compose maintainable, reusable, and dynamic DataFrame pipelines with Rust variables, control flow, functions, and schema inspection.**

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

## The Builder Model in Brief

**Each transformation returns another lazy `DataFrame`, so Rust code can name, pass, and compose plans before an action executes them.**

SQL text is parsed into the same kind of plan and, like the DataFrame API, remains lazy until an action. The DataFrame API constructs that plan programmatically: variables can hold intermediate plans, while each transformation consumes its `DataFrame` value. Use `.clone()` when another branch must retain access to the same underlying plan; execution begins at an action.

For the full [parser-versus-builder model](../Concepts/builder-parser.md#dataframe-api-the-builder-architecture), see the concepts page. For laziness, action boundaries, and ownership details, see the [execution lifecycle](../Concepts/execution-lifecycle.md#dataframe-method-categories) and [why `.clone()` appears in pipelines](../Concepts/execution-lifecycle.md#ownership-vs-execution-why-you-see-clone-everywhere).

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

---

## Naming and Retaining Intermediate Stages

**Name intermediate stages when inspection, reuse, or scope makes a single fluent chain harder to maintain.**

The following pipeline keeps each stage available for plan inspection before the action runs:

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

---

## Branching Pipelines and Ownership

**Move a `DataFrame` into a transformation when only one downstream pipeline needs it; call `.clone()` when multiple branches must retain the same stage.**

`.clone()` duplicates the plan description, not the underlying dataset; data is materialized only at an action. See the [ownership details](../Concepts/execution-lifecycle.md#ownership-vs-execution-why-you-see-clone-everywhere) for the complete model.

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

---

## Conditional and Dynamic Pipeline Construction

**Use Rust control flow to add transformations only when runtime parameters require them.**

Dynamic DataFrame construction avoids SQL string concatenation and its injection risk; see [where the DataFrame API helps with safety](../Concepts/builder-parser.md#safety-and-security-where-the-dataframe-api-shines) for the full model. Column resolution, schema checks, and incompatible expressions can surface at plan-build time through `Result`.

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

---

## Schema-Driven Pipeline Construction

**Inspect a runtime schema to decide which expressions to compose into a pipeline.**

Sometimes a pipeline does not know column names or types until runtime. The pattern below inspects the schema, chooses an expression for each field, and then builds the projection. For method coverage, see [dynamic column selection](selection.md#advanced-dynamic-column-selection).

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

Arrow's [`DataType`] provides [`.is_numeric()`], which returns `true` for numeric Arrow types and avoids a verbose match statement.

---

## Reusable Transformation Functions

**Extract repeated expressions and multi-step transformations into Rust functions that compose while plans are built.**

A Rust function returning `Expr` or `Result<DataFrame>` is convenient and testable, but it is not interchangeable with a registered SQL UDF or UDAF. Registered functions participate in SQL and serialized or distributed plans; see [Adding User Defined Functions](../../functions/adding-udfs.md).

DataFusion supports encapsulation at **two levels**:

| Level               | Returns             | Use Case                        | Example                                                      |
| ------------------- | ------------------- | ------------------------------- | ------------------------------------------------------------ |
| **Column-level**    | `Expr`              | Reusable column transformations | `clean_currency("amount")` → use in `.select()`, `.filter()` |
| **DataFrame-level** | `Result<DataFrame>` | Multi-step pipeline stages      | `summarize_sales(df)` → filtering, aggregating, joining      |

Both approaches let you build a library of tested, composable transformations that work across any DataFrame with compatible schemas.

### Functions Returning `Expr`

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

### Functions Returning `Result<DataFrame>`

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

---

## Organizing Errors and Pipeline Boundaries

**Propagate plan-construction errors through each reusable pipeline boundary so callers can decide how to recover, add context, or fail.**

Transformation methods return `Result<DataFrame>`, and reusable pipeline functions should normally do the same. Use `?` to propagate plan-build-time errors such as column resolution, schema checks, or incompatible expressions. Rust method signatures, argument types, and ownership are checked at compile time; data-source, resource, and physical failures occur at execution time.

For data-quality validation, see [data-quality validation](data-quality.md). For execution and runtime failures, see the [execution lifecycle](../Concepts/execution-lifecycle.md).

---

## Conclusion

Compose pipelines by naming stages that need reuse, moving or cloning plans deliberately, applying control flow to plan construction, and extracting repeated logic into functions.

- [Parser-versus-builder](../Concepts/builder-parser.md)
- [Laziness, actions, `.collect()`, and ownership](../Concepts/execution-lifecycle.md)
- [Streaming result APIs and unbounded-output memory](../Writing-DataFrames/streaming-execution.md#streaming-execution)
- [Dynamic column selection and enrichment](selection.md)
- [Switching between SQL and DataFrames, including SQL-expression bridges](hybrid-sql.md)
- [Data-quality validation](data-quality.md)
- [Registered UDFs and UDAFs](../../functions/adding-udfs.md)

[`.select()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.select
[`.filter()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.filter
[`.with_column()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.with_column
[`.schema()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.schema
[`.explain()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.explain
[`expr`]: https://docs.rs/datafusion/latest/datafusion/logical_expr/enum.Expr.html
[`dataframe`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html
[`result<dataframe>`]: https://docs.rs/datafusion/latest/datafusion/error/type.Result.html
[`datatype`]: https://docs.rs/arrow/latest/arrow/datatypes/enum.DataType.html
[`.is_numeric()`]: https://docs.rs/arrow/latest/arrow/datatypes/enum.DataType.html#method.is_numeric
