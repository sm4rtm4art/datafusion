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

<!--TODO (reframe as applied DataFrame pipeline capstone, 2026-07-18)
1. Reframe this page as an applied use of the transformation methods and the Rust composition model taught in builder-patterns.md.
2. Preserve the three useful validation strategies, subject to detailed review: - filter invalid rows, - retain rows and add validation flags, - aggregate quality metrics.
3. Separate lazy plan construction from actions clearly. Functions that call count(), collect(), show(), or other actions are execution workflows, not purely lazy transformation builders.
4. Replace advocacy such as "the DataFrame API excels" or "production-grade tooling, not check and hope" with a fair comparison: both SQL and DataFrame APIs can express relational validation checks; the DataFrame API is especially useful when rules must integrate with Rust functions, typed configuration, control flow, reuse, application errors, logging, or metrics.
5. Do not claim that SQL cannot parameterize thresholds, compose reusable logic, or integrate with applications. Compare concrete authoring and integration trade-offs instead.
6. Keep detailed schema validation in Schema Management. This page should recap and link to that owner, then focus on value constraints and quality reports.
7. Reassess whether .describe() belongs here as a bounded inspection tool or remains owned elsewhere.
8. Review error modeling. A failed business-quality threshold is not necessarily a DataFusion engine Execution error; show or discuss an application error boundary where appropriate.
9. Replace .show() examples and comment-only expected results with collected and asserted doctest output.
10. Verify empty-input behavior, null semantics, numeric casts, percentage calculations, and repeated-action costs during subtree implementation.
11. Write the abstract and conclusion last. The closing section should explain that this is one applied pattern enabled by composable DataFrame pipelines, not a claim that the DataFrame API is a complete data-quality framework. -->

# Data Validation & Quality

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

**Validation protects your pipeline at multiple levels: schema validation ensures structure, constraint validation ensures values, and quality inspection tracks how transformations affect your data.**

**Why the DataFrame-API excels here:** <br>
Validation is where DataFusion's DataFrame-API truly shines over the SQL-API. With the SQL-API, validation logic lives in query strings—you can't easily parameterize thresholds, compose rules as functions, or integrate with Rust's type system. The DataFrame-API lets you build validation as **reusable Rust functions** with configurable thresholds, return [`Result<DataFrame>`][`result<dataframe>`] to fail fast with meaningful errors, and connect validation failures directly to your logging, metrics, and alerting infrastructure. Both APIs produce the same optimized plans—but the DataFrame-API gives you production-grade data quality tooling, not just "check and hope."

## Validation Layers

This section covers three complementary approaches:

| Approach                  | Focus                                  | When to Use                                     |
| ------------------------- | -------------------------------------- | ----------------------------------------------- |
| **Schema Validation**     | Structure (fields, types, nullability) | First check—ensure DataFrame has expected shape |
| **Constraint Validation** | Business rules (price > 0, not null)   | Every pipeline—reject/flag bad data             |
| **Quality Inspection**    | Distribution tracking, bias detection  | ML pipelines, auditing, compliance              |

> **Schema validation** is covered in detail in [Schema Management § Comparing and Validating Schemas](../Schema-Management/schema-inspection.md#comparing-and-validating-schemas). This section focuses on constraint validation and quality inspection.

## Constraint Validation

Once schema validation confirms your DataFrame has the right structure, constraint validation ensures **values** meet business rules: no negative prices, required fields populated, values within expected ranges.

DataFusion's DataFrame-API provides **composable validation primitives**—filter, flag, and aggregate patterns—that integrate directly with your data pipeline. You get Rust's type safety, meaningful error messages via `Result<DataFrame>`, and validation logic that lives alongside your transformations rather than in a separate configuration layer.

> **Coming from other ecosystems?** <br>
> If you've used [Pandera] (Python), [Great Expectations], or [Deequ] (Spark), the patterns here serve a similar purpose: ensuring data meets business rules before processing. The DataFrame-API approach trades declarative schemas for programmatic flexibility—your validation rules are Rust functions you can test, version, and compose.
>
> For declarative validation built on DataFusion, projects such as Term aim to bring schema-based, declarative validation to the Rust/Arrow ecosystem.

**Three constraint validation strategies:**

| Strategy            | Behavior                 | Use When                                     |
| ------------------- | ------------------------ | -------------------------------------------- |
| **Filter-based**    | Removes invalid rows     | Data must be clean for downstream processing |
| **Flag-based**      | Marks rows, keeps all    | Need to report issues but preserve data      |
| **Aggregate-based** | Produces quality summary | Monitoring data health, CI/CD checks         |

### Filter-Based Validation

Use this when downstream processing requires clean data. Invalid rows are removed before they can cause calculation errors or corrupt aggregations.

**Trade-off:** <br>
Simple and fast, but you lose visibility into what was rejected. Consider logging reject counts.

> **Execution note:** `validate_sales` is `async` because it runs two `.count()` actions for reporting. The `.filter()` calls build a lazy plan, but those counts execute work before the function returns; the returned `DataFrame` is still lazy.

```rust
use datafusion::prelude::*;

/// Validate and clean sales data, returning only valid rows
async fn validate_sales(sales: DataFrame) -> datafusion::error::Result<DataFrame> {
    // Count rows before validation (for logging)
    let before_count = sales.clone().count().await?;

    // Define validation rules as filters
    // Each .filter() call is ANDed together—row must pass ALL rules
    let validated = sales
        .filter(col("price").gt(lit(0)))?           // Rule 1: price > 0
        .filter(col("quantity").gt(lit(0)))?        // Rule 2: quantity > 0
        .filter(col("customer").is_not_null())?;    // Rule 3: customer required

    // Count rows after validation
    let after_count = validated.clone().count().await?;
    let rejected = before_count - after_count;

    if rejected > 0 {
        eprintln!("Warning: Rejected {} invalid rows out of {}", rejected, before_count);
    }

    Ok(validated)
}

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let sales = dataframe!(
        "order_id" => [1, 99],
        "product" => ["Laptop", "Unknown"],
        "category" => [Some("Electronics"), None],
        "price" => [1200, -50],
        "quantity" => [1, 0],
        "customer" => [Some("Alice"), None]
    )?;

    let validated = validate_sales(sales).await?;
    validated.show().await?;
    // Only valid rows remain

    Ok(())
}
```

### Flag-Based Validation

Use this when you need to preserve all data but identify problems. Downstream processes can filter on `is_valid` or handle invalid rows differently.

**Trade-off:** <br>
Keeps all data for analysis, but requires downstream handling of invalid rows.

```rust
use datafusion::prelude::*;

/// Add validation flags and optionally fail if too many invalid rows
async fn validate_with_flags(
    sales: DataFrame,
    max_invalid_percent: f64,  // e.g., 0.1 = fail if >10% invalid
) -> datafusion::error::Result<DataFrame> {
    // Add a boolean column indicating row validity
    // All rules combined with AND—row is valid only if ALL pass
    let with_flags = sales
        .with_column("is_valid",
            col("price").gt(lit(0))
                .and(col("quantity").gt(lit(0)))
                .and(col("customer").is_not_null())
        )?;

    // Check invalid percentage and fail if threshold exceeded
    let total = with_flags.clone().count().await? as f64;
    let invalid = with_flags.clone()
        .filter(col("is_valid").eq(lit(false)))?
        .count().await? as f64;

    // Treat empty input as zero invalid percentage (avoid 0 / 0 = NaN)
    let invalid_percent = if total == 0.0 { 0.0 } else { invalid / total };
    if invalid_percent > max_invalid_percent {
        return Err(datafusion::error::DataFusionError::Execution(
            format!("Validation failed: {:.1}% invalid rows (threshold: {:.1}%)",
                invalid_percent * 100.0, max_invalid_percent * 100.0)
        ));
    }

    Ok(with_flags)
}

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let sales = dataframe!(
        "order_id" => [1, 2, 3],
        "product" => ["Laptop", "Mouse", "Keyboard"],
        "price" => [1200, 25, 75],
        "quantity" => [1, 5, 2],
        "customer" => ["Alice", "Bob", "Alice"]
    )?;

    // Usage: fail if more than 5% of rows are invalid
    let validated = validate_with_flags(sales.clone(), 0.05).await?;

    // Process valid and invalid rows separately
    let valid_rows = validated.clone().filter(col("is_valid").eq(lit(true)))?;
    let invalid_rows = validated.filter(col("is_valid").eq(lit(false)))?;

    valid_rows.show().await?;

    Ok(())
}
```

> **Note:** This helper reuses `DataFusionError::Execution` so it composes with `?`, but a threshold breach is a business-rule failure, not an engine execution error. In production you may prefer a dedicated application error type to keep quality-gate failures distinct from plan-build and I/O errors.

### Aggregate Quality Reports

Use this for monitoring pipelines, CI/CD quality gates, or dashboards. Produces a single-row summary of data health without modifying the data itself.

**Trade-off:** <br>
Great for observability, but doesn't fix or flag individual rows.

```rust
use datafusion::prelude::*;
use datafusion::functions_aggregate::expr_fn::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let sales = dataframe!(
        "order_id" => [1, 2, 3],
        "product" => ["Laptop", "Mouse", "Keyboard"],
        "price" => [1200, 25, 75],
        "quantity" => [1, 5, 2],
        "customer" => ["Alice", "Bob", "Alice"]
    )?;

    // Build a data quality report using aggregations
    let quality_report = sales.aggregate(
        vec![],  // No grouping—single summary row
        vec![
            count(lit(1)).alias("total_rows"),
            sum(case(col("price").is_null())
                .when(lit(true), lit(1))
                .otherwise(lit(0))?
            ).alias("null_prices"),
            sum(case(col("price").lt_eq(lit(0)))
                .when(lit(true), lit(1))
                .otherwise(lit(0))?
            ).alias("invalid_prices"),
            min(col("price")).alias("min_price"),
            max(col("price")).alias("max_price"),
        ]
    )?;

    // Show the quality report
    quality_report.show().await?;
    // +------------+-------------+----------------+-----------+-----------+
    // | total_rows | null_prices | invalid_prices | min_price | max_price |
    // +------------+-------------+----------------+-----------+-----------+
    // | 3          | 0           | 0              | 25        | 1200      |
    // +------------+-------------+----------------+-----------+-----------+

    println!("Data quality check passed!");
    Ok(())
}
```

**Combining strategies:** In production, you often use all three:

1. **Aggregate** first to assess incoming data quality
2. **Flag** rows to preserve audit trail
3. **Filter** before critical calculations

These patterns let you build validation into your pipeline without external dependencies.

## Quality Inspection

Quality inspection looks at the shape of the data itself—distributions, summary statistics, and how they change across a pipeline—rather than accepting or rejecting individual rows.

### Describing Data

[`.describe()`] is an async action that executes immediately and returns summary statistics for every column—similar to pandas' `df.describe()`. DataFusion packages the result into one standard summary layout; producing the same summary in SQL generally means composing several aggregate expressions or queries.

```rust
use datafusion::prelude::*;
use datafusion::error::Result;

#[tokio::main]
async fn main() -> Result<()> {
    let df = dataframe!(
        "product" => ["A", "B", "C", "D", "E"],
        "price" => [10.0, 25.0, 15.0, 30.0, 20.0],
        "quantity" => [100, 50, 75, 25, 60]
    )?;

    // .describe() executes immediately and returns a summary DataFrame
    let stats = df.describe().await?;
    stats.show().await?;

    // Summary rows: count, null_count, mean, std, min, max, median
    // +------------+---------+-------+----------+
    // | describe   | product | price | quantity |
    // +------------+---------+-------+----------+
    // | count      | 5.0     | 5.0   | 5.0      |
    // | null_count | 0.0     | 0.0   | 0.0      |
    // | mean       | null    | 20.0  | 62.0     |
    // | std        | null    | 7.9   | 27.4     |
    // | min        | A       | 10.0  | 25       |
    // | max        | E       | 30.0  | 100      |
    // | median     | null    | 20.0  | 60.0     |
    // +------------+---------+-------+----------+
    Ok(())
}
```

> **SQL equivalent:** The same summary in SQL generally requires composing several aggregate expressions or queries; `.describe()` packages them into one call.

### Tracking Transformation Effects

For ML pipelines and data-sensitive applications, understanding how transformations affect your data is critical. Did filtering introduce demographic bias? Did a join drop important records? DataFrames enable inspection patterns that answer these questions:

- **Tuple identifiers**: Use [`row_number()`] to assign stable IDs that track individual rows through transformations—essential for debugging "where did this row go?" questions
- **Distribution tracking**: Register pipeline steps as views and compare group counts before/after operations—catches bias introduced by filters or joins
- **Hybrid inspection**: Build pipelines with DataFrames, audit with SQL—leverage each API's strengths

These patterns come from research on [ML pipeline inspection][blue elephants inspecting pandas], which showed that many ML fairness issues originate in data preparation, not model training.

> **See [Advanced DataFrame Topics § Data Quality & Bias Inspection](../dataframes-advance.md#data-quality--bias-inspection)** for complete implementations with code examples.

## Conclusion

Validation and quality checks turn a DataFrame pipeline into something you can trust in production: schema validation confirms structure, constraint validation enforces business rules through filter, flag, and aggregate patterns, and quality inspection tracks how transformations reshape your data. Because these checks are ordinary DataFrame transformations and actions, you can package them as reusable, testable Rust functions—see [Composing DataFrame Pipelines](builder-patterns.md)—while routing structural checks to [Schema Management](../Schema-Management/schema-inspection.md#comparing-and-validating-schemas) and deeper bias auditing to [Advanced DataFrame Topics](../dataframes-advance.md#data-quality--bias-inspection).

[`result<dataframe>`]: https://docs.rs/datafusion/latest/datafusion/error/type.Result.html
[`.describe()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.describe
[`row_number()`]: https://docs.rs/datafusion/latest/datafusion/functions_window/row_number/fn.row_number.html
[pandera]: https://pandera.readthedocs.io/
[great expectations]: https://greatexpectations.io/
[deequ]: https://github.com/awslabs/deequ
[blue elephants inspecting pandas]: https://arxiv.org/abs/2309.07564 "Research paper on inspecting ML pipelines"
