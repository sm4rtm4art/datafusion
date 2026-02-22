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

# Creating DataFrames from SQL Queries

<!--TODO

1. ABSTRACT
2. INTRODUCTION
-->

```{contents}
:local:
:depth: 2
:caption: Creation DataFrames from SQL Queries
```

## Introduction (placeholder)

**[`ctx.sql()`][`.sql()`] returns a lazy `DataFrame`—making SQL a first-class DataFrame creation method, not just a query interface.**

[Section 2](#2-from-a-registered-table) showed how to register tables and access them via [`ctx.table()`][`.table()`]. Here, SQL itself becomes the entry point: you write a query, and the result is a `DataFrame` you can transform programmatically.

**The key insight**:<br> Since [`ctx.sql()`][`.sql()`] returns a DataFrame, you can combine SQL's
declarative power (CTEs, window functions, complex joins) with the DataFrame API's
programmatic flexibility (dynamic filters, conditional logic, Rust integration)—all
in a single, optimized pipeline.

The following patterns show two directions for bridging both APIs:

- **Pattern 1 (SQL → DataFrame)**: Start with SQL, refine with DataFrame operations
- **Pattern 2 (DataFrame → SQL → DataFrame)**: Use [`.into_view()`] to expose DataFrames to SQL mid-pipeline

#### Pattern 1: SQL-first workflow

**Start with SQL, finish with DataFrame—ideal when the analytical logic is naturally expressed in SQL.**

SQL handles the core analytical logic; DataFrame operations add the programmatic
finishing touches.

**Use this when** the core logic is best expressed in SQL and you want programmatic refinement afterward.

```rust
# use std::sync::Arc;
use datafusion::prelude::*;
# use datafusion::arrow::array::{ArrayRef, Int32Array, StringArray};
# use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::Result;

#[tokio::main]
async fn main() -> Result<()> {
    let ctx = SessionContext::new();
#
#     // Create in-memory sales data
#     let sales = RecordBatch::try_from_iter(vec![
#         ("region", Arc::new(StringArray::from(vec!["North", "North", "South", "South"])) as ArrayRef),
#         ("product", Arc::new(StringArray::from(vec!["Widget", "Gadget", "Widget", "Gadget"])) as ArrayRef),
#         ("amount", Arc::new(Int32Array::from(vec![8000, 3000, 6000, 4500])) as ArrayRef),
#     ])?;
#     ctx.register_batch("sales", sales)?;
    // Assume "sales" table is registered (Parquet, CSV, or in-memory)

    // Step 1: Execute complex analytical query in SQL
    let df = ctx.sql("
        WITH ranked_sales AS (
            SELECT
                region,
                product,
                amount,
                ROW_NUMBER() OVER (PARTITION BY region ORDER BY amount DESC) as rank
            FROM sales
        )
        SELECT * FROM ranked_sales WHERE rank <= 3
    ").await?;  // Returns a lazy DataFrame

    // Step 2: Continue with DataFrame API for dynamic refinement
    let top_profitable = df.filter(col("amount").gt(lit(5000)))?;

    top_profitable.show().await?;  // Executes the full, optimized pipeline
    Ok(())
}
```

#### Pattern 2: Round-trip workflow

**DataFrame → SQL → DataFrame—use both APIs at their strongest points in a single pipeline.**

This pattern uses [`.into_view()`] to convert a DataFrame into a logical view, which you then register with [`register_table()`][`.register_table()`] so SQL can reference it by name. The view captures the DataFrame's query plan (not materialized data)—each SQL query against it re-executes the underlying plan.

You prepare data programmatically (dynamic filters, computed columns), expose it to SQL for complex analytics, then continue with DataFrame operations for final enrichment.

**Use this when** you need programmatic preparation, SQL-based analysis, and programmatic finishing—all in one pipeline.

```rust
# use std::sync::Arc;
use datafusion::prelude::*;
# use datafusion::arrow::array::{ArrayRef, Int32Array, StringArray};
# use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::Result;

#[tokio::main]
async fn main() -> Result<()> {
    let ctx = SessionContext::new();
#
#     // Create in-memory sales data with varied amounts
#     let sales = RecordBatch::try_from_iter(vec![
#         ("region", Arc::new(StringArray::from(vec![
#             "North", "North", "North", "South", "South", "South"
#         ])) as ArrayRef),
#         ("product", Arc::new(StringArray::from(vec![
#             "Widget", "Gadget", "Gizmo", "Widget", "Gadget", "Gizmo"
#         ])) as ArrayRef),
#         ("amount", Arc::new(Int32Array::from(vec![
#             8000, 3000, 500, 12000, 4500, 200
#         ])) as ArrayRef),
#     ])?;
#     ctx.register_batch("sales", sales)?;
    // Assume "sales" table is registered

    // Step 1 (DataFrame): Programmatically prepare and filter
    let high_value = ctx.table("sales").await?
        .filter(col("amount").gt(lit(1000)))?
        .select(vec![col("region"), col("product"), col("amount")])?;

    // Step 2: Register intermediate DataFrame as temporary view
    ctx.register_table("high_value_sales", high_value.into_view())?;

    // Step 3 (SQL): Run complex aggregation on prepared data
    let summary = ctx.sql("
        SELECT region,
               COUNT(DISTINCT product) as product_count,
               SUM(amount) as total_revenue
        FROM high_value_sales
        GROUP BY region
        HAVING SUM(amount) > 10000
    ").await?;

    // Step 4 (DataFrame): Apply final programmatic enrichment
    let result = summary
        .with_column("revenue_millions", col("total_revenue") / lit(1_000_000))?
        .sort(vec![col("total_revenue").sort(false, true)])?
        .limit(0, Some(5))?;

    result.show().await?;
    Ok(())
}
```

#### Choosing the right tool

Now that you've seen both patterns, here's a quick reference for when each API shines:

| SQL excels at                          | DataFrame excels at                   |
| :------------------------------------- | ------------------------------------- |
| Window functions (`ROW_NUMBER`, `LAG`) | Dynamic filtering based on variables  |
| CTEs for multi-step transformations    | Programmatic column selection         |
| Complex JOINs and set operations       | Iterative/conditional transformations |
| Familiar syntax for SQL developers     | Type-safe Rust integration            |

For deeper guidance on when to choose which API, see [When to Choose Which?](concepts.md#when-to-choose-which) in the Concepts guide.

> **Advanced**: For external data sources (PostgreSQL, etc.) via custom [`TableProvider`]s, filters/projections may push down to the source system; remaining operations execute columnar in DataFusion.

#### Additional References

**Concepts & Guides:**

- [Two Paths to the Same Plan](concepts.md#two-paths-to-the-same-plan-parser-vs-builder) — How SQL and DataFrame APIs converge
- [When to Choose Which?](concepts.md#when-to-choose-which) — Decision guide for API selection
- [SQL Reference](../../user-guide/sql/index.rst) — Full SQL syntax, functions, and data types

**API Documentation:**

- [`SessionContext::sql()`][`.sql()`] — Execute SQL, returns a lazy DataFrame
- [`SessionContext::sql_with_options()`][`.sql_with_options()`] — SQL with safety controls (disable DDL, DML, or statements)
- [`.into_view()`] — Convert DataFrame to a view for SQL access
- [`register_table()`][`.register_table()`] — Register a TableProvider (including views) in the catalog

---
