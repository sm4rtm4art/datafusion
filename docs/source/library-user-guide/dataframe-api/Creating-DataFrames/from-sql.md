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

**[`.sql()`] turns a SQL string into a lazy [`DataFrame`] — enabling
SQL and the builder API as two paths to DataFusion's single query
engine.**

DataFusion provides two equally powerful ways to build query plans:
SQL strings and the DataFrame builder API. Both compile to the same
[`LogicalPlan`], receive the same optimizations, and execute with
identical performance. [`.sql()`] parses SQL into a [`LogicalPlan`]
and wraps it in a [`DataFrame`], so the result supports every builder
method — `.filter()`, `.select()`, `.aggregate()` — just like any
other DataFrame. This page covers the SQL entry point, safety
controls, and patterns for combining both APIs in a single pipeline.

**Key methods:**
| Method | Purpose |
| ------------------------------- | ------------------------------------------------------------ |
| [`.sql()`] | Parse a SQL query, return a lazy [`DataFrame`] |
| [`.sql_with_options()`] | Same, with controls to block DDL, DML, or session statements |
| [`SessionContext::parse_sql_expr()`] | Parse a SQL expression into an [`Expr`] for builder chains |
| [`.into_view()`] | Convert a [`DataFrame`]'s plan into a [`TableProvider`] |
| [`.register_table()`] | Place a [`TableProvider`] in the catalog under a name |

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

```{contents} Table of Contents
:local:
:depth: 2
```

## From SQL String to DataFrame

**The DataFrame API builds plans method by method. [`.sql()`] builds
the same plan from a SQL string — one call, same lazy [`DataFrame`]
back.**

In DataFusion, DataFrames can be created from multiple sources —
[files](from-files/index.md),
[registered tables](registered-tables.md), SQL queries, and more.
[`.sql()`] takes the SQL path: it parses the string into an AST,
converts the AST to a [`LogicalPlan`], and returns
`DataFrame::new(state, plan)`. The result is lazy — no data is read
until an action triggers execution. From that point, every builder
method works exactly as it does on any other DataFrame.

### Basic Usage

**Pass a SQL string to [`.sql()`] and chain builder methods on the
result — SQL defines the starting plan, the builder API refines it.**

The [`.sql()`] method accepts any valid SQL `SELECT` statement. The
returned [`DataFrame`] carries the parsed [`LogicalPlan`] and supports
the full builder API. This example uses `VALUES` to create inline
data, then applies `.filter()` and `.select()` to narrow the result.

```rust
use datafusion::prelude::*;
use datafusion::assert_batches_eq;
use datafusion::error::Result;

#[tokio::main]
async fn main() -> Result<()> {
    let ctx = SessionContext::new();

    // SQL creates the base plan — returns a lazy DataFrame
    let df = ctx.sql(
        "SELECT * FROM (VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Carol')) AS t(id, name)"
    ).await?;

    // Continue with the builder API
    let result = df
        .filter(col("id").gt(lit(1)))?
        .select(vec![col("name")])?
        .collect()
        .await?;

    assert_batches_eq!(
        &[
            "+-------+",
            "| name  |",
            "+-------+",
            "| Bob   |",
            "| Carol |",
            "+-------+",
        ],
        &result
    );

    Ok(())
}
```

### Controlling Allowed SQL with `.sql_with_options()`

**Restrict which SQL operations are permitted before they reach the
[`LogicalPlan`].**

[`.sql_with_options()`] validates the [`LogicalPlan`] against
[`SQLOptions`] before execution — rejecting disallowed operations
with an error instead of running them. Use this whenever SQL strings
come from outside your application (user input, configuration files,
API endpoints). For hardcoded SQL within your own code, plain
[`.sql()`] is sufficient.

| Flag                            | Blocks                                                     | Default |
| ------------------------------- | ---------------------------------------------------------- | ------- |
| `.with_allow_ddl(false)`        | `CREATE TABLE`, `DROP TABLE`, `ALTER TABLE`, `CREATE VIEW` | `true`  |
| `.with_allow_dml(false)`        | `INSERT INTO`, `COPY`, `UPDATE`, `DELETE`                  | `true`  |
| `.with_allow_statements(false)` | `SET VARIABLE`, `BEGIN TRANSACTION`                        | `true`  |

```rust
use datafusion::prelude::*;
use datafusion::error::Result;

#[tokio::main]
async fn main() -> Result<()> {
    let ctx = SessionContext::new();

    // Allow only SELECT — block DDL and DML
    let safe = SQLOptions::new()
        .with_allow_ddl(false)
        .with_allow_dml(false);

    // SELECT works: returns a DataFrame as usual
    let df = ctx.sql_with_options("SELECT 1 AS id", safe.clone()).await?;
    assert_eq!(df.collect().await?.len(), 1);

    // DDL is rejected before execution
    let err = ctx
        .sql_with_options("CREATE TABLE forbidden (x INT)", safe)
        .await
        .unwrap_err();
    assert!(err.to_string().contains("DDL"));

    Ok(())
}
```

[`.sql()`] can also execute DDL (`CREATE TABLE`, `DROP TABLE`) and
DML (`INSERT INTO`, `COPY`). Unlike `SELECT` queries, **these execute
eagerly** — the side effect happens inside the [`.sql()`] call itself,
and the returned [`DataFrame`] is empty (zero rows, no plan). For the
full SQL syntax, see the
[SQL Reference][sql-reference].

:::{admonition} One statement at a time
:class: warning
[`.sql()`] currently accepts exactly **one** SQL statement per call.
This is a known limitation; passing multiple semicolon-separated
statements returns an error. Call [`.sql()`] once per statement
instead:

```rust
# use datafusion::prelude::*;
# use datafusion::error::Result;
# #[tokio::main]
# async fn main() -> Result<()> {
# let ctx = SessionContext::new();
// ✅ One call per statement
ctx.sql("CREATE TABLE a (x INT)").await?;
ctx.sql("CREATE TABLE b (y INT)").await?;
# Ok(())
# }
```

:::

---

## SQL Workflow Patterns

**Each API has ergonomic blind spots — these patterns show how to
combine them so each operates at its strongest.**

The first pattern starts with a SQL query and extends it through the
builder API — ideal when the core logic is naturally declarative.
The second reverses the flow: build programmatically, expose to SQL
via a view, then continue with either API. This flexibility lets
DataFusion adapt to different architectural needs within the same
pipeline.

### SQL → DataFrame Refinement

**Use SQL for the core analytical query, then extend it with the
builder API — ideal when the heavy logic is naturally declarative.**

The [`DataFrame`] returned by [`.sql()`] is the transition point:
everything before it is SQL syntax, everything after it is the
builder API. To see the full SQL result before applying the builder
filter, we register the query as a view and query it twice:

```rust
# use std::sync::Arc;
# use datafusion::arrow::array::{ArrayRef, Int32Array, StringArray};
# use datafusion::arrow::record_batch::RecordBatch;
use datafusion::prelude::*;
use datafusion::assert_batches_sorted_eq;
use datafusion::error::Result;

#[tokio::main]
async fn main() -> Result<()> {
    let ctx = SessionContext::new();

    // Register "sales" (region, product, amount)
    # let sales = RecordBatch::try_from_iter(vec![
    #     ("region", Arc::new(StringArray::from(vec!["North", "North", "South", "South"])) as ArrayRef),
    #     ("product", Arc::new(StringArray::from(vec!["Widget", "Gadget", "Widget", "Gadget"])) as ArrayRef),
    #     ("amount", Arc::new(Int32Array::from(vec![8000, 3000, 6000, 4500])) as ArrayRef),
    # ])?;
    # ctx.register_batch("sales", sales)?;

    // SQL: CTE + window function — rank products per region
    let sql = "
        WITH ranked AS (
            SELECT region, product, amount,
                   ROW_NUMBER() OVER (PARTITION BY region ORDER BY amount DESC) AS rank
            FROM sales
        )
        SELECT region, product, amount FROM ranked WHERE rank = 1
    ";

    // Register the SQL query as a view for reuse
    ctx.register_table("top_products", ctx.sql(sql).await?.into_view())?;

    // Full SQL result — both region winners
    let full_result = ctx.table("top_products").await?.collect().await?;
    assert_batches_sorted_eq!(
        &[
            "+--------+---------+--------+",
            "| region | product | amount |",
            "+--------+---------+--------+",
            "| North  | Widget  | 8000   |",
            "| South  | Widget  | 6000   |",
            "+--------+---------+--------+",
        ],
        &full_result
    );

    // Builder API: apply a runtime threshold on top
    let min_amount = 7000;
    let filtered = ctx.table("top_products").await?
        .filter(col("amount").gt(lit(min_amount)))?
        .collect()
        .await?;

    assert_batches_sorted_eq!(
        &[
            "+--------+---------+--------+",
            "| region | product | amount |",
            "+--------+---------+--------+",
            "| North  | Widget  | 8000   |",
            "+--------+---------+--------+",
        ],
        &filtered
    );

    Ok(())
}
```

### Round-Trip: DataFrame → SQL → DataFrame

**Build data programmatically, expose it to SQL, then continue with
the builder API — use each API where its ergonomics shine.**

This pattern lets you place the API boundary exactly where it helps
most: dynamic logic and runtime conditions in the builder API,
complex analytics (CTEs, window functions, aggregation) in SQL.
The round-trip is possible because [`.into_view()`] and
[`.register_table()`] work as a two-step pipeline:

```text
┌─────────────────────────────────────────┐
│  DataFrame                              │
│  (carries a lazy LogicalPlan)           │
└──────────────┬──────────────────────────┘
               │
               │  .into_view()
               │  Consumes the DataFrame, wraps its
               │  LogicalPlan in a TableProvider.
               │  No data is materialized.
               ▼
┌─────────────────────────────────────────┐
│  Arc<dyn TableProvider>                 │
│  (plan-backed, implements TableProvider)│
└──────────────┬──────────────────────────┘
               │
               │  .register_table("name", provider)
               │  Places the TableProvider in the
               │  catalog under a queryable name.
               ▼
┌─────────────────────────────────────────┐
│  Catalog                                │
│  "SELECT ... FROM name" now resolves    │
│  to the original LogicalPlan.           │
└─────────────────────────────────────────┘
```

Both steps are required: [`.into_view()`] converts the
[`DataFrame`] into a [`TableProvider`], and [`.register_table()`]
makes it discoverable by name. The view stores the plan, not
materialized data, so queries compose lazily. For additional
details on view registration, see
[Registered Tables][registered-tables].

```rust
# use std::sync::Arc;
# use datafusion::arrow::array::{ArrayRef, Int32Array, StringArray};
# use datafusion::arrow::record_batch::RecordBatch;
use datafusion::prelude::*;
use datafusion::assert_batches_sorted_eq;
use datafusion::error::Result;

#[tokio::main]
async fn main() -> Result<()> {
    let ctx = SessionContext::new();

    // Register "sales" (region, product, amount)
    # let sales = RecordBatch::try_from_iter(vec![
    #     ("region", Arc::new(StringArray::from(vec![
    #         "North", "North", "South", "South", "South"
    #     ])) as ArrayRef),
    #     ("product", Arc::new(StringArray::from(vec![
    #         "Widget", "Gadget", "Widget", "Gadget", "Gizmo"
    #     ])) as ArrayRef),
    #     ("amount", Arc::new(Int32Array::from(vec![
    #         8000, 3000, 12000, 4500, 200
    #     ])) as ArrayRef),
    # ])?;
    # ctx.register_batch("sales", sales)?;

    // Step 1 (Builder API): programmatic filter
    let high_value = ctx.table("sales").await?
        .filter(col("amount").gt(lit(1000)))?;

    // Step 2: expose to SQL as a named view
    ctx.register_table("high_value_sales", high_value.into_view())?;

    // Step 3 (SQL): aggregation where declarative syntax shines
    let summary = ctx.sql("
        SELECT region,
               COUNT(DISTINCT product) AS product_count,
               SUM(amount) AS total_revenue
        FROM high_value_sales
        GROUP BY region
    ").await?;

    // Step 4 (Builder API): sort the result
    let result = summary
        .sort(vec![col("total_revenue").sort(false, true)])?
        .collect()
        .await?;

    assert_batches_sorted_eq!(
        &[
            "+--------+---------------+---------------+",
            "| region | product_count | total_revenue |",
            "+--------+---------------+---------------+",
            "| North  | 2             | 11000         |",
            "| South  | 2             | 16500         |",
            "+--------+---------------+---------------+",
        ],
        &result
    );

    Ok(())
}
```

### When to Use Which

**[`.into_view()`] and [`.register_table()`] bridge the two APIs —
choose each API where its ergonomics fit best.**

Because both APIs compile to the same [`LogicalPlan`], performance is
identical. The choice is about developer ergonomics: where does your
query logic originate, and which syntax expresses it most naturally?
Use [`.into_view()`] + [`.register_table()`] at the boundary whenever
you need to switch between APIs mid-pipeline.

| SQL excels at                          | DataFrame excels at                   |
| -------------------------------------- | ------------------------------------- |
| Window functions (`ROW_NUMBER`, `LAG`) | Dynamic filtering based on variables  |
| CTEs for multi-step transformations    | Programmatic column selection         |
| Complex JOINs and set operations       | Iterative/conditional transformations |
| Familiar syntax for SQL developers     | Type-safe integration, no injection   |
| Queries from config files / user input | Compile-time safety, IDE support      |

For the full architectural comparison — including the parser vs.
builder diagram and security analysis — see
[Two Paths to the Same Plan][builder-parser].

### External Data Sources and Pushdown

**When querying external systems through custom [`TableProvider`]s,
DataFusion's optimizer can push filters and projections to the
source — reducing data transfer without changing your SQL or
DataFrame code.**

If a [`TableProvider`] wrapping an external database (e.g.,
PostgreSQL) is registered in the catalog, both SQL and the builder
API benefit from pushdown. The [`TableProvider`] reports which
filters it can handle via `supports_filters_pushdown()`, and
DataFusion's optimizer pushes matching predicates and projections
to the source system automatically:

```rust,ignore
# use datafusion::prelude::*;
# use datafusion::error::Result;
# #[tokio::main]
# async fn main() -> Result<()> {
# let ctx = SessionContext::new();
// SQL path — optimizer pushes WHERE and SELECT to the source
let df = ctx.sql(
    "SELECT name FROM pg_users WHERE active = true"
).await?;

// Builder path — same pushdown, same result
let df = ctx.table("pg_users").await?
    .filter(col("active").eq(lit(true)))?
    .select(vec![col("name")])?;
# Ok(())
# }
```

For implementation details on building custom providers, see
[Custom Table Providers][custom-table-providers].

---

## Bringing It Together

[`.sql()`] and [`.sql_with_options()`] turn SQL strings into lazy
DataFrames — the same [`LogicalPlan`], the same optimizer, the same
execution engine as the builder API. Use SQL where declarative syntax
is most natural (CTEs, window functions, complex joins), then switch
to the builder API for dynamic logic, runtime conditions, and
type-safe composition. [`.into_view()`] and [`.register_table()`]
bridge the two directions, letting you place the API boundary
wherever it helps most. For external data sources, DataFusion's
pushdown optimization applies identically to both paths.

---

## Further Reading

**Concepts & Guides:**

- [Two Paths to the Same Plan][builder-parser] — How SQL and DataFrame APIs converge into the same [`LogicalPlan`]
- [Choosing the Right API][builder-parser] — Decision guide for API selection
- [Registered Tables][registered-tables] — Registration, views, and catalog inspection
- [SQL Reference][sql-reference] — Full SQL syntax, functions, and data types
- [Custom Table Providers][custom-table-providers] — Building providers with filter and projection pushdown

**API Documentation:**

- [`SessionContext::sql()`] — Execute SQL, returns a lazy [`DataFrame`]
- [`SessionContext::sql_with_options()`] — SQL with operation controls
- [`SessionContext::parse_sql_expr()`] — Parse a SQL expression into an [`Expr`]
- [`SQLOptions`] — DDL/DML/statement flags
- [`.into_view()`] — Convert a [`DataFrame`]'s plan into a [`TableProvider`]
- [`.register_table()`] — Place a [`TableProvider`] in the catalog under a name

---

<!-- References -->

<!-- Internal documentation -->

[builder-parser]: ../Concepts/builder-parser.md
[custom-table-providers]: ../../custom-table-providers.md
[registered-tables]: registered-tables.md
[sql-reference]: ../../../user-guide/sql/index.rst

<!-- Core types -->

[`dataframe`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html
[`expr`]: https://docs.rs/datafusion/latest/datafusion/logical_expr/enum.Expr.html
[`logicalplan`]: https://docs.rs/datafusion-expr/latest/datafusion_expr/logical_plan/enum.LogicalPlan.html
[`sqloptions`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SQLOptions.html
[`tableprovider`]: https://docs.rs/datafusion/latest/datafusion/catalog/trait.TableProvider.html

<!-- Methods and functions -->

[`.into_view()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.into_view
[`.register_table()`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.register_table
[`.sql()`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.sql
[`.sql_with_options()`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.sql_with_options
[`sessioncontext::parse_sql_expr()`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.parse_sql_expr
[`sessioncontext::sql()`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.sql
[`sessioncontext::sql_with_options()`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.sql_with_options
