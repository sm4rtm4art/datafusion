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

# Creating DataFrames from a LogicalPlan

**Crack open the DataFrame abstraction — extract, modify, and
reconstruct query plans for federation, DSL builders, or policy
injection.**

Every other creation method on these pages hides the [`LogicalPlan`]
behind convenience APIs. Under the hood, a [`DataFrame`] is a thin
wrapper around two values: a [`SessionState`] (configuration and
catalog) and a [`LogicalPlan`] (the query tree). When higher-level
methods do not give you the control you need — custom DSLs, cross-engine
federation, or transparent policy injection — you can disassemble the
DataFrame, reshape the plan, and seal it back. This page covers that
escape hatch: the extract → modify → reconstruct cycle, and when to
reach for it.

**Key methods:**

| Method                      | Purpose                                                       |
| --------------------------- | ------------------------------------------------------------- |
| [`DataFrame::new()`]        | Construct a DataFrame from a `SessionState` and `LogicalPlan` |
| [`.into_parts()`]           | Extract `(SessionState, LogicalPlan)` from a DataFrame        |
| [`.logical_plan()`]         | Borrow the plan without consuming the DataFrame               |
| [`.execute_logical_plan()`] | Execute a plan with DDL/DML handling, return a DataFrame      |

:::{admonition} Style Note
:class: note
:collapsible: closed

In this document, all code elements are highlighted with backticks.

- DataFrame methods are written as `.method()` (e.g., `.select()`) to reflect the chaining syntax central to the API.
- standalone functions `method()` (e.g `col()`)

- - static constructors `Struckt::method()` (e.g., `SessionContext::new()`).
- Rust types are formatted as `TypeName` (e.g., `SchemaRef`).

:::

```{contents} Table of Contents
:local:
:depth: 2
```

## The Core Pattern

**A DataFrame wraps `(SessionState, LogicalPlan)` — split it apart,
transform the plan with [`LogicalPlanBuilder`], and reassemble.**

Every DataFrame carries the full query context. The
[`.into_parts()`] method destructures it into `(SessionState,
LogicalPlan)`. From there, wrap the plan in a [`LogicalPlanBuilder`]
to chain transformations — filters, projections, joins — and call
[`DataFrame::new(state, plan)`][`DataFrame::new()`] to reconstruct a
new DataFrame. The result is a regular lazy DataFrame: you can continue
chaining `.filter()`, `.select()`, `.aggregate()` and all other
DataFrame operations, or execute it with `.collect()`.

The cycle has five steps:

1. **Create a DataFrame** — using any creation method.
2. **Extract the plan** — [`.into_parts()`] returns `(SessionState, LogicalPlan)`.
3. **Modify the plan** — wrap it in [`LogicalPlanBuilder`] to apply transformations fluently.
4. **Reconstruct** — [`DataFrame::new(state, plan)`][`DataFrame::new()`] creates a new DataFrame.
5. **Execute** — proceed with `.collect()` or any other action.

```rust
use datafusion::prelude::*;
use datafusion::logical_expr::LogicalPlanBuilder;
use datafusion::error::Result;
use datafusion::assert_batches_eq;

#[tokio::main]
async fn main() -> Result<()> {
    // 1. Create a DataFrame
    let df = dataframe!(
        "id" => [1, 2, 3],
        "value" => [10, 20, 30]
    )?;

    // 2. Extract the LogicalPlan and SessionState
    let (state, plan) = df.into_parts();

    // 3. Modify the plan with LogicalPlanBuilder
    let modified_plan = LogicalPlanBuilder::from(plan)
        .filter(col("value").gt(lit(15)))?
        .build()?;

    // 4. Reconstruct a new DataFrame
    let new_df = DataFrame::new(state, modified_plan);

    // 5. Execute and verify
    let batches = new_df.collect().await?;
    assert_batches_eq!(
        &[
            "+----+-------+",
            "| id | value |",
            "+----+-------+",
            "| 2  | 20    |",
            "| 3  | 30    |",
            "+----+-------+",
        ],
        &batches
    );

    Ok(())
}
```

:::{admonition} Why not just use `df.filter()`?
:class: tip

This simple filter _could_ be done with the DataFrame API. The power of
plan-level access becomes apparent when you need to:

- Transform nodes **throughout** the tree (not just append to the top)
- Combine or inspect plans from different sources
- Inject cross-cutting logic at specific node types (e.g., every `TableScan`)

:::

### Read-Only Inspection with `.logical_plan()`

When you need to inspect the plan without consuming the DataFrame,
[`.logical_plan()`] returns a shared reference. This is useful for
logging, debugging, or conditional branching based on plan shape:

```rust
use datafusion::prelude::*;
use datafusion::error::Result;

#[tokio::main]
async fn main() -> Result<()> {
    let df = dataframe!(
        "product" => ["bolt", "nut", "washer"],
        "price" => [1.50, 0.30, 0.10]
    )?;

    // Inspect the plan without consuming the DataFrame
    let plan = df.logical_plan();
    let schema = plan.schema();
    assert_eq!(schema.fields().len(), 2);

    // The DataFrame is still usable — .logical_plan() borrows, not consumes
    let batches = df.collect().await?;
    assert_eq!(batches.len(), 1);

    Ok(())
}
```

### `DataFrame::new()` vs `.execute_logical_plan()`

Both [`DataFrame::new()`] and [`.execute_logical_plan()`] produce a [`DataFrame`] from a [`LogicalPlan`], but they serve
different purposes:

| Method                      | Behavior                                                     | Use when                                    |
| --------------------------- | ------------------------------------------------------------ | ------------------------------------------- |
| [`DataFrame::new()`]        | Pure wrapping — no side effects, no DDL handling             | Plan manipulation, federation, DSL builders |
| [`.execute_logical_plan()`] | Handles DDL/DML (CREATE TABLE, COPY, INSERT) before wrapping | Plans that may contain DDL statements       |

[`.execute_logical_plan()`] inspects the plan: if it contains DDL or DML
statements, those are executed immediately; the resulting DataFrame
wraps whatever remains. `DataFrame::new()` does no such inspection — it
wraps the plan as-is, which is exactly what you want for pure query
transformations.

## When You Need This

**Most DataFusion users never touch `LogicalPlan` directly — the
DataFrame builder API and SQL cover the vast majority of use cases.**

Direct plan access is valuable in a few specific scenarios:

- **Custom DSL builders** — construct DataFusion queries from your own
  language, configuration, or UI-generated query trees
- **Query federation** — exchange plans between DataFusion and other
  engines via [Substrait] or similar formats
- **Policy injection** — transparently add row-level security filters,
  tenant isolation, or audit logging to every `TableScan`
- **Optimizer testing** — inspect or assert plan shapes to verify that
  predicates pushed down, projections pruned, or joins reordered

For deeper coverage of plan construction — manual [`LogicalPlan`]
assembly, [`LogicalPlanBuilder`] fluent API, [`TreeNodeRewriter`] for
tree-walking transformations, and physical plan translation — see the
dedicated [Building Logical Plans] guide.

## References

**Guides:**

- [Building Logical Plans] — comprehensive plan construction, `LogicalPlanBuilder`, and `TreeNodeRewriter`

**API:**

- [`DataFrame::new()`] — construct from `SessionState` and `LogicalPlan`
- [`DataFrame::into_parts()`][`.into_parts()`] — extract state and plan
- [`DataFrame::logical_plan()`][`.logical_plan()`] — borrow the plan
- [`.execute_logical_plan()`] — execute plan with DDL handling
- [`LogicalPlan`] — the query tree enum
- [`LogicalPlanBuilder`] — fluent plan builder

<!-- Link references -->

[Building Logical Plans]: ../../library-user-guide/building-logical-plans.md
[Substrait]: https://substrait.io/
[`DataFrame`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html
[`DataFrame::new()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.new
[`.into_parts()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.into_parts
[`.logical_plan()`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.logical_plan
[`.execute_logical_plan()`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.execute_logical_plan
[`LogicalPlan`]: https://docs.rs/datafusion/latest/datafusion/logical_expr/enum.LogicalPlan.html
[`LogicalPlanBuilder`]: https://docs.rs/datafusion/latest/datafusion/logical_expr/struct.LogicalPlanBuilder.html
[`SessionState`]: https://docs.rs/datafusion/latest/datafusion/execution/session_state/struct.SessionState.html
